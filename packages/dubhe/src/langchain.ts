import { randomUUID } from 'node:crypto';
import { CallbackManager, ensureHandler } from '@langchain/core/callbacks/manager';
import { BaseCallbackHandler } from '@langchain/core/callbacks/base';
import type { RunnableConfig } from '@langchain/core/runnables';
import { hrTime } from '@opentelemetry/core';
import { SpanStatusCode, type Span } from '@opentelemetry/api';
import type { RunOutcome, VerifiedTurn } from './domain.js';
import { modelRequest, recordUsage } from './model.js';
import { endTool, toolObservation, type ToolOptions, type ToolObservation } from './tool.js';
import { prepareTurn, validId, type PreparedTurn, type TracingSdk } from './sdk.js';

export type { ToolObservation, ToolOptions } from './tool.js';

export interface LangChainTracingOptions {
  /** Trusted native LangChain tool names mapped to canonical local tool IDs. */
  localTools?: Readonly<Record<string, string>>;
}

type Callbacks = NonNullable<RunnableConfig['callbacks']>;
type PreparedConfig<Config> = Omit<Config, 'callbacks'> & { callbacks: Callbacks };

function isAsyncIterable(value: unknown): value is AsyncIterable<unknown> {
  return value != null && typeof (value as Partial<AsyncIterable<unknown>>)[Symbol.asyncIterator] === 'function';
}

async function* observeStream(
  stream: AsyncIterable<unknown>,
  end: (outcome?: RunOutcome) => void,
  signal?: AbortSignal,
): AsyncGenerator<unknown> {
  let outcome: RunOutcome | undefined;
  try {
    for await (const chunk of stream) yield chunk;
    if (!signal?.aborted) outcome = 'completed';
  } catch (error) {
    if (!signal?.aborted) outcome = 'failed';
    throw error;
  } finally {
    end(outcome);
  }
}

function isDefaultAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError' && error.message === 'This operation was aborted';
}

export class LangChainTracing {
  readonly #handlers = new WeakMap<Callbacks, TurnCallback>();

  readonly #localTools: ReadonlyMap<string, string>;

  constructor(private readonly sdk: TracingSdk, options: LangChainTracingOptions = {}) {
    const entries = Object.entries(options.localTools ?? {});
    if (entries.some(([name, id]) => !name || !validId(id))) throw new TypeError('Invalid local tool mapping');
    this.#localTools = new Map(entries);
  }

  async prepareConfig<Config extends RunnableConfig = RunnableConfig>(
    turn: VerifiedTurn,
    existingConfig?: Config,
  ): Promise<PreparedConfig<Config>> {
    const existing = existingConfig?.callbacks;
    const callbacks = !existing || Array.isArray(existing) ? [...(existing ?? [])] : existing.copy();
    const preparedTurn = await this.sdk[prepareTurn](turn);
    const handler = new TurnCallback(this, preparedTurn, this.#localTools, existingConfig?.signal);
    if (Array.isArray(callbacks)) callbacks.push(handler);
    else callbacks.addHandler(handler, true);
    this.#handlers.set(callbacks, handler);
    return { ...existingConfig, callbacks } as PreparedConfig<Config>;
  }

  withAgent<T>(
    config: RunnableConfig,
    agentId: string,
    operation: (childConfig: RunnableConfig) => Promise<AsyncIterable<T>>,
  ): Promise<AsyncIterable<T>>;
  withAgent<T>(
    config: RunnableConfig,
    agentId: string,
    operation: (childConfig: RunnableConfig) => Promise<T>,
  ): Promise<T>;
  async withAgent(
    config: RunnableConfig,
    agentId: string,
    operation: (childConfig: RunnableConfig) => Promise<unknown>,
  ): Promise<unknown> {
    const handler = this.#handler(config.callbacks);
    return handler ? handler.withAgent(config, agentId, operation) : operation(config);
  }

  async withTool<T>(
    config: RunnableConfig,
    options: ToolOptions,
    operation: (tool: ToolObservation) => Promise<T>,
  ): Promise<T> {
    const handler = this.#handler(config.callbacks);
    return handler ? handler.withTool(config, options, operation) : operation(toolObservation(undefined, options));
  }

  #handler(callbacks?: Callbacks): TurnCallback | undefined {
    if (!callbacks) return undefined;
    return this.#handlers.get(callbacks) ??
      (Array.isArray(callbacks) ? callbacks : callbacks.handlers)
        .find((handler): handler is TurnCallback => handler instanceof TurnCallback && handler.owner === this);
  }

  finish(config: RunnableConfig): void {
    const callbacks = config.callbacks;
    if (!callbacks) return;
    const handler = this.#handlers.get(callbacks);
    this.#handlers.delete(callbacks);
    handler?.finish();
  }
}

class TurnCallback extends BaseCallbackHandler {
  readonly name = 'dubhe';
  readonly #parents = new Map<string, Span>();
  readonly #models = new Map<string, Span>();
  readonly #tools = new Map<string, Span>();
  readonly #agents = new WeakMap<Span, string>();
  readonly #openAgents = new Set<(outcome?: RunOutcome) => void>();
  readonly #ambiguousModels = new Map<Span, number>();
  #turn?: PreparedTurn;
  #rootRunId?: string;
  #rootResult?: { endedAt: number; outcome?: RunOutcome };
  #nestedChainAborted = false;
  #explicitChild = false;
  #topLevelModels?: Set<string>;
  #topLevelParentId?: string;
  #topLevelOutcome: RunOutcome | undefined = 'completed';

  constructor(
    readonly owner: LangChainTracing,
    turn: PreparedTurn,
    private readonly localTools: ReadonlyMap<string, string>,
    private readonly signal?: AbortSignal,
  ) {
    super({ _awaitHandler: true });
    this.#turn = turn;
    this.#agents.set(turn.root, turn.rootAgentId);
  }

  handleChainStart(...args: Parameters<NonNullable<BaseCallbackHandler['handleChainStart']>>): void {
    // The callback manager currently supplies the parent in slot four. Accept
    // the declared slot as well when it names a run this handler has observed.
    const parent = [args[3], args[7]].find((id) => id !== undefined && this.#parents.has(id));
    this.#start(args[2], parent);
  }

  handleChatModelStart(...args: Parameters<NonNullable<BaseCallbackHandler['handleChatModelStart']>>): void {
    const span = this.#start(args[2], args[3], true);
    if (span) span.setAttributes(modelRequest(args[4], this.#turn?.captureModelInputs === true, args[1][0] ?? []));
  }

  handleLLMStart(...args: Parameters<NonNullable<BaseCallbackHandler['handleLLMStart']>>): void {
    const span = this.#start(args[2], args[3], true);
    if (span) span.setAttributes(modelRequest(args[4], this.#turn?.captureModelInputs === true, args[1]));
  }

  handleToolStart(...args: Parameters<NonNullable<BaseCallbackHandler['handleToolStart']>>): void {
    const toolId = this.localTools.get(args[6] ?? '');
    const parent = this.#start(args[2], args[3]);
    if (!toolId || !parent || !this.#turn) return;
    const span = this.#child('tool_client', parent);
    toolObservation(span, { toolId, route: 'local' });
    this.#tools.set(args[2], span);
    this.#parents.set(args[2], span);
  }

  handleToolEnd(_result: unknown, runId: string): void {
    this.#finishTool(runId, 'completed');
  }

  handleToolError(error: unknown, runId: string): void {
    this.#finishTool(runId, this.signal?.aborted ? 'cancelled' : 'failed', error);
  }

  #finishTool(runId: string, outcome: RunOutcome, error?: unknown): void {
    const span = this.#tools.get(runId);
    if (span) endTool(span, outcome, error);
    this.#tools.delete(runId);
    this.#end(runId, outcome);
  }

  #callbackParent(config: RunnableConfig): Span | undefined {
    const callbacks = config.callbacks;
    const parentId = callbacks && !Array.isArray(callbacks) ? callbacks.getParentRunId() : undefined;
    return parentId ? this.#parents.get(parentId) : this.#turn?.root;
  }

  #child(kind: 'llm' | 'agent' | 'tool_client', parent: Span, agentId = this.#agents.get(parent)!): Span {
    const span = this.#turn!.startChild(kind, agentId, parent);
    this.#agents.set(span, agentId);
    return span;
  }

  async withAgent(
    config: RunnableConfig,
    agentId: string,
    operation: (config: RunnableConfig) => Promise<unknown>,
  ): Promise<unknown> {
    const parent = this.#callbackParent(config);
    if (!parent || !this.#turn?.allowsAgent(agentId)) {
      const callbacks = config.callbacks;
      if (!callbacks) return operation(config);
      if (Array.isArray(callbacks)) return operation({ ...config, callbacks: callbacks.filter((handler) => handler !== this) });
      const remaining = callbacks.copy();
      remaining.removeHandler(this);
      return operation({ ...config, callbacks: remaining });
    }
    const span = this.#child('agent', parent, agentId);
    this.#explicitChild = true;
    const runId = randomUUID();
    const callbacks = config.callbacks;
    const manager = new CallbackManager(runId);
    if (Array.isArray(callbacks)) manager.setHandlers(callbacks.map(ensureHandler));
    else if (callbacks) {
      manager.setHandlers(callbacks.inheritableHandlers);
      manager.addTags(callbacks.inheritableTags);
      manager.addMetadata(callbacks.inheritableMetadata);
    }
    this.#parents.set(runId, span);
    const end = (outcome?: RunOutcome) => {
      if (!this.#openAgents.delete(end)) return;
      this.#parents.delete(runId);
      if (outcome === 'completed') span.setStatus({ code: SpanStatusCode.OK });
      if (outcome === 'failed') span.setStatus({ code: SpanStatusCode.ERROR });
      span.end(hrTime());
    };
    this.#openAgents.add(end);
    try {
      const result = await operation({ ...config, callbacks: manager });
      if (isAsyncIterable(result)) return observeStream(result, end, config.signal);
      end(config.signal?.aborted ? undefined : 'completed');
      return result;
    } catch (error) {
      end(config.signal?.aborted ? undefined : 'failed');
      throw error;
    }
  }

  async withTool<T>(config: RunnableConfig, options: ToolOptions, operation: (tool: ToolObservation) => Promise<T>): Promise<T> {
    const turn = this.#turn;
    const parent = this.#callbackParent(config);
    if (!turn || !parent || !validId(options.toolId)) return operation(toolObservation(undefined, options));
    const span = this.#child('tool_client', parent);
    this.#explicitChild = true;
    const agentId = this.#agents.get(span)!;
    const observation = toolObservation(span, options, (url) => turn.gatewayBinding(agentId, url));
    try {
      const result = await operation(observation);
      endTool(span, 'completed');
      return result;
    } catch (error) {
      endTool(span, config.signal?.aborted ? 'cancelled' : 'failed', error);
      throw error;
    }
  }

  handleChainEnd(_output: unknown, runId: string): void {
    this.#end(runId, 'completed');
  }

  handleChainError(error: unknown, runId: string): void {
    if (runId !== this.#rootRunId) this.#nestedChainAborted = isDefaultAbort(error);
    // LangGraph reports an iterator close as a default abort in the nested run
    // and replaces it with its own Error('Abort') at the graph boundary.
    const graphAborted = runId === this.#rootRunId && this.#nestedChainAborted &&
      error instanceof Error && error.name === 'Error' && error.message === 'Abort';
    this.#end(runId, this.signal?.aborted || graphAborted || error == null ? undefined : 'failed');
  }

  handleLLMEnd(...args: Parameters<NonNullable<BaseCallbackHandler['handleLLMEnd']>>): void {
    try {
      const model = this.#models.get(args[1]);
      if (model) recordUsage(model, args[0], args[4]);
    } finally {
      this.#end(args[1], 'completed');
    }
  }

  handleLLMError(error: unknown, runId: string): void {
    const model = this.#models.get(runId);
    if (model && !this.signal?.aborted && isDefaultAbort(error)) {
      // The graph result or host confirmation can disambiguate this error.
      this.#ambiguousModels.set(model, performance.now());
      this.#models.delete(runId);
    }
    this.#end(runId, this.signal?.aborted ? undefined : 'failed');
  }

  #start(runId: string, parentRunId?: string, model = false): Span | undefined {
    const turn = this.#turn;
    if (!turn || this.#rootResult || this.#parents.has(runId)) return;
    let parent: Span | undefined;
    if (parentRunId) parent = this.#parents.get(parentRunId);
    if (!parent && (this.#rootRunId === undefined || (model && this.#topLevelModels && parentRunId === this.#topLevelParentId))) {
      if (this.#rootRunId === undefined) {
        this.#rootRunId = runId;
        if (model) {
          this.#topLevelModels = new Set();
          this.#topLevelParentId = parentRunId;
        }
      }
      this.#topLevelModels?.add(runId);
      parent = turn.root;
    }
    if (!parent) return;
    if (model) {
      parent = this.#child('llm', parent);
      this.#models.set(runId, parent);
    }
    this.#parents.set(runId, parent);
    return parent;
  }

  #hostOutcome(): RunOutcome | undefined {
    try {
      return this.#turn?.getHostOutcome();
    } catch {
      // A host lookup failure cannot change the result of the native invocation.
      return undefined;
    }
  }

  #end(runId: string, outcome?: RunOutcome): void {
    this.#parents.delete(runId);
    const model = this.#models.get(runId);
    if (model) {
      if (outcome === 'completed') model.setStatus({ code: SpanStatusCode.OK });
      if (outcome === 'failed') model.setStatus({ code: SpanStatusCode.ERROR });
      model.end(hrTime());
      this.#models.delete(runId);
    }
    if (this.#topLevelModels?.delete(runId)) {
      if (outcome === 'failed' || (outcome === undefined && this.#topLevelOutcome !== 'failed')) {
        this.#topLevelOutcome = outcome;
      }
      if (this.#topLevelModels.size === 0 && !this.#rootResult) {
        this.#rootResult = {
          endedAt: performance.now(), outcome: this.signal?.aborted ? undefined : this.#topLevelOutcome,
        };
      }
    } else if (runId === this.#rootRunId && !this.#rootResult) {
      this.#rootResult = { endedAt: performance.now(), outcome: this.signal?.aborted ? undefined : outcome };
    }
  }

  finish(): void {
    const turn = this.#turn;
    if (!turn) return;
    const confirmed = this.#hostOutcome();
    const outcome = confirmed ?? this.#rootResult?.outcome;
    for (const [model, endedAt] of this.#ambiguousModels) {
      if (this.#rootResult?.outcome && confirmed !== 'cancelled') model.setStatus({ code: SpanStatusCode.ERROR });
      model.end(hrTime(endedAt));
    }
    this.#ambiguousModels.clear();
    for (const model of this.#models.values()) model.end(hrTime());
    this.#models.clear();
    for (const tool of this.#tools.values()) tool.end(hrTime());
    this.#tools.clear();
    for (const end of this.#openAgents) end();
    const ran = this.#rootRunId !== undefined || this.#explicitChild;
    this.#parents.clear();
    // A prepared configuration can be discarded before LangChain starts.
    if (!ran) turn.close();
    if (outcome) turn.root.setAttribute('dubhe.run.outcome', outcome);
    if (outcome === 'completed') turn.root.setStatus({ code: SpanStatusCode.OK });
    if (outcome === 'failed') turn.root.setStatus({ code: SpanStatusCode.ERROR });
    turn.root.end(hrTime(this.#rootResult?.endedAt));
    if (ran) turn.close();
    this.#turn = undefined;
  }
}
