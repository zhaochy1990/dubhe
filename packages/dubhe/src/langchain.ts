import { BaseCallbackHandler } from '@langchain/core/callbacks/base';
import type { RunnableConfig } from '@langchain/core/runnables';
import { SpanStatusCode, type Span } from '@opentelemetry/api';
import type { RunOutcome, VerifiedTurn } from './domain.js';
import { prepareTurn, type PreparedTurn, type TracingSdk } from './sdk.js';

type Callbacks = NonNullable<RunnableConfig['callbacks']>;
type PreparedConfig<Config> = Omit<Config, 'callbacks'> & { callbacks: Callbacks };

function isDefaultAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError' && error.message === 'This operation was aborted';
}

export class LangChainTracing {
  readonly #handlers = new WeakMap<Callbacks, TurnCallback>();

  constructor(private readonly sdk: TracingSdk) {}

  async prepareConfig<Config extends RunnableConfig = RunnableConfig>(
    turn: VerifiedTurn,
    existingConfig?: Config,
  ): Promise<PreparedConfig<Config>> {
    const existing = existingConfig?.callbacks;
    const callbacks = !existing || Array.isArray(existing) ? [...(existing ?? [])] : existing.copy();
    const preparedTurn = await this.sdk[prepareTurn](turn);
    const handler = new TurnCallback(preparedTurn, existingConfig?.signal);
    if (Array.isArray(callbacks)) callbacks.push(handler);
    else callbacks.addHandler(handler, true);
    this.#handlers.set(callbacks, handler);
    return { ...existingConfig, callbacks } as PreparedConfig<Config>;
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
  readonly #ambiguousModels = new Map<Span, number>();
  #turn?: PreparedTurn;
  #rootRunId?: string;
  #rootResult?: { endedAt: number; outcome?: RunOutcome };
  #nestedChainAborted = false;

  constructor(turn: PreparedTurn, private readonly signal?: AbortSignal) {
    super({ _awaitHandler: true });
    this.#turn = turn;
  }

  handleChainStart(...args: Parameters<NonNullable<BaseCallbackHandler['handleChainStart']>>): void {
    // The callback manager currently supplies the parent in slot four. Accept
    // the declared slot as well when it names a run this handler has observed.
    const parent = [args[3], args[7]].find((id) => id !== undefined && this.#parents.has(id));
    this.#start(args[2], parent);
  }

  handleChatModelStart(...args: Parameters<NonNullable<BaseCallbackHandler['handleChatModelStart']>>): void {
    this.#start(args[2], args[3], true);
  }

  handleLLMStart(...args: Parameters<NonNullable<BaseCallbackHandler['handleLLMStart']>>): void {
    this.#start(args[2], args[3], true);
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

  handleLLMEnd(_output: unknown, runId: string): void {
    this.#end(runId, 'completed');
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

  #start(runId: string, parentRunId?: string, model = false): void {
    const turn = this.#turn;
    if (!turn || this.#rootResult || this.#parents.has(runId)) return;
    let parent: Span | undefined;
    if (this.#rootRunId === undefined) {
      this.#rootRunId = runId;
      parent = turn.root;
    } else if (parentRunId) {
      parent = this.#parents.get(parentRunId);
    }
    if (!parent) return;
    if (model) {
      parent = turn.startModelSpan(parent);
      this.#models.set(runId, parent);
    }
    this.#parents.set(runId, parent);
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
      model.end();
      this.#models.delete(runId);
    }
    if (runId === this.#rootRunId && !this.#rootResult) {
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
      model.end(endedAt);
    }
    this.#ambiguousModels.clear();
    for (const model of this.#models.values()) model.end();
    this.#models.clear();
    this.#parents.clear();
    // A prepared configuration can be discarded before LangChain starts.
    if (this.#rootRunId === undefined) turn.close();
    if (outcome) turn.root.setAttribute('dubhe.run.outcome', outcome);
    if (outcome === 'completed') turn.root.setStatus({ code: SpanStatusCode.OK });
    if (outcome === 'failed') turn.root.setStatus({ code: SpanStatusCode.ERROR });
    turn.root.end(this.#rootResult?.endedAt);
    if (this.#rootRunId !== undefined) turn.close();
    this.#turn = undefined;
  }
}
