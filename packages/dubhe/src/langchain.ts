import { BaseCallbackHandler } from '@langchain/core/callbacks/base';
import type { RunnableConfig } from '@langchain/core/runnables';
import { SpanStatusCode, type Span } from '@opentelemetry/api';
import type { RunOutcome, VerifiedTurn } from './domain.js';
import { prepareTurn, type PreparedTurn, type TracingSdk } from './sdk.js';

type Callbacks = NonNullable<RunnableConfig['callbacks']>;
type PreparedConfig<Config> = Omit<Config, 'callbacks'> & { callbacks: Callbacks };

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
  #turn?: PreparedTurn;
  #rootRunId?: string;
  #rootEnded = false;

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
    this.#end(runId, this.#isCancellation(error) || error == null ? undefined : 'failed');
  }

  handleLLMEnd(_output: unknown, runId: string): void {
    this.#end(runId, 'completed');
  }

  handleLLMError(error: unknown, runId: string): void {
    this.#end(runId, this.#isCancellation(error) ? undefined : 'failed');
  }

  #start(runId: string, parentRunId?: string, model = false): void {
    const turn = this.#turn;
    if (!turn || this.#rootEnded || this.#parents.has(runId)) return;
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

  #isCancellation(error: unknown): boolean {
    return this.signal?.aborted === true ||
      (typeof error === 'object' && error !== null && 'name' in error && error.name === 'AbortError');
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
    if (runId === this.#rootRunId) {
      const confirmed = this.#hostOutcome() ?? (this.signal?.aborted ? undefined : outcome);
      // An ambiguous abort may be confirmed by the host before finish().
      if (confirmed) this.#endRoot(confirmed);
    }
  }

  #endRoot(outcome?: RunOutcome): void {
    const root = this.#turn?.root;
    if (!root || this.#rootEnded) return;
    this.#rootEnded = true;
    if (outcome) root.setAttribute('dubhe.run.outcome', outcome);
    if (outcome === 'completed') root.setStatus({ code: SpanStatusCode.OK });
    if (outcome === 'failed') root.setStatus({ code: SpanStatusCode.ERROR });
    root.end();
  }

  finish(): void {
    const turn = this.#turn;
    if (!turn) return;
    for (const model of this.#models.values()) model.end();
    this.#models.clear();
    this.#parents.clear();
    // A prepared configuration can be discarded before LangChain starts.
    if (this.#rootRunId === undefined) turn.close();
    this.#endRoot(this.#rootEnded ? undefined : this.#hostOutcome());
    if (this.#rootRunId !== undefined) turn.close();
    this.#turn = undefined;
  }
}
