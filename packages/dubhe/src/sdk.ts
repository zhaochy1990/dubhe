import { ROOT_CONTEXT, trace, type Span } from '@opentelemetry/api';
import { callWithTimeout, hrTime } from '@opentelemetry/core';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
import { AlwaysOnSampler, TracerProvider } from '@opentelemetry/sdk-trace';
import { SpanDelivery, type FlushResult } from './delivery.js';
import {
  httpUrl,
  TraceAlreadyBoundError,
  type RunOutcome,
  type RunRegistration,
  type TraceScope,
  type TrustedTracingHost,
  type VerifiedRun,
  type VerifiedTurn,
} from './domain.js';

export interface TracingOptions {
  scope: TraceScope;
  producerId: string;
  otlpTraceEndpoint: string;
  getOtlpHeaders(): Promise<Readonly<Record<string, string>>>;
  host: TrustedTracingHost;
  registration: RunRegistration;
  maxBufferedSpans?: number;
  retryDelayMs?: number;
  acceptWindowMs?: number;
  captureModelInputs?: boolean;
  gatewayOrigins?: readonly string[];
}

export interface PreparedTurn {
  root: Span;
  captureModelInputs: boolean;
  rootAgentId: string;
  allowsAgent(agentId: string): boolean;
  startChild(kind: 'llm' | 'agent' | 'tool_client', agentId: string, parent: Span): Span;
  gatewayBinding(agentId: string, destination: string): Promise<string | undefined>;
  getHostOutcome(): RunOutcome | undefined;
  close(): void;
}

export const prepareTurn = Symbol('prepareTurn');
const maxAcceptWindowMs = 48 * 60 * 60 * 1_000;
const maxTimerMs = 2_147_483_647;

export function validId(value: unknown): value is string {
  return typeof value === 'string' && !!value && !value.includes('\0') &&
    value.isWellFormed() && Buffer.byteLength(value, 'utf8') <= 128;
}

function validateId(value: string): void {
  if (!validId(value)) {
    throw new TypeError('Opaque IDs must be nonempty, null-free strings within 128 UTF-8 bytes');
  }
}

function sameScope(left: TraceScope, right: TraceScope): boolean {
  return left.tenantId === right.tenantId && left.environmentId === right.environmentId &&
    left.projectId === right.projectId;
}

function verifyBinding(binding: VerifiedRun, candidate: VerifiedRun): boolean {
  const expected = new Set(candidate.agentGrants?.map(({ producerId, agentId }) => JSON.stringify([producerId, agentId])));
  const actual = binding.agentGrants ?? [];
  return actual.length === expected.size && actual.every(({ producerId, agentId }) =>
    expected.delete(JSON.stringify([producerId, agentId]))) &&
    binding.sessionInitiator?.kind === candidate.sessionInitiator?.kind &&
    binding.sessionInitiator?.id === candidate.sessionInitiator?.id &&
    sameScope(binding.scope, candidate.scope) && binding.sessionId === candidate.sessionId &&
    binding.turnId === candidate.turnId && binding.rootAgentId === candidate.rootAgentId &&
    binding.sessionStartedAt === candidate.sessionStartedAt && binding.traceId === candidate.traceId &&
    binding.rootSpanId === candidate.rootSpanId && binding.rootProducerId === candidate.rootProducerId &&
    binding.registeredAt === candidate.registeredAt && binding.acceptUntil === candidate.acceptUntil;
}

function boundedInteger(value: number, label: string, minimum: number, maximum: number): void {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new RangeError(`${label} must be an integer from ${minimum} to ${maximum}`);
  }
}

export class TracingSdk {
  readonly #delivery: SpanDelivery;
  readonly #provider: TracerProvider;
  readonly #tracer;
  readonly #scope: TraceScope;
  readonly #producerId: string;
  readonly #host: TrustedTracingHost;
  readonly #acceptWindowMs: number;
  readonly #captureModelInputs: boolean;
  readonly #gatewayOrigins: ReadonlySet<string>;
  #shutdown?: Promise<FlushResult>;

  constructor(options: TracingOptions) {
    const { maxBufferedSpans = 2048, retryDelayMs = 100, acceptWindowMs = maxAcceptWindowMs } = options;
    boundedInteger(maxBufferedSpans, 'maxBufferedSpans', 1, maxTimerMs);
    boundedInteger(retryDelayMs, 'retryDelayMs', 1, 5_000);
    boundedInteger(acceptWindowMs, 'acceptWindowMs', 1, maxAcceptWindowMs);
    this.#scope = Object.freeze({
      tenantId: options.scope.tenantId,
      environmentId: options.scope.environmentId,
      projectId: options.scope.projectId,
    });
    this.#producerId = options.producerId;
    this.#host = options.host;
    this.#acceptWindowMs = acceptWindowMs;
    this.#captureModelInputs = options.captureModelInputs === true;
    this.#gatewayOrigins = new Set(options.gatewayOrigins?.map((value) => {
      const url = new URL(httpUrl(value));
      if (url.href !== url.origin + '/') throw new TypeError('Gateway allowlist entries must be origins');
      return url.origin;
    }));
    for (const id of Object.values(this.#scope)) validateId(id);
    validateId(this.#producerId);
    const exporter = new OTLPTraceExporter({
      url: httpUrl(options.otlpTraceEndpoint),
      headers: () => callWithTimeout(options.getOtlpHeaders(), 3_000),
      timeoutMillis: 3_000,
    });
    this.#delivery = new SpanDelivery(options.registration, exporter, maxBufferedSpans, retryDelayMs);
    this.#provider = new TracerProvider({ sampler: new AlwaysOnSampler(), spanProcessors: [this.#delivery] });
    this.#tracer = this.#provider.getTracer('dubhe');
  }

  async [prepareTurn](turn: VerifiedTurn): Promise<PreparedTurn> {
    if (this.#shutdown) throw new Error('Tracing SDK is shut down');
    if (!sameScope(turn.scope, this.#scope)) throw new TypeError('Verified turn is outside the SDK scope');
    for (const id of [turn.sessionId, turn.turnId, turn.rootAgentId]) validateId(id);
    const agentGrants = turn.agentGrants?.map(({ producerId, agentId }) => {
      validateId(producerId);
      validateId(agentId);
      return Object.freeze({ producerId, agentId });
    });
    if (agentGrants && new Set(agentGrants.map(({ producerId, agentId }) => JSON.stringify([producerId, agentId]))).size !== agentGrants.length) {
      throw new TypeError('Agent grants must be unique');
    }
    const initiator = turn.sessionInitiator;
    if (initiator) {
      validateId(initiator.id);
      if (initiator.kind !== 'user' && initiator.kind !== 'workload') {
        throw new TypeError('Invalid verified session initiator');
      }
    }
    const sessionStart = Date.parse(turn.sessionStartedAt);
    const registeredAt = Date.now();
    if (!Number.isFinite(sessionStart) || sessionStart > registeredAt) {
      throw new TypeError('Session start must be a valid time at or before registration');
    }
    const root = this.#startSpan('run', turn.rootAgentId);
    const { traceId, spanId: rootSpanId } = root.spanContext();
    const candidate = Object.freeze({
      scope: this.#scope,
      sessionId: turn.sessionId,
      turnId: turn.turnId,
      rootAgentId: turn.rootAgentId,
      ...(agentGrants?.length ? { agentGrants: Object.freeze(agentGrants) } : {}),
      ...(initiator ? { sessionInitiator: Object.freeze({ kind: initiator.kind, id: initiator.id }) } : {}),
      sessionStartedAt: turn.sessionStartedAt,
      traceId,
      rootSpanId,
      rootProducerId: this.#producerId,
      registeredAt: new Date(registeredAt).toISOString(),
      acceptUntil: new Date(registeredAt + this.#acceptWindowMs).toISOString(),
    });
    try {
      const prepared = await this.#host.prepareRun(candidate);
      if (prepared.outcome === 'already_bound') throw new TraceAlreadyBoundError();
      if (!verifyBinding(prepared.binding, candidate)) throw new Error('Host returned a different Trace binding');
      if (this.#shutdown) throw new Error('Tracing SDK is shut down');
    } catch (error) {
      root.end(hrTime());
      throw error;
    }
    this.#delivery.add(candidate);
    return {
      root,
      captureModelInputs: this.#captureModelInputs,
      rootAgentId: candidate.rootAgentId,
      allowsAgent: (agentId) => agentId === candidate.rootAgentId || candidate.agentGrants?.some(
        (grant) => grant.producerId === this.#producerId && grant.agentId === agentId,
      ) === true,
      startChild: (kind, agentId, parent) => this.#startSpan(kind, agentId, parent),
      gatewayBinding: async (agentId, destination) => {
        const origin = new URL(httpUrl(destination)).origin;
        if (!this.#gatewayOrigins.has(origin)) throw new TypeError('Gateway origin is outside the configured allowlist');
        return this.#host.getGatewayBinding?.(candidate, agentId, origin);
      },
      getHostOutcome: () => this.#host.getRunOutcome?.(candidate),
      close: () => this.#delivery.close(traceId),
    };
  }

  #startSpan(kind: 'run' | 'llm' | 'agent' | 'tool_client', agentId: string, parent?: Span): Span {
    return this.#tracer.startSpan(kind, {
      startTime: hrTime(),
      attributes: { 'dubhe.span.kind': kind, 'dubhe.agent.id': agentId },
    }, parent ? trace.setSpan(ROOT_CONTEXT, parent) : ROOT_CONTEXT);
  }

  flush({ timeoutMs }: { timeoutMs: number }): Promise<FlushResult> {
    boundedInteger(timeoutMs, 'timeoutMs', 0, maxTimerMs);
    return this.#delivery.flush(timeoutMs);
  }

  shutdown({ timeoutMs }: { timeoutMs: number }): Promise<FlushResult> {
    boundedInteger(timeoutMs, 'timeoutMs', 0, maxTimerMs);
    if (!this.#shutdown) {
      this.#delivery.stopAccepting();
      this.#shutdown = this.#delivery.flush(timeoutMs).then((result) => {
        const stopped = this.#provider.shutdown();
        if (result.outcome === 'drained') return stopped.then(() => result);
        // A transport attempt already in progress may finish after the caller's deadline.
        void stopped.catch(() => undefined);
        return result;
      });
    }
    return this.#shutdown;
  }
}

export function createTracingSdk(options: TracingOptions): TracingSdk {
  return new TracingSdk(options);
}
