import { metrics } from '@opentelemetry/api';
import type { ReadableSpan, SpanExporter, SpanProcessor } from '@opentelemetry/sdk-trace';
import { setTimeout as delay } from 'node:timers/promises';
import { TransientRegistrationError, type RunRegistration, type VerifiedRun } from './domain.js';

export interface FlushResult {
  outcome: 'drained' | 'timed_out';
}

type RegistrationState = 'pending' | 'registered' | 'rejected';
interface DeliveryTurn {
  readonly deadline: number;
  state: RegistrationState;
}
interface BufferedSpan {
  readonly span: ReadableSpan;
  readonly turn: DeliveryTurn;
}

const meter = metrics.getMeter('dubhe');
const droppedSpans = meter.createCounter('dubhe.tracing.spans.dropped');
const registrationErrors = meter.createCounter('dubhe.tracing.registration.failures');
const exportErrors = meter.createCounter('dubhe.tracing.export.failures');
const maximumRetryDelayMs = 5_000;
const registrationAttemptMs = 3_000;
const maxBatchSize = 512;
const retryableNetworkErrors = new Set([
  'ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'ENETUNREACH', 'EHOSTUNREACH',
  'ENETDOWN', 'EAI_AGAIN', 'ENOTFOUND',
]);

function isRetryableExport(error?: Error): boolean {
  const code = (error as (Error & { code?: number | string }) | undefined)?.code;
  if (typeof code === 'number') return [429, 502, 503, 504].includes(code);
  if (typeof code === 'string') return retryableNetworkErrors.has(code);
  return error?.message === 'Request timed out' || error?.message === 'Export failed with retryable status';
}

/** Spans share one bounded buffer from end through registration, delivery and retry. */
export class SpanDelivery implements SpanProcessor {
  readonly #active = new Map<string, DeliveryTurn>();
  readonly #stop = new AbortController();
  readonly #waiters = new Set<() => void>();
  #registrationCount = 0;
  #buffer: BufferedSpan[] = [];
  #sending = false;
  #accepting = true;
  #timer?: NodeJS.Timeout;
  #retryBatch?: BufferedSpan[];
  #retryAt = 0;
  #nextRetryMs: number;

  constructor(
    private readonly registration: RunRegistration,
    private readonly exporter: SpanExporter,
    private readonly limit: number,
    private readonly retryDelayMs: number,
  ) {
    this.#nextRetryMs = retryDelayMs;
  }

  add(run: VerifiedRun): void {
    const turn: DeliveryTurn = { deadline: Date.parse(run.acceptUntil), state: 'pending' };
    this.#active.set(run.traceId, turn);
    this.#registrationCount++;
    // Registration is independent of callback execution and always uses the original binding.
    void this.#register(run, turn).finally(() => {
      this.#registrationCount--;
      this.#schedule();
      this.#notifyDrained();
    });
  }

  close(traceId: string): void {
    this.#active.delete(traceId);
  }

  onStart(): void {}

  onEnd(span: ReadableSpan): void {
    const turn = this.#active.get(span.spanContext().traceId);
    if (!turn) return;
    if (!this.#accepting || turn.state === 'rejected' || Date.now() >= turn.deadline) {
      droppedSpans.add(1, { reason: this.#accepting ? 'registration_or_expiry' : 'shutdown' });
    } else if (this.#buffer.length >= this.limit) {
      droppedSpans.add(1, { reason: 'buffer_full' });
    } else {
      this.#buffer.push({ span, turn });
      this.#schedule();
    }
  }

  async #register(run: VerifiedRun, turn: DeliveryTurn): Promise<void> {
    let backoff = this.retryDelayMs;
    const signal = this.#stop.signal;
    while (!signal.aborted && Date.now() < turn.deadline) {
      const attemptSignal = AbortSignal.any([
        signal, AbortSignal.timeout(Math.min(registrationAttemptMs, turn.deadline - Date.now())),
      ]);
      try {
        await this.registration.register(run, attemptSignal);
        if (!signal.aborted && Date.now() < turn.deadline) {
          turn.state = 'registered';
          return;
        }
        break;
      } catch (error) {
        if (signal.aborted) break;
        if (!(error instanceof TransientRegistrationError) && !attemptSignal.aborted) {
          registrationErrors.add(1, { reason: 'rejected' });
          break;
        }
        registrationErrors.add(1, { reason: 'retryable' });
      }
      await delay(Math.min(backoff, Math.max(0, turn.deadline - Date.now())), undefined, {
        ref: false, signal,
      }).catch(() => undefined);
      backoff = Math.min(backoff * 2, maximumRetryDelayMs);
    }
    turn.state = 'rejected';
  }

  #schedule(): void {
    if (this.#sending || this.#stop.signal.aborted) return;
    this.#prune();
    const ready = this.#retryBatch ?? this.#buffer.filter((span) => span.turn.state === 'registered');
    if (!ready.length) return;
    const now = Date.now();
    const expiry = Math.min(...ready.map((span) => span.turn.deadline));
    const next = this.#retryBatch ? Math.min(this.#retryAt, expiry) : now + 10;
    clearTimeout(this.#timer);
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      void this.#send();
    }, Math.max(0, next - now));
    this.#timer.unref();
  }

  async #send(): Promise<void> {
    this.#prune();
    if (this.#retryBatch && Date.now() < this.#retryAt) {
      this.#schedule();
      return;
    }
    const batch = this.#retryBatch ??
      this.#buffer.filter((span) => span.turn.state === 'registered').slice(0, maxBatchSize);
    if (!batch.length) return;
    this.#sending = true;
    let retry = false;
    try {
      const result = await new Promise<Parameters<Parameters<SpanExporter['export']>[1]>[0]>((resolve) => {
        this.exporter.export(batch.map((entry) => entry.span), resolve);
      });
      if (result.code !== 0) {
        retry = isRetryableExport(result.error);
        exportErrors.add(1, { reason: retry ? 'retryable' : 'rejected' });
      }
    } catch {
      exportErrors.add(1, { reason: 'rejected' });
    } finally {
      if (retry && !this.#stop.signal.aborted) {
        this.#retryBatch = batch;
        this.#retryAt = Date.now() + this.#nextRetryMs;
        this.#nextRetryMs = Math.min(this.#nextRetryMs * 2, maximumRetryDelayMs);
      } else {
        const delivered = new Set(batch);
        this.#buffer = this.#buffer.filter((entry) => !delivered.has(entry));
        this.#resetRetry();
      }
      this.#sending = false;
      this.#schedule();
      this.#notifyDrained();
    }
  }

  #resetRetry(): void {
    this.#retryBatch = undefined;
    this.#retryAt = 0;
    this.#nextRetryMs = this.retryDelayMs;
  }

  #prune(): void {
    const now = Date.now();
    const keep = ({ turn }: BufferedSpan) => turn.state !== 'rejected' && now < turn.deadline;
    const size = this.#buffer.length;
    this.#buffer = this.#buffer.filter(keep);
    if (this.#buffer.length < size) {
      droppedSpans.add(size - this.#buffer.length, { reason: 'registration_or_expiry' });
    }
    if (this.#retryBatch) {
      this.#retryBatch = this.#retryBatch.filter(keep);
      if (!this.#retryBatch.length) this.#resetRetry();
    }
    this.#notifyDrained();
  }

  #notifyDrained(): void {
    if (this.#registrationCount || this.#buffer.length || this.#sending) return;
    for (const notify of this.#waiters) notify();
  }

  flush(timeoutMs: number): Promise<FlushResult> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => complete('timed_out'), timeoutMs);
      const complete = (outcome: FlushResult['outcome']) => {
        clearTimeout(timer);
        this.#waiters.delete(drained);
        resolve({ outcome });
      };
      const drained = () => complete('drained');
      this.#waiters.add(drained);
      this.#schedule();
      this.#notifyDrained();
    });
  }

  async forceFlush(): Promise<void> {
    if ((await this.flush(30_000)).outcome !== 'drained') throw new Error('Tracing flush timed out');
  }

  stopAccepting(): void {
    this.#accepting = false;
  }

  async shutdown(): Promise<void> {
    this.stopAccepting();
    this.#stop.abort();
    clearTimeout(this.#timer);
    if (this.#buffer.length) droppedSpans.add(this.#buffer.length, { reason: 'shutdown' });
    this.#buffer = [];
    this.#resetRetry();
    this.#active.clear();
    await this.exporter.shutdown();
  }
}
