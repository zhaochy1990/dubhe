export interface TraceScope {
  readonly tenantId: string;
  readonly environmentId: string;
  readonly projectId: string;
}

/** The application entry point verifies the caller, session, and Agent before tracing. */
export interface VerifiedTurn {
  readonly scope: TraceScope;
  readonly sessionId: string;
  readonly turnId: string;
  readonly rootAgentId: string;
  /** RFC 3339 timestamp issued by the trusted host. */
  readonly sessionStartedAt: string;
}

export interface VerifiedRun extends VerifiedTurn {
  readonly traceId: string;
  readonly rootSpanId: string;
  readonly rootProducerId: string;
  readonly registeredAt: string;
  readonly acceptUntil: string;
}

export type RunOutcome = 'completed' | 'failed' | 'cancelled';

export type PrepareRunResult =
  | { outcome: 'started'; binding: VerifiedRun }
  | { outcome: 'already_bound' };

export interface TrustedTracingHost {
  /** Atomically persist the candidate against its scope, session and turn before returning it. */
  prepareRun(candidate: VerifiedRun): Promise<PrepareRunResult>;
  /** Read the host-confirmed outcome, including cancellation, when available. */
  getRunOutcome?(binding: VerifiedRun): RunOutcome | undefined;
}

export interface RunRegistration {
  register(run: VerifiedRun, signal: AbortSignal): Promise<void>;
}

export class TraceAlreadyBoundError extends Error {
  readonly code = 'TRACE_ALREADY_BOUND';

  constructor() {
    super('A Trace is already bound to this turn');
    this.name = 'TraceAlreadyBoundError';
  }
}

export class TransientRegistrationError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'TransientRegistrationError';
  }
}

export interface RunRegistrationOptions {
  url: string;
  getHeaders(): Promise<Readonly<Record<string, string>>>;
}

export function httpUrl(value: string): string {
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) {
    throw new TypeError('Tracing endpoints must be HTTP(S) URLs without user information');
  }
  return url.href;
}

export function createHttpRunRegistration(options: RunRegistrationOptions): RunRegistration {
  const url = httpUrl(options.url);
  return {
    async register(run, signal) {
      const headers = new Headers(await options.getHeaders());
      headers.set('Content-Type', 'application/json');
      let response: Response;
      try {
        response = await fetch(url, {
          method: 'POST', body: JSON.stringify(run), headers, signal, redirect: 'manual',
        });
      } catch (cause) {
        throw new TransientRegistrationError('Tracing registration request failed', { cause });
      }
      await response.body?.cancel();
      if (response.status === 204) return;
      const message = `Tracing registration returned HTTP ${response.status}`;
      if ([429, 502, 503, 504].includes(response.status)) {
        throw new TransientRegistrationError(message);
      }
      throw new Error(message);
    },
  };
}
