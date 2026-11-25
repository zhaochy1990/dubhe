import { strict as assert } from 'node:assert';
import { randomUUID } from 'node:crypto';
import type { CallbackManagerForLLMRun } from '@langchain/core/callbacks/manager';
import { BaseChatModel } from '@langchain/core/language_models/chat_models';
import { AIMessage, type BaseMessage } from '@langchain/core/messages';
import type { ChatGenerationChunk, ChatResult } from '@langchain/core/outputs';
import {
  createHttpRunRegistration,
  createTracingSdk,
  type RunOutcome,
  type TraceScope,
  type TracingOptions,
  type VerifiedRun,
  type VerifiedTurn,
} from 'dubhe';
import { LangChainTracing } from 'dubhe/langchain';

export const scope = {
  tenantId: 'tenant-alpha',
  environmentId: 'staging',
  projectId: 'support',
};

export const baseUrl = process.env.DUBHE_TEST_BASE_URL;
export const answer = 'Your balance is 42.';
export const inputs = { messages: [{ role: 'user', content: 'What is my balance?' }] };

export function newTurn(): VerifiedTurn {
  return {
    scope,
    sessionId: `session-${randomUUID()}`,
    turnId: 'turn-1',
    rootAgentId: 'support-agent',
    sessionStartedAt: new Date().toISOString(),
  };
}

function read(path: string, token: string, requestedScope: TraceScope, expectedTraceId?: string): Promise<Response> {
  const query = new URLSearchParams({
    tenant_id: requestedScope.tenantId,
    environment_id: requestedScope.environmentId,
    project_id: requestedScope.projectId,
  });
  if (expectedTraceId) query.set('expected_trace_id', expectedTraceId);
  return fetch(`${baseUrl}/v1/observability/${path}?${query}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
}

export function readTrace(traceId: string, token = 'reader-token', requestedScope = scope): Promise<Response> {
  return read(`traces/${traceId}`, token, requestedScope);
}

export function readNode(
  nodeId: string,
  expectedTraceId: string,
  token = 'reader-token',
  requestedScope = scope,
): Promise<Response> {
  return read(`nodes/${nodeId}`, token, requestedScope, expectedTraceId);
}

interface ModelOptions {
  invokeError?: Error;
  stream?: (options: { signal?: AbortSignal }) => AsyncGenerator<ChatGenerationChunk>;
}

export class TestModel extends BaseChatModel {
  constructor(private readonly fixture: ModelOptions = {}) {
    super({ maxRetries: 0 });
  }

  bindTools(): this {
    return this;
  }

  _llmType(): string {
    return 'dubhe-test-model';
  }

  async _generate(): Promise<ChatResult> {
    if (this.fixture.invokeError) throw this.fixture.invokeError;
    return { generations: [{ text: answer, message: new AIMessage(answer) }] };
  }

  async *_streamResponseChunks(
    messages: BaseMessage[],
    options: this['ParsedCallOptions'],
    runManager?: CallbackManagerForLLMRun,
  ): AsyncGenerator<ChatGenerationChunk> {
    if (!this.fixture.stream) {
      yield* super._streamResponseChunks(messages, options, runManager);
      return;
    }
    for await (const chunk of this.fixture.stream(options)) {
      await runManager?.handleLLMNewToken(chunk.text, undefined, undefined, undefined, undefined, { chunk });
      yield chunk;
    }
  }
}

export function setup(
  otlpUrl = `${baseUrl}/v1/traces`,
  delivery: Partial<Pick<TracingOptions, 'registration' | 'maxBufferedSpans'>> = {},
) {
  const bindings = new Map<string, VerifiedRun>();
  const outcomes = new Map<string, RunOutcome>();
  const sdk = createTracingSdk({
    scope,
    producerId: 'agent-worker',
    otlpTraceEndpoint: otlpUrl,
    getOtlpHeaders: async () => ({ Authorization: 'Bearer workload-token' }),
    host: {
      async prepareRun(candidate) {
        const key = JSON.stringify([candidate.sessionId, candidate.turnId]);
        if (bindings.has(key)) return { outcome: 'already_bound' };
        bindings.set(key, candidate);
        return { outcome: 'started', binding: candidate };
      },
      getRunOutcome: (binding) => outcomes.get(binding.traceId),
    },
    registration: delivery.registration ?? createHttpRunRegistration({
      url: `${baseUrl}/internal/v1/observability/runs`,
      getHeaders: async () => ({ Authorization: 'Bearer register-token' }),
    }),
    maxBufferedSpans: delivery.maxBufferedSpans,
  });
  return {
    sdk,
    tracing: new LangChainTracing(sdk),
    outcomes,
    binding(turn: VerifiedTurn) {
      const binding = bindings.get(JSON.stringify([turn.sessionId, turn.turnId]));
      assert.ok(binding);
      return binding;
    },
  };
}

export interface Node {
  node_id: string;
  span_id: string;
  parent_span_id: string | null;
  agent_id: string;
  kind: string;
  status: number;
}

interface Trace {
  trace_id: string;
  session_id: string;
  turn_id: string;
  root_agent_id: string;
  run_state: RunOutcome | null;
  end_ns: number | null;
  nodes: Node[];
}

export async function assertTrace(binding: VerifiedRun, outcome: RunOutcome | null): Promise<Trace> {
  const response = await readTrace(binding.traceId);
  assert.equal(response.status, 200, await response.clone().text());
  const trace: Trace = await response.json();
  assert.equal(trace.trace_id, binding.traceId);
  assert.equal(trace.session_id, binding.sessionId);
  assert.equal(trace.turn_id, binding.turnId);
  assert.equal(trace.root_agent_id, binding.rootAgentId);
  assert.equal(trace.run_state, outcome);
  assert.ok(trace.end_ns);
  assert.deepEqual(trace.nodes.map((node) => node.kind).sort(), ['llm', 'run']);
  const root = trace.nodes.find((node) => node.kind === 'run');
  const model = trace.nodes.find((node) => node.kind === 'llm');
  assert.equal(root?.span_id, binding.rootSpanId);
  assert.equal(root?.parent_span_id, null);
  assert.equal(model?.parent_span_id, binding.rootSpanId);
  assert.equal(model?.agent_id, binding.rootAgentId);
  return trace;
}
