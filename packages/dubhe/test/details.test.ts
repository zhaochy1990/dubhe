import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { CallbackManager } from '@langchain/core/callbacks/manager';
import { AIMessage, AIMessageChunk, HumanMessage, SystemMessage, ToolMessage } from '@langchain/core/messages';
import { ChatGenerationChunk } from '@langchain/core/outputs';
import { RunnableLambda } from '@langchain/core/runnables';
import { tool } from '@langchain/core/tools';
import { LangChainTracing } from '@lipaotech/dubhe/langchain';
import { answer, baseUrl, newTurn, readNode, readTrace, scope, setup, TestModel } from './support.js';

const live = { skip: !baseUrl, timeout: 20_000 };
const usage = { input_tokens: 46, output_tokens: 7, total_tokens: 53 };
const messages = [
  new SystemMessage('Use the account tools.'),
  new HumanMessage('Find my order.'),
  new AIMessage({ content: '', tool_calls: [{ name: 'lookup', args: { id: 'order-7' }, id: 'lookup-1' }] }),
  new ToolMessage({ tool_call_id: 'lookup-1', content: 'Order shipped.' }),
  new HumanMessage('When will it arrive?'),
];
const invocationParams = {
  model: 'provider-fixture', temperature: 0, max_tokens: 128,
  reasoning_effort: 'low', instructions: 'Return a concise reply.',
  tools: [{ type: 'function', function: { name: 'lookup', parameters: { type: 'object' } } }],
};

async function json(response: Response) {
  assert.equal(response.status, 200, await response.clone().text());
  return response.json();
}

test('native invoke and stream persist actual request settings, ordered history and provider usage', live, async (t) => {
  const { sdk, tracing, binding } = setup(undefined, { captureModelInputs: true });
  t.after(() => sdk.shutdown({ timeoutMs: 5_000 }));
  const model = new TestModel({
    invocationParams,
    tokens: usage,
    async *stream() {
      yield new ChatGenerationChunk({ text: 'first', message: new AIMessageChunk('first') });
      yield new ChatGenerationChunk({
        text: 'last', message: new AIMessageChunk({ content: 'last', usage_metadata: usage }),
      });
    },
  });

  const turn = newTurn();
  const nextTurn = { ...turn, turnId: 'turn-2' };
  for (const [call, currentTurn] of [['invoke', turn], ['stream', nextTurn]] as const) {
    const config = await tracing.prepareConfig(currentTurn, { callbacks: new CallbackManager('external-parent') });
    try {
      if (call === 'invoke') {
        assert.equal((await model.invoke(messages, config)).content, answer);
      } else {
        const stream = await model.stream(messages, config);
        const contents: unknown[] = [];
        for await (const chunk of stream) contents.push(chunk.content);
        assert.deepEqual(contents, ['first', 'last']);
      }
    } finally {
      tracing.finish(config);
    }
  }
  assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });
  const summary = {
    model_id: 'fixture-v1', model_name: 'provider-fixture', temperature: 0, max_output_tokens: 128,
    input_tokens: 46, output_tokens: 7, reasoning_effort: 'low',
  };
  for (const currentTurn of [turn, nextTurn]) {
    const traceId = binding(currentTurn).traceId;
    const trace = await json(await readTrace(traceId, 'content-reader'));
    const node = trace.nodes.find((node) => node.kind === 'llm');
    assert.deepEqual(node.metadata, summary);
    assert.equal(trace.agent_name, 'Customer Support');
    for (const reader of ['reader-token', 'tools-reader']) {
      const result = await json(await readNode(node.node_id, traceId, reader));
      assert.deepEqual(result.metadata, summary);
    }
    const details = await json(await readNode(node.node_id, traceId, 'model-reader'));
    assert.deepEqual(details.metadata.system_instructions, [{ type: 'text', content: invocationParams.instructions }]);
    assert.deepEqual(details.metadata.tool_definitions, [{ type: 'function', name: 'lookup', parameters: { type: 'object' } }]);
    assert.deepEqual(details.metadata.messages.map((message) => message.role), ['system', 'user', 'assistant', 'tool', 'user']);
    assert.deepEqual(details.metadata.messages[2].parts.at(-1), {
      type: 'tool_call', id: 'lookup-1', name: 'lookup', arguments: { id: 'order-7' },
    });
    assert.deepEqual(details.metadata.messages[3].parts, [
      { type: 'tool_call_response', id: 'lookup-1', response: 'Order shipped.' },
    ]);
    assert.deepEqual(details.metadata.messages[4].parts, [{ type: 'text', content: 'When will it arrive?' }]);
    assert.ok(!JSON.stringify(details).includes(answer));
  }

  const sessionsPath = 'sessions';
  const filter = { q: turn.sessionId, agent_id: 'support-agent', tz: 'Asia/Shanghai' };
  const session = await json(await read(sessionsPath, 'reader-token', filter));
  assert.equal(session.total, 1);
  assert.equal(session.distribution.reduce((total, bucket) => total + bucket.count, 0), 1);
  assert.equal(session.sessions[0].session_id, turn.sessionId);
  assert.equal(session.sessions[0].agent_name, 'Customer Support');
  const tracesPath = `sessions/${turn.sessionId}/traces`;
  const firstPage = await json(await read(tracesPath, 'reader-token', { limit: '1' }));
  assert.deepEqual(firstPage.traces.map((trace) => trace.trace_id), [binding(nextTurn).traceId]);
  assert.ok(firstPage.next_cursor);
  const secondPage = await json(await read(tracesPath, 'reader-token', { limit: '1', cursor: firstPage.next_cursor }));
  assert.deepEqual(secondPage.traces.map((trace) => trace.trace_id), [binding(turn).traceId]);
  assert.equal(secondPage.next_cursor, undefined);
  for (const [dimension, value] of [
    ['tenant', { tenant_id: 'tenant-beta' }],
    ['environment', { environment_id: 'Staging' }],
    ['project', { project_id: 'billing' }],
    ['agent', {}],
  ] as const) {
    const token = `other-${dimension}-reader`;
    const hidden = await json(await read(sessionsPath, token, { ...filter, ...value }));
    assert.equal(hidden.total, 0);
    assert.deepEqual(hidden.sessions, []);
    assert.deepEqual(hidden.agents, []);
    assert.ok(hidden.distribution.every(({ count }) => count === 0));
    assert.equal((await read(tracesPath, token, value)).status, 404);
    assert.equal((await read(tracesPath, token)).status, 404);
  }
});

test('a child Agent stream reports its actual completion status and preserves model parentage', live, async (t) => {
  const { sdk, tracing, binding } = setup();
  t.after(() => sdk.shutdown({ timeoutMs: 5_000 }));
  const turn = {
    ...newTurn(),
    agentGrants: [{ producerId: 'agent-worker', agentId: 'orders-agent' }],
  };
  const failure = new Error('provider failed after the first chunk');
  const model = new TestModel({
    async *stream() {
      yield new ChatGenerationChunk({ text: 'first', message: new AIMessageChunk('first') });
      throw failure;
    },
  });
  const config = await tracing.prepareConfig(turn);
  try {
    const stream = await tracing.withAgent(config, 'orders-agent', (child) => model.stream(messages, child));
    const iterator = stream[Symbol.asyncIterator]();
    assert.equal((await iterator.next()).value.content, 'first');
    await assert.rejects(iterator.next(), (error) => error === failure);
  } finally {
    tracing.finish(config);
  }
  assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });
  const trace = await json(await readTrace(binding(turn).traceId, 'content-reader'));
  const child = trace.nodes.find((node) => node.kind === 'agent');
  const modelNode = trace.nodes.find((node) => node.kind === 'llm');
  assert.equal(child.status, 2);
  assert.equal(modelNode.status, 2);
  assert.equal(modelNode.parent_span_id, child.span_id);
  assert.ok(child.end_ns >= modelNode.end_ns);
});

async function read(path: string, token: string, query: Record<string, string> = {}) {
  const params = new URLSearchParams({
    tenant_id: scope.tenantId, environment_id: scope.environmentId, project_id: scope.projectId, ...query,
  });
  return fetch(`${baseUrl}/v1/observability/${path}?${params}`, { headers: { 'X-API-Key': token } });
}

const audit = (event: unknown, token = 'audit-token') => fetch(`${baseUrl}/internal/v1/observability/gateway-events`, {
  method: 'POST', body: JSON.stringify(event), headers: { 'Content-Type': 'application/json', 'X-API-Key': token },
});

test('Gateway-only observations support scoped Session and Trace details', live, async () => {
  const traceId = randomUUID().replaceAll('-', '');
  const callId = `gateway-only-${traceId}`;
  const now = new Date().toISOString();
  const event = {
    event_id: `event-${traceId}`, trace_id: traceId, call_id: callId, sequence: 1, type: 'decision',
    agent_id: 'support-agent', tool_id: 'orders.lookup', occurred_at: now,
    decision: 'allow', execution_state: 'not_executed', policy_version: 'policy-3',
    initiator: { kind: 'workload', id: 'gateway-caller' },
    gateway_binding: { registered_at: now },
  };
  const first = await audit(event);
  assert.equal(first.status, 204, await first.text());
  assert.equal((await audit(event)).status, 204);
  const trace = await json(await readTrace(traceId));
  assert.equal(trace.observed_range, true);
  assert.equal(trace.run_state, null);
  assert.equal(trace.nodes.length, 1);
  assert.equal(trace.nodes[0].source, 'gateway');
  assert.equal(trace.governance.result, 'not_executed');
  const results = await json(await read('sessions', 'reader-token', { q: trace.session_id, result: 'not_executed' }));
  assert.equal(results.total, 1);
  assert.equal(results.sessions[0].observed_range, true);
  assert.equal(results.sessions[0].tool_calls, 1);
  const link = await json(await read(`tool-calls/${callId}/trace`, 'reader-token'));
  assert.equal(link.trace_id, traceId);
  assert.equal(link.node_id, trace.nodes[0].node_id);
  assert.equal((await read(`tool-calls/${callId}/trace`, 'other-agent-reader')).status, 404);
});

test('verified child Agents and Gateway audit preserve topology, authority and replay identity', live, async (t) => {
  const callId = `call-${randomUUID()}`;
  let recordedEvent: Record<string, unknown> | undefined;
  let downstream = 0;
  const gateway = createServer(async (request, response) => {
    const binding = JSON.parse(String(request.headers['x-dubhe-trace-binding']));
    const [, traceId, spanId] = String(request.headers.traceparent).split('-');
    assert.equal(traceId, binding.traceId);
    assert.equal(binding.agentId, 'orders-agent');
    const decision = request.url === '/blocked' ? 'block' : 'allow';
    recordedEvent = {
      event_id: `event-${callId}`, call_id: callId, trace_id: traceId, span_id: spanId,
      agent_id: binding.agentId, tool_id: 'orders.update', sequence: 1, type: 'decision',
      decision, execution_state: 'not_executed', occurred_at: new Date().toISOString(),
      policy_version: 'policy-17', initiator: { kind: 'user', id: 'verified-42' },
      parameters: { order: 'order-7' },
    };
    if (decision === 'allow') downstream++;
    response.writeHead(decision === 'block' ? 403 : 200, { 'x-dubhe-call-id': callId }).end();
  });
  await new Promise<void>((resolve) => gateway.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>((resolve, reject) => gateway.close((err) => err ? reject(err) : resolve())));
  const address = gateway.address();
  assert.ok(address && typeof address === 'object');
  const origin = `http://127.0.0.1:${address.port}`;
  const { sdk, binding } = setup(undefined, { gatewayOrigins: [origin], captureModelInputs: true });
  const tracing = new LangChainTracing(sdk, { localTools: { native_echo: 'local.echo' } });
  t.after(() => sdk.shutdown({ timeoutMs: 5_000 }));
  const turn = {
    ...newTurn(),
    agentGrants: [{ producerId: 'agent-worker', agentId: 'orders-agent' }],
    sessionInitiator: { kind: 'workload', id: 'trusted-entry' } as const,
  };
  const echo = tool(async () => 'echo', { name: 'native_echo', description: 'Echo a value' });
  const root = RunnableLambda.from(async (_input: string, config) => {
    assert.ok(config);
    await tracing.withAgent(config, 'forged-agent', async (untraced) => {
      assert.equal((await new TestModel().invoke(messages, untraced)).content, answer);
    });
    return tracing.withAgent(config, 'orders-agent', async (child) => {
      await new TestModel().invoke(messages, child);
      assert.equal(await echo.invoke('text', child), 'echo');
      await tracing.withTool(child, { toolId: 'orders.lookup', route: 'gateway' }, async (tool) => {
        tool.recordResponse({ statusCode: 503, callId: 'sdk-attempt' });
      });
      return tracing.withTool(child, { toolId: 'orders.update', route: 'gateway' }, async (tool) => {
        await assert.rejects(tool.gatewayHeaders('https://example.com/tool'), /outside the configured allowlist/);
        const response = await fetch(`${origin}/blocked`, {
          method: 'POST', redirect: 'error', headers: await tool.gatewayHeaders(`${origin}/blocked`),
        });
        tool.recordResponse({ statusCode: response.status, callId: response.headers.get('x-dubhe-call-id') ?? undefined });
        assert.equal(response.status, 403);
        // A separate Outbox delivery can run before Tracing's async registration.
        const deadline = Date.now() + 5_000;
        while (true) {
          const delivered = await audit(recordedEvent);
          if (delivered.status === 204) break;
          const message = await delivered.text();
          assert.equal(delivered.status, 503, message);
          assert.ok(Date.now() < deadline, message);
          await delay(25);
        }
        return response.status;
      });
    });
  });
  const config = await tracing.prepareConfig(turn);
  try {
    assert.equal(await root.invoke('run', config), 403);
  } finally {
    tracing.finish(config);
  }
  assert.equal(downstream, 0);
  assert.ok(recordedEvent);
  assert.equal((await audit(recordedEvent, 'workload-token')).status, 401);
  assert.equal((await audit({ ...recordedEvent, parameters: undefined })).status, 204);
  assert.equal((await audit({ ...recordedEvent, policy_version: 'different' })).status, 409);
  assert.equal((await audit({
    ...recordedEvent, event_id: `forward-${callId}`, sequence: 2, type: 'forward_intent',
    decision: 'allow', execution_state: 'executing', parameters: undefined,
  })).status, 409);
  assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });

  const traceId = binding(turn).traceId;
  const trace = await json(await readTrace(traceId, 'content-reader'));
  assert.deepEqual(trace.nodes.map((node) => node.kind).sort(), ['agent', 'llm', 'run', 'tool', 'tool_client', 'tool_client']);
  const agent = trace.nodes.find((node) => node.kind === 'agent');
  const toolNode = trace.nodes.find((node) => node.kind === 'tool');
  const model = trace.nodes.find((node) => node.kind === 'llm');
  assert.equal(agent.agent_name, 'Orders');
  assert.equal(agent.parent_span_id, binding(turn).rootSpanId);
  assert.equal(model.parent_span_id, agent.span_id);
  assert.equal(toolNode.parent_span_id, agent.span_id);
  assert.equal(toolNode.span_id, recordedEvent.span_id);
  assert.equal(toolNode.source, 'gateway');
  assert.equal(toolNode.metadata.execution_state, 'not_executed');
  assert.equal(toolNode.metadata.decision, 'block');
  assert.equal(toolNode.metadata.client.response_code, 403);
  assert.equal(toolNode.metadata.policy_version, 'policy-17');
  assert.deepEqual(toolNode.metadata.initiator, { kind: 'user', id: 'verified-42' });
  const sdkOnly = trace.nodes.find((node) => node.metadata.client_call_id === 'sdk-attempt');
  assert.equal(sdkOnly.source, 'sdk');
  assert.equal(sdkOnly.metadata.decision, undefined);
  assert.equal(sdkOnly.metadata.response_code, 503);
  assert.equal(sdkOnly.parent_span_id, agent.span_id);
  const local = trace.nodes.find((node) => node.metadata.tool_route === 'local');
  assert.equal(local.metadata.tool_id, 'local.echo');
  assert.equal(local.parent_span_id, agent.span_id);
  assert.deepEqual(model.metadata, {});

  const detail = await json(await readNode(toolNode.node_id, traceId, 'tools-reader'));
  assert.deepEqual(detail.metadata.parameters, { order: 'order-7' });
  assert.equal(detail.audit.length, 1);
  assert.equal(detail.audit[0].parameters, undefined);
  const noParams = await json(await readNode(toolNode.node_id, traceId, 'model-reader'));
  assert.equal(noParams.metadata.parameters, undefined);
  assert.equal((await readNode(toolNode.node_id, traceId)).status, 404);
  const restricted = await json(await readTrace(traceId));
  assert.deepEqual(restricted.nodes.map((node) => node.kind), ['run']);
  const base = await json(await read('sessions', 'content-reader', { q: turn.sessionId, result: 'blocked' }));
  assert.equal(base.sessions[0].tool_calls, 1);
  assert.equal(base.sessions[0].result, 'blocked');
  assert.deepEqual(base.sessions[0].initiator, turn.sessionInitiator);
  const summaryOnly = await json(await read('sessions', 'reader-token', { q: turn.sessionId }));
  assert.equal(summaryOnly.sessions[0].tool_calls, 0);
  assert.equal(summaryOnly.sessions[0].result, 'completed');
  // A late Outbox event with a different call ID remains a separate Gateway observation.
  const standalone = {
    ...recordedEvent, event_id: `standalone-${callId}`, call_id: `other-${callId}`,
    span_id: sdkOnly.span_id, tool_id: 'orders.lookup', decision: 'allow', parameters: {},
  };
  assert.equal((await audit(standalone)).status, 204);
  const latest = await json(await readTrace(traceId, 'content-reader'));
  const gatewayOnly = latest.nodes.find((node) => node.metadata.call_id === standalone.call_id);
  assert.equal(gatewayOnly.source, 'gateway');
  assert.equal(gatewayOnly.span_id, undefined);
  assert.equal(gatewayOnly.parent_span_id, null);
  assert.ok(latest.nodes.some((node) => node.node_id === sdkOnly.node_id));
  const intent = {
    ...standalone, event_id: `intent-${callId}`, sequence: 2, type: 'forward_intent',
    execution_state: 'executing', parameters: undefined,
  };
  const executed = {
    ...intent, event_id: `result-${callId}`, sequence: 3, type: 'execution_result',
    execution_state: 'succeeded',
  };
  assert.equal((await audit(executed)).status, 503);
  assert.equal((await audit(intent)).status, 204);
  assert.equal((await audit(executed)).status, 204);
  assert.equal((await audit(executed)).status, 204);
  const executedDetail = await json(await readNode(gatewayOnly.node_id, traceId, 'tools-reader'));
  assert.equal(executedDetail.metadata.execution_state, 'succeeded');
  assert.deepEqual(executedDetail.metadata.parameters, {});
  assert.deepEqual(executedDetail.audit.map((entry) => entry.type), ['decision', 'forward_intent', 'execution_result']);

});
