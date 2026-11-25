import { strict as assert } from 'node:assert';
import { randomUUID } from 'node:crypto';
import { test } from 'node:test';
import { BaseCallbackHandler } from '@langchain/core/callbacks/base';
import { CallbackManager } from '@langchain/core/callbacks/manager';
import type { RunnableConfig } from '@langchain/core/runnables';
import { MemorySaver } from '@langchain/langgraph';
import { createAgent } from 'langchain';
import { answer, assertTrace, baseUrl, inputs, newTurn, readNode, readTrace, scope, setup, TestModel } from './support.js';

const live = { skip: !baseUrl, timeout: 20_000 };

class ExistingCallback extends BaseCallbackHandler {
  name = 'existing-application-callback';
  invocations: unknown[] = [];

  handleChatModelStart(...args: Parameters<NonNullable<BaseCallbackHandler['handleChatModelStart']>>) {
    this.invocations.push(args[6]?.integration_marker);
  }
}

test('native invoke preserves callbacks and checkpoints and builds an authorized Trace tree', live, async (t) => {
  const { sdk, tracing, binding } = setup();
  t.after(() => sdk.shutdown({ timeoutMs: 5_000 }));
  const turn = newTurn();
  const checkpointThreadId = `checkpoint-${randomUUID()}`;
  const checkpointer = new MemorySaver();
  const agent = createAgent({ model: new TestModel(), tools: [], checkpointer });
  const callback = new ExistingCallback({ _awaitHandler: true });
  const manager = new CallbackManager();
  manager.addHandler(callback, true);
  const existingConfig: RunnableConfig = {
    callbacks: manager,
    configurable: { thread_id: checkpointThreadId },
    metadata: { integration_marker: 'kept' },
  };

  const config = await tracing.prepareConfig(turn, existingConfig);
  try {
    const result = await agent.invoke(inputs, config);
    assert.equal(result.messages.at(-1)?.content, answer);
  } finally {
    tracing.finish(config);
    tracing.finish(config);
  }

  const checkpoint = await checkpointer.getTuple({ configurable: { thread_id: checkpointThreadId } });
  const messages = checkpoint?.checkpoint.channel_values.messages;
  assert.ok(Array.isArray(messages));
  assert.deepEqual(messages.map((message) => message.content), [inputs.messages[0].content, answer]);
  assert.deepEqual(callback.invocations, ['kept']);
  assert.deepEqual(manager.handlers, [callback]);
  assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });

  const run = binding(turn);
  const trace = await assertTrace(run, 'completed');
  for (const node of trace.nodes) {
    const response = await readNode(node.node_id, trace.trace_id);
    assert.equal(response.status, 200, await response.clone().text());
    assert.deepEqual(await response.json(), {
      ...node,
      trace_id: run.traceId,
      tenant_id: scope.tenantId,
      environment_id: scope.environmentId,
      project_id: scope.projectId,
    });
  }

  async function assertUnavailable(token: string, requestedScope = scope) {
    const responses = await Promise.all([
      readTrace(run.traceId, token, requestedScope),
      ...trace.nodes.map((node) => readNode(node.node_id, run.traceId, token, requestedScope)),
    ]);
    for (const response of responses) {
      assert.equal(response.status, 404, await response.clone().text());
      assert.deepEqual(await response.json(), { detail: 'Not available' });
    }
  }

  const otherScopes = {
    tenant: { ...scope, tenantId: 'tenant-beta' },
    environment: { ...scope, environmentId: 'Staging' },
    project: { ...scope, projectId: 'billing' },
  };
  for (const [dimension, otherScope] of Object.entries(otherScopes)) {
    await t.test(`denies Trace and Span reads across ${dimension}s`, async () => {
      const otherReader = `other-${dimension}-reader`;
      await assertUnavailable('reader-token', otherScope);
      await assertUnavailable(otherReader);
      await assertUnavailable(otherReader, otherScope);
    });
  }
  await t.test('denies Trace and Span reads without a grant for the Agent', async () => {
    await assertUnavailable('other-agent-reader');
  });
});
