import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { BaseCallbackHandler } from '@langchain/core/callbacks/base';
import { CallbackManager } from '@langchain/core/callbacks/manager';
import type { RunnableConfig } from '@langchain/core/runnables';
import { MemorySaver } from '@langchain/langgraph';
import { createAgent } from 'langchain';
import { answer, assertTrace, baseUrl, inputs, newTurn, readNode, setup, TestModel, type Node } from './support.js';

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
  const checkpointer = new MemorySaver();
  const agent = createAgent({ model: new TestModel(), tools: [], checkpointer });
  const callback = new ExistingCallback({ _awaitHandler: true });
  const manager = new CallbackManager();
  manager.addHandler(callback, true);
  const existingConfig: RunnableConfig = {
    callbacks: manager,
    configurable: { thread_id: turn.sessionId },
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

  const checkpoint = await checkpointer.getTuple({ configurable: { thread_id: turn.sessionId } });
  assert.equal(checkpoint?.config.configurable?.thread_id, turn.sessionId);
  assert.deepEqual(callback.invocations, ['kept']);
  assert.deepEqual(manager.handlers, [callback]);
  assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });

  const trace = await assertTrace(binding(turn), 'completed');
  const model = trace.nodes.find((node) => node.kind === 'llm');
  assert.ok(model);
  const detail = await readNode(model.node_id, trace.trace_id);
  assert.equal(detail.status, 200, await detail.clone().text());
  assert.equal((await detail.json() as Node).parent_span_id, trace.nodes.find((node) => node.kind === 'run')?.span_id);
});
