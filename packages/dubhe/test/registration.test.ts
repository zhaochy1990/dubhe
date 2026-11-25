import assert from 'node:assert/strict';
import { addAbortListener } from 'node:events';
import { test } from 'node:test';
import { createHttpRunRegistration } from '@lipaotech/dubhe';
import { createAgent } from 'langchain';
import { answer, assertTrace, baseUrl, inputs, newTurn, readTrace, setup, TestModel } from './support.js';

test('discarding an unstarted configuration cancels its registration and lets flush drain', async (t) => {
  const started = Promise.withResolvers<AbortSignal>();
  const { sdk, tracing } = setup('http://127.0.0.1:1/v1/traces', {
    registration: {
      async register(_run, signal) {
        assert.ok(signal);
        started.resolve(signal);
        const aborted = Promise.withResolvers<never>();
        using listener = addAbortListener(signal, () => aborted.reject(signal.reason));
        await aborted.promise;
      },
    },
  });
  t.after(() => sdk.shutdown({ timeoutMs: 0 }));

  const config = await tracing.prepareConfig(newTurn());
  const signal = await started.promise;
  tracing.finish(config);

  assert.deepEqual(await sdk.flush({ timeoutMs: 100 }), { outcome: 'drained' });
  assert.ok(signal.aborted);
});

test('a registration outage bounds work and recovery exports only buffered turns', {
  skip: !baseUrl,
  timeout: 20_000,
}, async (t) => {
  const release = Promise.withResolvers<void>();
  const realRegistration = createHttpRunRegistration({
    url: `${baseUrl}/internal/v1/observability/runs`,
    getHeaders: async () => ({ Authorization: 'Bearer register-token' }),
  });
  const attempted = new Map<string, AbortSignal>();
  const { sdk, tracing, binding } = setup(undefined, {
    maxBufferedSpans: 2,
    registration: {
      async register(run, signal) {
        attempted.set(run.traceId, signal);
        const aborted = Promise.withResolvers<never>();
        using listener = addAbortListener(signal, () => aborted.reject(signal.reason));
        await Promise.race([release.promise, aborted.promise]);
        await realRegistration.register(run, signal);
      },
    },
  });
  t.after(async () => {
    release.resolve();
    await sdk.shutdown({ timeoutMs: 5_000 });
  });

  const agent = createAgent({ model: new TestModel(), tools: [] });
  const turns = Array.from({ length: 5 }, () => newTurn());
  const configs = await Promise.all(turns.map((turn) => tracing.prepareConfig(turn)));
  assert.ok(attempted.size <= 2, 'pending registrations must fit the configured capacity');
  for (const config of configs) {
    try {
      const result = await agent.invoke(inputs, config);
      assert.equal(result.messages.at(-1)?.content, answer);
    } finally {
      tracing.finish(config);
    }
  }

  assert.deepEqual(await sdk.flush({ timeoutMs: 0 }), { outcome: 'timed_out' });
  const [buffered, ...discarded] = turns.map(binding);
  assert.equal(attempted.get(buffered.traceId)?.aborted, false);
  for (const turn of discarded) assert.notEqual(attempted.get(turn.traceId)?.aborted, false);
  assert.equal((await readTrace(buffered.traceId)).status, 404);

  release.resolve();
  assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });
  await assertTrace(buffered, 'completed');
  for (const turn of discarded) assert.equal((await readTrace(turn.traceId)).status, 404);
});
