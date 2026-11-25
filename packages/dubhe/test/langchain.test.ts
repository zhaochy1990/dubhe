import { strict as assert } from 'node:assert';
import { addAbortListener, once } from 'node:events';
import { createServer } from 'node:http';
import { buffer } from 'node:stream/consumers';
import { test } from 'node:test';
import { AIMessageChunk, type BaseMessage } from '@langchain/core/messages';
import { ChatGenerationChunk } from '@langchain/core/outputs';
import { createAgent } from 'langchain';
import { assertTrace, baseUrl, inputs, newTurn, readTrace, setup, TestModel } from './support.js';

const live = { skip: !baseUrl, timeout: 20_000 };

function chunk(text: string): ChatGenerationChunk {
  return new ChatGenerationChunk({ text, message: new AIMessageChunk(text) });
}

function controlledModel(error?: Error) {
  let completed = false;
  const release = Promise.withResolvers<void>();
  const closed = Promise.withResolvers<void>();
  const model = new TestModel({
    async *stream({ signal }) {
      let listener: ReturnType<typeof addAbortListener> | undefined;
      try {
        yield chunk('Your balance ');
        signal?.throwIfAborted();
        const abort = Promise.withResolvers<never>();
        if (signal) listener = addAbortListener(signal, () => abort.reject(signal.reason));
        await Promise.race([release.promise, abort.promise]);
        if (error) throw error;
        yield chunk('is 42.');
        completed = true;
      } finally {
        listener?.[Symbol.dispose]();
        closed.resolve();
      }
    },
  });
  return {
    model,
    release: release.resolve,
    closed: closed.promise,
    isComplete: () => completed,
  };
}

async function firstText(iterator: AsyncIterator<[BaseMessage, unknown]>): Promise<string> {
  const first = await iterator.next();
  assert.equal(first.done, false);
  assert.equal(typeof first.value[0].content, 'string');
  return first.value[0].content as string;
}

for (const outcome of ['completed', 'failed'] as const) {
  test(`native stream emits before model completion and persists one ${outcome} root`, live, async (t) => {
    const providerError = new Error('Provider failed after the first chunk');
    const model = controlledModel(outcome === 'failed' ? providerError : undefined);
    const { sdk, tracing, binding } = setup();
    t.after(() => sdk.shutdown({ timeoutMs: 5_000 }));
    const turn = newTurn();
    const agent = createAgent({ model: model.model, tools: [] });
    const config = await tracing.prepareConfig(turn);
    try {
      const chunks = await agent.stream(inputs, { ...config, streamMode: 'messages' });
      const iterator = chunks[Symbol.asyncIterator]();
      assert.equal(await firstText(iterator), 'Your balance ');
      assert.equal(model.isComplete(), false);
      model.release();
      if (outcome === 'failed') {
        await assert.rejects(iterator.next(), (error) => error === providerError);
      } else {
        assert.equal(await firstText(iterator), 'is 42.');
        assert.equal((await iterator.next()).done, true);
        assert.equal(model.isComplete(), true);
      }
    } finally {
      model.release();
      tracing.finish(config);
      tracing.finish(config);
    }

    await model.closed;
    assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });
    await assertTrace(binding(turn), outcome);
  });
}

for (const confirmed of [true, false]) {
  test(`closing the native stream ${confirmed ? 'records confirmed cancellation' : 'leaves an unknown outcome'}`, live, async (t) => {
    const model = controlledModel();
    const { sdk, tracing, binding, outcomes } = setup();
    t.after(() => sdk.shutdown({ timeoutMs: 5_000 }));
    const turn = newTurn();
    const agent = createAgent({ model: model.model, tools: [] });
    const abort = new AbortController();
    const config = await tracing.prepareConfig(turn, { signal: abort.signal });
    try {
      const chunks = await agent.stream(inputs, { ...config, streamMode: 'messages' });
      for await (const [message] of chunks) {
        assert.equal(message.content, 'Your balance ');
        assert.equal(model.isComplete(), false);
        abort.abort();
        break;
      }
      await model.closed;
      if (confirmed) outcomes.set(binding(turn).traceId, 'cancelled');
    } finally {
      model.release();
      tracing.finish(config);
      tracing.finish(config);
    }

    assert.equal(model.isComplete(), false);
    assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });
    await assertTrace(binding(turn), confirmed ? 'cancelled' : null);
  });
}

test('native streaming completes during an OTLP outage and background retry delivers its spans', live, async (t) => {
  const rejected = Promise.withResolvers<void>();
  let available = false;
  const proxy = createServer(async (request, response) => {
    try {
      const body = new Uint8Array(await buffer(request));
      if (!available) {
        response.writeHead(503).end();
        rejected.resolve();
        return;
      }
      const headers = new Headers();
      for (const name of ['authorization', 'content-type', 'content-encoding']) {
        const value = request.headers[name];
        if (typeof value === 'string') headers.set(name, value);
      }
      const upstream = await fetch(`${baseUrl}/v1/traces`, { method: 'POST', headers, body });
      response.writeHead(upstream.status, { 'Content-Type': 'application/x-protobuf' });
      response.end(Buffer.from(await upstream.arrayBuffer()));
    } catch {
      response.writeHead(502).end();
    }
  });
  proxy.listen(0, '127.0.0.1');
  await once(proxy, 'listening');
  const address = proxy.address();
  assert.ok(address && typeof address !== 'string');

  const model = controlledModel();
  const { sdk, tracing, binding } = setup(`http://127.0.0.1:${address.port}/v1/traces`);
  t.after(async () => {
    await sdk.shutdown({ timeoutMs: 5_000 });
    await new Promise<void>((resolve, reject) => proxy.close((error) => error ? reject(error) : resolve()));
  });
  const turn = newTurn();
  const agent = createAgent({ model: model.model, tools: [] });
  const config = await tracing.prepareConfig(turn);
  try {
    const stream = await agent.stream(inputs, { ...config, streamMode: 'messages' });
    const iterator = stream[Symbol.asyncIterator]();
    assert.equal(await firstText(iterator), 'Your balance ');
    assert.equal(model.isComplete(), false);
    model.release();
    assert.equal(await firstText(iterator), 'is 42.');
    assert.equal((await iterator.next()).done, true);
  } finally {
    model.release();
    tracing.finish(config);
  }

  await rejected.promise;
  // Keep the outage longer than a single OpenTelemetry export attempt.
  assert.deepEqual(await sdk.flush({ timeoutMs: 3_500 }), { outcome: 'timed_out' });
  const stillUnavailable = await readTrace(binding(turn).traceId);
  assert.equal(stillUnavailable.status, 200, await stillUnavailable.clone().text());
  assert.deepEqual((await stillUnavailable.json()).nodes, []);
  available = true;
  assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });
  await assertTrace(binding(turn), 'completed');
});
