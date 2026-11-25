import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { test } from 'node:test';
import { gunzipSync } from 'node:zlib';
import { createAgent } from 'langchain';
import { createConnection, type RowDataPacket } from 'mysql2/promise';
import protobuf from 'protobufjs';
import { createHttpRunRegistration, createTracingSdk, type VerifiedRun } from 'dubhe';
import { LangChainTracing } from 'dubhe/langchain';
import { answer, baseUrl, inputs, newTurn, readTrace, scope, TestModel } from './support.js';

// Decode only the official OTLP envelope fields needed to compare the wire IDs.
const requestType = protobuf.parse(`
  syntax = "proto3";
  message ExportTraceServiceRequest { repeated ResourceSpans resource_spans = 1; }
  message ResourceSpans { repeated ScopeSpans scope_spans = 2; }
  message ScopeSpans { repeated Span spans = 2; }
  message Span { bytes trace_id = 1; bytes span_id = 2; bytes parent_span_id = 4; }
`).root.lookupType('ExportTraceServiceRequest');

interface SpanIDs {
  traceId: string;
  spanId: string;
  parentSpanId: string;
}

type WireSpanIDs = Record<keyof SpanIDs, Uint8Array>;

function decodeSpanIDs(body: Buffer, encoding: string | undefined): SpanIDs[] {
  const bytes = encoding === 'gzip' ? gunzipSync(body) : body;
  const decoded = requestType.toObject(requestType.decode(bytes), { defaults: true });
  return decoded.resourceSpans.flatMap(
    (resource: { scopeSpans: Array<{ spans: WireSpanIDs[] }> }) =>
      resource.scopeSpans.flatMap(({ spans }) =>
        spans.map(({ traceId, spanId, parentSpanId }) => ({
          traceId: Buffer.from(traceId).toString('hex'),
          spanId: Buffer.from(spanId).toString('hex'),
          parentSpanId: Buffer.from(parentSpanId).toString('hex'),
        })),
      ),
  );
}

function sorted(spans: SpanIDs[]): SpanIDs[] {
  return spans.toSorted((a, b) => a.spanId.localeCompare(b.spanId));
}

test('the SDK resends committed Span IDs when the OTLP acknowledgement is lost', {
  skip: !baseUrl,
  timeout: 20_000,
}, async () => {
  const mysqlURL = process.env.DUBHE_TEST_MYSQL_URL;
  assert.ok(mysqlURL, 'DUBHE_TEST_MYSQL_URL is required to verify the committed rows');
  const database = await createConnection(mysqlURL);
  async function readRows(traceId: string) {
    const [rows] = await database.execute<RowDataPacket[]>(
      `SELECT LOWER(HEX(span_id)) AS spanId, LOWER(HEX(trace_id)) AS traceId,
              LOWER(HEX(parent_span_id)) AS parentSpanId
       FROM tracing_spans WHERE tenant_id = ? AND environment_id = ? AND project_id = ?
         AND trace_id = UNHEX(?) ORDER BY span_id`,
      [scope.tenantId, scope.environmentId, scope.projectId, traceId],
    );
    return rows.map(({ spanId, traceId, parentSpanId }) => ({
      spanId, traceId, parentSpanId: parentSpanId ?? '',
    }));
  }

  let binding: VerifiedRun | undefined;
  const attempts: SpanIDs[][] = [];
  const proxyErrors: unknown[] = [];
  let committedBeforeDisconnect: SpanIDs[] | undefined;
  const proxy = createServer(async (request, response) => {
    try {
      assert.equal(request.method, 'POST');
      assert.equal(request.url, '/v1/traces');
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = Buffer.concat(chunks);
      const encoding = request.headers['content-encoding'];
      const isFirstAttempt = attempts.length === 0;
      attempts.push(decodeSpanIDs(body, encoding));

      const received = await fetch(`${baseUrl}/v1/traces`, {
        method: 'POST',
        headers: {
          authorization: request.headers.authorization ?? '',
          'content-type': request.headers['content-type'] ?? '',
          ...(encoding ? { 'content-encoding': encoding } : {}),
        },
        body: new Uint8Array(body),
      });
      const acknowledgement = await received.arrayBuffer();
      assert.equal(received.status, 200);
      assert.equal(acknowledgement.byteLength, 0, 'Go should acknowledge the entire OTLP batch');
      if (isFirstAttempt) {
        assert.ok(binding);
        // Verify the committed rows from a separate connection before dropping the response.
        committedBeforeDisconnect = await readRows(binding.traceId);
        request.socket.destroy();
        return;
      }
      response.writeHead(received.status, { 'content-type': 'application/x-protobuf' });
      response.end(Buffer.from(acknowledgement));
    } catch (error) {
      proxyErrors.push(error);
      response.writeHead(500).end();
    }
  });

  try {
    await new Promise<void>((resolve, reject) => {
      proxy.once('error', reject);
      proxy.listen(0, '127.0.0.1', resolve);
    });
    const address = proxy.address();
    assert.ok(address && typeof address !== 'string');
    const sdk = createTracingSdk({
      scope,
      producerId: 'agent-worker',
      otlpTraceEndpoint: `http://127.0.0.1:${address.port}/v1/traces`,
      getOtlpHeaders: async () => ({ Authorization: 'Bearer workload-token' }),
      host: {
        async prepareRun(candidate) {
          binding = candidate;
          return { outcome: 'started', binding: candidate };
        },
      },
      registration: createHttpRunRegistration({
        url: `${baseUrl}/internal/v1/observability/runs`,
        getHeaders: async () => ({ Authorization: 'Bearer register-token' }),
      }),
      retryDelayMs: 20,
    });

    try {
      const agent = createAgent({ model: new TestModel(), tools: [] });
      const tracing = new LangChainTracing(sdk);
      const config = await tracing.prepareConfig(newTurn(), {});
      try {
        const result = await agent.invoke(inputs, config);
        assert.equal(result.messages.at(-1)?.content, answer);
      } finally {
        tracing.finish(config);
      }
      const flush = await sdk.flush({ timeoutMs: 5_000 });
      assert.deepEqual(proxyErrors, []);
      assert.equal(flush.outcome, 'drained');

      const [initial, retry, ...later] = attempts;
      assert.ok(initial?.length && retry?.length, 'expected a retry after the first response was lost');
      assert.deepEqual(sorted(retry), sorted(initial));
      assert.deepEqual(committedBeforeDisconnect, sorted(initial));

      assert.ok(binding);
      const { traceId, rootSpanId } = binding;
      const emitted = [...initial, ...later.flat()];
      assert.equal(emitted.length, 2);
      const root = emitted.find((span) => span.spanId === rootSpanId);
      const child = emitted.find((span) => span.spanId !== rootSpanId);
      assert.ok(root && child);
      assert.equal(root.traceId, traceId);
      assert.equal(root.parentSpanId, '');
      assert.equal(child.traceId, traceId);
      assert.equal(child.parentSpanId, root.spanId);
      const expected = sorted([root, child]);
      assert.deepEqual(await readRows(traceId), expected);

      const traceResponse = await readTrace(traceId);
      assert.equal(traceResponse.status, 200);
      const trace = await traceResponse.json();
      assert.equal(trace.trace_id, traceId);
      const queried = trace.nodes.map((node: { trace_id: string; span_id: string; parent_span_id: string | null }) => ({
        traceId: node.trace_id, spanId: node.span_id, parentSpanId: node.parent_span_id ?? '',
      }));
      assert.deepEqual(sorted(queried), expected);
    } finally {
      await sdk.shutdown({ timeoutMs: 5_000 });
    }
  } finally {
    if (proxy.listening) await new Promise<void>((resolve) => proxy.close(() => resolve()));
    await database.end();
  }
});
