import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { test } from 'node:test';
import { createHttpRunRegistration } from '@lipaotech/dubhe';
import { createAgent } from 'langchain';
import { chromium, type Response } from 'playwright';
import { answer, inputs, newTurn, readTraceTopology, setup, TestModel, type Trace } from './support.js';

const env = spawnSync('docker', ['compose', '-f', 'compose.yaml', 'config', '--environment'], {
  cwd: new URL('../../../', import.meta.url),
  encoding: 'utf8',
});
if (env.status !== 0) throw new Error('Could not resolve the Compose environment', { cause: env.error });
const composeEnv = new Map<string, string>();
for (const line of env.stdout.split('\n')) {
  const separator = line.indexOf('=');
  if (separator > 0) composeEnv.set(line.slice(0, separator), line.slice(separator + 1));
}

const tracingUrl = `http://127.0.0.1:${composeEnv.get('DUBHE_TRACING_PORT') || '4318'}`;
const uiUrl = `http://127.0.0.1:${composeEnv.get('DUBHE_UI_PORT') || '3000'}`;
const workloadKey = composeEnv.get('DUBHE_WORKLOAD_API_KEY') || 'local-workload-key';
const registrationKey = composeEnv.get('DUBHE_REGISTRATION_API_KEY') || 'local-registration-key';
const readerKey = composeEnv.get('DUBHE_UI_READER_KEY') || 'local-reader-key';

test('a host Agent reports a Trace that the Compose browser ingress displays', { timeout: 45_000 }, async (t) => {
  const registrationPath = '/internal/v1/observability/runs';
  const ingestPath = '/v1/traces';
  const sessionsPath = '/v1/observability/sessions';

  assert.equal((await fetch(tracingUrl + registrationPath, {
    method: 'POST', headers: { 'X-API-Key': workloadKey },
  })).status, 401);
  assert.equal((await fetch(tracingUrl + ingestPath, {
    method: 'POST', headers: { 'X-API-Key': registrationKey },
  })).status, 401);
  assert.equal((await fetch(tracingUrl + sessionsPath, { headers: { 'X-API-Key': workloadKey } })).status, 404);
  for (const path of [registrationPath, ingestPath]) {
    assert.equal((await fetch(uiUrl + path, { method: 'POST' })).status, 404);
  }
  assert.equal((await fetch(uiUrl + sessionsPath, {
    headers: { Authorization: 'Bearer client-value', 'X-API-Key': registrationKey },
  })).status, 200);

  const { sdk, tracing, binding } = setup(tracingUrl + ingestPath, {
    getOtlpHeaders: async () => ({ 'X-API-Key': workloadKey }),
    registration: createHttpRunRegistration({
      url: tracingUrl + registrationPath,
      getHeaders: async () => ({ 'X-API-Key': registrationKey }),
    }),
  });
  t.after(() => sdk.shutdown({ timeoutMs: 5_000 }));
  const agent = createAgent({ model: new TestModel(), tools: [] });
  const turn = newTurn();
  const config = await tracing.prepareConfig(turn);
  try {
    const result = await agent.invoke(inputs, config);
    assert.equal(result.messages.at(-1)?.content, answer);
  } finally {
    tracing.finish(config);
  }
  assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });
  const run = binding(turn);

  const browser = await chromium.launch({ executablePath: process.env.DUBHE_TEST_CHROMIUM_PATH });
  t.after(() => browser.close());
  const page = await browser.newPage();
  page.setDefaultTimeout(8_000);
  const responses: Response[] = [];
  page.on('response', (response) => {
    if (new URL(response.url()).origin === uiUrl && response.status() === 200) {
      responses.push(response);
    }
  });

  assert.equal((await page.goto(uiUrl + '/tracing/'))?.status(), 200);
  await page.getByRole('img', { name: /会话数量分布/ }).waitFor();
  await page.getByRole('searchbox').fill(turn.sessionId);
  const [traceResponse] = await Promise.all([
    page.waitForResponse((response) =>
      response.status() === 200 && new URL(response.url()).pathname === `/v1/observability/traces/${run.traceId}`),
    page.getByRole('button', { name: turn.sessionId, exact: true }).click(),
  ]);
  const trace: Trace = await traceResponse.json();
  assert.equal(trace.session_id, turn.sessionId);
  assert.equal(trace.trace_id, run.traceId);
  const root = trace.nodes.find((node) => node.kind === 'run');
  const model = trace.nodes.find((node) => node.kind === 'llm');
  assert.ok(root && model);
  assert.equal(root.span_id, run.rootSpanId);
  assert.equal(root.parent_span_id, null);
  assert.equal(model.parent_span_id, root.span_id);

  await page.getByRole('button', { name: '调用树' }).click();
  assert.deepEqual(await readTraceTopology(page), [[root.node_id, null], [model.node_id, root.span_id]]);

  assert.ok(responses.some((response) => new URL(response.url()).pathname === '/tracing/tracing.js'));
  for (const response of responses) {
    const headers = await response.request().allHeaders();
    assert.equal(headers['x-api-key'], undefined);
    assert.equal(headers.authorization, undefined);
    assert.equal((await response.text()).includes(readerKey), false, `unexpected reader key in ${response.url()}`);
  }
  console.log(`Session ${turn.sessionId}, Trace ${run.traceId}`);
});
