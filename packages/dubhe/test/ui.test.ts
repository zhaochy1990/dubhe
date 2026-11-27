import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { test, type TestContext } from 'node:test';
import { HumanMessage } from '@langchain/core/messages';
import type { VerifiedTurn } from '@lipaotech/dubhe';
import { chromium, type Page } from 'playwright';
import { answer, baseUrl, newTurn, readNode, readTrace, readTraceTopology, setup, TestModel, type Trace } from './support.js';

const live = { skip: !baseUrl || process.env.DUBHE_TEST_BROWSER !== '1', timeout: 45_000 };
const orderID = '9223372036854775807';

async function json(response: Response) {
  assert.equal(response.status, 200, await response.clone().text());
  return response.json();
}

async function testIngress(t: TestContext) {
  let reader: string | null = 'content-reader';
  let unavailableOnce = false;
  const browserCredentials: string[] = [];
  const server = createServer(async (request, response) => {
    const url = new URL(request.url!, baseUrl);
    const query = url.pathname.startsWith('/v1/observability/');
    if (request.method !== 'GET' || url.origin !== baseUrl || (!query && !url.pathname.startsWith('/tracing/'))) {
      response.writeHead(404).end();
      return;
    }
    if (query) {
      browserCredentials.push(
        ...[request.headers['x-api-key'], request.headers.authorization].filter((value): value is string => typeof value === 'string'),
      );
      if (unavailableOnce) {
        unavailableOnce = false;
        response.writeHead(503).end();
        return;
      }
      if (reader === null) {
        response.writeHead(403).end();
        return;
      }
    }
    try {
      const upstream = await fetch(url, {
        redirect: 'manual',
        headers: query && reader ? { 'X-API-Key': reader } : undefined,
      });
      const body = Buffer.from(await upstream.arrayBuffer());
      const headers = Object.fromEntries(upstream.headers);
      delete headers['content-encoding'];
      delete headers['transfer-encoding'];
      response.writeHead(upstream.status, headers).end(body);
    } catch {
      response.writeHead(502).end();
    }
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>((resolve) => server.close(() => resolve())));
  const address = server.address();
  assert.ok(address && typeof address === 'object');
  return {
    url: `http://127.0.0.1:${address.port}/tracing/`,
    browserCredentials,
    as: (identity: string | null) => { reader = identity; },
    failNextRead: () => { unavailableOnce = true; },
  };
}

async function fixtures(t: TestContext) {
  const { sdk, tracing, binding } = setup(undefined, { captureModelInputs: true });
  t.after(() => sdk.shutdown({ timeoutMs: 5_000 }));
  const turn = { ...newTurn(), sessionInitiator: { kind: 'user', id: 'ui-user' } as const };
  const nextTurn = { ...turn, turnId: 'turn-2' };
  const oldTurn = {
    ...newTurn(), sessionId: `${randomUUID()}-ΟΣ`, rootAgentId: 'operations-agent',
    sessionStartedAt: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(),
  };
  const emptyTurn = { ...newTurn(), sessionId: `empty /?# ${randomUUID()}` };
  const callId = `ui-call-${randomUUID()}`;
  const prompt = 'Look up my order. <img src=x onerror=alert(1)>';
  const model = new TestModel({
    invocationParams: { model: 'provider-fixture', temperature: 0, max_tokens: 128 },
    tokens: { input_tokens: 12, output_tokens: 5, total_tokens: 17 },
  });
  async function complete(currentTurn: VerifiedTurn, withTool = false) {
    const config = await tracing.prepareConfig(currentTurn);
    try {
      assert.equal((await model.invoke([new HumanMessage(prompt)], config)).content, answer);
      if (withTool) {
        await tracing.withTool(config, { toolId: 'orders.lookup', route: 'gateway' }, async (call) => {
          call.recordResponse({ statusCode: 403, callId });
        });
      }
    } finally {
      tracing.finish(config);
    }
  }

  await complete(turn, true);
  await complete(nextTurn);
  await complete(oldTurn);
  const emptyConfig = await tracing.prepareConfig(emptyTurn);
  t.after(() => tracing.finish(emptyConfig));
  assert.deepEqual(await sdk.flush({ timeoutMs: 5_000 }), { outcome: 'drained' });

  const traceId = binding(turn).traceId;
  const secondTraceId = binding(nextTurn).traceId;
  const beforeAudit: Trace = await json(await readTrace(traceId, 'content-reader'));
  const toolSpan = beforeAudit.nodes.find((node) => node.kind === 'tool_client');
  assert.ok(toolSpan);
  const audit = await fetch(`${baseUrl}/internal/v1/observability/gateway-events`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': 'audit-token' },
    body: JSON.stringify({
      event_id: `decision-${callId}`, call_id: callId, trace_id: traceId, span_id: toolSpan.span_id,
      agent_id: 'support-agent', tool_id: 'orders.lookup', sequence: 1, type: 'decision',
      decision: 'block', execution_state: 'not_executed', occurred_at: new Date().toISOString(),
      policy_version: 'ui-policy-1', initiator: turn.sessionInitiator,
      parameters: { id: 'ui-order', order_id: orderID },
    }).replace(`"${orderID}"`, orderID),
  });
  assert.equal(audit.status, 204, await audit.text());
  const trace: Trace = await json(await readTrace(traceId, 'content-reader'));
  return { turn, trace, secondTraceId, oldTurn, emptyTurn, prompt };
}

async function search(page: Page, value: string, total: number) {
  const loaded = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return url.pathname.endsWith('/sessions') && (url.searchParams.get('q') ?? '') === value;
  });
  await page.getByRole('searchbox').fill(value);
  const response = await loaded;
  assert.equal(response.status(), 200);
  const result = await response.json();
  assert.equal(result.total, total);
  assert.equal(result.distribution.reduce((sum: number, { count }: { count: number }) => sum + count, 0), total);
  await page.getByRole('img', { name: new RegExp(`会话数量分布，共 ${total} 个会话`) }).waitFor();
  return result;
}

test('deployed Tracing UI reads the SDK Session and Span topology through the authorized Go API', live, async (t) => {
  const fixture = await fixtures(t);
  const { turn, trace, secondTraceId, oldTurn, emptyTurn, prompt } = fixture;
  const ingress = await testIngress(t);
  const browser = await chromium.launch({ executablePath: process.env.DUBHE_TEST_CHROMIUM_PATH });
  t.after(() => browser.close());
  const page = await browser.newPage({ viewport: { width: 1280, height: 850 } });
  page.setDefaultTimeout(5_000);
  const errors: Error[] = [];
  page.on('pageerror', (error) => errors.push(error));

  const listGate = Promise.withResolvers<void>();
  await page.route('**/v1/observability/sessions?*', async (route) => {
    await listGate.promise;
    await route.continue();
  }, { times: 1 });
  try {
    assert.equal((await page.goto(ingress.url))?.status(), 200);
    await page.getByRole('status').getByText('正在加载会话…').waitFor();
  } finally {
    listGate.resolve();
  }
  const agent = page.getByRole('combobox', { name: 'Agent', exact: true });
  assert.equal(await agent.inputValue(), '');
  const range = page.getByRole('combobox', { name: '时间范围' });
  await search(page, oldTurn.sessionId, 1);
  await agent.selectOption('support-agent');
  await page.getByText('没有匹配的会话。').waitFor();
  await agent.selectOption('');
  await page.getByRole('button', { name: oldTurn.sessionId, exact: true }).waitFor();
  await range.selectOption('1');
  await page.getByText('没有匹配的会话。').waitFor();
  await page.getByRole('img', { name: '会话数量分布，共 0 个会话' }).waitFor();
  await agent.selectOption('support-agent');
  const sessions = await search(page, turn.sessionId, 1);
  assert.equal(sessions.sessions[0].session_id, trace.session_id);
  assert.equal(sessions.sessions[0].tool_calls, 1);
  assert.deepEqual(await page.getByRole('columnheader').allTextContents(), [
    '开始时间', 'Session ID', 'Agent', '发起人', '工具调用', '耗时',
  ]);
  const sessionButton = page.getByRole('button', { name: trace.session_id, exact: true });
  const row = sessionButton.locator('xpath=../..');
  assert.deepEqual((await row.getByRole('cell').allTextContents()).slice(1, 5), [
    trace.session_id, 'Customer Support', '用户 · ui-user', '1 次调用',
  ]);
  await sessionButton.click();

  const turns = page.getByRole('combobox', { name: 'Trace 轮次' });
  await page.locator('#trace-identity').filter({ hasText: secondTraceId }).waitFor();
  assert.deepEqual(await turns.locator('option').evaluateAll((options) => options.map((option) => (option as HTMLOptionElement).value)),
    [secondTraceId, trace.trace_id]);
  await turns.selectOption(trace.trace_id);
  await page.locator('#trace-canvas .waterfall-track').first().waitFor();
  assert.deepEqual(await readTraceTopology(page), trace.nodes.map((node) => [node.node_id, node.parent_span_id]));

  const inspector = page.getByRole('complementary', { name: '调用详情' });
  const model = trace.nodes.find((node) => node.kind === 'llm');
  assert.ok(model);
  const details = await json(await readNode(model.node_id, trace.trace_id, 'content-reader'));
  const displayedMessages = await inspector.locator('pre[aria-label="输入消息（按请求顺序）"]').textContent();
  assert.deepEqual(JSON.parse(displayedMessages!), details.metadata.messages);
  assert.equal(details.metadata.messages[0].parts[0].content, prompt);
  assert.equal(await inspector.locator('img').count(), 0);
  assert.equal(await inspector.getByText('模型输出', { exact: false }).count(), 0);
  assert.equal(await inspector.getByText(answer, { exact: false }).count(), 0);
  assert.equal(await inspector.locator('dt').filter({ hasText: /^Temperature$/ }).locator('xpath=following-sibling::dd').textContent(), '0');
  assert.equal(await inspector.locator('dt').filter({ hasText: /^输入 Token$/ }).locator('xpath=following-sibling::dd').textContent(),
    String(details.metadata.input_tokens));

  await page.getByRole('button', { name: '调用树' }).click();
  assert.equal(await page.getByRole('button', { name: '调用树' }).getAttribute('aria-pressed'), 'true');
  assert.equal(await page.locator('#trace-canvas .waterfall-track').count(), 0);
  const tree = await readTraceTopology(page);
  assert.equal(tree[0][0], trace.nodes.find((node) => node.kind === 'run')?.node_id);
  assert.deepEqual([...tree].sort(), trace.nodes.map((node) => [node.node_id, node.parent_span_id]).sort());
  const tool = trace.nodes.find((node) => node.kind === 'tool');
  assert.ok(tool);
  const toolResponse = await readNode(tool.node_id, trace.trace_id, 'content-reader');
  assert.ok((await toolResponse.clone().text()).includes(orderID));
  const toolDetail = await json(toolResponse);
  await page.getByRole('button', { name: /orders\.lookup/ }).click();
  await inspector.getByRole('heading', { name: '客户端观测' }).waitFor();
  await inspector.getByRole('heading', { name: '网关事实' }).waitFor();
  assert.equal(await inspector.locator('dt').filter({ hasText: /^HTTP 状态码$/ }).locator('xpath=following-sibling::dd').textContent(),
    String(toolDetail.metadata.client.response_code));
  await inspector.getByText('已阻断', { exact: true }).waitFor();
  await inspector.getByText(toolDetail.metadata.policy_version, { exact: true }).waitFor();
  await inspector.getByText(toolDetail.metadata.parameters.id, { exact: false }).waitFor();
  await inspector.getByText(orderID, { exact: false }).waitFor();

  await page.getByRole('button', { name: '返回会话列表' }).click();
  assert.equal(await range.inputValue(), '1');
  assert.equal(await agent.inputValue(), 'support-agent');
  assert.equal(await page.getByRole('searchbox').inputValue(), turn.sessionId);
  assert.equal(await sessionButton.evaluate((button) => document.activeElement === button), true);

  await search(page, emptyTurn.sessionId, 1);
  await page.getByRole('button', { name: emptyTurn.sessionId, exact: true }).click();
  await page.getByText('此 Trace 暂无可查看的节点。').waitFor();

  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole('button', { name: '返回会话列表' }).click();
  await range.selectOption('7');
  await agent.selectOption('operations-agent');
  await page.getByText('没有匹配的会话。').waitFor();
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), 390);
  await agent.selectOption('support-agent');
  await search(page, turn.sessionId, 1);
  await sessionButton.click();
  await turns.selectOption(trace.trace_id);
  await page.getByRole('button', { name: /orders\.lookup/ }).click();
  await inspector.getByRole('heading', { name: '网关事实' }).waitFor();
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), 390);
  await inspector.getByRole('button', { name: '返回调用列表' }).click();
  assert.equal(await page.getByRole('button', { name: /orders\.lookup/ }).evaluate((button) => document.activeElement === button), true);

  ingress.as('reader-token');
  await page.reload();
  await search(page, turn.sessionId, 1);
  await sessionButton.click();
  await inspector.getByText('当前身份没有查看此内容的权限。').waitFor();
  assert.equal(await inspector.getByText(prompt, { exact: false }).count(), 0);

  ingress.failNextRead();
  await page.reload();
  await page.getByRole('alert').getByText('请求失败，请重试。').waitFor();
  await page.getByRole('button', { name: '重试' }).click();
  await page.getByRole('img', { name: /会话数量分布/ }).waitFor();

  ingress.as(null);
  await page.reload();
  await page.getByRole('alert').getByText('没有查看权限，请检查当前登录身份。').waitFor();
  assert.equal(await page.getByRole('table').count(), 0);
  assert.deepEqual(ingress.browserCredentials, []);
  assert.deepEqual(errors, []);
});
