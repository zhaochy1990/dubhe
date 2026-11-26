import { renderSession } from "./trace.js";
import { actor, dateTime, duration, element, query, queryError, status, timezone } from "./view.js";

const list = document.querySelector("#session-list");
const detail = document.querySelector("#session-detail");
const crumb = document.querySelector("#crumb");
const range = document.querySelector("#trace-time");
const agent = document.querySelector("#trace-agent");
const search = document.querySelector("#session-search");
const chart = document.querySelector("#chart");
const table = document.querySelector("#sessions-table");
const rows = document.querySelector("#session-rows");
const pagination = document.querySelector("#pagination");
const previous = document.querySelector("#previous-page");
const next = document.querySelector("#next-page");
const listStatus = document.querySelector("#sessions-status");
const count = document.querySelector("#session-count");
const day = 24 * 60 * 60 * 1000;

let request;
let debounce;
let filters;
let cursors;
let pageIndex = 0;
let nextCursor;

function showSession(session, trigger) {
  const controller = new AbortController();
  const scroll = window.scrollY;
  const tableScroll = table.scrollLeft;
  list.hidden = true;
  detail.hidden = false;
  crumb.textContent = "会话详情";
  renderSession(detail, session, controller.signal, () => {
    controller.abort();
    detail.hidden = true;
    detail.replaceChildren();
    list.hidden = false;
    crumb.textContent = "Agent 观测";
    table.scrollLeft = tableScroll;
    trigger.focus({ preventScroll: true });
    window.scrollTo(0, scroll);
  });
}

function renderRows(sessions) {
  rows.replaceChildren(...sessions.map((session) => {
    const row = element("tr");
    const button = element("button", session.session_id, "link session-id");
    button.type = "button";
    button.title = [session.tenant_id, session.environment_id, session.project_id].join(" / ");
    button.onclick = () => showSession(session, button);
    const elapsed = duration(session.duration_ns);
    const columns = [
      dateTime(session.started_at_ns), button, session.agent_name || session.root_agent_id,
      actor(session.initiator), `${session.tool_calls} 次调用`,
      session.observed_range ? `${elapsed}（观测）` : elapsed,
    ];
    for (const value of columns) {
      const cell = element("td");
      cell.append(value);
      row.append(cell);
    }
    return row;
  }));
}

function renderAgents(options) {
  const selected = agent.value;
  const selectedText = agent.selectedOptions[0]?.textContent;
  const names = new Map();
  for (const { id, name } of options) {
    if (!names.has(id)) names.set(id, new Set());
    names.get(id).add(name || id);
  }
  agent.replaceChildren(new Option("全部 Agent", ""));
  for (const [id, values] of names) {
    agent.add(new Option([...values].join(" / "), id));
  }
  if (selected && !names.has(selected)) agent.add(new Option(selectedText, selected));
  agent.value = selected;
}

function renderChart(distribution, total) {
  const chartDate = new Intl.DateTimeFormat("zh-CN", {
    month: "numeric", day: "numeric",
    ...(range.value === "1" ? { hour: "2-digit", minute: "2-digit", hourCycle: "h23" } : {}),
  });
  const peak = Math.max(1, ...distribution.map((bucket) => bucket.count));
  const bars = document.querySelector("#session-bars");
  const descriptions = [];
  bars.replaceChildren(...distribution.map((bucket) => {
    const bar = element("i");
    bar.style.height = `${bucket.count / peak * 100}%`;
    bar.title = `${chartDate.format(new Date(bucket.from))}：${bucket.count} 个会话`;
    if (bucket.count) descriptions.push(bar.title);
    return bar;
  }));
  bars.setAttribute("aria-label", [`会话数量分布，共 ${total} 个会话`, ...descriptions].join("；"));
  document.querySelector("#chart-range").textContent = range.selectedOptions[0].textContent;
  document.querySelector("#chart-start").textContent = chartDate.format(new Date(filters.get("from")));
  document.querySelector("#chart-end").textContent = chartDate.format(new Date(filters.get("to")));
  chart.hidden = false;
}

function showLoading() {
  chart.hidden = table.hidden = pagination.hidden = true;
  rows.replaceChildren();
  status(listStatus, "正在加载会话…");
}

async function loadSessions(index = 0, focusTarget = null) {
  request?.abort();
  const controller = new AbortController();
  request = controller;
  const params = new URLSearchParams(filters);
  if (cursors[index]) params.set("cursor", cursors[index]);
  showLoading();
  try {
    const result = await query("sessions", params, controller.signal);
    if (controller.signal.aborted) return;
    pageIndex = index;
    nextCursor = result.next_cursor;
    renderAgents(result.agents);
    renderChart(result.distribution, result.total);
    renderRows(result.sessions);
    status(listStatus, result.sessions.length ? "" : "没有匹配的会话。");
    table.hidden = false;
    pagination.hidden = false;
    previous.disabled = index === 0;
    next.disabled = !nextCursor;
    count.textContent = `共 ${result.total} 个 Session${index || nextCursor ? ` · 第 ${index + 1} 页` : ""}`;
    if (focusTarget) (focusTarget.disabled ? table : focusTarget).focus({ preventScroll: true });
  } catch (error) {
    if (!controller.signal.aborted) queryError(listStatus, error, () => refresh());
  }
}

function updateFilters() {
  clearTimeout(debounce);
  const now = Date.now();
  filters = new URLSearchParams({
    from: new Date(now - Number(range.value) * day).toISOString(),
    to: new Date(now).toISOString(),
    tz: timezone,
  });
  const searchTerm = search.value.trim();
  if (searchTerm) {
    // Match the API's UTF-8 byte limit without splitting characters.
    const encoder = new TextEncoder();
    let bytes = 0;
    let normalized = "";
    for (const char of searchTerm) {
      bytes += Math.max(encoder.encode(char).length, encoder.encode(char.toLowerCase()).length);
      if (bytes > 256) break;
      normalized += char;
    }
    filters.set("q", normalized);
  }
  if (agent.value) filters.set("agent_id", agent.value);
  cursors = [];
}

function refresh() {
  updateFilters();
  loadSessions();
}

range.onchange = refresh;
agent.onchange = refresh;
search.oninput = () => {
  request?.abort();
  updateFilters();
  showLoading();
  debounce = setTimeout(loadSessions, 200);
};
previous.onclick = () => loadSessions(pageIndex - 1, previous);
next.onclick = () => {
  cursors[pageIndex + 1] = nextCursor;
  loadSessions(pageIndex + 1, next);
};
refresh();
