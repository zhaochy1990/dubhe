import { nodeName, renderNodeDetail } from "./node-detail.js";
import { dateTime, duration, elapsed, element, label, query, queryError, scopeOf, status } from "./view.js";

function topology(nodes) {
  const bySpan = new Map(nodes.filter((node) => node.span_id).map((node) => [node.span_id, node]));
  const children = new Map();
  for (const node of nodes) {
    const parent = node.parent_span_id;
    if (!bySpan.has(parent)) continue;
    if (!children.has(parent)) children.set(parent, []);
    children.get(parent).push(node);
  }
  const ordered = [];
  const unlinked = [];
  const depths = new Map();
  const visit = (root, group) => {
    const pending = [[root, 0]];
    while (pending.length) {
      const [node, depth] = pending.pop();
      if (depths.has(node.node_id)) continue;
      depths.set(node.node_id, depth);
      group.push(node);
      const descendants = children.get(node.span_id) || [];
      for (let index = descendants.length - 1; index >= 0; index--) {
        pending.push([descendants[index], depth + 1]);
      }
    }
  };
  for (const node of nodes) {
    if (!bySpan.has(node.parent_span_id)) visit(node, node.parent_span_id ? unlinked : ordered);
  }
  // Keep every visible node even when an incomplete or cyclic graph has no root.
  for (const node of nodes) visit(node, unlinked);
  return { ordered, unlinked, depths, bySpan };
}

export function renderSession(container, session, signal, goBack) {
  container.replaceChildren(document.querySelector("#session-template").content.cloneNode(true));
  const find = (selector) => container.querySelector(selector);
  const heading = find("#session-heading");
  const turn = find("#trace-turn");
  const turnsStatus = find("#turns-status");
  const moreTurns = find("#more-turns");
  const work = find(".trace-page-body");
  const traceIdentity = find("#trace-identity");
  const traceStatus = find("#trace-status");
  const canvas = find("#trace-canvas");
  const moreNodes = find("#more-nodes");
  const inspector = find("#span-detail");
  const inspectorHeading = find("#node-heading");
  const scope = scopeOf(session);
  const turns = [];
  const viewButtons = [...container.querySelectorAll("[data-trace-view]")];
  const compact = matchMedia("(max-width: 750px)");

  let traceRequest;
  let nodeRequest;
  let selected;
  let trace;
  let nodes = [];
  let view = "timeline";

  const sessionLabel = [
    session.session_id, session.agent_name || session.root_agent_id,
    [scope.tenant_id, scope.environment_id, scope.project_id].join(" / "),
  ];
  find("#session-identity").textContent = sessionLabel.join(" · ");
  find("#back-to-sessions").onclick = goBack;
  heading.focus({ preventScroll: true });
  window.scrollTo(0, 0);

  async function loadTurns(cursor) {
    const params = new URLSearchParams({ ...scope, session_id: session.session_id });
    if (cursor) params.set("cursor", cursor);
    moreTurns.disabled = true;
    status(turnsStatus, "正在加载 Trace 轮次…");
    try {
      const result = await query("sessions/traces", params, signal);
      if (signal.aborted) return;
      const firstPage = turns.length === 0;
      for (const item of result.traces) {
        if (turns.some((entry) => entry.trace_id === item.trace_id)) continue;
        turns.push(item);
        turn.add(new Option([item.turn_id, dateTime(item.start_ns)].join(" · "), item.trace_id));
      }
      find(".turns-controls").hidden = !turns.length;
      status(turnsStatus, turns.length ? "" : "此会话暂无可查看的 Trace。");
      moreTurns.hidden = !result.next_cursor;
      moreTurns.disabled = false;
      moreTurns.onclick = () => loadTurns(result.next_cursor);
      if (cursor && moreTurns.hidden) turn.focus();
      if (firstPage && turns.length) selectTrace();
    } catch (error) {
      if (!signal.aborted) queryError(turnsStatus, error, () => loadTurns(cursor));
    }
  }

  function selectTrace() {
    traceRequest?.abort();
    nodeRequest?.abort();
    const controller = new AbortController();
    traceRequest = controller;
    trace = turns.find((item) => item.trace_id === turn.value);
    renderTraceIdentity();
    selected = undefined;
    nodes = [];
    canvas.replaceChildren();
    status(inspector, "请选择调用节点。");
    moreNodes.hidden = true;
    work.hidden = false;
    loadNodes(trace.trace_id, AbortSignal.any([signal, controller.signal]));
  }

  function renderTraceIdentity() {
    const state = label(trace.run_state) || "状态未记录";
    const summary = [trace.trace_id, state, duration(elapsed(trace))];
    if (trace.observed_range) summary.push("观测范围");
    traceIdentity.textContent = summary.join(" · ");
  }

  async function loadNodes(traceID, traceSignal, cursor) {
    const params = new URLSearchParams(scope);
    if (cursor) params.set("cursor", cursor);
    moreNodes.disabled = true;
    status(traceStatus, "正在加载 Trace 节点…");
    try {
      const result = await query(`traces/${traceID}`, params, traceSignal);
      if (traceSignal.aborted) return;
      trace = result;
      renderTraceIdentity();
      mergePage(result.nodes);
      if (!selected && nodes.length) {
        const first = nodes.find((node) => node.kind === "llm") || nodes[0];
        selectNode(first);
      }
      draw();
      status(traceStatus, nodes.length ? "" : "此 Trace 暂无可查看的节点。");
      if (!nodes.length) status(inspector, "暂无调用详情。");
      moreNodes.hidden = !result.next_cursor;
      moreNodes.disabled = false;
      moreNodes.onclick = () => loadNodes(traceID, traceSignal, result.next_cursor);
      if (cursor && moreNodes.hidden) focusSelected();
    } catch (error) {
      if (!traceSignal.aborted) queryError(traceStatus, error, () => loadNodes(traceID, traceSignal, cursor));
    }
  }

  function mergePage(incoming) {
    // Gateway audit may merge with a tool client Span between cursor requests.
    const byID = new Map(incoming.map((node) => [node.node_id, node]));
    const bySpan = new Map(incoming.filter((node) => node.span_id).map((node) => [node.span_id, node]));
    const replacement = (node) => byID.get(node.node_id) || bySpan.get(node.span_id);
    const current = nodes.find((node) => node.node_id === selected);
    const updated = current && replacement(current);
    nodes = nodes.filter((node) => !replacement(node)).concat(incoming);
    if (updated) selectNode(updated);
  }

  function selectNode(node, navigate = false) {
    selected = node.node_id;
    for (const button of canvas.querySelectorAll("[data-node]")) {
      const active = button.dataset.node === selected;
      button.classList.toggle("selected", active);
      button.setAttribute("aria-pressed", active);
    }
    nodeRequest?.abort();
    const controller = new AbortController();
    nodeRequest = controller;
    const nodeSignal = AbortSignal.any([signal, controller.signal]);
    loadNode(node, nodeSignal);
    if (navigate && compact.matches) inspectorHeading.focus();
  }

  async function loadNode(node, nodeSignal) {
    status(inspector, "正在加载调用详情…");
    const params = new URLSearchParams({ ...scope, expected_trace_id: node.trace_id, content_format: "json_text" });
    try {
      const result = await query(`nodes/${encodeURIComponent(node.node_id)}`, params, nodeSignal);
      if (!nodeSignal.aborted) renderNodeDetail(inspector, result, session.session_id);
    } catch (error) {
      if (!nodeSignal.aborted) queryError(inspector, error, () => loadNode(node, nodeSignal));
    }
  }

  function focusSelected() {
    [...canvas.querySelectorAll("[data-node]")].find((button) => button.dataset.node === selected)?.focus();
  }

  function draw() {
    canvas.replaceChildren();
    if (!nodes.length) return;
    const { ordered, unlinked, depths, bySpan } = topology(nodes);
    const timeline = view === "timeline";
    let start = trace.start_ns ?? nodes[0].start_ns;
    let end = trace.end_ns ?? start;
    for (const node of nodes) {
      start = Math.min(start, node.start_ns);
      end = Math.max(end, node.end_ns);
    }
    const scale = Math.max(1, end - start);
    if (timeline) {
      const axis = element("div", null, "trace-axis");
      const labels = element("div", null, "trace-axis-labels");
      labels.append(...[0, (end - start) / 2, end - start].map((point) => element("span", duration(point))));
      axis.append(labels);
      canvas.append(axis);
    }
    for (const node of timeline ? nodes : ordered.concat(unlinked)) {
      if (!timeline && node === unlinked[0]) {
        canvas.append(element("h3", "未关联节点", "unlinked-heading"));
      }
      const active = selected === node.node_id;
      const button = element("button", null, `waterfall-row${timeline ? "" : " tree-row"}${active ? " selected" : ""}`);
      button.type = "button";
      button.dataset.node = node.node_id;
      button.dataset.parentSpan = node.parent_span_id || "";
      button.setAttribute("aria-pressed", active);
      button.onclick = () => selectNode(node, true);

      const depth = depths.get(node.node_id);
      const name = element("span", nodeName(node), "waterfall-name");
      name.style.paddingInlineStart = `${Math.min(depth, 8) * 12}px`;
      if (depth) {
        const branch = element("span", "└ ");
        branch.setAttribute("aria-hidden", "true");
        name.prepend(branch);
      }
      const kind = { run: "轮次", agent: "Agent", llm: "模型", tool_client: "工具调用", tool: "工具调用" }[node.kind] || node.kind;
      const parent = bySpan.get(node.parent_span_id);
      const captions = [kind];
      if (node.parent_span_id && !parent) captions.push("父节点未加载");
      if (timeline && parent && timingOutsideParent(node, parent)) captions.push("超出父节点时间区间");
      name.append(element("small", captions.join(" · ")));
      button.append(name);
      const nodeDuration = elapsed(node);
      if (timeline) {
        const track = element("span", null, "waterfall-track");
        track.setAttribute("aria-hidden", "true");
        const bar = element("i", null, `bar-${node.kind}`);
        const offset = (node.start_ns - start) / scale * 100;
        bar.style.left = `${Math.min(98, offset)}%`;
        bar.style.width = `${Math.max(2, nodeDuration / scale * 100)}%`;
        track.append(bar);
        button.append(track);
      }
      button.append(element("span", duration(nodeDuration), "waterfall-duration"));
      canvas.append(button);
    }
  }

  for (const button of viewButtons) {
    button.onclick = () => {
      view = button.dataset.traceView;
      for (const option of viewButtons) {
        option.classList.toggle("active", option === button);
        option.setAttribute("aria-pressed", option === button);
      }
      draw();
    };
  }
  find(".inspector-back").onclick = focusSelected;
  turn.onchange = selectTrace;
  loadTurns();
}

function timingOutsideParent(node, parent) {
  const source = (item) => item.metadata?.client_producer_id || item.producer_id;
  return source(node) !== source(parent) && (node.start_ns < parent.start_ns || node.end_ns > parent.end_ns);
}
