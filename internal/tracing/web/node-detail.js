import { actor, duration, elapsed, element, label, timestamp } from "./view.js";

export function nodeName(node) {
  const metadata = node.metadata || {};
  switch (node.kind) {
    case "llm": return metadata.model_name || metadata.model_id || "模型调用";
    case "tool":
    case "tool_client": return metadata.tool_id || "工具调用";
    default: return node.agent_name || node.agent_id || node.kind;
  }
}

function section(container, title) {
  const group = element("section");
  group.append(element("h3", title));
  container.append(group);
  return group;
}

function facts(container, entries, className = "trace-facts") {
  const list = element("dl", null, className);
  for (const [key, value] of entries) {
    const pair = element("div");
    pair.append(element("dt", key), element("dd", value ?? "—"));
    list.append(pair);
  }
  container.append(list);
}

function sdkStatus(value) {
  return ({ 0: "未设置", 1: "正常", 2: "错误" })[value];
}

function content(container, title, value) {
  container.append(element("h4", title));
  if (value == null) {
    container.append(element("p", "未采集此项。"));
    return;
  }
  const data = element("pre", value, "io-box");
  data.tabIndex = 0;
  data.setAttribute("aria-label", title);
  container.append(data);
}

function contentNotice(container, state) {
  const text = {
    forbidden: "当前身份没有查看此内容的权限。",
    expired: "内容留存期限已过。",
    not_captured: "暂无可查看的采集内容。",
  }[state] || "内容不可用。";
  container.append(element("p", text));
}

function modelDetail(container, node, metadata) {
  const settings = section(container, "模型配置与用量");
  facts(settings, [
    ["模型", metadata.model_name], ["模型 ID", metadata.model_id], ["Temperature", metadata.temperature],
    ["思考强度", metadata.reasoning_effort], ["最大输出 Token", metadata.max_output_tokens],
    ["输入 Token", metadata.input_tokens], ["输出 Token", metadata.output_tokens],
  ]);
  const inputs = section(container, "模型输入");
  if (node.content_state !== "available") {
    contentNotice(inputs, node.content_state);
    return;
  }
  content(inputs, "系统指令", metadata.system_instructions);
  content(inputs, "输入消息（按请求顺序）", metadata.messages);
  content(inputs, "工具定义", metadata.tool_definitions);
}

function toolDetail(container, node, metadata) {
  const client = node.source === "sdk" ? metadata : metadata.client;
  const observation = section(container, "客户端观测");
  if (client) {
    facts(observation, [
      ["调用路径", label(client.tool_route)], ["客户端结果", label(client.client_result)],
      ["HTTP 状态码", client.response_code], ["客户端调用 ID", client.client_call_id],
      ["客户端耗时", duration(elapsed(node))], ["Span 状态", sdkStatus(node.status)],
      ["上报来源", metadata.client_producer_id || node.producer_id],
    ]);
  } else {
    observation.append(element("p", "暂无关联的客户端 Span。"));
  }

  if (node.source !== "gateway" && client?.tool_route !== "gateway") return;
  const gateway = section(container, "网关事实");
  if (node.source !== "gateway") {
    gateway.append(element("p", "尚无关联的网关审计记录。"));
    return;
  }
  facts(gateway, [
    ["网关决策", label(metadata.decision)], ["确认状态", label(metadata.confirmation_state)],
    ["执行状态", label(metadata.execution_state)], ["策略版本", metadata.policy_version],
    ["原因代码", metadata.reason_code], ["发起身份", actor(metadata.initiator)],
    ["网关调用 ID", metadata.call_id], ["审计来源", node.producer_id],
  ]);
  if (node.content_state === "available") {
    content(gateway, "网关判定参数", metadata.parameters);
  } else {
    gateway.append(element("h4", "网关判定参数"));
    contentNotice(gateway, node.content_state);
  }
  if (node.audit?.length) {
    gateway.append(element("h4", "网关审计事件"));
    const audit = element("ol", null, "audit-events");
    for (const event of node.audit) {
      audit.append(element("li", [
        label(event.type), timestamp.format(new Date(event.occurred_at)), label(event.decision),
        label(event.execution_state), label(event.confirmation_state),
      ].filter(Boolean).join(" · ")));
    }
    gateway.append(audit);
  }
}

export function renderNodeDetail(container, node, sessionID) {
  const metadata = node.metadata || {};
  container.replaceChildren(element("h3", nodeName(node)));
  if (node.kind === "tool" || node.kind === "tool_client") {
    toolDetail(container, node, metadata);
  } else {
    const summary = [["耗时", duration(elapsed(node))], ["Span 状态", sdkStatus(node.status)]];
    if (metadata.run_outcome) summary.push(["轮次结果", label(metadata.run_outcome)]);
    facts(container, summary);
    if (node.kind === "llm") modelDetail(container, node, metadata);
  }
  const related = section(container, "关联信息");
  facts(related, [
    ["Session", sessionID], ["Trace", node.trace_id], ["节点 ID", node.node_id],
    ["Span ID", node.span_id], ["父 Span ID", node.parent_span_id],
    ["Agent", node.agent_name ? `${node.agent_name} · ${node.agent_id}` : node.agent_id],
  ], "related-lines");
}
