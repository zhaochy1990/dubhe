const api = new URL("../v1/observability/", document.baseURI);

class ApiError extends Error {
  constructor(status) {
    super(`Tracing HTTP ${status}`);
    this.status = status;
  }
}

export async function query(path, params, signal) {
  const url = new URL(path, api);
  url.search = params;
  const response = await fetch(url, { signal, headers: { Accept: "application/json" } });
  if (!response.ok) throw new ApiError(response.status);
  return response.json();
}

export function scopeOf({ tenant_id, environment_id, project_id }) {
  return { tenant_id, environment_id, project_id };
}

export function element(tag, content, className) {
  const el = document.createElement(tag);
  if (content != null) el.textContent = content;
  if (className) el.className = className;
  return el;
}

export function status(container, message, { retry, error } = {}) {
  container.replaceChildren();
  if (!message) return;
  const label = element("p", message);
  label.setAttribute("role", error ? "alert" : "status");
  container.append(label);
  if (retry) {
    const button = element("button", "重试", "btn");
    button.type = "button";
    button.onclick = retry;
    container.append(button);
  }
}

export function queryError(container, error, retry) {
  if ([401, 403, 404].includes(error.status)) {
    status(container, error.status === 404 ? "数据不可用，可能已过期或没有查看权限。" : "没有查看权限，请检查当前登录身份。", { error: true });
  } else {
    status(container, "请求失败，请重试。", { retry, error: true });
  }
}

export const timestamp = new Intl.DateTimeFormat("zh-CN", {
  year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
});
export const timezone = timestamp.resolvedOptions().timeZone;

export function dateTime(ns) {
  return ns == null ? "—" : timestamp.format(ns / 1e6);
}

export function duration(ns) {
  if (ns == null) return "—";
  const ms = ns / 1e6;
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)} s` : `${Number(ms.toFixed(2))} ms`;
}

export function elapsed({ start_ns, end_ns }) {
  return start_ns == null || end_ns == null ? null : Math.max(0, end_ns - start_ns);
}

export function actor(value) {
  if (!value) return "—";
  return `${value.kind === "user" ? "用户" : "工作负载"} · ${value.id}`;
}

const labels = {
  allow: "已允许", deny: "已拒绝", block: "已阻断", require_confirmation: "需要确认",
  pending: "待确认", approved: "已批准", rejected: "已拒绝", expired: "已过期",
  not_executed: "未执行", executing: "执行中", succeeded: "执行成功", failed: "失败",
  unknown: "未知", completed: "已完成", cancelled: "已取消",
  gateway: "网关", local: "本地", direct: "直连", response: "收到响应", timeout: "超时", error: "错误",
  decision: "决策", confirmation: "确认", revalidation: "重新校验",
  forward_intent: "转发意图", execution_result: "执行结果", reconciliation: "结果核对",
};

export function label(value) {
  return Object.hasOwn(labels, value) ? labels[value] : value;
}
