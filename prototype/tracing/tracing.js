const sessionData = [
  [
    "SES-2026-0842",
    "客服助手",
    "用户 A",
    "今天 10:42",
    "3 次调用",
    "已阻断",
    "1.28 s",
  ],
  [
    "SES-2026-0841",
    "运营分析 Agent",
    "自主任务",
    "2 天前 10:14",
    "3 次调用",
    "已拒绝",
    "860 ms",
  ],
  [
    "SES-2026-0840",
    "客服助手",
    "用户 B",
    "9 天前 09:36",
    "1 次调用",
    "已完成",
    "425 ms",
  ],
];
const sessionAges = [0, 2, 9];
function renderSessions(state = {}) {
  document.getElementById("crumb").textContent = "Agent 观测";
  let p = document.getElementById("page");
  p.innerHTML = `<div class="heading"><div><h1>Agent 观测</h1><p>按会话查看 Agent、模型和工具的调用轨迹。</p></div><select class="btn" id="traceTime" aria-label="时间范围"><option>最近 7 天</option><option>最近 24 小时</option><option>最近 30 天</option></select></div><div class="observe-layout"><aside class="panel observe-filters"><div class="panel-head"><h2>筛选</h2><button class="link" id="resetObserve">清除</button></div><div class="observe-group"><b>Agent</b>${[...new Set(sessionData.map((x) => x[1]))].map((v) => `<label><input type="checkbox" value="${v}" data-observe="agent">${v}</label>`).join("")}</div><div class="observe-group"><b>结果</b>${["已完成", "已阻断", "已拒绝"].map((v) => `<label><input type="checkbox" value="${v}" data-observe="result">${v}</label>`).join("")}</div></aside><section class="panel observe-main"><div class="observe-toolbar"><h2>Sessions</h2><input id="sessionSearch" aria-label="搜索 Session 或 Agent" placeholder="搜索 Session 或 Agent"></div><div class="observe-chart"><div><b>会话数量</b><span id="chartRange"></span></div><div class="bars" id="sessionBars" role="img" aria-label="会话数量分布"></div><div class="chart-labels"><span id="chartStart"></span><span>今天</span></div></div><div class="table-wrap"><table><thead><tr><th>开始时间</th><th>Session ID</th><th>Agent</th><th>发起人</th><th>工具调用</th><th>结果</th><th>耗时</th></tr></thead><tbody id="sessionRows"></tbody></table></div><div class="pagination"><span id="sessionCount"></span></div></section></div>`;
  function draw() {
    let q = document.getElementById("sessionSearch").value.toLowerCase(),
      agents = [...p.querySelectorAll('[data-observe="agent"]:checked')].map(
        (x) => x.value,
      ),
      results = [...p.querySelectorAll('[data-observe="result"]:checked')].map(
        (x) => x.value,
      );
    const range = p.querySelector("#traceTime");
    const days = [7, 1, 30][range.selectedIndex];
    let filtered = sessionData.filter(
      (r, i) =>
        sessionAges[i] < days &&
        r.join(" ").toLowerCase().includes(q) &&
        (!agents.length || agents.includes(r[1])) &&
        (!results.length || results.includes(r[5])),
    );
    const bucketCount = days === 1 ? 24 : days;
    const counts = Array.from({ length: bucketCount }, () => 0);
    for (const row of filtered) {
      const age = sessionAges[sessionData.indexOf(row)];
      counts[bucketCount - 1 - age]++;
    }
    p.querySelector("#sessionBars").innerHTML = counts
      .map((count) => `<i style="height:${count ? count * 36 : 0}px"></i>`)
      .join("");
    p.querySelector("#sessionBars").setAttribute(
      "aria-label",
      `会话数量分布，共 ${filtered.length} 个会话`,
    );
    p.querySelector("#chartRange").textContent = range.value;
    p.querySelector("#chartStart").textContent =
      days === 1 ? "24 小时前" : `${days} 天前`;
    document.getElementById("sessionRows").innerHTML =
      filtered
        .map(
          (r) =>
            `<tr class="session-row" data-id="${r[0]}">${[r[3], r[0], r[1], r[2], r[4], r[5], r[6]].map((v, i) => `<td>${i === 1 ? `<button class="link session-id" type="button">${v}</button>` : v}</td>`).join("")}</tr>`,
        )
        .join("") || '<tr><td colspan="7">没有匹配的会话</td></tr>';
    document.getElementById("sessionCount").textContent =
      `共 ${filtered.length} 个 Session`;
    p.querySelectorAll(".session-id").forEach(
      (el) =>
        (el.onclick = () =>
          renderTrace(el.closest("tr").dataset.id, {
            query: p.querySelector("#sessionSearch").value,
            agents,
            results,
            time: p.querySelector("#traceTime").value,
            scroll: window.scrollY,
          })),
    );
  }
  p.querySelectorAll("[data-observe]").forEach((el) => (el.onchange = draw));
  document.getElementById("sessionSearch").oninput = draw;
  p.querySelector("#traceTime").onchange = draw;
  document.getElementById("resetObserve").onclick = () => {
    p.querySelectorAll("[data-observe]").forEach((el) => (el.checked = false));
    document.getElementById("sessionSearch").value = "";
    p.querySelector("#traceTime").selectedIndex = 0;
    draw();
  };
  p.querySelector("#sessionSearch").value = state.query || "";
  if (state.time) p.querySelector("#traceTime").value = state.time;
  p.querySelectorAll("[data-observe]").forEach(
    (el) =>
      (el.checked =
        (el.dataset.observe === "agent"
          ? state.agents
          : state.results
        )?.includes(el.value) || false),
  );
  draw();
}
const traceSystemPrompt = `你是企业客服助手，负责查看订单、整理物流进度，并在授权范围内协助客户沟通。请用简明的中文回答，先说明已核实的结果，再说明下一步。

订单查询以订单服务返回的状态为准。客户资料以客户服务返回的授权范围为准；如有多笔订单，按创建时间降序选择最新一笔。保留订单编号和预计送达日期，明确“预计”并非承诺。不要猜测物流单号、付款金额或配送地址。

邮件只能发送给本次任务授权的收件人。正文只包含客户识别订单所需的最少信息：订单编号、物流状态和预计送达时间。不得附上地址、手机号、支付信息、内部备注或完整的客户资料。准备草稿和实际发送是两件不同的事；只有邮件服务返回成功回执，才可告诉用户已发送。

如果网关阻断调用，保留已完成的查询结果，说明未执行的操作和可行的后续步骤。不要重试相同的阻断请求，也不要改用其他渠道绕过限制。`;
const traceUserPrompt = `请帮我查一下客户 A 最近一笔订单的状态。如果订单已经发货，请整理一份中文进度摘要，包括订单编号、当前物流状态和预计送达时间，再把摘要发给客户。客服工单里暂时填写的收件地址是 customer.a@example.test。

邮件不用附带账单或订单明细，也不要包含电话和地址。预计送达请写成预计时间；如果物流服务暂时没有这个信息，就只说当前状态。处理完请告诉我查到了什么、邮件是否真的发出了，以及还有什么需要我确认。`;
const traceModelSamples = {
  1: {
    context:
      "工单 CS-10482 · 客户编号 customer-A。当前还没有订单查询或收件人校验结果。",
    output: `收到。我会先查询客户 A 最近一笔订单，并另行核对工单填写的收件地址是否在本次任务的授权范围内。

订单查询会优先取得订单编号、物流状态和预计送达时间。对客户资料只核对收件地址的授权，不需要读取付款信息、联系电话或配送地址。如果查询到多笔订单，我会以最近创建的一笔为准；如果物流服务未返回预计时间，邮件摘要不会自行补填。

在拿到这两项结果前，我不会把订单状态或邮件结果写成已确认。最终会分开告诉你查询到了什么，以及邮件是否实际发送成功。`,
  },
  6: {
    context: `订单服务：SO-20261118-024，状态 SHIPPED，预计 2026-11-19 送达。客户服务：地址 customer.a@example.test 来自工单；工具返回结果未提供本轮外发授权。尚无邮件服务回执。`,
    output: `已查询到客户 A 最近一笔订单 SO-20261118-024。订单已经发货，物流服务预计 11 月 19 日送达。这个日期是当前预估，可能随配送进度更新。

我整理了以下摘要：
主题：订单 SO-20261118-024 配送进度
正文：您好，您的订单 SO-20261118-024 已发货，预计于 11 月 19 日送达。实际到达时间以承运商的配送更新为准。若需要查询最新进度，可以通过原客服工单联系我们。

这份摘要未包含账单、配送地址或联系电话。工单中填写了收件地址，但仅填写地址不代表已经授权外发；邮件状态需以网关判定和邮件服务回执为准。`,
  },
  8: {
    context: `邮件调用返回 403，错误码 RECIPIENT_OUT_OF_SCOPE。网关在转发前阻断，未产生邮件服务回执。此前的订单查询已经成功。`,
    output: `订单已查到，邮件未发送。

订单 SO-20261118-024 是客户 A 最近一笔订单，当前状态为已发货，物流服务预计 11 月 19 日送达。预计日期可能随配送进度调整。我已整理好中文进度摘要，内容仅包含订单编号、物流状态和预计送达日期，不含电话、配送地址或付款信息。

工单里填的收件地址不在本次任务授权的收件人范围内，邮件请求在转发前被网关阻断。没有邮件发送回执，因此不能告诉客户邮件已经发出。

如需继续，请在工单中选择已授权的收件人，或按流程补充这次外发授权。确认后可以重新提交发送请求。`,
  },
};
function renderTrace(id, listState = {}) {
  let session = sessionData.find((s) => s[0] === id),
    total = 1280;
  let nodes = [
    { name: "客服助手", kind: "agent", parent: null, start: 0, duration: 1280 },
    { name: "LLM 调用", kind: "llm", parent: 0, start: 0, duration: 300 },
    {
      name: "订单查询子 Agent",
      kind: "agent",
      parent: 0,
      start: 300,
      duration: 400,
    },
    {
      name: "查询客户订单",
      kind: "tool",
      parent: 2,
      start: 320,
      duration: 300,
    },
    {
      name: "客户信息子 Agent",
      kind: "agent",
      parent: 0,
      start: 300,
      duration: 380,
    },
    {
      name: "查询客户资料",
      kind: "tool",
      parent: 4,
      start: 340,
      duration: 260,
    },
    { name: "LLM 调用", kind: "llm", parent: 0, start: 700, duration: 140 },
    { name: "发送邮件", kind: "tool", parent: 0, start: 840, duration: 160 },
    { name: "LLM 调用", kind: "llm", parent: 0, start: 1000, duration: 280 },
  ];
  const tracePage = document.getElementById("page");
  document.getElementById("crumb").textContent = "会话详情";
  tracePage.innerHTML = `<div class="heading trace-page-heading"><div><h1 tabindex="-1">会话详情</h1><p><b>${session[0]}</b> · ${session[1]} · ${session[2]}</p></div><button class="btn" id="backToSessions" type="button">返回会话列表</button></div>${id === "SES-2026-0842" ? `<div class="panel trace-page-body"><div class="trace-work"><div class="trace-work-head view-switch"><b>Trace</b><div><button class="btn active" type="button" data-trace-view="timeline" aria-pressed="true">时间线</button><button class="btn" type="button" data-trace-view="tree" aria-pressed="false">调用树</button></div></div><div id="traceCanvas"></div></div><aside class="trace-inspector"><div class="panel-head"><h2>调用详情</h2></div><div class="trace-detail" id="spanDetail"></div></aside></div>` : '<div class="panel trace-empty">此会话暂无调用明细</div>'}`;
  tracePage.querySelector("#backToSessions").onclick = () => {
    renderSessions(listState);
    window.scrollTo(0, listState.scroll || 0);
    [...tracePage.querySelectorAll(".session-id")]
      .find((button) => button.textContent === id)
      ?.focus({ preventScroll: true });
  };
  tracePage.querySelector("h1").focus({ preventScroll: true });
  window.scrollTo(0, 0);
  if (id !== "SES-2026-0842") return;
  let selected = 1,
    view = "timeline";
  function depth(n) {
    let d = 0;
    while (nodes[n].parent !== null) {
      d++;
      n = nodes[n].parent;
    }
    return d;
  }
  function draw() {
    let canvas = tracePage.querySelector("#traceCanvas");
    canvas.innerHTML =
      (view === "timeline"
        ? `<div class="trace-axis"><span>0 ms</span><span>640 ms</span><span>1280 ms</span></div>`
        : "") +
      nodes
        .map(
          (n, i) =>
            `<button class="waterfall-row ${selected === i ? "selected" : ""} ${view === "tree" ? "tree-row" : ""}" data-node="${i}" aria-pressed="${selected === i}"><span class="waterfall-name" style="padding-left:${depth(i) * 18}px">${depth(i) ? "└ " : ""}${n.name}<small>${n.kind === "llm" ? "LLM Call · qwen-plus" : n.kind === "tool" ? "工具调用" : "Agent"}</small></span>${view === "timeline" ? `<span class="waterfall-track"><i style="left:${(n.start / total) * 100}%;width:${Math.max(2, (n.duration / total) * 100)}%;background:${n.kind === "llm" ? "#9474c6" : n.kind === "agent" ? "#84bcae" : "#59a995"}"></i></span>` : ""}<span class="waterfall-duration">${n.duration} ms</span></button>`,
        )
        .join("");
    canvas.querySelectorAll("[data-node]").forEach(
      (el) =>
        (el.onclick = () => {
          selected = +el.dataset.node;
          draw();
          detail();
          canvas
            .querySelector(`[data-node="${selected}"]`)
            ?.focus({ preventScroll: true });
        }),
    );
  }
  function detail() {
    let n = nodes[selected],
      isModel = n.kind === "llm",
      isTool = n.kind === "tool",
      blocked = n.name === "发送邮件";
    const sample = traceModelSamples[selected];
    const io = (title, id) =>
      `<div class="io-heading"><h3>${title}</h3><button type="button" class="link copy-io" data-target="${id}" aria-label="复制 ${title}">复制</button></div><pre class="io-box" id="${id}" tabindex="0" aria-label="${title}"></pre>`;
    tracePage.querySelector("#spanDetail").innerHTML =
      `<h3>${n.name}</h3>${isModel ? `<div class="model-meta"><div>模型：<b>qwen-plus</b></div><div>耗时：<b>${n.duration} ms</b></div><div>调用状态：<b>已完成</b></div></div><h3>模型参数</h3><div class="trace-facts"><div><small>思考强度</small><b>中</b></div><div><small>最大输出 Token</small><b>2,048</b></div><div><small>输入 Token</small><b>1,284</b></div><div><small>输出 Token</small><b>286</b></div><div><small>Temperature</small><b>0.7</b></div></div><span class="trace-sample-label">脱敏样例</span>${io("System Prompt", "systemPrompt")}${io("User Prompt", "userPrompt")}${io("工具与会话上下文", "modelContext")}${io("模型输出", "modelOutput")}` : isTool ? `<p class="sub">耗时：${n.duration} ms</p><div class="trace-facts"><div><small>网关决策</small><b>${blocked ? "已阻断" : "已允许"}</b></div><div><small>执行结果</small><b>${blocked ? "未执行" : "执行完成"}</b></div><div><small>发起身份</small><b>${session[2]}</b></div></div><h3>请求参数</h3><pre>{ "customer_id": "customer-A", "内容": "已脱敏" }</pre>` : `<p class="sub">耗时：${n.duration} ms</p><p>父级：${n.parent === null ? "无" : nodes[n.parent].name}</p>`}<h3>关联信息</h3><div class="related-lines"><div>Session：${session[0]}</div><div>Trace：TR-${id.slice(-4)}</div>${isTool ? "<div>策略版本：v1.2</div>" : ""}<div>内容：已脱敏</div></div>`;
    if (isModel) {
      const blocks = {
        systemPrompt: traceSystemPrompt,
        userPrompt: traceUserPrompt,
        modelContext: sample.context,
        modelOutput: sample.output,
      };
      Object.entries(blocks).forEach(([key, value]) => {
        tracePage.querySelector("#" + key).textContent = value;
      });
      tracePage.querySelectorAll(".copy-io").forEach(
        (btn) =>
          (btn.onclick = async () => {
            const pre = tracePage.querySelector("#" + btn.dataset.target);
            try {
              await navigator.clipboard.writeText(pre.textContent);
              btn.textContent = "已复制";
            } catch {
              const range = document.createRange();
              range.selectNodeContents(pre);
              const selection = window.getSelection();
              selection.removeAllRanges();
              selection.addRange(range);
              btn.textContent = "已选中";
            }
          }),
      );
    }
  }
  tracePage.querySelectorAll("[data-trace-view]").forEach(
    (btn) =>
      (btn.onclick = () => {
        view = btn.dataset.traceView;
        tracePage.querySelectorAll("[data-trace-view]").forEach((el) => {
          el.classList.toggle("active", el === btn);
          el.setAttribute("aria-pressed", String(el === btn));
        });
        draw();
      }),
  );
  draw();
  detail();
}
renderSessions();
