# Agent Tracing SDK API 设计

V1 设计草案 · 2026-11-23

本设计供可信应用入口和 Agent 运行时接入。SDK 给运行过程创建 OpenTelemetry Span，通过 OTLP/HTTP 把结构化元数据送到 `POST /v1/traces`。受管工具的策略决策、确认和执行回执由 Tooling Gateway 审计提供。SDK 自带的工具 Span 负责客户端视角。

[整体架构](tracing-architecture.md) · [实体关系图](tracing-architecture.html#data-model) · [V1 OTLP 接入决策](../adr/0001-v1-otlp-trace-ingest.md)

## 1. 调用方与初始化

可信应用入口创建 Session 和轮次的运行上下文。参与一个运行的受信工作负载通过已有的内部分派渠道接收当前父上下文及业务关联凭据。进程按归属和工作负载身份复用 SDK 实例，每个 exporter 的鉴权绑定一个租户、环境和项目。可信入口的登记器与签发器通过平台内部适配器接入。

下面展示 TypeScript 形态，其他语言按相同的生命周期、属性和协议实现。类型表达业务 API，不规定 SDK 必须使用 TypeScript。

```ts
export type OpaqueId = string
export type RunKind = 'turn' | 'autonomous'
export type RunOutcome = 'completed' | 'failed' | 'cancelled'
export type ToolClientResult = 'response' | 'timeout' | 'error' | 'cancelled'

export interface TraceScope {
  tenantId: OpaqueId
  environmentId: OpaqueId
  projectId: OpaqueId
}

/** 运行时持有的上下文，在公开 API 里作为不透明句柄。 */
declare const contextBrand: unique symbol

export interface TraceContext {
  readonly [contextBrand]: true
  readonly traceId: string
}

export interface RunOptions {
  sessionId: OpaqueId
  kind: RunKind
  runId: OpaqueId
  rootAgentId: OpaqueId
  taskId?: OpaqueId
  correlationId?: OpaqueId
}

export interface VerifiedRun {
  scope: TraceScope
  sessionId: OpaqueId
  kind: RunKind
  runId: OpaqueId
  traceId: string
  rootSpanId: string
  rootProducerId: OpaqueId
  rootAgentId: OpaqueId
  initiator?: { kind: 'user' | 'workload'; id: OpaqueId }
  sessionStartedAt: string
  registeredAt: string
  producerGrants: readonly { producerId: OpaqueId; agentId: OpaqueId }[]
  acceptUntil: string
  taskId?: OpaqueId
  correlationId?: OpaqueId
}

/** 由宿主的可信入口实现。工作负载配置限制哪些入口可以调用。 */
export interface RunRegistration {
  register(run: VerifiedRun): Promise<void>
  waitUntilRegistered(run: Pick<VerifiedRun, 'scope' | 'traceId'>): Promise<void>
}

export interface RunBinding {
  run: VerifiedRun
  activeAgentId: OpaqueId
  associationCredential: string
}

export type PrepareRunResult =
  | { outcome: 'started'; binding: RunBinding }
  | { outcome: 'already_bound' }

export interface HandoffTarget {
  agentId: OpaqueId
  producerId: OpaqueId
}

/** 只通过平台内部受信分派传递。 */
export interface Handoff {
  readonly carrier: Readonly<Record<string, string>>
  readonly associationCredential: string
}

export interface TrustedTracingHost {
  prepareRun(
    options: RunOptions,
    ids: Pick<VerifiedRun, 'traceId' | 'rootSpanId' | 'registeredAt'>
  ): Promise<PrepareRunResult>
  resumeHandoff(options: RunOptions): Promise<Handoff>
  issueHandoff(
    binding: RunBinding,
    carrier: Readonly<Record<string, string>>,
    target: HandoffTarget
  ): Promise<Handoff>
  verifyHandoff(handoff: Handoff): Promise<RunBinding>
  getGatewayAssociation(binding: RunBinding, agentId: OpaqueId): Promise<string>
  classifyError(error: unknown): {
    kind: 'timeout' | 'error' | 'cancelled'
    code?: string
  }
}

export interface RunSpan {
  context: TraceContext
  setOutcome(outcome: RunOutcome): void
}

export interface ModelObservation {
  context: TraceContext
  recordUsage(usage: { inputTokens?: number; outputTokens?: number }): void
}

export interface ToolObservation {
  context: TraceContext
  gatewayHeaders(destination: string): Promise<Readonly<Record<string, string>>>
  recordResponse(response: { statusCode?: number; callId?: OpaqueId }): void
}

export interface FlushResult {
  outcome: 'drained' | 'timed_out'
}

export interface TracingOptions {
  scope: TraceScope
  otlpTraceEndpoint: string
  getOtlpHeaders(): Promise<Readonly<Record<string, string>>>
  gatewayOrigins: readonly string[]
  host: TrustedTracingHost
  registration: RunRegistration
  maxBufferedSpans?: number
}

export interface TracingSdk {
  run<T>(options: RunOptions, operation: (span: RunSpan) => Promise<T>): Promise<T>
  handoff(parent: TraceContext, target: HandoffTarget): Promise<Handoff>
  continueRun<T>(handoff: Handoff, operation: (parent: TraceContext) => Promise<T>): Promise<T>
  withAgent<T>(parent: TraceContext, agentId: OpaqueId, operation: (span: TraceContext) => Promise<T>): Promise<T>
  withModel<T>(
    parent: TraceContext,
    options: { modelId: OpaqueId; maxOutputTokens?: number; temperature?: number },
    operation: (span: ModelObservation) => Promise<T>
  ): Promise<T>
  withTool<T>(
    parent: TraceContext,
    options: { toolId: OpaqueId; route: 'gateway' | 'local' | 'direct' },
    operation: (span: ToolObservation) => Promise<T>
  ): Promise<T>
  flush(options: { timeoutMs: number }): Promise<FlushResult>
  shutdown(options: { timeoutMs: number }): Promise<FlushResult>
}

export declare function createTracingSdk(options: TracingOptions): TracingSdk
```

`TraceScope`、会话起始时间、签名凭据及生产者授权来自宿主验证后的会话和工作负载上下文，通过 `TrustedTracingHost` 提供。业务调用方只传业务 ID，授权字段由宿主求得。业务代码把 `TraceContext` 和 `Handoff` 当作只读句柄，在同一次运行中使用。接入方法只接受本 SDK 实例生成的 TraceContext；跨进程通过 Handoff 验证并恢复。避免将凭据或 carrier 记入日志。

## 2. 运行、登记与传播

1. `run` 创建候选 OTel 根 Span，随后由宿主 `prepareRun` 在可信执行域按归属、Session、运行类别和运行 ID 原子绑定 Trace。成功绑定后才签发凭据。已存在绑定时返回 `TRACE_ALREADY_BOUND`，丢弃候选根且不启动第二个根回调。这个返回值只说明 Trace 已绑定。业务执行、幂等和故障恢复继续由宿主的既有业务状态机负责。
2. 宿主将原 Trace 绑定与已有业务分派关联后再执行回调。如果进程在绑定后、开始回调前退出，或绑定响应丢失，宿主先按自己的状态判断是否需要恢复。确认可恢复时，用 `resumeHandoff(options)` 取原 Trace 的授权分派，并从宿主持久化的原绑定继续未完成的登记，再用 `continueRun` 执行宿主允许的恢复回调。若原根 Span 未能导出，保持缺失根状态，可信入口可用内部运行状态事件补齐业务终态。
3. 不变的 `sessionStartedAt` 和 `registeredAt` 同时用于签发的业务关联和异步登记。SDK 调用 `registration.register`，重复登记使用相同绑定。登记包含根 Span ID、根生产者、根 Agent 和各项生产者授权。Tracing 登记暂时失败时，宿主已授权的分派和网关校验照常进行。SDK 异步重试登记，未确认的 Span 在本地有界缓冲中等待。
4. `handoff` 从当前 Span 注入 W3C Context，宿主按目标 Agent 和生产者签发受限的分派凭据，经可信内部通道交给目标工作负载。`continueRun` 先验证分派目标、过期时间和作用域，再检查 carrier 的 Trace ID 与凭据一致。无效分派在执行回调前返回校验错误。子工作负载以已验证的目标 Agent 为当前 Agent。续签由宿主按原 Trace 和授权范围处理，OTLP 凭据与业务关联凭据在各自通道使用。
5. 可信 Trace 登记成功后才允许 SDK 导出。远端工作负载通过 `waitUntilRegistered` 异步等待同一已验证 Trace。没有登记权限、登记永久失败或超过接收截止时丢弃待发送遥测并计数。根 Span 结束后，只要还在接收窗口内，已登记的子 Span 仍可抵达。采集故障不会重新运行回调。
6. `withAgent`、`withModel` 和 `withTool` 各自以传入上下文创建一个子 Span。并行任务共享同一父上下文。`withAgent` 切换当前 Agent，后续模型和工具 Span 继承它，接入仍核验生产者是否获准上报该 Agent。回调成功、抛出或取消时在 `finally` 中结束 Span，原有返回值与异常持续传给调用方。根运行默认以正常返回或异常推导终态，根回调可用 `setOutcome` 标记业务已失败但正常返回的情况。取消依据宿主确认。只有上报的 Span ID、生产者和执行 Agent 均与可信登记的 `rootSpanId`、`rootProducerId`、`rootAgentId` 匹配时，接入端才更新根运行终态。

SDK 使用标准上下文传播和 OTLP exporter。V1 配置完整的 Trace URL 并使用 HTTP Protobuf 作为 SDK 默认序列化，接收端按 OTLP/HTTP 契约处理。工作负载鉴权 Header 从宿主获取，凭据更新不改变已登记的生产者身份。 [OpenTelemetry Trace API](https://opentelemetry.io/docs/specs/otel/trace/api/) · [W3C Trace Context](https://www.w3.org/TR/trace-context/) · [OTLP exporter 配置](https://opentelemetry.io/docs/specs/otel/protocol/exporter/)

## 3. 工具 API 与网关响应

| 场景 | SDK 动作 | Tracing 展示 |
| --- | --- | --- |
| 经网关调用 | 用工具客户端 Span 注入 `traceparent`。应用在网关响应时把 `call_id` 和 HTTP 状态提供给 `recordResponse`。 | SDK 提供父节点、客户端耗时、响应状态或异常。网关提供独立审计。 |
| SDK 等待超时或连接失败 | 结束本次客户端 Span，记录受控结束类型，`call_id` 可为空。 | 展示客户端观测。网关有独立审计时照常显示网关节点，并按已有证据标记执行状态。 |
| 本地或直连工具 | 正常记录一个工具客户端 Span，保留内部工具 ID 和观察结果。 | 展示标记为 SDK 来源的客户端节点。 |

`gatewayHeaders(destination)` 只供 `route: 'gateway'` 使用。SDK 校验目标 origin 和 `gatewayOrigins` 精确匹配，并从宿主取得与当前 Agent 及调用主体绑定的有效凭据。Header 由工具客户端 Span 生成 `traceparent`，关联凭据放入拟定的 `x-dubhe-trace-binding`。工具客户端避免将这些 Header 带到跨 origin 重定向。SDK 重发 OTLP 批次时不触发工具业务重试。应用沿用原有业务鉴权、幂等键和确认流程。

**拟定的 HTTP 适配。** 由网关在已分配调用 ID 后的业务响应中设置 `x-dubhe-call-id`。SDK 通过工具客户端适配器读取，适用于正常响应、策略拒绝或待确认等已形成调用记录的响应。代理转发时保留该头。没有可信响应头时调用 ID 为空。其他消费方式可复用相同的业务语义，通过适配器把可信响应元数据转成 `recordResponse()`。

网关保存的 `traceparent.parent-id` 对应 SDK 工具客户端 Span 的 `span_id`。合并时同时核对归属、可信 Trace、执行 Agent、`call_id` 和该 Span ID。合并后的网关主节点再按 SDK Span 的 `parent_span_id` 挂树。如果首次响应丢失，后续客户端重试即使收到原 `call_id`，只要 Span ID 不匹配，也保留为单独的客户端尝试。SDK 的 HTTP 状态、超时及异常都属于客户端观测。

```ts
declare const tracing: TracingSdk
declare const gatewayUrl: string
declare const sessionId: OpaqueId
declare const turnId: OpaqueId
declare const rootAgentId: OpaqueId

export async function runTurn() {
  return tracing.run(
    { sessionId, kind: 'turn', runId: turnId, rootAgentId },
    async ({ context }) =>
      tracing.withTool(context, { toolId: 'customer.lookup', route: 'gateway' }, async tool => {
        const response = await fetch(gatewayUrl, {
          method: 'POST',
          redirect: 'error',
          headers: await tool.gatewayHeaders(gatewayUrl),
        })
        tool.recordResponse({
          statusCode: response.status,
          callId: response.headers.get('x-dubhe-call-id') ?? undefined,
        })
        return response
      })
  )
}
```

真实业务客户端按既有接口附加其认证和业务数据，并在 HTTP 错误映射成应用异常前先提取响应元数据。若已经记录响应，SDK 保留该客户端响应状态，同时可标记回调结束后的应用异常。本地和直连工具的记录仅采用客户端状态。模型适配器把已知 Token 计数传给 `recordUsage`。并行子 Agent 以同一个 `TraceContext` 分别调用 `withAgent`。

## 4. V1 Span 属性与逻辑映射

Dubhe 以 `dubhe.*` 属性传输业务语义。跨进程身份仍通过认证与可信登记校验。SDK 的值来自有限枚举或可信目录。任意文本、自定义属性、Prompt、输入输出原文、凭据和异常栈不进入 V1 Span。模型参数不可得时保持缺失。

| Span 属性 | 用途与类型 | Span 逻辑属性 |
| --- | --- | --- |
| OTLP `traceId/spanId/parentSpanId` | OTel 生成的运行、节点和父节点 ID | `trace_id/span_id/parent_span_id` |
| OTLP `startTimeUnixNano/endTimeUnixNano/status.code` | 起止时间和 OTel 状态 | `start_ns/end_ns/status` |
| `dubhe.span.kind` | `run`、`agent`、`llm`、`tool_client` | `kind` |
| `dubhe.agent.id` | 本 Span 声明的执行 Agent，入站须匹配生产者授权 | `agent_id` |
| `dubhe.run.outcome` | 根 Span 的显式终态 | `run_outcome` |
| `dubhe.model.id` | 可信目录模型 ID | `model_id` |
| `gen_ai.usage.input_tokens`、`gen_ai.usage.output_tokens` | 已知输入及输出计数 | `input_tokens/output_tokens` |
| `gen_ai.request.max_tokens`、`gen_ai.request.temperature` | 已知输出上限和采样参数 | `max_output_tokens/temperature` |
| `dubhe.tool.id` | 可信目录工具 ID | `tool_id` |
| `dubhe.tool.route` | `gateway`、`local`、`direct` | `tool_route` |
| `dubhe.tool.call_id` | 网关已回传的调用 ID | `client_call_id` |
| `dubhe.tool.client_result` | `response`、`timeout`、`error`、`cancelled` | `client_result` |
| `http.response.status_code` | 已接收 HTTP 状态码 | `response_code` |
| `dubhe.error.code` | SDK 错误适配器提供的固定分类 | `error_code` |

`producer_id` 和唯一作用域从认证求得，Session 从该范围的可信 Trace 登记求得。每个批次可混合此范围内的不同 Trace。传输层校验这些复合约束。V1 在受控导出路径配置固定字段集合，默认不发送自动插桩附加的原始 HTTP、数据库或模型属性。 [OpenTelemetry GenAI 属性](https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/)

## 5. 投递与验收

- 运行时采集与业务执行分开。缓冲满时丢弃新遥测并增加丢弃计数，SDK 的队列不调用网关，也不承担网关审计重放。
- 只有 OTLP 可重试失败或连接中断才退避重发同一批次。成功响应根据协议处理部分成功，整批不重发。服务端按作用域、Trace ID 和 Span ID 幂等持久化。SDK 记录接收失败、超过截止、队列丢弃及拒绝数量，指标按受控原因分类。
- `flush` 有调用方时限。队列排空表示本进程待处理记录已完成本地投递流程，包括失败终止或被拒绝的记录。它适合测试、短任务结束和应用退场。`shutdown` 停止接收新遥测再尽力排空，可重复调用。业务回调的行为继续由业务决定。
- 验收覆盖多轮根 Trace、并行和跨进程父子关系、在绑定后开始回调前退出的宿主恢复、登记故障和恢复、SDK 重传、网关拒绝却有调用 ID、无响应超时、仅本地调用、两路数据的不同到达顺序、错误作用域及禁采属性不出现在真实 OTLP 包中。 [OTLP/HTTP 规范](https://opentelemetry.io/docs/specs/otlp/#otlphttp)
