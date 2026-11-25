# Agent Tracing SDK API 设计

V1 设计草案 · 2026-11-24

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

export interface ModelInputMessage {
  role: 'system' | 'developer' | 'user' | 'assistant' | 'tool'
  content: string
}

export interface ModelRequestMetadata {
  modelName?: string
  temperature?: number
  systemInstructions?: readonly string[]
  messages?: readonly ModelInputMessage[]
}

export interface ModelObservation {
  context: TraceContext
  recordRequest(request: ModelRequestMetadata): void
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
  captureModelInputs?: boolean
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

## 3. LangChain 与 LangGraph 接入

在应用启动时创建一次 `LangChainTracing`。每轮由可信请求入口调用 `prepare_config(turn, config=...)`（Python）或 `prepareConfig(turn, existingConfig?)`（TypeScript）。它复用核心 SDK 的原子绑定机制，在调用框架之前完成可信绑定，异步登记 Trace。入参包含已校验的 Session、轮次和根 Agent 标识。适配器返回本轮专用的原生框架配置，把 Dubhe handler 加入已有 callbacks，并保留 `thread_id`、metadata 和其他配置。准备失败沿用核心 SDK 的错误语义。每次框架执行使用一份配置。

handler 使用框架的运行 ID 和父运行 ID 关联事件，将本轮最外层运行映射到已绑定的根 Span。模型和本地工具生成子 Span。受管工具的 HTTP 发送由共享网关客户端创建客户端 Span、传播上下文并读取网关 `call_id`，handler 传递父上下文，同一次发送只记录一个工具客户端 Span。模型适配器可补充框架事件未提供的实际供应商请求字段，内容采集沿用项目策略。

流式路径沿用原生异步迭代器。准备配置时由核心 SDK 创建候选根 Span 并绑定其 ID。框架开始执行时，handler 接管同一个 Span。流初始化返回时根 Span 仍保持打开。请求入口在断连时关闭迭代器或传递取消信号，并在退出时调用幂等的 `finish(config)`，释放该轮配置持有的状态。TypeScript handler 在终态回调中记下框架结果与结束时刻，`finish(config)` 优先采用宿主确认的结果，并按已记录的时刻结束根 Span。模型服务异常按失败记录。提前关闭时若宿主未确认结果，运行终态保持缺失。业务输出按原生事件逐块交付，OTLP 后台批量发送已结束的 Span。已准备但未启动的流按核心 SDK 的绑定恢复规则处理，候选根 Span 不导出。

[LangChain callback 接入参考](https://langfuse.com/integrations/frameworks/langchain) · [Python 流式调用](https://docs.langchain.com/oss/python/langchain/streaming) · [TypeScript 流式调用](https://docs.langchain.com/oss/javascript/langchain/streaming)

### Python

启动时使用 `dubhe` 初始化 SDK。示例中的 `agent` 可以是现有 Agent 或已编译的 Graph，`inputs` 采用它自己的输入格式。

```python
from contextlib import aclosing

from dubhe import create_tracing_sdk
from dubhe.langchain import LangChainTracing

langchain_tracing = LangChainTracing(create_tracing_sdk(tracing_options))

async def invoke_turn(turn, inputs, existing_config=None):
    config = await langchain_tracing.prepare_config(turn, config=existing_config)
    try:
        return await agent.ainvoke(inputs, config=config)
    finally:
        langchain_tracing.finish(config)

async def stream_turn(turn, inputs, send_chunk, existing_config=None):
    config = await langchain_tracing.prepare_config(turn, config=existing_config)
    try:
        async with aclosing(agent.astream(inputs, config=config, stream_mode="messages")) as chunks:
            async for chunk in chunks:
                await send_chunk(chunk)
    finally:
        langchain_tracing.finish(config)
```

### TypeScript

```ts
import { createTracingSdk } from '@lipaotech/dubhe'
import { LangChainTracing } from '@lipaotech/dubhe/langchain'

const langchainTracing = new LangChainTracing(createTracingSdk(tracingOptions))

async function invokeTurn(turn: VerifiedTurn, inputs: AgentInputs, existingConfig?: RunnableConfig) {
  const config = await langchainTracing.prepareConfig(turn, existingConfig)
  try {
    return await agent.invoke(inputs, config)
  } finally {
    langchainTracing.finish(config)
  }
}

async function streamTurn(
  turn: VerifiedTurn,
  inputs: AgentInputs,
  sendChunk: (chunk: unknown) => Promise<void>,
  existingConfig: RunnableConfig,
  signal: AbortSignal,
) {
  const config = await langchainTracing.prepareConfig(turn, { ...existingConfig, signal })
  try {
    const chunks = await agent.stream(inputs, { ...config, streamMode: 'messages' })
    for await (const chunk of chunks) {
      await sendChunk(chunk)
    }
  } finally {
    langchainTracing.finish(config)
  }
}
```

`VerifiedTurn`、`AgentInputs` 和 `RunnableConfig` 分别表示应用已验证的轮次类型、现有 Agent/Graph 的输入类型和框架配置类型。示例中的 SDK 入口为拟议接口。流式路径的 `send_chunk` / `sendChunk` 由应用实现，逐块发送可展示内容。TypeScript 的 `signal` 由宿主在客户端断连时触发。

## 4. 工具 API 与网关响应

| 场景 | SDK 动作 | Tracing 展示 |
| --- | --- | --- |
| 经网关调用 | 用工具客户端 Span 注入 `traceparent`。应用在网关响应时把 `call_id` 和 HTTP 状态提供给 `recordResponse`。 | SDK 提供父节点、客户端耗时、响应状态或异常。网关提供独立审计。 |
| SDK 等待超时或连接失败 | 结束本次客户端 Span，记录受控结束类型，`call_id` 可为空。 | 展示客户端观测。网关有独立审计时照常显示网关节点，并按已有证据标记执行状态。 |
| 本地或直连工具 | 正常记录一个工具客户端 Span，保留内部工具 ID 和观察结果。 | 展示标记为 SDK 来源的客户端节点。 |

`gatewayHeaders(destination)` 只供 `route: 'gateway'` 使用。SDK 校验目标 origin 和 `gatewayOrigins` 精确匹配，并从宿主取得与当前 Agent 及调用主体绑定的有效凭据。Header 由工具客户端 Span 生成 `traceparent`，关联凭据放入拟定的 `x-dubhe-trace-binding`。工具客户端避免将这些 Header 带到跨 origin 重定向。SDK 重发 OTLP 批次时不触发工具业务重试。应用沿用原有业务鉴权、幂等键和确认流程。

**拟定的 HTTP 适配。** 由网关在已分配调用 ID 后的业务响应中设置 `x-dubhe-call-id`。SDK 通过工具客户端适配器读取，适用于正常响应、策略拒绝或待确认等已形成调用记录的响应。代理转发时保留该头。没有可信响应头时调用 ID 为空。其他消费方式可复用相同的业务语义，通过适配器把可信响应元数据转成 `recordResponse()`。

网关从已验证的 `traceparent.parent-id` 获取 `ToolCall.span_id`。SDK 把响应中的调用 ID 记在工具 Span 的 `metadata.client_call_id`。合并时核对归属、可信 Trace、执行 Agent、`call_id` 和 SDK Span ID。匹配后的网关主节点按 SDK Span 的 `parent_span_id` 挂树。同一幂等键的请求重试沿用原 `call_id`，重新创建客户端工具 Span 时则生成新的 `span_id`。如果重试拿回原 `call_id` 而 Span ID 不匹配，仍显示为单独的客户端尝试。V1 网关每个 `call_id` 最多转发一次。同键重传返回已持久化的调用状态。网关在转发前同一事务更新 `ToolCall.execution_state` 并写入转发意图的审计事件。

OpenTelemetry 建议为每次实际发送 HTTP 请求创建独立的客户端 Span。因此按尝试埋点时，每次业务重试都会生成新的 `span_id`。HTTP 客户端未暴露单次尝试时，规范允许使用一个外层客户端 Span。OTLP 导出重试发送同一批已完成的 Span，沿用原 ID。网关的 `call_id` 和幂等规则属于业务协议。 [HTTP Span 规范](https://opentelemetry.io/docs/specs/semconv/http/http-spans/#http-client-span) · [OTLP 重试规范](https://opentelemetry.io/docs/specs/otlp/#all-other-responses)

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

真实业务客户端按既有接口附加其认证和业务数据，并在 HTTP 错误映射成应用异常前先提取响应元数据。若已经记录响应，SDK 保留该客户端响应状态，同时可标记回调结束后的应用异常。本地和直连工具的记录采用客户端状态。网关工具的 `ToolCall.parameters` 由执行侧审计提供。并行子 Agent 以同一个 `TraceContext` 分别调用 `withAgent`。

模型适配器在发出请求前用 `recordRequest` 传入最终模型名、采样参数和实际输入。单独传给模型的 System Prompt 放入 `systemInstructions`，消息中的 System Prompt、User Prompt 和历史消息按发送顺序记录到 `messages`。适配器用 `recordUsage` 写入已知 Token 数。SDK 的 `captureModelInputs` 关闭时不缓存或发送原文。接入端按项目和环境策略独立校验，关闭时也立即停止返回原文详情。接入端根据可信目录验证请求模型名并保存规范名，无法验证的名称不写入摘要。

## 5. V1 Span 属性与逻辑映射

Dubhe 用 `dubhe.*` 属性传输自己的业务语义。跨进程身份通过认证与可信登记校验。结构化字段来自允许的枚举或目录。已开启采集的模型输入只通过指定内容属性上报，实际发出的历史消息可能包含前序模型回复和工具结果。当前调用的独立输出、认证头、异常栈及任意自定义属性不采集。模型参数不可得时保持缺失。

| Span 属性 | 用途与类型 | Span 逻辑属性 |
| --- | --- | --- |
| OTLP `traceId/spanId/parentSpanId` | OTel 生成的运行、节点和父节点 ID | `trace_id/span_id/parent_span_id` |
| OTLP `startTimeUnixNano/endTimeUnixNano/status.code` | 起止时间和 OTel 状态 | `start_ns/end_ns/status` |
| `dubhe.span.kind` | `run`、`agent`、`llm`、`tool_client` | `kind` |
| `dubhe.agent.id` | 本 Span 声明的执行 Agent，入站须匹配生产者授权 | `agent_id` |
| `dubhe.run.outcome` | 根 Span 的显式终态 | `metadata.run_outcome` |
| `dubhe.model.id`、`gen_ai.request.model` | 可信目录模型 ID、与目录匹配的最终请求模型名 | `metadata.model_id`、`metadata.model_name` |
| `gen_ai.usage.input_tokens`、`gen_ai.usage.output_tokens` | 已知输入及输出计数 | `metadata.input_tokens`、`metadata.output_tokens` |
| `gen_ai.request.max_tokens`、`gen_ai.request.temperature` | 已知输出上限和采样参数 | `metadata.max_output_tokens`、`metadata.temperature` |
| `gen_ai.system_instructions`、`gen_ai.input.messages` | 独立的 System Prompt 和按顺序发送的输入消息 | `metadata.system_instructions`、`metadata.messages` |
| `gen_ai.tool.definitions` | 供应商请求中暴露的函数定义 | `metadata.tool_definitions` |
| `gen_ai.request.reasoning.effort` | 供应商明确暴露的推理强度 | `metadata.reasoning_effort` |
| `dubhe.tool.id` | 可信目录工具 ID | `metadata.tool_id` |
| `dubhe.tool.route` | `gateway`、`local`、`direct` | `metadata.tool_route` |
| `dubhe.tool.call_id` | 网关已回传的调用 ID，仅用于 SDK 工具 Span | `metadata.client_call_id` |
| `dubhe.tool.client_result` | `response`、`timeout`、`error`、`cancelled` | `metadata.client_result` |
| `http.response.status_code` | 已接收 HTTP 状态码 | `metadata.response_code` |
| `dubhe.error.code` | SDK 错误适配器提供的固定分类 | `metadata.error_code` |

`producer_id` 和唯一作用域从认证求得，Session 从该范围的可信 Trace 登记求得。每个批次可混合此范围内的不同 Trace。`metadata` 是 Tracing 归一化后的逻辑 JSON。OTLP 仍分属性传输，模型输入适配为 GenAI 约定的内容结构。语言 SDK 缺少结构化属性能力时可按约定使用 JSON 字符串，接入端解析并校验后写入相同结构。 [OpenTelemetry GenAI 属性](https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/)

## 6. 投递与验收

- 运行时采集与业务执行分开。缓冲满时丢弃新遥测并增加丢弃计数，SDK 的队列不调用网关，也不承担网关审计重放。
- TypeScript SDK 的待登记任务数最多为 `maxBufferedSpans`。轮次关闭且没有待发送 Span 时取消其待登记任务。自定义 `RunRegistration` 需要响应传入的 `AbortSignal`。已缓冲的 Span 继续等待原轮次绑定登记成功后再导出。
- 只有 OTLP 可重试失败或连接中断才退避重发同一批次。成功响应根据协议处理部分成功，整批不重发。服务端在已验证的租户、环境和项目范围内按 `span_id` 幂等持久化，并校验重传数据的 `trace_id` 与原归属一致。SDK 记录接收失败、超过截止、队列丢弃及拒绝数量，指标按受控原因分类。
- `flush` 有调用方时限。队列排空表示本进程待处理记录已完成本地投递流程，包括失败终止或被拒绝的记录。它适合测试、短任务结束和应用退场。`shutdown` 停止接收新遥测再尽力排空，可重复调用。业务回调的行为继续由业务决定。
- LangChain 和 LangGraph 验收覆盖已有 callback 和 checkpoint 配置保留、普通调用成功和异常、首块早于流完成送达、流中途报错、断连和提前关闭，以及 OTLP 暂时不可用时业务流继续输出。每轮根 Span 只结束一次。
- 验收覆盖多轮根 Trace、并行和跨进程父子关系、在绑定后开始回调前退出的宿主恢复、登记故障和恢复、SDK 重传、网关拒绝却有调用 ID、无响应超时、仅本地调用、两路数据的不同到达顺序、错误作用域及禁采属性不出现在真实 OTLP 包中。 [OTLP/HTTP 规范](https://opentelemetry.io/docs/specs/otlp/#otlphttp)
