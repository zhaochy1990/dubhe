# Dubhe

Dubhe 为 LangChain Agent 提供 Tracing。TypeScript SDK `@lipaotech/dubhe` 通过 LangChain callback 记录运行、模型、子 Agent 和工具客户端 Span。Go 服务接收 OTLP/HTTP 数据，在数据库事务提交后确认，并按租户、环境、项目及 Agent 授权查询。服务支持 MySQL InnoDB 和本地 SQLite 文件。

## TypeScript 和 Go 端到端验收

准备 Node.js 24、Go 1.26 或更新版本和 C 编译器（SQLite 驱动使用 CGO）。验收入口在仓库根目录。

```sh
./scripts/test-tracing-v1.sh
```

脚本先执行 Go API 测试，再用临时 SQLite 文件启动真实 Go HTTP 服务、构建 TypeScript 包并执行端到端验收。模型由测试控制，无需供应商凭据。Go 测试通过 `t.TempDir()` 清理各自的数据库，脚本退出时删除端到端验收的临时文件。CI 可以直接执行 `go test ./...` 或运行完整脚本。可通过 `GO` 指定 Go 工具链路径。设置 `DUBHE_TEST_MYSQL_DSN` 可在专用的 MySQL 8.4 测试库运行同一组 TypeScript 端到端用例。

### 接入 TypeScript Agent

在应用启动时创建一次 SDK。可信宿主的 `prepareRun` 按作用域、Session 和轮次原子认领 Trace，并持久保存绑定。SDK 使用独立的登记身份和上报身份，在可信登记提交后发送已结束的 Span。

```typescript
import { createHttpRunRegistration, createTracingSdk } from "@lipaotech/dubhe";
import { LangChainTracing } from "@lipaotech/dubhe/langchain";

const sdk = createTracingSdk({
  scope,
  producerId,
  otlpTraceEndpoint: `${tracingUrl}/v1/traces`,
  getOtlpHeaders: async () => ({ "X-API-Key": workloadAPIKey }),
  host: trustedHost,
  registration: createHttpRunRegistration({
    url: `${tracingUrl}/internal/v1/observability/runs`,
    getHeaders: async () => ({ "X-API-Key": registrationAPIKey }),
  }),
});
const tracing = new LangChainTracing(sdk);

const config = await tracing.prepareConfig(verifiedTurn, existingConfig);
try {
  await agent.invoke(input, config);
} finally {
  tracing.finish(config);
}
```

流式调用沿用 `await agent.stream(input, { ...config, streamMode: "messages" })` 返回的异步迭代器。应用负责消费和关闭迭代器，退出时调用 `finish(config)`。提前关闭时由可信宿主确认轮次的取消结果。应用退出时可用 `await sdk.shutdown({ timeoutMs: 5_000 })` 排空遥测。

启用 `captureModelInputs` 后，SDK 记录 callback 可见的有序输入和独立系统指令。模型名由服务端作用域目录验证，其他模型字段取自供应商 callback 暴露的请求参数和实际用量。可信入口可登记 `VerifiedTurn.agentGrants` 和 `sessionInitiator`。用 `tracing.withAgent(config, agentId, childConfig => child.invoke(input, childConfig))` 记录已授权的子 Agent。通过 `LangChainTracing` 的 `localTools` 映射识别原生本地工具。网关或直连发送由共享客户端用 `tracing.withTool` 包装。网关客户端用 `gatewayHeaders` 注入已允许 origin 的绑定头，禁用自动重定向，并用 `recordResponse` 记录响应状态与网关回传的 `x-dubhe-call-id`。

### 运行 Go Tracing 服务

服务用 Viper 加载 YAML 配置，`--database-driver` 可在启动时选择 `sqlite` 或 `mysql`。[示例配置](config.example.yaml)

```sh
go run ./cmd/dubhe-tracing --config config.example.yaml --database-driver sqlite
go run ./cmd/dubhe-tracing --config config.yaml --database-driver mysql
```

数据库连接取自 `database.sqlite` 或 `database.mysql`。其中 `dsn` 用于写入，可选的 `read_dsn` 为查询配置同库的独立连接池。MySQL 查询账号可仅授予 `SELECT` 权限。配置优先级由高到低为命令行、环境变量和 YAML。可用 `DUBHE_DATABASE_DRIVER`、`DUBHE_LISTEN`、`DUBHE_SQLITE_DSN`、`DUBHE_MYSQL_DSN` 及对应的 `READ_DSN` 环境变量覆盖配置。`DUBHE_TRACING_AUTH` 仍接受旧版 JSON 鉴权配置。

YAML 的 `auth` 分别为 `workloads`、`registrars`、`gateways` 和 `readers` 配置独立的 `api_key`。请求使用 `X-API-Key`。各身份绑定 `scope`，写身份还绑定 `producer_id`，读身份包含 `agent_ids`。读身份可独立授予 `model_input` 和 `tool_parameters`。跨项目读者可配置 `grants`，每个条目独立定义作用域、Agent 范围和内容权限。多个查询实例可配置同一个 `tracing.cursor_signing_key`，以支持跨实例分页。

按作用域设置 `tracing.scopes[].content_days` 可启用内容保存。设置 `content_enabled_since` 可将延迟到达的登记按可信登记时间与策略生效时间求交。内容在到期或关闭采集时停止展示，并由服务清理。会话列表和 Trace 拓扑返回结构化摘要。Agent 展示名、模型名和 SDK 工具标识来自作用域目录。

旧版 YAML 的 `token` 字段和 `DUBHE_TRACING_AUTH` 中的凭据继续接受 `Authorization: Bearer`。Go 的 `authenticator` 接口负责把请求凭据解析后的值映射为指定角色的身份，供后续接入 OAuth 2.0。[OAuth 2.0 实现票 #10](https://github.com/zhaochy1990/dubhe/issues/10)

- `POST /internal/v1/observability/runs` 接受可信入口使用独立内部凭据提交的轮次绑定。
- `POST /v1/traces` 接受以工作负载身份鉴权的 OTLP/HTTP 二进制 Protobuf 批次，支持 gzip。
- `POST /internal/v1/observability/gateway-events` 接收网关 Outbox 的可信事件，提交后返回 204。网关发送每个调用的唯一 `event_id`、`call_id`、从可信绑定获得的 `trace_id`、已验证的可选 `span_id`、`agent_id`、`tool_id`、`sequence`、`type`、`occurred_at`、`decision`、`execution_state`、`policy_version` 和 `initiator`。首条决策可携带用于判定的 `parameters`。登记或前序事件尚未到达时返回可重试的 503。
- `GET /v1/observability/tool-calls/{call_id}/trace` 返回可见网关调用的 Trace 和节点标识。
- `GET /v1/observability/sessions` 返回会话、总数、Agent 选项和时间分布，支持 `from`、`to`、`agent_id`、`result`、`q`、`tz`、`limit` 和 `cursor`。时间参数使用 RFC 3339。
- `GET /v1/observability/sessions/{session_id}/traces` 返回多轮 Trace 摘要，支持 `limit` 和 `cursor`。
- `GET /v1/observability/traces/{trace_id}` 返回 Trace 及其节点。节点支持 `limit` 和 `cursor`，并标记当前页尚未加载的可见父节点。
- `GET /v1/observability/nodes/{node_id}` 返回 `span:{span_id}` 或 `tool:{call_id}` 的详情，可带 `expected_trace_id` 确认 Trace 归属。

设置 `tracing.gateway_outboxes` 可拉取网关 Outbox。每个来源配置 `url`、`api_key`、`scope` 和 `source_id`。Tracing 向 `{url}/lease` 提交 `{"limit":32}`。网关返回 204 或包含 `deliveries` 的 JSON，每项包含 `lease_token`、`version: 1` 和 `event`。Tracing 提交投影后向 `{url}/ack` 发送 `{"lease_token":"…"}`，网关用 204 确认。只有网关观测时，首条事件可通过 `gateway_binding` 提供经过验证的 Session 与 Trace 登记时间。

查询使用 `X-API-Key` 发送具有作用域和 Agent 权限的读身份凭据。详情和会话下的 Trace 列表须提供 `tenant_id`、`environment_id` 和 `project_id`。会话列表可直接使用读身份的完整作用域。

## Python 参考实现

### 本地验证

需要 Python 3.11 或更新版本、[uv](https://docs.astral.sh/uv/) 和 Docker。以下命令启动本地 MySQL 8.4，然后运行端到端测试。模型使用固定响应，无需供应商凭据。

```sh
docker run -d --rm --name dubhe-mysql -p 127.0.0.1:3306:3306 \
  -e MYSQL_ROOT_PASSWORD=local-root -e MYSQL_DATABASE=dubhe \
  -e MYSQL_USER=dubhe -e MYSQL_PASSWORD=dubhe mysql:8.4

# MySQL 就绪时返回 1。
docker exec dubhe-mysql mysql -h 127.0.0.1 -udubhe -pdubhe dubhe -N -e 'SELECT 1'
uv run --locked --all-extras pytest -q
```

可以用 `DUBHE_TEST_MYSQL_HOST`、`DUBHE_TEST_MYSQL_PORT`、`DUBHE_TEST_MYSQL_USER`、`DUBHE_TEST_MYSQL_PASSWORD` 和 `DUBHE_TEST_MYSQL_DATABASE` 指定专用测试库。测试在实际 HTTP 端口上启动接收和查询服务，验证现有 callback 和 LangGraph checkpoint、登记重试、普通调用的异常和取消、Trace 中的父子关系及跨作用域的查询结果。

### 接入原生调用

在应用启动时创建 `TracingSdk` 和 `LangChainTracing`。`prepare_run` 由可信宿主实现，按作用域、Session 和轮次原子认领 `VerifiedRun`，已有绑定时返回 `False`。`register_run` 接收同一份绑定并在后台登记。登记暂时不可用时，适配器抛出 `dubhe.TransientRegistrationError`，SDK 对同一绑定退避重试，每次退避等待最长 5 秒，并在接收截止或 SDK 停止排空时结束重试。`TraceStore.register_run` 会转换 MySQL 连接中断和事务锁竞争等暂时故障。SDK 在登记提交后才发送已结束的 Span。

```python
from dubhe import TracingOptions, create_tracing_sdk
from dubhe.langchain import LangChainTracing

sdk = create_tracing_sdk(TracingOptions(
    scope=trusted_scope,
    producer_id=trusted_producer_id,
    otlp_trace_endpoint="https://tracing.example.com/v1/traces",
    otlp_headers=workload_headers,
    prepare_run=trusted_host.prepare_run,
    register_run=trusted_registration.register_run,
))
tracing = LangChainTracing(sdk)


async def invoke_turn(agent, verified_turn, inputs, existing_config=None):
    config = await tracing.prepare_config(verified_turn, config=existing_config)
    try:
        return await agent.ainvoke(inputs, config=config)
    finally:
        tracing.finish(config)
```

`verified_turn` 由宿主根据已认证的会话和请求构建，Session 内各轮共用可信会话开始时间和根 Agent。应用退出时调用 `await sdk.shutdown(timeout_ms=5_000)`。在测试或短生命周期程序中，可以先 `await sdk.flush(timeout_ms=5_000)` 排空已结束 Span 的队列，再通过查询确认记录可见。

### 接收与查询

`dubhe.service.create_app(store, auth)` 构建 ASGI 应用，可用 Uvicorn 运行。通过 `TraceStore.initialize_schema()` 初始化 MySQL 表，然后用可信的内部适配器调用 `TraceStore.register_run()`。可以用 `query_store` 为查询配置独立的只读 MySQL 账号。

- `POST /v1/traces` 接收以工作负载身份鉴权的二进制 Protobuf 批次，支持 gzip。
- `GET /v1/observability/traces/{trace_id}` 返回 Trace 及其节点。节点支持 `limit` 和 `cursor`，并标记当前页尚未加载的可见父节点。
- `GET /v1/observability/nodes/span:{span_id}` 返回单个 Span，可带 `expected_trace_id` 约束 Trace 归属。

两个查询均需要读身份和 `tenant_id`、`environment_id`、`project_id` 查询参数。当前参考实现一次返回 Trace 的授权节点。SDK 的登记缓冲和 OTLP 批处理队列受 `max_buffered_spans` 限制，缓冲或队列溢出时可能丢弃 Span。

测试里的 `BearerAuth` 用分开的静态 Token 映射演示服务端工作负载身份及读取授权，测试的宿主使用进程内绑定。部署时由实际宿主提供可信的持久绑定，服务接入平台的身份校验与授权实现。

[Tracing 架构](docs/architecture/tracing-architecture.md) · [SDK 设计](docs/architecture/tracing-sdk.md)
