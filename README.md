# Dubhe

Dubhe 为 LangChain Agent 提供 Tracing。TypeScript SDK `@lipaotech/dubhe` 通过 LangChain callback 记录运行和模型 Span。Go 服务接收 OTLP/HTTP 数据，在数据库事务提交后确认，并按租户、环境、项目及 Agent 授权查询。服务支持 MySQL InnoDB 和本地 SQLite 文件。

## TypeScript 和 Go 端到端验收

准备 Node.js 24、Go 1.26 或更新版本和 C 编译器（SQLite 驱动使用 CGO）。验收入口在仓库根目录。

```sh
./scripts/test-tracing-v1.sh
```

脚本先执行 Go API 测试，再用临时 SQLite 文件启动真实 Go HTTP 服务、构建 TypeScript 包并执行端到端验收。模型由测试控制，无需供应商凭据。Go 测试通过 `t.TempDir()` 清理各自的数据库，脚本退出时删除端到端验收的临时文件。CI 可以直接执行 `go test ./...` 或运行完整脚本。可通过 `GO` 指定 Go 工具链路径。

### 接入 TypeScript Agent

在应用启动时创建一次 SDK。可信宿主的 `prepareRun` 按作用域、Session 和轮次原子认领 Trace，并持久保存绑定。SDK 使用独立的登记身份和上报身份，在可信登记提交后发送已结束的 Span。

```typescript
import { createHttpRunRegistration, createTracingSdk } from "@lipaotech/dubhe";
import { LangChainTracing } from "@lipaotech/dubhe/langchain";

const sdk = createTracingSdk({
  scope,
  producerId,
  otlpTraceEndpoint: `${tracingUrl}/v1/traces`,
  getOtlpHeaders: getWorkloadHeaders,
  host: trustedHost,
  registration: createHttpRunRegistration({
    url: `${tracingUrl}/internal/v1/observability/runs`,
    getHeaders: getRegistrationHeaders,
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

### 运行 Go Tracing 服务

服务用 Viper 加载 YAML 配置，`--database-driver` 可在启动时选择 `sqlite` 或 `mysql`。[示例配置](config.example.yaml)

```sh
go run ./cmd/dubhe-tracing --config config.example.yaml --database-driver sqlite
go run ./cmd/dubhe-tracing --config config.yaml --database-driver mysql
```

数据库连接取自 `database.sqlite` 或 `database.mysql`。其中 `dsn` 用于写入，可选的 `read_dsn` 为查询配置同库的独立连接池。MySQL 查询账号可仅授予 `SELECT` 权限。配置优先级由高到低为命令行、环境变量和 YAML。可用 `DUBHE_DATABASE_DRIVER`、`DUBHE_LISTEN`、`DUBHE_SQLITE_DSN`、`DUBHE_MYSQL_DSN` 及对应的 `READ_DSN` 环境变量覆盖配置。`DUBHE_TRACING_AUTH` 仍接受旧版 JSON 鉴权配置。

YAML 的 `auth` 配置提供 `workloads`、`registrars` 和 `readers` 三类服务端身份。各身份绑定 `scope`，上报和登记身份还绑定 `producer_id`，读身份包含 `agent_ids`。将示例中的 Token 替换为自己的凭据。

- `POST /internal/v1/observability/runs` 接受可信入口使用独立内部凭据提交的轮次绑定。
- `POST /v1/traces` 接受以工作负载身份鉴权的 OTLP/HTTP 二进制 Protobuf 批次，支持 gzip。
- `GET /v1/observability/traces/{trace_id}` 返回 Trace 及其节点。
- `GET /v1/observability/nodes/span:{span_id}` 返回单个 Span，可带 `expected_trace_id` 确认 Trace 归属。

查询须提供 `tenant_id`、`environment_id` 和 `project_id` 参数，并使用有相应作用域和 Agent 权限的读身份。

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
- `GET /v1/observability/traces/{trace_id}` 返回 Trace 及其节点。
- `GET /v1/observability/nodes/span:{span_id}` 返回单个 Span，可带 `expected_trace_id` 约束 Trace 归属。

两个查询均需要读身份和 `tenant_id`、`environment_id`、`project_id` 查询参数。当前参考实现一次返回 Trace 的授权节点。SDK 的登记缓冲和 OTLP 批处理队列受 `max_buffered_spans` 限制，缓冲或队列溢出时可能丢弃 Span。

测试里的 `BearerAuth` 用分开的静态 Token 映射演示服务端工作负载身份及读取授权，测试的宿主使用进程内绑定。部署时由实际宿主提供可信的持久绑定，服务接入平台的身份校验与授权实现。

[Tracing 架构](docs/architecture/tracing-architecture.md) · [SDK 设计](docs/architecture/tracing-sdk.md)
