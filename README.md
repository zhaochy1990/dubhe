# Dubhe

Dubhe 为 Python LangChain Agent 提供 Tracing。可信入口为已验证的轮次绑定 Trace，应用继续调用原生 `ainvoke()`。Dubhe 通过标准 OpenTelemetry OTLP/HTTP exporter 上报根运行和模型 Span，Tracing 将归一化记录提交到 MySQL InnoDB 后确认。Trace 和 Span 详情查询校验租户、环境、项目及 Agent 权限。

## 本地验证

需要 Python 3.11 或更新版本、[uv](https://docs.astral.sh/uv/) 和 Docker。以下命令启动本地 MySQL 8.4，然后运行端到端测试。模型使用固定响应，无需供应商凭据。

```sh
docker run -d --rm --name dubhe-mysql -p 127.0.0.1:3306:3306 \
  -e MYSQL_ROOT_PASSWORD=local-root -e MYSQL_DATABASE=dubhe \
  -e MYSQL_USER=dubhe -e MYSQL_PASSWORD=dubhe mysql:8.4

# MySQL 就绪时返回 1。
docker exec dubhe-mysql mysql -h 127.0.0.1 -udubhe -pdubhe dubhe -N -e 'SELECT 1'
uv run --locked --all-extras pytest -q
```

可以用 `DUBHE_TEST_MYSQL_HOST`、`DUBHE_TEST_MYSQL_PORT`、`DUBHE_TEST_MYSQL_USER`、`DUBHE_TEST_MYSQL_PASSWORD` 和 `DUBHE_TEST_MYSQL_DATABASE` 指定专用测试库。测试在实际 HTTP 端口上启动接收和查询服务，验证现有 callback、LangGraph checkpoint、Trace 中的父子关系及跨作用域的查询结果。

## 接入原生调用

在应用启动时创建 `TracingSdk` 和 `LangChainTracing`。`prepare_run` 由可信宿主实现，按作用域、Session 和轮次原子认领 `VerifiedRun`，已有绑定时返回 `False`。`register_run` 接收同一份绑定并在后台登记。SDK 在登记提交后才发送已结束的 Span。

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

## 接收与查询

`dubhe.service.create_app(store, auth)` 构建 ASGI 应用，可用 Uvicorn 运行。通过 `TraceStore.initialize_schema()` 初始化 MySQL 表，然后用可信的内部适配器调用 `TraceStore.register_run()`。可以用 `query_store` 为查询配置独立的只读 MySQL 账号。

- `POST /v1/traces` 接收以工作负载身份鉴权的二进制 Protobuf 批次，支持 gzip。
- `GET /v1/observability/traces/{trace_id}` 返回 Trace 及其节点。
- `GET /v1/observability/nodes/span:{span_id}` 返回单个 Span，可带 `expected_trace_id` 约束 Trace 归属。

两个查询均需要读身份和 `tenant_id`、`environment_id`、`project_id` 查询参数。测试里的 `BearerAuth` 用分开的静态 Token 映射演示服务端工作负载身份及读取授权，测试的宿主使用进程内绑定。部署时由实际宿主提供可信的持久绑定，服务接入平台的身份校验与授权实现。

[Tracing 架构](docs/architecture/tracing-architecture.md) · [SDK 设计](docs/architecture/tracing-sdk.md)
