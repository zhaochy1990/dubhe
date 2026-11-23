# V1 Tracing 外部上报使用 OTLP

V1 以鉴权的 OTLP/HTTP Trace 接口 `POST /v1/traces` 作为唯一外部上报入口。标准 OpenTelemetry exporter 即可接入，平台只需维护一套鉴权、解析和投递契约。OTLP/gRPC、其他遥测信号和厂商专用上报协议留待有接入需求时评估。

业务请求用 W3C Trace Context 传播追踪上下文。网关、确认服务和执行器的权威审计事件通过内部 Outbox 投影至查询视图。

[OTLP/HTTP 规范](https://opentelemetry.io/docs/specs/otlp/#otlphttp) · [Tracing 架构](../architecture/tracing-architecture.md)
