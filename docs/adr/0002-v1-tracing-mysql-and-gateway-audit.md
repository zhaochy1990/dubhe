# V1 Tracing 直写 MySQL 并异步接收网关审计

Tracing 直接用 MySQL InnoDB 保存结构化 Span、会话及工具节点。OTLP 接入在同步事务提交后确认。每个租户、环境和项目范围内以 `span_id` 标识 Span 并去重，`trace_id` 关联所属 Trace。库表授权由 MySQL 管理，行级访问范围由 Tracing 数据访问层校验。试点先复用同一个存储引擎完成写入和查询，避免引入原始报文暂存和消息队列。Span 与工具调用的内容采集范围单独定义。 [V1 内容采集](0003-v1-tracing-call-content.md)

Go 服务通过 GORM 在启动时选择数据库。本地开发和 CI 可以用 SQLite 文件执行同一套 Tracing HTTP API 与事务逻辑。服务部署可选择 MySQL InnoDB，并沿用现有表结构。

Tooling Gateway 持有决策、确认和执行的审计事实，在执行侧事务内写入 Outbox。Tracing 后台消费者异步生成受管工具节点，SDK 通过 OTLP 补充客户端耗时和调用拓扑。两边独立部署和管理数据，网关执行无需依赖 Tracing 的服务或数据库。

V1 网关每个 `call_id` 最多转发下游一次。执行客户端和受控代理关闭自动重试及会再次发送请求的自动重定向。转发前提交 `ToolCall.execution_state` 的条件更新、执行截止时间及转发意图审计事件。同键重传从共享记录返回原调用 ID 和持久化状态。超时且仍在执行中的调用转为未知，执行侧按调用 ID 或首次转发前绑定的下游请求引用核对证据。提交转发意图后，恢复和对账只更新结果。

[Tracing 架构](../architecture/tracing-architecture.md) · [V1 OTLP 接入](0001-v1-otlp-trace-ingest.md)
