# V1 Tracing 直写 MySQL 并异步接收网关审计

Tracing 直接用 MySQL InnoDB 保存结构化 Span、会话及工具节点。OTLP 接入在同步事务提交后确认，重试按带租户、环境和项目范围的 Span 唯一键去重。库表授权由 MySQL 管理，行级访问范围由 Tracing 数据访问层校验。试点先复用同一个存储引擎完成写入和查询，避免引入原始报文暂存和消息队列。Span 与工具调用的内容采集范围单独定义。 [V1 内容采集](0003-v1-tracing-call-content.md)

Tooling Gateway 持有决策、确认和执行的审计事实，在执行侧事务内写入 Outbox。Tracing 后台消费者异步生成受管工具节点，SDK 通过 OTLP 补充客户端耗时和调用拓扑。两边独立部署和管理数据，网关执行无需依赖 Tracing 的服务或数据库。

[Tracing 架构](../architecture/tracing-architecture.md) · [V1 OTLP 接入](0001-v1-otlp-trace-ingest.md)
