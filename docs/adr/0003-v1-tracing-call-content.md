# V1 Tracing 采集调用内容

V1 支持在项目和环境中显式开启调用内容采集。SDK 按 Span 类型上报实际模型请求中的 System Prompt、User Prompt、模型名和采样参数。Tracing 归一化为 `Span.metadata` JSON。网关在 `ToolCall.parameters` JSON 中提供用于策略判定的业务参数快照。这样可以复盘模型输入和网关实际判定的参数。

内容详情单独授权，按配置留存且不超过 Trace 的期限。列表、聚合及诊断日志使用结构化摘要。内容到期后清除，重传不恢复已清除的内容。延长保留执行证据时沿用最小记录范围。模型输入中的历史消息可能包含前序模型回复和工具结果。当前调用的独立输出、原始 OTLP 报文和认证头不采集。实时脱敏和按租户或 Agent 限流留到后续评估。

[Tracing 架构](../architecture/tracing-architecture.md) · [SDK API](../architecture/tracing-sdk.md)
