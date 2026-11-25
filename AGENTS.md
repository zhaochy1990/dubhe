## Agent skills

### Issue tracker

Read `docs/agents/issue-tracker.md` when reading, publishing, or updating work in GitHub Issues.

### Triage labels

Read `docs/agents/triage-labels.md` when querying or updating issue triage state.

### Domain docs

This repo uses a single context. Read `docs/agents/domain.md` before exploring domain terminology or architecture decisions.

## ER 图

绘制或修改 ER 图时，默认交付逻辑模型。参考 `docs/architecture/tracing-architecture.html#data-model` 中的「实体与主要属性」。

- 图的标题下用一句话说明业务范围。每个实体用一张卡片，标题写领域实体名，正文只列主要属性。左侧用 `PK` 标识键、`FK` 关联键，右侧用简短中文说明属性含义。
- 在关系线上写业务含义，两端都标基数。统一用 `1`、`0..1`、`0..*`、`1..*`，图底附键标和基数的图例。
- 优先直连，转弯用正交连线。检查桌面与窄屏，卡片、关系标注和图例须完整可读，窄屏可在图内横向滚动。
- 数据库字段类型、索引、完整字段字典和建表 SQL 按单独请求交付。
