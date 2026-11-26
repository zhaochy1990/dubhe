# Tracing 镜像使用 CalVer

沿用 Auth 和 Running 的共享 `calver-release` action。发布路径由 `.github/release-packages.json` 管理，版本格式为 `YYYY.M.N`，以 UTC 月份为准，每月从 `.1` 开始。首次发布从空的 `versions.json` 计算，已有提交 SHA 标签可继续用于原有镜像。

每次推送到 `master` 按顺序读取最新已发布版本，并用对应源码提交构建和冒烟测试。工作流将计划中的版本用于 ACR 镜像标签，推送成功后才把同一份计划记入 `versions.json`。OCI 镜像标签保留完整源码 SHA，发布摘要提供按 digest 拉取的地址。

TypeScript 和 Python 包各自维护版本。Tracing 的 PR 继续执行构建和冒烟测试，发布由 `master` 触发。
