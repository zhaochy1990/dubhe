# 本地 Compose 使用分开的 Agent 和浏览器入口

本地 Compose 以 MySQL 8.4 命名卷保留 Tracing 数据，通过应用账号查询、Go HTTP 响应和鉴权 Session 查询依次验证服务就绪。Go 服务沿用现有配置和 schema 初始化。

Agent 在宿主机通过回环端口使用独立的登记和上报凭据。浏览器通过另一个回环端口访问 Nginx，以同源方式加载 Tracing UI 和查询 API。查询读凭据由 Nginx 在服务端附加，浏览器入口只接受本机主机名和读取请求。

发布镜像默认固定在已发布的 CalVer。Compose 可传入完整镜像引用，也可使用同一个 Dockerfile 从源码构建本机架构的镜像。
