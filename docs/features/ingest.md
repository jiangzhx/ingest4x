# ingest 能力

`/ingest` 是 `ingest4x` 最核心的收数能力，当前作为必选功能随默认构建一起编译。

## 提供的能力

- 注册 `POST /ingest`
- 注册 `GET /ingest`
- 通过 `cargo test` 运行 JLT 校验
- `/ingest` 相关 rules、normalization 和测试

## 请求处理流程

`POST /ingest` 和 `GET /ingest` 共用同一条 registry-backed 项目校验链路。

`POST /ingest` 的主链路是：

1. 解析 JSON 并读取 `appid` / `xwhat`
2. 通过 `ProjectRegistryState` 校验 `appid` 对应项目是否存在
3. 检查 payload 大小
4. 将原始请求写入 WAL，成功 ACK 的持久化边界由 WAL 决定
5. WAL replay 读取 record，执行 Rhai processor 和业务 rules
6. replay 按 processor emit 的目标写入 `[events.sink.*]`

`GET /ingest` 会先把 querystring 中的 `data` 做 base64 解码并解析成 JSON，再复用同一条处理链路，因此 `appid` 校验同样走 registry-backed 项目表。

## 依赖的运行时组件

- WAL
- Kafka 或 stdout sink
- `ProjectRepository`
- `ProjectRegistryState`
- `SQLite` / `MySQL`
- 数据库内置 seed ruleset

补充说明：

- 配置了 `[database]` 时，项目表主存储由连接串决定，支持 SQLite / MySQL，后台刷新任务会把已启用项目同步到内存 registry
- 运行时规则来自数据库；文件 rules 只作为测试 fixture 和 JLT 输入
- 未配置 `[database]` 时，会使用内存 SQLite 并导入内置示例项目 `APPID`，再复用同一套 registry 校验
- 事件输出由 Rhai processor 的 `emit(target, event)` 和 `[events.sink.*]` 配置决定

## 相关入口

- 路由注册：`src/server.rs`
- 接入面端口：`8090`，只承载 `/` 与 `/ingest`
- 管理面端口：`18090`，承载 `/metrics`、`/admin`、`/api/admin/*` 与 API 文档
- WAL 可靠写入语义：`docs/wal-reliable-write-v0.1.md`
- `GET/POST /ingest` handler：`src/ingest/endpoint.rs`
- 项目仓储：`src/repositories/projects.rs`
- 项目 registry：`src/services/project.rs`
- 第一版管理 API：`src/admin/projects.rs`
- 默认规则 seed：`src/db/seed.rs`
- JLT 用例：`tests/jlt/core`

## 常用命令

编译：

```bash
cargo build --release
```

运行 seed + JLT 测试：

```bash
cargo test --test ingest ingest_jlt_cases_match_rules
```

只跑规则兼容测试：

```bash
cargo test --test ingest ingest_jlt_cases_match_rules
```
