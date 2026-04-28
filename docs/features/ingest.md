# ingest feature

`ingest` 是 `ingest4x` 最核心的收数能力。

## 提供的能力

- 注册 `POST /ingest`
- 注册 `GET /ingest`
- 通过 `cargo test` 运行 JLT 校验
- 启用 `/ingest` 相关 rules、normalization 和测试

如果你只是需要“收数”，通常只开这个 feature 就够了。

## 请求处理流程

`POST /ingest` 和 `GET /ingest` 共用同一条 registry-backed 项目校验链路。

`POST /ingest` 的主链路是：

1. 按 `xwhat` 选择 `rules.ingest` 规则做校验
2. 校验失败时，把原始 JSON 按 `events.invalid.routes` 写入事件 sink，并返回 `400`
3. 将 JSON 解析为 `Event`
4. 通过 `ProjectRegistryState` 校验 `appid` 对应项目是否存在
5. 对事件做 normalization
6. 将事件按 `events.valid.routes` 写入事件 sink

`GET /ingest` 会先把 querystring 中的 `data` 做 base64 解码并解析成 JSON，再复用同一条处理链路，因此 `appid` 校验同样走 registry-backed 项目表。

## 依赖的运行时组件

- Kafka
- `ProjectRepository`
- `ProjectRegistryState`
- `SQLite`
- 数据库内置 seed ruleset

补充说明：

- 配置了 `[database]` 时，项目表主存储是 `SQLite`，后台刷新任务会把已启用项目同步到内存 registry
- 运行时规则来自数据库；文件 rules 只作为测试 fixture 和 JLT 输入
- 未配置 `[database]` 时，会使用内存 SQLite 并导入内置示例项目 `APPID`，再复用同一套 registry 校验
- 事件输出由 `[events.sink.*]` 和 `events.valid/invalid.routes` 配置决定，Kafka、文件和 stdout 都是平级 sink

## 相关入口

- 路由注册：`src/server.rs`
- 接入面端口：`8090`，只承载 `/` 与 `/ingest`
- 管理面端口：`18090`，承载 `/metrics`、`/admin`、`/api/admin/*` 与 API 文档
- `POST /ingest` handler：`src/ingest/json.rs`
- `GET /ingest` handler：`src/ingest/query.rs`
- normalization：`src/ingest/normalize.rs`
- 项目仓储：`src/projects/repository.rs`
- 项目 registry：`src/projects/registry.rs`
- 第一版管理 API：`src/admin/projects.rs`
- 默认规则 seed：`src/db/seed.rs`
- JLT 用例：`tests/jlt/core`

## 常用命令

只编译 `ingest`：

```bash
cargo build --release --no-default-features --features ingest
```

只用 `ingest` 运行 seed + JLT 测试：

```bash
cargo test --no-default-features --features ingest --test test_ingest_rules_compat
```

只跑 `ingest` 相关测试：

```bash
cargo test --no-default-features --features ingest --test test_ingest_rules_compat
```
