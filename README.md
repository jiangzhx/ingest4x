# ingest4x

`ingest4x` 是一个基于 Rust 的事件接收服务，`/ingest` 收数主链路是核心能力；对应规则定义、JLT 校验和事件 normalization 都围绕它展开。

## Quick Start

如果你现在只关心 `/ingest`，建议先从数据库 seed 测试和 `ingest4x.example.toml` 开始。

### 1. 先跑通数据库 seed + JLT 测试

直接运行：

```bash
cargo test --test ingest ingest_jlt_cases_match_rules
```

这条命令会创建内存 SQLite，执行内置 seed，再用 seed 出来的 ruleset 跑 `tests/jlt/core/*.jlt`，适合先确认：

- 数据库 ruleset 能被正确编译
- `.jlt` 用例格式是对的
- 仓库内置 JLT 与 seed 规则一致

### 2. 启动服务

如果你使用仓库根目录的默认配置，并且 `ingest4x.toml` 中的 `[database]` 指向一个可写的 SQLite 文件，就可以直接启动：

```bash
cargo build --release
./target/release/ingest4x
# 或者显式写成
./target/release/ingest4x server
```

这个模式下：

- `ingest4x.toml` 需要提供 `[database]`
- `/ingest` 的 `appid` 校验来自 SQLite-backed `ProjectRegistryState`
- 项目数据由 `SQLite -> ProjectRepository -> ProjectRegistryState` 加载，不再依赖 Redis lookup

### 3. 使用完整示例配置启动

`ingest4x.example.toml` 是完整示例配置：MySQL 存储项目和规则元数据，WAL 作为 ACK 持久化边界，Kafka 作为 replay 后的事件 sink。启动前先准备：

- MySQL 数据库：`CREATE DATABASE ingest4x CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;`
- Kafka topics：`ingest4x-events`、`ingest4x-events-error`
- 按本机环境修改 `database.url`、`events.sink.*.bootstrap_servers`、`management.admin_password` 和 `wal.dir`

```bash
cargo run --bin ingest4x -- \
  server -c ingest4x.example.toml
```

然后另开一个终端发送示例请求：

```bash
curl -X POST http://127.0.0.1:8090/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "appid": "APPID",
    "xwhat": "custom_event",
    "xcontext": {
      "installid": "iid-1",
      "os": "ios",
      "idfa": "idfa-1",
      "currencytype": "cny"
    }
  }'
```

这个模式下：

- 需要启动 MySQL 和 Kafka
- `appid` 校验来自 MySQL-backed `ProjectRegistryState`
- 请求成功返回只表示事件已经写入并持久化到 WAL
- 下游 Kafka 投递由 WAL replay 负责
- `8090` 是接入面端口，只承载 `/` 与 `/ingest`

### 4. 理解 `/ingest` 的处理逻辑

`/ingest` 的主链路是：

1. 解析 JSON 并读取 `appid` / `xwhat`
2. 用 registry 校验 `appid` 对应项目是否存在
3. 检查 payload 大小
4. 将原始请求写入 WAL，`no_sync = false` 时等待 WAL 持久化后 ACK
5. 后台 WAL replay 读取 record，执行 Rhai processor 和业务 rules
6. replay 按 processor emit 的目标写入 `[events.sink.*]`，例如 Kafka 或 stdout

### 5. 管理后台与 API 文档

服务启动后会拆成两个 Actix Web 端口：

- `8090`：接入面，只承载 `/` 与 `/ingest`
- `18090`：管理面，承载 `/metrics`、`/admin`、`/api/admin/*` 与 API 文档

管理后台入口为 `http://127.0.0.1:18090/admin`，服务启动后可以直接从这里进入第一版管理界面。

本地前端开发方式：

```bash
cd web/admin
npm install
npm run dev
```

前端检查方式：

```bash
npm run test
npm run check
```

管理员密码默认来自环境变量 `INGEST4X_ADMIN_PASSWORD`。如果没有设置这个环境变量，只有配置 `management.allow_default_admin_password = true` 时管理 API 才会接受内置默认密码。

当前前端登录是轻量方案：密码只保存在当前页面内存中，刷新后需要重新登录。

管理后台相关接口与文档入口包括：

- Metrics：`http://127.0.0.1:18090/metrics`
- 项目管理 API：`http://127.0.0.1:18090/api/admin/projects`
- OpenAPI JSON：`http://127.0.0.1:18090/api-docs/openapi.json`
- Swagger UI：`http://127.0.0.1:18090/swagger-ui/`

这些入口都由 `Actix Web` 直接挂载和托管。

## 文档索引

### Feature

- [ingest feature](docs/features/ingest.md)
- [ingest4x 下载与发布](docs/ingest4x-release.md)

### Rules / JLT

- [Rules 字段语义](docs/rules-fields.md)
- [JLT 格式](docs/jlt-format.md)

### 维护

- [版本升级](docs/versioning.md)
