# ingest4x

> **状态说明**
>
> 当前项目仍处于 `0.0.1` 早期版本，不建议直接用于生产环境。后续版本可能调整 WAL 文件格式和兼容策略，升级前需要关注 release notes 和迁移说明。

每次做数据类型的产品，几乎每个项目都会重新搭一遍类似的事件接入链路：Nginx 或 OpenResty 先收一层，Flume、Logstash、Filebeat 之类再把日志写到 Kafka 或文件，后面接 Flink、Spark 或自研任务处理，管理、监控、重试和规则配置又散在别的地方。`ingest4x` 想把这些常见但零散的环节收成一个独立工具。

它解决四类问题：

- 接得住：接入层只负责鉴权、限流式校验和可靠落盘，避免下游抖动直接影响上报成功率。
- 管得住：不同项目可以配置自己的事件校验规则、加工逻辑和投递目标。
- 送得到：事件先进入本地 WAL，再由后台任务重放处理；下游失败时可以重试，已处理位置按 sink 独立记录。
- 看得见：提供管理界面维护项目、规则、加工脚本和投递配置，并暴露完整 metrics 监控接入、WAL、replay 和投递状态。

因此，`/ingest` 的成功响应表示事件已经被接收并进入后续处理链路；事件是否合法、是否需要补充字段、最终送到哪里，都由项目配置决定。

## 项目概览

- HTTP 接入：`POST /ingest` 和 `GET /ingest?data=<base64-json>`。
- 项目鉴权：通过 `x-ingest-token` 或 `Authorization: Bearer <token>` 鉴权，token 来自已启用的项目。
- WAL：本地分段写入、checkpoint、按 sink replay、失败重试，详见 [WAL](docs/wal.md)。
- Rules：数据库内置 Rhai validation rule，支持按项目绑定 rule set。
- Processor：Rhai `process(event, request)`，通过 `validate(event)` 和 `emit(target, event)` 处理事件。
- Sinks：当前内置 `stdout` 和 `kafka` provider，运行时配置来自数据库。
- Admin：管理后台、OpenAPI、Swagger UI、Prometheus metrics、service node 注册和心跳。
- 存储：SQLite / MySQL，启动时自动执行 migration 和默认 seed。

### 运行模型

```text
+--------+
| Client |
+--------+
    |
    v
+--------------------------------------------------------------------------------+
| Ingest API                                                                     |
|                                                                                |
| +---------+    +------------------------+    +------------+    +---------+     |
| | /ingest | -> | Project token registry | -> | WAL append | -> | ACK 200 |     |
| +---------+    +------------------------+    +------------+    +---------+     |
+--------------------------------------------------------------------------------+
                                                   |
                                                   v
+--------------------------------------------------------------------------------+
| Replay worker                                                                  |
|                                                                                |
| +------------+                                                                 |
| | WAL replay |                                                                 |
| +------------+                                                                 |
|        |                                                                       |
|        v                                                                       |
| +--------------------+                                                         |
| | Load project rules |                                                         |
| +--------------------+                                                         |
|        |                                                                       |
|        v                                                                       |
| +-------------------------------------+                                        |
| | Run Rhai processor                  |                                        |
| | validate(event), emit(target,event) |                                        |
| +-------------------------------------+                                        |
|        |                                                                       |
|        v                                                                       |
| +----------------------+                                                       |
| | Processor deliveries |                                                       |
| +----------------------+                                                       |
+--------------------------------------------------------------------------------+
                                                   |
                                                   v
+--------------------------------------------------------------------------------+
| Sink delivery                                                                  |
|                                                                                |
| +-------------+    +---------------------+                                     |
| | Event sinks | -> | Checkpoint per sink |                                     |
| +-------------+    +---------------------+                                     |
+--------------------------------------------------------------------------------+
```

## 快速开始

### 1. 跑核心测试

```bash
cargo test --test ingest ingest_jlt_cases_match_rules
```

这条测试会用内存 SQLite 初始化默认 seed，再用 `tests/jlt/core/*.jlt` 校验默认规则，适合确认规则、JLT 和 seed 仍然一致。

更完整的本地验证：

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

### 2. 启动服务

仓库根目录的 `ingest4x.toml` 默认使用 SQLite 文件 `db/ingest4x.db` 和 WAL 目录 `./wal`：

```bash
cargo run --bin ingest4x
```

也可以显式指定配置：

```bash
cargo run --bin ingest4x -- server -c ingest4x.toml
```

默认端口：

| 端口 | 用途 |
| --- | --- |
| `8090` | 接入面，提供 `/`、`/ingest` |
| `18090` | 管理面，提供 `/healthz`、`/admin`、`/api/admin/*`、`/metrics`、OpenAPI 和 Swagger UI |

启动后 seed 会确保存在本地测试项目：

```text
project: test_app
ingest token: igx_local_test_token
```

管理后台入口：

```text
http://localhost:18090/admin/
```

默认管理员密码来自 `ingest4x.toml`：

```text
ingest4x
```

如果设置了环境变量 `INGEST4X_ADMIN_PASSWORD`，会优先使用环境变量里的密码。

### 3. 发送 POST 事件

```bash
curl -X POST http://127.0.0.1:8090/ingest \
  -H 'Content-Type: application/json' \
  -H 'x-ingest-token: igx_local_test_token' \
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

成功时响应体是：

```text
200
```

注意：项目鉴权只看 ingest token，不用 payload 里的 `appid` 做认证。`appid` 仍然是业务事件字段，默认 rules 会校验它是否存在。

### 4. 发送 GET 事件

`GET /ingest` 使用 querystring 里的 `data` 参数，内容是事件 JSON 的 base64：

```bash
DATA=$(
  printf '%s' '{"appid":"APPID","xwhat":"custom_event","xcontext":{"installid":"iid-1","os":"ios","idfa":"idfa-1"}}' \
    | base64 \
    | tr -d '\n'
)

curl "http://127.0.0.1:8090/ingest?data=$DATA" \
  -H 'x-ingest-token: igx_local_test_token'
```

## 请求语义

`/ingest` 当前只处理单事件 JSON object，不支持批量数组。

接入层会做这些检查：

1. 读取请求 payload。`POST` 使用 body，`GET` 使用 `data` query 参数并做 base64 解码。
2. 读取 ingest token。优先使用 `x-ingest-token`，也支持 `Authorization: Bearer <token>`。
3. 在内存项目 registry 中认证 token，只允许已启用项目。
4. 检查 payload 大小，默认 `256 KiB`。
5. 解析 JSON，并从 `xwhat` 读取事件名；缺失时内部事件名按 `default` 处理。
6. 写入 WAL。写入成功后返回 `200`。

常见失败响应：

| 场景 | HTTP |
| --- | --- |
| 缺少或非法 token | `401` |
| GET 缺少 `data` | `400` |
| base64 或 JSON 非法 | `400` |
| payload 超过 `ingest.max_event_bytes` | `413` |
| WAL 容量不足或不可写 | `503` |

接入 token 不会写入 WAL record 的 headers。

默认 processor：

```rhai
fn process(event, request) {
    let validation = validate(event);
    if validation["ok"] {
        emit(SINK_EVENTS, event);
    } else {
        emit(SINK_EVENTS_ERROR, event);
    }
}
```

默认会创建两个 stdout event sinks：

- `events`
- `events_error`

启动时也会创建一个 `Local Kafka` delivery target，指向 `127.0.0.1:9092`。如果要把事件投递到 Kafka，需要在管理后台或管理 API 中创建/启用对应的 event sink。

## 配置

最小配置结构：

```toml
[logging]
level = "info"
format = "json"

[ingest]
bind_address = "0.0.0.0:8090"
max_event_bytes = 262144

[management]
bind_address = "0.0.0.0:18090"
admin_password = "ingest4x"

[database]
url = "sqlite://db/ingest4x.db?mode=rwc"
refresh_interval_secs = 3

[wal]
dir = "./wal"
flush_max_interval = "10ms"
flush_max_records = 1000
no_sync = false
wal_segment_max_bytes = 134217728

[wal.checkpoint]
flush_interval = "1s"
flush_records = 1000
flush_bytes = 67108864
```

关键配置：

| 配置 | 说明 |
| --- | --- |
| `ingest.bind_address` | 接入面监听地址 |
| `ingest.max_event_bytes` | 单事件最大字节数 |
| `management.bind_address` | 管理面监听地址 |
| `management.admin_password` | 管理 API 密码；环境变量 `INGEST4X_ADMIN_PASSWORD` 优先级更高 |
| `database.url` | SQLite 或 MySQL 连接串 |
| `database.refresh_interval_secs` | 项目、sink、processor 刷新间隔 |
| `wal.dir` | WAL 数据目录 |
| `wal.no_sync` | `false` 表示按可靠写入语义等待持久化；`true` 是性能优先的弱可靠模式 |

`ingest4x.example.toml` 提供 MySQL + 本地 Kafka 的完整示例。

## 管理后台和 API

管理后台入口、认证方式、OpenAPI/Swagger UI 和管理资源列表见 [管理后台和 API](docs/admin-api.md)。

## 事件校验和加工

事件处理在 replay 阶段分成两步：

- 事件校验：`fn validate(event)`，负责校验事件字段。
- 事件加工：`fn process(event, request)`，负责调用校验规则、改写或补充事件并 `emit` 到 event sinks。

详细说明见 [事件处理](docs/event-processing/index.md)。

## WAL 和投递

WAL 的 ACK 语义、record 内容、segment、checkpoint、replay、清理和故障处理见 [WAL](docs/wal.md)。

## 前端开发

```bash
cd web/admin
npm install
npm run dev
```

前端检查：

```bash
npm run test
npm run check
```

生产服务会直接托管 `web/admin/dist`。如果需要更新内置管理后台，需要先构建前端产物。

## 发布和版本

版本升级和 GitHub Release 发布流程见 [发布和版本](docs/release-versioning.md)。

## 更多文档

- [WAL](docs/wal.md)
- [事件处理](docs/event-processing/index.md)
- [管理后台和 API](docs/admin-api.md)
- [发布和版本](docs/release-versioning.md)
- [项目结构](docs/project-structure.md)
