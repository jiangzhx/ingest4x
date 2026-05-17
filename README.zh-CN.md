# ingest4x

> **状态说明**
>
> 当前版本为 `0.0.1`，尚不建议直接用于生产环境。后续版本可能会改变 WAL 文件格式与兼容策略。升级前请先查看发布说明和迁移说明。

每个系统通常会把接入链路拆成多段：先有 Nginx/OpenResty，再经过 Flume/Logstash/Filebeat 写入 Kafka 或文件，再经过 Flink/Spark/自定义任务，监控与重试、规则配置又在其他系统中维护。`ingest4x` 的目标是把这些能力收敛为一个统一的服务。

它主要解决四类问题：

- 接入稳定性：在入口层处理鉴权、校验和持久化，减少下游抖动对接入成功率的直接影响。
- 可治理性：每个项目可自定义处理脚本逻辑和投递目标。
- 送达可靠性：事件先持久写入本地 WAL，再由后台重放 worker 投递，失败会重试，重放通过单个 pipeline checkpoint 推进。
- 可观测性：管理界面可配置项目、处理脚本和 sink，指标覆盖 ingest、WAL、重放和投递。

因此 `/ingest/{project_key}` 返回成功仅表示事件已被接入系统接收。是否字段合法、是否需要补充字段、以及最终送达哪里，取决于项目配置。

## 总体说明

`ingest4x` 通过事件 sink 将结果交付到下游。内置 sink 类型如下：

| Sink 类型 | 使用场景 | 主要配置 | 状态 |
| --- | --- | --- | --- |
| [`blackhole`](docs/zh-CN/sink-parameters.md#blackhole) | 丢弃事件，适用于生产/客户压测、容量验证与下游故障注入。 | 不需要 `delivery target`；`event sink` 支持 `mode` 与 `delay_ms`。 | 已支持 |
| [`kafka`](docs/zh-CN/sink-parameters.md#kafka) | 投递到 Kafka topic，适用于流处理与数据平台链路。 | `delivery target` 需要 `bootstrap_servers`；`event sink` 需要 `topic`。 | 已支持 |
| [`parquet`](docs/zh-CN/sink-parameters.md#parquet) | 将 emit 后的事件写成 Parquet 文件，适用于离线分析、归档和对象存储链路。 | `delivery target` 需要 `scheme` 与 OpenDAL `options`；`event sink` 支持 `path_prefix`、可选 `batch`、`columns` 和 `include_event_json`。 | 已支持 |
| [`stdout`](docs/zh-CN/sink-parameters.md#stdout) | 输出到标准输出，适合本地开发、processor 调试或种子验证。 | 无额外配置。 | 已支持 |

- 接口接入：`POST /ingest/{project_key}`、`GET /ingest/{project_key}?appid=...&xwhat=...`
- 项目访问：`auth_mode = token | public`，可选配置项目 IP allowlist。
- WAL：本地分段写入、pipeline checkpoint、重放与失败重试。详见 [WAL 文档](docs/zh-CN/wal.md)。
- Processor：Rhai `process(event, request)`，在脚本内部通过 `event` 上的校验 helper 做字段校验，再通过 `emit(target, event)` 产出投递计划。
- Sink：运行时配置来自数据库，默认支持的 sink 类型见上方。
- 管理能力：管理后台、OpenAPI、Swagger UI、Prometheus 指标、节点注册与心跳。
- 存储：SQLite / MySQL，支持迁移和启动 seed 初始化。

### 运行时模型

```text
+--------+
| Client |
+--------+
    |
    v
+--------------------------------------------------------------------------------+
| Ingest API                                                                     |
|                                                                                |
| +-----------------------+    +-------------------------+    +------------+     |
| | /ingest/{project_key} | -> | Project registry + auth | -> | WAL append |     |
| +-----------------------+    +-------------------------+    +------------+     |
|                                                                   |            |
|                                                                   v            |
|                                                               +---------+      |
|                                                               | ACK 200 |      |
|                                                               +---------+      |
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
| +------------------------+                                                     |
| | Load project processor |                                                     |
| +------------------------+                                                     |
|        |                                                                       |
|        v                                                                       |
| +-------------------------------------+                                        |
| | Run Rhai processor                  |                                        |
| | event.required(...), emit(target,event) |                                    |
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
| +-------------+    +--------------------------------+                          |
| | Event sinks | -> | Sink-defined commit + checkpoint |                        |
| +-------------+    +--------------------------------+                          |
+--------------------------------------------------------------------------------+
```

## 快速开始

### 1. 运行核心测试

```bash
cargo test --test ingest
```

该命令会运行当前的 ingest 集成测试，覆盖默认 seed、WAL 重放和 Rhai processor 执行路径。

完整本地校验：

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

HTTP 级 e2e 压测位于 `e2e/load/`，默认使用 `blackhole` sink，避免 Kafka/内网下游成为瓶颈：

```bash
e2e/load/run.sh
```

默认 seed 包含 `loadtest_app` 项目、`igx_loadtest_token` ingest token、`loadtest_blackhole` delivery target、`loadtest_events` event sink，以及 `loadtest_blackhole_processor`。若在公开/客户环境运行，请将该 token 按常规方式当作可写 token 管理；不需要压测时请在 admin 中停用 `loadtest_app`。

最近一次本地 `blackhole` 压测摘要：

- 机器：Apple M5, arm64, 10 逻辑核, 24 GiB RAM, macOS 26.3.1 (25D771280a)
- 启动命令：`cargo run --bin ingest4x -- server -c e2e/load/ingest4x.load.toml`
- 每个目标持续时长：1m

| 目标速率 | 实际速率 | WAL 收到总量 | 失败请求比例 | p95 延迟 | 排队窗口结束后积压 | 结论 |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 500 req/s | 499.936677 req/s | 30001 | 0.0000% | 20.399 ms | 0 | Pass |
| 1000 req/s | 999.835627 req/s | 60000 | 0.0000% | 22.209 ms | 22288 | HTTP pass; replay backlog |
| 3000 req/s | 2999.213727 req/s | 180001 | 0.0000% | 24.182 ms | 153377 | HTTP pass; replay backlog |

完整本地报告见：[本地 blackhole 压测报告](docs/zh-CN/load-test-local-blackhole.md)。

### 2. 启动服务

默认 `ingest4x.toml` 使用 `db/ingest4x.db` 的 SQLite 和 `./wal` 目录：

```bash
cargo run --bin ingest4x
```

也可以显式指定配置文件：

```bash
cargo run --bin ingest4x -- server -c ingest4x.toml
```

默认端口如下：

| 端口 | 用途 |
| --- | --- |
| `8090` | Ingress：`/ingest/{project_key}` |
| `18090` | 管理：`/healthz`、`/admin`、`/api/admin/*`、`/metrics`、OpenAPI 与 Swagger UI |

启动后，seed 会确保本地测试项目存在：

```text
project: test_app
ingest token: igx_local_test_token
```

Admin 地址：

```text
http://localhost:18090/admin/
```

默认 admin 密码来自 `ingest4x.toml`：

```text
ingest4x
```

若设置了 `INGEST4X_ADMIN_PASSWORD`，环境变量优先。

### 3. 发送 POST 事件

支持的数据上报方式：

| 方法 | Endpoint | 数据形态 | Token 位置 | 典型场景 |
| --- | --- | --- | --- | --- |
| `POST` | `/ingest/{project_key}` | JSON object body | `x-ingest-token` header 或 JSON 根字段 | 自有客户端和服务端 |
| `POST` | `/ingest/{project_key}` | `application/x-www-form-urlencoded` 表单字段 | `x-ingest-token` header 或 form 字段 | 不能设置自定义 header 的第三方 callback |
| `GET` | `/ingest/{project_key}?appid=...&xwhat=...` | querystring 字段 | 仅 `x-ingest-token` header | 简单测试客户端，以及能设置 header 的发送方 |

项目访问由每个项目的 `auth_mode = token | public` 控制，并可附加 IP allowlist。完整解析、鉴权与错误语义见 [接入协议](docs/zh-CN/ingest-protocol.md)。

JSON POST 示例：

```bash
curl -X POST http://127.0.0.1:8090/ingest/test_app \
  -H 'Content-Type: application/json' \
  -H 'x-ingest-token: igx_local_test_token' \
  -d '{
    "appid": "test_app",
    "xwhat": "custom_event",
    "xcontext": {
      "installid": "iid-1",
      "os": "ios",
      "idfa": "idfa-1",
      "currencytype": "cny"
    }
  }'
```

Form POST 示例：

```bash
curl -X POST http://127.0.0.1:8090/ingest/test_app \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'x-ingest-token=igx_local_test_token' \
  --data-urlencode 'appid=test_app' \
  --data-urlencode 'xwhat=custom_event' \
  --data-urlencode 'installid=iid-1' \
  --data-urlencode 'os=ios' \
  --data-urlencode 'idfa=idfa-1'
```

成功响应：

```text
200
```

项目从路径里的 `{project_key}` 解析。payload 或 form 里的 `appid` 是业务字段，不参与项目路由。

### 4. 发送 GET 事件

GET 示例：

```bash
curl "http://127.0.0.1:8090/ingest/test_app?appid=test_app&xwhat=custom_event&installid=iid-1&os=ios&idfa=idfa-1" \
  -H 'x-ingest-token: igx_local_test_token'
```

GET query 字段会转换成一个扁平 JSON object。字段名按原样保留；ingress 不展开 dotted path，也不会自动生成 `xcontext`。GET 不支持从 querystring 或 path 传 token。

## 请求语义

`/ingest/{project_key}` 每次请求只接受一个事件；数组 payload 不支持。详细的请求解析、鉴权行为、字段映射和失败响应见 [接入协议](docs/zh-CN/ingest-protocol.md)。

默认 processor 写法：

```rhai
fn process(event, request) {
    try {
        event.required("appid").string().min(1);
        event.required("xwhat").string().min(1);
        emit(SINK_EVENTS, event);
    } catch (err) {
        emit(SINK_EVENTS_ERROR, event);
    }
}
```

默认 seed 会创建两个 stdout event sink：

- `events`
- `events_error`

默认也会创建一个指向 `127.0.0.1:9092` 的 `Local Kafka` delivery target。要向 Kafka 投递，请在 admin/API 中创建并启用对应的 event sink。

本地/客户集群压测场景默认 seed 同时创建：

- project: `loadtest_app`
- ingest token: `igx_loadtest_token`
- delivery target: `loadtest_blackhole`
- event sink: `loadtest_events`
- processor script: `loadtest_blackhole_processor`

该场景通过 `blackhole` sink 参与完整的 WAL replay、processor、sink checkpoint 与指标链路，但不写入外部系统。`igx_loadtest_token` 是真实可写 ingest token；若不允许公开测试接入，可停用 `loadtest_app` 或替换/轮换 token。

## 5. 回放处理

回放处理以单入口 `fn process(event, request)` 为中心：

- 字段校验通过 `event.required(...)`、`event.optional(...)`、`event.any(...)` 等 helper 在脚本内联完成。
- 校验 helper 抛错后，可由脚本自己 `try/catch` 并转发到错误 sink；未捕获时则按 processor runtime failure 处理。

详见 [事件处理](docs/zh-CN/event-processing/index.md)。

## WAL 与投递

关于 ACK 语义、记录格式、分段、checkpoint、重放清理与失败处理，请见 [WAL](docs/zh-CN/wal.md)。

## 前端

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

生产环境服务会直接提供 `web/admin/dist` 构建产物。更新嵌入式管理端资源前请先完成前端构建。

## 发布与版本

见 [发布与版本](docs/zh-CN/release-versioning.md)。

## 更多文档

- [WAL](docs/zh-CN/wal.md)
- [事件处理](docs/zh-CN/event-processing/index.md)
- [管理端与 API](docs/zh-CN/admin-api.md)
- [本地 blackhole 压测报告](docs/zh-CN/load-test-local-blackhole.md)
- [发布与版本](docs/zh-CN/release-versioning.md)
- [项目结构](docs/zh-CN/project-structure.md)
