# WAL

WAL（Write-Ahead Logging）是 ingest4x 的持久化入口层。`/ingest/{project_key}` 接收事件后，不会在请求线程执行规则、processor 或下游 sink 投递，而是先将事件写入本地 WAL，再由后台重放线程读取记录并执行业务处理与 sink 投递。

这将 ingress ACK 与下游投递解耦。只要事件已可靠写入 WAL，即使 Kafka/stdout/processor 暂时故障，`/ingest/{project_key}` 仍可返回成功；后续重放会重试或将错误记录隔离。

## 端到端路径

```text
client
  -> /ingest/{project_key}
  -> project auth
  -> payload decode/size check
  -> WAL append
  -> 200

background replay
  -> read WAL after pipeline checkpoint
  -> parse original body
  -> load project rules
  -> run Rhai processor
  -> emit deliveries
  -> batch deliveries by sink and wait for sink-defined commit
  -> advance pipeline checkpoint
  -> cleanup covered WAL segments
```

关键点：

- `/ingest/{project_key}` 每次请求只接收一个事件，不支持数组批量。HTTP 层支持 JSON body、form body 和 GET query 字段，详见 [接入协议](ingest-protocol.md)。
- WAL 记录存储原始 payload 与请求元数据。
- 字段归一化、校验、错误标记、路由在重放阶段发生，而不是 append 阶段。
- 重放只有一个 pipeline checkpoint。任意已 emit 的 sink 失败时，checkpoint 不前进，整个 replay window 后续重试。

## ACK 语义

默认是 `wal.write.no_sync = false`。此模式下，`/ingest/{project_key}` 仅在 WAL append 成功且写入路径等待当前 segment 的 `sync_data()` 成功后返回 `200`。

```toml
[wal.write]
no_sync = false
flush_interval = "10ms"
flush_records = 1000
```

`wal.write.flush_interval` 与 `wal.write.flush_records` 控制 group commit。多个请求可以在一个短窗口内合并 flush，降低 sync 频率。`wal.write.no_sync = false` 时，每个请求会等待自己的 flush 完成后才返回成功。

当 `wal.write.no_sync = true` 时，append 在写入内存缓冲后返回，flush 由后台异步执行。该模式可降低延迟，但进程在 flush 前崩溃会丢失最近一批未刷盘数据；因此它不是强持久 ACK。

## WAL 记录

核心字段：

| 字段 | 含义 |
| --- | --- |
| `record_id` | 接收时生成的记录 ID，如 `wal-<received_at_ms>-<sequence>` |
| `lsn` | append 阶段生成的递增逻辑序列号 |
| `node_id` | 持久化在 WAL 目录中的服务节点 ID |
| `project_id` | 通过 `{project_key}` 解析并完成鉴权后的项目 ID |
| `received_at_ms` | ingress 接收时间戳 |
| `method` / `path` / `query` | 原始 HTTP 请求信息 |
| `remote_addr` | 远端 socket 地址 |
| `headers` | 移除 ingest4x 自身鉴权 token 后的 request header |
| `body` | 写入 WAL 的事件 JSON 字节 |

客户传入的 request headers 会作为 WAL request context 的一部分保留。ingest4x 只会在写 WAL headers 前移除自身使用的 `x-ingest-token`。

`received_at_ms` 会进入 processor 的 `request` 上下文；它是接收时间戳，不是重放时间或客户端事件时间戳。

## 文件与目录

配置中的 WAL 目录：

```toml
[wal]
dir = "./wal"
```

目录下主要文件：

```text
wal/
  node_id
  wal.lock
  checkpoint.json
  00000000000000000001.wal
  00000000000000000002.wal
```

说明：

- `node_id`：服务节点 ID。首次启动会创建；如配置节点 ID 与持久化值不一致，启动失败。
- `wal.lock`：目录锁，防止同一 WAL 被多个进程同时使用。
- `checkpoint.json`：持久化的 pipeline replay checkpoint。
- `*.wal`：分段 WAL 文件。每个分段包含固定 header 和顺序 record frame。

`.wal` 内部是二进制 frame（非 JSONL），包含 magic/version/payload length/CRC 等元数据，payload 使用 Rust 序列化存储。排障时不要按普通文本日志直接解析 `.wal`。

## 写入与分段

WAL 从 segment `1` 开始。每次 append 分配：

- `lsn`：全局递增的逻辑序列号。
- `segment`：当前 segment 编号。
- `offset`：frame 在 segment 中起始偏移。

当下一个 frame 会超过 `wal.write.segment_max_bytes` 时，writer 会创建新 segment。默认：

```toml
[wal.write]
segment_max_bytes = 134217728
```

写入前会检查磁盘剩余空间；若 `wal.write.min_free_bytes` 非零且写入后剩余空间低于阈值，append 失败，`/ingest/{project_key}` 返回 `503`。

## 重放

服务启动时会启动 WAL 重放循环。每次运行默认读取一批记录（默认 read limit 为 `1024`）。

重放仍然逐条 WAL record 执行 rules 与 processor，因为 Rhai 当前每次接收一个 JSON event。processor 输出校验完成后，系统会在当前 replay window 内按 sink 暂存 delivery。当达到 `wal.replay.max_records` 或 `wal.replay.max_bytes` 时，当前 replay window 会被 flush。在这个 replay window 内，每个 sink 的待投递 JSON events 会再按 `wal.replay.sink_batch.max_events` 与 `wal.replay.sink_batch.max_bytes` 切成一次或多次 `send_batch` 调用。

逐条规划流程：

1. 将 `body` 解析为 JSON。
2. 校验 `project_id` 仍在内存 registry 中。
3. 编译并加载当前规则。
4. 调用 Rhai processor：`process(event, request)`。
5. 校验 processor 的 `emit` sink 目标存在。

投递流程：

1. 按 sink target 聚合已规划的 delivery。
2. 按配置的 sink batch 限制切分每个 sink 的待投递 JSON events。
3. 对当前 replay window 内该 sink 的一个或多个 chunk 调用 `send_batch`。
4. 只有所有已 emit sink 的 batch 都达到各自定义的 commit 点后，才推进 pipeline checkpoint。

每种 sink 自己定义 commit 完成标准。Kafka 可以把 broker delivery report 成功视为 commit；本地文件 sink 必须等最终文件提交完成；对象存储 sink 必须等 upload 完成或 multipart complete 成功。WAL replay 只观察 sink runtime 的结果：所有 sink 返回 `Ok` 才允许 pipeline checkpoint 前进；任意 `Err` 都会保持 checkpoint 滞后并等待重试。

重放使用当前数据库中的规则/processor/sink 配置，而不是 append 当时的配置。因此配置变更后，历史记录会按新配置执行。

processor 与 sink 之间的统一内存格式仍是 JSON event。Parquet 这类列式 sink 会在自身编码阶段把一批 JSON events 转成 Arrow arrays；Kafka/stdout 仍可直接使用 JSON，不需要经过 Arrow 再转回 JSON。

## Checkpoint

重放只有一个 pipeline checkpoint：

```text
<wal.dir>/checkpoint.json
```

Checkpoint 内容：

| 字段 | 含义 |
| --- | --- |
| `node_id` | 本 checkpoint 所属 WAL 节点 |
| `sink_id` | pipeline checkpoint 固定为 `null` |
| `checkpoint_lsn` | 已覆盖 LSN |
| `checkpoint_segment_id` | 覆盖到的 segment ID |
| `checkpoint_segment_offset` | 下一条可读偏移 |
| `checksum` | checkpoint 完整性校验 |

Checkpoint 写入流程使用临时文件、`sync_data()`、rename 和目录同步，避免将半写入 checkpoint 误判为有效。

不存在 checkpoint 时，活跃 sink 的 `auto_offset_reset` 会合并决定 pipeline 起点：

| 值 | 行为 |
| --- | --- |
| 任意 sink 是 `earliest` | 从可读 WAL 最早偏移重放 |
| 所有 sink 都是 `latest` | 初始化到当前 WAL 尾部，只消费新事件 |

默认 seed 对 `events`、`events_error`、`loadtest_events` 使用 `latest`。

若已有 checkpoint 落后于当前 WAL floor，可能是旧 segment 已清理，系统会按同样的 pipeline `auto_offset_reset` 合并规则重置。

## 保留与清理

只有当 pipeline checkpoint 已经过某个 segment 后，该 segment 才可清理。实践含义：

- 慢速或失败 sink 会阻塞 pipeline checkpoint。
- 如果某个 sink 已 commit，但另一个 sink 失败，重试时前者可能再次收到同批事件。
- 下游若不能接受重复投递，需要按事件 ID 或业务主键做幂等。

长时间失败的 sink 会阻塞 WAL 空间复用；修复或停用该 sink 后，replay 才能继续推进。

## 失败处理

重放会区分可隔离（quarantine）与可重试（transient）记录。

常见入列隔离场景：

- WAL body 不是合法 JSON。
- `project_id` 已不存在。
- Processor 运行时失败且可判定为隔离类型。
- Processor emit 到空目标或未知 sink。

隔离记录不会写入业务 sink，而是写入 `ingest4x::wal::quarantine` 的结构化事件，包含 record ID、LSN、请求元数据、错误码/错误信息、base64 原始 body。该 WAL 记录标记为已处理，checkpoint 可继续前进。

sink commit 失败不进入隔离。任意已 emit sink 在达到自身 commit 点前失败，pipeline checkpoint 都不前进，重放会按退避重试；同一 replay window 内已经成功 commit 的其它 sink，后续重试时可能再次收到这些事件。

## 可观测性

管理端 `/healthz` 返回 WAL 状态：

```json
{
  "status": "ok",
  "wal_enabled": true,
  "wal_ready": true
}
```

若磁盘不足或 WAL 不健康，`wal_ready` 可能为 `false`，健康检查会返回 `503`。

管理端 `/metrics` 导出与 WAL 相关的 Prometheus 指标，包括：

| 指标 | 含义 |
| --- | --- |
| `wal_node_info` | 当前 WAL 节点信息 |
| `wal_enabled` | WAL 是否启用 |
| `wal_ready` | WAL 是否可写 |
| `wal_reliable_ack` | 是否启用可靠 ACK |
| `wal_no_sync` | 是否开启 no-sync |
| `wal_available_bytes` | WAL 目录可用字节 |
| `wal_min_free_bytes` | 配置的最小空闲阈值 |
| `wal_active_segment_id` | 当前 active segment |
| `wal_active_segment_bytes` | 当前 active segment 写入量 |
| `wal_max_lsn` | 当前最大 writer LSN |
| `wal_checkpoint_lsn` | 当前 pipeline checkpoint |
| `wal_replay_lag_lsn` | 最大 LSN 与 checkpoint 的差值 |
| `wal_append_errors_total` | WAL append 错误计数 |
| `wal_replay_errors_total` | 重放错误计数 |

出现积压时，优先检查 `wal_replay_lag_lsn`、sink 错误日志与 checkpoint 文件更新时间。

## 配置项

常用参数：

```toml
[wal]
dir = "./wal"

[wal.write]
flush_interval = "10ms"
flush_records = 1000
no_sync = false
segment_max_bytes = 134217728
min_free_bytes = 0
```

```toml
[wal.checkpoint]
flush_interval = "1s"
flush_records = 1000
flush_bytes = 67108864

[wal.replay]
max_records = 1000
max_bytes = 67108864

[wal.replay.sink_batch]
max_events = 1000
max_bytes = 67108864
```

含义：

| 配置 | 说明 |
| --- | --- |
| `wal.dir` | WAL 数据目录 |
| `wal.write.flush_interval` | writer 缓冲最长 flush 间隔 |
| `wal.write.flush_records` | writer 强制 flush 的最大记录数 |
| `wal.write.no_sync` | 写入时是否跳过同步等待 |
| `wal.write.segment_max_bytes` | 分段最大大小 |
| `wal.write.min_free_bytes` | 最小空闲阈值，非零时生效 |
| `wal.checkpoint.flush_interval` | 两次 checkpoint flush 最大间隔 |
| `wal.checkpoint.flush_records` | checkpoint 文件 flush 前最多累计的成功重放记录数 |
| `wal.checkpoint.flush_bytes` | checkpoint 文件 flush 前最多累计的成功重放 WAL 字节数 |
| `wal.replay.max_records` | 单个 replay window 的最大 WAL 记录数 |
| `wal.replay.max_bytes` | 单个 replay window 的最大 WAL 字节数 |
| `wal.replay.sink_batch.max_events` | replay window 内单次 sink `send_batch` 的最大 event 数 |
| `wal.replay.sink_batch.max_bytes` | replay window 内单次 sink `send_batch` 的最大 JSON event 字节数 |

## 边界说明

WAL 提供“ingress + replay”链路内的持久性保证，不是完整的业务幂等系统。

- WAL 不会生成业务事件 ID。
- WAL 不会去重重复提交。
- 如果进程在 sink commit 与 checkpoint 写入之间崩溃，或某个 sink 已 commit 但另一个 sink 阻塞 pipeline checkpoint，重放可能重复投递同一记录；下游需要 exactly-once 时应使用事件 ID 或业务主键。
- WAL payload 当前与 Rust 序列化绑定，跨旧版本 WAL 的 schema 兼容性不做长期保证。
- 多节点部署下，每个节点应使用独立 WAL 目录和 node_id，不要共享同一 WAL 目录。

## 故障排查

| 现象 | 优先排查 |
| --- | --- |
| `/ingest/{project_key}` 返回 `503` | WAL 目录权限、磁盘空间、`wal.lock`、`wal_append_errors_total` |
| `/healthz` 显示 `wal_ready=false` | 检查 `wal.write.min_free_bytes` 与 WAL 可用空间 |
| 重放堆积 | 关注 `wal_replay_lag_lsn`、sink 连通性、checkpoint 更新时间 |
| Sink 未消费历史 | 检查 `auto_offset_reset` 与 checkpoint 是否已移动到尾部 |
| Checkpoint 文件异常 | 验证 `node_id` 与 checksum 完整性 |
| 出现 `quarantine` 日志 | 检查 `ingest4x::wal::quarantine` 记录和原始 body |
