# WAL

WAL 是 ingest4x 的接入持久化层。`/ingest` 收到事件后，不会在请求线程里执行规则、processor 或下游 sink 投递，而是先把原始事件写入本地 WAL；后台 replay 再从 WAL 读取事件，执行业务处理并投递到 event sinks。

这让接入 ACK 和下游投递解耦：下游 Kafka、stdout 或 processor 暂时失败时，只要事件已经进入 WAL，接入面仍然可以先返回成功，后续由 replay 继续重试或隔离坏记录。

## 端到端链路

```text
client
  -> /ingest
  -> token auth
  -> payload size/json check
  -> WAL append
  -> 200

background replay
  -> read WAL after sink checkpoints
  -> parse original body
  -> load project rules
  -> run Rhai processor
  -> emit deliveries
  -> send to event sinks
  -> advance each sink checkpoint
  -> cleanup covered WAL segments
```

关键点：

- `/ingest` 只处理单事件 JSON object，不支持批量数组。
- WAL record 保存原始 payload 和必要请求元数据。
- 业务字段归一化、规则校验、错误标记和分流都发生在 replay 阶段，不发生在 append 路径。
- 每个 event sink 有独立 checkpoint，一个 sink 卡住不会直接把其他 sink 的 checkpoint 回滚。

## ACK 语义

默认配置 `wal.no_sync = false`。此时 `/ingest` 返回 `200` 表示 WAL append 已经完成，并且写入路径会等待 segment `sync_data()` 成功后才通知请求。

```toml
[wal]
no_sync = false
flush_max_interval = "10ms"
flush_max_records = 1000
```

`flush_max_interval` 和 `flush_max_records` 控制 group commit：多个请求可以在很短时间窗口内一起落盘，以减少 sync 次数。对单个请求来说，只要 `no_sync = false`，请求会等待自己所在的 flush 完成后才返回成功。

如果设置 `wal.no_sync = true`，append 会在写入内存 buffer 后立即返回，后台 flush 线程稍后落盘。这会降低延迟，但进程崩溃时最近一批还没有 flush 的事件可能丢失。这个模式不应该被理解为强持久 ACK。

## WAL Record

当前 WAL record 的核心字段：

| 字段 | 说明 |
| --- | --- |
| `record_id` | 接收时生成的记录 ID，格式类似 `wal-<received_at_ms>-<sequence>` |
| `lsn` | WAL append 时分配的递增序号 |
| `node_id` | 当前服务节点 ID，持久化在 WAL 目录 |
| `project_id` | ingest token 认证出来的项目 ID |
| `received_at_ms` | 接入层收到事件时的时间戳 |
| `method` / `path` / `query` | 原始 HTTP 请求信息 |
| `remote_addr` | 远端地址，取决于 Actix 连接信息 |
| `headers` | 过滤敏感认证头后的请求 headers |
| `body` | 原始事件 JSON bytes |

接入 token 不会写入 WAL headers。`authorization` 和 `x-ingest-token` 会在生成 WAL record headers 时被过滤。

`received_at_ms` 会传给 processor 的 `request` 上下文。它代表接入接收时间，不是 replay 时间，也不是客户端事件发生时间。

## 文件和目录

默认 WAL 目录来自配置：

```toml
[wal]
dir = "./wal"
```

目录内主要文件：

```text
wal/
  node_id
  wal.lock
  00000000000000000001.wal
  00000000000000000002.wal
  checkpoints/
    events.json
    events_error.json
```

说明：

- `node_id`：服务节点 ID。首次启动会创建；如果配置显式指定的 node ID 和已持久化的值不一致，启动会失败。
- `wal.lock`：目录锁，防止两个进程同时使用同一个 WAL 目录。
- `*.wal`：分段 WAL 文件。每个 segment 有固定 header，后面是连续 record frame。
- `checkpoints/*.json`：每个 event sink 独立的 replay checkpoint。

WAL record 在 segment 内是二进制 frame，不是 JSONL。frame 包含 magic、version、payload length、CRC 等元数据；payload 使用当前 Rust 结构序列化。排查时不要把 `.wal` 文件当文本日志直接解析。

## 写入和分段

WAL 从 segment `1` 开始写入。每个 record append 时会分配：

- `lsn`：全局递增的逻辑序号。
- `segment`：当前 segment ID。
- `offset`：record frame 在 segment 内的起始偏移。

当当前 segment 写入下一个 frame 会超过 `wal_segment_max_bytes` 时，writer 会创建新 segment。默认值：

```toml
[wal]
wal_segment_max_bytes = 134217728
```

写入前还会检查可用磁盘空间。如果 `min_free_bytes` 配置非零，且写入后剩余空间会低于阈值，append 会失败，`/ingest` 返回 `503`。

## Replay

服务启动后会启动 WAL replay loop。每轮 replay 最多读取一批 WAL entry，当前批大小是 `1024`。

每条 record 的处理流程：

1. 解析 `body` 为 JSON。
2. 检查 `project_id` 仍然存在于内存项目 registry。
3. 编译并加载当前项目绑定的 rules。
4. 调用 Rhai processor：`process(event, request)`。
5. 校验 processor emit 的 sink target 是否存在。
6. 按 sink 投递事件。
7. 投递成功后推进对应 sink 的 checkpoint。

replay 使用的是当前数据库里的规则、processor 和 sink 配置，而不是事件写入 WAL 时的旧配置。因此修改 processor 或 rules 后，尚未 replay 的历史 WAL record 会按新配置处理。

## Checkpoint

每个 event sink 有独立 checkpoint：

```text
<wal.dir>/checkpoints/<sink>.json
```

checkpoint 记录：

| 字段 | 说明 |
| --- | --- |
| `node_id` | checkpoint 所属 WAL 节点 |
| `sink_id` | event sink ID |
| `checkpoint_lsn` | 已覆盖到的 LSN |
| `checkpoint_segment_id` | 已覆盖到的 segment |
| `checkpoint_segment_offset` | 下次继续读取的 offset |
| `checksum` | checkpoint 内容校验 |

checkpoint 写入使用临时文件、`sync_data()`、rename 和目录 sync，避免半写入文件被当成有效 checkpoint。

当新 sink 没有 checkpoint 时，由该 sink 的 `auto_offset_reset` 决定起点：

| 值 | 行为 |
| --- | --- |
| `earliest` | 从 WAL 当前最早可读位置开始 replay |
| `latest` | 直接把 checkpoint 初始化到 WAL 当前尾部，只消费之后的新事件 |

默认 seed 创建的 `events` 和 `events_error` sink 使用 `latest`。

如果已有 checkpoint 早于 WAL 当前 floor，说明对应旧 segment 已经清理掉；此时也会按该 sink 的 `auto_offset_reset` 重置。

## 清理策略

WAL segment 只有在所有当前启用 sink 的最小 checkpoint 都覆盖它之后，才会被清理。换句话说：

- 快 sink 的 checkpoint 可以继续前进。
- 慢 sink 或失败 sink 会保留它尚未覆盖的 segment。
- 清理以所有 sink 的最小 checkpoint 为水位线。

这意味着一个长期失败的 sink 会阻止 WAL 空间释放，需要通过修复 sink、停用 sink，或明确重置 checkpoint 来解除积压。

## 失败处理

replay 会区分可隔离的坏 record 和需要重试的运行时失败。

会被隔离的常见情况包括：

- WAL body 不是合法 JSON。
- record 的 `project_id` 当前不存在。
- processor 运行失败，且错误被归类为可隔离 record。
- processor emit 了空 target 或未知 sink target。

隔离时不会写入新的业务 sink，而是把一条结构化记录写入日志 target：

```text
ingest4x::wal::quarantine
```

quarantine 日志会包含 record ID、LSN、请求信息、错误码、错误消息和 base64 后的原始 body。对应 WAL entry 会被标记为已处理，checkpoint 可以继续前进。

sink 投递失败不是 quarantine。某个 sink 投递失败时，该 sink 的 checkpoint 不会推进，replay loop 会退避后重试。其他 sink 如果已经成功处理同一条 record，可以推进自己的 checkpoint。

## 可观测性

管理面 `/healthz` 会返回 WAL 状态：

```json
{
  "status": "ok",
  "wal_enabled": true,
  "wal_ready": true
}
```

如果 WAL 磁盘空间不足或状态不可用，`wal_ready` 会变成 `false`，健康检查可能返回 `503`。

管理面 `/metrics` 会暴露 WAL 相关 Prometheus 指标，包括：

| 指标 | 说明 |
| --- | --- |
| `wal_node_info` | 当前 WAL node 信息 |
| `wal_enabled` | WAL 是否启用 |
| `wal_ready` | WAL 是否可写 |
| `wal_reliable_ack` | 当前是否为可靠 ACK 模式 |
| `wal_no_sync` | 是否启用 no-sync 模式 |
| `wal_available_bytes` | WAL 目录可用空间 |
| `wal_min_free_bytes` | 配置的最小剩余空间 |
| `wal_active_segment_id` | 当前写入 segment |
| `wal_active_segment_bytes` | 当前 segment 已写字节数 |
| `wal_max_lsn` | writer 已分配的最大 LSN |
| `wal_checkpoint_lsn` | 当前 sink checkpoint 水位 |
| `wal_replay_lag_lsn` | WAL max LSN 和 checkpoint LSN 的差值 |
| `wal_append_errors_total` | append 错误计数 |
| `wal_replay_errors_total` | replay 错误计数 |

排查积压时优先看 `wal_replay_lag_lsn`、sink 错误日志和 checkpoint 文件更新时间。

## 配置项

常用配置：

```toml
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

含义：

| 配置 | 说明 |
| --- | --- |
| `wal.dir` | WAL 数据目录 |
| `wal.flush_max_interval` | buffer 最长等待多久必须 flush |
| `wal.flush_max_records` | buffer 累计多少条必须 flush |
| `wal.no_sync` | 是否跳过同步落盘等待 |
| `wal.wal_segment_max_bytes` | 单个 segment 最大字节数 |
| `wal.min_free_bytes` | WAL 目录最小剩余空间要求，非零时启用 |
| `wal.checkpoint.flush_interval` | checkpoint 最长 flush 间隔 |
| `wal.checkpoint.flush_records` | checkpoint 累计多少条后 flush |
| `wal.checkpoint.flush_bytes` | checkpoint 累计多少 WAL bytes 后 flush |

## 边界

WAL 只保证 ingest4x 接收后的本地持久化和 replay，不等同于完整业务幂等系统。

- WAL 不会替客户端生成业务事件 ID。
- WAL 不会自动去重重复上报。
- replay 可能因为进程崩溃在 sink 投递和 checkpoint 写入之间重试同一条 record；下游如果需要 exactly-once，需要自己用事件 ID 或业务键做幂等。
- WAL payload 当前跟 Rust 结构序列化格式绑定，旧版本 WAL 跨 schema 兼容不是长期承诺。开发环境升级结构后如果遇到 decode 问题，可以清理本地 `wal/` 和测试数据库后重启。
- 多节点部署时，每个节点应使用自己的 WAL 目录和 node ID；不要让多个进程共享同一个 WAL 目录。

## 排障入口

| 现象 | 优先检查 |
| --- | --- |
| `/ingest` 返回 `503` | WAL 目录权限、磁盘空间、`wal.lock`、`wal_append_errors_total` |
| `/healthz` 里 `wal_ready=false` | `wal.min_free_bytes` 和 WAL 目录可用空间 |
| replay 积压 | `wal_replay_lag_lsn`、sink 连接、sink checkpoint 更新时间 |
| 某个 sink 不消费历史事件 | 该 sink 的 `auto_offset_reset` 和 checkpoint 是否已初始化到 tail |
| checkpoint 文件报错 | `node_id` 是否变化、checkpoint checksum 是否损坏 |
| 日志出现 quarantine | 查 `ingest4x::wal::quarantine` target 的结构化记录和原始 body |
