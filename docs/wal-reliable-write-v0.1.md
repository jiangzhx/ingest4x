# 收数服务 WAL 可靠写入规范 v0.1

本文定义 `ingest4x` 收数链路的第一版本地单机强持久 WAL 语义。

## 当前结论

第一版核心目标：

> 弱客户端上报事件后，只要服务端返回成功，该事件就已经可靠进入 WAL；即使进程崩溃、系统重启、短暂断电，服务端仍然可以从 WAL 恢复并继续写下游。

本版暂不定义：

- WAL 中间损坏后的生产恢复策略。
- RULES 引擎自身异常处理。
- JSON 基础校验细节；这部分由接入层或业务层负责。

## 适用范围

适用于：

- 弱客户端事件收集服务。
- 单事件上报模型。
- 本地单机强持久 WAL。
- 下游为 Kafka、Parquet 或 RULES 路由后的目标存储。

当前事件模型：

- 一个请求体 = 一个事件 JSON object。
- 一个事件 = 一个 WAL record。
- 一个 WAL record = 一个 LSN。

第一版不支持：

- 批量事件数组。
- `events` 数组。
- 一个请求中包含多条事件。

## ACK 语义

服务端返回成功的唯一含义是：

> 事件已经写入 WAL，并且 WAL 已经完成持久化。

标准流程：

1. 客户端请求。
2. 服务端读取完整事件。
3. 基础接入层处理。
4. 写入 WAL。
5. `fdatasync` 或 `fsync` 成功。
6. 返回成功。

禁止：

- 只写入内存就返回成功。
- 只进入队列就返回成功。
- 只 `write()` 到 page cache 就返回成功。
- 只完成 RULES 判断就返回成功。
- 只写入下游 buffer 就返回成功。

### no_sync 兼容语义

`no_sync` 可以作为业务方显式选择的性能/弱可靠模式保留，但它不属于 WAL v0.1 的强持久 ACK 承诺。

要求：

- `no_sync = false` 时，ACK 才能表示 WAL 已持久化。
- `no_sync = true` 时，ACK 不得声明为强持久 ACK。
- `no_sync = true` 时，文档、启动日志、健康检查或 `/wal/status` 必须显式暴露当前节点处于弱可靠模式。
- `no_sync = true` 时，不得把当前节点标记为满足 WAL v0.1 可靠写入标准。

## 故障覆盖范围

第一版承诺覆盖：

- 服务进程崩溃。
- 服务进程被 kill。
- 操作系统崩溃。
- 机器重启。
- 短暂断电。

前提是本地 WAL 磁盘没有永久损坏。

第一版不承诺覆盖：

- 整机永久损坏。
- 本地磁盘损坏。
- WAL 目录被人为删除。
- 机房级故障。

本版定位为单机强持久 WAL。

## WAL 原子性与接入层事件大小

恢复时只允许两种结果：

1. 事件完整存在，可以 replay。
2. 事件不完整，不允许 replay。

禁止恢复半个事件或事件的部分字段。

事件大小限制属于服务端接入层容量策略，不是 WAL 模块内部职责。进入 WAL 前，服务端接入层应完成大小检查。

默认接入层事件大小限制：

```yaml
max_event_bytes: 256KB
max_event_bytes_min: 16KB
max_event_bytes_max: 1MB
```

超过上限时：

- 返回失败。
- 不写 WAL。
- 不 ACK。
- HTTP 接口建议返回 `413 Payload Too Large`。

## xcontext.event_id 规范

下游只基于 `xcontext.event_id` 排重，不基于 timestamp、appid、xwhat、xwho、payload hash 或业务自然键排重。

`xcontext.event_id` 是业务 ID，属于事件 payload / 业务语义，不属于 WAL 或收数服务基础设施语义。

要求：

- 如果客户端提供 `xcontext.event_id`，WAL 只按原始 payload 保存该值。
- WAL 不生成 `xcontext.event_id`。
- WAL replay 不得把 `node_id + lsn` 写成 `xcontext.event_id`。
- 如果业务需要补齐 `xcontext.event_id`，应由 replay 阶段的业务 rules / processor 显式完成。
- `node_id + lsn` 是 WAL record 标识，只用于 WAL 恢复、checkpoint、定位和排障，不作为业务排重 ID。

## WAL record 内容

WAL 保存客户端原始事件 payload 和必要 metadata。`xcontext.event_id`、`xcontext.process_info`、大小写归一化、二次加工和分流等业务字段变化不应发生在 WAL append 路径。

每条 WAL record 至少包含：

| 字段 | 含义 |
| --- | --- |
| `magic` | 识别合法 WAL record |
| `version` | WAL 格式版本 |
| `header_len` | header 长度，方便未来扩展 |
| `record_type` | record 类型，例如 `DATA` |
| `flags` | 压缩、加密、分片等标记 |
| `lsn` | 本节点 WAL 逻辑序号 |
| `node_id` | 写入 WAL 时的节点标识 |
| `payload_len` | payload 字节长度 |
| `payload_crc` | payload 校验和 |
| `created_at` | 服务端接收或写入时间 |
| `raw_payload` | 客户端原始事件内容 |

replay 时必须校验：

- `magic`
- `version`
- `payload_len`
- `payload_crc`

只有校验通过的 record 才允许进入 replay 流程。

## LSN 规范

每个收数节点维护自己的 WAL LSN：

```text
lsn = 本节点 WAL 内单调递增序号
```

不要求所有节点共享全局递增 LSN。

全局唯一 WAL record 标识：

```text
node_id + lsn
```

启动时必须恢复 `max_lsn`，新写入从 `max_lsn + 1` 继续。

禁止重启后 LSN 重置，否则会导致：

- `event_id` 重复。
- checkpoint 混乱。
- 下游排重错误。

## node_id 规范

`node_id` 是 WAL 基础设施字段，用于 WAL record、segment、checkpoint、监控和排障，不写入事件 payload，也不写入 `xcontext.process_info`。

来源优先级：

1. 配置中显式提供 `wal.node_id` 时，使用配置值。
2. 配置未提供时，如果 `wal/node_id` 已存在，读取并复用该值。
3. 配置未提供且 `wal/node_id` 不存在时，生成随机 UUID，写入 `wal/node_id.tmp`，fsync 后 rename 为 `wal/node_id`，再 fsync WAL 目录。

要求：

- 同一个 WAL 目录的 `node_id` 必须稳定。
- 禁止每次启动都重新随机生成 `node_id`。
- WAL record 必须保存写入时使用的 `node_id`。
- checkpoint 和 segment header 应保存或校验对应 `node_id`。
- `node_id` 不作为下游排重键。

## WAL 并发模型

同一个 WAL 目录同一时刻只允许一个写入进程：

```text
一个 wal/ 目录 = 一个 writer 进程
```

服务启动时必须获取 WAL 目录锁 `wal.lock`。锁获取失败时拒绝启动。

服务内部可以并发接收请求，但最终 WAL append 必须串行化：

```text
request workers
  -> wal append queue
  -> single WAL writer
  -> group commit
  -> fdatasync / fsync
  -> ACK
```

LSN 应由 WAL writer 分配，不建议由多个请求线程各自分配。

## Group Commit

允许多个请求合并一次持久化操作，但任何请求都不能在所属 WAL batch 完成 `fsync` 或 `fdatasync` 前返回成功。

满足任一条件触发 group commit：

```yaml
wal_flush_max_interval: 10ms
wal_flush_max_bytes: 4MB
wal_flush_max_records: 1000
```

当前实现状态：

- `flush_max_interval` 已作为配置字段接入。
- `flush_max_records` 已作为配置字段接入。
- `flush_max_bytes` 已作为保留配置字段接入，但字节数触发 group commit 的逻辑尚未实现。

允许配置范围：

```yaml
wal_flush_max_interval: 1ms ~ 100ms
wal_flush_max_bytes: 1MB ~ 16MB
wal_flush_max_records: 100 ~ 10000
```

## Segment 规范

WAL 必须按固定大小 segment 拆分，禁止单个 WAL 文件无限追加。

目录示例：

```text
wal/
  0000000000000001.wal
  0000000000000002.wal
  0000000000000003.wal
```

segment 文件名应满足：

- 单调递增。
- 固定宽度。
- 可按字典序排序。
- 可直接推断先后顺序。

推荐格式：

```text
%016d.wal
```

默认大小：

```yaml
wal_segment_size: 128MB
wal_segment_size_min: 64MB
wal_segment_size_max: 1GB
```

新 segment 必须通过临时文件创建：

1. `open 0000000000000002.wal.tmp`
2. write segment header
3. `fsync` 或 `fdatasync` segment file
4. `rename 0000000000000002.wal.tmp -> 0000000000000002.wal`
5. `fsync` WAL directory
6. 开始写入 WAL record

恢复时只识别 `*.wal`，未完成的 `*.wal.tmp` 不得作为有效 WAL replay。

每个 segment 开头建议写入：

- `segment_magic`
- `segment_version`
- `segment_id`
- `created_at`
- `node_id`
- `start_lsn`
- `segment_header_crc`

segment 创建失败时必须拒绝新请求，不得 ACK。

## Checkpoint 规范

只有对应 WAL LSN 的数据已经被下游可靠接收或持久化后，才能推进 checkpoint。

标准流程：

```text
读取 WAL
  -> 解析 raw_payload
  -> 执行 RULES
  -> 写入 RULES 指定下游
  -> 下游确认成功
  -> 推进 applied_lsn
  -> 批量持久化 checkpoint
```

禁止：

- 读取 WAL 后立即推进 checkpoint。
- RULES 判定完成但未写下游就推进 checkpoint。
- 下游未确认成功就推进 checkpoint。
- 先推进 checkpoint 再写下游。

checkpoint 必须使用临时文件写入，并通过 rename 原子替换正式文件：

1. write `checkpoint.tmp`
2. `fsync` 或 `fdatasync checkpoint.tmp`
3. `rename checkpoint.tmp -> checkpoint`
4. `fsync` checkpoint directory

checkpoint 文件建议至少包含：

- `version`
- `node_id`
- `checkpoint_lsn`
- `checkpoint_segment_id`
- `checkpoint_segment_offset`
- `updated_at`
- `checksum`

`checkpoint_lsn` 表示下游已经确认成功处理到的连续最大 LSN。

checkpoint 不要求每处理一条 WAL 都持久化一次。默认批量持久化条件：

```yaml
checkpoint_flush_interval: 1s
checkpoint_flush_records: 1000
checkpoint_flush_bytes: 64MB
```

只有 durable checkpoint 覆盖到的 segment 才允许删除：

```text
segment.max_lsn <= durable_checkpoint_lsn
```

禁止根据时间、文件数量、磁盘压力或内存 applied_lsn 删除未 checkpoint 的 WAL。

## Replay 规范

WAL replay 采用 `at-least-once`：

- 已 ACK 的 WAL 数据必须至少被处理一次。
- 允许重复 replay。
- 不允许丢失。
- 重复影响由下游基于 `xcontext.event_id` 排重解决。

第一版 replay 严格按 LSN 串行处理。

如果某个 LSN 失败：

- replay 停在当前 LSN。
- 不得跳过。
- 不得推进 checkpoint。
- 不得处理后续 LSN。

服务启动时不要求先 replay 完所有历史 WAL。只要满足以下条件即可 `readyz = true` 并接收新请求：

- WAL writer 恢复完成。
- `max_lsn` 已恢复。
- checkpoint 已读取。
- active segment 可继续追加或可创建新 segment。
- WAL 目录可写。
- 磁盘空间满足要求。

后台从 `checkpoint_lsn + 1` 开始 replay。

## 下游写入规范

只有下游适配器确认数据已经被可靠接收或持久化后，才算成功。

Kafka 成功定义：

```text
send record
  -> broker ack success
  -> DownstreamWriter 返回成功
  -> 允许推进 applied_lsn
```

Parquet 本地文件推荐成功定义：

```text
写临时文件
  -> close / flush writer
  -> fsync 文件
  -> rename 为正式文件
  -> fsync 目录
  -> DownstreamWriter 返回成功
```

统一语义：

```text
DownstreamWriter.Write(event) success = event 已经被下游可靠接收 / 持久化
```

第一版事件路由采用单 sink 模型：

- 每条 route 必须且只能配置一个 `sinks` 目标。
- 不再定义 `ack` 列表。
- replay 只有在该 sink 写入返回成功后，才能推进 checkpoint。

只有该接口返回成功后，才能推进 `applied_lsn`。

## 下游失败处理

如果下游写入失败：

- replay 停在失败 LSN。
- checkpoint 不推进。
- WAL 不删除。
- 不得跳过失败 LSN。
- 新收数请求仍可继续写 WAL 并 ACK。

下游失败会导致 WAL backlog 增长；如果 WAL 磁盘空间不足，则按磁盘空间策略拒收。

下游写入失败时必须无限重试，并使用指数退避：

```yaml
retry_initial_interval: 100ms
retry_max_interval: 30s
retry_multiplier: 2
```

禁止重试 N 次失败后丢弃、推进 checkpoint 或跳过当前 LSN。

## RULES 规范

RULES 负责：

- 检查数据。
- 转换数据。
- 判断规则。
- 决定写入哪个下游。

RULES 不负责最终持久化下游。

checkpoint 推进条件是：

```text
RULES 给出明确处理决策 + 该决策对应的下游写入成功
```

不是 RULES 必须判定业务通过。

RULES 返回结构建议：

```json
{
  "decision": "ROUTE",
  "target": "kafka://normal-topic",
  "status": "accepted",
  "rule_id": "rule_001",
  "rule_version": "v3",
  "reason": "matched normal data rule"
}
```

禁止：

- RULES 判定失败后直接丢弃。
- RULES 判定失败后不写任何下游就推进 checkpoint。
- RULES 没有给出明确处理决策就推进 checkpoint。
- RULES 指定的下游写入失败后推进 checkpoint。

## 磁盘空间与拒收策略

WAL 必须进行磁盘空间检查：

> 能可靠写 WAL，就接收；不能可靠写 WAL，就拒绝。

写 WAL 前应检查当前 WAL 磁盘可用空间，并预估本次事件所需空间：

```text
estimated_wal_bytes =
  wal_record_header_size
  + processed_payload_size
  + metadata_size
  + 安全冗余
```

只有满足以下条件才允许进入 WAL 写入流程：

```text
available_bytes - estimated_wal_bytes >= wal_min_free_bytes
```

以下情况必须返回失败且不得 ACK：

- WAL 空间不足。
- 无法创建 segment。
- write WAL 失败。
- `fdatasync` 或 `fsync` 失败。
- segment rename 失败。
- directory fsync 失败。

当节点 WAL 无法可靠写入时：

- `readyz = false`
- 前端负载均衡应停止向该节点分配新收数请求。

建议暴露：

- `/healthz`
- `/readyz`
- `/wal/status`

WAL 容量不足或无法写入时建议返回：

```http
503 Service Unavailable
```

响应示例：

```json
{
  "error": "wal_capacity_exceeded",
  "message": "WAL disk space is insufficient"
}
```

## 健康检查与监控指标

`readyz = true` 表示当前节点可以可靠接收新事件，并将新事件写入 WAL 后 ACK。

它不要求历史 WAL 已全部 replay 完成。

至少暴露以下指标：

- `wal_max_lsn`
- `wal_checkpoint_lsn`
- `wal_replay_current_lsn`
- `wal_replay_lag_lsn`
- `wal_replay_lag_bytes`
- `wal_backlog_segments`
- `wal_active_segment_id`
- `wal_active_segment_bytes`
- `wal_available_bytes`
- `wal_min_free_bytes`
- `wal_last_fsync_latency_ms`
- `wal_last_fsync_error`
- `wal_group_commit_batch_size`
- `wal_group_commit_bytes`
- `wal_replay_retry_count`
- `wal_downstream_last_error`
- `wal_downstream_retry_delay_ms`
- `wal_ready`

关键告警：

- WAL 可用空间低于阈值。
- `readyz=false`。
- WAL replay lag 持续增长。
- 下游重试持续超过阈值。
- fsync 延迟异常升高。
- checkpoint 长时间不推进。
- active segment 创建失败。
- WAL writer 错误。

## 启动恢复流程

推荐流程：

1. 获取 WAL 目录锁。
2. 读取配置 `wal.node_id`，或读取/生成 `wal/node_id`。
3. 读取 durable checkpoint。
4. 扫描正式 `.wal` segment。
5. 忽略或清理 `.tmp` segment。
6. 校验 segment header。
7. 恢复 `max_lsn`。
8. 定位 active segment。
9. 确认 active segment 可追加，或创建新 segment。
10. 检查 WAL 目录可写。
11. 检查 WAL 磁盘空间。
12. 启动 WAL writer。
13. `readyz=true`。
14. 开始接收新请求。
15. 后台从 `checkpoint_lsn + 1` 开始串行 replay。

## 正常写入流程

```text
客户端上报事件
  -> 接入层完成已有基础处理
  -> 检查事件大小 <= max_event_bytes
  -> 检查 WAL 磁盘空间
  -> 请求进入 WAL append queue
  -> WAL writer 分配 LSN
  -> 构造 WAL record
  -> 写入 active segment
  -> group commit 触发
  -> fdatasync / fsync 成功
  -> 返回 ACK
  -> 后台 replay 读取该 WAL
  -> 执行 RULES
  -> 写入 RULES 指定下游
  -> 下游确认成功
  -> 推进 applied_lsn
  -> 批量持久化 checkpoint
  -> 清理 checkpoint 覆盖的旧 WAL segment
```

## 下游失败流程

```text
replay LSN 102
  -> RULES 给出目标下游
  -> 写 Kafka / Parquet 失败
  -> checkpoint 不推进
  -> WAL 不删除
  -> replay 停在 LSN 102
  -> 指数退避重试
  -> 新收数请求继续进入 WAL
  -> WAL backlog 增长
  -> 如果 WAL 磁盘不足，readyz=false，新请求返回失败
```

## 默认配置

```yaml
server:
  max_event_bytes: 256KB
  max_event_bytes_min: 16KB
  max_event_bytes_max: 1MB

wal:
  segment_size: 128MB
  segment_size_min: 64MB
  segment_size_max: 1GB
  flush_max_interval: 10ms
  flush_max_bytes: 4MB
  flush_max_records: 1000
  min_free_bytes: <按部署环境配置>

checkpoint:
  flush_interval: 1s
  flush_records: 1000
  flush_bytes: 64MB

replay:
  mode: serial
  delivery: at_least_once

downstream_retry:
  initial_interval: 100ms
  max_interval: 30s
  multiplier: 2

node:
  node_id_strategy: configured_or_persistent_random
```

## 第一版核心承诺

1. 服务端 ACK 只在 WAL 持久化成功后返回。
2. 一个事件对应一个 WAL record 和一个 LSN。
3. WAL 使用 segment 文件组织。
4. WAL record 必须包含 header、LSN、payload_len、checksum。
5. 每个 WAL 目录同一时刻只有一个 writer。
6. 允许 group commit，但 ACK 必须等 fsync 成功。
7. `xcontext.event_id` 是业务 ID，WAL 不生成、不改写、不用 `node_id + lsn` 替代。
8. 下游如果采用 `xcontext.event_id` 排重，该字段必须来自客户端或业务 rules。
9. replay 采用 `at-least-once`。
10. replay 第一版串行处理。
11. RULES 决定写入哪个下游。
12. RULES 指定下游写入成功后，才能推进 checkpoint。
13. 下游失败时 replay 停在当前 LSN，指数退避无限重试。
14. 新收数不等待历史 WAL replay 完成。
15. WAL 磁盘不足或无法 fsync 时返回失败，并让负载均衡切走流量。
16. checkpoint 使用 tmp + fsync + rename + directory fsync 持久化。
17. 只有 durable checkpoint 覆盖的 WAL segment 才允许删除。
