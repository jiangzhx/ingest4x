# Sink 参数

本页汇总 `sink type` 在 `delivery target`（连接配置）与 `event sink`（投递配置）两侧的参数。

所有配置均为 JSON 对象。JSON 不支持注释。后端与前端都对 JSON 做严格校验和解析。

## 通用规则

- `delivery target` 在管理台 `Delivery Target` 页面配置。
- `event sink` 在管理台 `Event Sink` 页面配置。
- 配置必须是合法 JSON 对象。
- 后端严格解析器通常会拒绝未定义字段。

## blackhole

用途：丢弃事件，用于压测、故障注入与吞吐验证。

### Delivery target (`target_type = "blackhole"`)

```json
{}
```

### Event sink `destination_json`

```json
{
  "mode": "ok",
  "delay_ms": 0
}
```

`blackhole` 的 `destination_json` 字段：

| 字段 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `mode` | string | 否 | `ok` | 取值 `ok` / `slow` / `fail` |
| `delay_ms` | number | 否 | `0` | 成功返回前的延迟（毫秒） |

示例：

- 成功下发：`{"mode":"ok"}`
- 模拟慢下游：`{"mode":"slow","delay_ms":20}`
- 模拟失败：`{"mode":"fail"}`

## kafka

用途：将事件投递到 Kafka topic。

### Delivery target (`target_type = "kafka"`)

```json
{
  "bootstrap_servers": "127.0.0.1:9092",
  "delivery_timeout_ms": "3000",
  "queue_buffering_max_ms": "0",
  "batch_num_messages": "100",
  "queue_buffering_max_messages": "300",
  "linger_ms": "100"
}
```

支持的 `config_json` 字段：

| 字段 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `bootstrap_servers` | string | 是 | 无 | Kafka broker 列表 |
| `delivery_timeout_ms` | string | 否 | `"3000"` | Producer 写超时 |
| `queue_buffering_max_ms` | string | 否 | `"0"` | `queue.buffering.max.ms` |
| `batch_num_messages` | string | 否 | `"100"` | `batch.num.messages` |
| `queue_buffering_max_messages` | string | 否 | `"300"` | `queue.buffering.max.messages` |
| `linger_ms` | string | 否 | `"100"` | `linger.ms` |

### Event sink `destination_json`

```json
{
  "topic": "events"
}
```

支持的 `destination_json` 字段：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `topic` | string | 是 | 目标 topic |

## parquet

用途：通过 OpenDAL-backed storage 将事件写成 Parquet 文件。

存储配置采用 OpenDAL 的 `scheme + options` 模型。当前启用的 scheme 是 `fs`、`s3` 和 `cos`；测试覆盖本地文件系统写入。

### Delivery target (`target_type = "parquet"`)

```json
{
  "scheme": "fs",
  "options": {
    "root": "/var/lib/ingest4x/parquet"
  }
}
```

支持的 `config_json` 字段：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `scheme` | string | 是 | OpenDAL service scheme，当前支持 `fs`、`s3` 或 `cos` |
| `options` | object | 是 | OpenDAL service options，例如 `fs` 使用 `{"root": "/var/lib/ingest4x/parquet"}` |

S3/COS 示例使用 OpenDAL option 名称：

```json
{
  "scheme": "s3",
  "options": {
    "bucket": "ingest4x",
    "region": "ap-shanghai",
    "endpoint": "https://s3.example.com",
    "access_key_id": "...",
    "secret_access_key": "..."
  }
}
```

```json
{
  "scheme": "cos",
  "options": {
    "bucket": "ingest4x-1250000000",
    "region": "ap-shanghai",
    "secret_id": "...",
    "secret_key": "..."
  }
}
```

### Event sink `destination_json`

```json
{
  "path_prefix": "events",
  "columns": [
    {
      "name": "appid",
      "path": "appid",
      "type": "string"
    },
    {
      "name": "xwhat",
      "path": "xwhat",
      "type": "string"
    },
    {
      "name": "installid",
      "path": "xcontext.installid",
      "type": "string",
      "nullable": true
    }
  ],
  "include_event_json": true
}
```

支持的 `destination_json` 字段：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `path_prefix` | string | 是 | OpenDAL operator root 下的相对路径前缀；文件最终以 `.parquet` 提交 |
| `columns` | array | 否 | 有序 Parquet 投影列；每列通过 `path` 从 emit 出来的 JSON event 取值 |
| `include_event_json` | boolean | 否 | 默认 `true`；追加完整 emit event 到 `event_json` 字符串列 |

支持的 column 字段：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `name` | string | 是 | 输出 Parquet 列名 |
| `path` | string | 是 | emit JSON event 中的点分路径，或用 `$` 表示整个 event |
| `type` | string | 是 | 物理 Parquet 类型：`string`、`number`、`integer`、`boolean` 或 `json` |
| `nullable` | boolean | 否 | 默认 `false`；必填值缺失或为 null 时 sink 写入失败 |

`rules` 仍然是事件契约。Parquet `columns` 只描述当前 sink 的物理投影与列顺序。如果省略 `columns`，sink 仍会把完整 emit event 写入 `event_json` 列。只有当前 replay window 内所有已 emit sink 的写入都达到各自 commit 点后，WAL pipeline checkpoint 才会前进。

## stdout

用途：在开发与调试场景下输出到标准输出。

### Delivery target / Event sink

```json
{}
```

`stdout` 不需要额外参数，`delivery target` 与 `destination_json` 均为空对象。
