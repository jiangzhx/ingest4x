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

## stdout

用途：在开发与调试场景下输出到标准输出。

### Delivery target / Event sink

```json
{}
```

`stdout` 不需要额外参数，`delivery target` 与 `destination_json` 均为空对象。
