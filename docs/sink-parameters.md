# Sink 参数说明

本页汇总各类 `sink type` 的 `delivery target`（连接配置）与 `event sink`（投递配置）参数。

所有配置项都按 JSON 对象提交，不支持 JSON 注释；`api/admin` 与前端都会做 JSON 解析与字段校验。

## 通用约定

- `delivery target` 配置写到管理后台的 `Delivery Target` 页面。
- `event sink` 配置写到管理后台的 `Event Sink` 页面。
- 配置必须是合法 JSON 对象。
- 未声明字段一般会被拒绝（后端使用严格解析）。

## blackhole

用途：丢弃事件，用于压测、故障注入、容量验证。

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

`blackhole` 的 `destination_json` 支持字段：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `mode` | string | 否 | `ok` | 取值 `ok` / `slow` / `fail` |
| `delay_ms` | number | 否 | `0` | 投递前延迟（毫秒） |

示例：

- 成功投递：`{"mode":"ok"}`
- 模拟慢下游：`{"mode":"slow","delay_ms":20}`
- 模拟失败下游：`{"mode":"fail"}`

## kafka

用途：将事件发送到 Kafka topic。

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

`kafka` 的 `config_json` 支持字段：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `bootstrap_servers` | string | 是 | 无 | Kafka broker 列表 |
| `delivery_timeout_ms` | string | 否 | `"3000"` | Kafka 生产者写入超时 |
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

`kafka` 的 `destination_json` 支持字段：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `topic` | string | 是 | 目标 topic |

## stdout

用途：将事件打印到服务标准输出，适合开发、调试。

### Delivery target / Event sink

```json
{}
```

`stdout` 的 `delivery target` 和 `destination_json` 都不需要额外参数。

