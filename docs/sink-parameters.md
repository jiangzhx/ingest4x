# Sink parameters

This page summarizes `sink type` configuration for both `delivery target` (connection settings) and `event sink` (delivery settings).

All values are JSON objects, JSON comments are not supported. Both API and frontend validate and parse JSON strictly.

## Common rules

- `delivery target` is configured in the admin `Delivery Target` page.
- `event sink` is configured in the admin `Event Sink` page.
- Configuration must be valid JSON objects.
- Unknown fields are typically rejected by backend strict parser/validator.

## blackhole

Purpose: discard events for load tests, fault injection, and capacity validation.

### Delivery target (`target_type = "blackhole")

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

`blackhole` `destination_json` supports:

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `mode` | string | No | `ok` | One of `ok`, `slow`, or `fail` |
| `delay_ms` | number | No | `0` | Delay in milliseconds before successful response |

Examples:

- Successful delivery: `{"mode":"ok"}`
- Slow downstream: `{"mode":"slow","delay_ms":20}`
- Failed downstream: `{"mode":"fail"}`

## kafka

Purpose: deliver events to a Kafka topic.

### Delivery target (`target_type = "kafka")

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

Supported `config_json` fields:

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `bootstrap_servers` | string | Yes | none | Kafka broker list |
| `delivery_timeout_ms` | string | No | `"3000"` | Producer write timeout |
| `queue_buffering_max_ms` | string | No | `"0"` | `queue.buffering.max.ms` |
| `batch_num_messages` | string | No | `"100"` | `batch.num.messages` |
| `queue_buffering_max_messages` | string | No | `"300"` | `queue.buffering.max.messages` |
| `linger_ms` | string | No | `"100"` | `linger.ms` |

### Event sink `destination_json`

```json
{
  "topic": "events"
}
```

Supported `destination_json` fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `topic` | string | Yes | Kafka topic to send events to |

## stdout

Purpose: write events to service stdout for development and debugging.

### Delivery target / Event sink

```json
{}
```

`stdout` requires no extra parameters for both `delivery target` and `destination_json`.
