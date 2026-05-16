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

## parquet

Purpose: write events as Parquet files through OpenDAL-backed storage.

The storage config follows OpenDAL's `scheme + options` model. Current enabled schemes are `fs`, `s3`, and `cos`; tests cover local filesystem writes.

### Delivery target (`target_type = "parquet"`)

```json
{
  "scheme": "fs",
  "options": {
    "root": "/var/lib/ingest4x/parquet"
  }
}
```

Supported `config_json` fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `scheme` | string | Yes | OpenDAL service scheme, currently `fs`, `s3`, or `cos` |
| `options` | object | Yes | OpenDAL service options, for example `{"root": "/var/lib/ingest4x/parquet"}` for `fs` |

S3/COS examples use OpenDAL option names:

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
  "batch": {
    "max_events": 1000,
    "max_bytes": 16777216
  },
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

Supported `destination_json` fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `path_prefix` | string | Yes | Relative path prefix under the OpenDAL operator root; files are committed as `.parquet` files |
| `batch` | object | No | Per-sink batch override. Missing fields inherit `[wal.replay.sink_batch]` |
| `columns` | array | No | Ordered Parquet projection columns. Each column reads from the emitted JSON event by `path` |
| `include_event_json` | boolean | No | Defaults to `true`; appends the full emitted event as an `event_json` string column |

Supported `batch` fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `max_events` | integer | No | Max events in one `send_batch` call for this sink |
| `max_bytes` | integer | No | Max JSON event bytes in one `send_batch` call for this sink |

Supported column fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | Yes | Output Parquet column name |
| `path` | string | Yes | Dot-separated path in the emitted JSON event, or `$` for the whole event |
| `type` | string | Yes | Physical Parquet type: `string`, `number`, `integer`, `boolean`, or `json` |
| `nullable` | boolean | No | Defaults to `false`; missing or null required values fail the sink write |

`rules` remains the event contract. Parquet `columns` only describe physical projection and column order for this sink. If `columns` is omitted, the sink still writes the full emitted event to the `event_json` column. `batch` is a common Event Sink field, not a Delivery Target field, so different sinks sharing the same storage target can use different batch sizes. WAL pipeline checkpoint advances only after all emitted sink writes in the replay window reach their commit points.

## stdout

Purpose: write events to service stdout for development and debugging.

### Delivery target / Event sink

```json
{}
```

`stdout` requires no extra parameters for both `delivery target` and `destination_json`.
