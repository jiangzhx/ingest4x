# ingest4x
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh-CN.md)
> **Status note**
>
> This project is currently at `0.0.1` and is not yet recommended for direct production use. Future releases may change WAL file format and compatibility behavior. Check release notes and migration guidance before upgrading.

Each application usually builds its own event-ingest chain: Nginx/OpenResty first, then something like Flume/Logstash/Filebeat into Kafka or files, then Flink/Spark/custom jobs, while management, monitoring, retry, and rule configuration are spread across systems. `ingest4x` is designed to make these pieces an integrated service.

It mainly addresses four concerns:

- Ingest resilience: auth, validation, and durable persistence are handled at ingress, so downstream instability does not directly reduce ingress success.
- Manageability: each project can define its own validation rules, transformation logic, and delivery targets.
- Delivery reliability: events are persisted to local WAL first, then replayed by background workers; failures are retried and each event sink tracks its own progress.
- Observability: admin UI manages projects, rules, processors, and sinks, with metrics covering ingest, WAL, replay, and delivery.

Thus a successful `/ingest/{project_key}` response means the event is accepted into ingest pipeline. Whether a single event is valid, needs extra fields, and where it is delivered is determined by project configuration.

## Overview

`ingest4x` results are delivered to downstream systems through event sinks. Built-in sink types:

| Sink type | Use case | Main config | Status |
| --- | --- | --- | --- |
| [`blackhole`](docs/sink-parameters.md#blackhole) | Discard events, suitable for production/customer load testing, capacity validation, and downstream fault simulation. | No `delivery target`; `event sink` supports `mode` and `delay_ms`. | Supported |
| [`kafka`](docs/sink-parameters.md#kafka) | Deliver to Kafka topics, suitable for streaming jobs and data platform pipelines. | `delivery target`: `bootstrap_servers`; `event sink`: `topic`. | Supported |
| [`stdout`](docs/sink-parameters.md#stdout) | Print to stdout for local dev, rule debugging, or seed verification. | No extra config. | Supported |

- Ingress: `POST /ingest/{project_key}`, `GET /ingest/{project_key}?appid=...&xwhat=...`.
- Project access: `auth_mode = token | public`, with optional project IP allowlist.
- WAL: local segmented write, checkpoint, per-sink replay, and failure retry. See [WAL](docs/wal.md).
- Rules: Rhai validation rules from DB, bound per project via rule sets.
- Processor: Rhai `process(event, request)` plus `validate(event)` and `emit(target, event)`.
- Sinks: runtime config from DB, default supported sinks listed above.
- Admin: admin console, OpenAPI, Swagger UI, Prometheus metrics, service node registration and heartbeat.
- Storage: SQLite / MySQL with migration and default seed on startup.

### Runtime model

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

## Quick start

### 1. Run core tests

```bash
cargo test --test ingest ingest_jlt_cases_match_rules
```

This initializes default seed in in-memory SQLite and validates default rules with `tests/jlt/core/*.jlt`.

For full local verification:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

HTTP e2e tests are in `e2e/load/`, using `blackhole` sink by default to avoid Kafka/internal downstream bottlenecks:

```bash
e2e/load/run.sh
```

Seed setup includes `loadtest_app` project, `igx_loadtest_token` ingest token, `loadtest_blackhole` delivery target, `loadtest_events` event sink, and `loadtest_blackhole_processor`. If running in public or customer environments, manage this token as a normal writable token. Disable `loadtest_app` from admin when not testing.

Latest local `blackhole` run summary:

- Environment: Apple M5, arm64, 10 logical CPUs, 24 GiB RAM, macOS 26.3.1 (25D771280a)
- Start command: `cargo run --bin ingest4x -- server -c e2e/load/ingest4x.load.toml`
- Duration per target: 1m

| Target rate | Actual rate | WAL received | Failed requests | p95 latency | Replay backlog after drain window | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 500 req/s | 499.936677 req/s | 30001 | 0.0000% | 20.399 ms | 0 | Pass |
| 1000 req/s | 999.835627 req/s | 60000 | 0.0000% | 22.209 ms | 22288 | HTTP pass; replay backlog |
| 3000 req/s | 2999.213727 req/s | 180001 | 0.0000% | 24.182 ms | 153377 | HTTP pass; replay backlog |

Complete local report: [local blackhole load test](docs/load-test-local-blackhole.md).

### 2. Start service

Default root config `ingest4x.toml` uses SQLite at `db/ingest4x.db` and WAL at `./wal`:

```bash
cargo run --bin ingest4x
```

You can also specify a config file explicitly:

```bash
cargo run --bin ingest4x -- server -c ingest4x.toml
```

Default ports:

| Port | Purpose |
| --- | --- |
| `8090` | Ingress: `/ingest/{project_key}` |
| `18090` | Admin: `/healthz`, `/admin`, `/api/admin/*`, `/metrics`, OpenAPI and Swagger UI |

After startup, the seed ensures a local test project exists:

```text
project: test_app
ingest token: igx_local_test_token
```

Admin URL:

```text
http://localhost:18090/admin/
```

Default admin password is from `ingest4x.toml`:

```text
ingest4x
```

If `INGEST4X_ADMIN_PASSWORD` is set, it takes precedence.

### 3. Send POST event

Supported ingest request methods:

| Method | Endpoint | Data shape | Token location | Typical use |
| --- | --- | --- | --- | --- |
| `POST` | `/ingest/{project_key}` | JSON object body | `x-ingest-token` header or JSON root field | First-party clients and services |
| `POST` | `/ingest/{project_key}` | `application/x-www-form-urlencoded` form fields | `x-ingest-token` header or form field | Third-party callbacks that cannot set custom headers |
| `GET` | `/ingest/{project_key}?appid=...&xwhat=...` | Query string fields | `x-ingest-token` header only | Simple test clients and senders that can set headers |

Project access is controlled per project by `auth_mode = token | public`, plus an optional IP allowlist. See [ingest protocol](docs/ingest-protocol.md) for full parsing, auth, and error semantics.

JSON POST example:

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

Form POST example:

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

Successful response:

```text
200
```

Project is resolved from `{project_key}` in the path. Payload or form `appid` is business data and is not used for project routing.

### 4. Send GET event

GET example:

```bash
curl "http://127.0.0.1:8090/ingest/test_app?appid=test_app&xwhat=custom_event&installid=iid-1&os=ios&idfa=idfa-1" \
  -H 'x-ingest-token: igx_local_test_token'
```

GET query fields are converted into one flat JSON object. Field names are kept as-is; ingress does not expand dotted paths or create `xcontext` automatically. GET does not support token in query string or path.

## Request semantics

`/ingest/{project_key}` accepts one event per request; array payloads are not supported. For detailed request decoding, auth behavior, field mapping, and failure responses, see [ingest protocol](docs/ingest-protocol.md).

Default processor implementation:

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

Default seed creates two stdout event sinks:

- `events`
- `events_error`

A default `Local Kafka` delivery target is also created pointing to `127.0.0.1:9092`. To deliver to Kafka, create/enable the corresponding event sink in admin/API.

For local/customer cluster load testing, the default seed also creates:

- project: `loadtest_app`
- ingest token: `igx_loadtest_token`
- delivery target: `loadtest_blackhole`
- event sink: `loadtest_events`
- processor script: `loadtest_blackhole_processor`

This path uses the `blackhole` sink and participates in full WAL replay, processor, sink checkpoint, and metrics chain without writing to external systems. The token `igx_loadtest_token` is a real writable ingest token; in environments where public testing ingress is not allowed, disable `loadtest_app` or rotate/replace the token.

## Configuration

Minimal config example:

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

[wal.write]
flush_interval = "10ms"
flush_records = 1000
no_sync = false
segment_max_bytes = 134217728

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

Key settings:

| Config | Description |
| --- | --- |
| `ingest.bind_address` | ingress listen address |
| `ingest.max_event_bytes` | max payload size per event |
| `management.bind_address` | admin listen address |
| `management.admin_password` | admin API password; `INGEST4X_ADMIN_PASSWORD` has higher priority |
| `database.url` | SQLite or MySQL connection string |
| `database.refresh_interval_secs` | refresh interval for projects/sinks/processors |
| `wal.dir` | WAL data directory |
| `wal.write.no_sync` | `false` means reliable append with fsync-style durability; `true` is a performance-first weaker durability mode |
| `wal.replay.max_records` | max WAL records in one replay window |
| `wal.replay.sink_batch.max_events` | max events in one sink `send_batch` call |

`ingest4x.example.toml` contains a full MySQL + local Kafka sample.

## Admin console and API

See [Admin console and API](docs/admin-api.md) for endpoint URLs, auth behavior, and resource list.

## Validation and transform

Replay processing is two-stage:

- Validation: `fn validate(event)` validates event fields.
- Transformation/delivery: `fn process(event, request)` applies rule set validation, mutates/extends events, and emits to event sinks.

See [Event processing](docs/event-processing/index.md).

## WAL and delivery

For ACK semantics, record format, segment/checkpoint/replay/cleanup/failure handling, see [WAL](docs/wal.md).

## Frontend

```bash
cd web/admin
npm install
npm run dev
```

Frontend checks:

```bash
npm run test
npm run check
```

The production service serves built assets from `web/admin/dist`. Build the frontend before updating embedded admin output.

## Release and versioning

See [release and versioning](docs/release-versioning.md).

## More docs

- [WAL](docs/wal.md)
- [Event processing](docs/event-processing/index.md)
- [Admin console and API](docs/admin-api.md)
- [Local blackhole load report](docs/load-test-local-blackhole.md)
- [Release and versioning](docs/release-versioning.md)
- [Project structure](docs/project-structure.md)
