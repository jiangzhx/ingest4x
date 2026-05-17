# Transform and delivery

Transformation scripts process WAL records during replay: they validate fields through helper APIs on `event`, optionally mutate/augment events, and decide which event sinks receive events. Implementation is Rhai.

Entry point is fixed:

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

Processor receives two parameters:

| Parameter | Description |
| --- | --- |
| `event` | Event JSON parsed from WAL record body, mutable in script |
| `request` | Request context from WAL metadata and ingress HTTP request |

Processor output is recorded through `emit(target, event)`; `process(...)` return value is not used.

## Transform script API

| API | Description |
| --- | --- |
| `emit(target, event)` | Add delivery record to the chosen sink |
| `epoch_ms()` | Current service timestamp in milliseconds |
| `host_ip()` | Current service node IP |
| `ingest4x_version()` | Current ingest4x version |

Validation helper APIs exposed on `event` are documented in [Validation helpers](event-validation.md).

## Sink constants

Processors should use sink constants, not string targets:

```rhai
emit(SINK_EVENTS, event);
emit(SINK_EVENTS_ERROR, event);
```

Constants are generated from enabled event sink `sink_id`:

| `sink_id` | Rhai constant |
| --- | --- |
| `events` | `SINK_EVENTS` |
| `events_error` | `SINK_EVENTS_ERROR` |
| `kafka-mutated` | `SINK_KAFKA_MUTATED` |

Admin API validates scripts with linting for `emit(...)`: the first argument must be a known sink constant; string targets or unknown constants are rejected.

## Request context

`request` exposes:

| API | Description |
| --- | --- |
| `request.ip()` | Remote request address; returns unit when unavailable |
| `request.method()` | HTTP method |
| `request.path()` | Request path |
| `request.header(name)` | Read header by name (case-insensitive by lower-casing) |
| `request.request_id()` | WAL record ID |
| `request.received_at_ms()` | Ingress receive timestamp (ms) |

Customer-supplied headers such as `authorization` are available in the processor request context. ingest4x only removes its own `x-ingest-token` header before writing WAL.

## Modules and bindings

Processor scripts are persisted in DB and support:

- Default processor.
- Project-specific processor binding.
- Processor modules imported by entry scripts.

Runtime refreshes processor snapshot every `database.refresh_interval_secs`; admin write operations also attempt immediate refresh. Replay always uses current DB config, not the config captured when each record was appended to WAL.

## Failure semantics

| Failure point | Behavior |
| --- | --- |
| Processor compile failure | Admin write or runtime refresh fails |
| Validation helper failure | Raises a normal Rhai runtime error; script may catch it and emit to an error sink |
| Processor runtime error | Replay treats the record as processor failure and moves it to quarantine |
| `emit` target missing | Replay treats as delivery plan error and moves to quarantine |
| Sink commit failure | Not quarantined; pipeline checkpoint does not advance and the replay window retries later |

`blackhole` is production-grade diagnostic sink:
`mode = "ok"` drops events and allows checkpoint progress; `mode = "slow"` succeeds after `delay_ms` delay; `mode = "fail"` returns a sink failure before commit to verify that failed sinks block the pipeline checkpoint and cause WAL backlog.

## Default script

Default seed creates a baseline processor with inline validation logic:

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

In short, `process(...)` owns validation, transformation, and delivery planning in one script. Validation helpers only provide field checks and comparisons.
