# Transform and delivery

Transformation scripts process WAL records during replay: they run project validation rules, optionally mutate/augment events, and decide which event sinks receive events. Implementation is Rhai.

Entry point is fixed:

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

Processor receives two parameters:

| Parameter | Description |
| --- | --- |
| `event` | Event JSON parsed from WAL record body, mutable in script |
| `request` | Request context from WAL metadata and ingress HTTP request |

Processor output is recorded through `emit(target, event)`; `process(...)` return value is not used.

## Transform script API

| API | Description |
| --- | --- |
| `validate(event)` | Run current project rules; returns a map like `{ ok, code, message, path }` |
| `emit(target, event)` | Add delivery record to the chosen sink |
| `epoch_ms()` | Current service timestamp in milliseconds |
| `host_ip()` | Current service node IP |
| `ingest4x_version()` | Current ingest4x version |

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

`authorization` and `x-ingest-token` are filtered from WAL headers, so processor cannot read them.

## Modules and bindings

Processor scripts are persisted in DB and support:

- Default processor.
- Project-specific processor binding.
- Processor modules imported by entry scripts.

Runtime refreshes processor snapshot every `database.refresh_interval_secs`; admin write operations also attempt immediate refresh. Replay always uses current DB config, not the config captured when each record was appended to WAL.

## Failure semantics

| Failure point | Behavior |
| --- | --- |
| Rules compile failure | Admin write or runtime replay compile of project rules fails |
| Rule execution failure | `validate(event)` returns failed result; default processor emits to `SINK_EVENTS_ERROR` |
| Processor compile failure | Admin write or runtime refresh fails |
| Processor runtime error | Replay treats the record as processor failure and moves it to quarantine |
| `emit` target missing | Replay treats as delivery plan error and moves to quarantine |
| Sink delivery failure | Not quarantined; target sink checkpoint does not advance and will retry later |

`blackhole` is production-grade diagnostic sink:
`mode = "ok"` drops events and advances checkpoint; `mode = "slow"` succeeds after `delay_ms` delay; `mode = "fail"` returns delivery failure to verify that failed sinks do not advance checkpoints and cause WAL backlog.

## Default script

Default seed creates a baseline rule and processor with simple logic:

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

In short, rules decide whether an event is valid; processor decides how to transform and where to deliver it.
