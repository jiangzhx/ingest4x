# WAL

WAL is ingest4x’s persistent ingress layer. After `/ingest` receives an event, it does not run rules, processor, or downstream sink delivery in the request thread. It first writes the raw event to local WAL, then background replay reads records and performs business processing and sink delivery.

This decouples ingress ACK from downstream delivery. If Kafka/stdout/processor are temporarily failing, `/ingest` can still return success as long as the event is durably written to WAL; replay later retries or quarantines bad records.

## End-to-end path

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

Key points:

- `/ingest` only accepts a single JSON object, not batch arrays.
- WAL record stores raw payload plus request metadata.
- Field normalization, validation, error marking, and routing happen in replay, not append path.
- Each event sink has an independent checkpoint; one sink stall does not roll back another sink checkpoint.

## ACK semantics

Default is `wal.no_sync = false`. In this mode, `/ingest` returns `200` only after WAL append is complete and the write path waits for segment `sync_data()` success.

```toml
[wal]
no_sync = false
flush_max_interval = "10ms"
flush_max_records = 1000
```

`flush_max_interval` and `flush_max_records` control group commit. Multiple requests may flush together in a short window, reducing sync frequency. With `no_sync = false`, each request waits for its own flush before success.

If `wal.no_sync = true`, append returns after writing to memory buffer and flush happens later in background. This lowers latency, but if process crashes before flush, the most recent unflushed batch may be lost. This mode is not a strong durability ACK.

## WAL record

Core fields:

| Field | Meaning |
| --- | --- |
| `record_id` | Record ID generated at receive time, like `wal-<received_at_ms>-<sequence>` |
| `lsn` | Increasing LSN assigned during append |
| `node_id` | Service node ID persisted in WAL directory |
| `project_id` | Project ID resolved by ingest token |
| `received_at_ms` | Ingress receive timestamp |
| `method` / `path` / `query` | Raw HTTP request info |
| `remote_addr` | Remote socket address |
| `headers` | Request headers excluding filtered sensitive auth fields |
| `body` | Raw event JSON bytes |

Ingest token is not written to WAL headers. `authorization` and `x-ingest-token` are filtered out when generating headers.

`received_at_ms` is passed into processor `request` context. It is ingest receive time, not replay time or client event timestamp.

## Files and directories

Default WAL directory from config:

```toml
[wal]
dir = "./wal"
```

Main files under directory:

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

Notes:

- `node_id`: service node ID. Created on first start; startup fails if configured explicit node ID differs from persisted value.
- `wal.lock`: directory lock preventing concurrent processes on same WAL.
- `*.wal`: segmented WAL files. Each segment has fixed header then consecutive record frames.
- `checkpoints/*.json`: per-event-sink replay checkpoint.

WAL records inside `.wal` are binary frames (not JSONL). Frames include magic/version/payload length/CRC metadata. Payload is Rust-serialized. Do not parse `.wal` as plain text logs when troubleshooting.

## Write and segmenting

WAL starts at segment `1`. Each append allocates:

- `lsn`: global monotonically increasing logical sequence number.
- `segment`: current segment ID.
- `offset`: starting offset of frame in segment.

When the next frame would exceed `wal_segment_max_bytes`, writer creates a new segment. Default:

```toml
[wal]
wal_segment_max_bytes = 134217728
```

Before append, available disk space is checked. If `min_free_bytes` is non-zero and post-write free space is below threshold, append fails and `/ingest` returns `503`.

## Replay

A WAL replay loop starts on service boot. Each run reads up to one batch of entries (default batch size is `1024`).

Per-record flow:

1. Parse `body` as JSON.
2. Verify `project_id` still exists in in-memory registry.
3. Compile and load current project rules.
4. Invoke Rhai processor: `process(event, request)`.
5. Validate processor `emit` sink target exists.
6. Deliver by sink.
7. Advance corresponding sink checkpoint after successful delivery.

Replay uses current DB rules/processor/sink configuration, not the configuration at time of append. So records appended before config changes are processed under new config.

## Checkpoint

Each event sink has independent checkpoint:

```text
<wal.dir>/checkpoints/<sink>.json
```

Checkpoint record:

| Field | Meaning |
| --- | --- |
| `node_id` | WAL node owning this checkpoint |
| `sink_id` | Event sink ID |
| `checkpoint_lsn` | Covered LSN |
| `checkpoint_segment_id` | Segment ID covered |
| `checkpoint_segment_offset` | Next read offset |
| `checksum` | Checkpoint integrity checksum |

Checkpoint write path uses temp file, `sync_data()`, rename, and directory sync to avoid partial checkpoint being treated as valid.

For a new sink with no checkpoint, `auto_offset_reset` defines start point:

| Value | Behavior |
| --- | --- |
| `earliest` | Replay from earliest readable WAL offset |
| `latest` | Initialize checkpoint to current WAL tail and consume only new events |

Default seed uses `latest` for `events`, `events_error`, and `loadtest_events` sinks.

If existing checkpoint is behind current WAL floor, old segments were likely cleaned up and reset follows this sink’s `auto_offset_reset`.

## Retention and cleanup

A WAL segment is cleaned only after all enabled sinks’ minimum checkpoint has passed it. In practice:

- Fast sink checkpoints keep advancing.
- Slow/failing sink checkpoints hold back older segments.
- Global cleanup watermark is minimum checkpoint across all active sinks.

A long-failing sink can block WAL space reuse; fix/disable the sink or explicitly reset checkpoint to recover.

## Failure handling

Replay distinguishes quarantine-able records from transient runtime retries.

Common quarantine cases:

- WAL body is not valid JSON.
- `project_id` no longer exists.
- Processor runtime failure classified as quarantinable record.
- Processor emitted to empty target or unknown sink.

Quarantine does not write to business sinks. Instead it writes a structured event to `ingest4x::wal::quarantine` target containing record ID, LSN, request metadata, error code/message, and base64 original body. The WAL entry is marked handled and checkpoints may continue advancing.

Sink delivery failures are not quarantine. If a sink fails, its checkpoint does not advance; replay retries with backoff. Other sinks that already handled the same record can still advance their checkpoints.

## Observability

Admin `/healthz` returns WAL status:

```json
{
  "status": "ok",
  "wal_enabled": true,
  "wal_ready": true
}
```

If disk is low or WAL unhealthy, `wal_ready` may be `false`, and health check can return `503`.

Admin `/metrics` exports WAL-related Prometheus metrics, including:

| Metric | Meaning |
| --- | --- |
| `wal_node_info` | Current WAL node information |
| `wal_enabled` | Whether WAL is enabled |
| `wal_ready` | Whether WAL is writable |
| `wal_reliable_ack` | Whether reliable ACK mode is enabled |
| `wal_no_sync` | Whether no-sync mode is enabled |
| `wal_available_bytes` | WAL directory free bytes |
| `wal_min_free_bytes` | Configured minimum free bytes |
| `wal_active_segment_id` | Current active segment |
| `wal_active_segment_bytes` | Bytes written in active segment |
| `wal_max_lsn` | Largest writer-allocated LSN |
| `wal_checkpoint_lsn` | Current minimum sink checkpoint level |
| `wal_replay_lag_lsn` | Difference between max LSN and checkpoint LSN |
| `wal_append_errors_total` | WAL append error count |
| `wal_replay_errors_total` | Replay error count |

When backlog exists, check `wal_replay_lag_lsn`, sink error logs, and checkpoint file update times first.

## Config options

Common tuning:

```toml
[wal]
dir = "./wal"
flush_max_interval = "10ms"
flush_max_records = 1000
no_sync = false
wal_segment_max_bytes = 134217728
min_free_bytes = 0
```

```
[wal.checkpoint]
flush_interval = "1s"
flush_records = 1000
flush_bytes = 67108864
```

Meaning:

| Config | Meaning |
| --- | --- |
| `wal.dir` | WAL data directory |
| `wal.flush_max_interval` | Max interval before buffer flush |
| `wal.flush_max_records` | Max records before forced flush |
| `wal.no_sync` | Whether to skip waiting for sync on write |
| `wal.wal_segment_max_bytes` | Max segment size |
| `wal.min_free_bytes` | Minimum free space threshold, if non-zero |
| `wal.checkpoint.flush_interval` | Max interval between checkpoint flushes |
| `wal.checkpoint.flush_records` | Max records before checkpoint flush |
| `wal.checkpoint.flush_bytes` | Max bytes before checkpoint flush |

## Boundary notes

WAL guarantees durability after ingress and replay participation only; it is not a complete business idempotency system.

- WAL does not generate business event IDs.
- WAL does not de-duplicate duplicate submissions.
- Replay can retry the same record if process crashes between sink delivery and checkpoint write; downstream exactly-once systems should use event IDs or business keys.
- WAL payload format is currently coupled to Rust serialization; schema compatibility across old WAL versions is not guaranteed long term.
- In multi-node deployment each node should use unique WAL directory and node ID. Never share one WAL directory across processes.

## Troubleshooting

| Symptom | First checks |
| --- | --- |
| `/ingest` returns `503` | WAL directory permissions, disk space, `wal.lock`, `wal_append_errors_total` |
| `/healthz` shows `wal_ready=false` | `wal.min_free_bytes` and WAL free space |
| Replay backlog | `wal_replay_lag_lsn`, sink connectivity, checkpoint file update time |
| Sink does not consume history | Verify sink `auto_offset_reset` and checkpoint moved to tail |
| Checkpoint file error | Verify `node_id` and checksum integrity |
| `quarantine` log appears | Inspect `ingest4x::wal::quarantine` records and raw body |
