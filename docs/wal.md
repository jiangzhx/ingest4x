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
  -> read WAL after pipeline checkpoint
  -> parse original body
  -> load project rules
  -> run Rhai processor
  -> emit deliveries
  -> batch deliveries by sink and wait for sink-defined commit
  -> advance pipeline checkpoint
  -> cleanup covered WAL segments
```

Key points:

- `/ingest` only accepts a single JSON object, not batch arrays.
- WAL record stores raw payload plus request metadata.
- Field normalization, validation, error marking, and routing happen in replay, not append path.
- Replay has one pipeline checkpoint. If any emitted sink fails, the checkpoint does not advance and the replay window is retried.

## ACK semantics

Default is `wal.write.no_sync = false`. In this mode, `/ingest` returns `200` only after WAL append is complete and the write path waits for segment `sync_data()` success.

```toml
[wal.write]
no_sync = false
flush_interval = "10ms"
flush_records = 1000
```

`wal.write.flush_interval` and `wal.write.flush_records` control group commit. Multiple requests may flush together in a short window, reducing sync frequency. With `wal.write.no_sync = false`, each request waits for its own flush before success.

If `wal.write.no_sync = true`, append returns after writing to memory buffer and flush happens later in background. This lowers latency, but if process crashes before flush, the most recent unflushed batch may be lost. This mode is not a strong durability ACK.

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
| `headers` | Request headers after removing ingest4x's own auth token |
| `body` | Raw event JSON bytes |

Customer-supplied request headers are kept as part of the WAL request context. ingest4x only removes its own `x-ingest-token` header before writing WAL headers.

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
  checkpoint.json
  00000000000000000001.wal
  00000000000000000002.wal
```

Notes:

- `node_id`: service node ID. Created on first start; startup fails if configured explicit node ID differs from persisted value.
- `wal.lock`: directory lock preventing concurrent processes on same WAL.
- `checkpoint.json`: durable pipeline replay checkpoint.
- `*.wal`: segmented WAL files. Each segment has fixed header then consecutive record frames.

WAL records inside `.wal` are binary frames (not JSONL). Frames include magic/version/payload length/CRC metadata. Payload is Rust-serialized. Do not parse `.wal` as plain text logs when troubleshooting.

## Write and segmenting

WAL starts at segment `1`. Each append allocates:

- `lsn`: global monotonically increasing logical sequence number.
- `segment`: current segment ID.
- `offset`: starting offset of frame in segment.

When the next frame would exceed `wal.write.segment_max_bytes`, writer creates a new segment. Default:

```toml
[wal.write]
segment_max_bytes = 134217728
```

Before append, available disk space is checked. If `wal.write.min_free_bytes` is non-zero and post-write free space is below threshold, append fails and `/ingest` returns `503`.

## Replay

A WAL replay loop starts on service boot. Each run reads up to one batch of entries (default read limit is `1024`).

Replay still runs rules and processor per WAL record, because Rhai receives one JSON event at a time. After processor output is validated, deliveries are buffered by sink for the current replay window and each sink receives one `send_batch` call for its pending events. The replay window is flushed when `wal.replay.max_records` or `wal.replay.max_bytes` is reached, so these settings bound a single sink `send_batch` call and the duplicate-delivery window after partial sink success.

Per-record planning flow:

1. Parse `body` as JSON.
2. Verify `project_id` still exists in in-memory registry.
3. Compile and load current project rules.
4. Invoke Rhai processor: `process(event, request)`.
5. Validate processor `emit` sink target exists.

Delivery flow:

1. Group planned deliveries by sink target.
2. Call each sink once with the batched JSON events for the current replay window.
3. Advance the pipeline checkpoint only after every emitted sink batch reaches its sink-defined commit point.

Each sink defines its own commit point. Kafka can treat a successful broker delivery report as commit; local file sinks must wait for their final file commit; object storage sinks must wait for upload completion or multipart complete. WAL replay only observes the sink runtime result: all `Ok` results allow pipeline checkpoint progress; any `Err` keeps the checkpoint behind for retry.

Replay uses current DB rules/processor/sink configuration, not the configuration at time of append. So records appended before config changes are processed under new config.

The processor/sink boundary uses JSON events as the common in-memory format. Columnar sinks such as Parquet convert the batched JSON events into Arrow arrays internally during encoding; Kafka/stdout can continue to use JSON without an Arrow round trip.

## Checkpoint

Replay has one pipeline checkpoint:

```text
<wal.dir>/checkpoint.json
```

Checkpoint record:

| Field | Meaning |
| --- | --- |
| `node_id` | WAL node owning this checkpoint |
| `sink_id` | Always `null` for pipeline checkpoint |
| `checkpoint_lsn` | Covered LSN |
| `checkpoint_segment_id` | Segment ID covered |
| `checkpoint_segment_offset` | Next read offset |
| `checksum` | Checkpoint integrity checksum |

Checkpoint write path uses temp file, `sync_data()`, rename, and directory sync to avoid partial checkpoint being treated as valid.

When no checkpoint exists, active sink `auto_offset_reset` settings define the pipeline start point:

| Value | Behavior |
| --- | --- |
| any sink is `earliest` | Replay from earliest readable WAL offset |
| all sinks are `latest` | Initialize checkpoint to current WAL tail and consume only new events |

Default seed uses `latest` for `events`, `events_error`, and `loadtest_events` sinks.

If the existing checkpoint is behind current WAL floor, old segments were likely cleaned up and reset follows the same pipeline `auto_offset_reset` merge rule.

## Retention and cleanup

A WAL segment is cleaned only after the pipeline checkpoint has passed it. In practice:

- A slow or failing sink blocks pipeline checkpoint progress.
- Sinks that committed before another sink failed may receive duplicate events on retry.
- Downstream sinks should be idempotent if duplicate delivery is not acceptable.

A long-failing sink can block WAL space reuse; fix or disable the sink, then replay can advance again.

## Failure handling

Replay distinguishes quarantine-able records from transient runtime retries.

Common quarantine cases:

- WAL body is not valid JSON.
- `project_id` no longer exists.
- Processor runtime failure classified as quarantinable record.
- Processor emitted to empty target or unknown sink.

Quarantine does not write to business sinks. Instead it writes a structured event to `ingest4x::wal::quarantine` target containing record ID, LSN, request metadata, error code/message, and base64 original body. The WAL entry is marked handled and checkpoints may continue advancing.

Sink commit failures are not quarantine. If any emitted sink fails before reaching its own commit point, the pipeline checkpoint does not advance and replay retries with backoff. Other sinks that already committed events from the same replay window can receive those events again on retry.

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
| `wal_checkpoint_lsn` | Current pipeline checkpoint level |
| `wal_replay_lag_lsn` | Difference between max LSN and checkpoint LSN |
| `wal_append_errors_total` | WAL append error count |
| `wal_replay_errors_total` | Replay error count |

When backlog exists, check `wal_replay_lag_lsn`, sink error logs, and checkpoint file update times first.

## Config options

Common tuning:

```toml
[wal]
dir = "./wal"

[wal.write]
flush_interval = "10ms"
flush_records = 1000
no_sync = false
segment_max_bytes = 134217728
min_free_bytes = 0
```

```toml
[wal.checkpoint]
flush_interval = "1s"
flush_records = 1000
flush_bytes = 67108864

[wal.replay]
max_records = 1000
max_bytes = 67108864
```

Meaning:

| Config | Meaning |
| --- | --- |
| `wal.dir` | WAL data directory |
| `wal.write.flush_interval` | Max interval before writer buffer flush |
| `wal.write.flush_records` | Max records before forced writer flush |
| `wal.write.no_sync` | Whether to skip waiting for sync on write |
| `wal.write.segment_max_bytes` | Max segment size |
| `wal.write.min_free_bytes` | Minimum free space threshold, if non-zero |
| `wal.checkpoint.flush_interval` | Max interval between checkpoint flushes |
| `wal.checkpoint.flush_records` | Max successfully replayed records before checkpoint file flush |
| `wal.checkpoint.flush_bytes` | Max successfully replayed WAL bytes before checkpoint file flush |
| `wal.replay.max_records` | Max WAL records in one replay window / sink batch |
| `wal.replay.max_bytes` | Max WAL bytes in one replay window / sink batch |

## Boundary notes

WAL guarantees durability after ingress and replay participation only; it is not a complete business idempotency system.

- WAL does not generate business event IDs.
- WAL does not de-duplicate duplicate submissions.
- Replay can retry the same record if process crashes between sink commit and checkpoint write, or if another sink blocks pipeline checkpoint progress after one sink has already committed. Downstream exactly-once systems should use event IDs or business keys.
- WAL payload format is currently coupled to Rust serialization; schema compatibility across old WAL versions is not guaranteed long term.
- In multi-node deployment each node should use unique WAL directory and node ID. Never share one WAL directory across processes.

## Troubleshooting

| Symptom | First checks |
| --- | --- |
| `/ingest` returns `503` | WAL directory permissions, disk space, `wal.lock`, `wal_append_errors_total` |
| `/healthz` shows `wal_ready=false` | `wal.write.min_free_bytes` and WAL free space |
| Replay backlog | `wal_replay_lag_lsn`, sink connectivity, checkpoint file update time |
| Sink does not consume history | Verify sink `auto_offset_reset` and checkpoint moved to tail |
| Checkpoint file error | Verify `node_id` and checksum integrity |
| `quarantine` log appears | Inspect `ingest4x::wal::quarantine` records and raw body |
