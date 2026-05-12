# E2E Load Test

This load test uses real HTTP, real WAL, real replay loop, real Rhai processor, and real sink checkpointing. It uses the `blackhole` sink by default so Kafka, internal network, and downstream consumer capacity do not become the bottleneck.

## Local Run

```bash
e2e/load/run.sh
```

By default, it:

- Starts the local service with `e2e/load/ingest4x.load.toml`.
- Uses `127.0.0.1:18091` as ingest port.
- Uses `127.0.0.1:18092` as management port.
- Reuses the benchmark project in the standard seed, including `blackhole` delivery target, `loadtest_events` event sink, and project-scoped processor.
- Sends k6 load against `POST /ingest`.
- Saves k6 summaries and Prometheus metrics to `e2e/load/runtime/results/`.

Common parameters:

```bash
LOAD_RATE=1000 LOAD_DURATION=2m LOAD_PRE_ALLOCATED_VUS=200 LOAD_MAX_VUS=1000 e2e/load/run.sh
```

Standard seed is preconfigured:

- project: `loadtest_app`
- ingest token: `igx_loadtest_token`
- delivery target: `loadtest_blackhole`
- event sink: `loadtest_events`
- processor script: `loadtest_blackhole_processor`

The local initialization script is no longer run automatically. Before testing, ensure the database includes:

- `loadtest_app`
- `igx_loadtest_token`
- `loadtest_blackhole`
- `loadtest_events`
- `loadtest_blackhole_processor`

`ok/slow/fail` downstream behavior is configured directly in the Admin UI by setting `destination_json` on the blackhole sink (`mode` / `delay_ms`). The script only records the intended behavior via environment variables.

## Customer Cluster Run

If a customer cluster already runs ingest4x, do not let the script start a local process:

```bash
START_SERVER=0 \
ADMIN_URL=http://customer-host:18090 \
INGEST_URL=http://customer-host:8090 \
ADMIN_PASSWORD='<admin-password>' \
LOAD_RATE=1000 \
LOAD_DURATION=5m \
e2e/load/run.sh
```

If the customer cluster already runs a version that includes the standard seed, the script reuses the built-in load-test resources by default and does not write to Kafka:

- project: `loadtest_app`
- ingest token: `igx_loadtest_token`
- delivery target: `loadtest_blackhole`
- event sink: `loadtest_events`
- processor script: `loadtest_blackhole_processor`

If you need to use a customer-specific token or an older database, confirm the corresponding project and blackhole sink/processor are ready in Admin first, then set `INGEST_TOKEN` when running.

Load-test payloads include `xcontext.test_run_id`, which helps correlate runs in logs and metrics.

## Sink Behavior Simulation
## Sink Behavior Simulation

Throughput-capacity test:

```bash
LOADTEST_SINK_MODE=ok e2e/load/run.sh
```

Slow downstream test:

```bash
LOADTEST_SINK_MODE=slow LOADTEST_DELAY_MS=20 e2e/load/run.sh
```

Failing downstream test:

```bash
LOADTEST_SINK_MODE=fail e2e/load/run.sh
```

In `fail` mode, `/ingest` should still return 200 because requests have already been written to WAL; replay stops advancing the `loadtest_events` checkpoint when sink delivery fails, so the script does not wait for `wal_replay_lag_lsn` to return to zero.

## Metrics to Observe
## Metrics to Observe

Focus on:

- `http_req_failed`
- `http_req_duration`
- `ingest_events_total{result="wal_appended"}`
- `wal_replay_lag_lsn`
- `wal_append_errors_total`
- `wal_replay_errors_total`

For `ok` / `slow` modes, the run waits for `wal_replay_lag_lsn` to return to 0 before finishing. For `fail` mode, backlog is expected and validates WAL retention and checkpoint behavior for a failing sink.
