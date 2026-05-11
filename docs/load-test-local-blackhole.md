# ingest4x local blackhole load test: 500 / 1000 / 3000 req/s

- Date: 2026-05-11
- Scenario: `e2e/load/scenarios/blackhole.js`
- Sink mode: `blackhole` / `ok`
- Ingest URL: `http://127.0.0.1:18091`
- Admin URL: `http://127.0.0.1:18092`
- Duration per run: 1m
- Startup mode: local `cargo run --bin ingest4x -- server -c e2e/load/ingest4x.load.toml`
- Load parameters use `LOAD_*` environment variables. `K6_*` is reserved by k6 and can override scenario configuration.

## Summary

| Target rate | Actual rate | WAL received | Failed requests | p95 latency | Replay backlog after drain window | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 500 req/s | 499.936677 req/s | 30001 | 0.0000% | 20.399 ms | 0 | Pass |
| 1000 req/s | 999.835627 req/s | 60000 | 0.0000% | 22.209 ms | 22288 | HTTP pass; replay backlog |
| 3000 req/s | 2999.213727 req/s | 180001 | 0.0000% | 24.182 ms | 153377 | HTTP pass; replay backlog |

The ingest HTTP path handled 3000 req/s locally with no request failures and p95 below 100 ms. The bottleneck in this run is the asynchronous WAL replay path: 500 req/s drained, while 1000 req/s and 3000 req/s still had backlog after the 60s drain window.

## 500 req/s

| Metric | Value |
| --- | ---: |
| Total requests | 30001 |
| Request rate | 499.936677 req/s |
| Failed requests | 0.0000% |
| Checks passed | 60002 / 60002 |
| Latency avg | 13.019 ms |
| Latency p90 | 18.933 ms |
| Latency p95 | 20.399 ms |
| Latency max | 34.728 ms |
| wal_max_lsn | 30001 |
| wal_checkpoint_lsn | 30001 |
| wal_replay_lag_lsn after | 3800 |
| wal_replay_lag_lsn after drain | 0 |
| wal_append_errors_total | 0 |
| wal_replay_errors_total | 0 |
| ingest_events_total wal_appended | 30001 |

- Artifacts: `e2e/load/runtime/results-500`

## 1000 req/s

| Metric | Value |
| --- | ---: |
| Total requests | 60000 |
| Request rate | 999.835627 req/s |
| Failed requests | 0.0000% |
| Checks passed | 120000 / 120000 |
| Latency avg | 14.491 ms |
| Latency p90 | 20.598 ms |
| Latency p95 | 22.209 ms |
| Latency max | 79.401 ms |
| wal_max_lsn | 60000 |
| wal_checkpoint_lsn final | 37712 |
| wal_replay_lag_lsn after | 39696 |
| wal_replay_lag_lsn final | 22288 |
| wal_append_errors_total | 0 |
| wal_replay_errors_total | 0 |
| ingest_events_total wal_appended | 60000 |

- Result: HTTP thresholds passed, but WAL drain timed out at 60s.
- Artifacts: `e2e/load/runtime/results-1000`

## 3000 req/s

| Metric | Value |
| --- | ---: |
| Total requests | 180001 |
| Request rate | 2999.213727 req/s |
| Failed requests | 0.0000% |
| Checks passed | 360002 / 360002 |
| Latency avg | 16.690 ms |
| Latency p90 | 22.331 ms |
| Latency p95 | 24.182 ms |
| Latency max | 99.054 ms |
| wal_max_lsn | 180001 |
| wal_checkpoint_lsn final | 26624 |
| wal_replay_lag_lsn after | 164641 |
| wal_replay_lag_lsn final | 153377 |
| wal_append_errors_total | 0 |
| wal_replay_errors_total | 0 |
| ingest_events_total wal_appended | 180001 |

- Result: HTTP thresholds passed, but WAL drain timed out at 60s.
- Artifacts: `e2e/load/runtime/results-3000`
