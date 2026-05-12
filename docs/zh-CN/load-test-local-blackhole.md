# ingest4x local blackhole 压测（500 / 1000 / 3000 req/s）

- 日期：2026-05-11
- 场景：`e2e/load/scenarios/blackhole.js`
- Sink 模式：`blackhole` / `ok`
- Ingest 地址：`http://127.0.0.1:18091`
- Admin 地址：`http://127.0.0.1:18092`
- 每轮时长：1m
- 启动模式：本地 `cargo run --bin ingest4x -- server -c e2e/load/ingest4x.load.toml`
- 压测参数通过 `LOAD_*` 环境变量传入。`K6_*` 由 k6 使用并可能覆盖场景配置。

## 概览

| 目标速率 | 实际速率 | WAL 收到总数 | 失败请求率 | p95 延迟 | Drain 窗口后积压 | 结果 |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 500 req/s | 499.936677 req/s | 30001 | 0.0000% | 20.399 ms | 0 | Pass |
| 1000 req/s | 999.835627 req/s | 60000 | 0.0000% | 22.209 ms | 22288 | HTTP pass; replay backlog |
| 3000 req/s | 2999.213727 req/s | 180001 | 0.0000% | 24.182 ms | 153377 | HTTP pass; replay backlog |

该次本地 ingest HTTP 可在 3000 req/s 下无请求失败，p95 小于 100ms。当前瓶颈主要在异步 WAL 重放链路：500 req/s 可全部排空，1000 和 3000 req/s 在 60 秒排空窗口后仍存在积压。

## 500 req/s

| 指标 | 值 |
| --- | ---: |
| 总请求数 | 30001 |
| 请求速率 | 499.936677 req/s |
| 失败率 | 0.0000% |
| checks passed | 60002 / 60002 |
| 平均延迟 | 13.019 ms |
| P90 延迟 | 18.933 ms |
| P95 延迟 | 20.399 ms |
| 最大延迟 | 34.728 ms |
| wal_max_lsn | 30001 |
| wal_checkpoint_lsn | 30001 |
| wal_replay_lag_lsn after | 3800 |
| wal_replay_lag_lsn after drain | 0 |
| wal_append_errors_total | 0 |
| wal_replay_errors_total | 0 |
| ingest_events_total wal_appended | 30001 |

- 产物目录：`e2e/load/runtime/results-500`

## 1000 req/s

| 指标 | 值 |
| --- | ---: |
| 总请求数 | 60000 |
| 请求速率 | 999.835627 req/s |
| 失败率 | 0.0000% |
| checks passed | 120000 / 120000 |
| 平均延迟 | 14.491 ms |
| P90 延迟 | 20.598 ms |
| P95 延迟 | 22.209 ms |
| 最大延迟 | 79.401 ms |
| wal_max_lsn | 60000 |
| wal_checkpoint_lsn final | 37712 |
| wal_replay_lag_lsn after | 39696 |
| wal_replay_lag_lsn final | 22288 |
| wal_append_errors_total | 0 |
| wal_replay_errors_total | 0 |
| ingest_events_total wal_appended | 60000 |

- 结果：HTTP 阈值通过，但 WAL 重放在 60 秒窗口内未完全清空。
- 产物目录：`e2e/load/runtime/results-1000`

## 3000 req/s

| 指标 | 值 |
| --- | ---: |
| 总请求数 | 180001 |
| 请求速率 | 2999.213727 req/s |
| 失败率 | 0.0000% |
| checks passed | 360002 / 360002 |
| 平均延迟 | 16.690 ms |
| P90 延迟 | 22.331 ms |
| P95 延迟 | 24.182 ms |
| 最大延迟 | 99.054 ms |
| wal_max_lsn | 180001 |
| wal_checkpoint_lsn final | 26624 |
| wal_replay_lag_lsn after | 164641 |
| wal_replay_lag_lsn final | 153377 |
| wal_append_errors_total | 0 |
| wal_replay_errors_total | 0 |
| ingest_events_total wal_appended | 180001 |

- 结果：HTTP 阈值通过，但 WAL 重放在 60 秒窗口内未完全清空。
- 产物目录：`e2e/load/runtime/results-3000`
