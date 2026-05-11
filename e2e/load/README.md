# E2E load test

这套压测用真实 HTTP、真实 WAL、真实 replay loop、真实 Rhai processor 和真实 sink checkpoint。默认使用 `blackhole` sink，避免 Kafka、内网和下游消费能力成为瓶颈。

## 本地运行

```bash
e2e/load/run.sh
```

默认会：

- 使用 `e2e/load/ingest4x.load.toml` 启动本地服务。
- 使用 `127.0.0.1:18091` 作为 ingest 端口。
- 使用 `127.0.0.1:18092` 作为 management 端口。
- 通过 admin API 创建或更新压测项目、`blackhole` delivery target、`loadtest_events` event sink 和项目专用 processor。
- 用 k6 压 `POST /ingest`。
- 保存 k6 summary 和 Prometheus metrics 到 `e2e/load/runtime/results/`。

常用参数：

```bash
LOAD_RATE=1000 LOAD_DURATION=2m LOAD_PRE_ALLOCATED_VUS=200 LOAD_MAX_VUS=1000 e2e/load/run.sh
```

## 客户集群运行

客户集群已经有 ingest4x 服务时，不要让脚本启动本地进程：

```bash
START_SERVER=0 \
ADMIN_URL=http://customer-host:18090 \
INGEST_URL=http://customer-host:8090 \
ADMIN_PASSWORD='<admin-password>' \
INGEST_TOKEN=igx_customer_loadtest_token \
LOAD_RATE=1000 \
LOAD_DURATION=5m \
e2e/load/run.sh
```

脚本会在客户集群创建或更新专用压测资源，不会写 Kafka：

- project: `loadtest_app`
- delivery target: `loadtest_blackhole`
- event sink: `loadtest_events`
- processor script: `loadtest_blackhole_processor`

压测 payload 会带 `xcontext.test_run_id`，便于从日志和 metrics 中区分批次。

## 下游模拟

吞吐上限测试：

```bash
LOADTEST_SINK_MODE=ok e2e/load/run.sh
```

慢下游测试：

```bash
LOADTEST_SINK_MODE=slow LOADTEST_DELAY_MS=20 e2e/load/run.sh
```

失败下游测试：

```bash
LOADTEST_SINK_MODE=fail e2e/load/run.sh
```

`fail` 模式下 `/ingest` 仍应返回 200，因为请求已经写入 WAL；replay 会因为 sink 投递失败而不推进 `loadtest_events` checkpoint，所以脚本不会等待 `wal_replay_lag_lsn` 清零。

## 观察指标

重点看：

- `http_req_failed`
- `http_req_duration`
- `ingest_events_total{result="wal_appended"}`
- `wal_replay_lag_lsn`
- `wal_append_errors_total`
- `wal_replay_errors_total`

对于 `ok` / `slow` 模式，脚本结束前会等待 `wal_replay_lag_lsn` 回到 0。对于 `fail` 模式，积压是预期结果，用来验证失败 sink 的 WAL 保留和 checkpoint 行为。
