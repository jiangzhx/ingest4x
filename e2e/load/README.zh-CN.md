# E2E 压测

该压测使用真实 HTTP、真实 WAL、真实 replay loop、真实 Rhai processor 与真实 sink checkpoint。默认使用 `blackhole` sink，避免 Kafka、内网传输及下游消费能力成为瓶颈。

## 本地运行

```bash
e2e/load/run.sh
```

默认行为：

- 使用 `e2e/load/ingest4x.load.toml` 启动本地服务。
- 使用 `127.0.0.1:18091` 作为 ingest 端口。
- 使用 `127.0.0.1:18092` 作为 management 端口。
- 默认复用标准 seed：`blackhole` delivery target、`loadtest_events` event sink、`loadtest_blackhole` project processor。
- 使用 k6 压测 `POST /ingest`。
- 将 k6 summary 与 Prometheus 指标写入 `e2e/load/runtime/results/`。

常用参数：

```bash
LOAD_RATE=1000 LOAD_DURATION=2m LOAD_PRE_ALLOCATED_VUS=200 LOAD_MAX_VUS=1000 e2e/load/run.sh
```

标准 seed 已内置：

- project: `loadtest_app`
- ingest token: `igx_loadtest_token`
- delivery target: `loadtest_blackhole`
- event sink: `loadtest_events`
- processor script: `loadtest_blackhole_processor`

本地默认不再执行自动初始化脚本。压测前请先确认数据库已包含：

- `loadtest_app`
- `igx_loadtest_token`
- `loadtest_blackhole`
- `loadtest_events`
- `loadtest_blackhole_processor`

`ok/slow/fail` 下游行为请在 Admin 页面直接配置 blackhole sink 的 `destination_json`（`mode` / `delay_ms`），脚本仅通过环境变量记录本次压测的预期行为。

## 客户集群运行

客户集群已有 ingest4x 服务时，不要让脚本启动本地进程：

```bash
START_SERVER=0 \
ADMIN_URL=http://customer-host:18090 \
INGEST_URL=http://customer-host:8090 \
ADMIN_PASSWORD='<admin-password>' \
LOAD_RATE=1000 \
LOAD_DURATION=5m \
e2e/load/run.sh
```

若客户集群已经部署了包含标准 seed 的版本，脚本默认复用内置压测资源且不写 Kafka：

- project: `loadtest_app`
- ingest token: `igx_loadtest_token`
- delivery target: `loadtest_blackhole`
- event sink: `loadtest_events`
- processor script: `loadtest_blackhole_processor`

如需使用客户专用 token 或旧数据库，请先在 Admin 确认对应项目和 blackhole sink/processor 就绪，再设置 `INGEST_TOKEN` 运行。

压测 payload 会携带 `xcontext.test_run_id`，便于从日志和指标区分批次。

## 下游行为模拟

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

在 `fail` 模式下，`/ingest` 仍应返回 200，因为请求已写入 WAL；由于 sink 投递失败，replay 不会推进 `loadtest_events` checkpoint，因此脚本不会等待 `wal_replay_lag_lsn` 归零。

## 观察指标

重点关注：

- `http_req_failed`
- `http_req_duration`
- `ingest_events_total{result="wal_appended"}`
- `wal_replay_lag_lsn`
- `wal_append_errors_total`
- `wal_replay_errors_total`

`ok` / `slow` 场景下，脚本结束前会等待 `wal_replay_lag_lsn` 回到 0。`fail` 场景下积压为预期结果，用于验证失败 sink 的 WAL 保留与 checkpoint 行为。
