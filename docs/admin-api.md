# Admin Console and API

Admin console is served on the management interface:

```text
http://127.0.0.1:18090/admin
```

Admin API password is resolved by:

1. Environment variable `INGEST4X_ADMIN_PASSWORD`
2. Config `management.admin_password`

Environment variable takes precedence. After successful login, frontend stores the password in session and sends it on `/api/admin/*` requests as:

```text
x-admin-password: <password>
```

## API docs

OpenAPI JSON:

```text
http://127.0.0.1:18090/api-docs/openapi.json
```

Swagger UI:

```text
http://127.0.0.1:18090/swagger-ui/
```

OpenAPI and Swagger UI are publicly accessible; protected business APIs under `/api/admin/*` require `x-admin-password`.

## Admin resources

| Resource | API |
| --- | --- |
| Login | `POST /api/admin/auth/login` |
| Projects | `/api/admin/projects` |
| Rule sets | `/api/admin/rule-sets` |
| Project rule binding | `/api/admin/projects/{project_id}/rule-sets` |
| Processor scripts | `/api/admin/processor-scripts` |
| Project processor binding | `/api/admin/projects/{project_id}/processor` |
| Sink types | `/api/admin/sink-types` |
| Delivery targets | `/api/admin/delivery-targets` |
| Event sinks | `/api/admin/event-sinks` |
| Service nodes | `/api/admin/service-nodes` |

Sink details are defined in [Sink parameters](sink-parameters.md), including full field lists for both `delivery target` and `event sink`.

| Sink type | Purpose | Configuration |
| --- | --- | --- |
| `blackhole` | Drop events, used for diagnostics, load testing, and downstream fault simulation | `delivery target`: `{}`, `event sink`: `mode` / `delay_ms` |
| `kafka` | Deliver to a Kafka topic | `delivery target`: `bootstrap_servers` and related connection options, `event sink`: `topic` |
| `stdout` | Print to stdout | No additional config for both `delivery target` and `event sink` |

Default seed creates `loadtest_app`, `igx_loadtest_token`, `loadtest_blackhole`, `loadtest_events`, and `loadtest_blackhole_processor` for e2e testing. `igx_loadtest_token` is a real writable ingest token. Disable `loadtest_app` or replace the token if a public/customer environment cannot keep load-test ingress open.

## Metrics and health

Admin exposes:

```text
http://127.0.0.1:18090/healthz
http://127.0.0.1:18090/metrics
```

`/healthz` reports WAL readiness. `/metrics` exposes Prometheus metrics for WAL state, replay lag, and ingest event counts.
