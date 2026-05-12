# 管理端与 API

管理界面默认在管理端口提供：

```text
http://127.0.0.1:18090/admin
```

管理密码解析顺序：

1. 环境变量 `INGEST4X_ADMIN_PASSWORD`
2. 配置文件 `management.admin_password`

环境变量优先。登录成功后，前端会在 session 中保存密码，并在 `/api/admin/*` 请求中发送：

```text
x-admin-password: <password>
```

## API 文档

OpenAPI JSON：

```text
http://127.0.0.1:18090/api-docs/openapi.json
```

Swagger UI：

```text
http://127.0.0.1:18090/swagger-ui/
```

OpenAPI 与 Swagger UI 为公开可访问；受保护业务接口在 `/api/admin/*` 下需携带 `x-admin-password`。

## 管理资源

| 资源 | API |
| --- | --- |
| 登录 | `POST /api/admin/auth/login` |
| 项目 | `/api/admin/projects` |
| 规则集 | `/api/admin/rule-sets` |
| 项目绑定规则集 | `/api/admin/projects/{project_id}/rule-sets` |
| Processor 脚本 | `/api/admin/processor-scripts` |
| 项目绑定 Processor | `/api/admin/projects/{project_id}/processor` |
| Sink 类型 | `/api/admin/sink-types` |
| Delivery Target | `/api/admin/delivery-targets` |
| Event Sink | `/api/admin/event-sinks` |
| 服务节点 | `/api/admin/service-nodes` |

Sink 的配置细节见 [Sink 参数说明](sink-parameters.md)，包括 `delivery target` 与 `event sink` 的完整字段列表。

| Sink 类型 | 用途 | 配置 |
| --- | --- | --- |
| `blackhole` | 丢弃事件，常用于调试、压测和下游故障注入 | `delivery target`: `{}`，`event sink`: `mode` / `delay_ms` |
| `kafka` | 投递到 Kafka topic | `delivery target`: `bootstrap_servers` 等连接参数，`event sink`: `topic` |
| `stdout` | 输出到标准输出 | `delivery target` 与 `event sink` 均无额外配置 |

默认 seed 会创建 `loadtest_app`、`igx_loadtest_token`、`loadtest_blackhole`、`loadtest_events` 和 `loadtest_blackhole_processor`，用于 e2e 压测；`igx_loadtest_token` 是真实可写 token。若公开环境或客户环境不能长期开放压测入口，请停用 `loadtest_app` 或替换 token。

## 健康检查与指标

管理端提供：

```text
http://127.0.0.1:18090/healthz
http://127.0.0.1:18090/metrics
```

`/healthz` 返回 WAL 是否就绪。`/metrics` 提供 Prometheus 指标，包含 WAL 状态、重放积压和 ingest 计数。
