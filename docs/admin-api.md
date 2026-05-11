# 管理后台和 API

管理后台运行在 management 端口，默认入口：

```text
http://127.0.0.1:18090/admin
```

管理 API 登录密码来自：

1. 环境变量 `INGEST4X_ADMIN_PASSWORD`
2. 配置项 `management.admin_password`

环境变量优先级更高。前端登录成功后，会把密码保存在当前浏览器会话中，并在后续 `/api/admin/*` 请求里附带：

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

OpenAPI 和 Swagger UI 不需要管理员密码；`/api/admin/*` 业务接口需要 `x-admin-password`。

## 管理资源

| 资源 | API |
| --- | --- |
| 登录 | `POST /api/admin/auth/login` |
| 项目 | `/api/admin/projects` |
| Rule sets | `/api/admin/rule-sets` |
| Project rule binding | `/api/admin/projects/{project_id}/rule-sets` |
| Processor scripts | `/api/admin/processor-scripts` |
| Project processor binding | `/api/admin/projects/{project_id}/processor` |
| Sink types | `/api/admin/sink-types` |
| Delivery targets | `/api/admin/delivery-targets` |
| Event sinks | `/api/admin/event-sinks` |
| Service nodes | `/api/admin/service-nodes` |

## Metrics 和健康检查

管理面提供：

```text
http://127.0.0.1:18090/healthz
http://127.0.0.1:18090/metrics
```

`/healthz` 会检查 WAL 是否 ready。`/metrics` 输出 Prometheus metrics，包括 WAL 状态、replay lag 和 ingest event 统计。
