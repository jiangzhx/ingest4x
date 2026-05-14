# 接入协议

本文描述 `/ingest/{project_key}` 的 HTTP 接入协议。

## 支持的请求方式

| 方法 | Endpoint | 数据来源 | Token 来源 | 说明 |
| --- | --- | --- | --- | --- |
| `POST` | `/ingest/{project_key}` | `Content-Type: application/json` 的 JSON object body | `x-ingest-token` header 或 JSON 根字段 `x-ingest-token` | 推荐给自有客户端和服务端使用。 |
| `POST` | `/ingest/{project_key}` | `Content-Type: application/x-www-form-urlencoded` 的 form body | `x-ingest-token` header 或 form 字段 `x-ingest-token` | 推荐给不能设置自定义 header 的第三方 callback 使用。 |
| `GET` | `/ingest/{project_key}?appid=...&xwhat=...` | querystring 字段 | 仅 `x-ingest-token` header | 不支持 query/path token。 |

不支持 `Authorization: Bearer ...`。

## 项目解析

项目只从路径里的 `{project_key}` 解析：

```text
/ingest/{project_key}
```

payload 字段里的 `appid` 是业务事件字段，不用于选择项目。

不保留没有 `{project_key}` 的 `/ingest` 兼容路径。

## 鉴权

项目访问由两个项目设置控制：

| 设置 | 含义 |
| --- | --- |
| `auth_mode = token` | 请求必须提供该项目的 ingest token。 |
| `auth_mode = public` | 请求不需要 ingest token。 |
| `allowed_ips = [...]` | 可选 IP allowlist。配置后，客户端 IP 必须匹配才能通过。 |

`allowed_ips` 独立于 `auth_mode`。项目可以是 public 但受 IP allowlist 限制，也可以同时要求 token 和 IP allowlist。

当 `auth_mode = token` 时，token 解析规则如下：

| 请求类型 | Token 来源 |
| --- | --- |
| `POST application/json` | `x-ingest-token` header 或 JSON 根字段 `x-ingest-token` |
| `POST application/x-www-form-urlencoded` | `x-ingest-token` header 或 form 字段 `x-ingest-token` |
| `GET` | 仅 `x-ingest-token` header |

如果 header token 和 POST body/form token 同时存在且不一致，请求会被拒绝。

鉴权成功后，`x-ingest-token` 会在写 WAL、执行 processor、投递 sink 前从事件 payload 中移除。

## 事件映射

### POST JSON

`POST application/json` 接受单个 JSON object：

```json
{
  "x-ingest-token": "igx_local_test_token",
  "appid": "test_app",
  "xwhat": "custom_event",
  "xcontext": {
    "installid": "iid-1",
    "os": "ios"
  }
}
```

移除 `x-ingest-token` 后，JSON object 会作为事件 body 使用。

### POST Form

`POST application/x-www-form-urlencoded` 接受 key/value 字段：

```text
x-ingest-token=igx_local_test_token&appid=test_app&xwhat=install&installid=iid-1&os=ios
```

Form 字段会转换成扁平 JSON object：

```json
{
  "appid": "test_app",
  "xwhat": "install",
  "installid": "iid-1",
  "os": "ios"
}
```

字段名按原样保留。Ingress 不展开 dotted path，不推断嵌套对象，不创建 `xcontext`，不创建 `raw`，也不把字符串自动转成数字或布尔值。

### GET Query

`GET /ingest/{project_key}` 的 querystring 字段按 form 字段同样映射：

```bash
curl "http://127.0.0.1:8090/ingest/test_app?appid=test_app&xwhat=install&installid=iid-1&os=ios" \
  -H 'x-ingest-token: igx_local_test_token'
```

事件会变成：

```json
{
  "appid": "test_app",
  "xwhat": "install",
  "installid": "iid-1",
  "os": "ios"
}
```

`GET` 不再支持 `data=<base64-json>`。querystring 中出现 `x-ingest-token` 会被拒绝。

## 请求流程

入口会执行以下检查：

1. 解码请求数据：
   - `POST application/json`：JSON object body。
   - `POST application/x-www-form-urlencoded`：form 字段。
   - `GET`：querystring 字段；出现 `x-ingest-token` 时拒绝。
2. 从路径 `{project_key}` 解析项目。
3. 如果项目配置了 `allowed_ips`，校验客户端 IP。
4. 按 `auth_mode` 鉴权。
5. 如果 header/body token 同时存在且不一致，拒绝请求。
6. 从 POST JSON/form payload 中移除 `x-ingest-token`。
7. 校验 payload 大小，默认 `256 KiB`。
8. 从 `xwhat` 读取事件名；缺失时内部事件名为 `default`。
9. 写入 WAL，成功后返回 `200`。

## 失败响应

| 场景 | HTTP | Body |
| --- | --- | --- |
| 未知 project key | `404` | `project not found` |
| `auth_mode = token` 但缺少 token | `401` | `missing ingest token` |
| token 无效 | `401` | `invalid ingest token` |
| header/body token 冲突 | `401` | `conflicting ingest token` |
| querystring 中包含 token | `400` | `query ingest token is not supported` |
| IP 不在 allowlist | `403` | `ip not allowed` |
| POST body 为空 | `400` | `missing request body` |
| JSON body 无效 | `400` | `invalid json payload: ...` |
| form body 无效 | `400` | `invalid form payload: ...` |
| payload 超过 `ingest.max_event_bytes` | `413` | `Payload Too Large` |
| WAL 不可写或磁盘不足 | `503` | JSON error payload |

## 安全说明

- 不要把 ingest token 放在 URL path 或 querystring 中。
- `project_key` 是标识符，不是 secret。
- Query 和 form 解码只是 transport decode：没有隐式 `xcontext`、`raw`、dotted path 展开或类型转换。
- 发送方能提供 `x-ingest-token` 时，使用 `auth_mode = token`。
- 对于不能设置 header 或 body token 的 callback，可以使用 `auth_mode = public` 配合 `allowed_ips`。
- ingest token 不会写入 WAL header 或事件 payload。
