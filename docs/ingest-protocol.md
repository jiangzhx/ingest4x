# Ingest Protocol

This document describes the HTTP ingest protocol for `/ingest/{project_key}`.

## Supported Request Methods

| Method | Endpoint | Data source | Token source | Notes |
| --- | --- | --- | --- | --- |
| `POST` | `/ingest/{project_key}` | JSON object body with `Content-Type: application/json` | `x-ingest-token` header or JSON root field `x-ingest-token` | Recommended for first-party clients and services. |
| `POST` | `/ingest/{project_key}` | Form body with `Content-Type: application/x-www-form-urlencoded` | `x-ingest-token` header or form field `x-ingest-token` | Recommended for third-party callbacks that cannot set custom headers. |
| `GET` | `/ingest/{project_key}?appid=...&xwhat=...` | Query string fields | `x-ingest-token` header only | Query/path token is not supported. |

`Authorization: Bearer ...` is not supported.

## Project Resolution

The project is resolved only from `{project_key}` in the path:

```text
/ingest/{project_key}
```

Payload fields such as `appid` are business event fields. They are not used to select the project.

There is no compatibility route for `/ingest` without `{project_key}`.

## Auth

Project access is controlled by two project settings:

| Setting | Meaning |
| --- | --- |
| `auth_mode = token` | Request must provide the project's ingest token. |
| `auth_mode = public` | Request does not need an ingest token. |
| `allowed_ips = [...]` | Optional IP allowlist. If configured, the client IP must match before the request can pass. |

`allowed_ips` is independent from `auth_mode`. A project can be public but still restricted by IP allowlist, or token-protected and also restricted by IP allowlist.

For `auth_mode = token`, token resolution depends on request type:

| Request type | Token source |
| --- | --- |
| `POST application/json` | `x-ingest-token` header or JSON root field `x-ingest-token` |
| `POST application/x-www-form-urlencoded` | `x-ingest-token` header or form field `x-ingest-token` |
| `GET` | `x-ingest-token` header only |

If both the header token and POST body/form token exist and differ, the request is rejected.

After auth succeeds, `x-ingest-token` is removed from the event payload before WAL append, processor execution, and sink delivery.

## Event Mapping

### POST JSON

`POST application/json` accepts one JSON object:

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

The JSON object is used as the event body after removing `x-ingest-token` when present.

### POST Form

`POST application/x-www-form-urlencoded` accepts key/value fields:

```text
x-ingest-token=igx_local_test_token&appid=test_app&xwhat=install&installid=iid-1&os=ios
```

Form fields are converted into a flat JSON object:

```json
{
  "appid": "test_app",
  "xwhat": "install",
  "installid": "iid-1",
  "os": "ios"
}
```

Field names are kept as-is. Ingress does not expand dotted paths, infer nested objects, create `xcontext`, create `raw`, or convert string values into numbers or booleans.

### GET Query

`GET /ingest/{project_key}` maps query string fields the same way as form fields:

```bash
curl "http://127.0.0.1:8090/ingest/test_app?appid=test_app&xwhat=install&installid=iid-1&os=ios" \
  -H 'x-ingest-token: igx_local_test_token'
```

The event becomes:

```json
{
  "appid": "test_app",
  "xwhat": "install",
  "installid": "iid-1",
  "os": "ios"
}
```

`GET` no longer supports `data=<base64-json>`. `x-ingest-token` in query string is rejected.

## Request Flow

Checks performed by ingress:

1. Decode request data:
   - `POST application/json`: JSON object body.
   - `POST application/x-www-form-urlencoded`: form fields.
   - `GET`: query string fields; reject `x-ingest-token` when present.
2. Resolve project from `{project_key}` in the path.
3. Enforce `allowed_ips` when the project has an IP allowlist.
4. Authenticate according to `auth_mode`.
5. Reject conflicting header/body token when both are present and different.
6. Remove `x-ingest-token` from POST JSON/form payload.
7. Validate payload size; default `256 KiB`.
8. Read event name from `xwhat`; if absent, internal event name is `default`.
9. Write WAL and return `200` on success.

## Failure Responses

| Scenario | HTTP | Body |
| --- | --- | --- |
| Unknown project key | `404` | `project not found` |
| Missing token for `auth_mode = token` | `401` | `missing ingest token` |
| Invalid token | `401` | `invalid ingest token` |
| Conflicting header/body token | `401` | `conflicting ingest token` |
| Token in query string | `400` | `query ingest token is not supported` |
| IP not allowed | `403` | `ip not allowed` |
| Missing POST body | `400` | `missing request body` |
| Invalid JSON body | `400` | `invalid json payload: ...` |
| Invalid form body | `400` | `invalid form payload: ...` |
| Payload exceeds `ingest.max_event_bytes` | `413` | `Payload Too Large` |
| WAL not writable or insufficient disk | `503` | JSON error payload |

## Security Notes

- Do not put ingest tokens in URL path or query string.
- `project_key` is an identifier, not a secret.
- Query and form decoding is transport-only: no implicit `xcontext`, `raw`, dotted-path expansion, or type conversion.
- Use `auth_mode = token` when the sender can provide `x-ingest-token`.
- Use `auth_mode = public` with `allowed_ips` for callbacks that cannot set headers or body token.
- Ingest token is not written into WAL headers or event payload.
