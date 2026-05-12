# 转换与投递

转换脚本在 WAL 重放阶段执行：它会先执行项目规则、可选地变更/扩展事件，并决定事件送达哪些 sink。

入口固定为：

```rhai
fn process(event, request) {
    let validation = validate(event);
    if validation["ok"] {
        emit(SINK_EVENTS, event);
    } else {
        emit(SINK_EVENTS_ERROR, event);
    }
}
```

处理器有两个入参：

| 参数 | 说明 |
| --- | --- |
| `event` | 从 WAL 记录 body 解析出的事件 JSON，可被脚本修改 |
| `request` | 由 WAL 元数据和入口 HTTP 请求拼装的请求上下文 |

处理器通过 `emit(target, event)` 记录投递目标，`process(...)` 的返回值不再使用。

## 转换脚本 API

| API | 说明 |
| --- | --- |
| `validate(event)` | 执行当前项目规则；返回形如 `{ ok, code, message, path }` |
| `emit(target, event)` | 将事件添加到指定 sink 的投递队列 |
| `epoch_ms()` | 当前服务时间戳（毫秒） |
| `host_ip()` | 当前服务节点 IP |
| `ingest4x_version()` | 当前 ingest4x 版本 |

## Sink 常量

处理脚本应使用 sink 常量，而非字符串目标：

```rhai
emit(SINK_EVENTS, event);
emit(SINK_EVENTS_ERROR, event);
```

常量由已启用的 event sink `sink_id` 生成：

| `sink_id` | Rhai 常量 |
| --- | --- |
| `events` | `SINK_EVENTS` |
| `events_error` | `SINK_EVENTS_ERROR` |
| `kafka-mutated` | `SINK_KAFKA_MUTATED` |

Admin API 会在提交时通过 lint 检查 `emit(...)`：第一个参数必须是已知常量，不允许字符串或未知常量。

## 请求上下文

`request` 暴露的能力：

| API | 说明 |
| --- | --- |
| `request.ip()` | 远端请求地址，不可用时返回 unit |
| `request.method()` | HTTP 方法 |
| `request.path()` | 请求路径 |
| `request.header(name)` | 按名读 header（内部统一小写） |
| `request.request_id()` | WAL 记录 ID |
| `request.received_at_ms()` | 接收时间戳（毫秒） |

`authorization` 和 `x-ingest-token` 在 WAL headers 已被过滤，因此处理器无法读取。

## 模块与绑定

处理脚本存储在数据库，支持：

- 默认 processor
- 按项目绑定 processor
- 条目脚本 import 模块

运行时会按 `database.refresh_interval_secs` 周期刷新 processor 快照；管理 API 写入也会触发立即刷新。重放总是使用当前数据库配置，不会固定使用事件写入时的快照。

## 失败语义

| 失败点 | 行为 |
| --- | --- |
| 规则编译失败 | 项目规则编译失败，admin 写入或运行时刷新失败 |
| 规则执行失败 | `validate(event)` 返回失败结果，默认 processor 发到 `SINK_EVENTS_ERROR` |
| 处理脚本编译失败 | Admin 写入或运行时刷新失败 |
| 处理脚本运行失败 | 重放将记录为处理失败并进入隔离 |
| `emit` 目标缺失 | 重放将按投递计划错误进入隔离 |
| sink 投递失败 | 不进入隔离；该 sink checkpoint 不前进，后续重试 |

`blackhole` 作为生产级诊断 sink：
`mode = "ok"` 丢弃后直接前进 checkpoint；`mode = "slow"` 在 `delay_ms` 后成功；`mode = "fail"` 返回投递失败，用于验证失败 sink 下 checkpoint 不前进并形成 WAL backlog。

## 默认脚本

默认 seed 的基础规则和处理逻辑如下：

```rhai
fn process(event, request) {
    let validation = validate(event);
    if validation["ok"] {
        emit(SINK_EVENTS, event);
    } else {
        emit(SINK_EVENTS_ERROR, event);
    }
}
```

简而言之：规则决定事件是否有效，processor 决定如何转换和投递。
