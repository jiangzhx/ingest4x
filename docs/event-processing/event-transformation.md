# 事件加工和投递

事件加工脚本负责处理 replay 出来的 WAL record：调用项目校验规则、按需要改写或补充事件内容，并决定投递到哪些 event sinks。底层使用 Rhai。

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

Processor 接收两个参数：

| 参数 | 说明 |
| --- | --- |
| `event` | 从 WAL record body 解析出来的 JSON 事件，可在脚本里读取和修改 |
| `request` | 请求上下文，来自 WAL record 保存的 HTTP 元数据 |

Processor 的输出不是函数返回值，而是通过 `emit(target, event)` 记录出来的 deliveries。

## 加工脚本 API

| API | 说明 |
| --- | --- |
| `validate(event)` | 执行当前项目绑定的 rules，返回 `{ ok, code, message, path }` 这类 map |
| `emit(target, event)` | 把事件加入指定 sink 的 delivery 列表 |
| `epoch_ms()` | 当前服务时间戳，毫秒 |
| `host_ip()` | 当前服务节点 IP |
| `ingest4x_version()` | 当前 ingest4x 版本 |

## Sink 常量

Processor 里应该使用 sink 常量，而不是字符串 target：

```rhai
emit(SINK_EVENTS, event);
emit(SINK_EVENTS_ERROR, event);
```

常量由已启用 event sink 的 `sink_id` 生成：

| sink_id | Rhai 常量 |
| --- | --- |
| `events` | `SINK_EVENTS` |
| `events_error` | `SINK_EVENTS_ERROR` |
| `kafka-mutated` | `SINK_KAFKA_MUTATED` |

管理 API 保存 processor 脚本时会 lint `emit(...)` 的第一个参数。字符串 target 或未知常量会被拒绝。

## Request 上下文

`request` 暴露以下方法：

| API | 说明 |
| --- | --- |
| `request.ip()` | 请求远端地址；没有时返回 unit |
| `request.method()` | HTTP method |
| `request.path()` | 请求 path |
| `request.header(name)` | 读取 header，名称会按小写匹配 |
| `request.request_id()` | WAL record ID |
| `request.received_at_ms()` | 接入层收到事件时的毫秒时间戳 |

`authorization` 和 `x-ingest-token` 不会进入 WAL record headers，所以 processor 也读不到这两个认证头。

## 模块和绑定

Processor 脚本存在数据库里，支持：

- 默认 processor。
- 按项目绑定 processor。
- processor module，供入口脚本 import 使用。

运行时会按 `database.refresh_interval_secs` 周期刷新 processor snapshot；管理 API 的写操作也会尝试立即刷新。replay 处理 WAL record 时使用刷新后的当前配置，不使用 record 写入 WAL 时的旧配置。

## 失败语义

| 失败点 | 行为 |
| --- | --- |
| Rules 编译失败 | 管理写入或 replay 编译项目 rules 时失败 |
| Rules 执行失败 | processor 的 `validate(event)` 返回失败结果，默认 processor 会 emit 到 `SINK_EVENTS_ERROR` |
| Processor 编译失败 | 管理写入或 runtime 刷新失败 |
| Processor 执行异常 | WAL replay 将该 record 视为 processor 失败并进入 quarantine |
| `emit` 目标不存在 | WAL replay 将该 record 视为 delivery plan 错误并进入 quarantine |
| Sink 投递失败 | 不进入 quarantine；对应 sink checkpoint 不推进，后续重试 |

`blackhole` sink 是一个生产可用的诊断 sink。`mode = "ok"` 会丢弃事件并推进 checkpoint；`mode = "slow"` 会按 `delay_ms` 延迟后成功；`mode = "fail"` 会返回投递失败，用来验证失败 sink 不推进 checkpoint 和 WAL 积压行为。

## 默认脚本

默认 seed 会创建一套 rules 和 processor。默认 processor 的策略很简单：

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

因此，rules 决定事件是否合法；processor 决定事件如何加工，以及加工后投递到哪个 sink。
