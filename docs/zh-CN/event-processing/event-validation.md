# 校验 helper

`ingest4x` 当前不再把校验建模成独立的 `fn validate(event)` 阶段。运行时的有效模型是单个 Rhai 入口：

```rhai
fn process(event, request) {
    try {
        event.required("appid").string().min(1);
        event.required("xwhat").string().min(1);
        emit(SINK_EVENTS, event);
    } catch (err) {
        emit(SINK_EVENTS_ERROR, event);
    }
}
```

这里的 `event` 不是原生 JSON 对象，而是 ingest4x 提供给 Rhai 的封装，负责在 `process(...)` 内暴露一组字段校验 helper。

## 字段路径

嵌套字段使用点号路径：

```rhai
event.required("xcontext.installid").string().min(1);
event.optional("xcontext.currencyamount").number();
```

## 存在性与类型

| API | 含义 |
| --- | --- |
| `event.required(path)` | 字段必须存在且不为 `null` |
| `event.optional(path)` | 字段缺失或 `null` 时跳过后续校验 |
| `event.field(path)` | 读取字段，但不要求存在 |
| `.string()` | 校验类型为字符串 |
| `.number()` | 校验类型为数字 |
| `.integer()` | 校验类型为整数 |
| `.boolean()` | 校验类型为布尔 |
| `.object()` | 校验类型为对象 |
| `.array()` | 校验类型为数组 |

`.string()` 只做类型校验；若要求非空字符串，需要继续链 `.min(1)`。

## 类型约束

### 字符串

| API | 说明 |
| --- | --- |
| `.min(n)` | 最小长度 |
| `.enum([...])` | 字符串枚举，需在 `.string()` 之后 |
| `.ignore_case()` | 忽略大小写，常用于 `.enum(...)`、`.eq(...)`、`.matches(...)` |
| `.matches(pattern)` | 正则匹配，需在 `.string()` 之后 |
| `.date(format)` | 日期格式校验，需在 `.string()` 之后 |
| `.time(format)` | 时间格式校验，需在 `.string()` 之后 |
| `.datetime(format)` | 日期时间格式校验，需在 `.string()` 之后 |

示例：

```rhai
let os = event.required("xcontext.os")
    .string()
    .ignore_case()
    .enum(["ios", "android"]);

event.optional("xcontext.event_date").string().date("%Y-%m-%d");
```

### 数值 / 整数

| API | 说明 |
| --- | --- |
| `.gt(n)` | 大于 |
| `.gte(n)` | 大于等于 |
| `.lt(n)` | 小于 |
| `.lte(n)` | 小于等于 |

需在 `.number()` 或 `.integer()` 后使用：

```rhai
event.required("xcontext.level").integer().gt(0);
event.required("xcontext.currencyamount").number().gte(0.01);
```

### 布尔 / 对象 / 数组

目前只做类型校验：

```rhai
event.optional("xcontext.paymentstatus").boolean();
event.required("xcontext").object();
event.optional("items").array();
```

## 通用辅助 API

| API | 说明 |
| --- | --- |
| `.eq(value)` | 和目标值比较，返回布尔 |
| `.exists()` | 字段存在且不为 `null` |
| `.missing()` | 字段不存在或为 `null` |
| `.value()` | 读取原始值 |

常见分支写法：

```rhai
let xwhat = event.required("xwhat").string().min(1);

if xwhat.eq("payment") {
    event.required("xcontext.transactionid").string().min(1);
}
```

## 组合校验

| API | 说明 |
| --- | --- |
| `event.any([...]).required()` | 列表中至少一个字段存在且不为 `null` |

示例：

```rhai
let os = event.required("xcontext.os")
    .string()
    .ignore_case()
    .enum(["ios", "android", "harmony"]);

if os.eq("ios") {
    event.any(["xcontext.idfa", "xcontext.caid"]).required();
}

if os.eq("android") || os.eq("harmony") {
    event.any(["xcontext.oaid", "xcontext.androidid"]).required();
}
```

## 失败语义

校验 helper 不再返回独立的 `result` 对象。每次调用只有两种结果：

- 通过时返回下一个可链式调用的 `FieldRef`
- 失败时抛出普通 Rhai 运行时错误

因此脚本作者自己决定失败后的行为：

```rhai
fn process(event, request) {
    try {
        event.required("appid").string().min(1);
        emit(SINK_EVENTS, event);
    } catch (err) {
        event["xcontext"]["validation_error"] = `${err}`;
        emit(SINK_EVENTS_ERROR, event);
    }
}
```

如果脚本没有捕获错误，重放会把这条记录按 processor runtime failure 处理并隔离。

## 已移除的兼容 API

以下旧式 validator API 不再属于受支持的对外接口：

- `event.result()`
- `event.field(path).required("string")`
- `event.field(path).optional("string")`

当前支持的写法是显式链式语义：

```rhai
event.required("appid").string().min(1);
event.optional("xcontext.paymentstatus").boolean();
```
