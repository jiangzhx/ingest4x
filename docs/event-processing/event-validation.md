# 事件校验

事件校验脚本负责校验事件字段。底层使用 Rhai，入口固定为：

```rhai
fn validate(event) {
    event.required("appid").string().min(1);
    event.required("xwhat").string().min(1);
    event.required("xcontext").object();
    event.required("xcontext.installid").string().min(1);
}
```

`event` 不是普通 JSON object，而是 ingest4x 暴露给 Rhai 的 validation wrapper。脚本通过 `event.required(...)`、`event.optional(...)`、`event.field(...)` 读取字段并记录校验错误。

## 字段路径

字段路径使用点号访问嵌套字段：

```rhai
event.required("xcontext.installid").string().min(1);
event.optional("xcontext.currencyamount").number();
```

## Presence 和类型

| API | 语义 |
| --- | --- |
| `event.required(path)` | 字段必须存在且不能是 `null` |
| `event.optional(path)` | 字段缺失或为 `null` 时跳过后续校验 |
| `event.field(path)` | 只读取字段，不自动要求存在 |
| `.string()` | 必须是 string |
| `.number()` | 必须是 number |
| `.integer()` | 必须是 integer |
| `.boolean()` | 必须是 boolean |
| `.object()` | 必须是 object |
| `.array()` | 必须是 array |

注意：`.string()` 只校验类型，不要求非空字符串。需要非空时继续加 `.min(1)`：

```rhai
event.required("xwho").string().min(1);
```

## 按类型使用约束

不同约束依赖不同字段类型。推荐先用 `.string()`、`.number()`、`.integer()` 等明确类型，再继续链式追加对应约束。

### String

| API | 说明 |
| --- | --- |
| `.min(n)` | 字符串最小长度 |
| `.enum([...])` | 字符串枚举，必须跟在 `.string()` 后 |
| `.ignore_case()` | 后续字符串比较忽略大小写，常和 `.enum(...)`、`.eq(...)`、`.matches(...)` 搭配 |
| `.matches(pattern)` | 正则匹配，必须跟在 `.string()` 后 |
| `.date(format)` | 日期格式校验，使用 chrono 格式，必须跟在 `.string()` 后 |
| `.time(format)` | 时间格式校验，必须跟在 `.string()` 后 |
| `.datetime(format)` | 日期时间格式校验，必须跟在 `.string()` 后 |

示例：

```rhai
event.required("xwho").string().min(1);
event.required("xcontext.os")
    .string()
    .ignore_case()
    .enum(["ios", "android"]);
event.optional("xcontext.event_date").string().date("%Y-%m-%d");
```

### Number / Integer

| API | 说明 |
| --- | --- |
| `.gt(n)` | 大于 |
| `.gte(n)` | 大于等于 |
| `.lt(n)` | 小于 |
| `.lte(n)` | 小于等于 |

这些约束应跟在 `.number()` 或 `.integer()` 后使用。

```rhai
event.required("xcontext.level").integer().gt(0);
event.required("xcontext.currencyamount").number().gte(0.01);
```

### Boolean / Object / Array

这些类型目前主要做类型校验，没有额外的专用链式约束：

```rhai
event.optional("xcontext.paymentstatus").boolean();
event.required("xcontext").object();
event.optional("items").array();
```

### 通用读取和判断

| API | 说明 |
| --- | --- |
| `.eq(value)` | 读取字段并和给定值比较，返回 bool |
| `.exists()` | 判断字段是否存在且不是 `null` |
| `.missing()` | 判断字段不存在或为 `null` |
| `.value()` | 读取字段值 |

这些 API 常用于条件分支：

```rhai
let xwhat = event.required("xwhat").string().min(1);

if xwhat.eq("payment") {
    event.required("xcontext.transactionid").string().min(1);
}
```

### 组合校验

| API | 说明 |
| --- | --- |
| `event.any([...]).required()` | 多个字段里至少有一个存在且不是 `null` |

示例：

```rhai
fn validate(event) {
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
}
```

`event.any([...]).required()` 表示多个字段里至少要有一个存在且非 `null`。

## 结果判定和失败

事件校验是 side-effect DSL。`fn validate(event)` 的返回值不参与 runtime 判定；runtime 会读取 `event.required(...)`、`.string()`、`.min()` 等调用记录在 validation wrapper 内部的第一个错误。

```rhai
fn validate(event) {
    event.required("appid").string().min(1);
    event.required("xcontext.installid").string().min(1);
}
```

因此不需要在脚本最后调用 `event.result()`。`event.result()` 仍然保留为兼容和调试辅助，只是把当前内部状态转成 map；它的返回值不会决定这次校验通过或失败。

具体行为：

- 不调用 `event.result()`，校验仍然生效。
- 即使脚本最后返回 `{ ok: true }`，只要前面记录过错误，runtime 仍然判定失败。
- 即使脚本最后返回 `{ ok: false }`，如果没有通过 DSL 记录错误，runtime 仍然判定通过。

当 rules 校验失败时，processor 里的 `validate(event)` 会得到类似结果：

```text
{
  ok: false,
  code: "...",
  message: "...",
  error: "...",
  path: "xcontext.installid"
}
```

## Rules 存储模型

Rhai validation rule 存在数据库 rule set 里。当前约束是：

- 一个 Rhai validation rule set 只能有一个启用的 Rhai rule。
- 这个 Rhai rule 必须是 root wildcard rule。
- Rhai rule 必须定义 `fn validate(event)`。
- 项目通过 rule set 绑定使用对应 rules。

这和旧的 YAML/tree rule 可以共存于代码层，但一个启用的 Rhai rule set 不能混入其他启用 rule。
