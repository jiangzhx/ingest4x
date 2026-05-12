# 校验

校验脚本用于检查事件字段，采用 Rhai 编写。入口固定为：

```rhai
fn validate(event) {
    event.required("appid").string().min(1);
    event.required("xwhat").string().min(1);
    event.required("xcontext").object();
    event.required("xcontext.installid").string().min(1);
}
```

这里的 `event` 不是原生 JSON 对象，而是 ingest4x 提供给 Rhai 的封装。校验使用
`event.required(...)`、`event.optional(...)`、`event.field(...)` 去读取字段并记录错误。

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

注意：`.string()` 只做类型校验，需搭配 `.min(1)` 做非空校验。

```rhai
event.required("xwho").string().min(1);
```

## 类型约束

先做类型判断后再加范围/格式约束，例如 `.string()`、`.number()`、`.integer()`。

### 字符串

| API | 说明 |
| --- | --- |
| `.min(n)` | 最小长度 |
| `.enum([...])` | 字符串枚举，需在 `.string()` 之后 |
| `.ignore_case()` | 忽略大小写，常用于 `.enum(...)`、`.eq(...)`、`.matches(...)` |
| `.matches(pattern)` | 正则匹配，需在 `.string()` 之后 |
| `.date(format)` | 日期格式校验（chrono 格式），需在 `.string()` 之后 |
| `.time(format)` | 时间格式校验，需在 `.string()` 之后 |
| `.datetime(format)` | 日期时间格式校验，需在 `.string()` 之后 |

示例：

```rhai
event.required("xwho").string().min(1);
event.required("xcontext.os")
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

上述 API 当前只做类型校验。

```rhai
event.optional("xcontext.paymentstatus").boolean();
event.required("xcontext").object();
event.optional("items").array();
```

### 通用辅助 API

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

### 组合校验

| API | 说明 |
| --- | --- |
| `event.any([...]).required()` | 只要列表中任一字段存在且不为空 |

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

`event.any([...]).required()` 表示列表中至少一个字段存在且不为 `null`。

## 结果语义

校验是“副作用驱动”的。`fn validate(event)` 的返回值不直接决定通过失败，运行时仅读取校验过程中的首个错误。

```rhai
fn validate(event) {
    event.required("appid").string().min(1);
    event.required("xcontext.installid").string().min(1);
}
```

因此不需要显式调用 `event.result()`。`event.result()` 仅保留用于兼容与排障，会把内部状态转换为 map；但它返回值不决定 pass/fail。

行为说明：

- 即使没有 `event.result()`，校验也会按规则执行。
- 即使末尾返回 `{ ok: true }`，若前面已有错误仍会失败。
- 即使末尾返回 `{ ok: false }`，若无错误记录也不会直接失败。

校验失败时，处理器 `validate(event)` 返回类似：

```text
{
  ok: false,
  code: "...",
  message: "...",
  error: "...",
  path: "xcontext.installid"
}
```

## 规则集模型

Rhai 校验规则存储在数据库中的规则集中。当前限制：

- 一条 Rhai 校验规则集只允许启用一个 Rhai 规则。
- Rhai 规则必须是根通配规则（wildcard）。
- Rhai 规则必须定义 `fn validate(event)`。
- 项目与规则集进行绑定。

遗留的 YAML/tree 形式规则与其余实现可能并存，但已启用的 Rhai 规则集不能与其他启用规则类型混用。
