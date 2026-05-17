# Validation helpers

Validation in `ingest4x` is no longer modeled as a separate `fn validate(event)` stage. The active runtime model is a single Rhai entrypoint:

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

`event` is not a plain JSON object. It is an ingest4x wrapper that exposes validation helpers for use inside `process(...)`.

## Field path

Nested fields use dot notation:

```rhai
event.required("xcontext.installid").string().min(1);
event.optional("xcontext.currencyamount").number();
```

## Presence and type

| API | Meaning |
| --- | --- |
| `event.required(path)` | Field must exist and not be `null` |
| `event.optional(path)` | Skip subsequent checks when missing or `null` |
| `event.field(path)` | Read field without requiring existence |
| `.string()` | Type must be string |
| `.number()` | Type must be number |
| `.integer()` | Type must be integer |
| `.boolean()` | Type must be boolean |
| `.object()` | Type must be object |
| `.array()` | Type must be array |

`.string()` validates type only. Add `.min(1)` when the string must be non-empty.

## Type-specific constraints

### String

| API | Description |
| --- | --- |
| `.min(n)` | Minimum string length |
| `.enum([...])` | String enumeration, must follow `.string()` |
| `.ignore_case()` | Case-insensitive compare; commonly used with `.enum(...)`, `.eq(...)`, `.matches(...)` |
| `.matches(pattern)` | Regex match, must follow `.string()` |
| `.date(format)` | Date format check using chrono format, must follow `.string()` |
| `.time(format)` | Time format check, must follow `.string()` |
| `.datetime(format)` | Date-time format check, must follow `.string()` |

Example:

```rhai
let os = event.required("xcontext.os")
    .string()
    .ignore_case()
    .enum(["ios", "android"]);

event.optional("xcontext.event_date").string().date("%Y-%m-%d");
```

### Number / Integer

| API | Description |
| --- | --- |
| `.gt(n)` | Greater than |
| `.gte(n)` | Greater than or equal |
| `.lt(n)` | Less than |
| `.lte(n)` | Less than or equal |

Use these after `.number()` or `.integer()`.

```rhai
event.required("xcontext.level").integer().gt(0);
event.required("xcontext.currencyamount").number().gte(0.01);
```

### Boolean / Object / Array

These are type checks only today.

```rhai
event.optional("xcontext.paymentstatus").boolean();
event.required("xcontext").object();
event.optional("items").array();
```

## General helper APIs

| API | Description |
| --- | --- |
| `.eq(value)` | Compare field value with `value`, returns boolean |
| `.exists()` | Whether field exists and is not `null` |
| `.missing()` | Whether field is missing or `null` |
| `.value()` | Read raw field value |

These are useful for branching:

```rhai
let xwhat = event.required("xwhat").string().min(1);

if xwhat.eq("payment") {
    event.required("xcontext.transactionid").string().min(1);
}
```

## Compound validation

| API | Description |
| --- | --- |
| `event.any([...]).required()` | At least one of listed fields must exist and be non-null |

Example:

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

## Failure semantics

Validation helpers do not return a structured `result` object anymore. They either:

- return the next `FieldRef` in the chain when the check passes
- raise a normal Rhai runtime error when the check fails

That means processor authors decide the behavior:

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

If the script does not catch the error, replay treats it as a processor runtime failure and quarantines the record.

## Removed compatibility APIs

These legacy validator APIs are no longer part of the supported surface:

- `event.result()`
- `event.field(path).required("string")`
- `event.field(path).optional("string")`

The supported style is the explicit chain:

```rhai
event.required("appid").string().min(1);
event.optional("xcontext.paymentstatus").boolean();
```
