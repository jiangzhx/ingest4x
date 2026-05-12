# Validation

Validation scripts check event fields and are implemented with Rhai. Entry point is fixed:

```rhai
fn validate(event) {
    event.required("appid").string().min(1);
    event.required("xwhat").string().min(1);
    event.required("xcontext").object();
    event.required("xcontext.installid").string().min(1);
}
```

`event` is not a plain JSON object. It is an ingest4x validation wrapper exposed to Rhai. Validation uses
`event.required(...)`, `event.optional(...)`, and `event.field(...)` to read values and record errors.

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

Note: `.string()` validates type only; add `.min(1)` for non-empty checks.

```rhai
event.required("xwho").string().min(1);
```

## Type-specific constraints

Choose constraints after the type check, for example `.string()`, `.number()`, `.integer()`.

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
event.required("xwho").string().min(1);
event.required("xcontext.os")
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

Add these after `.number()` or `.integer()`.

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

### General helper APIs

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

### Compound validation

| API | Description |
| --- | --- |
| `event.any([...]).required()` | At least one of listed fields must exist and be non-null |

Example:

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

`event.any([...]).required()` means at least one field in the list must exist and be non-null.

## Result semantics

Validation is side-effect-based. The return value of `fn validate(event)` is not used directly; runtime reads the first error recorded in validation calls.

```rhai
fn validate(event) {
    event.required("appid").string().min(1);
    event.required("xcontext.installid").string().min(1);
}
```

Therefore calling `event.result()` is not required. `event.result()` is retained for compatibility/debugging and only converts internal state to a map; its return value does not decide pass/fail.

Behavior:

- Validation works even without `event.result()`.
- `{ ok: true }` at end does not force success if DSL previously recorded errors.
- `{ ok: false }` does not force failure if no errors were recorded.

When validation fails, processor `validate(event)` returns something like:

```text
{
  ok: false,
  code: "...",
  message: "...",
  error: "...",
  path: "xcontext.installid"
}
```

## Rule set model

Rhai validation rules are stored in rule sets in DB. Current constraints:

- A Rhai validation rule set can have only one enabled Rhai rule.
- The Rhai rule must be a root wildcard rule.
- The Rhai rule must define `fn validate(event)`.
- Projects bind to rule sets.

Legacy YAML/tree rule forms may coexist in code paths, but an enabled Rhai rule set cannot be mixed with other enabled rule types.
