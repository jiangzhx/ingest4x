# Event processing

`ingest4x` uses two event-processing stages, both implemented in Rhai:

| Purpose | Entry function | Stage | Main responsibility |
| --- | --- | --- | --- |
| [Validation](event-validation.md) | `fn validate(event)` | WAL replay, called by processor via `validate(event)` | Validate whether event fields satisfy project rules |
| [Transform and delivery](event-transformation.md) | `fn process(event, request)` | WAL replay, per WAL record | Run validation rules, mutate/extend event, decide target event sinks |

Neither stage runs on the ingress `/ingest` request thread. `/ingest` only performs token auth, payload checks, and WAL append; actual processing runs in background replay.

## Overall flow

```text
+--------------------------------------------------------------------------------+
| Replay worker                                                                  |
|                                                                                |
| +------------+    +--------------------+    +--------------------------+        |
| | WAL record | -> | Validation rules    | -> | Transformation processor  |        |
| +------------+    +--------------------+    +--------------------------+        |
|                                                |                               |
|                                                v                               |
|                                      +----------------------+                  |
|                                      | Delivery-ready event  |                  |
|                                      +----------------------+                  |
+--------------------------------------------------------------------------------+
```

For each WAL record, replay first loads the current validation rule bound to the project, then the current processor for that project. If no custom processor exists, default processor is used.

## Docs

- [Validation](event-validation.md): field paths, presence/type constraints, result semantics, and rule-set storage model.
- [Transform and delivery](event-transformation.md): `process(event, request)`, `validate(event)`, `emit(...)`, sink constants, request context, module/import usage, and bindings.
