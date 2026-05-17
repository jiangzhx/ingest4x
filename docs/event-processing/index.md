# Event processing

`ingest4x` event processing is centered on one Rhai stage during WAL replay:

| Purpose | Entry function | Stage | Main responsibility |
| --- | --- | --- | --- |
| [Transform and delivery](event-transformation.md) | `fn process(event, request)` | WAL replay, per WAL record | Validate fields through `event` helpers, mutate/extend event, decide target event sinks |

Neither stage runs on the ingress `/ingest` request thread. `/ingest` only performs token auth, payload checks, and WAL append; actual processing runs in background replay.

## Overall flow

```text
+--------------------------------------------------------------------------------+
| Replay worker                                                                  |
|                                                                                |
| +------------+    +-----------------------------------------------+            |
| | WAL record | -> | Rhai processor with inline validation helpers |            |
| +------------+    +-----------------------------------------------+            |
|                                                |                               |
|                                                v                               |
|                                      +----------------------+                  |
|                                      | Delivery-ready event  |                  |
|                                      +----------------------+                  |
+--------------------------------------------------------------------------------+
```

For each WAL record, replay loads the current processor bound to the project. If no custom processor exists, the default processor is used.

## Docs

- [Validation helpers](event-validation.md): field paths, presence/type constraints, helper semantics, and error behavior inside `process(...)`.
- [Transform and delivery](event-transformation.md): `process(event, request)`, `emit(...)`, sink constants, request context, module/import usage, and bindings.
