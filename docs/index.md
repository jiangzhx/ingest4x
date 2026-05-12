# ingest4x Documentation

`ingest4x` is a standalone event-ingest service that brings together receiving, buffering, validation, transformation, delivery, operations, and monitoring in one place.

## Entry

- [Project README](https://github.com/jiangzhx/ingest4x#readme)
- [WAL, checkpoint, and replay](wal.md)
- [Event processing](event-processing/)
- [Admin console and API](admin-api.md)
- [Sink parameters](sink-parameters.md)
- [Local blackhole load test report](load-test-local-blackhole.md)
- [Release and versioning](release-versioning.md)
- [Project structure](project-structure.md)

## Quick flow

```text
business event
  -> /ingest
  -> WAL
  -> validation
  -> transform and delivery
  -> event sinks
```

GitHub Pages setup:

1. Open repository `Settings -> Pages`.
2. Under `Build and deployment`, choose `Deploy from a branch`.
3. Select branch `main`.
4. Select folder `/docs`.
5. Open `https://jiangzhx.github.io/ingest4x/`.
