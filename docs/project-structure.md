# Project structure

This file maps important repository directories and configuration boundaries.

## Core

| Path | Purpose |
| --- | --- |
| `src/ingest` | `/ingest` ingress, payload parsing, token auth, and WAL append |
| `src/wal` | WAL write/read, segmenting, checkpoint, and replay |
| `src/rules` | Rule types, loader, Rhai validation DSL |
| `src/rhai_ctx` | Host API exposed to Rhai processors/rules |
| `src/repositories` | SeaORM-backed repositories for projects, rules, processors, sinks, and service nodes |
| `src/entities` | SeaORM entity definitions |
| `src/services` | Runtime service state shared across repositories |
| `src/admin` | Admin APIs, OpenAPI, and admin routes |
| `src/sinks` | Sink providers, delivery targets, and runtime deliveries |
| `src/db` | DB setup, migrations, seed, and schema initialization |
| `src/utils` | Shared utilities |
| `src/routes.rs` | Routing for ingress and admin surfaces |
| `src/server.rs` | Server boot, bind addresses, state initialization, background tasks |

## Frontend

| Path | Purpose |
| --- | --- |
| `web/admin` | React admin source code |
| `web/admin/dist` | Built admin static assets served by the binary |

## Tests and rule samples

| Path | Purpose |
| --- | --- |
| `tests/jlt/core` | Default JLT rule cases |
| `tests/ingest` | `/ingest`, compatibility, and seed-related tests |
| `tests/wal_tests` | WAL append, replay, checkpoint, and failure-handling tests |
| `e2e/load` | k6 + `blackhole` sink HTTP e2e load test suite |

## Configuration

| Path | Purpose |
| --- | --- |
| `ingest4x.toml` | Default local config using SQLite and `./wal` |
| `ingest4x.example.toml` | MySQL + Kafka example config |

## Docs

| Path | Purpose |
| --- | --- |
| `docs/index.md` | GitHub Pages documentation home |
| `docs/wal.md` | WAL, checkpoint, and replay |
| `docs/event-processing/index.md` | Event processing overview |
| `docs/event-processing/event-validation.md` | Validation rule DSL |
| `docs/event-processing/event-transformation.md` | Transformation and delivery |
| `docs/admin-api.md` | Admin console and API |
| `docs/release-versioning.md` | Release and release process |
| `docs/project-structure.md` | Project directory layout |
