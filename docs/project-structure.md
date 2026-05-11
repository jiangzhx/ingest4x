# 项目结构

这页说明仓库内主要目录和配置文件的职责。README 只保留快速启动和核心运行模型；需要定位模块时看这里。

## 核心目录

| 路径 | 说明 |
| --- | --- |
| `src/ingest` | `/ingest` 接入、payload 解析、项目鉴权和 WAL append |
| `src/wal` | WAL 写入、读取、segment、checkpoint 和 replay |
| `src/rules` | rules 类型、loader、Rhai validation DSL |
| `src/rhai_ctx` | 暴露给 Rhai processor/rules 的宿主 API |
| `src/repositories` | SeaORM-backed 项目、规则、processor、sink、service node 仓储 |
| `src/entities` | SeaORM 实体定义 |
| `src/services` | 跨仓储的运行时服务状态 |
| `src/admin` | 管理 API、OpenAPI 和管理面路由 |
| `src/sinks` | sink provider、delivery target 和运行时投递 |
| `src/db` | 数据库连接、migration、seed 和 schema 初始化 |
| `src/utils` | 通用辅助函数 |
| `src/routes.rs` | 接入面和管理面路由装配 |
| `src/server.rs` | 服务启动、端口绑定、状态初始化和后台任务 |

## 前端

| 路径 | 说明 |
| --- | --- |
| `web/admin` | React 管理后台源码 |
| `web/admin/dist` | 构建后的管理后台静态产物，生产服务会直接托管 |

## 测试和规则样例

| 路径 | 说明 |
| --- | --- |
| `tests/jlt/core` | 默认规则的 JLT 用例 |
| `tests/ingest` | `/ingest` 接入、规则兼容和 seed 相关测试 |
| `tests/wal_tests` | WAL append、replay、checkpoint 和故障处理测试 |

## 配置

| 路径 | 说明 |
| --- | --- |
| `ingest4x.toml` | 本地默认配置，默认使用 SQLite 和 `./wal` |
| `ingest4x.example.toml` | MySQL + Kafka 示例配置 |

## 文档

| 路径 | 说明 |
| --- | --- |
| `docs/index.md` | GitHub Pages 文档首页 |
| `docs/wal.md` | WAL、checkpoint 和 replay |
| `docs/event-processing/index.md` | 事件处理总览 |
| `docs/event-processing/event-validation.md` | 事件校验 |
| `docs/event-processing/event-transformation.md` | 事件加工和投递 |
| `docs/admin-api.md` | 管理后台和 API |
| `docs/release-versioning.md` | 发布和版本 |
| `docs/project-structure.md` | 项目结构 |
