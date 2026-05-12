# 代码结构

该文档整理仓库关键目录与职责边界。

## 核心模块

| 路径 | 职责 |
| --- | --- |
| `src/ingest` | `/ingest` 路由、请求解析、鉴权、WAL append |
| `src/wal` | WAL 写入/读取、分段、checkpoint、重放 |
| `src/rules` | 规则类型、加载、Rhai 校验 DSL |
| `src/rhai_ctx` | 提供给 Rhai processor/rule 的宿主 API |
| `src/repositories` | 基于 SeaORM 的 project/rules/processor/sink/service-node 数据访问 |
| `src/entities` | SeaORM 实体定义 |
| `src/services` | 跨仓库服务状态与共享状态 |
| `src/admin` | 管理 API、OpenAPI、admin 路由 |
| `src/sinks` | Sink 提供者、delivery target、运行时投递 |
| `src/db` | 数据库初始化、migration、seed、Schema 初始化 |
| `src/utils` | 共享工具 |
| `src/routes.rs` | ingress/admin 路由总入口 |
| `src/server.rs` | 服务启动、监听地址、状态初始化、后台任务 |

## 前端

| 路径 | 职责 |
| --- | --- |
| `web/admin` | React 管理端源码 |
| `web/admin/dist` | binary 内置服务的前端构建产物 |

## 测试与规则样例

| 路径 | 职责 |
| --- | --- |
| `tests/jlt/core` | 默认 JLT 规则用例 |
| `tests/ingest` | `/ingest`、兼容性、seed 相关测试 |
| `tests/wal_tests` | WAL append、重放、checkpoint、故障处理 |
| `e2e/load` | k6 + `blackhole` sink 的 HTTP e2e 压测 |

## 配置

| 路径 | 职责 |
| --- | --- |
| `ingest4x.toml` | 默认本地配置（SQLite + `./wal`） |
| `ingest4x.example.toml` | MySQL + Kafka 示例配置 |

## 文档

| 路径 | 职责 |
| --- | --- |
| `docs/index.md` | GitHub Pages 主页 |
| `docs/wal.md` | WAL、checkpoint 与重放 |
| `docs/event-processing/index.md` | 事件处理总览 |
| `docs/event-processing/event-validation.md` | 校验 DSL |
| `docs/event-processing/event-transformation.md` | 转换与投递 |
| `docs/admin-api.md` | 管理端与 API |
| `docs/release-versioning.md` | 发布与版本 |
| `docs/project-structure.md` | 仓库目录 |
