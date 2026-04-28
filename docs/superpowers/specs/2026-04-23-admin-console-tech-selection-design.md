# ingest4x 管理后台技术选型设计

## 背景

`ingest4x` 当前已经具备 `Actix Web` 后端服务能力，但还缺少面向运营和技术人员的管理界面。

第一阶段的管理需求比较明确，主要集中在基础配置型 CRUD：

- 项目配置管理
- Token 管理
- Rules 管理

同时，项目有几个明确约束：

- 保持单仓库
- 生产部署尽量简单
- 由现有 `Actix Web` 直接托管前端构建产物
- 不额外引入新的重型前端框架层
- 管理台本质上是单页面后台

本次设计的目标不是一次性定义完整后台产品，而是确定第一版管理后台的技术选型、`projects` 持久化方案和落地边界。

## 目标

- 为管理后台确定一套可长期演进的前端技术栈
- 让 `projects` 从配置/Redis 校验迁移到数据库主存储
- 满足第一版 `projects` 的基础 CRUD 需求，并为 `tokens`、`rules` 预留后续接入方式
- 保持与现有 `Actix Web` 后端部署模型兼容
- 为后续扩展详情页、编辑页、调试页预留空间

## 非目标

- 不在本次设计中引入后台低代码框架或元框架
- 不在本次设计中引入服务端渲染或 Node.js 前端运行时
- 不在本次设计中设计完整权限系统、审计日志或复杂工作流
- 不在本次设计中把 `tokens`、`rules` 一并迁移到数据库
- 不在本次设计中实现规则版本发布、对比、在线测试等高级能力

## 选型结论

第一版管理后台采用以下技术组合：

- `React`
- `Vite`
- `TypeScript`
- `Ant Design`
- `TanStack Query`
- `TanStack Router`

后端保持：

- `Actix Web`
- `SQLite`
- `SeaORM`

部署方式保持：

- 前端构建为静态资源
- 由 `Actix Web` 直接托管到管理后台入口路径

## 选型理由

### React

管理后台属于典型的组件化 CRUD 界面，`React` 在这类场景下生态成熟、团队接受度高，也便于后续引入更复杂的页面拆分和状态组织。

### Vite

`Vite` 适合当前项目的部署模型：

- 开发阶段启动快
- 生产阶段直接输出静态资源
- 不要求额外 Node.js 服务常驻
- 与 `Actix Web` 托管静态文件的模式天然匹配

它比 `Next.js` 更符合当前“单 Rust 服务部署”的目标。

### Ant Design

管理后台第一版以表格、表单、抽屉、弹窗、分页、筛选为主，`Ant Design` 在这些企业后台组件上最成熟，能显著降低页面开发成本。

相较于 `shadcn/ui` 这类偏组件基底的方案，`Ant Design` 更适合快速落地标准后台 CRUD。

### TanStack Query

管理后台的大多数页面都围绕服务端数据展开，`TanStack Query` 负责：

- 列表查询
- 详情查询
- 创建、更新、删除后的失效与刷新
- 减少手写请求状态管理

这比把请求状态散落在组件内部更稳定，也更适合后续扩展。

### TanStack Router

虽然管理台是单页面后台，但仍然需要标准前端路由能力：

- `projects`、`tokens`、`rules` 需要独立 URL
- 页面支持刷新后保持当前视图
- 支持浏览器前进和后退
- 便于后续增加详情页、编辑页和调试页

因此管理台采用 SPA 形态，但保留前端路由，而不是做成单页内纯本地状态切换。

### SQLite

`projects` 第一版更接近配置型主数据，而不是高并发交易数据。使用 `SQLite` 有几个直接优势：

- 保持单服务部署，不需要额外数据库实例
- 本地开发和测试成本低
- 与当前“单仓库、单 Rust 服务优先”的约束一致
- 足够承载第一版 `projects` 管理和查询

### SeaORM

当前仓库还没有 ORM 依赖。第一版若要为后续 `tokens`、`rules` 扩展留出空间，建议直接引入 `SeaORM`：

- 与 `Actix Web` 的异步模型更自然
- 自带 entity / migration 组织方式，便于后续继续长出更多管理表
- 对 `SQLite` 支持成熟

相比手写 SQL，它在第一版会增加少量接入成本，但能明显降低后续扩展时的迁移和维护成本。

## 为什么不选择其他方案

### 不选择 Next.js

当前项目已经有 `Actix Web` 后端，若使用 `Next.js`：

- 要么退化为静态导出，等同于“用 Next.js 做 Vite 能做的事”
- 要么需要额外运行 Node.js 服务，增加部署复杂度

这与当前“单仓库、单 Rust 服务优先”的目标不匹配。

### 不选择 Refine

`Refine` 对 CRUD 管理后台很高效，但它会额外引入一层更强的前端抽象。

当前项目第一版只需要稳定、可控的后台壳，不需要额外的后台元框架。直接使用 `Ant Design + TanStack` 更清晰，后续扩展时边界也更容易掌控。

### 不选择纯模板渲染方案

如 `Askama + htmx` 这类方案部署很轻，但当 `rules` 管理逐步复杂后，交互和状态组织会更快碰到上限。

当前项目已经接受前端单独构建，因此没有必要为“全 Rust 页面渲染”牺牲后续演进空间。

### 不选择 Redis 作为 `projects` 缓存

在本设计提出时，`/ingest` 的 `appid` 校验背景问题是 Redis lookup；当前第一版实现已经切换为 SQLite-backed `ProjectRegistryState`，`projects` 也已经成为可持久化管理的主数据。

如果继续以 Redis 为校验主路径，会带来两个问题：

- 管理后台写入后仍要维护数据库到 Redis 的同步链路
- 系统会同时维护数据库、Redis、内存三套状态，复杂度过高

因此第一版直接收敛为：

- `SQLite` 是 `projects` 的唯一主存储
- 服务启动和后台轮询把 `projects` 加载到内存
- `/ingest` 运行时只读内存快照

Redis 不再参与 `projects` 的缓存或兜底。

### 不选择 SQLite watch 作为核心同步机制

SQLite 并不适合作为跨连接、跨线程、跨进程的通用数据库事件总线。相比依赖 update hook、文件监听或 WAL 变化侦测，版本号驱动的定时刷新更稳：

- 对 ORM 和连接池更友好
- 部署行为更可预测
- 失败时更容易保留上一份有效快照

## 架构设计

### 总体结构

- 后端：`Actix Web`
- 管理前端：独立子目录
- 构建方式：`Vite build`
- 托管方式：`Actix Web` 静态资源服务

建议目录：

- `web/admin`
  - 管理后台前端工程
- `src/...`
  - 现有后端代码

### 前后端边界

后端负责：

- 提供管理后台 API
- 提供前端构建产物的静态托管
- 保持业务逻辑和数据访问
- 管理 `projects` 的数据库持久化和内存快照刷新

前端负责：

- 页面渲染
- 表单交互
- 列表展示
- 前端路由
- 基于 API 的数据获取和提交

### API 前缀

建议统一使用管理后台 API 前缀：

- `/api/admin/projects`
- `/api/admin/tokens`
- `/api/admin/rules`

这样可以与现有收数接口明确隔离，避免后续公开接口和管理接口混杂。

当前第一版后端已经提供 `projects` CRUD，并由 `Actix Web` 直接挂载接口文档入口：

- `/api/admin/projects`
- `/api-docs/openapi.json`
- `/swagger-ui/`

### 管理后台路径

建议统一挂载到：

- `/admin`

并由前端路由进一步扩展，例如：

- `/admin/projects`
- `/admin/tokens`
- `/admin/rules`

### 数据流边界

`projects` 相关链路统一收敛为：

1. 管理后台通过 `/api/admin/projects` 写入 `SQLite`
2. 写入事务内递增 `projects_version`
3. 后台刷新任务定时检查 `projects_version`
4. 发现版本变化后，全量加载已启用的 `projects`
5. 原子替换服务内存快照
6. `/ingest` 只查询内存中的 `appid` 索引

这样可以把高频读和低频写清晰拆开：

- 写路径：数据库事务
- 读路径：内存快照
- 同步路径：版本号驱动的后台刷新

### `projects` 数据模型

第一版 `projects` 只保留最小可用字段：

- `id`
- `appid`
- `name`
- `enabled`
- `created_at`
- `updated_at`

说明：

- `appid` 是唯一业务键，也是 `/ingest` 校验入口
- `name` 只用于管理台展示
- `enabled` 用于控制项目是否生效
- 旧的 `os`、`re_attribution` 等字段不再作为本次设计的一部分

### 元信息表

增加一个极小的元信息表，例如 `app_meta`：

- `key`
- `value`

其中至少保留：

- `projects_version`

每次 `projects` 的新增、修改、删除、启停都在同一事务里同步更新该版本号。

### 运行时内存态

后端增加一个新的项目注册表状态，例如 `ProjectRegistryState`：

- 启动时全量加载一次 `enabled = true` 的 `projects`
- 内部维护 `appid -> project` 的内存索引
- 后台刷新时整体重建并原子替换

这里不建议把 `/ingest` 改成每次直接查 `SQLite`。`/ingest` 是高频路径，直接查库会把原本轻量的项目存在性校验变成数据库热点。

## 页面范围

第一版仅覆盖以下页面：

- 项目列表页
- 项目编辑页

允许采用以下简化方式：

- 列表页 + 抽屉编辑
- 列表页 + 独立编辑页

这部分不在本设计中强行固定，但要求路由结构和接口边界为后续扩展保留余地。

## 开发与部署模式

### 开发模式

- 前端在 `web/admin` 单独启动 Vite 开发服务器
- `Actix Web` 单独启动本地后端服务
- 前端通过代理访问本地后端 API

### 生产模式

- 执行前端构建
- 输出静态资源目录
- 由 `Actix Web` 将该目录挂载到 `/admin`

生产部署仍然维持一个主服务入口，不引入额外 Node.js 常驻进程。

## 后续演进边界

本次技术选型已经为以下扩展留出空间：

- Token 管理
- 登录鉴权
- 角色权限
- 审计日志
- 规则详情页
- 规则差异对比
- 规则调试与测试页面

这些能力可以在当前技术栈上继续演进，不要求重新更换前端框架。

## 迁移顺序

建议按以下顺序推进：

### 阶段 1：建立前端工程骨架

- 创建 `web/admin`
- 初始化 `React + Vite + TypeScript`
- 接入 `Ant Design`
- 接入 `TanStack Query`
- 接入 `TanStack Router`
- 建立后台主布局

### 阶段 2：接入静态托管

- 在 `Actix Web` 中增加 `/admin` 静态资源托管
- 明确开发与生产环境的资源路径策略

### 阶段 3：引入数据库持久化基础设施

- 接入 `SQLite`
- 接入 `SeaORM`
- 增加 `projects` 与 `app_meta` migration
- 建立数据库初始化和连接管理
- 实现 `projects_version` 刷新机制

### 阶段 4：实现 `projects` 管理接口

- `projects` CRUD
- `ProjectRegistryState` 内存快照
- `/ingest` 从 Redis 校验切换到内存项目表

### 阶段 5：实现第一版后台页面

- 项目列表
- 项目创建
- 项目编辑
- 项目删除

## 运行时行为约束

`projects` 持久化接入后，服务行为需要满足以下约束：

- 服务启动时必须成功连接 `SQLite` 并加载首份 `projects` 快照，否则启动失败
- 运行中若数据库轮询失败，继续保留上一份有效快照，不中断 `/ingest`
- 管理接口写库成功后，允许存在秒级刷新延迟
- `/ingest` 对不存在或禁用的 `appid` 一律按“项目不存在”处理，保持外部语义稳定

## 推荐接口边界

第一版 `projects` 管理接口建议为：

- `GET /api/admin/projects`
- `GET /api/admin/projects/{appid}`
- `POST /api/admin/projects`
- `PUT /api/admin/projects/{appid}`
- `DELETE /api/admin/projects/{appid}`

读模型返回：

- `appid`
- `name`
- `enabled`
- `created_at`
- `updated_at`

写模型只接收：

- 创建：`appid`、`name`、`enabled`
- 更新：`name`、`enabled`

## 验收标准

- 管理后台可以通过 `/admin` 访问
- 前端为单页面后台，路由可正常切换和刷新恢复
- `projects` 具备基础 CRUD 页面
- 新增、编辑、删除或禁用项目后，数秒内 `/ingest` 的项目校验结果同步变化
- 服务重启后，项目数据从 `SQLite` 恢复，不依赖 Redis
- 生产部署不需要额外运行 Node.js 前端服务
- 技术栈边界清晰，不引入额外后台元框架
