# Projects SQLite Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `ingest4x` 引入 `projects` 的 SQLite 持久化和管理接口，并将 `/ingest` 的项目校验从 Redis lookup 切换为数据库驱动的内存快照。

**Architecture:** 采用“数据库主存储 + 版本号轮询刷新 + 内存快照读”的结构。管理接口把 `projects` 写入 SQLite，并在同一事务内递增 `projects_version`；服务启动时加载首份项目快照，后台定时轮询版本号，发现变化后全量重载；`/ingest` 运行时只读内存项目表，不再访问 Redis。

**Tech Stack:** Rust 2021, Actix Web 4, SeaORM, SQLite, Tokio/Actix async runtime, serde, config, tempfile, assert-json-diff

---

## File Structure

### New / Updated Responsibilities

- `Cargo.toml`
  - 增加 `sea-orm`、`sea-query`、`sea-orm-migration`、`tokio` 相关依赖
- `config/development.toml`
  - 增加 SQLite 数据库配置
  - 去掉“生产/开发必须有 Redis 才能启动”的隐含前提
- `src/settings.rs`
  - 增加数据库配置模型
  - 让服务可以读取 SQLite 路径、轮询间隔等参数
- `src/server.rs`
  - 初始化数据库连接
  - 初始化 `ProjectRegistryState`
  - 注册后台刷新任务和 `/api/admin/projects` 路由
  - 将 `/ingest` 改为读取项目注册表
- `src/lib.rs`
  - 导出新增的 `admin`、`db`、`projects` 模块
- `src/db/mod.rs`
  - 数据库连接初始化和共享状态
- `src/db/migrate.rs`
  - 启动时执行最小 migration
- `src/db/entities/mod.rs`
  - 汇总 SeaORM entity
- `src/db/entities/projects.rs`
  - `projects` 表 entity / model
- `src/db/entities/app_meta.rs`
  - `app_meta` 表 entity / model
- `src/projects/mod.rs`
  - 汇总项目域能力
- `src/projects/model.rs`
  - 面向服务层和接口层的 `ProjectRecord`、创建/更新 DTO
- `src/projects/repository.rs`
  - `projects` 读写和 `projects_version` 管理
- `src/projects/registry.rs`
  - 内存快照、版本检查、全量刷新、后台轮询任务
- `src/admin/mod.rs`
  - 管理后台 API 模块入口
- `src/admin/projects.rs`
  - `GET/POST/PUT/DELETE /api/admin/projects` handler
- `tests/mock_services.rs`
  - 用 SQLite / 内存项目注册表替换测试里的 Redis 项目注入方式
- `tests/test_ingest_post_endpoint.rs`
  - 让 `/ingest` 测试基于数据库项目数据，而不是 Redis lookup
- `tests/test_admin_projects_endpoint.rs`
  - 新增，覆盖 `projects` CRUD 和版本刷新行为
- `tests/test_settings_database.rs`
  - 新增，覆盖数据库配置加载
- `tests/test_project_registry.rs`
  - 新增，覆盖版本号轮询和快照替换

### Implementation Notes

- 第一版只持久化 `projects`，不同时迁移 `tokens`、`rules`。
- `projects` 的最小字段固定为：`appid`、`name`、`enabled`、`created_at`、`updated_at`。
- `enabled = false` 的项目不进入 `/ingest` 可用快照。
- `/ingest` 对“禁用”和“不存在”统一返回 `404 Project not found`。
- 数据库不可用时：
  - 启动阶段：直接启动失败
  - 运行阶段：保留上一份快照并记录错误日志
- 本计划不要求删除 `redis` 依赖本身，但要把 `projects` 校验逻辑从 `Redis` 路径中摘掉。

## Task 1: Add Database Configuration And Bootstrap

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/settings.rs`
- Modify: `config/development.toml`
- Create: `tests/test_settings_database.rs`
- Test: `tests/test_settings_database.rs`

- [ ] **Step 1: Write the failing database settings test**

```rust
#[test]
fn settings_reads_database_section() {
    let settings = Settings::init_with_file("tests/fixtures/database-settings.toml")
        .expect("load settings");

    let database = settings.database.expect("database config");
    assert_eq!(database.url, "sqlite://tmp/admin.db?mode=rwc");
    assert_eq!(database.refresh_interval_secs, 3);
}
```

- [ ] **Step 2: Add a fixture config file for the test**

```toml
[database]
url = "sqlite://tmp/admin.db?mode=rwc"
refresh_interval_secs = 3
```

- [ ] **Step 3: Run the new settings test to verify it fails**

Run: `cargo test --test test_settings_database -q`  
Expected: FAIL with a deserialize error because `Settings` does not yet define a `database` section.

- [ ] **Step 4: Add the database config model**

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseSettings {
    pub url: String,
    #[serde(default = "default_projects_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub metrics: MetricsSettings,
    #[serde(default)]
    pub runtime: RuntimeSettings,
    pub database: Option<DatabaseSettings>,
    // ...
}
```

- [ ] **Step 5: Add default helpers and development config**

```toml
[database]
url = "sqlite://data/ingest4x.db?mode=rwc"
refresh_interval_secs = 3
```

- [ ] **Step 6: Add the ORM dependencies**

```toml
sea-orm = { version = "1", default-features = false, features = ["macros", "runtime-tokio-rustls", "sqlx-sqlite"] }
sea-orm-migration = { version = "1", default-features = false, features = ["runtime-tokio-rustls", "sqlx-sqlite"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "sync"] }
tempfile = "3.15.0"
```

- [ ] **Step 7: Re-run the settings test**

Run: `cargo test --test test_settings_database -q`  
Expected: PASS, proving the service can read database settings before any runtime wiring.

- [ ] **Step 8: Commit the settings/bootstrap foundation**

```bash
git add Cargo.toml src/settings.rs config/development.toml tests/test_settings_database.rs tests/fixtures/database-settings.toml
git commit -m "feat: add database configuration"
```

## Task 2: Create SQLite Schema And Project Repository

**Files:**
- Create: `src/db/mod.rs`
- Create: `src/db/migrate.rs`
- Create: `src/db/entities/mod.rs`
- Create: `src/db/entities/projects.rs`
- Create: `src/db/entities/app_meta.rs`
- Create: `src/projects/mod.rs`
- Create: `src/projects/model.rs`
- Create: `src/projects/repository.rs`
- Modify: `src/lib.rs`
- Create: `tests/test_project_repository.rs`
- Test: `tests/test_project_repository.rs`

- [ ] **Step 1: Write the failing repository test for create/list behavior**

```rust
#[actix_rt::test]
async fn project_repository_creates_and_lists_enabled_projects() {
    let db = init_test_db().await;
    let repo = ProjectRepository::new(db.clone());

    repo.create_project(CreateProjectInput {
        appid: "APPID".into(),
        name: "Primary".into(),
        enabled: true,
    })
    .await
    .expect("create project");

    let projects = repo.list_enabled_projects().await.expect("list projects");
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].appid, "APPID");
}
```

- [ ] **Step 2: Write the failing repository test for version bump**

```rust
#[actix_rt::test]
async fn mutating_projects_bumps_projects_version() {
    let db = init_test_db().await;
    let repo = ProjectRepository::new(db.clone());

    let version_before = repo.projects_version().await.expect("version before");
    repo.create_project(CreateProjectInput {
        appid: "APPID".into(),
        name: "Primary".into(),
        enabled: true,
    })
    .await
    .expect("create project");
    let version_after = repo.projects_version().await.expect("version after");

    assert!(version_after > version_before);
}
```

- [ ] **Step 3: Run repository tests to verify they fail**

Run: `cargo test --test test_project_repository -q`  
Expected: FAIL because the database module, migrations, and repository types do not exist yet.

- [ ] **Step 4: Add the two entities and migration**

```rust
// projects
appid: String,
name: String,
enabled: bool,
created_at: DateTimeUtc,
updated_at: DateTimeUtc,

// app_meta
key: String,
value: String,
```

- [ ] **Step 5: Implement startup migration**

```rust
pub async fn run_migrations(db: &DatabaseConnection) -> Result<()> {
    db.execute_unprepared(
        r#"
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            appid TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        INSERT OR IGNORE INTO app_meta(key, value) VALUES ('projects_version', '0');
        "#,
    )
    .await?;
    Ok(())
}
```

- [ ] **Step 6: Implement repository methods**

```rust
pub async fn create_project(&self, input: CreateProjectInput) -> Result<ProjectRecord>;
pub async fn update_project(&self, appid: &str, input: UpdateProjectInput) -> Result<Option<ProjectRecord>>;
pub async fn delete_project(&self, appid: &str) -> Result<bool>;
pub async fn list_projects(&self) -> Result<Vec<ProjectRecord>>;
pub async fn list_enabled_projects(&self) -> Result<Vec<ProjectRecord>>;
pub async fn get_project(&self, appid: &str) -> Result<Option<ProjectRecord>>;
pub async fn projects_version(&self) -> Result<u64>;
```

- [ ] **Step 7: Ensure every mutation bumps `projects_version` in the same transaction**

```rust
txn.execute( /* insert or update project */ ).await?;
txn.execute( /* update app_meta set value = value + 1 */ ).await?;
txn.commit().await?;
```

- [ ] **Step 8: Re-run the repository tests**

Run: `cargo test --test test_project_repository -q`  
Expected: PASS, proving SQLite schema and repository behavior are correct before wiring them into HTTP handlers.

- [ ] **Step 9: Commit the repository layer**

```bash
git add src/db src/projects src/lib.rs tests/test_project_repository.rs
git commit -m "feat: add sqlite project repository"
```

## Task 3: Add Project Registry Snapshot And Refresh Loop

**Files:**
- Modify: `src/projects/mod.rs`
- Create: `src/projects/registry.rs`
- Modify: `src/server.rs`
- Create: `tests/test_project_registry.rs`
- Test: `tests/test_project_registry.rs`

- [ ] **Step 1: Write the failing registry bootstrap test**

```rust
#[actix_rt::test]
async fn registry_loads_enabled_projects_into_memory() {
    let db = init_test_db_with_projects(&[
        ("APPID", "Primary", true),
        ("DISABLED", "Disabled", false),
    ])
    .await;
    let repo = ProjectRepository::new(db.clone());

    let registry = ProjectRegistryState::load(repo.clone()).await.expect("load registry");

    assert!(registry.contains("APPID"));
    assert!(!registry.contains("DISABLED"));
}
```

- [ ] **Step 2: Write the failing refresh test**

```rust
#[actix_rt::test]
async fn registry_reload_replaces_snapshot_when_version_changes() {
    let db = init_test_db().await;
    let repo = ProjectRepository::new(db.clone());
    let registry = ProjectRegistryState::load(repo.clone()).await.expect("load registry");

    repo.create_project(CreateProjectInput {
        appid: "APPID".into(),
        name: "Primary".into(),
        enabled: true,
    })
    .await
    .expect("create");

    registry.refresh_if_needed().await.expect("refresh");
    assert!(registry.contains("APPID"));
}
```

- [ ] **Step 3: Run the registry tests to verify they fail**

Run: `cargo test --test test_project_registry -q`  
Expected: FAIL because the registry state and refresh logic do not exist yet.

- [ ] **Step 4: Implement the in-memory registry**

```rust
pub struct ProjectRegistryState {
    snapshot: Arc<RwLock<HashMap<String, ProjectRecord>>>,
    version: AtomicU64,
    repository: ProjectRepository,
}

impl ProjectRegistryState {
    pub async fn load(repository: ProjectRepository) -> Result<Self> { /* ... */ }
    pub fn contains(&self, appid: &str) -> bool { /* ... */ }
    pub async fn refresh_if_needed(&self) -> Result<bool> { /* ... */ }
}
```

- [ ] **Step 5: Add the background refresh loop**

```rust
pub fn spawn_project_refresh_task(
    registry: Data<ProjectRegistryState>,
    interval_secs: u64,
) {
    actix_web::rt::spawn(async move {
        let interval = Duration::from_secs(interval_secs);
        loop {
            if let Err(err) = registry.refresh_if_needed().await {
                log::error!("failed to refresh project registry: {err}");
            }
            actix_web::rt::time::sleep(interval).await;
        }
    });
}
```

- [ ] **Step 6: Wire the registry into `AppState`, but do not switch `/ingest` yet**

```rust
pub struct AppState {
    settings: Arc<Settings>,
    rules: Data<RuleSets>,
    kafka_producer: Data<KafkaProducerState>,
    project_registry: Data<ProjectRegistryState>,
}
```

- [ ] **Step 7: Re-run the registry tests**

Run: `cargo test --test test_project_registry -q`  
Expected: PASS, proving snapshot loading and version-based refresh work independently from the HTTP layer.

- [ ] **Step 8: Commit the registry layer**

```bash
git add src/projects/registry.rs src/projects/mod.rs src/server.rs tests/test_project_registry.rs
git commit -m "feat: add project registry snapshot refresh"
```

## Task 4: Switch `/ingest` Project Validation From Redis To The Registry

**Files:**
- Modify: `src/ingest/json.rs`
- Modify: `src/server.rs`
- Modify: `src/utils/mock.rs`
- Modify: `tests/mock_services.rs`
- Modify: `tests/test_ingest_post_endpoint.rs`
- Test: `tests/test_ingest_post_endpoint.rs`
- Test: `tests/test_ingest_mock_mode.rs`

- [ ] **Step 1: Rewrite the failing ingest test setup to use a database-backed project registry**

```rust
#[actix_rt::test]
async fn post_ingest_returns_not_found_when_project_is_missing() {
    let (app, _testservice) = create_app_with_projects(&[]).await;

    let req = test::TestRequest::post()
        .uri("/ingest")
        .set_json(valid_payload())
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
```

- [ ] **Step 2: Add a mock-mode regression proving `MockSettings.projects` is no longer the runtime source**

```rust
#[actix_rt::test]
async fn mock_mode_reads_projects_from_database_registry() {
    let app = create_configured_app_with_database().await;
    // assert ingest works only after inserting sqlite project rows
}
```

- [ ] **Step 3: Run the ingest tests and verify they fail**

Run: `cargo test --test test_ingest_post_endpoint --test test_ingest_mock_mode -q`  
Expected: FAIL because the handlers still depend on `ProjectLookupState`.

- [ ] **Step 4: Replace `ProjectLookupState` usage in the handler**

```rust
pub async fn post_ingest(
    req: HttpRequest,
    data: web::Json<Value>,
    project_registry: Data<ProjectRegistryState>,
    kafka_producer: Data<KafkaProducerState>,
    rules: Data<RuleSets>,
) -> HttpResponse {
    // ...
    if !project_registry.contains(event.appid()) {
        return HttpResponse::NotFound().body("Project not found");
    }
    // ...
}
```

- [ ] **Step 5: Remove the Redis hard requirement from server bootstrap**

```rust
match settings.runtime.mode {
    RuntimeMode::Production | RuntimeMode::Development => {
        if settings.kafka.is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "production mode requires [kafka] config"));
        }
        if settings.database.is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "production mode requires [database] config"));
        }
    }
    RuntimeMode::Mock => {}
}
```

- [ ] **Step 6: Re-run the ingest endpoint tests**

Run: `cargo test --test test_ingest_post_endpoint --test test_ingest_mock_mode -q`  
Expected: PASS, proving `/ingest` project validation now depends on the registry snapshot rather than Redis.

- [ ] **Step 7: Re-run the broader suite around startup and settings**

Run: `cargo test --test test_settings_database --test test_project_repository --test test_project_registry --test test_ingest_post_endpoint --test test_ingest_mock_mode -q`  
Expected: PASS.

- [ ] **Step 8: Commit the ingest switch**

```bash
git add src/ingest/json.rs src/server.rs src/utils/mock.rs tests/mock_services.rs tests/test_ingest_post_endpoint.rs tests/test_ingest_mock_mode.rs
git commit -m "feat: switch ingest project validation to sqlite registry"
```

## Task 5: Add Admin `projects` CRUD Endpoints

**Files:**
- Create: `src/admin/mod.rs`
- Create: `src/admin/projects.rs`
- Modify: `src/server.rs`
- Create: `tests/test_admin_projects_endpoint.rs`
- Test: `tests/test_admin_projects_endpoint.rs`

- [ ] **Step 1: Write the failing list/create endpoint test**

```rust
#[actix_rt::test]
async fn admin_projects_create_then_list() {
    let app = create_admin_app().await;

    let create_req = test::TestRequest::post()
        .uri("/api/admin/projects")
        .set_json(json!({
            "appid": "APPID",
            "name": "Primary",
            "enabled": true
        }))
        .to_request();
    let create_resp = test::call_service(&app, create_req).await;
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let list_req = test::TestRequest::get().uri("/api/admin/projects").to_request();
    let list_resp = test::call_service(&app, list_req).await;
    assert_eq!(list_resp.status(), StatusCode::OK);
}
```

- [ ] **Step 2: Write the failing update/delete endpoint tests**

```rust
#[actix_rt::test]
async fn admin_projects_update_changes_name_and_enabled() { /* ... */ }

#[actix_rt::test]
async fn admin_projects_delete_removes_project() { /* ... */ }
```

- [ ] **Step 3: Run the admin endpoint tests to verify they fail**

Run: `cargo test --test test_admin_projects_endpoint -q`  
Expected: FAIL with route-not-found because `/api/admin/projects` is not registered yet.

- [ ] **Step 4: Implement the handlers with repository-backed DTOs**

```rust
web::scope("/api/admin/projects")
    .route("", web::get().to(list_projects))
    .route("", web::post().to(create_project))
    .route("/{appid}", web::get().to(get_project))
    .route("/{appid}", web::put().to(update_project))
    .route("/{appid}", web::delete().to(delete_project));
```

- [ ] **Step 5: Return stable HTTP semantics**

```rust
POST   /api/admin/projects        -> 201 Created
GET    /api/admin/projects        -> 200 Ok
GET    /api/admin/projects/{id}   -> 200 Ok / 404 Not Found
PUT    /api/admin/projects/{id}   -> 200 Ok / 404 Not Found
DELETE /api/admin/projects/{id}   -> 204 No Content / 404 Not Found
```

- [ ] **Step 6: Extend the test to prove CRUD changes reach `/ingest` after refresh**

```rust
#[actix_rt::test]
async fn admin_project_enablement_propagates_to_ingest() {
    let (app, _svc) = create_full_app().await;
    // create project through admin API
    // trigger refresh_if_needed or wait for poll interval in test
    // assert /ingest accepts the project
}
```

- [ ] **Step 7: Re-run the admin endpoint tests**

Run: `cargo test --test test_admin_projects_endpoint -q`  
Expected: PASS, proving CRUD endpoints and registry propagation work end-to-end.

- [ ] **Step 8: Commit the admin projects API**

```bash
git add src/admin src/server.rs tests/test_admin_projects_endpoint.rs
git commit -m "feat: add admin projects api"
```

## Task 6: Update Runtime Docs, Example Config, And Full Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-04-23-admin-console-tech-selection-design.md`
- Modify: `config/development.toml`
- Modify: `docs/features/ingest.md`
- Test: `tests/test_index_endpoint.rs`
- Test: `tests/test_ingest_post_endpoint.rs`
- Test: `tests/test_admin_projects_endpoint.rs`

- [ ] **Step 1: Update the runtime docs and config examples**

```md
- 新增 `[database]` 配置段
- 说明 `projects` 来自 SQLite，而不是 Redis lookup
- 说明 `/api/admin/projects` 是第一版管理接口
```

- [ ] **Step 2: Add a smoke assertion for startup config when database is missing**

```rust
#[test]
fn build_app_state_requires_database_in_development_mode() {
    // settings.runtime.mode = Development
    // settings.database = None
    // assert error contains "[database]"
}
```

- [ ] **Step 3: Run focused verification**

Run: `cargo test --test test_settings_database --test test_project_repository --test test_project_registry --test test_ingest_post_endpoint --test test_admin_projects_endpoint -q`  
Expected: PASS.

- [ ] **Step 4: Run the full suite**

Run: `cargo test`  
Expected: PASS.

- [ ] **Step 5: Commit the docs and verification finish**

```bash
git add README.md docs/features/ingest.md docs/superpowers/specs/2026-04-23-admin-console-tech-selection-design.md config/development.toml
git commit -m "docs: document sqlite-backed projects management"
```
