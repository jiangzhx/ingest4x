# ingest4x Open Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将当前 `receiver` 仓库收敛为对外统一叫 `ingest4x` 的开源收数服务，新增 `/ingest` 主入口，同时保留旧 `/up` 和旧 `ad` 路由作为迁移参考。

**Architecture:** 采用“新入口先落地、旧入口先保留”的渐进式迁移。新增 `src/ingest/` 承接 `POST /ingest` 和 `GET /ingest`，先用行为对齐测试把新入口钉住，再逐步抽取与旧 `up` / `ad` 共享的处理逻辑，最后再收敛对外命名、文档和默认 feature 范围。

**Tech Stack:** Rust 2021, Actix Web 4, Clap 4, serde/serde_json, rdkafka, redis, config, tempfile, assert-json-diff

---

## File Structure

### New / Updated Responsibilities

- `src/ingest/mod.rs`
  - 新的统一 ingest 模块入口
  - 暴露 `POST /ingest` 和 `GET /ingest` handler
- `src/ingest/json.rs`
  - `POST /ingest` 的 JSON 输入适配
  - 先对齐旧 `/up` 行为
- `src/ingest/query.rs`
  - `GET /ingest` 的 querystring 输入适配
  - 先对齐精简后的 `ad` 行为
- `src/server.rs`
  - 注册新 `/ingest` 路由
  - 暂时保留旧 `/up` 和旧 `ad` 路由
- `src/handlers/up/mod.rs`
  - 迁移期旧实现
  - 后续抽出可复用 JSON ingest 流程
- `src/handlers/ad/click.rs`
  - 迁移期旧实现
  - 后续抽出可复用 query ingest 流程
- `src/handlers/ad/link.rs`
  - v1 删除跳转能力时清理或下线
- `src/lib.rs`
  - 导出新的 `ingest` 模块
- `Cargo.toml`
  - 收敛包名、二进制名、默认 feature 范围
- `README.md`
  - 改成 `ingest4x` 对外叙事
- `examples/README.md`
  - 改成 `/ingest` 示例和本地运行路径
- `examples/receiver.mock.toml`
  - 保留 mock/local 体验，但文档切换为 `ingest`
- `examples/receiver.toml`
  - 保留生产模式示例，并改成 `ingest` 路由叙事
- `tests/test_ingest_post_endpoint.rs`
  - 新增，覆盖 `POST /ingest`
- `tests/test_ingest_get_endpoint.rs`
  - 新增，覆盖 `GET /ingest`
- `tests/test_mock_mode.rs`
  - 补充 mock/local 下 `/ingest` 路由可用性
- `tests/test_receiver_bin.rs`
  - 迁移二进制名和 help/version 预期
- `tests/test_click_endpoint.rs`
  - 迁移期保留，用于新旧 GET 行为对照
- `tests/test_up_endpoint.rs`
  - 迁移期保留，用于新旧 POST 行为对照

### Implementation Notes

- v1 不改请求字段，也不改成功/失败返回体。
- `/ingest` 先允许和旧路由并存，不在同一批改动删除 `/up` 和 `/s/...`。
- `attribution_status` 和 `caid` 要从开源版公共表面下线，但不要求第一步就物理删除全部代码。
- mock/local 是正式运行模式，不能退化成“只能测、不能用”。

## Task 1: Add `/ingest` Route Skeleton And Parity Tests

**Files:**
- Create: `src/ingest/mod.rs`
- Create: `src/ingest/json.rs`
- Create: `src/ingest/query.rs`
- Modify: `src/lib.rs`
- Modify: `src/server.rs`
- Create: `tests/test_ingest_post_endpoint.rs`
- Create: `tests/test_ingest_get_endpoint.rs`
- Modify: `tests/mock_services.rs`
- Test: `tests/test_ingest_post_endpoint.rs`
- Test: `tests/test_ingest_get_endpoint.rs`

- [ ] **Step 1: Write the failing `POST /ingest` parity test**

```rust
#[actix_rt::test]
async fn post_ingest_accepts_the_same_payload_as_up() {
    let (app, _testservice) = create_up_app().await;
    let req = test::TestRequest::post()
        .uri("/ingest")
        .set_json(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-1",
                "os": "ios",
                "idfa": "idfa-1",
                "currencytype": "cny"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}
```

- [ ] **Step 2: Run the failing `POST /ingest` test**

Run: `cargo test --test test_ingest_post_endpoint --no-default-features --features up -q`  
Expected: FAIL with `404` or route-not-found because `/ingest` is not registered yet.

- [ ] **Step 3: Write the failing `GET /ingest` parity test**

```rust
#[actix_rt::test]
async fn get_ingest_accepts_querystring_payload() {
    let (app, _testservice) = create_app().await;
    let req = test::TestRequest::get()
        .uri("/ingest?shorten=xasda&xwhat=click&a=1&b=2")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}
```

- [ ] **Step 4: Run the failing `GET /ingest` test**

Run: `cargo test --test test_ingest_get_endpoint --no-default-features --features ad -q`  
Expected: FAIL with `404` or route-not-found because `/ingest` is not registered yet.

- [ ] **Step 5: Add the minimal route skeleton**

```rust
pub mod ingest;

pub fn configure_app(cfg: &mut ServiceConfig, state: AppState) {
    #[cfg(feature = "up")]
    cfg.service(web::resource("/ingest").route(web::post().to(ingest::post_ingest)));

    #[cfg(feature = "ad")]
    cfg.service(web::resource("/ingest").route(web::get().to(ingest::get_ingest)));
}
```

- [ ] **Step 6: Make the new handlers return obvious placeholders**

```rust
pub async fn post_ingest() -> HttpResponse {
    HttpResponse::NotImplemented().finish()
}

pub async fn get_ingest() -> HttpResponse {
    HttpResponse::NotImplemented().finish()
}
```

- [ ] **Step 7: Re-run the new endpoint tests**

Run: `cargo test --test test_ingest_post_endpoint --test test_ingest_get_endpoint --no-default-features --features "up ad" -q`  
Expected: FAIL with `501 Not Implemented`, proving the route exists and the next work item is business behavior, not registration.

- [ ] **Step 8: Commit the route skeleton**

```bash
git add src/lib.rs src/server.rs src/ingest tests/test_ingest_post_endpoint.rs tests/test_ingest_get_endpoint.rs tests/mock_services.rs
git commit -m "feat: add ingest route skeleton"
```

## Task 2: Implement `POST /ingest` By Reusing The Existing `/up` Behavior

**Files:**
- Modify: `src/ingest/mod.rs`
- Modify: `src/ingest/json.rs`
- Modify: `src/handlers/up/mod.rs`
- Modify: `src/server.rs`
- Modify: `tests/test_ingest_post_endpoint.rs`
- Modify: `tests/test_up_endpoint.rs`
- Modify: `tests/test_mock_mode.rs`
- Test: `tests/test_ingest_post_endpoint.rs`
- Test: `tests/test_up_endpoint.rs`
- Test: `tests/test_mock_mode.rs`

- [ ] **Step 1: Expand the `POST /ingest` test to assert full parity**

```rust
assert_eq!(std::str::from_utf8(body.as_ref()).unwrap(), "200");
assert_json_eq!(
    event_from_kafka.into_value().unwrap(),
    json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xwho": Value::Null,
        "xwhen": xwhen,
        "xcontext": {
            "installid": "iid-1",
            "os": "ios",
            "idfa": "idfa-1",
            "currencytype": "CNY",
            "platform": "ios",
            "ip": "8.8.8.8"
        }
    })
);
```

- [ ] **Step 2: Run the parity test and verify it fails on placeholder behavior**

Run: `cargo test --test test_ingest_post_endpoint --no-default-features --features up -q`  
Expected: FAIL because handler still returns `501`.

- [ ] **Step 3: Implement `post_ingest` as a thin adapter to the existing JSON ingest flow**

```rust
pub async fn post_ingest(
    req: HttpRequest,
    data: web::Json<Value>,
    project_lookup: Data<ProjectLookupState>,
    kafka_producer: Data<KafkaProducerState>,
    rules: Data<RuleSets>,
) -> HttpResponse {
    crate::handlers::up::up(req, data, project_lookup, kafka_producer, rules).await
}
```

- [ ] **Step 4: Add a mock-mode route regression in `tests/test_mock_mode.rs`**

```rust
let req = test::TestRequest::post()
    .uri("/ingest")
    .set_json(valid_up_payload())
    .to_request();
```

- [ ] **Step 5: Re-run the POST-related tests**

Run: `cargo test --test test_ingest_post_endpoint --test test_up_endpoint --test test_mock_mode --no-default-features --features up -q`  
Expected: PASS for both `/ingest` and `/up`, proving coexistence without protocol drift.

- [ ] **Step 6: Commit the POST ingest parity**

```bash
git add src/ingest src/handlers/up/mod.rs tests/test_ingest_post_endpoint.rs tests/test_up_endpoint.rs tests/test_mock_mode.rs
git commit -m "feat: add post ingest endpoint"
```

## Task 3: Implement `GET /ingest` As The Non-Redirect Querystring Ingest Path

**Files:**
- Modify: `src/ingest/mod.rs`
- Modify: `src/ingest/query.rs`
- Modify: `src/handlers/ad/click.rs`
- Modify: `src/handlers/ad/mod.rs`
- Modify: `src/server.rs`
- Modify: `tests/test_ingest_get_endpoint.rs`
- Modify: `tests/test_click_endpoint.rs`
- Test: `tests/test_ingest_get_endpoint.rs`
- Test: `tests/test_click_endpoint.rs`

- [ ] **Step 1: Expand the `GET /ingest` test to assert the produced event**

```rust
let req = test::TestRequest::get()
    .uri("/ingest?shorten=xasda&xwhat=click&a=1&b=2")
    .to_request();

let resp = test::call_service(&app, req).await;
assert_eq!(resp.status(), StatusCode::OK);
assert_eq!(std::str::from_utf8(body.as_ref()).unwrap(), "200");
```

- [ ] **Step 2: Run the `GET /ingest` test and verify it fails**

Run: `cargo test --test test_ingest_get_endpoint --no-default-features --features ad -q`  
Expected: FAIL because the placeholder handler does not build the ad-style event yet.

- [ ] **Step 3: Extract a reusable query ingest function from `click.rs`**

```rust
pub async fn ingest_query_event(
    shorten: String,
    xwhat: String,
    query_params: Query<HashMap<String, String>>,
    state: &AdClickState,
    req: &HttpRequest,
) -> HttpResponse
```

- [ ] **Step 4: Make the old click handlers delegate to the shared function**

```rust
pub async fn click(...) -> HttpResponse {
    ingest_query_event(shorten, xwhat, query_params, &state, &req).await
}
```

- [ ] **Step 5: Implement `GET /ingest` as the new query adapter**

```rust
pub async fn get_ingest(
    query_params: web::Query<HashMap<String, String>>,
    state: Data<AdClickState>,
    req: HttpRequest,
) -> HttpResponse {
    let shorten = query_params.get("shorten").cloned().unwrap_or_default();
    let xwhat = query_params.get("xwhat").cloned().unwrap_or_default();
    crate::handlers::ad::click::ingest_query_event(shorten, xwhat, query_params, &state, &req).await
}
```

- [ ] **Step 6: Re-run the GET-related tests**

Run: `cargo test --test test_ingest_get_endpoint --test test_click_endpoint --no-default-features --features ad -q`  
Expected: PASS for `/ingest` GET and existing `/s/...` click routes.

- [ ] **Step 7: Commit the query ingest path**

```bash
git add src/ingest src/handlers/ad/click.rs src/handlers/ad/mod.rs tests/test_ingest_get_endpoint.rs tests/test_click_endpoint.rs
git commit -m "feat: add get ingest endpoint"
```

## Task 4: Separate Legacy Routes From The New Ingest Surface

**Files:**
- Create: `src/legacy/mod.rs`
- Create: `src/legacy/up.rs`
- Create: `src/legacy/ad.rs`
- Modify: `src/lib.rs`
- Modify: `src/server.rs`
- Modify: `src/handlers/mod.rs`
- Modify: `tests/test_up_endpoint.rs`
- Modify: `tests/test_click_endpoint.rs`
- Modify: `README.md`
- Test: `tests/test_up_endpoint.rs`
- Test: `tests/test_click_endpoint.rs`

- [ ] **Step 1: Add a failing test or assertion that both new and old routes remain reachable**

```rust
let up_resp = test::call_service(&app, legacy_up_req).await;
let ingest_resp = test::call_service(&app, ingest_req).await;
assert_eq!(up_resp.status(), ingest_resp.status());
```

- [ ] **Step 2: Run the legacy/new coexistence tests**

Run: `cargo test --test test_up_endpoint --test test_click_endpoint --no-default-features --features "up ad" -q`  
Expected: PASS before refactor, giving a safe baseline.

- [ ] **Step 3: Move route registration labels into a `legacy` namespace without changing behavior**

```rust
pub mod legacy;

pub fn configure_legacy_routes(cfg: &mut ServiceConfig, state: AppState) {
    legacy::up::configure(cfg, state.clone());
    legacy::ad::configure(cfg, state);
}
```

- [ ] **Step 4: Keep `src/handlers/*` as the implementation home for now**

```rust
pub use crate::handlers::up::up as legacy_up_handler;
pub use crate::handlers::ad::click::click as legacy_ad_click_handler;
```

- [ ] **Step 5: Update README to mark `/up` and old ad routes as compatibility endpoints**

```markdown
`/ingest` 是正式入口；`/up` 和旧 ad 路由在迁移期保留，用于兼容和对照测试。
```

- [ ] **Step 6: Re-run coexistence and route registration tests**

Run: `cargo test --test test_up_endpoint --test test_click_endpoint --test test_ingest_post_endpoint --test test_ingest_get_endpoint --no-default-features --features "up ad" -q`  
Expected: PASS with no route regressions.

- [ ] **Step 7: Commit the legacy split**

```bash
git add src/lib.rs src/server.rs src/legacy src/handlers/mod.rs README.md tests/test_up_endpoint.rs tests/test_click_endpoint.rs
git commit -m "refactor: separate legacy routes from ingest surface"
```

## Task 5: Remove V1-Excluded Features From The Public Open-Source Surface

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/server.rs`
- Modify: `src/lib.rs`
- Modify: `README.md`
- Modify: `docs/features/attribution_status.md`
- Modify: `docs/features/caid.md`
- Modify: `docs/features/up.md`
- Modify: `docs/features/ad.md`
- Modify: `tests/test_receiver_bin.rs`
- Test: `tests/test_receiver_bin.rs`
- Test: `cargo test` feature matrix

- [ ] **Step 1: Add a failing surface test that the default build only advertises `ingest`-related capabilities**

```rust
let output = Command::new("cargo")
    .arg("run")
    .arg("--quiet")
    .arg("--bin")
    .arg("ingest4x")
    .arg("--")
    .arg("--help")
    .output()
    .expect("run ingest4x --help");
```

- [ ] **Step 2: Run the binary/help test and confirm the old name and feature surface still leak**

Run: `cargo test --test test_receiver_bin -q`  
Expected: FAIL because the binary is still `receiver` and help text still reflects the old product.

- [ ] **Step 3: Remove `attribution_status` and `caid` from the default open-source surface**

```toml
[features]
default = ["up", "ad"]
up = []
ad = []
```

- [ ] **Step 4: Stop registering excluded routes in the default server path**

```rust
#[cfg(feature = "attribution_status")]
// delete or gate behind non-default builds

#[cfg(feature = "caid")]
// delete or gate behind non-default builds
```

- [ ] **Step 5: Rewrite docs to state that v1 only supports JSON ingest and query ingest**

```markdown
v1 正式支持 `POST /ingest` 和 `GET /ingest`。`attribution_status` 与 `caid` 不属于开源版范围。
```

- [ ] **Step 6: Run the feature-matrix checks**

Run: `cargo test --no-default-features --features up -q`  
Expected: PASS

Run: `cargo test --no-default-features --features ad -q`  
Expected: PASS

Run: `cargo test --no-default-features --features "up ad" -q`  
Expected: PASS

- [ ] **Step 7: Commit the v1 surface reduction**

```bash
git add Cargo.toml src/server.rs src/lib.rs README.md docs/features tests/test_receiver_bin.rs
git commit -m "refactor: remove non-v1 features from public surface"
```

## Task 6: Rename The Public Product Surface To `ingest4x` And Update Examples

**Files:**
- Modify: `Cargo.toml`
- Modify: `README.md`
- Modify: `examples/README.md`
- Modify: `examples/receiver.mock.toml`
- Modify: `examples/receiver.toml`
- Modify: `docs/recommended-workflow.md`
- Modify: `docs/jlt-format.md`
- Modify: `tests/test_receiver_bin.rs`
- Modify: `tests/test_mock_mode.rs`
- Test: `tests/test_receiver_bin.rs`
- Test: `tests/test_mock_mode.rs`

- [ ] **Step 1: Add a failing version/help test for the new binary name**

```rust
let output = Command::new("cargo")
    .arg("run")
    .arg("--quiet")
    .arg("--bin")
    .arg("ingest4x")
    .arg("--")
    .arg("--version")
    .output()
    .expect("run ingest4x --version");
```

- [ ] **Step 2: Run the binary naming test**

Run: `cargo test --test test_receiver_bin -q`  
Expected: FAIL because `ingest4x` bin does not exist yet.

- [ ] **Step 3: Rename the package/bin surface**

```toml
[package]
name = "ingest4x"

[[bin]]
name = "ingest4x"
path = "src/main.rs"
```

- [ ] **Step 4: Rewrite the main docs and examples around the new product story**

```markdown
# ingest4x

`ingest4x` 是一个可直接部署、默认可本地跑通的通用收数服务。
```

- [ ] **Step 5: Update the curl examples to use `/ingest`**

```bash
curl -X POST http://127.0.0.1:8090/ingest \
  -H 'Content-Type: application/json' \
  -d '{"appid":"APPID","xwhat":"custom_event","xcontext":{"installid":"iid-1"}}'
```

- [ ] **Step 6: Re-run the naming and mock/local verification**

Run: `cargo test --test test_receiver_bin --test test_mock_mode --no-default-features --features up -q`  
Expected: PASS with the new binary name and `/ingest` examples still working.

- [ ] **Step 7: Commit the public rename**

```bash
git add Cargo.toml README.md examples/README.md examples/receiver.mock.toml examples/receiver.toml docs/recommended-workflow.md docs/jlt-format.md tests/test_receiver_bin.rs tests/test_mock_mode.rs
git commit -m "feat: rename public product surface to ingest4x"
```

## Task 7: Final Verification And Documentation Cleanup

**Files:**
- Modify: `README.md`
- Modify: `examples/README.md`
- Modify: `docs/features/up.md`
- Modify: `docs/features/ad.md`
- Modify: `docs/recommended-workflow.md`
- Modify: `tests/test_ingest_post_endpoint.rs`
- Modify: `tests/test_ingest_get_endpoint.rs`
- Test: repository verification matrix

- [ ] **Step 1: Re-read the spec and create a final verification checklist**

```markdown
- `/ingest` is primary
- request fields unchanged
- return values unchanged
- mock/local path documented
- legacy routes retained
- excluded features removed from public surface
```

- [ ] **Step 2: Run the focused test suites**

Run: `cargo test --test test_ingest_post_endpoint --test test_ingest_get_endpoint --test test_up_endpoint --test test_click_endpoint --no-default-features --features "up ad" -q`  
Expected: PASS

Run: `cargo test --test test_mock_mode --no-default-features --features up -q`  
Expected: PASS

Run: `cargo test --test test_settings_rules -q`  
Expected: PASS

- [ ] **Step 3: Run the full dual-feature test matrix**

Run: `cargo test --no-default-features --features "up ad" -q`  
Expected: PASS

- [ ] **Step 4: Run the binary smoke checks**

Run: `cargo run --quiet --bin ingest4x -- --version`  
Expected: prints `CARGO_PKG_VERSION`

Run: `cargo run --quiet --bin ingest4x -- test --help`  
Expected: exits `0` and shows the JLT/test subcommand help.

- [ ] **Step 5: Update any docs still using `/up`, `/s/`, or `receiver` as the primary public story**

```markdown
Use `/ingest` as the primary example. Mention `/up` and legacy ad routes only in compatibility notes.
```

- [ ] **Step 6: Commit the verification cleanup**

```bash
git add README.md examples/README.md docs/features docs/recommended-workflow.md tests/test_ingest_post_endpoint.rs tests/test_ingest_get_endpoint.rs
git commit -m "docs: finalize ingest4x open source migration"
```

## Execution Notes

- 按顺序执行，不要跳过测试先做大批量重命名。
- 每个任务都先让测试失败，再做最小实现，再重跑测试。
- 在 `Task 5` 之前，不要急着物理删除 `attribution_status` / `caid` 相关代码；先缩小公开表面。
- 在 `Task 6` 之前，不要急着全仓替换 `receiver` 为 `ingest4x`；先把 `/ingest` 逻辑跑通，否则会同时引入协议迁移和产品重命名两个变量。
- 当前仓库有未跟踪目录 `.idea/`，执行计划时忽略它，不要纳入提交。
