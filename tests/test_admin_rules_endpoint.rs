use actix_http::StatusCode;
use actix_web::{test, App};
use ingest4x::server;
use ingest4x::settings::Settings;
use serde_json::{json, Value};
use std::fs;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use tempfile::tempdir;

const ADMIN_PASSWORD_HEADER: &str = "x-admin-password";
const TEST_ADMIN_PASSWORD: &str = "test-admin-password";
const ADMIN_PASSWORD_ENV: &str = "INGEST4X_ADMIN_PASSWORD";

struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: Option<&str>) -> Self {
        let previous = std::env::var_os(key);

        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }

        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

fn admin_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn with_admin_password_env<F, Fut>(password: Option<&str>, action: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    let _guard = admin_env_lock().lock().expect("lock poisoned");
    let _env_guard = EnvVarGuard::set(ADMIN_PASSWORD_ENV, password);

    action().await;
}

fn with_admin_password(request: test::TestRequest) -> test::TestRequest {
    request.insert_header((ADMIN_PASSWORD_HEADER, TEST_ADMIN_PASSWORD))
}

#[actix_rt::test]
async fn admin_can_create_rule_set_rule_and_assign_rule_set_to_project() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;

        let create_project = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/projects")
                .set_json(json!({
                    "appid": "admin-rules-app",
                    "name": "Admin Rules App",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(create_project.status(), StatusCode::CREATED);

        let create_rule_set = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/rule-sets")
                .set_json(json!({
                    "name": "Admin Rule Set",
                    "description": "Created by admin test",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        let rule_set_status = create_rule_set.status();
        let rule_set: Value = test::read_body_json(create_rule_set).await;

        assert_eq!(rule_set_status, StatusCode::CREATED);
        assert_eq!(rule_set["name"], "Admin Rule Set");

        let create_rule = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri(format!("/api/admin/rule-sets/{}/rules", rule_set["id"]).as_str())
                .set_json(json!({
                    "parent_id": null,
                    "name": "Install",
                    "xwhat": "install",
                    "content": "fields: {}\n",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        let rule_status = create_rule.status();
        let rule: Value = test::read_body_json(create_rule).await;

        assert_eq!(rule_status, StatusCode::CREATED);
        assert_eq!(rule["xwhat"], "install");
        assert!(rule.get("sort_order").is_none());

        let assign = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri("/api/admin/projects/admin-rules-app/rule-sets")
                .set_json(json!({
                    "rule_set_id": rule_set["id"],
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        let assign_status = assign.status();
        let assign_body = test::read_body(assign).await;
        assert_eq!(
            assign_status,
            StatusCode::OK,
            "{}",
            std::str::from_utf8(assign_body.as_ref()).unwrap()
        );

        let list_assignments = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/projects/admin-rules-app/rule-sets")
                .to_request(),
        )
        .await;
        let assignments_status = list_assignments.status();
        let assignments: Value = test::read_body_json(list_assignments).await;

        assert_eq!(assignments_status, StatusCode::OK);
        assert!(assignments
            .as_array()
            .expect("assignments should be an array")
            .iter()
            .any(|assignment| assignment["rule_set_id"] == rule_set["id"]));

        let create_second_rule_set = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/rule-sets")
                .set_json(json!({
                    "name": "Second Admin Rule Set",
                    "description": null,
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        let second_rule_set: Value = test::read_body_json(create_second_rule_set).await;

        let replace_assign = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri("/api/admin/projects/admin-rules-app/rule-sets")
                .set_json(json!({
                    "rule_set_id": second_rule_set["id"],
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(replace_assign.status(), StatusCode::OK);

        let replaced_assignments = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/projects/admin-rules-app/rule-sets")
                .to_request(),
        )
        .await;
        let assignments: Value = test::read_body_json(replaced_assignments).await;
        let assignments = assignments
            .as_array()
            .expect("assignments should be an array");
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0]["rule_set_id"], second_rule_set["id"]);
    })
    .await;
}

#[actix_rt::test]
async fn openapi_json_includes_admin_rules_paths() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api-docs/openapi.json")
                .to_request(),
        )
        .await;
        let status = response.status();
        let body: Value = test::read_body_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert!(body["paths"]["/api/admin/rule-sets"].is_object());
        assert!(body["paths"]["/api/admin/rule-sets/{rule_set_id}/rules"].is_object());
        assert!(body["paths"]["/api/admin/projects/{appid}/rule-sets"].is_object());
    })
    .await;
}

async fn create_app() -> impl actix_web::dev::Service<
    actix_http::Request,
    Response = actix_web::dev::ServiceResponse,
    Error = actix_web::Error,
> {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("mock-config.toml");

    fs::write(
        &config_path,
        r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.stdout]
type = "stdout"

[[events.valid.routes]]
sinks = ["stdout"]
ack = ["stdout"]

[[events.invalid.routes]]
sinks = ["stdout"]
ack = ["stdout"]
"#,
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");

    test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
    }))
    .await
}
