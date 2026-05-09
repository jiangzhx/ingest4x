use crate::support::mock_services::build_app_state_with_test_processor;
use actix_http::StatusCode;
use actix_web::{test, App};
use ingest4x::server;
use ingest4x::settings::Settings;
use serde_json::{json, Value};
use std::fs;
use std::future::Future;
use std::sync::Arc;
use tempfile::tempdir;

const ADMIN_PASSWORD_HEADER: &str = "x-admin-password";
const TEST_ADMIN_PASSWORD: &str = "test-admin-password";
const ADMIN_PASSWORD_ENV: &str = "INGEST4X_ADMIN_PASSWORD";

struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn remove(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        std::env::remove_var(key);
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

async fn with_admin_password_env<F, Fut>(_password: Option<&str>, action: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    let _env_guard = EnvVarGuard::remove(ADMIN_PASSWORD_ENV);
    action().await;
}

fn with_admin_password(request: test::TestRequest) -> test::TestRequest {
    request.insert_header((ADMIN_PASSWORD_HEADER, TEST_ADMIN_PASSWORD))
}

#[actix_rt::test]
async fn create_then_list_projects() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let create_response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/projects")
                .set_json(json!({
                    "name": "Admin App",
                    "enabled": true,
                    "ingest_token": "igx_admin_app"
                }))
                .to_request(),
        )
        .await;
        let create_status = create_response.status();
        let created: Value = test::read_body_json(create_response).await;

        assert_eq!(create_status, StatusCode::CREATED);
        assert_eq!(
            created,
            json!({
                "name": "Admin App",
                "enabled": true,
                "id": created["id"],
                "ingest_token": "igx_admin_app",
                "ingest_token_prefix": "igx_admin_ap...",
                "created_at": created["created_at"],
                "updated_at": created["updated_at"]
            })
        );

        let list_response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/projects")
                .to_request(),
        )
        .await;
        let list_status = list_response.status();
        let listed: Value = test::read_body_json(list_response).await;

        assert_eq!(list_status, StatusCode::OK);
        let listed = listed.as_array().expect("projects should be an array");
        assert!(listed.iter().any(|project| project["id"] == created["id"]
            && project["ingest_token"] == json!("igx_admin_app")));
        assert!(listed
            .iter()
            .any(|project| project["name"] == json!("test_app")));

        let detail_response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri(format!("/api/admin/projects/{}", created["id"]).as_str())
                .to_request(),
        )
        .await;
        let detail_status = detail_response.status();
        let detailed: Value = test::read_body_json(detail_response).await;

        assert_eq!(detail_status, StatusCode::OK);
        assert_eq!(detailed["id"], created["id"]);
        assert_eq!(detailed["ingest_token"], json!("igx_admin_app"));
    })
    .await;
}

#[actix_rt::test]
async fn update_changes_name_and_enabled() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;
        let project = create_project_for_test(&app, "APPID", "Original Name").await;

        let response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri(format!("/api/admin/projects/{}", project["id"]).as_str())
                .set_json(json!({
                    "name": "Updated Name",
                    "enabled": false
                }))
                .to_request(),
        )
        .await;
        let status = response.status();
        let updated: Value = test::read_body_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(updated["id"], project["id"]);
        assert_eq!(updated["name"], "Updated Name");
        assert_eq!(updated["enabled"], false);
    })
    .await;
}

#[actix_rt::test]
async fn delete_removes_project() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;
        let project = create_project_for_test(&app, "APPID", "Delete Me").await;

        let delete_response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::delete())
                .uri(format!("/api/admin/projects/{}", project["id"]).as_str())
                .to_request(),
        )
        .await;

        assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

        let list_response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/projects")
                .to_request(),
        )
        .await;
        let list_status = list_response.status();
        let listed: Value = test::read_body_json(list_response).await;

        assert_eq!(list_status, StatusCode::OK);
        let listed = listed.as_array().expect("projects should be an array");
        assert!(!listed
            .iter()
            .any(|candidate| candidate["id"] == project["id"]));
        assert!(listed
            .iter()
            .any(|project| project["name"] == json!("test_app")));

        let detail_response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri(format!("/api/admin/projects/{}", project["id"]).as_str())
                .to_request(),
        )
        .await;

        assert_eq!(detail_response.status(), StatusCode::NOT_FOUND);
    })
    .await;
}

async fn create_project_for_test<S>(app: &S, appid: &str, name: &str) -> Value
where
    S: actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse,
        Error = actix_web::Error,
    >,
{
    let response = test::call_service(
        app,
        with_admin_password(test::TestRequest::post())
            .uri("/api/admin/projects")
            .set_json(json!({
                "name": name,
                "enabled": true,
                "ingest_token": format!("igx_{appid}")
            }))
            .to_request(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CREATED);
    test::read_body_json(response).await
}

#[actix_rt::test]
async fn admin_create_is_visible_to_ingest_immediately() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app_state = create_app_state("").await;
        let public_app = test::init_service(App::new().configure(|cfg| {
            server::configure_public_app(cfg, app_state.clone());
        }))
        .await;
        let private_app = test::init_service(App::new().configure(|cfg| {
            server::configure_private_app(cfg, app_state.clone());
        }))
        .await;

        let ingest_before = test::call_service(
            &public_app,
            test::TestRequest::post()
                .uri("/ingest")
                .insert_header(("x-ingest-token", "igx_admin_app"))
                .set_json(valid_ingest_payload("admin-app"))
                .to_request(),
        )
        .await;
        let ingest_before_status = ingest_before.status();
        let ingest_before_body = test::read_body(ingest_before).await;

        assert_eq!(ingest_before_status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            std::str::from_utf8(ingest_before_body.as_ref()).unwrap(),
            "invalid ingest token"
        );

        let create_response = test::call_service(
            &private_app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/projects")
                .set_json(json!({
                    "name": "Admin App",
                    "enabled": true,
                    "ingest_token": "igx_admin_app"
                }))
                .to_request(),
        )
        .await;

        assert_eq!(create_response.status(), StatusCode::CREATED);

        let ingest_after = test::call_service(
            &public_app,
            test::TestRequest::post()
                .uri("/ingest")
                .insert_header(("x-ingest-token", "igx_admin_app"))
                .set_json(valid_ingest_payload("admin-app"))
                .to_request(),
        )
        .await;
        let ingest_after_status = ingest_after.status();
        let ingest_after_body = test::read_body(ingest_after).await;

        assert_eq!(ingest_after_status, StatusCode::OK);
        assert_eq!(
            std::str::from_utf8(ingest_after_body.as_ref()).unwrap(),
            "200"
        );
    })
    .await;
}

#[actix_rt::test]
async fn openapi_json_includes_admin_projects_paths() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

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
        assert!(body["paths"]["/api/admin/projects"].is_object());
        assert!(body["paths"]["/api/admin/projects/{project_id}"].is_object());
        assert!(body["paths"]["/api/admin/projects"]["post"]["responses"]["400"].is_object());
        assert!(body["paths"]["/api/admin/projects"]["post"]["responses"]["415"].is_null());
        assert_eq!(
            body["paths"]["/api/admin/projects"]["post"]["responses"]["500"]["description"],
            "Repository failure"
        );
        assert!(
            body["paths"]["/api/admin/projects/{project_id}"]["put"]["responses"]["400"]
                .is_object()
        );
        assert!(
            body["paths"]["/api/admin/projects/{project_id}"]["put"]["responses"]["415"].is_null()
        );
        assert_eq!(
            body["paths"]["/api/admin/projects/{project_id}"]["put"]["responses"]["500"]
                ["description"],
            "Repository failure"
        );
        assert_eq!(
            body["paths"]["/api/admin/projects/{project_id}"]["delete"]["responses"]["500"]
                ["description"],
            "Repository failure"
        );
    })
    .await;
}

#[actix_rt::test]
async fn admin_projects_requires_password_header() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/admin/projects")
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    })
    .await;
}

#[actix_rt::test]
async fn admin_projects_rejects_invalid_password_header() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/admin/projects")
                .insert_header((ADMIN_PASSWORD_HEADER, "wrong-password"))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    })
    .await;
}

#[actix_rt::test]
async fn admin_projects_accepts_valid_password_header() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/projects")
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
    })
    .await;
}

#[actix_rt::test]
async fn admin_login_accepts_valid_password() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/admin/auth/login")
                .set_json(json!({
                    "password": TEST_ADMIN_PASSWORD
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    })
    .await;
}

#[actix_rt::test]
async fn admin_login_rejects_invalid_password() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/admin/auth/login")
                .set_json(json!({
                    "password": "wrong-password"
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    })
    .await;
}

#[actix_rt::test]
async fn swagger_and_openapi_routes_do_not_require_admin_password() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let openapi_response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api-docs/openapi.json")
                .to_request(),
        )
        .await;
        let swagger_response = test::call_service(
            &app,
            test::TestRequest::get().uri("/swagger-ui/").to_request(),
        )
        .await;

        assert_eq!(openapi_response.status(), StatusCode::OK);
        assert_eq!(swagger_response.status(), StatusCode::OK);
    })
    .await;
}

#[actix_rt::test]
async fn swagger_ui_is_served_from_actix() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let response = test::call_service(
            &app,
            test::TestRequest::get().uri("/swagger-ui/").to_request(),
        )
        .await;
        let status = response.status();
        let body = test::read_body(response).await;
        let content = std::str::from_utf8(body.as_ref()).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(content.contains("Swagger UI"));
    })
    .await;
}

#[actix_rt::test]
async fn create_rejects_invalid_json_with_bad_request() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/projects")
                .insert_header(("content-type", "application/json"))
                .set_payload("{")
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    })
    .await;
}

#[actix_rt::test]
async fn create_reports_bad_request_for_non_json_content_type() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app_with_mock_projects("").await;

        let response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/projects")
                .insert_header(("content-type", "text/plain"))
                .set_payload("not json")
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    })
    .await;
}

fn valid_ingest_payload(appid: &str) -> Value {
    json!({
        "appid": appid,
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-1",
            "os": "ios",
            "idfa": "idfa-1"
        }
    })
}

async fn create_app_with_mock_projects(
    mock_projects_toml: &str,
) -> impl actix_web::dev::Service<
    actix_http::Request,
    Response = actix_web::dev::ServiceResponse,
    Error = actix_web::Error,
> {
    let app_state = create_app_state(mock_projects_toml).await;

    test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
    }))
    .await
}

async fn create_app_state(mock_projects_toml: &str) -> server::AppState {
    let _ = mock_projects_toml;
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("mock-config.toml");

    fs::write(
        &config_path,
        format!(
            r#"
[ingest]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"
admin_password = "test-admin-password"

[database]
url = "sqlite::memory:"

[wal]
dir = "{}"

[events.sink.events]
type = "stdout"

[events.sink.events_error]
type = "stdout"
"#,
            temp.path().join("wal").display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");
    let _kept_temp = temp.keep();
    app_state
}
