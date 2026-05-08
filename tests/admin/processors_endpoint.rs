use actix_http::StatusCode;
use actix_web::web::{self, Data, Path};
use actix_web::{test, App, HttpResponse};
use ingest4x::ingest::processor::{
    ProcessorRegistryState, ProcessorRequestContext, ProcessorRuntime,
};
use ingest4x::rules::Rules;
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
async fn admin_can_create_script_and_bind_project_processor() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;
        let project = create_project_for_test(&app, "app-custom").await;

        let before_create = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/processor-scripts")
                .to_request(),
        )
        .await;
        assert_eq!(before_create.status(), StatusCode::OK);

        let create_script = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/processor-scripts")
                .set_json(json!({
                    "script_key": "custom_pipeline",
                    "name": "Custom Pipeline",
                    "entry_module": "main",
                    "status": "active",
                    "modules": [
                        {
                            "module_name": "main",
                            "source": "fn process(event, request) { emit(\"custom_events\", event); }"
                        }
                    ]
                }))
                .to_request(),
        )
        .await;
        let create_status = create_script.status();
        assert_eq!(create_status, StatusCode::CREATED);
        let script: Value = test::read_body_json(create_script).await;
        assert_eq!(script["script_key"], "custom_pipeline");

        let detail = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri(format!("/api/admin/processor-scripts/{}", script["id"]).as_str())
                .to_request(),
        )
        .await;
        let detail_status = detail.status();
        let detail: Value = test::read_body_json(detail).await;
        assert_eq!(detail_status, StatusCode::OK);
        assert_eq!(detail["modules"][0]["module_name"], "main");

        let assign = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri(format!("/api/admin/projects/{}/processor", project["id"]).as_str())
                .set_json(json!({
                    "processor_script_id": script["id"],
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(assign.status(), StatusCode::NO_CONTENT);

        let bindings = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/project-processors")
                .to_request(),
        )
        .await;
        let bindings_status = bindings.status();
        let bindings: Value = test::read_body_json(bindings).await;
        assert_eq!(bindings_status, StatusCode::OK);
        assert!(bindings
            .as_array()
            .expect("project processors should be an array")
            .iter()
            .any(|binding| binding["project_id"] == project["id"]
                && binding["processor_script_id"] == script["id"]));

        let probe = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri(format!("/probe-processors/{}", project["id"]).as_str())
                .to_request(),
        )
        .await;
        let probe_status = probe.status();
        let probe: Value = test::read_body_json(probe).await;
        assert_eq!(probe_status, StatusCode::OK);
        assert_eq!(probe["targets"], json!(["custom_events"]));

        let delete = test::call_service(
            &app,
            with_admin_password(test::TestRequest::delete())
                .uri(format!("/api/admin/projects/{}/processor", project["id"]).as_str())
                .to_request(),
        )
        .await;
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    })
    .await;
}

#[actix_rt::test]
async fn admin_rejects_inactive_binding_and_disabling_script_in_use() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;
        let project = create_project_for_test(&app, "app-guarded").await;

        let draft = create_processor_script(
            &app,
            "draft_pipeline",
            "Draft Pipeline",
            "draft",
            "fn process(event, request) { emit(\"draft_events\", event); }",
        )
        .await;
        let assign_draft = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri(format!("/api/admin/projects/{}/processor", project["id"]).as_str())
                .set_json(json!({
                    "processor_script_id": draft["id"],
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(assign_draft.status(), StatusCode::BAD_REQUEST);

        let active = create_processor_script(
            &app,
            "guarded_pipeline",
            "Guarded Pipeline",
            "active",
            "fn process(event, request) { emit(\"guarded_events\", event); }",
        )
        .await;
        let assign_active = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri(format!("/api/admin/projects/{}/processor", project["id"]).as_str())
                .set_json(json!({
                    "processor_script_id": active["id"],
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(assign_active.status(), StatusCode::NO_CONTENT);

        let disable_active = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri(format!("/api/admin/processor-scripts/{}/status", active["id"]).as_str())
                .set_json(json!({ "status": "archived" }))
                .to_request(),
        )
        .await;
        assert_eq!(disable_active.status(), StatusCode::CONFLICT);
    })
    .await;
}

#[actix_rt::test]
async fn admin_create_project_persists_default_processor_binding() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;
        let project = create_project_for_test(&app, "app-default").await;

        let scripts = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/processor-scripts")
                .to_request(),
        )
        .await;
        assert_eq!(scripts.status(), StatusCode::OK);
        let scripts: Value = test::read_body_json(scripts).await;
        let default_script_id = scripts
            .as_array()
            .expect("processor scripts should be an array")
            .iter()
            .find(|script| {
                script["script_key"] == json!("default") && script["status"] == json!("active")
            })
            .and_then(|script| script["id"].as_i64())
            .expect("active default processor should exist");

        let bindings = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/project-processors")
                .to_request(),
        )
        .await;
        assert_eq!(bindings.status(), StatusCode::OK);
        let bindings: Value = test::read_body_json(bindings).await;

        assert!(bindings
            .as_array()
            .expect("project processors should be an array")
            .iter()
            .any(|binding| binding["project_id"] == project["id"]
                && binding["enabled"] == json!(true)
                && binding["processor_script_id"] == json!(default_script_id)));
    })
    .await;
}

#[actix_rt::test]
async fn openapi_json_includes_admin_processor_paths() {
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
        assert!(body["paths"]["/api/admin/processor-scripts"].is_object());
        assert!(
            body["paths"]["/api/admin/processor-scripts/{processor_script_id}/status"].is_object()
        );
        assert!(body["paths"]["/api/admin/project-processors"].is_object());
        assert!(body["paths"]["/api/admin/projects/{project_id}/processor"].is_object());
    })
    .await;
}

async fn create_processor_script(
    app: &impl actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse,
        Error = actix_web::Error,
    >,
    script_key: &str,
    name: &str,
    status: &str,
    source: &str,
) -> Value {
    let response = test::call_service(
        app,
        with_admin_password(test::TestRequest::post())
            .uri("/api/admin/processor-scripts")
            .set_json(json!({
                "script_key": script_key,
                "name": name,
                "entry_module": "main",
                "status": status,
                "modules": [
                    {
                        "module_name": "main",
                        "source": source
                    }
                ]
            }))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    test::read_body_json(response).await
}

async fn create_project_for_test(
    app: &impl actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse,
        Error = actix_web::Error,
    >,
    appid: &str,
) -> Value {
    let response = test::call_service(
        app,
        with_admin_password(test::TestRequest::post())
            .uri("/api/admin/projects")
            .set_json(json!({
                "name": format!("Project {appid}"),
                "enabled": true,
                "ingest_token": format!("igx_{appid}")
            }))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    test::read_body_json(response).await
}

async fn probe_processor(path: Path<i32>, processor: Data<ProcessorRegistryState>) -> HttpResponse {
    match processor.process_event(
        *path,
        json!({
            "appid": format!("project-{}", *path),
            "xwhat": "probe",
            "xcontext": {}
        }),
        Rules::default(),
        ProcessorRequestContext::new(None, "POST", "/ingest", Default::default()),
    ) {
        Ok(output) => HttpResponse::Ok().json(json!({
            "targets": output
                .deliveries
                .into_iter()
                .map(|delivery| delivery.target)
                .collect::<Vec<_>>()
        })),
        Err(error) => HttpResponse::InternalServerError().body(error.to_string()),
    }
}

async fn create_app() -> impl actix_web::dev::Service<
    actix_http::Request,
    Response = actix_web::dev::ServiceResponse,
    Error = actix_web::Error,
> {
    let app_state = create_app_state().await;

    test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
        cfg.route("/probe-processors/{appid}", web::get().to(probe_processor));
    }))
    .await
}

async fn create_app_state() -> server::AppState {
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
"#,
            temp.path().join("wal").display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let _kept_temp = temp.keep();
    app_state
}
