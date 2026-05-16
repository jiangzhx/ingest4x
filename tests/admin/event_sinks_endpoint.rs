use crate::support::mock_services::build_app_state_with_test_processor;
use actix_http::StatusCode;
use actix_web::web::{self, Data, Path};
use actix_web::{test, App, HttpResponse};
use ingest4x::server;
use ingest4x::settings::Settings;
use ingest4x::sinks::EventSinkState;
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
async fn admin_can_list_registered_sink_types() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;

        let response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/sink-types")
                .to_request(),
        )
        .await;
        let status = response.status();
        let body: Value = test::read_body_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!([
                {
                    "target_type": "blackhole",
                    "label": "Blackhole"
                },
                {
                    "target_type": "kafka",
                    "label": "Kafka"
                },
                {
                    "target_type": "parquet",
                    "label": "Parquet"
                },
                {
                    "target_type": "stdout",
                    "label": "stdout"
                }
            ])
        );
    })
    .await;
}

#[actix_rt::test]
async fn admin_can_create_and_list_delivery_targets_and_event_sinks() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;

        let create_target = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/delivery-targets")
                .set_json(json!({
                    "target_id": "stdout_main",
                    "name": "Main stdout",
                    "target_type": "stdout",
                    "config_json": {},
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        let target_status = create_target.status();
        let target: Value = test::read_body_json(create_target).await;
        assert_eq!(target_status, StatusCode::CREATED);

        let create_sink = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/event-sinks")
                .set_json(json!({
                    "sink_id": "api_events",
                    "name": "API events",
                    "delivery_target_id": target["id"],
                    "destination_json": {},
                    "auto_offset_reset": "latest",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        let sink_status = create_sink.status();
        let sink: Value = test::read_body_json(create_sink).await;
        assert_eq!(sink_status, StatusCode::CREATED);
        assert_eq!(sink["sink_id"], "api_events");

        let list_sinks = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/event-sinks")
                .to_request(),
        )
        .await;
        let listed: Value = test::read_body_json(list_sinks).await;
        assert!(listed
            .as_array()
            .expect("event sinks should be an array")
            .iter()
            .any(|item| item["sink_id"] == json!("api_events")));
    })
    .await;
}

#[actix_rt::test]
async fn admin_event_sink_create_refreshes_runtime_router() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app_state = create_app_state().await;
        let app = test::init_service(App::new().configure(|cfg| {
            server::configure_private_app(cfg, app_state.clone());
            cfg.route("/probe-sinks/{sink_id}", web::get().to(probe_sink));
        }))
        .await;

        let before = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/probe-sinks/api_events")
                .to_request(),
        )
        .await;
        assert_eq!(before.status(), StatusCode::NOT_FOUND);

        let target = create_stdout_target(&app).await;
        let create_sink = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/event-sinks")
                .set_json(json!({
                    "sink_id": "api_events",
                    "name": "API events",
                    "delivery_target_id": target["id"],
                    "destination_json": {},
                    "auto_offset_reset": "earliest",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(create_sink.status(), StatusCode::CREATED);

        let after = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/probe-sinks/api_events")
                .to_request(),
        )
        .await;
        assert_eq!(after.status(), StatusCode::OK);
    })
    .await;
}

#[actix_rt::test]
async fn admin_can_disable_and_delete_event_sinks_with_runtime_refresh() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app_state = create_app_state().await;
        let app = test::init_service(App::new().configure(|cfg| {
            server::configure_private_app(cfg, app_state.clone());
            cfg.route("/probe-sinks/{sink_id}", web::get().to(probe_sink));
        }))
        .await;

        let target = create_stdout_target(&app).await;
        let create_sink = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/event-sinks")
                .set_json(json!({
                    "sink_id": "api_events",
                    "name": "API events",
                    "delivery_target_id": target["id"],
                    "destination_json": {},
                    "auto_offset_reset": "latest",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        let sink: Value = test::read_body_json(create_sink).await;

        let visible = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/probe-sinks/api_events")
                .to_request(),
        )
        .await;
        assert_eq!(visible.status(), StatusCode::OK);

        let disable = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri(format!("/api/admin/event-sinks/{}", sink["id"]).as_str())
                .set_json(json!({
                    "enabled": false
                }))
                .to_request(),
        )
        .await;
        assert_eq!(disable.status(), StatusCode::OK);

        let hidden = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/probe-sinks/api_events")
                .to_request(),
        )
        .await;
        assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

        let delete = test::call_service(
            &app,
            with_admin_password(test::TestRequest::delete())
                .uri(format!("/api/admin/event-sinks/{}", sink["id"]).as_str())
                .to_request(),
        )
        .await;
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);

        let list = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/api/admin/event-sinks")
                .to_request(),
        )
        .await;
        let listed: Value = test::read_body_json(list).await;
        assert!(!listed
            .as_array()
            .expect("event sinks should be an array")
            .iter()
            .any(|item| item["sink_id"] == json!("api_events")));
    })
    .await;
}

#[actix_rt::test]
async fn admin_can_disable_delivery_target_with_runtime_refresh() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app_state = create_app_state().await;
        let app = test::init_service(App::new().configure(|cfg| {
            server::configure_private_app(cfg, app_state.clone());
            cfg.route("/probe-sinks/{sink_id}", web::get().to(probe_sink));
        }))
        .await;

        let target = create_stdout_target(&app).await;
        let create_sink = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/event-sinks")
                .set_json(json!({
                    "sink_id": "api_events",
                    "name": "API events",
                    "delivery_target_id": target["id"],
                    "destination_json": {},
                    "auto_offset_reset": "latest",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(create_sink.status(), StatusCode::CREATED);

        let visible = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/probe-sinks/api_events")
                .to_request(),
        )
        .await;
        assert_eq!(visible.status(), StatusCode::OK);

        let disable_target = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri(format!("/api/admin/delivery-targets/{}", target["id"]).as_str())
                .set_json(json!({
                    "enabled": false
                }))
                .to_request(),
        )
        .await;
        assert_eq!(disable_target.status(), StatusCode::OK);

        let hidden = test::call_service(
            &app,
            with_admin_password(test::TestRequest::get())
                .uri("/probe-sinks/api_events")
                .to_request(),
        )
        .await;
        assert_eq!(hidden.status(), StatusCode::NOT_FOUND);
    })
    .await;
}

#[actix_rt::test]
async fn admin_rejects_deleting_delivery_target_used_by_event_sink() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;
        let target = create_stdout_target(&app).await;

        let create_sink = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/event-sinks")
                .set_json(json!({
                    "sink_id": "api_events",
                    "name": "API events",
                    "delivery_target_id": target["id"],
                    "destination_json": {},
                    "auto_offset_reset": "latest",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(create_sink.status(), StatusCode::CREATED);

        let delete_target = test::call_service(
            &app,
            with_admin_password(test::TestRequest::delete())
                .uri(format!("/api/admin/delivery-targets/{}", target["id"]).as_str())
                .to_request(),
        )
        .await;

        assert_eq!(delete_target.status(), StatusCode::CONFLICT);
    })
    .await;
}

#[actix_rt::test]
async fn admin_can_update_event_sink_delivery_target() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;
        let stdout_target = create_stdout_target(&app).await;
        let kafka_target = create_kafka_target(&app).await;

        let create_sink = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/event-sinks")
                .set_json(json!({
                    "sink_id": "api_events",
                    "name": "API events",
                    "delivery_target_id": stdout_target["id"],
                    "destination_json": {},
                    "auto_offset_reset": "latest",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;
        let sink: Value = test::read_body_json(create_sink).await;

        let update = test::call_service(
            &app,
            with_admin_password(test::TestRequest::put())
                .uri(format!("/api/admin/event-sinks/{}", sink["id"]).as_str())
                .set_json(json!({
                    "delivery_target_id": kafka_target["id"],
                    "destination_json": {"topic": "ingest4x-events"}
                }))
                .to_request(),
        )
        .await;
        let status = update.status();
        let updated: Value = test::read_body_json(update).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(updated["delivery_target_id"], kafka_target["id"]);
        assert_eq!(updated["destination_json"]["topic"], "ingest4x-events");
    })
    .await;
}

#[actix_rt::test]
async fn admin_rejects_sink_destination_that_does_not_match_target_type() {
    with_admin_password_env(Some(TEST_ADMIN_PASSWORD), || async {
        let app = create_app().await;
        let target = create_kafka_target(&app).await;

        let response = test::call_service(
            &app,
            with_admin_password(test::TestRequest::post())
                .uri("/api/admin/event-sinks")
                .set_json(json!({
                    "sink_id": "bad_events",
                    "name": "Bad events",
                    "delivery_target_id": target["id"],
                    "destination_json": {"table": "events"},
                    "auto_offset_reset": "latest",
                    "enabled": true
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    })
    .await;
}

#[actix_rt::test]
async fn openapi_json_includes_admin_event_sink_paths() {
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
        assert!(body["paths"]["/api/admin/delivery-targets"].is_object());
        assert!(body["paths"]["/api/admin/event-sinks"].is_object());
    })
    .await;
}

async fn create_stdout_target(
    app: &impl actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse,
        Error = actix_web::Error,
    >,
) -> Value {
    let response = test::call_service(
        app,
        with_admin_password(test::TestRequest::post())
            .uri("/api/admin/delivery-targets")
            .set_json(json!({
                "target_id": "stdout_main",
                "name": "Main stdout",
                "target_type": "stdout",
                "config_json": {},
                "enabled": true
            }))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    test::read_body_json(response).await
}

async fn create_kafka_target(
    app: &impl actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse,
        Error = actix_web::Error,
    >,
) -> Value {
    let response = test::call_service(
        app,
        with_admin_password(test::TestRequest::post())
            .uri("/api/admin/delivery-targets")
            .set_json(json!({
                "target_id": "kafka_main",
                "name": "Main Kafka",
                "target_type": "kafka",
                "config_json": {
                    "bootstrap_servers": "127.0.0.1:9092"
                },
                "enabled": true
            }))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    test::read_body_json(response).await
}

async fn probe_sink(path: Path<String>, event_sinks: Data<EventSinkState>) -> HttpResponse {
    if event_sinks.contains_sink(&path) {
        HttpResponse::Ok().finish()
    } else {
        HttpResponse::NotFound().finish()
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
    let app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");
    let _kept_temp = temp.keep();
    app_state
}
