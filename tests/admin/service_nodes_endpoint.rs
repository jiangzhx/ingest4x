use crate::support::mock_services::build_app_state_with_test_processor;
use actix_http::StatusCode;
use actix_web::{test, App};
use ingest4x::repositories::{RegisterServiceNodeInput, ServiceNodeStatus};
use ingest4x::server;
use ingest4x::settings::Settings;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use serde_json::{json, Value};
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

const ADMIN_PASSWORD_HEADER: &str = "x-admin-password";
const TEST_ADMIN_PASSWORD: &str = "test-admin-password";

fn with_admin_password(request: test::TestRequest) -> test::TestRequest {
    request.insert_header((ADMIN_PASSWORD_HEADER, TEST_ADMIN_PASSWORD))
}

#[actix_rt::test]
async fn list_service_nodes_returns_registered_nodes() {
    let app_state = create_app_state().await;
    app_state
        .service_node_repository()
        .register_service_node(RegisterServiceNodeInput {
            node_id: "node-a".to_string(),
            hostname: Some("ingest-a".to_string()),
            machine_ip: Some("10.0.0.1".to_string()),
            ingest_bind_address: "0.0.0.0:8090".to_string(),
            management_bind_address: "127.0.0.1:18090".to_string(),
            version: "0.0.1".to_string(),
            status: ServiceNodeStatus::Running,
            metadata_json: Some(json!({"zone": "az-a"})),
        })
        .await
        .expect("service node should register");

    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
    }))
    .await;

    let response = test::call_service(
        &app,
        with_admin_password(test::TestRequest::get())
            .uri("/api/admin/service-nodes")
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
                "node_id": "node-a",
                "hostname": "ingest-a",
                "machine_ip": "10.0.0.1",
                "ingest_bind_address": "0.0.0.0:8090",
                "management_bind_address": "127.0.0.1:18090",
                "version": "0.0.1",
                "status": "running",
                "started_at": body[0]["started_at"],
                "last_seen_at": body[0]["last_seen_at"],
                "updated_at": body[0]["updated_at"],
                "metadata_json": {
                    "zone": "az-a"
                }
            }
        ])
    );
}

#[actix_rt::test]
async fn service_nodes_requires_admin_password_header() {
    let app_state = create_app_state().await;
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
    }))
    .await;

    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/api/admin/service-nodes")
            .to_request(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[actix_rt::test]
async fn list_service_nodes_marks_old_running_nodes_as_stale() {
    let app_state = create_app_state().await;
    app_state
        .service_node_repository()
        .register_service_node(RegisterServiceNodeInput {
            node_id: "node-stale".to_string(),
            hostname: None,
            machine_ip: None,
            ingest_bind_address: "0.0.0.0:8090".to_string(),
            management_bind_address: "127.0.0.1:18090".to_string(),
            version: "0.0.1".to_string(),
            status: ServiceNodeStatus::Running,
            metadata_json: None,
        })
        .await
        .expect("service node should register");
    app_state
        .service_node_repository()
        .connection()
        .execute(Statement::from_string(
            DbBackend::Sqlite,
            "UPDATE service_nodes SET last_seen_at = 1 WHERE node_id = 'node-stale'",
        ))
        .await
        .expect("service node heartbeat should age");

    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
    }))
    .await;

    let response = test::call_service(
        &app,
        with_admin_password(test::TestRequest::get())
            .uri("/api/admin/service-nodes")
            .to_request(),
    )
    .await;
    let status = response.status();
    let body: Value = test::read_body_json(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body[0]["status"], json!("stale"));
}

#[actix_rt::test]
async fn openapi_json_includes_admin_service_nodes_path() {
    let app_state = create_app_state().await;
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
    }))
    .await;

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
    assert!(body["paths"]["/api/admin/service-nodes"].is_object());
    assert_eq!(
        body["paths"]["/api/admin/service-nodes"]["get"]["responses"]["200"]["description"],
        "List service nodes"
    );
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
