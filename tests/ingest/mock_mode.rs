use actix_http::StatusCode;
use actix_web::{test, App};
use ingest4x::server;
use ingest4x::settings::Settings;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

#[actix_rt::test]
async fn config_without_database_runs_ingest_without_external_services() {
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
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;

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
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::OK);
    assert_eq!(std::str::from_utf8(body.as_ref()).unwrap(), "200");
}

#[actix_rt::test]
async fn config_uses_info_json_logging_by_default() {
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

    let settings = Settings::init_with_file(config_path.to_str().expect("config path"))
        .expect("settings should load");

    assert_eq!(settings.logging.level, ingest4x::settings::LogLevel::Info);
    assert_eq!(settings.logging.format, "json");
}

#[actix_rt::test]
async fn config_without_database_real_server_wiring_rejects_unknown_project_on_post_ingest() {
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
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;

    let req = test::TestRequest::post()
        .uri("/ingest")
        .set_json(json!({
            "appid": "UNKNOWN",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-1",
                "os": "ios",
                "idfa": "idfa-1"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::NOT_FOUND);
    assert_eq!(
        std::str::from_utf8(body.as_ref()).unwrap(),
        "Project not found"
    );
}
