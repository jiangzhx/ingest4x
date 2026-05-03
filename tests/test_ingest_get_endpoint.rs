#![cfg(feature = "ingest")]

use actix_http::StatusCode;
use actix_web::{test, App};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ingest4x::server;
use ingest4x::settings::Settings;
use serde_json::{json, Value};
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[actix_rt::test]
async fn get_ingest_decodes_base64_json_and_sends_it_to_file_sink() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("mock-config.toml");
    let sink_path = temp.path().join("mock-events.jsonl");

    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.file_valid]
type = "file"
path = "{}"
format = "jsonl"
rotation = "never"

[[events.valid.routes]]
sinks = ["file_valid"]
ack = ["file_valid"]

[events.sink.stdout_invalid]
type = "stdout"

[[events.invalid.routes]]
sinks = ["stdout_invalid"]
ack = ["stdout_invalid"]
"#,
            sink_path.display()
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

    let input_payload = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-1",
            "os": "ios",
            "idfa": "idfa-1",
            "currencytype": "cny"
        }
    });

    let encoded = STANDARD.encode(serde_json::to_vec(&input_payload).expect("serialize payload"));
    let query = serde_urlencoded::to_string([("data", encoded.as_str())]).expect("encode query");
    let req = test::TestRequest::get()
        .uri(format!("/ingest?{query}").as_str())
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::OK);
    assert_eq!(std::str::from_utf8(body.as_ref()).unwrap(), "200");

    let output = fs::read_to_string(&sink_path).expect("read event sink");
    let line = output.lines().next().expect("missing emitted event");
    let emitted = parse_event_sink_line(line);

    assert_eq!(emitted["appid"], input_payload["appid"]);
    assert_eq!(emitted["xwhat"], input_payload["xwhat"]);
    assert_eq!(emitted["xcontext"]["installid"], json!("iid-1"));
    assert_eq!(emitted["xcontext"]["os"], json!("ios"));
    assert_eq!(emitted["xcontext"]["idfa"], json!("idfa-1"));
    assert_eq!(emitted["xcontext"]["currencytype"], json!("CNY"));
    assert_eq!(emitted["xcontext"]["platform"], json!("ios"));
    assert!(emitted["xcontext"]["process_info"].is_object());
    assert!(emitted["xcontext"]["process_info"]["receive_time"].is_number());
    assert_eq!(
        emitted["xcontext"]["process_info"]["ingest4x_version"],
        json!(env!("CARGO_PKG_VERSION"))
    );
    assert!(emitted["xwhen"].is_number());
}

fn parse_event_sink_line(line: &str) -> Value {
    serde_json::from_str(line).expect("event sink line should be valid json")
}

fn read_first_line_with_retry(path: &std::path::Path) -> String {
    let deadline = Instant::now() + Duration::from_secs(2);

    loop {
        if let Ok(output) = fs::read_to_string(path) {
            if let Some(line) = output.lines().next() {
                return line.to_string();
            }
        }

        if Instant::now() >= deadline {
            panic!("missing event sink line at {}", path.display());
        }

        thread::sleep(Duration::from_millis(20));
    }
}

#[actix_rt::test]
async fn get_ingest_default_rhai_processor_uses_existing_validator() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("mock-config.toml");
    let valid_sink_path = temp.path().join("valid.jsonl");
    let invalid_sink_path = temp.path().join("invalid.jsonl");

    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.file_valid]
type = "file"
path = "{}"
format = "jsonl"
rotation = "never"

[[events.valid.routes]]
sinks = ["file_valid"]
ack = ["file_valid"]

[events.sink.file_invalid]
type = "file"
path = "{}"
format = "jsonl"
rotation = "never"

[[events.invalid.routes]]
sinks = ["file_invalid"]
ack = ["file_invalid"]
"#,
            valid_sink_path.display(),
            invalid_sink_path.display()
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

    let invalid_payload = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "os": "ios"
        }
    });

    let encoded = STANDARD.encode(serde_json::to_vec(&invalid_payload).expect("serialize payload"));
    let query = serde_urlencoded::to_string([("data", encoded.as_str())]).expect("encode query");
    let req = test::TestRequest::get()
        .uri(format!("/ingest?{query}").as_str())
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;
    let body_text = std::str::from_utf8(body.as_ref()).unwrap();

    assert_eq!(status_code, StatusCode::BAD_REQUEST);
    assert!(body_text.contains("xcontext.installid"));

    assert!(
        fs::read_to_string(&valid_sink_path)
            .map(|content| content.trim().is_empty())
            .unwrap_or(true),
        "invalid payload should not emit valid sink events"
    );
    let line = read_first_line_with_retry(&invalid_sink_path);
    assert_eq!(parse_event_sink_line(&line), invalid_payload);
}

#[actix_rt::test]
async fn get_ingest_returns_not_found_for_unknown_project_via_real_server_wiring() {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("mock-config.toml");
    let sink_path = temp.path().join("mock-events.jsonl");

    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.file_valid]
type = "file"
path = "{}"
format = "jsonl"
rotation = "never"

[[events.valid.routes]]
sinks = ["file_valid"]
ack = ["file_valid"]

[events.sink.stdout_invalid]
type = "stdout"

[[events.invalid.routes]]
sinks = ["stdout_invalid"]
ack = ["stdout_invalid"]
"#,
            sink_path.display()
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

    let input_payload = json!({
        "appid": "UNKNOWN",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-1",
            "os": "ios",
            "idfa": "idfa-1"
        }
    });

    let encoded = STANDARD.encode(serde_json::to_vec(&input_payload).expect("serialize payload"));
    let query = serde_urlencoded::to_string([("data", encoded.as_str())]).expect("encode query");
    let req = test::TestRequest::get()
        .uri(format!("/ingest?{query}").as_str())
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::NOT_FOUND);
    assert_eq!(
        std::str::from_utf8(body.as_ref()).unwrap(),
        "Project not found"
    );
    assert!(
        fs::read_to_string(&sink_path)
            .map(|content| content.trim().is_empty())
            .unwrap_or(true),
        "unknown project should not emit event sink events"
    );
}
