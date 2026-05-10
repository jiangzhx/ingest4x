use crate::support::mock_services::build_app_state_with_test_processor;
use actix_http::StatusCode;
use actix_web::{test, App};
use ingest4x::server;
use ingest4x::settings::Settings;
use ingest4x::utils::get_host_ip;
use ingest4x::utils::prometheus::init_private_prometheus;
use ingest4x::wal::read_all_records;
use prometheus::Registry;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

#[actix_rt::test]
async fn healthz_reports_ok_before_accepting_wal_ingest_requests() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
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
flush_max_records = 1

"#,
            wal_dir.display()
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
    let public_app = test::init_service(App::new().configure(|cfg| {
        server::configure_public_app(cfg, app_state.clone());
    }))
    .await;
    let private_app = test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
    }))
    .await;

    let public_health_resp = test::call_service(
        &public_app,
        test::TestRequest::get().uri("/healthz").to_request(),
    )
    .await;
    assert_eq!(public_health_resp.status(), StatusCode::NOT_FOUND);

    let health_resp = test::call_service(
        &private_app,
        test::TestRequest::get().uri("/healthz").to_request(),
    )
    .await;
    assert_eq!(health_resp.status(), StatusCode::OK);
    let health_body: serde_json::Value =
        serde_json::from_slice(&test::read_body(health_resp).await).expect("health json");
    assert_eq!(health_body["status"], json!("ok"));
    assert_eq!(health_body["wal_enabled"], json!(true));
    assert_eq!(health_body["wal_ready"], json!(true));

    let ingest_resp = test::call_service(
        &public_app,
        test::TestRequest::post()
            .uri("/ingest")
            .insert_header(("x-ingest-token", "igx_APPID"))
            .set_payload(
                serde_json::to_vec(&json!({
                    "appid": "APPID",
                    "xwhat": "startup",
                    "xcontext": {
                        "installid": "iid-ready",
                        "os": "ios",
                        "idfa": "idfa-ready"
                    }
                }))
                .expect("serialize payload"),
            )
            .insert_header(("content-type", "application/json"))
            .to_request(),
    )
    .await;
    assert_eq!(ingest_resp.status(), StatusCode::OK);
    assert_eq!(
        read_all_records(&wal_dir).expect("read wal records").len(),
        1
    );
}

#[actix_rt::test]
async fn private_metrics_include_wal_health_gauges() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
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
flush_max_records = 1

"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let mut app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");
    let registry = Registry::new();
    server::register_wal_prometheus_metrics(&registry, &mut app_state)
        .expect("register wal metrics");

    let public_app = test::init_service(App::new().configure(|cfg| {
        server::configure_public_app(cfg, app_state.clone());
    }))
    .await;
    let private_app = test::init_service(
        App::new()
            .wrap(init_private_prometheus(registry))
            .configure(|cfg| {
                server::configure_private_app(cfg, app_state.clone());
            }),
    )
    .await;

    let ingest_resp = test::call_service(
        &public_app,
        test::TestRequest::post()
            .uri("/ingest")
            .insert_header(("x-ingest-token", "igx_APPID"))
            .set_payload(
                serde_json::to_vec(&json!({
                    "appid": "APPID",
                    "xwhat": "startup",
                    "xcontext": {
                        "installid": "iid-metrics",
                        "os": "ios",
                        "idfa": "idfa-metrics"
                    }
                }))
                .expect("serialize payload"),
            )
            .insert_header(("content-type", "application/json"))
            .to_request(),
    )
    .await;
    assert_eq!(ingest_resp.status(), StatusCode::OK);

    let metrics_resp = test::call_service(
        &private_app,
        test::TestRequest::get().uri("/metrics").to_request(),
    )
    .await;
    assert_eq!(metrics_resp.status(), StatusCode::OK);
    let metrics =
        String::from_utf8(test::read_body(metrics_resp).await.to_vec()).expect("metrics text");
    let node_id = fs::read_to_string(wal_dir.join("node_id")).expect("read node id");
    let node_id = node_id.trim();
    let machine_ip = get_host_ip();

    assert!(metrics.contains("# HELP wal_enabled"));
    assert!(metrics.contains("# HELP wal_node_info"));
    assert!(metrics.contains(
        format!(r#"wal_node_info{{machine_ip="{machine_ip}",node_id="{node_id}"}} 1"#).as_str()
    ));
    assert!(metrics.contains("wal_enabled 1"));
    assert!(metrics.contains("wal_ready 1"));
    assert!(metrics.contains("wal_reliable_ack 1"));
    assert!(metrics.contains("wal_no_sync 0"));
    assert!(metrics.contains("wal_min_free_bytes 0"));
    assert!(metrics.contains("wal_active_segment_id 1"));
    assert!(metrics.contains("wal_checkpoint_lsn 0"));
    assert!(metrics.contains("wal_max_lsn 1"));
    assert!(metrics.contains("wal_replay_lag_lsn 1"));
    assert!(metrics.contains(
        r#"ingest_events_total{project_id="1",result="wal_appended",xwhat="startup"} 1"#
    ));
    assert!(metrics.contains(
        r#"ingest_event_duration_seconds_count{project_id="1",result="wal_appended",xwhat="startup"} 1"#
    ));
}

#[actix_rt::test]
async fn private_metrics_refresh_wal_lag_after_successful_replay() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
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
flush_max_records = 1

"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let mut app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");
    let registry = Registry::new();
    server::register_wal_prometheus_metrics(&registry, &mut app_state)
        .expect("register wal metrics");

    let public_app = test::init_service(App::new().configure(|cfg| {
        server::configure_public_app(cfg, app_state.clone());
    }))
    .await;
    let private_app = test::init_service(
        App::new()
            .wrap(init_private_prometheus(registry))
            .configure(|cfg| {
                server::configure_private_app(cfg, app_state.clone());
            }),
    )
    .await;

    let ingest_resp = test::call_service(
        &public_app,
        test::TestRequest::post()
            .uri("/ingest")
            .insert_header(("x-ingest-token", "igx_APPID"))
            .set_payload(
                serde_json::to_vec(&json!({
                    "appid": "APPID",
                    "xwhat": "startup",
                    "xcontext": {
                        "installid": "iid-replay-metrics",
                        "os": "ios",
                        "idfa": "idfa-replay-metrics"
                    }
                }))
                .expect("serialize payload"),
            )
            .insert_header(("content-type", "application/json"))
            .to_request(),
    )
    .await;
    assert_eq!(ingest_resp.status(), StatusCode::OK);

    assert_eq!(
        server::replay_wal_once(&app_state)
            .await
            .expect("replay wal"),
        1
    );

    let metrics_resp = test::call_service(
        &private_app,
        test::TestRequest::get().uri("/metrics").to_request(),
    )
    .await;
    assert_eq!(metrics_resp.status(), StatusCode::OK);
    let metrics =
        String::from_utf8(test::read_body(metrics_resp).await.to_vec()).expect("metrics text");

    assert!(metrics.contains("wal_checkpoint_lsn 1"));
    assert!(metrics.contains("wal_replay_lag_lsn 0"));
}

#[actix_rt::test]
async fn private_metrics_include_ingest_business_labels_for_wal_appends() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
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
flush_max_records = 1

"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let mut app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");
    let registry = Registry::new();
    server::register_wal_prometheus_metrics(&registry, &mut app_state).expect("register metrics");

    let public_app = test::init_service(App::new().configure(|cfg| {
        server::configure_public_app(cfg, app_state.clone());
    }))
    .await;
    let private_app = test::init_service(
        App::new()
            .wrap(init_private_prometheus(registry))
            .configure(|cfg| {
                server::configure_private_app(cfg, app_state.clone());
            }),
    )
    .await;

    let ingest_resp = test::call_service(
        &public_app,
        test::TestRequest::post()
            .uri("/ingest")
            .insert_header(("x-ingest-token", "igx_APPID"))
            .set_payload(
                serde_json::to_vec(&json!({
                    "appid": "APPID",
                    "xwhat": "startup",
                    "xcontext": {
                        "installid": "iid-business",
                        "os": "android",
                        "androidid": "androidid-business"
                    }
                }))
                .expect("serialize payload"),
            )
            .insert_header(("content-type", "application/json"))
            .to_request(),
    )
    .await;
    assert_eq!(ingest_resp.status(), StatusCode::OK);

    let metrics_resp = test::call_service(
        &private_app,
        test::TestRequest::get().uri("/metrics").to_request(),
    )
    .await;
    assert_eq!(metrics_resp.status(), StatusCode::OK);
    let metrics =
        String::from_utf8(test::read_body(metrics_resp).await.to_vec()).expect("metrics text");

    assert!(metrics.contains(
        r#"ingest_events_total{project_id="1",result="wal_appended",xwhat="startup"} 1"#
    ));
    assert!(metrics.contains(
        r#"ingest_event_duration_seconds_count{project_id="1",result="wal_appended",xwhat="startup"} 1"#
    ));
}

#[actix_rt::test]
async fn build_app_state_rejects_wal_when_min_free_bytes_is_unavailable() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
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
min_free_bytes = 9223372036854775807

"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );

    let error = match build_app_state_with_test_processor(settings).await {
        Ok(_) => panic!("app state should reject WAL without enough disk space"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("wal disk space is insufficient"));
}
