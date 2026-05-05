use actix_http::StatusCode;
use actix_web::{test, App};
use ingest4x::server;
use ingest4x::settings::Settings;
use ingest4x::utils::prometheus::init_private_prometheus;
use prometheus::Registry;
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

#[actix_rt::test]
async fn public_app_only_registers_ingest_surface() {
    let app_state = build_mock_app_state("").await;
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_public_app(cfg, app_state.clone());
    }))
    .await;

    assert_eq!(
        test::call_service(&app, test::TestRequest::get().uri("/").to_request())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/admin/projects")
                .to_request()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        test::call_service(&app, test::TestRequest::get().uri("/admin").to_request())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        test::call_service(
            &app,
            test::TestRequest::get().uri("/swagger-ui/").to_request()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        test::call_service(&app, test::TestRequest::get().uri("/healthz").to_request())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        test::call_service(&app, test::TestRequest::get().uri("/metrics").to_request())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[actix_rt::test]
async fn private_app_registers_management_surface() {
    let app_state = build_mock_app_state("").await;
    let app = test::init_service(
        App::new()
            .wrap(init_private_prometheus(Registry::new()))
            .configure(|cfg| {
                server::configure_private_app(cfg, app_state.clone());
            }),
    )
    .await;

    assert_eq!(
        test::call_service(&app, test::TestRequest::get().uri("/metrics").to_request())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        test::call_service(&app, test::TestRequest::get().uri("/healthz").to_request())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/admin/projects")
                .to_request()
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_ne!(
        test::call_service(
            &app,
            test::TestRequest::get().uri("/swagger-ui/").to_request()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
}

async fn build_mock_app_state(_mock_projects_toml: &str) -> server::AppState {
    let temp = tempdir().expect("temp dir");
    let config_path = temp.path().join("mock-config.toml");

    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[events.sink.stdout]
type = "stdout"

[[events.valid.routes]]
sinks = ["stdout"]

[[events.invalid.routes]]
sinks = ["stdout"]
"#
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );

    server::build_app_state(settings)
        .await
        .expect("build app state")
}
