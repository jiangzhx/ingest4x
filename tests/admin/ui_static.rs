use actix_http::StatusCode;
use actix_web::{test, App};
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[actix_rt::test]
async fn admin_ui_serves_index_html_for_root_and_spa_routes() {
    let temp = tempdir().expect("temp dir");
    prepare_admin_ui_fixture(temp.path());

    let app = test::init_service(
        App::new().configure(|cfg| ingest4x::admin_ui::configure_with_dist_dir(cfg, temp.path())),
    )
    .await;

    for uri in [
        "/admin",
        "/admin/login",
        "/admin/projects",
        "/admin/projects/alpha",
        "/admin/rules",
        "/admin/project-rules",
        "/admin/unknown/path",
    ] {
        let response =
            test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;
        let status = response.status();
        let body = test::read_body(response).await;
        let content = std::str::from_utf8(body.as_ref()).expect("utf8 body");

        assert_eq!(status, StatusCode::OK, "unexpected status for {uri}");
        assert!(
            content.contains("admin ui"),
            "unexpected body for {uri}: {content}"
        );
    }
}

#[actix_rt::test]
async fn admin_ui_serves_existing_static_assets() {
    let temp = tempdir().expect("temp dir");
    prepare_admin_ui_fixture(temp.path());

    let app = test::init_service(
        App::new().configure(|cfg| ingest4x::admin_ui::configure_with_dist_dir(cfg, temp.path())),
    )
    .await;

    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/admin/assets/main.js")
            .to_request(),
    )
    .await;
    let status = response.status();
    let body = test::read_body(response).await;
    let content = std::str::from_utf8(body.as_ref()).expect("utf8 body");

    assert_eq!(status, StatusCode::OK);
    assert_eq!(content, "console.log('admin asset');");
}

#[actix_rt::test]
async fn admin_ui_returns_not_found_for_missing_static_assets() {
    let temp = tempdir().expect("temp dir");
    prepare_admin_ui_fixture(temp.path());

    let app = test::init_service(
        App::new().configure(|cfg| ingest4x::admin_ui::configure_with_dist_dir(cfg, temp.path())),
    )
    .await;

    for uri in ["/admin/assets/missing.js", "/admin/favicon.ico"] {
        let response =
            test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "unexpected status for {uri}"
        );
    }
}

#[actix_rt::test]
async fn admin_ui_returns_not_found_when_dist_assets_are_missing() {
    let temp = tempdir().expect("temp dir");

    let app = test::init_service(
        App::new().configure(|cfg| ingest4x::admin_ui::configure_with_dist_dir(cfg, temp.path())),
    )
    .await;

    for uri in ["/admin", "/admin/projects"] {
        let response =
            test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "unexpected status for {uri}"
        );
    }
}

fn prepare_admin_ui_fixture(dist_dir: &Path) {
    let assets_dir = dist_dir.join("assets");
    fs::create_dir_all(&assets_dir).expect("assets dir");
    fs::write(
        dist_dir.join("index.html"),
        "<html><body>admin ui</body></html>",
    )
    .expect("index.html");
    fs::write(assets_dir.join("main.js"), "console.log('admin asset');").expect("main.js");
}
