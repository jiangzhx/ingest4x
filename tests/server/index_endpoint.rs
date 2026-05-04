use actix_web::http::header::ContentType;
use actix_web::{test, web, App};
use ingest4x::server::index;
#[actix_web::test]
async fn test_index_get() {
    let app = test::init_service(App::new().route("/", web::get().to(index))).await;
    let req = test::TestRequest::default()
        .insert_header(ContentType::plaintext())
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}
