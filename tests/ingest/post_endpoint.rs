use crate::support::mock_services::{
    create_app, create_app_with_processor_script, create_app_with_project, replay_once, TestService,
};
use actix_http::StatusCode;
use actix_web::test;
use assert_json_diff::assert_json_eq;
use ingest4x::event::Event;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use serde_json::{json, Value};
use std::net::SocketAddr;

#[actix_rt::test]
async fn post_ingest_normalizes_and_sends_event() {
    let (app, testservice) = create_app().await;
    let consumer = create_consumer(&testservice, "ingest-post-main-topic", &testservice.topic);

    let req = test::TestRequest::post()
        .peer_addr("8.8.8.8:8080".parse::<SocketAddr>().unwrap())
        .uri("/ingest/APPID")
        .insert_header(("x-ingest-token", "igx_test_token"))
        .set_json(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-1",
                "os": "iOS",
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
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&consumer).await;
    let mut event_from_kafka =
        Event::from_json(&serde_json::from_str(kafka_string.as_str()).unwrap()).unwrap();
    let xwhen = event_from_kafka.xwhen().unwrap();
    let process_info = event_from_kafka.xcontext_mut().remove("process_info");
    assert!(process_info.is_some());

    assert_json_eq!(
        event_from_kafka.into_value().unwrap(),
        json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xwho": Value::Null,
            "xwhen": xwhen,
            "xcontext": {
                "installid": "iid-1",
                "os": "ios",
                "idfa": "idfa-1",
                "currencytype": "CNY",
                "platform": "ios",
                "ip": "8.8.8.8"
            }
        })
    );
}

#[actix_rt::test]
async fn post_ingest_path_project_key_uses_project_token_auth() {
    let (app, testservice) = create_app().await;
    let consumer = create_consumer(
        &testservice,
        "ingest-post-path-token-topic",
        &testservice.topic,
    );

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .insert_header(("x-ingest-token", "igx_test_token"))
        .set_json(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-path-token",
                "os": "ios",
                "idfa": "idfa-path-token"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&consumer).await;
    let emitted: Value = serde_json::from_str(kafka_string.as_str()).expect("event json");
    assert_eq!(emitted["xcontext"]["installid"], json!("iid-path-token"));
}

#[actix_rt::test]
async fn post_ingest_without_project_key_path_is_not_registered() {
    let (app, _testservice) = create_app().await;

    let req = test::TestRequest::post()
        .uri("/ingest")
        .insert_header(("x-ingest-token", "igx_test_token"))
        .set_json(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-no-project-key-path",
                "os": "ios",
                "idfa": "idfa-no-project-key-path"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[actix_rt::test]
async fn post_ingest_public_mode_can_use_ip_allowlist_without_token() {
    let (app, testservice) = create_app_with_project(std::collections::HashMap::from([
        ("name".to_string(), "ip-app".to_string()),
        ("auth_mode".to_string(), "public".to_string()),
        ("allowed_ips".to_string(), "8.8.8.8".to_string()),
    ]))
    .await;
    let consumer = create_consumer(
        &testservice,
        "ingest-post-path-public-ip-topic",
        &testservice.topic,
    );

    let req = test::TestRequest::post()
        .peer_addr("8.8.8.8:8080".parse::<SocketAddr>().unwrap())
        .uri("/ingest/ip-app")
        .set_json(json!({
            "appid": "PUBLICIP",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-public-ip",
                "os": "ios",
                "idfa": "idfa-public-ip"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&consumer).await;
    let emitted: Value = serde_json::from_str(kafka_string.as_str()).expect("event json");
    assert_eq!(emitted["xcontext"]["installid"], json!("iid-public-ip"));
}

#[actix_rt::test]
async fn post_ingest_no_longer_accepts_authorization_bearer_token() {
    let (app, _testservice) = create_app().await;

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .insert_header(("authorization", "Bearer igx_test_token"))
        .set_json(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-bearer",
                "os": "ios",
                "idfa": "idfa-bearer"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::UNAUTHORIZED);
    assert_eq!(
        std::str::from_utf8(body.as_ref()).unwrap(),
        "missing ingest token"
    );
}

#[actix_rt::test]
async fn post_ingest_accepts_json_body_token_and_removes_it_from_event() {
    let script = r#"
fn process(event, request) {
    emit(SINK_EVENTS, event);
}
"#;
    let (app, testservice) = create_app_with_processor_script(script).await;
    let consumer = create_consumer(
        &testservice,
        "ingest-post-json-body-token-topic",
        &testservice.topic,
    );

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .set_json(json!({
            "x-ingest-token": "igx_test_token",
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-body-token",
                "os": "ios",
                "idfa": "idfa-body-token"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&consumer).await;
    let emitted: Value = serde_json::from_str(kafka_string.as_str()).expect("event json");
    assert_eq!(emitted["xcontext"]["installid"], json!("iid-body-token"));
    assert!(emitted.get("x-ingest-token").is_none());
}

#[actix_rt::test]
async fn post_ingest_accepts_form_token_and_maps_form_fields_to_flat_event() {
    let script = r#"
fn process(event, request) {
    emit(SINK_EVENTS, event);
}
"#;
    let (app, testservice) = create_app_with_processor_script(script).await;
    let consumer = create_consumer(
        &testservice,
        "ingest-post-form-token-topic",
        &testservice.topic,
    );
    let form_body = serde_urlencoded::to_string([
        ("x-ingest-token", "igx_test_token"),
        ("appid", "adjust"),
        ("xwhat", "install"),
        ("event_name", "signup"),
        ("adid", "adjust-adid-1"),
        ("created_at", "2026-05-14T10:00:00Z"),
    ])
    .expect("encode form");

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .insert_header(("content-type", "application/x-www-form-urlencoded"))
        .set_payload(form_body)
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&consumer).await;
    let emitted: Value = serde_json::from_str(kafka_string.as_str()).expect("event json");
    assert_eq!(emitted["appid"], json!("adjust"));
    assert_eq!(emitted["xwhat"], json!("install"));
    assert_eq!(emitted["event_name"], json!("signup"));
    assert_eq!(emitted["adid"], json!("adjust-adid-1"));
    assert_eq!(emitted["created_at"], json!("2026-05-14T10:00:00Z"));
    assert!(emitted.get("xcontext").is_none());
    assert!(emitted.get("raw").is_none());
    assert!(emitted.get("x-ingest-token").is_none());
}

#[actix_rt::test]
async fn post_ingest_rejects_conflicting_header_and_body_tokens() {
    let (app, _testservice) = create_app().await;

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .insert_header(("x-ingest-token", "igx_test_token"))
        .set_json(json!({
            "x-ingest-token": "igx_other_token",
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-conflict",
                "os": "ios",
                "idfa": "idfa-conflict"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::UNAUTHORIZED);
    assert_eq!(
        std::str::from_utf8(body.as_ref()).unwrap(),
        "conflicting ingest token"
    );
}

#[actix_rt::test]
async fn get_ingest_accepts_query_fields_as_event_payload() {
    let script = r#"
fn process(event, request) {
    emit(SINK_EVENTS, event);
}
"#;
    let (app, testservice) = create_app_with_processor_script(script).await;
    let consumer = create_consumer(
        &testservice,
        "ingest-get-query-fields-topic",
        &testservice.topic,
    );
    let query = serde_urlencoded::to_string([
        ("appid", "APPID"),
        ("xwhat", "custom_event"),
        ("installid", "iid-query-fields"),
        ("os", "ios"),
        ("idfa", "idfa-query-fields"),
    ])
    .expect("encode query");

    let req = test::TestRequest::get()
        .uri(format!("/ingest/APPID?{query}").as_str())
        .insert_header(("x-ingest-token", "igx_test_token"))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&consumer).await;
    let emitted: Value = serde_json::from_str(kafka_string.as_str()).expect("event json");
    assert_eq!(emitted["appid"], json!("APPID"));
    assert_eq!(emitted["xwhat"], json!("custom_event"));
    assert_eq!(emitted["installid"], json!("iid-query-fields"));
    assert_eq!(emitted["os"], json!("ios"));
    assert_eq!(emitted["idfa"], json!("idfa-query-fields"));
    assert!(emitted.get("xcontext").is_none());
    assert!(emitted.get("raw").is_none());
}

#[actix_rt::test]
async fn get_ingest_treats_data_as_a_normal_query_field() {
    let script = r#"
fn process(event, request) {
    emit(SINK_EVENTS, event);
}
"#;
    let (app, testservice) = create_app_with_processor_script(script).await;
    let consumer = create_consumer(
        &testservice,
        "ingest-get-query-data-field-topic",
        &testservice.topic,
    );
    let query = serde_urlencoded::to_string([
        ("appid", "APPID"),
        ("xwhat", "custom_event"),
        ("installid", "iid-query-data"),
        ("os", "ios"),
        ("idfa", "idfa-query-data"),
        ("data", "not-base64-json"),
    ])
    .expect("encode query");

    let req = test::TestRequest::get()
        .uri(format!("/ingest/APPID?{query}").as_str())
        .insert_header(("x-ingest-token", "igx_test_token"))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&consumer).await;
    let emitted: Value = serde_json::from_str(kafka_string.as_str()).expect("event json");
    assert_eq!(emitted["installid"], json!("iid-query-data"));
    assert_eq!(emitted["data"], json!("not-base64-json"));
    assert!(emitted.get("xcontext").is_none());
    assert!(emitted.get("raw").is_none());
}

#[actix_rt::test]
async fn get_ingest_rejects_query_token() {
    let (app, _testservice) = create_app().await;
    let query = serde_urlencoded::to_string([
        ("appid", "APPID"),
        ("xwhat", "custom_event"),
        ("x-ingest-token", "igx_test_token"),
    ])
    .expect("encode query");

    let req = test::TestRequest::get()
        .uri(format!("/ingest/APPID?{query}").as_str())
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::BAD_REQUEST);
    assert_eq!(
        std::str::from_utf8(body.as_ref()).unwrap(),
        "query ingest token is not supported"
    );
}

#[actix_rt::test]
async fn post_ingest_accepts_payload_without_appid() {
    let script = r#"
fn process(event, request) {
    emit(SINK_EVENTS, event);
}
"#;
    let (app, testservice) = create_app_with_processor_script(script).await;
    let consumer = create_consumer(
        &testservice,
        "ingest-post-without-appid-topic",
        &testservice.topic,
    );
    let input_payload = json!({
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-no-appid",
            "os": "ios",
            "idfa": "idfa-no-appid"
        }
    });

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .insert_header(("x-ingest-token", "igx_test_token"))
        .set_json(input_payload.clone())
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&consumer).await;
    let emitted: Value = serde_json::from_str(kafka_string.as_str()).expect("event json");
    assert_json_eq!(emitted, input_payload);
}

#[actix_rt::test]
async fn post_ingest_sends_invalid_payload_to_error_topic() {
    let (app, testservice) = create_app().await;
    let error_consumer = create_consumer(
        &testservice,
        "ingest-post-error-topic",
        &testservice.error_topic,
    );

    let invalid_payload = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "os": "ios"
        }
    });

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .insert_header(("x-ingest-token", "igx_test_token"))
        .set_json(invalid_payload.clone())
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();

    assert_eq!(status_code, StatusCode::OK);
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&error_consumer).await;
    let mut emitted = serde_json::from_str::<Value>(kafka_string.as_str()).unwrap();
    assert!(emitted["xcontext"]["process_info"]["reason"]
        .as_str()
        .unwrap()
        .contains("xcontext.installid"));
    assert_eq!(
        emitted["xcontext"]["process_info"]["error_code"],
        emitted["xcontext"]["process_info"]["reason"]
    );
    emitted["xcontext"]
        .as_object_mut()
        .unwrap()
        .remove("process_info");
    assert_json_eq!(emitted, invalid_payload);
}

#[actix_rt::test]
async fn post_ingest_replays_wal_through_rhai_processor_before_kafka_sink() {
    let script = r#"
fn process(event, ctx) {
    try {
        event.required("appid").string().min(1);
        event.required("xwhat").string().min(1);
        event.required("xcontext").object();
        event.required("xcontext.installid").string().min(1);
        event.required("xcontext.os").string().min(1);
    } catch (err) {
        emit(SINK_EVENTS_ERROR, event);
        return;
    }
    event["xcontext"]["processor_marker"] = "rhai";
    emit(SINK_EVENTS, event);
}
"#;
    let (app, testservice) = create_app_with_processor_script(script).await;
    let consumer = create_consumer(
        &testservice,
        "ingest-post-processor-main-topic",
        &testservice.topic,
    );

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .insert_header(("x-ingest-token", "igx_test_token"))
        .set_json(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-1",
                "os": "ios",
                "idfa": "idfa-1"
            }
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(replay_once(&testservice).await.expect("replay wal once"), 1);

    let kafka_string = read_message_payload(&consumer).await;
    let emitted: Value = serde_json::from_str(kafka_string.as_str()).expect("event json");

    assert_eq!(emitted["xcontext"]["processor_marker"], json!("rhai"));
}

#[actix_rt::test]
async fn post_ingest_returns_not_found_when_project_is_missing() {
    let (app, _testservice) = create_app_with_project(Default::default()).await;

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .insert_header(("x-ingest-token", "igx_test_token"))
        .set_json(json!({
            "appid": "APPID",
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
        "project not found"
    );
}

#[actix_rt::test]
async fn post_ingest_requires_ingest_token() {
    let (app, _testservice) = create_app().await;

    let req = test::TestRequest::post()
        .uri("/ingest/APPID")
        .set_json(json!({
            "appid": "APPID",
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

    assert_eq!(status_code, StatusCode::UNAUTHORIZED);
    assert_eq!(
        std::str::from_utf8(body.as_ref()).unwrap(),
        "missing ingest token"
    );
}

#[actix_rt::test]
async fn post_ingest_with_querystring_but_empty_body_returns_standard_error() {
    let (app, _testservice) = create_app().await;

    let req = test::TestRequest::post()
        .uri("/ingest/APPID?appid=APPID&xwhat=install&installid=iid-empty-body")
        .insert_header(("x-ingest-token", "igx_test_token"))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::BAD_REQUEST);
    assert_eq!(
        std::str::from_utf8(body.as_ref()).unwrap(),
        "missing request body"
    );
}

fn create_consumer(testservice: &TestService, group_id: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &testservice.bootstrap_servers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "2000")
        .create()
        .expect("consumer creation error");
    consumer.subscribe(&[topic]).expect("subscribe topic");
    consumer
}

async fn read_message_payload(consumer: &StreamConsumer) -> String {
    let message = consumer.recv().await.expect("read kafka message");
    std::str::from_utf8(message.payload().expect("payload"))
        .expect("utf8 payload")
        .to_string()
}
