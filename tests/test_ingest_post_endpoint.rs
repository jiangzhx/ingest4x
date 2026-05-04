#![cfg(feature = "ingest")]

mod mock_services;

use crate::mock_services::{
    create_app, create_app_with_processor_script, create_app_with_project, TestService,
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
        .uri("/ingest")
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
        .uri("/ingest")
        .set_json(invalid_payload.clone())
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;
    let body_text = std::str::from_utf8(body.as_ref()).unwrap();

    assert_eq!(status_code, StatusCode::BAD_REQUEST);
    assert!(body_text.contains("xcontext.installid"));

    let kafka_string = read_message_payload(&error_consumer).await;
    let mut emitted = serde_json::from_str::<Value>(kafka_string.as_str()).unwrap();
    assert!(emitted["xcontext"]["process_info"]["reason"]
        .as_str()
        .unwrap()
        .contains("xcontext.installid"));
    emitted["xcontext"]
        .as_object_mut()
        .unwrap()
        .remove("process_info");
    assert_json_eq!(emitted, invalid_payload);
}

#[actix_rt::test]
async fn post_ingest_runs_rhai_processor_before_kafka_sink() {
    let script = r#"
fn main(event, ctx) {
    let validation = validate(event);
    if !validation["ok"] {
        return reject(event, validation["error"]);
    }

    event["xcontext"]["processor_marker"] = "rhai";
    return accept(event);
}
"#;
    let (app, testservice) = create_app_with_processor_script(script).await;
    let consumer = create_consumer(
        &testservice,
        "ingest-post-processor-main-topic",
        &testservice.topic,
    );

    let req = test::TestRequest::post()
        .uri("/ingest")
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

    let kafka_string = read_message_payload(&consumer).await;
    let emitted: Value = serde_json::from_str(kafka_string.as_str()).expect("event json");

    assert_eq!(emitted["xcontext"]["processor_marker"], json!("rhai"));
}

#[actix_rt::test]
async fn post_ingest_returns_not_found_when_project_is_missing() {
    let (app, _testservice) = create_app_with_project(Default::default()).await;

    let req = test::TestRequest::post()
        .uri("/ingest")
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
        "Project not found"
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
