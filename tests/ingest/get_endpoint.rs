use crate::support::mock_services::build_app_state_with_test_processor;
use actix_http::StatusCode;
use actix_web::{test, App};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ingest4x::server;
use ingest4x::settings::Settings;
use ingest4x::wal::read_all_records;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::{ClientConfig, Message};
use serde_json::{json, Value};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[actix_rt::test]
async fn get_ingest_decodes_base64_json_and_sends_it_to_kafka_sink() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("mock-config.toml");
    let kafka = create_kafka_config("get-ingest-valid");
    let consumer = create_consumer(&kafka, "get-ingest-main-topic", &kafka.topic);

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

[events.sink.events]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[events.sink.events_error]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"
"#,
            wal_dir.display(),
            kafka.bootstrap_servers,
            kafka.topic,
            kafka.bootstrap_servers,
            kafka.error_topic
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
    let records = read_all_records(&wal_dir).expect("read wal records");
    let received_at_ms = records[0].received_at_ms;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        server::replay_wal_once(&app_state)
            .await
            .expect("replay wal"),
        1
    );

    let kafka_string = read_message_payload(&consumer).await;
    let emitted = parse_event_sink_line(kafka_string.as_str());

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
    assert_eq!(emitted["xwhen"], json!(received_at_ms));
    assert_eq!(
        emitted["xcontext"]["process_info"]["receive_time"],
        json!(received_at_ms)
    );
}

fn parse_event_sink_line(line: &str) -> Value {
    serde_json::from_str(line).expect("event sink line should be valid json")
}

#[actix_rt::test]
async fn get_ingest_default_rhai_processor_uses_existing_validator() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("mock-config.toml");
    let kafka = create_kafka_config("get-ingest-invalid");
    let error_consumer = create_consumer(&kafka, "get-ingest-error-topic", &kafka.error_topic);

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

[events.sink.events]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[events.sink.events_error]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"
"#,
            wal_dir.display(),
            kafka.bootstrap_servers,
            kafka.topic,
            kafka.bootstrap_servers,
            kafka.error_topic
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

    assert_eq!(status_code, StatusCode::OK);
    assert_eq!(
        server::replay_wal_once(&app_state)
            .await
            .expect("replay wal"),
        1
    );

    let kafka_string = read_message_payload(&error_consumer).await;
    let mut emitted = parse_event_sink_line(kafka_string.as_str());
    assert!(emitted["xcontext"]["process_info"]["reason"]
        .as_str()
        .unwrap()
        .contains("xcontext.installid"));
    emitted["xcontext"]
        .as_object_mut()
        .unwrap()
        .remove("process_info");
    assert_eq!(emitted, invalid_payload);
}

#[actix_rt::test]
async fn get_ingest_returns_not_found_for_unknown_project_via_real_server_wiring() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("mock-config.toml");
    let kafka = create_kafka_config("get-ingest-unknown-project");

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
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[events.sink.events_error]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"
"#,
            wal_dir.display(),
            kafka.bootstrap_servers,
            kafka.topic,
            kafka.bootstrap_servers,
            kafka.error_topic
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
}

struct TestKafkaConfig {
    bootstrap_servers: String,
    topic: String,
    error_topic: String,
    _kafka_cluster: MockCluster<'static, DefaultProducerContext>,
}

fn create_kafka_config(prefix: &str) -> TestKafkaConfig {
    let topic = format!("{prefix}-events");
    let error_topic = format!("{prefix}-events-error");
    let kafka_cluster = MockCluster::new(3).expect("create kafka mock cluster");
    kafka_cluster
        .create_topic(topic.as_str(), 1, 1)
        .expect("create kafka mock topic");
    kafka_cluster
        .create_topic(error_topic.as_str(), 1, 1)
        .expect("create kafka mock error topic");

    TestKafkaConfig {
        bootstrap_servers: kafka_cluster.bootstrap_servers(),
        topic,
        error_topic,
        _kafka_cluster: kafka_cluster,
    }
}

fn create_consumer(kafka: &TestKafkaConfig, group_id: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka.bootstrap_servers)
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
