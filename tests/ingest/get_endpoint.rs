use crate::support::mock_services::build_app_state_with_test_processor;
use actix_http::StatusCode;
use actix_web::{test, App};
use ingest4x::db::init_sqlite_database;
use ingest4x::ingest::processor::ProcessorState;
use ingest4x::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, CreateProjectInput, DeliveryTargetType,
    EventSinkRepository, ProjectRepository,
};
use ingest4x::server;
use ingest4x::settings::{AutoOffsetReset, Settings};
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
async fn get_ingest_maps_query_fields_and_sends_it_to_kafka_sink() {
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

[database]
url = "{}"

[wal]
dir = "{}"

[wal.write]
flush_records = 1
"#,
            sqlite_url(temp.path()),
            wal_dir.display(),
        ),
    )
    .expect("write config");
    seed_kafka_event_sinks(sqlite_url(temp.path()).as_str(), &kafka).await;

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = build_app_state_with_passthrough_processor(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;

    let query = serde_urlencoded::to_string([
        ("appid", "UNRELATED_APPID"),
        ("xwhat", "custom_event"),
        ("installid", "iid-1"),
        ("os", "ios"),
        ("idfa", "idfa-1"),
        ("currencytype", "cny"),
    ])
    .expect("encode query");
    let req = test::TestRequest::get()
        .uri(format!("/ingest/APPID?{query}").as_str())
        .insert_header(("x-ingest-token", "igx_APPID"))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::OK);
    assert_eq!(std::str::from_utf8(body.as_ref()).unwrap(), "200");
    let records = read_all_records(&wal_dir).expect("read wal records");
    assert_eq!(records[0].project_id(), 1);
    let http = records[0].http();
    assert!(!http.headers.contains_key("x-ingest-token"));
    assert!(!http
        .headers
        .values()
        .any(|value| value.contains("igx_APPID")));
    let received_at_ms = records[0].received_at_ms();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        server::replay_wal_once(&app_state)
            .await
            .expect("replay wal"),
        1
    );

    let kafka_string = read_message_payload(&consumer).await;
    let emitted = parse_event_sink_line(kafka_string.as_str());

    assert_eq!(emitted["appid"], json!("UNRELATED_APPID"));
    assert_eq!(emitted["xwhat"], json!("custom_event"));
    assert_eq!(emitted["installid"], json!("iid-1"));
    assert_eq!(emitted["os"], json!("ios"));
    assert_eq!(emitted["idfa"], json!("idfa-1"));
    assert_eq!(emitted["currencytype"], json!("cny"));
    assert!(emitted.get("xcontext").is_none());
    assert!(emitted.get("raw").is_none());
    assert!(received_at_ms > 0);
}

fn parse_event_sink_line(line: &str) -> Value {
    serde_json::from_str(line).expect("event sink line should be valid json")
}

async fn build_app_state_with_passthrough_processor(
    settings: Arc<Settings>,
) -> std::io::Result<server::AppState> {
    let processor = ProcessorState::new(
        r#"
fn process(event, request) {
    emit(SINK_EVENTS, event);
}
"#
        .to_string(),
        10_000,
    )
    .expect("passthrough processor should initialize");

    server::build_app_state_with_processor(settings, processor).await
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

[database]
url = "{}"

[wal]
dir = "{}"

[wal.write]
flush_records = 1
"#,
            sqlite_url(temp.path()),
            wal_dir.display(),
        ),
    )
    .expect("write config");
    seed_kafka_event_sinks(sqlite_url(temp.path()).as_str(), &kafka).await;

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

    let query =
        serde_urlencoded::to_string([("appid", "APPID"), ("xwhat", "custom_event"), ("os", "ios")])
            .expect("encode query");
    let req = test::TestRequest::get()
        .uri(format!("/ingest/APPID?{query}").as_str())
        .insert_header(("x-ingest-token", "igx_APPID"))
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
        .contains("xcontext"));
    emitted["xcontext"]
        .as_object_mut()
        .unwrap()
        .remove("process_info");
    assert_eq!(emitted["appid"], json!("APPID"));
    assert_eq!(emitted["xwhat"], json!("custom_event"));
    assert_eq!(emitted["os"], json!("ios"));
    assert!(emitted["xcontext"].as_object().unwrap().is_empty());
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

[database]
url = "{}"

[wal]
dir = "{}"
"#,
            sqlite_url(temp.path()),
            wal_dir.display(),
        ),
    )
    .expect("write config");
    seed_kafka_event_sinks(sqlite_url(temp.path()).as_str(), &kafka).await;

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

    let query = serde_urlencoded::to_string([
        ("appid", "UNKNOWN"),
        ("xwhat", "custom_event"),
        ("installid", "iid-1"),
        ("os", "ios"),
        ("idfa", "idfa-1"),
    ])
    .expect("encode query");
    let req = test::TestRequest::get()
        .uri(format!("/ingest/APPID?{query}").as_str())
        .insert_header(("x-ingest-token", "igx_missing_token"))
        .to_request();

    let resp = test::call_service(&app, req).await;
    let status_code = resp.status();
    let body = test::read_body(resp).await;

    assert_eq!(status_code, StatusCode::UNAUTHORIZED);
    assert_eq!(
        std::str::from_utf8(body.as_ref()).unwrap(),
        "invalid ingest token"
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

fn sqlite_url(dir: &std::path::Path) -> String {
    format!("sqlite://{}?mode=rwc", dir.join("ingest4x.db").display())
}

async fn seed_kafka_event_sinks(db_url: &str, kafka: &TestKafkaConfig) {
    let db = init_sqlite_database(db_url)
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");

    let repository = EventSinkRepository::new(db);
    let target = repository
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "test_kafka".to_string(),
            name: "Test Kafka".to_string(),
            target_type: DeliveryTargetType::kafka(),
            config_json: json!({
                "bootstrap_servers": &kafka.bootstrap_servers,
                "delivery_timeout_ms": "5000",
                "queue_buffering_max_ms": "0",
                "batch_num_messages": "1",
                "queue_buffering_max_messages": "300",
                "linger_ms": "0"
            }),
            enabled: true,
        })
        .await
        .expect("kafka delivery target should be created");

    for (sink_id, topic) in [
        ("events", kafka.topic.as_str()),
        ("events_error", kafka.error_topic.as_str()),
    ] {
        repository
            .create_event_sink(CreateEventSinkInput {
                sink_id: sink_id.to_string(),
                name: sink_id.to_string(),
                delivery_target_id: target.id,
                destination_json: json!({ "topic": topic }),
                auto_offset_reset: AutoOffsetReset::Latest,
                enabled: true,
            })
            .await
            .expect("event sink should be created");
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
