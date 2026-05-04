#![cfg(feature = "ingest")]

use actix_http::StatusCode;
use actix_web::{test, App};
use ingest4x::db::init_sqlite_database;
use ingest4x::ingest::processor::ProcessorState;
use ingest4x::projects::{CreateProjectInput, ProjectRegistryState, ProjectRepository};
use ingest4x::rules::RuleRepository;
use ingest4x::server;
use ingest4x::settings::{
    EventRouteSet, EventRouteSettings, EventSinkConfig, EventsSettings, Settings,
};
use ingest4x::utils::events::init_event_sinks;
use ingest4x::wal::{new_record, WalRecord, WalWriter};
use ingest4x::wal_replay::{replay_once, WalReplayContext};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::{ClientConfig, Message};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

#[actix_rt::test]
async fn wal_replay_sends_records_to_kafka_and_advances_checkpoint() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-replay.toml");
    let kafka = create_kafka_config("wal-replay-valid");
    let consumer = create_consumer(&kafka, "wal-replay-main-topic", &kafka.topic);

    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[[events.valid.routes]]
sinks = ["kafka_valid"]
ack = ["kafka_valid"]

[events.sink.kafka_invalid]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[[events.invalid.routes]]
sinks = ["kafka_invalid"]
ack = ["kafka_invalid"]
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
            "installid": "iid-wal-replay",
            "os": "ios",
            "idfa": "idfa-1",
            "currencytype": "cny"
        }
    });

    let req = test::TestRequest::post()
        .uri("/ingest")
        .set_payload(serde_json::to_vec(&input_payload).expect("serialize payload"))
        .insert_header(("content-type", "application/json"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    assert_eq!(
        server::replay_wal_once(&app_state)
            .await
            .expect("replay wal"),
        1
    );
    assert_eq!(
        server::replay_wal_once(&app_state)
            .await
            .expect("replay wal again"),
        0
    );

    let emitted = parse_json_message(read_message_payload(&consumer).await.as_str());
    assert_eq!(emitted["appid"], input_payload["appid"]);
    assert_eq!(emitted["xwhat"], input_payload["xwhat"]);
    assert_eq!(emitted["xcontext"]["installid"], json!("iid-wal-replay"));
    assert_eq!(emitted["xcontext"]["currencytype"], json!("CNY"));
    assert!(wal_dir.join("checkpoint.json").exists());
    let checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoint.json")).expect("read checkpoint"),
    )
    .expect("checkpoint json");
    assert_eq!(checkpoint["checkpoint_lsn"], json!(1));
    assert_eq!(checkpoint["checkpoint_segment_id"], json!(1));
    assert!(checkpoint["checkpoint_segment_offset"].is_number());
}

#[actix_rt::test]
async fn wal_replay_routes_valid_event_by_original_wal_keys() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let kafka = create_kafka_config("wal-replay-original-route");
    let mutated_topic = format!("{}-mutated", kafka.topic);
    kafka
        ._kafka_cluster
        .create_topic(mutated_topic.as_str(), 1, 1)
        .expect("create mutated topic");
    let original_consumer =
        create_consumer(&kafka, "wal-replay-original-route-topic", &kafka.topic);
    let mutated_consumer =
        create_consumer(&kafka, "wal-replay-mutated-route-topic", &mutated_topic);
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    project_repository
        .create_project(CreateProjectInput {
            appid: "APPID".to_string(),
            name: "APPID".to_string(),
            enabled: true,
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([
            (
                "kafka_original".to_string(),
                EventSinkConfig::Kafka {
                    bootstrap_servers: kafka.bootstrap_servers.clone(),
                    topic: kafka.topic.clone(),
                    delivery_timeout_ms: "5000".to_string(),
                    queue_buffering_max_ms: "0".to_string(),
                    batch_num_messages: "1".to_string(),
                    queue_buffering_max_messages: "300".to_string(),
                    linger_ms: "0".to_string(),
                },
            ),
            (
                "kafka_mutated".to_string(),
                EventSinkConfig::Kafka {
                    bootstrap_servers: kafka.bootstrap_servers.clone(),
                    topic: mutated_topic,
                    delivery_timeout_ms: "5000".to_string(),
                    queue_buffering_max_ms: "0".to_string(),
                    batch_num_messages: "1".to_string(),
                    queue_buffering_max_messages: "300".to_string(),
                    linger_ms: "0".to_string(),
                },
            ),
        ]),
        valid: EventRouteSet {
            routes: vec![
                EventRouteSettings {
                    xwhat: Some(vec!["custom_event".to_string()]),
                    sinks: vec!["kafka_original".to_string()],
                    ack: vec!["kafka_original".to_string()],
                    ..Default::default()
                },
                EventRouteSettings {
                    xwhat: Some(vec!["mutated_event".to_string()]),
                    sinks: vec!["kafka_mutated".to_string()],
                    ack: vec!["kafka_mutated".to_string()],
                    ..Default::default()
                },
            ],
        },
        invalid: EventRouteSet::default(),
    })
    .expect("event sinks should initialize");
    let processor = ProcessorState::new(
        r#"
            fn main(event, request) {
                event["xwhat"] = "mutated_event";
                return #{ status: "accepted", event: event };
            }
        "#
        .to_string(),
        10_000,
    )
    .expect("processor should initialize");
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        wal_flush_interval: "1s".to_string(),
        wal_max_write_buffer_size: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
    })
    .expect("wal writer");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-route",
                "os": "ios"
            }
        })))
        .expect("append record");
    drop(writer);

    assert_eq!(
        replay_once(WalReplayContext {
            dir: &wal_dir,
            event_sinks: &event_sinks,
            project_registry: &project_registry,
            rule_repository: &rule_repository,
            processor: &processor,
        })
        .await
        .expect("replay wal"),
        1
    );

    let emitted = parse_json_message(
        read_message_payload_with_timeout(&original_consumer)
            .await
            .as_str(),
    );
    assert_eq!(emitted["xwhat"], json!("mutated_event"));
    assert!(read_message_payload_with_short_timeout(&mutated_consumer)
        .await
        .is_none());
}

#[actix_rt::test]
async fn wal_replay_skips_invalid_json_record_and_continues() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-replay-invalid-json.toml");
    let kafka = create_kafka_config("wal-replay-invalid-json");
    let consumer = create_consumer(&kafka, "wal-replay-invalid-json-topic", &kafka.topic);

    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[[events.valid.routes]]
sinks = ["kafka_valid"]
ack = ["kafka_valid"]

[events.sink.kafka_invalid]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[[events.invalid.routes]]
sinks = ["kafka_invalid"]
ack = ["kafka_invalid"]
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
    let writer = WalWriter::new(settings.wal.as_ref().expect("wal settings")).expect("wal writer");
    writer
        .append(&new_record(
            "POST",
            "/ingest",
            None,
            None,
            BTreeMap::new(),
            b"{not-json".to_vec(),
        ))
        .expect("append invalid json record");
    writer
        .append(&new_record(
            "POST",
            "/ingest",
            None,
            None,
            BTreeMap::new(),
            serde_json::to_vec(&json!({
                "appid": "APPID",
                "xwhat": "custom_event",
                "xcontext": {
                    "installid": "iid-after-invalid-json",
                    "os": "ios",
                    "idfa": "idfa-after-invalid-json"
                }
            }))
            .expect("serialize payload"),
        ))
        .expect("append valid record");
    drop(writer);

    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");

    assert_eq!(
        server::replay_wal_once(&app_state)
            .await
            .expect("replay wal"),
        2
    );

    let emitted = parse_json_message(read_message_payload(&consumer).await.as_str());
    assert_eq!(
        emitted["xcontext"]["installid"],
        json!("iid-after-invalid-json")
    );
}

#[actix_rt::test]
async fn wal_replay_does_not_checkpoint_processor_drop_without_downstream_write() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    project_repository
        .create_project(CreateProjectInput {
            appid: "APPID".to_string(),
            name: "APPID".to_string(),
            enabled: true,
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([("stdout".to_string(), EventSinkConfig::Stdout)]),
        valid: EventRouteSet {
            routes: vec![EventRouteSettings {
                sinks: vec!["stdout".to_string()],
                ack: vec!["stdout".to_string()],
                ..Default::default()
            }],
        },
        invalid: EventRouteSet::default(),
    })
    .expect("event sinks should initialize");
    let processor = ProcessorState::new(
        r#"
            fn main(event, request) {
                return #{
                    status: "dropped",
                    reason: "not a durable downstream decision"
                };
            }
        "#
        .to_string(),
        10_000,
    )
    .expect("processor should initialize");
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        wal_flush_interval: "1s".to_string(),
        wal_max_write_buffer_size: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
    })
    .expect("wal writer");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-drop",
                "os": "ios"
            }
        })))
        .expect("append record");
    drop(writer);

    let error = replay_once(WalReplayContext {
        dir: &wal_dir,
        event_sinks: &event_sinks,
        project_registry: &project_registry,
        rule_repository: &rule_repository,
        processor: &processor,
    })
    .await
    .expect_err("processor drop should stop WAL replay");

    assert!(error
        .to_string()
        .contains("unsupported processor status `dropped`"));
    assert!(!wal_dir.join("checkpoint.json").exists());
}

#[actix_rt::test]
async fn wal_replay_removes_segments_before_checkpoint() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-replay-cleanup.toml");
    let kafka = create_kafka_config("wal-replay-cleanup");

    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"
wal_segment_max_bytes = 16

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[[events.valid.routes]]
sinks = ["kafka_valid"]
ack = ["kafka_valid"]

[events.sink.kafka_invalid]
type = "kafka"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[[events.invalid.routes]]
sinks = ["kafka_invalid"]
ack = ["kafka_invalid"]
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
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;

    for installid in ["iid-clean-1", "iid-clean-2"] {
        let req = test::TestRequest::post()
            .uri("/ingest")
            .set_payload(
                serde_json::to_vec(&json!({
                    "appid": "APPID",
                    "xwhat": "custom_event",
                    "xcontext": {
                        "installid": installid,
                        "os": "ios",
                        "idfa": format!("idfa-{installid}")
                    }
                }))
                .expect("serialize payload"),
            )
            .insert_header(("content-type", "application/json"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
    assert!(wal_dir.join("00000000000000000001.wal").exists());
    assert!(wal_dir.join("00000000000000000002.wal").exists());

    assert_eq!(
        server::replay_wal_once(&app_state)
            .await
            .expect("replay wal"),
        2
    );

    assert!(!wal_dir.join("00000000000000000001.wal").exists());
    assert!(wal_dir.join("00000000000000000002.wal").exists());
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

fn parse_json_message(line: &str) -> Value {
    serde_json::from_str(line).expect("event sink message should be valid json")
}

fn test_wal_record(payload: Value) -> WalRecord {
    new_record(
        "POST",
        "/ingest",
        None,
        None,
        BTreeMap::new(),
        serde_json::to_vec(&payload).expect("serialize payload"),
    )
}

async fn read_message_payload_with_timeout(consumer: &StreamConsumer) -> String {
    actix_rt::time::timeout(
        std::time::Duration::from_secs(5),
        read_message_payload(consumer),
    )
    .await
    .expect("read kafka message before timeout")
}

async fn read_message_payload_with_short_timeout(consumer: &StreamConsumer) -> Option<String> {
    actix_rt::time::timeout(
        std::time::Duration::from_millis(300),
        read_message_payload(consumer),
    )
    .await
    .ok()
}
