use crate::support::mock_services::build_app_state_with_test_processor;
use actix_http::StatusCode;
use actix_web::{test, App};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ingest4x::db::init_sqlite_database;
use ingest4x::ingest::processor::ProcessorState;
use ingest4x::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, CreateProcessorScriptInput,
    CreateProcessorScriptModuleInput, CreateProjectInput, DeliveryTargetType, EventSinkRepository,
    ProcessorRepository, ProcessorScriptStatus, ProjectRepository, RuleRepository,
};
use ingest4x::server;
use ingest4x::services::ProjectRegistryState;
use ingest4x::settings::{
    AutoOffsetReset, CheckpointSettings, EventSinkConfig, EventsSettings, Settings,
};
use ingest4x::utils::events::init_event_sinks;
use ingest4x::wal::replay::{initialize_sink_checkpoints, replay_once, WalReplayContext};
use ingest4x::wal::{new_record, read_entries_after_limit, WalRecord, WalWriter};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::{ClientConfig, Message};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

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
[ingest]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.events]
type = "kafka"
auto_offset_reset = "earliest"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[events.sink.events_error]
type = "kafka"
auto_offset_reset = "earliest"
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
            "installid": "iid-wal-replay",
            "os": "ios",
            "idfa": "idfa-1",
            "currencytype": "cny"
        }
    });

    let req = test::TestRequest::post()
        .uri("/ingest")
        .insert_header(("x-ingest-token", "igx_APPID"))
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
    assert!(!wal_dir.join("checkpoint.json").exists());
    let checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/events.json")).expect("read sink checkpoint"),
    )
    .expect("sink checkpoint json");
    assert_eq!(checkpoint["checkpoint_lsn"], json!(1));
    assert_eq!(checkpoint["checkpoint_segment_id"], json!(1));
    assert!(checkpoint["checkpoint_segment_offset"].is_number());
    assert_eq!(
        checkpoint["node_id"],
        json!(fs::read_to_string(wal_dir.join("node_id"))
            .expect("read node id")
            .trim())
    );
    assert!(checkpoint["updated_at"].is_number());
    assert!(checkpoint["checksum"].as_u64().unwrap_or(0) > 0);
}

#[actix_rt::test]
async fn wal_replay_uses_project_bound_database_processor_script() {
    let temp = tempdir().expect("temp dir");
    let db_path = temp.path().join("ingest4x.db");
    let db_url = format!("sqlite://{}?mode=rwc", db_path.display());
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-replay-db-processor.toml");
    let kafka = create_kafka_config("wal-replay-db-processor");
    let consumer = create_consumer(&kafka, "wal-replay-db-processor-topic", &kafka.topic);

    let db = init_sqlite_database(&db_url)
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let event_sink_repository = EventSinkRepository::new(db.clone());
    let target = event_sink_repository
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "events_target".to_string(),
            name: "events target".to_string(),
            target_type: DeliveryTargetType::Kafka,
            config_json: json!({
                "bootstrap_servers": kafka.bootstrap_servers,
                "delivery_timeout_ms": "5000",
                "queue_buffering_max_ms": "0",
                "batch_num_messages": "1",
                "queue_buffering_max_messages": "300",
                "linger_ms": "0"
            }),
            enabled: true,
        })
        .await
        .expect("events target should be created");
    event_sink_repository
        .create_event_sink(CreateEventSinkInput {
            sink_id: "events".to_string(),
            name: "events".to_string(),
            delivery_target_id: target.id,
            destination_json: json!({ "topic": kafka.topic }),
            auto_offset_reset: AutoOffsetReset::Earliest,
            enabled: true,
        })
        .await
        .expect("events sink should be created");
    let processor_repository = ProcessorRepository::new(db);
    let project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let script = processor_repository
        .create_script(CreateProcessorScriptInput {
            script_key: "project_pipeline".to_string(),
            name: "Project pipeline".to_string(),
            entry_module: "main".to_string(),
            status: ProcessorScriptStatus::Active,
            modules: vec![CreateProcessorScriptModuleInput {
                module_name: "main".to_string(),
                source: r#"
fn process(event, request) {
    event["xcontext"]["processor_marker"] = "project-db";
    emit(SINK_EVENTS, event);
}
"#
                .to_string(),
            }],
        })
        .await
        .expect("processor script should be created");
    processor_repository
        .assign_project_processor(project.id, script.id, true)
        .await
        .expect("project processor should be assigned");

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

[events.sink.events]
type = "kafka"
auto_offset_reset = "earliest"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[events.sink.events_error]
type = "kafka"
auto_offset_reset = "earliest"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"
"#,
            db_url,
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
    let req = test::TestRequest::post()
        .uri("/ingest")
        .insert_header(("x-ingest-token", "igx_APPID"))
        .set_payload(
            serde_json::to_vec(&json!({
                "appid": "APPID",
                "xwhat": "custom_event",
                "xcontext": {
                    "installid": "iid-db-processor",
                    "os": "ios",
                    "idfa": "idfa-db-processor"
                }
            }))
            .expect("serialize payload"),
        )
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

    let emitted = parse_json_message(read_message_payload(&consumer).await.as_str());
    assert_eq!(emitted["xcontext"]["processor_marker"], json!("project-db"));
}

#[actix_rt::test]
async fn wal_replay_uses_processor_declared_sink_targets() {
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
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
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
                    auto_offset_reset: AutoOffsetReset::Earliest,
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
                    auto_offset_reset: AutoOffsetReset::Earliest,
                    delivery_timeout_ms: "5000".to_string(),
                    queue_buffering_max_ms: "0".to_string(),
                    batch_num_messages: "1".to_string(),
                    queue_buffering_max_messages: "300".to_string(),
                    linger_ms: "0".to_string(),
                },
            ),
        ]),
    })
    .expect("event sinks should initialize");
    let processor = processor_with_sinks(
        r#"
            fn process(event, request) {
                event["xwhat"] = "mutated_event";
                emit(SINK_KAFKA_MUTATED, event);
            }
        "#,
        &["kafka_mutated"],
    );
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
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
            checkpoint: CheckpointSettings::default(),
        })
        .await
        .expect("replay wal"),
        1
    );

    let emitted = parse_json_message(
        read_message_payload_with_timeout(&mutated_consumer)
            .await
            .as_str(),
    );
    assert_eq!(emitted["xwhat"], json!("mutated_event"));
    assert!(read_message_payload_with_short_timeout(&original_consumer)
        .await
        .is_none());
}

#[actix_rt::test]
async fn wal_replay_quarantines_invalid_json_record_and_continues() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-replay-invalid-json.toml");
    let kafka = create_kafka_config("wal-replay-invalid-json");
    let consumer = create_consumer(&kafka, "wal-replay-invalid-json-topic", &kafka.topic);

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
auto_offset_reset = "earliest"
bootstrap_servers = "{}"
topic = "{}"
delivery_timeout_ms = "5000"
queue_buffering_max_ms = "0"
batch_num_messages = "1"
queue_buffering_max_messages = "300"
linger_ms = "0"

[events.sink.events_error]
type = "kafka"
auto_offset_reset = "earliest"
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
    let writer = WalWriter::new(&settings.wal).expect("wal writer");
    writer
        .append(&new_record(
            "POST",
            "/ingest",
            None,
            None,
            BTreeMap::new(),
            1,
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
            1,
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

    let app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");
    let (quarantine_logs, _guard) = install_quarantine_capture();

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
    let checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/events.json")).expect("sink checkpoint"),
    )
    .expect("sink checkpoint json");
    assert_eq!(checkpoint["checkpoint_lsn"], json!(2));
    let quarantine = quarantine_logs.single_record();
    assert_eq!(quarantine["code"], json!("replay_invalid_json_body"));
    assert_eq!(quarantine["action"], json!("quarantine_record"));
    assert!(quarantine["message"]
        .as_str()
        .unwrap()
        .contains("invalid wal record json body"));
    assert!(quarantine["body_base64"].as_str().is_some());
    assert!(!wal_dir.join("quarantine.jsonl").exists());
}

#[actix_rt::test]
async fn wal_replay_flushes_checkpoint_after_quarantined_record_at_batch_end() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([("stdout".to_string(), stdout_sink(AutoOffsetReset::Earliest))]),
    })
    .expect("event sinks should initialize");
    let processor = processor_with_sinks(
        r#"
            fn process(event, request) {
                emit(SINK_STDOUT, event);

            }
        "#,
        &["stdout"],
    );
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
    })
    .expect("wal writer");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-before-invalid-json",
                "os": "ios"
            }
        })))
        .expect("append valid record");
    writer
        .append(&new_record(
            "POST",
            "/ingest",
            None,
            None,
            BTreeMap::new(),
            1,
            b"{not-json".to_vec(),
        ))
        .expect("append invalid json record");
    drop(writer);
    let (quarantine_logs, _guard) = install_quarantine_capture();

    assert_eq!(
        replay_once(WalReplayContext {
            dir: &wal_dir,
            event_sinks: &event_sinks,
            project_registry: &project_registry,
            rule_repository: &rule_repository,
            processor: &processor,
            checkpoint: CheckpointSettings {
                flush_interval: "1h".to_string(),
                flush_records: 1000,
                flush_bytes: 64 * 1024 * 1024,
            },
        })
        .await
        .expect("replay wal"),
        2
    );

    let checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/stdout.json")).expect("sink checkpoint"),
    )
    .expect("sink checkpoint json");
    assert_eq!(checkpoint["checkpoint_lsn"], json!(2));
    let quarantine = quarantine_logs.single_record();
    assert_eq!(quarantine["code"], json!("replay_invalid_json_body"));
    assert_eq!(quarantine["action"], json!("quarantine_record"));
    assert!(!wal_dir.join("quarantine.jsonl").exists());
}

#[actix_rt::test]
async fn wal_replay_advances_checkpoint_when_processor_emits_no_delivery() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([("stdout".to_string(), stdout_sink(AutoOffsetReset::Earliest))]),
    })
    .expect("event sinks should initialize");
    let processor = ProcessorState::new(
        r#"
            fn process(event, request) {
            }
        "#
        .to_string(),
        10_000,
    )
    .expect("processor should initialize");
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
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

    assert_eq!(
        replay_once(WalReplayContext {
            dir: &wal_dir,
            event_sinks: &event_sinks,
            project_registry: &project_registry,
            rule_repository: &rule_repository,
            processor: &processor,
            checkpoint: CheckpointSettings::default(),
        })
        .await
        .expect("processor without emit should drop and advance WAL"),
        1
    );

    let checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/stdout.json")).expect("sink checkpoint"),
    )
    .expect("sink checkpoint json");
    assert_eq!(checkpoint["checkpoint_lsn"], json!(1));
    let sink_checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/stdout.json")).expect("sink checkpoint"),
    )
    .expect("sink checkpoint json");
    assert_eq!(sink_checkpoint["sink_id"], json!("stdout"));
    assert_eq!(sink_checkpoint["checkpoint_lsn"], json!(1));
}

#[actix_rt::test]
async fn wal_replay_advances_unemitted_registered_sink_checkpoint() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([
            ("sink_a".to_string(), stdout_sink(AutoOffsetReset::Earliest)),
            ("sink_b".to_string(), stdout_sink(AutoOffsetReset::Earliest)),
        ]),
    })
    .expect("event sinks should initialize");
    let processor = processor_with_sinks(
        r#"
            fn process(event, request) {
                emit(SINK_SINK_A, event);
            }
        "#,
        &["sink_a"],
    );
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
    })
    .expect("wal writer");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-single-target",
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
            checkpoint: CheckpointSettings::default(),
        })
        .await
        .expect("replay should advance all registered sinks"),
        1
    );

    let sink_a_checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/sink_a.json")).expect("sink_a checkpoint"),
    )
    .expect("sink_a checkpoint json");
    let sink_b_checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/sink_b.json")).expect("sink_b checkpoint"),
    )
    .expect("sink_b checkpoint json");
    assert_eq!(sink_a_checkpoint["checkpoint_lsn"], json!(1));
    assert_eq!(sink_b_checkpoint["checkpoint_lsn"], json!(1));
    assert_eq!(sink_b_checkpoint["sink_id"], json!("sink_b"));
}

#[actix_rt::test]
async fn wal_replay_latest_offset_reset_skips_existing_wal_for_new_sink() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([("stdout".to_string(), stdout_sink(AutoOffsetReset::Latest))]),
    })
    .expect("event sinks should initialize");
    let processor = processor_with_sinks(
        r#"
            fn process(event, request) {
                emit(SINK_STDOUT, event);
            }
        "#,
        &["stdout"],
    );
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
    })
    .expect("wal writer");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-latest-skip",
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
            checkpoint: CheckpointSettings::default(),
        })
        .await
        .expect("latest reset should skip existing WAL"),
        0
    );
    let sink_checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/stdout.json")).expect("sink checkpoint"),
    )
    .expect("sink checkpoint json");
    assert_eq!(sink_checkpoint["checkpoint_lsn"], json!(1));
    assert!(!wal_dir.join("checkpoint.json").exists());
}

#[actix_rt::test]
async fn wal_replay_latest_offset_reset_initialized_before_append_reads_future_wal() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([("stdout".to_string(), stdout_sink(AutoOffsetReset::Latest))]),
    })
    .expect("event sinks should initialize");
    let processor = processor_with_sinks(
        r#"
            fn process(event, request) {
                emit(SINK_STDOUT, event);
            }
        "#,
        &["stdout"],
    );
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
    })
    .expect("wal writer");

    initialize_sink_checkpoints(&wal_dir, &event_sinks).expect("initialize sink checkpoints");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-latest-future",
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
            checkpoint: CheckpointSettings::default(),
        })
        .await
        .expect("latest reset initialized before append should read future WAL"),
        1
    );
    let sink_checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/stdout.json")).expect("sink checkpoint"),
    )
    .expect("sink checkpoint json");
    assert_eq!(sink_checkpoint["checkpoint_lsn"], json!(1));
    assert!(!wal_dir.join("checkpoint.json").exists());
}

#[actix_rt::test]
async fn wal_replay_quarantines_unknown_sink_target_and_advances_checkpoint() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([("stdout".to_string(), stdout_sink(AutoOffsetReset::Earliest))]),
    })
    .expect("event sinks should initialize");
    let processor = processor_with_sinks(
        r#"
            fn process(event, request) {
                emit(SINK_STDOUT, event);
                emit(SINK_MISSING_SINK, event);
            }
        "#,
        &["stdout", "missing_sink"],
    );
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
    })
    .expect("wal writer");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-unknown-sink",
                "os": "ios"
            }
        })))
        .expect("append record");
    drop(writer);
    let (quarantine_logs, _guard) = install_quarantine_capture();

    assert_eq!(
        replay_once(WalReplayContext {
            dir: &wal_dir,
            event_sinks: &event_sinks,
            project_registry: &project_registry,
            rule_repository: &rule_repository,
            processor: &processor,
            checkpoint: CheckpointSettings::default(),
        })
        .await
        .expect("unknown sink target should quarantine and continue"),
        1
    );

    let quarantine = quarantine_logs.single_record();
    assert_eq!(quarantine["code"], json!("replay_unknown_sink_target"));
    assert_eq!(quarantine["action"], json!("quarantine_record"));
    assert_eq!(quarantine["appid"], json!("APPID"));
    assert_eq!(quarantine["xwhat"], json!("custom_event"));
    assert_eq!(quarantine["target"], json!("missing_sink"));
    let body = STANDARD
        .decode(quarantine["body_base64"].as_str().expect("body_base64"))
        .expect("decode body");
    let body_json: Value = serde_json::from_slice(&body).expect("body json");
    assert_eq!(
        body_json["xcontext"]["installid"],
        json!("iid-unknown-sink")
    );
    assert!(!wal_dir.join("quarantine.jsonl").exists());
    let sink_checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/stdout.json")).expect("sink checkpoint"),
    )
    .expect("sink checkpoint json");
    assert_eq!(sink_checkpoint["checkpoint_lsn"], json!(1));
    assert!(!wal_dir.join("checkpoint.json").exists());
}

#[actix_rt::test]
async fn wal_replay_rejects_tampered_sink_checkpoint() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let kafka = create_kafka_config("wal-replay-checkpoint-checksum");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([(
            "kafka_valid".to_string(),
            EventSinkConfig::Kafka {
                bootstrap_servers: kafka.bootstrap_servers.clone(),
                topic: kafka.topic.clone(),
                auto_offset_reset: AutoOffsetReset::Earliest,
                delivery_timeout_ms: "5000".to_string(),
                queue_buffering_max_ms: "0".to_string(),
                batch_num_messages: "1".to_string(),
                queue_buffering_max_messages: "300".to_string(),
                linger_ms: "0".to_string(),
            },
        )]),
    })
    .expect("event sinks should initialize");
    let processor = processor_with_sinks(
        r#"
            fn process(event, request) {
                emit(SINK_KAFKA_VALID, event);
            }
        "#,
        &["kafka_valid"],
    );
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
    })
    .expect("wal writer");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-checkpoint-checksum",
                "os": "ios"
            }
        })))
        .expect("append record");
    drop(writer);

    let context = WalReplayContext {
        dir: &wal_dir,
        event_sinks: &event_sinks,
        project_registry: &project_registry,
        rule_repository: &rule_repository,
        processor: &processor,
        checkpoint: CheckpointSettings::default(),
    };
    assert_eq!(replay_once(context).await.expect("initial replay"), 1);

    let checkpoint_path = wal_dir.join("checkpoints/kafka_valid.json");
    let mut checkpoint: Value =
        serde_json::from_slice(&fs::read(&checkpoint_path).expect("read checkpoint"))
            .expect("checkpoint json");
    checkpoint["checkpoint_lsn"] = json!(0);
    fs::write(
        &checkpoint_path,
        serde_json::to_vec(&checkpoint).expect("serialize checkpoint"),
    )
    .expect("tamper checkpoint");

    let error = replay_once(WalReplayContext {
        dir: &wal_dir,
        event_sinks: &event_sinks,
        project_registry: &project_registry,
        rule_repository: &rule_repository,
        processor: &processor,
        checkpoint: CheckpointSettings::default(),
    })
    .await
    .expect_err("tampered checkpoint should fail checksum validation");

    assert!(error.to_string().contains("checkpoint checksum mismatch"));
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
[ingest]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"
wal_segment_max_bytes = 16

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

    for installid in ["iid-clean-1", "iid-clean-2"] {
        let req = test::TestRequest::post()
            .uri("/ingest")
            .insert_header(("x-ingest-token", "igx_APPID"))
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
    assert!(wal_dir.join("0000000000000001.wal").exists());
    assert!(wal_dir.join("0000000000000002.wal").exists());

    assert_eq!(
        server::replay_wal_once(&app_state)
            .await
            .expect("replay wal"),
        2
    );

    assert!(!wal_dir.join("0000000000000001.wal").exists());
    assert!(wal_dir.join("0000000000000002.wal").exists());
}

#[actix_rt::test]
async fn wal_replay_rejects_checkpoint_for_different_node_id() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let kafka = create_kafka_config("wal-replay-checkpoint-node");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([(
            "kafka_valid".to_string(),
            EventSinkConfig::Kafka {
                bootstrap_servers: kafka.bootstrap_servers.clone(),
                topic: kafka.topic.clone(),
                auto_offset_reset: AutoOffsetReset::Earliest,
                delivery_timeout_ms: "5000".to_string(),
                queue_buffering_max_ms: "0".to_string(),
                batch_num_messages: "1".to_string(),
                queue_buffering_max_messages: "300".to_string(),
                linger_ms: "0".to_string(),
            },
        )]),
    })
    .expect("event sinks should initialize");
    let processor = processor_with_sinks(
        r#"
            fn process(event, request) {
                emit(SINK_KAFKA_VALID, event);
            }
        "#,
        &["kafka_valid"],
    );
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
    })
    .expect("wal writer");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-checkpoint-node",
                "os": "ios"
            }
        })))
        .expect("append record");
    drop(writer);

    let context = WalReplayContext {
        dir: &wal_dir,
        event_sinks: &event_sinks,
        project_registry: &project_registry,
        rule_repository: &rule_repository,
        processor: &processor,
        checkpoint: CheckpointSettings::default(),
    };
    assert_eq!(replay_once(context).await.expect("initial replay"), 1);
    fs::write(wal_dir.join("node_id"), "different-node\n").expect("change node id");

    let error = replay_once(WalReplayContext {
        dir: &wal_dir,
        event_sinks: &event_sinks,
        project_registry: &project_registry,
        rule_repository: &rule_repository,
        processor: &processor,
        checkpoint: CheckpointSettings::default(),
    })
    .await
    .expect_err("checkpoint node_id mismatch should fail");

    assert!(error.to_string().contains("checkpoint node_id mismatch"));
}

#[actix_rt::test]
async fn wal_replay_stops_on_lsn_gap_without_checkpointing_later_record() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let project_repository = ProjectRepository::new(db.clone());
    let _project = project_repository
        .create_project(CreateProjectInput {
            name: "APPID".to_string(),
            enabled: true,
            ingest_token: "igx_APPID".to_string(),
        })
        .await
        .expect("project should be created");
    let project_registry = ProjectRegistryState::load(project_repository)
        .await
        .expect("project registry should load");
    let rule_repository = RuleRepository::new(db);
    let event_sinks = init_event_sinks(&EventsSettings {
        sink: HashMap::from([("stdout".to_string(), stdout_sink(AutoOffsetReset::Earliest))]),
    })
    .expect("event sinks should initialize");
    let processor = processor_with_sinks(
        r#"
            fn process(event, request) {
                emit(SINK_STDOUT, event);
            }
        "#,
        &["stdout"],
    );
    let writer = WalWriter::new(&ingest4x::settings::WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1s".to_string(),
        flush_max_records: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
        checkpoint: Default::default(),
    })
    .expect("wal writer");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-lsn-1",
                "os": "ios"
            }
        })))
        .expect("append first record");
    writer
        .append(&test_wal_record(json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": "iid-lsn-3",
                "os": "ios"
            }
        })))
        .expect("append second record");
    drop(writer);
    rewrite_wal_entry_lsn(&wal_dir, 1, 3);

    let error = replay_once(WalReplayContext {
        dir: &wal_dir,
        event_sinks: &event_sinks,
        project_registry: &project_registry,
        rule_repository: &rule_repository,
        processor: &processor,
        checkpoint: CheckpointSettings {
            flush_interval: "1h".to_string(),
            flush_records: 1,
            flush_bytes: 64 * 1024 * 1024,
        },
    })
    .await
    .expect_err("LSN gap should stop WAL replay");

    assert!(error.to_string().contains("non-contiguous wal lsn"));
    let checkpoint: Value = serde_json::from_slice(
        &fs::read(wal_dir.join("checkpoints/stdout.json")).expect("sink checkpoint"),
    )
    .expect("sink checkpoint json");
    assert_eq!(checkpoint["checkpoint_lsn"], json!(1));
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

fn processor_with_sinks(script: &str, sink_targets: &[&str]) -> ProcessorState {
    ProcessorState::new_with_sink_targets(
        script.to_string(),
        Vec::new(),
        sink_targets
            .iter()
            .map(|target| (*target).to_string())
            .collect(),
        10_000,
    )
    .expect("processor should initialize")
}

fn test_wal_record(payload: Value) -> WalRecord {
    new_record(
        "POST",
        "/ingest",
        None,
        None,
        BTreeMap::new(),
        1,
        serde_json::to_vec(&payload).expect("serialize payload"),
    )
}

#[derive(Clone, Default)]
struct CapturedQuarantineLogs {
    records: Arc<Mutex<Vec<Value>>>,
}

impl CapturedQuarantineLogs {
    fn single_record(&self) -> Value {
        let records = self.records.lock().expect("quarantine records");
        assert_eq!(records.len(), 1, "expected one quarantine record");
        records[0].clone()
    }
}

impl<S> Layer<S> for CapturedQuarantineLogs
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "ingest4x::wal::quarantine" {
            return;
        }

        let mut fields = JsonValueVisitor::default();
        event.record(&mut fields);
        let record = fields
            .values
            .remove("record")
            .unwrap_or_else(|| Value::Object(fields.values));
        self.records
            .lock()
            .expect("quarantine records")
            .push(record);
    }
}

#[derive(Default)]
struct JsonValueVisitor {
    values: Map<String, Value>,
}

impl JsonValueVisitor {
    fn insert_string_or_json(&mut self, field: &Field, value: &str) {
        let value =
            serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()));
        self.values.insert(field.name().to_string(), value);
    }
}

impl Visit for JsonValueVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.values
            .insert(field.name().to_string(), Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.values
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.values
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert_string_or_json(field, value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}");
        let unquoted = value
            .strip_prefix('"')
            .and_then(|text| text.strip_suffix('"'))
            .unwrap_or(value.as_str());
        self.insert_string_or_json(field, unquoted);
    }
}

fn install_quarantine_capture() -> (CapturedQuarantineLogs, tracing::subscriber::DefaultGuard) {
    let logs = CapturedQuarantineLogs::default();
    let subscriber = tracing_subscriber::registry().with(logs.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    (logs, guard)
}

fn stdout_sink(auto_offset_reset: AutoOffsetReset) -> EventSinkConfig {
    EventSinkConfig::Stdout { auto_offset_reset }
}

fn rewrite_wal_entry_lsn(wal_dir: &Path, entry_index: usize, new_lsn: u64) {
    let entries = read_entries_after_limit(wal_dir, None, None).expect("read wal entries");
    let entry = entries.get(entry_index).expect("entry to rewrite");
    let path = wal_dir.join(format!("{:016}.wal", entry.position.segment));
    let frame_len = usize::try_from(entry.next_position.offset - entry.position.offset)
        .expect("frame length should fit usize");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open wal segment");
    file.seek(SeekFrom::Start(entry.position.offset))
        .expect("seek frame");
    let mut frame = vec![0_u8; frame_len];
    file.read_exact(&mut frame).expect("read frame");

    let header_len = u16::from_be_bytes(frame[10..12].try_into().expect("header len")) as usize;
    let payload_len = u32::from_be_bytes(frame[34..38].try_into().expect("payload len")) as usize;
    let payload_start = header_len;
    let payload_end = payload_start + payload_len;
    let mut record: WalRecord =
        bitcode::deserialize(&frame[payload_start..payload_end]).expect("deserialize record");
    record.lsn = new_lsn;
    let payload = bitcode::serialize(&record).expect("serialize record");
    assert_eq!(payload.len(), payload_len);

    frame[16..24].copy_from_slice(&new_lsn.to_be_bytes());
    frame[38..42].copy_from_slice(&crc32fast::hash(&payload).to_be_bytes());
    frame[payload_start..payload_end].copy_from_slice(&payload);

    file.seek(SeekFrom::Start(entry.position.offset))
        .expect("seek frame for rewrite");
    file.write_all(&frame).expect("rewrite frame");
    file.sync_data().expect("sync rewritten frame");
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
