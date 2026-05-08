use ingest4x::db::init_sqlite_database;
use ingest4x::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, DeliveryTargetType, EventSinkRepository,
    EventSinkRepositoryError,
};
use ingest4x::settings::AutoOffsetReset;
use serde_json::json;

#[tokio::test]
async fn create_kafka_target_and_sink_with_valid_typed_json() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = EventSinkRepository::new(db);

    let target = repo
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "kafka_main".to_string(),
            name: "Main Kafka".to_string(),
            target_type: DeliveryTargetType::Kafka,
            config_json: json!({
                "bootstrap_servers": "127.0.0.1:9092",
                "delivery_timeout_ms": "3000",
                "queue_buffering_max_ms": "0",
                "batch_num_messages": "100",
                "queue_buffering_max_messages": "300",
                "linger_ms": "100"
            }),
            enabled: true,
        })
        .await
        .expect("delivery target should be created");

    let sink = repo
        .create_event_sink(CreateEventSinkInput {
            sink_id: "events".to_string(),
            name: "Main events".to_string(),
            delivery_target_id: target.id,
            destination_json: json!({
                "topic": "ingest4x-events"
            }),
            auto_offset_reset: AutoOffsetReset::Latest,
            enabled: true,
        })
        .await
        .expect("event sink should be created");

    assert_eq!(sink.sink_id, "events");
    assert_eq!(sink.name, "Main events");
    assert_eq!(sink.delivery_target_id, target.id);
    assert_eq!(sink.auto_offset_reset, AutoOffsetReset::Latest);
    assert_eq!(
        repo.event_sinks_version()
            .await
            .expect("event sinks version should load"),
        2
    );

    let runtime = repo
        .list_enabled_runtime_sinks()
        .await
        .expect("runtime sinks should load");
    assert_eq!(runtime.len(), 1);
    assert_eq!(runtime[0].sink_id, "events");
    assert_eq!(runtime[0].target.target_id, "kafka_main");
}

#[tokio::test]
async fn rejects_kafka_target_config_unknown_fields() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = EventSinkRepository::new(db);

    let error = repo
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "kafka_main".to_string(),
            name: "Main Kafka".to_string(),
            target_type: DeliveryTargetType::Kafka,
            config_json: json!({
                "bootstrap_servers": "127.0.0.1:9092",
                "unknown": true
            }),
            enabled: true,
        })
        .await
        .expect_err("unknown config fields should be rejected");

    assert!(matches!(
        error,
        EventSinkRepositoryError::InvalidConfig { ref message }
            if message.contains("unknown field")
    ));
}

#[tokio::test]
async fn rejects_sink_destination_that_does_not_match_target_type() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = EventSinkRepository::new(db);

    let target = repo
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "kafka_main".to_string(),
            name: "Main Kafka".to_string(),
            target_type: DeliveryTargetType::Kafka,
            config_json: json!({
                "bootstrap_servers": "127.0.0.1:9092"
            }),
            enabled: true,
        })
        .await
        .expect("delivery target should be created");

    let error = repo
        .create_event_sink(CreateEventSinkInput {
            sink_id: "events".to_string(),
            name: "Main events".to_string(),
            delivery_target_id: target.id,
            destination_json: json!({
                "table": "events"
            }),
            auto_offset_reset: AutoOffsetReset::Latest,
            enabled: true,
        })
        .await
        .expect_err("kafka sink destination should require topic");

    assert!(matches!(
        error,
        EventSinkRepositoryError::InvalidConfig { ref message }
            if message.contains("missing field `topic`")
    ));
}

#[tokio::test]
async fn rejects_deleting_delivery_target_used_by_event_sink() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = EventSinkRepository::new(db);

    let target = repo
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "stdout_main".to_string(),
            name: "Main stdout".to_string(),
            target_type: DeliveryTargetType::Stdout,
            config_json: json!({}),
            enabled: true,
        })
        .await
        .expect("delivery target should be created");

    repo.create_event_sink(CreateEventSinkInput {
        sink_id: "events".to_string(),
        name: "Main events".to_string(),
        delivery_target_id: target.id,
        destination_json: json!({}),
        auto_offset_reset: AutoOffsetReset::Latest,
        enabled: true,
    })
    .await
    .expect("event sink should be created");

    let error = repo
        .delete_delivery_target(target.id)
        .await
        .expect_err("target used by an event sink should not be deleted");

    assert!(matches!(
        error,
        EventSinkRepositoryError::DeliveryTargetInUse { id } if id == target.id
    ));
}
