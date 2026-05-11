use actix_web::web::Data;
use ingest4x::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, DeliveryTarget, DeliveryTargetType,
    EventSinkRepository, RuntimeEventSink,
};
use ingest4x::settings::AutoOffsetReset;
use ingest4x::sinks::{init_event_sinks_from_runtime_sinks, EventSinkState};
use sea_orm::DatabaseConnection;
use serde_json::json;

#[allow(dead_code)]
pub async fn create_default_event_sinks(db: &DatabaseConnection) {
    let repository = EventSinkRepository::new(db.clone());
    let target = repository
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "default_stdout".to_string(),
            name: "Default Stdout".to_string(),
            target_type: DeliveryTargetType::stdout(),
            config_json: json!({}),
            enabled: true,
        })
        .await
        .expect("default stdout target should be created");

    for sink_id in ["events", "events_error"] {
        repository
            .create_event_sink(CreateEventSinkInput {
                sink_id: sink_id.to_string(),
                name: sink_id.to_string(),
                delivery_target_id: target.id,
                destination_json: json!({}),
                auto_offset_reset: AutoOffsetReset::Latest,
                enabled: true,
            })
            .await
            .expect("default event sink should be created");
    }
}

#[allow(dead_code)]
pub fn init_stdout_event_sinks() -> Data<EventSinkState> {
    init_event_sinks_from_runtime_sinks(vec![
        stdout_runtime_sink("events", AutoOffsetReset::Latest),
        stdout_runtime_sink("events_error", AutoOffsetReset::Latest),
    ])
    .expect("stdout event sinks should initialize")
}

#[allow(dead_code)]
pub fn init_kafka_event_sinks(
    bootstrap_servers: &str,
    topic: &str,
    error_topic: &str,
) -> Data<EventSinkState> {
    init_event_sinks_from_runtime_sinks(vec![
        kafka_runtime_sink("events", bootstrap_servers, topic, AutoOffsetReset::Latest),
        kafka_runtime_sink(
            "events_error",
            bootstrap_servers,
            error_topic,
            AutoOffsetReset::Latest,
        ),
    ])
    .expect("kafka event sinks should initialize")
}

#[allow(dead_code)]
pub fn stdout_runtime_sink(sink_id: &str, auto_offset_reset: AutoOffsetReset) -> RuntimeEventSink {
    RuntimeEventSink {
        sink_id: sink_id.to_string(),
        name: sink_id.to_string(),
        destination_json: json!({}),
        auto_offset_reset,
        target: DeliveryTarget {
            id: 1,
            target_id: format!("{sink_id}_target"),
            name: format!("{sink_id} target"),
            target_type: DeliveryTargetType::stdout(),
            config_json: json!({}),
            enabled: true,
            created_at: 0,
            updated_at: 0,
        },
    }
}

#[allow(dead_code)]
pub fn blackhole_runtime_sink(
    sink_id: &str,
    destination_json: serde_json::Value,
    auto_offset_reset: AutoOffsetReset,
) -> RuntimeEventSink {
    RuntimeEventSink {
        sink_id: sink_id.to_string(),
        name: sink_id.to_string(),
        destination_json,
        auto_offset_reset,
        target: DeliveryTarget {
            id: 1,
            target_id: format!("{sink_id}_target"),
            name: format!("{sink_id} target"),
            target_type: DeliveryTargetType::parse("blackhole")
                .expect("blackhole target type should be registered"),
            config_json: json!({}),
            enabled: true,
            created_at: 0,
            updated_at: 0,
        },
    }
}

#[allow(dead_code)]
pub fn kafka_runtime_sink(
    sink_id: &str,
    bootstrap_servers: &str,
    topic: &str,
    auto_offset_reset: AutoOffsetReset,
) -> RuntimeEventSink {
    RuntimeEventSink {
        sink_id: sink_id.to_string(),
        name: sink_id.to_string(),
        destination_json: json!({ "topic": topic }),
        auto_offset_reset,
        target: DeliveryTarget {
            id: 1,
            target_id: format!("{sink_id}_target"),
            name: format!("{sink_id} target"),
            target_type: DeliveryTargetType::kafka(),
            config_json: json!({
                "bootstrap_servers": bootstrap_servers,
                "delivery_timeout_ms": "5000",
                "queue_buffering_max_ms": "0",
                "batch_num_messages": "1",
                "queue_buffering_max_messages": "300",
                "linger_ms": "0"
            }),
            enabled: true,
            created_at: 0,
            updated_at: 0,
        },
    }
}
