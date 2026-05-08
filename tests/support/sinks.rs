use ingest4x::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, DeliveryTargetType, EventSinkRepository,
};
use ingest4x::settings::AutoOffsetReset;
use sea_orm::DatabaseConnection;
use serde_json::json;

#[allow(dead_code)]
pub async fn create_default_event_sinks(db: &DatabaseConnection) {
    let repository = EventSinkRepository::new(db.clone());
    let target = repository
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "default_stdout".to_string(),
            name: "Default Stdout".to_string(),
            target_type: DeliveryTargetType::Stdout,
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
