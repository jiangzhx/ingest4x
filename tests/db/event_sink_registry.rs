use ingest4x::db::init_sqlite_database;
use ingest4x::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, DeliveryTargetType, EventSinkRepository,
};
use ingest4x::settings::AutoOffsetReset;
use ingest4x::sinks::EventSinkState;
use serde_json::json;

#[tokio::test]
async fn event_sink_state_refreshes_when_database_sinks_change() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = EventSinkRepository::new(db);

    let target = repo
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "stdout_main".to_string(),
            name: "Main stdout".to_string(),
            target_type: DeliveryTargetType::stdout(),
            config_json: json!({}),
            enabled: true,
        })
        .await
        .expect("delivery target should be created");

    repo.create_event_sink(CreateEventSinkInput {
        sink_id: "events".to_string(),
        name: "Events".to_string(),
        delivery_target_id: target.id,
        destination_json: json!({}),
        auto_offset_reset: AutoOffsetReset::Latest,
        enabled: true,
    })
    .await
    .expect("initial event sink should be created");

    let state = EventSinkState::load(repo.clone())
        .await
        .expect("event sink state should load");
    assert!(state.contains_sink("events"));
    assert!(!state.contains_sink("payments"));

    repo.create_event_sink(CreateEventSinkInput {
        sink_id: "payments".to_string(),
        name: "Payments".to_string(),
        delivery_target_id: target.id,
        destination_json: json!({}),
        auto_offset_reset: AutoOffsetReset::Earliest,
        enabled: true,
    })
    .await
    .expect("new event sink should be created");

    assert!(state
        .refresh_if_needed()
        .await
        .expect("event sink state should refresh"));
    assert!(state.contains_sink("payments"));
    assert_eq!(
        state.auto_offset_reset("payments"),
        Some(AutoOffsetReset::Earliest)
    );
}
