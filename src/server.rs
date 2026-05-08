use crate::db::{init_database, seed};
use crate::ingest::processor::{ProcessorRegistryState, ProcessorState};
use crate::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, CreateProjectInput, DeliveryTargetType,
    EventSinkRepository, ProcessorRepository, ProjectRepository, RuleRepository,
};
use crate::routes;
use crate::services::{spawn_project_registry_refresh_loop, ProjectRegistryState};
use crate::settings::{
    default_database_refresh_interval_secs, default_kafka_batch_num_messages,
    default_kafka_delivery_timeout_ms, default_kafka_linger_ms,
    default_kafka_queue_buffering_max_messages, default_kafka_queue_buffering_max_ms,
    AutoOffsetReset, EventSinkConfig, EventsSettings, Settings,
};
use crate::utils::events::EventSinkState;
use crate::utils::prometheus::{
    init_private_prometheus, init_public_prometheus, IngestPrometheusMetrics, WalPrometheusMetrics,
};
use crate::wal::replay::{initialize_sink_checkpoints, replay_once, WalReplayContext};
use crate::wal::WalWriter;
use actix_web::web::{Data, ServiceConfig};
use actix_web::{App, HttpResponse, HttpServer};
use prometheus::Registry;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing::warn;

pub async fn index() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION").to_string()
    }))
}

pub async fn healthz(wal: Data<WalWriter>) -> HttpResponse {
    let wal_ready = wal.check_ready().is_ok();

    let status = if wal_ready { "ok" } else { "error" };
    let body = serde_json::json!({
        "status": status,
        "wal_enabled": true,
        "wal_ready": wal_ready,
    });

    if wal_ready {
        HttpResponse::Ok().json(body)
    } else {
        HttpResponse::ServiceUnavailable().json(body)
    }
}

#[derive(Clone)]
pub struct AppState {
    pub(crate) settings: Arc<Settings>,
    pub(crate) event_sinks: Data<EventSinkState>,
    pub(crate) project_repository: Data<ProjectRepository>,
    pub(crate) rule_repository: Data<RuleRepository>,
    pub(crate) event_sink_repository: Data<EventSinkRepository>,
    pub(crate) processor_repository: Data<ProcessorRepository>,
    pub(crate) project_registry: Data<ProjectRegistryState>,
    pub(crate) processor: Data<ProcessorRegistryState>,
    pub(crate) wal: Data<WalWriter>,
    pub(crate) wal_metrics: Option<Data<WalPrometheusMetrics>>,
    pub(crate) ingest_metrics: Option<Data<IngestPrometheusMetrics>>,
}

pub async fn start(settings: Arc<Settings>) -> std::io::Result<()> {
    let shared_registry = Registry::new();
    let mut app_state = build_app_state(settings.clone()).await?;
    register_wal_prometheus_metrics(&shared_registry, &mut app_state)?;
    spawn_project_registry_refresh_loop(
        app_state.project_registry.clone(),
        project_registry_refresh_interval(&settings),
    );
    spawn_event_sink_refresh_loop(
        app_state.event_sinks.clone(),
        project_registry_refresh_interval(&settings),
    );
    spawn_processor_refresh_loop(
        app_state.processor.clone(),
        project_registry_refresh_interval(&settings),
    );
    spawn_wal_replay_loop(app_state.clone());

    let public_prometheus = init_public_prometheus(shared_registry.clone());
    let ingest_bind_address = settings.ingest.bind_address.clone();
    let management_bind_address = settings.management.bind_address.clone();

    let public_app_state = app_state.clone();
    let main_server = HttpServer::new(move || {
        App::new()
            .wrap(public_prometheus.clone())
            .configure(|cfg| configure_public_app(cfg, public_app_state.clone()))
    })
    .bind(ingest_bind_address.as_str())?
    .run();
    info!("ingest4x ingest server listening on http://{ingest_bind_address}");

    let private_prometheus = init_private_prometheus(shared_registry.clone());

    let private_app_state = app_state.clone();
    let management_server = HttpServer::new(move || {
        App::new()
            .wrap(private_prometheus.clone())
            .configure(|cfg| configure_private_app(cfg, private_app_state.clone()))
    })
    .bind(management_bind_address.as_str())?
    .run();
    info!("ingest4x management server listening on http://{management_bind_address}");

    futures::try_join!(main_server, management_server)?;

    Ok(())
}

pub async fn build_app_state(settings: Arc<Settings>) -> std::io::Result<AppState> {
    let (
        project_repository,
        rule_repository,
        event_sink_repository,
        processor_repository,
        project_registry,
    ) = init_repository_state(settings.clone()).await?;
    let processor = ProcessorRegistryState::load(processor_repository.get_ref().clone())
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    build_app_state_from_parts(
        settings,
        project_repository,
        rule_repository,
        event_sink_repository,
        processor_repository,
        project_registry,
        processor,
    )
    .await
}

pub async fn build_app_state_with_processor(
    settings: Arc<Settings>,
    processor: ProcessorState,
) -> std::io::Result<AppState> {
    let (
        project_repository,
        rule_repository,
        event_sink_repository,
        processor_repository,
        project_registry,
    ) = init_repository_state(settings.clone()).await?;
    build_app_state_from_parts(
        settings,
        project_repository,
        rule_repository,
        event_sink_repository,
        processor_repository,
        project_registry,
        ProcessorRegistryState::from_processor(processor),
    )
    .await
}

async fn build_app_state_from_parts(
    settings: Arc<Settings>,
    project_repository: Data<ProjectRepository>,
    rule_repository: Data<RuleRepository>,
    event_sink_repository: Data<EventSinkRepository>,
    processor_repository: Data<ProcessorRepository>,
    project_registry: Data<ProjectRegistryState>,
    processor: ProcessorRegistryState,
) -> std::io::Result<AppState> {
    let event_sinks = Data::new(
        EventSinkState::load(event_sink_repository.get_ref().clone())
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    );
    let processor = Data::new(processor);
    let sink_names = event_sinks.sink_names();
    let wal = Data::new(
        WalWriter::new_for_active_sinks(&settings.wal, &sink_names)
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    );
    initialize_sink_checkpoints(std::path::Path::new(&settings.wal.dir), &event_sinks)
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(AppState {
        settings,
        event_sinks,
        project_repository,
        rule_repository,
        event_sink_repository,
        processor_repository,
        project_registry,
        processor,
        wal,
        wal_metrics: None,
        ingest_metrics: None,
    })
}

pub fn register_wal_prometheus_metrics(
    registry: &Registry,
    state: &mut AppState,
) -> std::io::Result<()> {
    let ingest_metrics = Data::new(
        IngestPrometheusMetrics::register(registry)
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    );
    let metrics = Data::new(
        WalPrometheusMetrics::register(registry)
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    );
    let sink_names = state.event_sinks.sink_names();
    metrics.observe(&state.settings, state.wal.get_ref(), &sink_names);
    state.wal_metrics = Some(metrics);
    state.ingest_metrics = Some(ingest_metrics);
    Ok(())
}

pub fn configure_public_app(cfg: &mut ServiceConfig, state: AppState) {
    routes::configure_public_surface(cfg, state);
}

pub fn configure_private_app(cfg: &mut ServiceConfig, state: AppState) {
    routes::configure_management_surface(cfg, state);
}

pub fn configure_app(cfg: &mut ServiceConfig, state: AppState) {
    configure_public_app(cfg, state);
}

pub async fn replay_wal_once(state: &AppState) -> anyhow::Result<usize> {
    let replayed = replay_once(WalReplayContext {
        dir: std::path::Path::new(&state.settings.wal.dir),
        event_sinks: &state.event_sinks,
        project_registry: &state.project_registry,
        rule_repository: &state.rule_repository,
        processor: state.processor.get_ref(),
        checkpoint: state.settings.wal.checkpoint.clone(),
    })
    .await?;

    if let Some(metrics) = state.wal_metrics.as_ref() {
        let sink_names = state.event_sinks.sink_names();
        metrics.observe(&state.settings, state.wal.get_ref(), &sink_names);
    }

    Ok(replayed)
}

fn spawn_wal_replay_loop(state: AppState) {
    tokio::spawn(async move {
        let mut consecutive_errors = 0_u32;
        loop {
            match replay_wal_once(&state).await {
                Ok(0) => {
                    consecutive_errors = 0;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Ok(_) => {
                    consecutive_errors = 0;
                }
                Err(error) => {
                    if let Some(metrics) = state.wal_metrics.as_ref() {
                        metrics.inc_replay_errors();
                        let sink_names = state.event_sinks.sink_names();
                        metrics.observe(&state.settings, state.wal.get_ref(), &sink_names);
                    }
                    let retry_delay = wal_replay_retry_delay(consecutive_errors);
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    warn!(
                        error = %error,
                        retry_delay_ms = retry_delay.as_millis(),
                        "wal replay failed; retrying"
                    );
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    });
}

fn spawn_event_sink_refresh_loop(
    event_sinks: Data<EventSinkState>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;

            if let Err(error) = event_sinks.refresh_if_needed().await {
                warn!("refresh event sink router snapshot failed: {error}");
            }
        }
    })
}

fn spawn_processor_refresh_loop(
    processor: Data<ProcessorRegistryState>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;

            if let Err(error) = processor.refresh_if_needed().await {
                warn!("refresh processor router snapshot failed: {error}");
            }
        }
    })
}

fn wal_replay_retry_delay(consecutive_errors: u32) -> Duration {
    let capped_shift = consecutive_errors.min(9);
    let delay_ms = 100_u64.saturating_mul(1_u64 << capped_shift).min(30_000);
    Duration::from_millis(delay_ms)
}

#[cfg(test)]
mod tests {
    use super::wal_replay_retry_delay;
    use std::time::Duration;

    #[test]
    fn wal_replay_retry_delay_uses_exponential_backoff_with_cap() {
        assert_eq!(wal_replay_retry_delay(0), Duration::from_millis(100));
        assert_eq!(wal_replay_retry_delay(1), Duration::from_millis(200));
        assert_eq!(wal_replay_retry_delay(2), Duration::from_millis(400));
        assert_eq!(wal_replay_retry_delay(20), Duration::from_secs(30));
    }
}

async fn init_repository_state(
    settings: Arc<Settings>,
) -> std::io::Result<(
    Data<ProjectRepository>,
    Data<RuleRepository>,
    Data<EventSinkRepository>,
    Data<ProcessorRepository>,
    Data<ProjectRegistryState>,
)> {
    let db = match settings.database.as_ref() {
        Some(database) => init_database(&database.url)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?,
        None => init_database("sqlite::memory:")
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    };

    let repository = ProjectRepository::new(db.clone());
    if settings.database.is_none() {
        import_mock_projects(&repository, &default_mock_projects()).await?;
    }

    let rule_repository = RuleRepository::new(db.clone());
    let event_sink_repository = EventSinkRepository::new(db.clone());
    let processor_repository = ProcessorRepository::new(db);
    seed_default_delivery_targets(&event_sink_repository).await?;
    import_config_event_sinks(&event_sink_repository, &settings.events).await?;
    seed_default_event_sinks(&event_sink_repository).await?;
    seed::run(&repository, &rule_repository, &processor_repository).await?;
    let project_registry = Data::new(
        ProjectRegistryState::load(repository.clone())
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    );

    Ok((
        Data::new(repository),
        Data::new(rule_repository),
        Data::new(event_sink_repository),
        Data::new(processor_repository),
        project_registry,
    ))
}

async fn seed_default_delivery_targets(repository: &EventSinkRepository) -> std::io::Result<()> {
    let existing = repository
        .list_delivery_targets()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if existing
        .iter()
        .any(|target| target.target_id == "local_kafka")
    {
        return Ok(());
    }

    repository
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "local_kafka".to_string(),
            name: "Local Kafka".to_string(),
            target_type: DeliveryTargetType::Kafka,
            config_json: json!({
                "bootstrap_servers": "127.0.0.1:9092",
                "delivery_timeout_ms": default_kafka_delivery_timeout_ms(),
                "queue_buffering_max_ms": default_kafka_queue_buffering_max_ms(),
                "batch_num_messages": default_kafka_batch_num_messages(),
                "queue_buffering_max_messages": default_kafka_queue_buffering_max_messages(),
                "linger_ms": default_kafka_linger_ms()
            }),
            enabled: true,
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn import_config_event_sinks(
    repository: &EventSinkRepository,
    settings: &EventsSettings,
) -> std::io::Result<()> {
    if settings.sink.is_empty() {
        return Ok(());
    }

    let existing = repository
        .list_event_sinks()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if !existing.is_empty() {
        return Ok(());
    }

    for (sink_id, config) in &settings.sink {
        let target_id = format!("{sink_id}_target");
        let target = repository
            .create_delivery_target(config_delivery_target_input(sink_id, &target_id, config))
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;

        repository
            .create_event_sink(config_event_sink_input(sink_id, target.id, config))
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }

    Ok(())
}

async fn seed_default_event_sinks(repository: &EventSinkRepository) -> std::io::Result<()> {
    let existing = repository
        .list_event_sinks()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let missing_sink_ids = ["events", "events_error"]
        .into_iter()
        .filter(|sink_id| !existing.iter().any(|sink| sink.sink_id == *sink_id))
        .collect::<Vec<_>>();
    if missing_sink_ids.is_empty() {
        return Ok(());
    }

    let targets = repository
        .list_delivery_targets()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let target = match targets
        .into_iter()
        .find(|target| target.target_id == "default_stdout")
    {
        Some(target) => target,
        None => repository
            .create_delivery_target(CreateDeliveryTargetInput {
                target_id: "default_stdout".to_string(),
                name: "Default Stdout".to_string(),
                target_type: DeliveryTargetType::Stdout,
                config_json: json!({}),
                enabled: true,
            })
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    };

    for sink_id in missing_sink_ids {
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
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }

    Ok(())
}

fn config_delivery_target_input(
    sink_id: &str,
    target_id: &str,
    config: &EventSinkConfig,
) -> CreateDeliveryTargetInput {
    match config {
        EventSinkConfig::Kafka {
            bootstrap_servers,
            topic: _,
            auto_offset_reset: _,
            delivery_timeout_ms,
            queue_buffering_max_ms,
            batch_num_messages,
            queue_buffering_max_messages,
            linger_ms,
        } => CreateDeliveryTargetInput {
            target_id: target_id.to_string(),
            name: format!("{sink_id} target"),
            target_type: DeliveryTargetType::Kafka,
            config_json: json!({
                "bootstrap_servers": bootstrap_servers,
                "delivery_timeout_ms": delivery_timeout_ms,
                "queue_buffering_max_ms": queue_buffering_max_ms,
                "batch_num_messages": batch_num_messages,
                "queue_buffering_max_messages": queue_buffering_max_messages,
                "linger_ms": linger_ms
            }),
            enabled: true,
        },
        EventSinkConfig::Stdout {
            auto_offset_reset: _,
        } => CreateDeliveryTargetInput {
            target_id: target_id.to_string(),
            name: format!("{sink_id} target"),
            target_type: DeliveryTargetType::Stdout,
            config_json: json!({}),
            enabled: true,
        },
    }
}

fn config_event_sink_input(
    sink_id: &str,
    delivery_target_id: i32,
    config: &EventSinkConfig,
) -> CreateEventSinkInput {
    let destination_json = match config {
        EventSinkConfig::Kafka { topic, .. } => json!({ "topic": topic }),
        EventSinkConfig::Stdout { .. } => json!({}),
    };

    CreateEventSinkInput {
        sink_id: sink_id.to_string(),
        name: sink_id.to_string(),
        delivery_target_id,
        destination_json,
        auto_offset_reset: config.auto_offset_reset(),
        enabled: true,
    }
}

async fn import_mock_projects(
    repository: &ProjectRepository,
    mock_projects: &HashMap<String, HashMap<String, String>>,
) -> std::io::Result<()> {
    for (appid, attributes) in mock_projects {
        if !mock_project_enabled(attributes) {
            continue;
        }

        repository
            .create_project(CreateProjectInput {
                name: attributes
                    .get("name")
                    .cloned()
                    .unwrap_or_else(|| appid.clone()),
                enabled: true,
                ingest_token: attributes
                    .get("ingest_token")
                    .cloned()
                    .unwrap_or_else(|| format!("igx_{appid}")),
            })
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }

    Ok(())
}

fn default_mock_projects() -> HashMap<String, HashMap<String, String>> {
    HashMap::from([(
        "APPID".to_string(),
        HashMap::from([
            ("re_attribution".to_string(), "300".to_string()),
            ("os".to_string(), "android".to_string()),
        ]),
    )])
}

fn mock_project_enabled(attributes: &HashMap<String, String>) -> bool {
    attributes
        .get("enabled")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(true)
}

fn project_registry_refresh_interval(settings: &Settings) -> Duration {
    let seconds = settings
        .database
        .as_ref()
        .map(|database| database.refresh_interval_secs)
        .unwrap_or_else(default_database_refresh_interval_secs);

    Duration::from_secs(seconds.max(1))
}
