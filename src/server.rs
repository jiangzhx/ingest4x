use crate::db::{init_database, seed};
use crate::ingest::processor::ProcessorState;
use crate::repositories::{CreateProjectInput, ProjectRepository, RuleRepository};
use crate::routes;
use crate::services::{spawn_project_registry_refresh_loop, ProjectRegistryState};
use crate::settings::{default_database_refresh_interval_secs, Settings};
use crate::utils::events::{init_event_sinks, EventSinkState};
use crate::utils::prometheus::{
    init_private_prometheus, init_public_prometheus, IngestPrometheusMetrics, WalPrometheusMetrics,
};
use crate::wal::replay::{replay_once, WalReplayContext};
use crate::wal::WalWriter;
use actix_web::web::{Data, ServiceConfig};
use actix_web::{App, HttpResponse, HttpServer};
use prometheus::Registry;
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

pub async fn healthz(settings: Data<Arc<Settings>>, wal: Option<Data<WalWriter>>) -> HttpResponse {
    let wal_enabled = settings.wal.is_some();
    let wal_ready = match (wal_enabled, wal.as_ref()) {
        (false, _) => true,
        (true, Some(wal)) => wal.check_ready().is_ok(),
        (true, None) => false,
    };

    let status = if wal_ready { "ok" } else { "error" };
    let body = serde_json::json!({
        "status": status,
        "wal_enabled": wal_enabled,
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
    pub(crate) project_registry: Data<ProjectRegistryState>,
    pub(crate) processor: Data<ProcessorState>,
    pub(crate) wal: Option<Data<WalWriter>>,
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
    let event_sinks = init_event_sinks(&settings.events).map_err(|error| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
    })?;
    let (project_repository, rule_repository, project_registry) =
        init_project_state(settings.clone()).await?;
    let processor = Data::new(ProcessorState::from_default_entry().map_err(|error| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
    })?);
    let wal = settings
        .wal
        .as_ref()
        .map(WalWriter::new)
        .transpose()
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .map(Data::new);

    Ok(AppState {
        settings,
        event_sinks,
        project_repository,
        rule_repository,
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
    metrics.observe(&state.settings, state.wal.as_ref().map(Data::get_ref));
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
    let Some(wal) = state.settings.wal.as_ref() else {
        return Ok(0);
    };

    replay_once(WalReplayContext {
        dir: std::path::Path::new(&wal.dir),
        event_sinks: &state.event_sinks,
        project_registry: &state.project_registry,
        rule_repository: &state.rule_repository,
        processor: &state.processor,
        checkpoint: wal.checkpoint.clone(),
    })
    .await
}

fn spawn_wal_replay_loop(state: AppState) {
    if state.settings.wal.is_none() {
        return;
    }

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
                        metrics.observe(&state.settings, state.wal.as_ref().map(Data::get_ref));
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

async fn init_project_state(
    settings: Arc<Settings>,
) -> std::io::Result<(
    Data<ProjectRepository>,
    Data<RuleRepository>,
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

    let rule_repository = RuleRepository::new(db);
    seed::run(&repository, &rule_repository).await?;
    let project_registry = Data::new(
        ProjectRegistryState::load(repository.clone())
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    );

    Ok((
        Data::new(repository),
        Data::new(rule_repository),
        project_registry,
    ))
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
                appid: appid.clone(),
                name: attributes
                    .get("name")
                    .cloned()
                    .unwrap_or_else(|| appid.clone()),
                enabled: true,
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
