use crate::admin;
use crate::admin_ui;
use crate::db::{init_database, seed};
#[cfg(feature = "ingest")]
use crate::ingest;
#[cfg(feature = "ingest")]
use crate::ingest::processor::ProcessorState;
use crate::projects::{
    spawn_project_registry_refresh_loop, CreateProjectInput, ProjectRegistryState,
    ProjectRepository,
};
use crate::rules::RuleRepository;
use crate::settings::{default_database_refresh_interval_secs, Settings};
use crate::utils::events::{init_event_sinks, EventSinkState};
use crate::utils::prometheus::{init_private_prometheus, init_public_prometheus};
#[cfg(feature = "ingest")]
use crate::wal::WalWriter;
#[cfg(feature = "ingest")]
use crate::wal_replay::{replay_once, WalReplayContext};
use actix_web::web::{Data, ServiceConfig};
use actix_web::{web, App, HttpResponse, HttpServer};
use prometheus::Registry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
#[cfg(feature = "ingest")]
use tracing::warn;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub async fn index() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION").to_string()
    }))
}

pub async fn health(
    event_sinks: Data<EventSinkState>,
    redis_pool: Data<r2d2::Pool<redis::Client>>,
) -> HttpResponse {
    use redis::Commands;

    let events_status = if event_sinks.check_alive().await.is_ok() {
        "ok"
    } else {
        "error"
    };

    let pong: String = redis_pool.get().unwrap().ping().unwrap();
    let redis_status = if pong == "PONG" { "ok" } else { "error" };

    let overall_status = if events_status == "ok" && redis_status == "ok" {
        "ok"
    } else {
        "error"
    };

    HttpResponse::Ok().json(serde_json::json!({
        "status": overall_status,
        "events":events_status,
        "redis":redis_status
    }))
}

#[derive(Clone)]
pub struct AppState {
    settings: Arc<Settings>,
    event_sinks: Data<EventSinkState>,
    project_repository: Data<ProjectRepository>,
    rule_repository: Data<RuleRepository>,
    project_registry: Data<ProjectRegistryState>,
    #[cfg(feature = "ingest")]
    processor: Data<ProcessorState>,
    #[cfg(feature = "ingest")]
    wal: Option<Data<WalWriter>>,
}

pub async fn start(settings: Arc<Settings>) -> std::io::Result<()> {
    let shared_registry = Registry::new();
    let app_state = build_app_state(settings.clone()).await?;
    spawn_project_registry_refresh_loop(
        app_state.project_registry.clone(),
        project_registry_refresh_interval(&settings),
    );
    #[cfg(feature = "ingest")]
    spawn_wal_replay_loop(app_state.clone());

    let public_prometheus = init_public_prometheus(shared_registry.clone());
    let server_bind_address = settings.server.bind_address.clone();
    let management_bind_address = settings.management.bind_address.clone();

    let public_app_state = app_state.clone();
    let main_server = HttpServer::new(move || {
        App::new()
            .wrap(public_prometheus.clone())
            .configure(|cfg| configure_public_app(cfg, public_app_state.clone()))
    })
    .bind(server_bind_address.as_str())?
    .run();
    info!("ingest4x public server listening on http://{server_bind_address}");

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
    #[cfg(feature = "ingest")]
    let processor = Data::new(ProcessorState::from_default_entry().map_err(|error| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
    })?);
    #[cfg(feature = "ingest")]
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
        #[cfg(feature = "ingest")]
        processor,
        #[cfg(feature = "ingest")]
        wal,
    })
}

pub fn configure_public_app(cfg: &mut ServiceConfig, state: AppState) {
    cfg.app_data(Data::new(state.settings.clone()))
        .app_data(state.project_registry.clone())
        .service(web::scope("/").route("", web::get().to(index)));

    #[cfg(feature = "ingest")]
    {
        if let Some(wal) = state.wal {
            cfg.app_data(wal);
        }
        cfg.service(
            web::resource("/ingest")
                .app_data(state.rule_repository.clone())
                .app_data(state.event_sinks.clone())
                .app_data(state.project_registry.clone())
                .app_data(state.processor.clone())
                .route(web::post().to(ingest::post_ingest))
                .route(web::get().to(ingest::get_ingest)),
        );
    }
}

pub fn configure_private_app(cfg: &mut ServiceConfig, state: AppState) {
    cfg.app_data(Data::new(state.settings.clone()))
        .app_data(state.project_repository.clone())
        .app_data(state.rule_repository.clone())
        .app_data(state.project_registry.clone())
        .configure(admin::configure)
        .configure(admin_ui::configure)
        .service(
            SwaggerUi::new("/swagger-ui/{_:.*}")
                .url("/api-docs/openapi.json", admin::AdminApiDoc::openapi()),
        );
}

pub fn configure_app(cfg: &mut ServiceConfig, state: AppState) {
    configure_public_app(cfg, state);
}

#[cfg(feature = "ingest")]
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
    })
    .await
}

#[cfg(feature = "ingest")]
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

#[cfg(feature = "ingest")]
fn wal_replay_retry_delay(consecutive_errors: u32) -> Duration {
    let capped_shift = consecutive_errors.min(9);
    let delay_ms = 100_u64.saturating_mul(1_u64 << capped_shift).min(30_000);
    Duration::from_millis(delay_ms)
}

#[cfg(all(test, feature = "ingest"))]
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
    let repository = match settings.database.as_ref() {
        Some(database) => {
            let db = init_database(&database.url)
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;

            ProjectRepository::new(db)
        }
        None => {
            let db = init_database("sqlite::memory:")
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            let repository = ProjectRepository::new(db);

            import_mock_projects(&repository, &default_mock_projects()).await?;

            repository
        }
    };

    let rule_repository = RuleRepository::new(repository.database());
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
