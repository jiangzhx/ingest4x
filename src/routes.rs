use crate::admin::{self, ui as admin_ui_handlers};
use crate::ingest;
use crate::server::{healthz, index, AppState};
use actix_web::web::{self, Data, ServiceConfig};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub fn configure_public_surface(cfg: &mut ServiceConfig, state: AppState) {
    register_public_app_data(cfg, &state);
    register_public_routes(cfg, state);
}

pub fn configure_management_surface(cfg: &mut ServiceConfig, state: AppState) {
    register_management_app_data(cfg, &state);
    register_management_routes(cfg);
}

fn register_public_app_data(cfg: &mut ServiceConfig, state: &AppState) {
    cfg.app_data(Data::new(state.settings.clone()))
        .app_data(state.project_registry.clone())
        .app_data(state.wal.clone());
    if let Some(wal_metrics) = state.wal_metrics.clone() {
        cfg.app_data(wal_metrics);
    }
    if let Some(ingest_metrics) = state.ingest_metrics.clone() {
        cfg.app_data(ingest_metrics);
    }
}

fn register_public_routes(cfg: &mut ServiceConfig, state: AppState) {
    cfg.service(web::scope("/").route("", web::get().to(index)))
        .service(
            web::resource("/ingest")
                .app_data(state.rule_repository.clone())
                .app_data(state.event_sinks.clone())
                .app_data(state.project_registry.clone())
                .app_data(state.processor.clone())
                .route(web::post().to(ingest::ingest))
                .route(web::get().to(ingest::ingest)),
        );
}

fn register_management_app_data(cfg: &mut ServiceConfig, state: &AppState) {
    cfg.app_data(Data::new(state.settings.clone()))
        .app_data(state.project_repository.clone())
        .app_data(state.rule_repository.clone())
        .app_data(state.event_sink_repository.clone())
        .app_data(state.processor_repository.clone())
        .app_data(state.event_sinks.clone())
        .app_data(state.project_registry.clone())
        .app_data(state.processor.clone())
        .app_data(state.wal.clone());
    if let Some(wal_metrics) = state.wal_metrics.clone() {
        cfg.app_data(wal_metrics);
    }
}

fn register_management_routes(cfg: &mut ServiceConfig) {
    cfg.service(web::resource("/healthz").route(web::get().to(healthz)))
        .configure(admin::configure)
        .configure(admin_ui_handlers::configure)
        .service(
            SwaggerUi::new("/swagger-ui/{_:.*}")
                .url("/api-docs/openapi.json", admin::AdminApiDoc::openapi()),
        );
}
