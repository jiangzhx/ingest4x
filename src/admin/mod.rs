pub mod auth;
pub mod event_sinks;
pub mod processors;
pub mod projects;
pub mod rules;
pub mod ui;

use actix_web::middleware::from_fn;
use actix_web::web::{self, ServiceConfig};
use utoipa::OpenApi;

pub struct AdminApiDoc;

impl OpenApi for AdminApiDoc {
    fn openapi() -> utoipa::openapi::OpenApi {
        let mut openapi = projects::AdminApiDoc::openapi();
        let rules_openapi = rules::AdminApiDoc::openapi();
        let event_sinks_openapi = event_sinks::AdminApiDoc::openapi();
        let processors_openapi = processors::AdminApiDoc::openapi();

        openapi.paths.paths.extend(rules_openapi.paths.paths);
        openapi.paths.paths.extend(event_sinks_openapi.paths.paths);
        openapi.paths.paths.extend(processors_openapi.paths.paths);

        if let Some(mut rules_components) = rules_openapi.components {
            let components = openapi
                .components
                .get_or_insert_with(utoipa::openapi::Components::new);
            components.schemas.append(&mut rules_components.schemas);
            components.responses.append(&mut rules_components.responses);
            components
                .security_schemes
                .append(&mut rules_components.security_schemes);
        }
        if let Some(mut event_sinks_components) = event_sinks_openapi.components {
            let components = openapi
                .components
                .get_or_insert_with(utoipa::openapi::Components::new);
            components
                .schemas
                .append(&mut event_sinks_components.schemas);
            components
                .responses
                .append(&mut event_sinks_components.responses);
            components
                .security_schemes
                .append(&mut event_sinks_components.security_schemes);
        }
        if let Some(mut processors_components) = processors_openapi.components {
            let components = openapi
                .components
                .get_or_insert_with(utoipa::openapi::Components::new);
            components
                .schemas
                .append(&mut processors_components.schemas);
            components
                .responses
                .append(&mut processors_components.responses);
            components
                .security_schemes
                .append(&mut processors_components.security_schemes);
        }

        match (&mut openapi.tags, rules_openapi.tags) {
            (Some(tags), Some(mut rules_tags)) => tags.append(&mut rules_tags),
            (None, Some(rules_tags)) => openapi.tags = Some(rules_tags),
            _ => {}
        }
        match (&mut openapi.tags, event_sinks_openapi.tags) {
            (Some(tags), Some(mut event_sinks_tags)) => tags.append(&mut event_sinks_tags),
            (None, Some(event_sinks_tags)) => openapi.tags = Some(event_sinks_tags),
            _ => {}
        }
        match (&mut openapi.tags, processors_openapi.tags) {
            (Some(tags), Some(mut processors_tags)) => tags.append(&mut processors_tags),
            (None, Some(processors_tags)) => openapi.tags = Some(processors_tags),
            _ => {}
        }

        openapi
    }
}

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.service(
        web::scope("/api/admin").configure(auth::configure).service(
            web::scope("")
                .wrap(from_fn(auth::require_admin_password))
                .configure(rules::configure)
                .configure(processors::configure)
                .configure(projects::configure)
                .configure(event_sinks::configure),
        ),
    );
}
