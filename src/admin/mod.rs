pub mod auth;
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

        openapi.paths.paths.extend(rules_openapi.paths.paths);

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

        match (&mut openapi.tags, rules_openapi.tags) {
            (Some(tags), Some(mut rules_tags)) => tags.append(&mut rules_tags),
            (None, Some(rules_tags)) => openapi.tags = Some(rules_tags),
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
                .configure(projects::configure),
        ),
    );
}
