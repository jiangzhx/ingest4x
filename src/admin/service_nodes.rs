use crate::repositories::{ServiceNode, ServiceNodeRepository};
use crate::settings::Settings;
use actix_web::web::{self, Data, ServiceConfig};
use actix_web::HttpResponse;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};

#[derive(Debug, Serialize, PartialEq, ToSchema)]
struct ServiceNodeResponse {
    node_id: String,
    hostname: Option<String>,
    machine_ip: Option<String>,
    ingest_bind_address: String,
    management_bind_address: String,
    version: String,
    status: String,
    started_at: i64,
    last_seen_at: i64,
    updated_at: i64,
    #[schema(value_type = Object)]
    metadata_json: Option<Value>,
}

#[derive(OpenApi)]
#[openapi(
    paths(list_service_nodes),
    components(schemas(ServiceNodeResponse)),
    tags((name = "admin.service_nodes", description = "Admin service node endpoints"))
)]
pub struct AdminApiDoc;

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.route("/service-nodes", web::get().to(list_service_nodes));
}

#[utoipa::path(
    get,
    path = "/api/admin/service-nodes",
    tag = "admin.service_nodes",
    responses(
        (status = 200, description = "List service nodes", body = [ServiceNodeResponse]),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn list_service_nodes(
    repository: Data<ServiceNodeRepository>,
    settings: Data<Arc<Settings>>,
) -> HttpResponse {
    let stale_after_ms = service_node_stale_after_ms(settings.database.as_ref());
    match repository.list_service_nodes().await {
        Ok(nodes) => HttpResponse::Ok().json(
            nodes
                .into_iter()
                .map(|node| ServiceNodeResponse::from_node(node, stale_after_ms))
                .collect::<Vec<_>>(),
        ),
        Err(error) => HttpResponse::InternalServerError().body(error.to_string()),
    }
}

fn service_node_stale_after_ms(database: Option<&crate::settings::DatabaseSettings>) -> i64 {
    let refresh_interval_secs = database
        .map(|settings| settings.refresh_interval_secs)
        .unwrap_or_else(crate::settings::default_database_refresh_interval_secs);
    refresh_interval_secs.saturating_mul(3).saturating_mul(1000) as i64
}

impl ServiceNodeResponse {
    fn from_node(value: ServiceNode, stale_after_ms: i64) -> Self {
        let status = if value.status.as_str() == "running"
            && crate::current_timestamp_as_u64() as i64 - value.last_seen_at > stale_after_ms
        {
            "stale".to_string()
        } else {
            value.status.as_str().to_string()
        };

        Self {
            node_id: value.node_id,
            hostname: value.hostname,
            machine_ip: value.machine_ip,
            ingest_bind_address: value.ingest_bind_address,
            management_bind_address: value.management_bind_address,
            version: value.version,
            status,
            started_at: value.started_at,
            last_seen_at: value.last_seen_at,
            updated_at: value.updated_at,
            metadata_json: value.metadata_json,
        }
    }
}
