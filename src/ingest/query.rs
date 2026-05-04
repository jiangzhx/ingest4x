#[cfg(feature = "ingest")]
use crate::ingest::json::{append_wal_record, process_ingest_payload, processor_request_context};
#[cfg(feature = "ingest")]
use crate::ingest::processor::ProcessorState;
#[cfg(feature = "ingest")]
use crate::projects::ProjectRegistryState;
#[cfg(feature = "ingest")]
use crate::rules::RuleRepository;
#[cfg(feature = "ingest")]
use crate::utils::events::EventSinkState;
#[cfg(feature = "ingest")]
use crate::wal::WalWriter;
#[cfg(feature = "ingest")]
use actix_web::web::{Data, Query};
#[cfg(feature = "ingest")]
use actix_web::{HttpRequest, HttpResponse};
#[cfg(feature = "ingest")]
use base64::engine::general_purpose::STANDARD;
#[cfg(feature = "ingest")]
use base64::Engine;
#[cfg(feature = "ingest")]
use serde_json::Value;
#[cfg(feature = "ingest")]
use std::collections::HashMap;

#[cfg(feature = "ingest")]
pub async fn get_ingest(
    req: HttpRequest,
    query_params: Query<HashMap<String, String>>,
    project_registry: Data<ProjectRegistryState>,
    event_sinks: Data<EventSinkState>,
    rule_repository: Data<RuleRepository>,
    processor: Data<ProcessorState>,
    wal: Option<Data<WalWriter>>,
) -> HttpResponse {
    let query_params = query_params.into_inner();

    let Some(data) = query_params.get("data") else {
        return HttpResponse::BadRequest().body("missing query param: data");
    };

    let decoded = match STANDARD.decode(data) {
        Ok(decoded) => decoded,
        Err(err) => return HttpResponse::BadRequest().body(format!("invalid base64 data: {err}")),
    };

    if let Some(wal) = wal {
        return append_wal_record(&req, decoded, &project_registry, &wal).await;
    }

    let json = match serde_json::from_slice::<Value>(&decoded) {
        Ok(json) => json,
        Err(err) => return HttpResponse::BadRequest().body(format!("invalid json payload: {err}")),
    };

    process_ingest_payload(
        json,
        project_registry,
        event_sinks,
        rule_repository,
        processor,
        processor_request_context(&req),
    )
    .await
}
