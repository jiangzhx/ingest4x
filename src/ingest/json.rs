#[cfg(feature = "ingest")]
use crate::event::Event;
#[cfg(feature = "ingest")]
use crate::ingest::processor::{ProcessorOutput, ProcessorRequestContext, ProcessorState};
#[cfg(feature = "ingest")]
use crate::projects::ProjectRegistryState;
#[cfg(feature = "ingest")]
use crate::rules::RuleRepository;
#[cfg(feature = "ingest")]
use crate::settings::{default_max_event_bytes, Settings};
#[cfg(feature = "ingest")]
use crate::utils::events::{EventSinkState, EventStatus};
#[cfg(feature = "ingest")]
use crate::utils::get_ip;
#[cfg(feature = "ingest")]
use crate::wal::{new_record, WalWriter};
#[cfg(feature = "ingest")]
use actix_web::web::Data;
#[cfg(feature = "ingest")]
use actix_web::{web, HttpRequest, HttpResponse};
#[cfg(feature = "ingest")]
use serde_json::Value;
#[cfg(feature = "ingest")]
use std::collections::{BTreeMap, HashMap};
#[cfg(feature = "ingest")]
use std::sync::Arc;
#[cfg(feature = "ingest")]
use tracing::{error, warn};

#[cfg(feature = "ingest")]
pub async fn post_ingest(
    req: HttpRequest,
    body: web::Bytes,
    project_registry: Data<ProjectRegistryState>,
    event_sinks: Data<EventSinkState>,
    rule_repository: Data<RuleRepository>,
    processor: Data<ProcessorState>,
    wal: Option<Data<WalWriter>>,
    settings: Option<Data<Arc<Settings>>>,
) -> HttpResponse {
    if let Some(response) = reject_if_payload_too_large(
        body.len(),
        settings
            .as_ref()
            .map(|settings| settings.get_ref().as_ref()),
    ) {
        return response;
    }

    if let Some(wal) = wal {
        return append_wal_record(
            &req,
            body.to_vec(),
            &project_registry,
            &rule_repository,
            &processor,
            &wal,
        )
        .await;
    }

    let json = match serde_json::from_slice::<Value>(&body) {
        Ok(json) => json,
        Err(err) => return HttpResponse::BadRequest().body(format!("invalid json payload: {err}")),
    };
    let event_name = json["xwhat"].as_str().unwrap_or("default").to_string();
    let original_json = json.clone();
    let mut event = match Event::from_value(json) {
        Ok(event) => event,
        Err(err) => {
            let appid = original_json
                .get("appid")
                .and_then(Value::as_str)
                .unwrap_or("<missing>");
            warn!(
                appid,
                xwhat = event_name.as_str(),
                error = %err,
                "failed to parse ingest payload into event"
            );
            return HttpResponse::BadRequest().body(err.to_string());
        }
    };

    if !project_registry.contains(event.appid()) {
        warn!(
            appid = event.appid(),
            xwhat = event_name.as_str(),
            "project not found"
        );
        return HttpResponse::NotFound().body("Project not found");
    }

    let rules = match rule_repository.compile_project_rules(event.appid()).await {
        Ok(rules) => rules,
        Err(err) => {
            error!(
                appid = event.appid(),
                xwhat = event_name.as_str(),
                error = %err,
                "failed to compile project rules"
            );
            return HttpResponse::InternalServerError().body(err.to_string());
        }
    };

    let request_context = processor_request_context(&req);
    let processed = match processor.process(original_json.clone(), rules, request_context) {
        Ok(output) => output,
        Err(err) => {
            error!(
                appid = event.appid(),
                xwhat = event_name.as_str(),
                error = %err,
                "failed to process ingest payload"
            );
            return HttpResponse::InternalServerError().body("Failed to process event");
        }
    };

    let processed_json = match processed {
        ProcessorOutput::Accepted(value) => value,
        ProcessorOutput::Rejected {
            event: rejected_event,
            error,
        } => {
            warn!(
                appid = event.appid(),
                xwhat = event_name.as_str(),
                error = %error,
                "ingest payload rejected by processor"
            );
            if let Err(sink_err) = event_sinks
                .send_json(
                    EventStatus::Invalid,
                    event.appid(),
                    event_name.as_str(),
                    &rejected_event,
                )
                .await
            {
                warn!(
                    appid = event.appid(),
                    xwhat = event_name.as_str(),
                    error = %sink_err,
                    "failed to send rejected ingest payload to event sinks"
                );
            }
            return HttpResponse::BadRequest().body(error);
        }
        ProcessorOutput::Dropped { reason } => {
            warn!(
                appid = event.appid(),
                xwhat = event_name.as_str(),
                reason = %reason,
                "ingest payload dropped by processor"
            );
            return HttpResponse::Ok().body("200");
        }
    };

    event = match Event::from_value(processed_json) {
        Ok(event) => event,
        Err(err) => {
            error!(
                appid = event.appid(),
                xwhat = event_name.as_str(),
                error = %err,
                "processor returned invalid canonical event"
            );
            return HttpResponse::InternalServerError().body("Processor returned invalid event");
        }
    };

    match event_sinks
        .send_json(
            EventStatus::Valid,
            event.appid(),
            event_name.as_str(),
            &event,
        )
        .await
    {
        Ok(_) => HttpResponse::Ok().body("200"),
        Err(err) => {
            error!(
                appid = event.appid(),
                xwhat = event_name.as_str(),
                error = %err,
                "failed to send event to sinks"
            );
            HttpResponse::InternalServerError().body("Failed to send event")
        }
    }
}

#[cfg(feature = "ingest")]
pub(crate) fn reject_if_payload_too_large(
    payload_len: usize,
    settings: Option<&Settings>,
) -> Option<HttpResponse> {
    let max_event_bytes = settings
        .map(|settings| settings.server.max_event_bytes)
        .unwrap_or_else(default_max_event_bytes);
    if payload_len > max_event_bytes {
        return Some(HttpResponse::PayloadTooLarge().body("Payload Too Large"));
    }
    None
}

#[cfg(feature = "ingest")]
pub(crate) async fn process_ingest_payload(
    json: Value,
    project_registry: Data<ProjectRegistryState>,
    event_sinks: Data<EventSinkState>,
    rule_repository: Data<RuleRepository>,
    processor: Data<ProcessorState>,
    request_context: ProcessorRequestContext,
) -> HttpResponse {
    let event_name = json["xwhat"].as_str().unwrap_or("default").to_string();

    let Some(appid) = json.get("appid").and_then(Value::as_str) else {
        warn!(xwhat = event_name.as_str(), "missing or invalid appid");
        return HttpResponse::BadRequest().body("missing or invalid appid");
    };

    if !project_registry.contains(appid) {
        warn!(appid, xwhat = event_name.as_str(), "project not found");
        return HttpResponse::NotFound().body("Project not found");
    }

    let rules = match rule_repository.compile_project_rules(appid).await {
        Ok(rules) => rules,
        Err(err) => {
            error!(
                appid,
                xwhat = event_name.as_str(),
                error = %err,
                "failed to compile project rules"
            );
            return HttpResponse::InternalServerError().body(err.to_string());
        }
    };

    let processed = match processor.process(json.clone(), rules, request_context) {
        Ok(output) => output,
        Err(err) => {
            error!(
                appid,
                xwhat = event_name.as_str(),
                error = %err,
                "failed to process ingest payload"
            );
            return HttpResponse::InternalServerError().body("Failed to process event");
        }
    };

    let json = match processed {
        ProcessorOutput::Accepted(value) => value,
        ProcessorOutput::Rejected { event, error } => {
            warn!(
                appid,
                xwhat = event_name.as_str(),
                error = %error,
                "ingest payload rejected by processor"
            );
            if let Err(sink_err) = event_sinks
                .send_json(EventStatus::Invalid, appid, event_name.as_str(), &event)
                .await
            {
                warn!(
                    appid,
                    xwhat = event_name.as_str(),
                    error = %sink_err,
                    "failed to send rejected ingest payload to event sinks"
                );
            }
            return HttpResponse::BadRequest().body(error);
        }
        ProcessorOutput::Dropped { reason } => {
            warn!(appid, xwhat = event_name.as_str(), reason = %reason, "ingest payload dropped by processor");
            return HttpResponse::Ok().body("200");
        }
    };

    match event_sinks
        .send_json(EventStatus::Valid, appid, event_name.as_str(), &json)
        .await
    {
        Ok(_) => HttpResponse::Ok().body("200"),
        Err(err) => {
            error!(
                appid,
                xwhat = event_name.as_str(),
                error = %err,
                "failed to send event to sinks"
            );
            HttpResponse::InternalServerError().body(err.to_string())
        }
    }
}

#[cfg(feature = "ingest")]
pub(crate) fn processor_request_context(req: &HttpRequest) -> ProcessorRequestContext {
    let headers = req
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.to_string()))
        })
        .collect::<HashMap<_, _>>();

    ProcessorRequestContext::new(
        get_ip(req),
        req.method().as_str().to_string(),
        req.path().to_string(),
        headers,
    )
}

#[cfg(feature = "ingest")]
pub(crate) async fn append_wal_record(
    req: &HttpRequest,
    body: Vec<u8>,
    project_registry: &ProjectRegistryState,
    rule_repository: &RuleRepository,
    processor: &ProcessorState,
    wal: &WalWriter,
) -> HttpResponse {
    let json = match serde_json::from_slice::<Value>(&body) {
        Ok(json) => json,
        Err(err) => return HttpResponse::BadRequest().body(format!("invalid json payload: {err}")),
    };

    let Some(appid) = json
        .get("appid")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return HttpResponse::BadRequest().body("missing or invalid appid");
    };

    if !project_registry.contains(&appid) {
        return HttpResponse::NotFound().body("Project not found");
    }

    let event_name = json["xwhat"].as_str().unwrap_or("default").to_string();
    let rules = match rule_repository.compile_project_rules(&appid).await {
        Ok(rules) => rules,
        Err(err) => {
            error!(
                appid,
                xwhat = event_name.as_str(),
                error = %err,
                "failed to compile project rules before wal append"
            );
            return HttpResponse::InternalServerError().body(err.to_string());
        }
    };

    let processed = match processor.process(json, rules, processor_request_context(req)) {
        Ok(output) => output,
        Err(err) => {
            error!(
                appid,
                xwhat = event_name.as_str(),
                error = %err,
                "failed to process ingest payload before wal append"
            );
            return HttpResponse::InternalServerError().body("Failed to process event");
        }
    };

    let json = match processed {
        ProcessorOutput::Accepted(value) => value,
        ProcessorOutput::Rejected { error, .. } => {
            warn!(
                appid,
                xwhat = event_name.as_str(),
                error = %error,
                "ingest payload rejected by processor before wal append"
            );
            return HttpResponse::BadRequest().body(error);
        }
        ProcessorOutput::Dropped { reason } => {
            warn!(
                appid,
                xwhat = event_name.as_str(),
                reason = %reason,
                "ingest payload dropped by processor before wal append"
            );
            return HttpResponse::Ok().body("200");
        }
    };

    let body = match serde_json::to_vec(&json) {
        Ok(body) => body,
        Err(err) => {
            error!(
                appid,
                xwhat = event_name.as_str(),
                error = %err,
                "failed to serialize processor output before wal append"
            );
            return HttpResponse::InternalServerError().body("Processor returned invalid event");
        }
    };

    let record = new_record(
        req.method().as_str(),
        req.path(),
        req.uri().query().map(ToString::to_string),
        req.peer_addr().map(|addr| addr.to_string()),
        request_headers(req),
        body,
    );

    match wal.append(&record) {
        Ok(_) => HttpResponse::Ok().body("200"),
        Err(err) => {
            error!(
                error = %err,
                "failed to append ingest payload to wal"
            );
            HttpResponse::ServiceUnavailable().body("Failed to persist event")
        }
    }
}

#[cfg(feature = "ingest")]
fn request_headers(req: &HttpRequest) -> BTreeMap<String, String> {
    req.headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}
