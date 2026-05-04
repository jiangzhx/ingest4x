use crate::ingest::processor::{ProcessorOutput, ProcessorRequestContext, ProcessorState};
use crate::repositories::RuleRepository;
use crate::services::ProjectRegistryState;
use crate::settings::{default_max_event_bytes, Settings};
use crate::utils::events::{EventSinkState, EventStatus};
use crate::utils::get_ip;
use crate::wal::{new_record, WalWriter};
use actix_web::http::Method;
use actix_web::web::{Data, Query};
use actix_web::{web, HttpRequest, HttpResponse};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::{error, warn};

pub async fn ingest(
    req: HttpRequest,
    body: web::Bytes,
    query_params: Query<HashMap<String, String>>,
    project_registry: Data<ProjectRegistryState>,
    event_sinks: Data<EventSinkState>,
    rule_repository: Data<RuleRepository>,
    processor: Data<ProcessorState>,
    wal: Option<Data<WalWriter>>,
    settings: Option<Data<Arc<Settings>>>,
) -> HttpResponse {
    let body = match request_payload(&req, body, query_params.into_inner()) {
        Ok(body) => body,
        Err(response) => return response,
    };

    if let Some(response) = reject_if_payload_too_large(
        body.len(),
        settings
            .as_ref()
            .map(|settings| settings.get_ref().as_ref()),
    ) {
        return response;
    }

    let payload = match validate_ingest_payload(&body, &project_registry) {
        Ok(payload) => payload,
        Err(response) => return response,
    };

    if let Some(wal) = wal {
        return append_wal_record(&req, body, &wal).await;
    }

    process_ingest_payload(
        payload,
        event_sinks,
        rule_repository,
        processor,
        processor_request_context(&req),
    )
    .await
}

fn request_payload(
    req: &HttpRequest,
    body: web::Bytes,
    query_params: HashMap<String, String>,
) -> Result<Vec<u8>, HttpResponse> {
    match *req.method() {
        Method::GET => decode_query_payload(&query_params),
        Method::POST => Ok(body.to_vec()),
        _ => Err(HttpResponse::MethodNotAllowed().finish()),
    }
}

fn decode_query_payload(query_params: &HashMap<String, String>) -> Result<Vec<u8>, HttpResponse> {
    let Some(data) = query_params.get("data") else {
        return Err(HttpResponse::BadRequest().body("missing query param: data"));
    };

    match STANDARD.decode(data) {
        Ok(decoded) => Ok(decoded),
        Err(err) => Err(HttpResponse::BadRequest().body(format!("invalid base64 data: {err}"))),
    }
}

struct ValidatedIngestPayload {
    json: Value,
    appid: String,
    event_name: String,
}

fn validate_ingest_payload(
    body: &[u8],
    project_registry: &ProjectRegistryState,
) -> Result<ValidatedIngestPayload, HttpResponse> {
    let json = match serde_json::from_slice::<Value>(body) {
        Ok(json) => json,
        Err(err) => {
            return Err(HttpResponse::BadRequest().body(format!("invalid json payload: {err}")))
        }
    };
    let event_name = json["xwhat"].as_str().unwrap_or("default").to_string();

    let Some(appid) = json
        .get("appid")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        warn!(xwhat = event_name.as_str(), "missing or invalid appid");
        return Err(HttpResponse::BadRequest().body("missing or invalid appid"));
    };

    if !project_registry.contains(&appid) {
        warn!(
            appid = appid.as_str(),
            xwhat = event_name.as_str(),
            "project not found"
        );
        return Err(HttpResponse::NotFound().body("Project not found"));
    }

    Ok(ValidatedIngestPayload {
        json,
        appid,
        event_name,
    })
}

fn reject_if_payload_too_large(
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

async fn process_ingest_payload(
    payload: ValidatedIngestPayload,
    event_sinks: Data<EventSinkState>,
    rule_repository: Data<RuleRepository>,
    processor: Data<ProcessorState>,
    request_context: ProcessorRequestContext,
) -> HttpResponse {
    let ValidatedIngestPayload {
        json,
        appid,
        event_name,
    } = payload;

    let rules = match rule_repository.compile_project_rules(&appid).await {
        Ok(rules) => rules,
        Err(err) => {
            error!(
                appid = appid.as_str(),
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
                appid = appid.as_str(),
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
                appid = appid.as_str(),
                xwhat = event_name.as_str(),
                error = %error,
                "ingest payload rejected by processor"
            );
            if let Err(sink_err) = event_sinks
                .send_json(EventStatus::Invalid, &appid, event_name.as_str(), &event)
                .await
            {
                warn!(
                    appid = appid.as_str(),
                    xwhat = event_name.as_str(),
                    error = %sink_err,
                    "failed to send rejected ingest payload to event sinks"
                );
            }
            return HttpResponse::BadRequest().body(error);
        }
    };

    match event_sinks
        .send_json(EventStatus::Valid, &appid, event_name.as_str(), &json)
        .await
    {
        Ok(_) => HttpResponse::Ok().body("200"),
        Err(err) => {
            error!(
                appid = appid.as_str(),
                xwhat = event_name.as_str(),
                error = %err,
                "failed to send event to sinks"
            );
            HttpResponse::InternalServerError().body(err.to_string())
        }
    }
}

fn processor_request_context(req: &HttpRequest) -> ProcessorRequestContext {
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

async fn append_wal_record(req: &HttpRequest, body: Vec<u8>, wal: &WalWriter) -> HttpResponse {
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
            HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "error": "wal_capacity_exceeded",
                "message": "WAL disk space is insufficient or unavailable"
            }))
        }
    }
}

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
