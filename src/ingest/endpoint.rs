use crate::services::ProjectRegistryState;
use crate::settings::{default_max_event_bytes, Settings};
use crate::utils::events::EventSinkState;
use crate::utils::get_ip;
use crate::utils::prometheus::{IngestPrometheusMetrics, WalPrometheusMetrics};
use crate::wal::{new_record, WalWriter};
use actix_web::http::Method;
use actix_web::web::{Data, Query};
use actix_web::{web, HttpRequest, HttpResponse};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, warn};

pub async fn ingest(
    req: HttpRequest,
    body: web::Bytes,
    query_params: Query<HashMap<String, String>>,
    project_registry: Data<ProjectRegistryState>,
    event_sinks: Data<EventSinkState>,
    wal: Data<WalWriter>,
    wal_metrics: Option<Data<WalPrometheusMetrics>>,
    ingest_metrics: Option<Data<IngestPrometheusMetrics>>,
    settings: Option<Data<Arc<Settings>>>,
) -> HttpResponse {
    let started = Instant::now();
    let body = match request_payload(&req, body, query_params.into_inner()) {
        Ok(body) => body,
        Err(issue) => return issue.into_response(),
    };
    let project = match authenticate_project(&req, &project_registry) {
        Ok(project) => project,
        Err(issue) => return issue.into_response(),
    };

    if let Some(issue) = reject_if_payload_too_large(
        body.len(),
        settings
            .as_ref()
            .map(|settings| settings.get_ref().as_ref()),
    ) {
        return issue.into_response();
    }

    let payload = match validate_ingest_payload(&body) {
        Ok(payload) => payload,
        Err(issue) => return issue.into_response(),
    };

    append_wal_record(
        &req,
        body,
        project.id,
        payload.appid.as_str(),
        payload.event_name.as_str(),
        &wal,
        &event_sinks.sink_names(),
        settings.as_ref().map(Data::get_ref).map(Arc::as_ref),
        wal_metrics.as_ref().map(Data::get_ref),
        ingest_metrics.as_ref().map(Data::get_ref),
        started,
    )
    .await
}

fn request_payload(
    req: &HttpRequest,
    body: web::Bytes,
    query_params: HashMap<String, String>,
) -> Result<Vec<u8>, IngestIssue> {
    match *req.method() {
        Method::GET => decode_query_payload(&query_params),
        Method::POST => Ok(body.to_vec()),
        _ => Err(IngestIssue::MethodNotAllowed),
    }
}

fn decode_query_payload(query_params: &HashMap<String, String>) -> Result<Vec<u8>, IngestIssue> {
    let Some(data) = query_params.get("data") else {
        return Err(IngestIssue::MissingData);
    };

    match STANDARD.decode(data) {
        Ok(decoded) => Ok(decoded),
        Err(err) => Err(IngestIssue::InvalidBase64 {
            message: err.to_string(),
        }),
    }
}

struct ValidatedIngestPayload {
    appid: String,
    event_name: String,
}

fn authenticate_project(
    req: &HttpRequest,
    project_registry: &ProjectRegistryState,
) -> Result<crate::repositories::Project, IngestIssue> {
    let Some(token) = ingest_token_from_request(req) else {
        return Err(IngestIssue::MissingIngestToken);
    };
    project_registry
        .authenticate(token)
        .ok_or(IngestIssue::InvalidIngestToken)
}

fn ingest_token_from_request(req: &HttpRequest) -> Option<&str> {
    if let Some(value) = req.headers().get("x-ingest-token") {
        return value
            .to_str()
            .ok()
            .map(str::trim)
            .filter(|value| !value.is_empty());
    }

    let value = req.headers().get("authorization")?.to_str().ok()?.trim();
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn validate_ingest_payload(body: &[u8]) -> Result<ValidatedIngestPayload, IngestIssue> {
    let json = match serde_json::from_slice::<Value>(body) {
        Ok(json) => json,
        Err(err) => {
            return Err(IngestIssue::InvalidJson {
                message: err.to_string(),
            })
        }
    };
    let event_name = json["xwhat"].as_str().unwrap_or("default").to_string();

    let Some(appid) = json
        .get("appid")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        warn!(xwhat = event_name.as_str(), "missing or invalid appid");
        return Err(IngestIssue::MissingAppid);
    };

    Ok(ValidatedIngestPayload { appid, event_name })
}

fn reject_if_payload_too_large(
    payload_len: usize,
    settings: Option<&Settings>,
) -> Option<IngestIssue> {
    let max_event_bytes = settings
        .map(|settings| settings.ingest.max_event_bytes)
        .unwrap_or_else(default_max_event_bytes);
    if payload_len > max_event_bytes {
        return Some(IngestIssue::PayloadTooLarge);
    }
    None
}

enum IngestIssue {
    // Internal `/ingest` error codes are stable for logs/metrics; HTTP responses
    // intentionally keep the current compatibility surface.
    MethodNotAllowed,
    MissingData,
    MissingIngestToken,
    InvalidIngestToken,
    InvalidBase64 { message: String },
    PayloadTooLarge,
    InvalidJson { message: String },
    MissingAppid,
    WalAppendFailed,
}

impl IngestIssue {
    fn code(&self) -> &'static str {
        match self {
            Self::MethodNotAllowed => "ingest_method_not_allowed",
            Self::MissingData => "ingest_missing_data",
            Self::MissingIngestToken => "ingest_missing_token",
            Self::InvalidIngestToken => "ingest_invalid_token",
            Self::InvalidBase64 { .. } => "ingest_invalid_base64",
            Self::PayloadTooLarge => "ingest_payload_too_large",
            Self::InvalidJson { .. } => "ingest_invalid_json",
            Self::MissingAppid => "ingest_missing_appid",
            Self::WalAppendFailed => "ingest_wal_append_failed",
        }
    }

    fn into_response(self) -> HttpResponse {
        match self {
            Self::MethodNotAllowed => HttpResponse::MethodNotAllowed().finish(),
            Self::MissingData => HttpResponse::BadRequest().body("missing query param: data"),
            Self::MissingIngestToken => HttpResponse::Unauthorized().body("missing ingest token"),
            Self::InvalidIngestToken => HttpResponse::Unauthorized().body("invalid ingest token"),
            Self::InvalidBase64 { message } => {
                HttpResponse::BadRequest().body(format!("invalid base64 data: {message}"))
            }
            Self::PayloadTooLarge => HttpResponse::PayloadTooLarge().body("Payload Too Large"),
            Self::InvalidJson { message } => {
                HttpResponse::BadRequest().body(format!("invalid json payload: {message}"))
            }
            Self::MissingAppid => HttpResponse::BadRequest().body("missing or invalid appid"),
            Self::WalAppendFailed => HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "error": "wal_capacity_exceeded",
                "message": "WAL disk space is insufficient or unavailable"
            })),
        }
    }
}

async fn append_wal_record(
    req: &HttpRequest,
    body: Vec<u8>,
    project_id: i32,
    appid: &str,
    event_name: &str,
    wal: &WalWriter,
    active_sink_names: &[String],
    settings: Option<&Settings>,
    wal_metrics: Option<&WalPrometheusMetrics>,
    ingest_metrics: Option<&IngestPrometheusMetrics>,
    started: Instant,
) -> HttpResponse {
    let record = new_record(
        req.method().as_str(),
        req.path(),
        req.uri().query().map(ToString::to_string),
        get_ip(req),
        request_headers(req),
        project_id,
        body,
    );

    match wal.append_async(record).await {
        Ok(_) => {
            if let (Some(metrics), Some(settings)) = (wal_metrics, settings) {
                metrics.observe(settings, wal, active_sink_names);
            }
            observe_ingest_event(ingest_metrics, appid, event_name, "wal_appended", started);
            HttpResponse::Ok().body("200")
        }
        Err(err) => {
            let issue = IngestIssue::WalAppendFailed;
            if let Some(metrics) = wal_metrics {
                metrics.inc_append_errors();
                if let Some(settings) = settings {
                    metrics.observe(settings, wal, active_sink_names);
                }
            }
            observe_ingest_event(
                ingest_metrics,
                appid,
                event_name,
                "wal_append_error",
                started,
            );
            error!(
                code = issue.code(),
                error = %err,
                "failed to append ingest payload to wal"
            );
            issue.into_response()
        }
    }
}

fn observe_ingest_event(
    metrics: Option<&IngestPrometheusMetrics>,
    appid: &str,
    event_name: &str,
    result: &str,
    started: Instant,
) {
    if let Some(metrics) = metrics {
        metrics.observe_event(appid, event_name, result, started.elapsed().as_secs_f64());
    }
}

fn request_headers(req: &HttpRequest) -> BTreeMap<String, String> {
    req.headers()
        .iter()
        .filter(|(name, _)| {
            !matches!(
                name.as_str().to_ascii_lowercase().as_str(),
                "authorization" | "x-ingest-token"
            )
        })
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::IngestIssue;
    use actix_http::StatusCode;

    #[test]
    fn ingest_issue_codes_are_stable_while_responses_stay_compatible() {
        let issue = IngestIssue::MissingAppid;
        assert_eq!(issue.code(), "ingest_missing_appid");

        let response = issue.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
