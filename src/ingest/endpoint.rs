use crate::repositories::{Project, ProjectAuthMode};
use crate::services::ProjectRegistryState;
use crate::settings::{default_max_event_bytes, Settings};
use crate::sinks::EventSinkState;
use crate::utils::get_ip;
use crate::utils::prometheus::{IngestPrometheusMetrics, WalPrometheusMetrics};
use crate::wal::{new_record, WalWriter};
use actix_web::http::header::CONTENT_TYPE;
use actix_web::http::Method;
use actix_web::web::{Data, Query};
use actix_web::{web, HttpRequest, HttpResponse};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;
use tracing::error;

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
    let parsed = match request_payload(&req, body, query_params.into_inner()) {
        Ok(parsed) => parsed,
        Err(issue) => return issue.into_response(),
    };
    let project = match authenticate_project(&req, &parsed, &project_registry) {
        Ok(project) => project,
        Err(issue) => return issue.into_response(),
    };

    if let Some(issue) = reject_if_payload_too_large(
        parsed.body.len(),
        settings
            .as_ref()
            .map(|settings| settings.get_ref().as_ref()),
    ) {
        return issue.into_response();
    }

    let payload = match validate_ingest_payload(&parsed.body) {
        Ok(payload) => payload,
        Err(issue) => return issue.into_response(),
    };

    append_wal_record(
        &req,
        parsed.body,
        project.id,
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

struct ValidatedIngestPayload {
    event_name: String,
}

struct ParsedIngestRequest {
    body: Vec<u8>,
    body_token: Option<String>,
}

fn request_payload(
    req: &HttpRequest,
    body: web::Bytes,
    query_params: HashMap<String, String>,
) -> Result<ParsedIngestRequest, IngestIssue> {
    match *req.method() {
        Method::GET => decode_query_payload(&query_params),
        Method::POST => decode_post_payload(req, body.as_ref()),
        _ => Err(IngestIssue::MethodNotAllowed),
    }
}

fn decode_post_payload(req: &HttpRequest, body: &[u8]) -> Result<ParsedIngestRequest, IngestIssue> {
    if body.is_empty() {
        return Err(IngestIssue::MissingRequestBody);
    }

    if is_form_urlencoded(req) {
        return decode_form_payload(body);
    }

    decode_json_payload(body)
}

fn decode_json_payload(body: &[u8]) -> Result<ParsedIngestRequest, IngestIssue> {
    let mut json = parse_json(body)?;
    let body_token = json
        .as_object_mut()
        .and_then(|object| remove_string_field(object, "x-ingest-token"));

    Ok(ParsedIngestRequest {
        body: serde_json::to_vec(&json).map_err(|err| IngestIssue::InvalidJson {
            message: err.to_string(),
        })?,
        body_token,
    })
}

fn decode_form_payload(body: &[u8]) -> Result<ParsedIngestRequest, IngestIssue> {
    let pairs = serde_urlencoded::from_bytes::<Vec<(String, String)>>(body).map_err(|err| {
        IngestIssue::InvalidForm {
            message: err.to_string(),
        }
    })?;
    let mut fields = BTreeMap::new();
    let mut body_token = None;

    for (key, value) in pairs {
        if key == "x-ingest-token" {
            body_token = non_empty_token(value);
        } else {
            fields.insert(key, value);
        }
    }

    Ok(ParsedIngestRequest {
        body: serde_json::to_vec(&fields_to_json_object(fields)).map_err(|err| {
            IngestIssue::InvalidJson {
                message: err.to_string(),
            }
        })?,
        body_token,
    })
}

fn decode_query_payload(
    query_params: &HashMap<String, String>,
) -> Result<ParsedIngestRequest, IngestIssue> {
    let mut fields = BTreeMap::new();
    for (key, value) in query_params {
        if key == "x-ingest-token" {
            return Err(IngestIssue::QueryIngestTokenNotSupported);
        }
        fields.insert(key.to_string(), value.to_string());
    }

    Ok(ParsedIngestRequest {
        body: serde_json::to_vec(&fields_to_json_object(fields)).map_err(|err| {
            IngestIssue::InvalidJson {
                message: err.to_string(),
            }
        })?,
        body_token: None,
    })
}

fn parse_json(body: &[u8]) -> Result<Value, IngestIssue> {
    serde_json::from_slice::<Value>(body).map_err(|err| IngestIssue::InvalidJson {
        message: err.to_string(),
    })
}

fn remove_string_field(object: &mut Map<String, Value>, field: &str) -> Option<String> {
    object
        .remove(field)
        .and_then(|value| value.as_str().map(str::to_string))
        .and_then(non_empty_token)
}

fn non_empty_token(value: String) -> Option<String> {
    let token = value.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn fields_to_json_object(fields: BTreeMap<String, String>) -> Value {
    Value::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key, Value::String(value)))
            .collect(),
    )
}

fn is_form_urlencoded(req: &HttpRequest) -> bool {
    req.headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|content_type| {
            content_type.eq_ignore_ascii_case("application/x-www-form-urlencoded")
        })
}

fn authenticate_project(
    req: &HttpRequest,
    parsed: &ParsedIngestRequest,
    project_registry: &ProjectRegistryState,
) -> Result<crate::repositories::Project, IngestIssue> {
    let project_key = req
        .match_info()
        .get("project_key")
        .ok_or(IngestIssue::ProjectNotFound)?;
    let project = project_registry
        .project_by_key(project_key)
        .ok_or(IngestIssue::ProjectNotFound)?;
    authorize_project(req, parsed, &project)?;
    Ok(project)
}

fn ingest_token_from_request(req: &HttpRequest) -> Option<&str> {
    req.headers()
        .get("x-ingest-token")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn request_ingest_token(
    req: &HttpRequest,
    parsed: &ParsedIngestRequest,
) -> Result<Option<String>, IngestIssue> {
    let header_token = ingest_token_from_request(req).map(str::to_string);
    match (header_token, parsed.body_token.clone()) {
        (Some(header), Some(body)) if header != body => Err(IngestIssue::ConflictingIngestToken),
        (Some(header), _) => Ok(Some(header)),
        (None, Some(body)) => Ok(Some(body)),
        (None, None) => Ok(None),
    }
}

fn authorize_project(
    req: &HttpRequest,
    parsed: &ParsedIngestRequest,
    project: &Project,
) -> Result<(), IngestIssue> {
    if !project.allowed_ips.is_empty() {
        let ip = get_ip(req).unwrap_or_default();
        if !project.allowed_ips.iter().any(|allowed| allowed == &ip) {
            return Err(IngestIssue::IpNotAllowed);
        }
    }

    match project.auth_mode {
        ProjectAuthMode::Token => {
            let Some(token) = request_ingest_token(req, parsed)? else {
                return Err(IngestIssue::MissingIngestToken);
            };
            if token == project.ingest_token {
                Ok(())
            } else {
                Err(IngestIssue::InvalidIngestToken)
            }
        }
        ProjectAuthMode::Public => Ok(()),
    }
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

    Ok(ValidatedIngestPayload { event_name })
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
    ProjectNotFound,
    MissingRequestBody,
    MissingIngestToken,
    InvalidIngestToken,
    ConflictingIngestToken,
    QueryIngestTokenNotSupported,
    IpNotAllowed,
    InvalidForm { message: String },
    PayloadTooLarge,
    InvalidJson { message: String },
    WalAppendFailed,
}

impl IngestIssue {
    fn code(&self) -> &'static str {
        match self {
            Self::MethodNotAllowed => "ingest_method_not_allowed",
            Self::ProjectNotFound => "ingest_project_not_found",
            Self::MissingRequestBody => "ingest_missing_request_body",
            Self::MissingIngestToken => "ingest_missing_token",
            Self::InvalidIngestToken => "ingest_invalid_token",
            Self::ConflictingIngestToken => "ingest_conflicting_token",
            Self::QueryIngestTokenNotSupported => "ingest_query_token_not_supported",
            Self::IpNotAllowed => "ingest_ip_not_allowed",
            Self::InvalidForm { .. } => "ingest_invalid_form",
            Self::PayloadTooLarge => "ingest_payload_too_large",
            Self::InvalidJson { .. } => "ingest_invalid_json",
            Self::WalAppendFailed => "ingest_wal_append_failed",
        }
    }

    fn into_response(self) -> HttpResponse {
        match self {
            Self::MethodNotAllowed => HttpResponse::MethodNotAllowed().finish(),
            Self::ProjectNotFound => HttpResponse::NotFound().body("project not found"),
            Self::MissingRequestBody => HttpResponse::BadRequest().body("missing request body"),
            Self::MissingIngestToken => HttpResponse::Unauthorized().body("missing ingest token"),
            Self::InvalidIngestToken => HttpResponse::Unauthorized().body("invalid ingest token"),
            Self::ConflictingIngestToken => {
                HttpResponse::Unauthorized().body("conflicting ingest token")
            }
            Self::QueryIngestTokenNotSupported => {
                HttpResponse::BadRequest().body("query ingest token is not supported")
            }
            Self::IpNotAllowed => HttpResponse::Forbidden().body("ip not allowed"),
            Self::InvalidForm { message } => {
                HttpResponse::BadRequest().body(format!("invalid form payload: {message}"))
            }
            Self::PayloadTooLarge => HttpResponse::PayloadTooLarge().body("Payload Too Large"),
            Self::InvalidJson { message } => {
                HttpResponse::BadRequest().body(format!("invalid json payload: {message}"))
            }
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
            observe_ingest_event(
                ingest_metrics,
                project_id,
                event_name,
                "wal_appended",
                started,
            );
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
                project_id,
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
    project_id: i32,
    event_name: &str,
    result: &str,
    started: Instant,
) {
    if let Some(metrics) = metrics {
        metrics.observe_event(
            project_id,
            event_name,
            result,
            started.elapsed().as_secs_f64(),
        );
    }
}

fn request_headers(req: &HttpRequest) -> BTreeMap<String, String> {
    req.headers()
        .iter()
        .filter(|(name, _)| {
            !matches!(
                name.as_str().to_ascii_lowercase().as_str(),
                "x-ingest-token"
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
        let issue = IngestIssue::PayloadTooLarge;
        assert_eq!(issue.code(), "ingest_payload_too_large");

        let response = issue.into_response();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
