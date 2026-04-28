#[cfg(feature = "ingest")]
use crate::event::Event;
#[cfg(feature = "ingest")]
use crate::ingest::normalize::normalize_ingest_event;
#[cfg(feature = "ingest")]
use crate::projects::ProjectRegistryState;
#[cfg(feature = "ingest")]
use crate::rules::RuleRepository;
#[cfg(feature = "ingest")]
use crate::utils::events::{EventSinkState, EventStatus};
#[cfg(feature = "ingest")]
use actix_web::web::Data;
#[cfg(feature = "ingest")]
use actix_web::{web, HttpRequest, HttpResponse};
#[cfg(feature = "ingest")]
use serde_json::Value;
#[cfg(feature = "ingest")]
use tracing::{error, warn};

#[cfg(feature = "ingest")]
pub async fn post_ingest(
    req: HttpRequest,
    data: web::Json<Value>,
    project_registry: Data<ProjectRegistryState>,
    event_sinks: Data<EventSinkState>,
    rule_repository: Data<RuleRepository>,
) -> HttpResponse {
    let json = data.into_inner();
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

    if rules.event(&event_name).is_none()
        && matches!(
            rule_repository
                .enabled_rule_exists_for_xwhat(&event_name)
                .await,
            Ok(true)
        )
    {
        warn!(
            appid = event.appid(),
            xwhat = event_name.as_str(),
            "rule not enabled for project"
        );
        return HttpResponse::BadRequest().body("rule not enabled for project");
    }

    if !rules.can_validate(&event_name) {
        warn!(
            appid = event.appid(),
            xwhat = event_name.as_str(),
            "unknown rule for xwhat"
        );
        return HttpResponse::BadRequest().body("unknown rule for xwhat");
    }

    if let Err(err) = rules.validate(&event_name, &original_json) {
        warn!(
            appid = event.appid(),
            xwhat = event_name.as_str(),
            error = %err,
            "ingest payload failed validation"
        );
        if let Err(sink_err) = event_sinks
            .send_json(
                EventStatus::Invalid,
                event.appid(),
                event_name.as_str(),
                &original_json,
            )
            .await
        {
            warn!(
                appid = event.appid(),
                xwhat = event_name.as_str(),
                error = %sink_err,
                "failed to send invalid ingest payload to event sinks"
            );
        }

        return HttpResponse::BadRequest().body(err.to_string());
    }

    if let Err(err) = normalize_ingest_event(&mut event, &req) {
        error!(
            appid = event.appid(),
            xwhat = event_name.as_str(),
            error = %err,
            "failed to normalize ingest payload"
        );
        return HttpResponse::InternalServerError().body("Failed to normalize event");
    }

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
pub(crate) async fn process_ingest_payload(
    json: Value,
    project_registry: Data<ProjectRegistryState>,
    event_sinks: Data<EventSinkState>,
    rule_repository: Data<RuleRepository>,
) -> HttpResponse {
    let event_name = json["xwhat"].as_str().unwrap_or("default");

    let Some(appid) = json.get("appid").and_then(Value::as_str) else {
        warn!(xwhat = event_name, "missing or invalid appid");
        return HttpResponse::BadRequest().body("missing or invalid appid");
    };

    if !project_registry.contains(appid) {
        warn!(appid, xwhat = event_name, "project not found");
        return HttpResponse::NotFound().body("Project not found");
    }

    let rules = match rule_repository.compile_project_rules(appid).await {
        Ok(rules) => rules,
        Err(err) => {
            error!(
                appid,
                xwhat = event_name,
                error = %err,
                "failed to compile project rules"
            );
            return HttpResponse::InternalServerError().body(err.to_string());
        }
    };

    if rules.event(event_name).is_none()
        && matches!(
            rule_repository
                .enabled_rule_exists_for_xwhat(event_name)
                .await,
            Ok(true)
        )
    {
        warn!(appid, xwhat = event_name, "rule not enabled for project");
        return HttpResponse::BadRequest().body("rule not enabled for project");
    }

    if !rules.can_validate(event_name) {
        warn!(appid, xwhat = event_name, "unknown rule for xwhat");
        return HttpResponse::BadRequest().body("unknown rule for xwhat");
    }

    if let Err(err) = rules.validate(event_name, &json) {
        warn!(
            appid,
            xwhat = event_name,
            error = %err,
            "ingest payload failed validation"
        );
        if let Err(sink_err) = event_sinks
            .send_json(EventStatus::Invalid, appid, event_name, &json)
            .await
        {
            warn!(
                appid,
                xwhat = event_name,
                error = %sink_err,
                "failed to send invalid ingest payload to event sinks"
            );
        }
        return HttpResponse::BadRequest().body(err.to_string());
    }

    match event_sinks
        .send_json(EventStatus::Valid, appid, event_name, &json)
        .await
    {
        Ok(_) => HttpResponse::Ok().body("200"),
        Err(err) => {
            error!(
                appid,
                xwhat = event_name,
                error = %err,
                "failed to send event to sinks"
            );
            HttpResponse::InternalServerError().body(err.to_string())
        }
    }
}
