use crate::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, DeliveryTarget, DeliveryTargetType, EventSink,
    EventSinkRepository, EventSinkRepositoryError, UpdateDeliveryTargetInput, UpdateEventSinkInput,
};
use crate::settings::AutoOffsetReset;
use crate::utils::events::EventSinkState;
use actix_web::web::{self, Data, Json, Path, ServiceConfig};
use actix_web::HttpResponse;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{OpenApi, ToSchema};

#[derive(Debug, Deserialize, ToSchema)]
struct CreateDeliveryTargetRequest {
    target_id: String,
    name: String,
    target_type: String,
    #[schema(value_type = Object)]
    config_json: Value,
    enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateDeliveryTargetRequest {
    name: Option<String>,
    #[schema(value_type = Object)]
    config_json: Option<Value>,
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateEventSinkRequest {
    sink_id: String,
    name: String,
    delivery_target_id: i32,
    #[schema(value_type = Object)]
    destination_json: Value,
    auto_offset_reset: String,
    enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateEventSinkRequest {
    name: Option<String>,
    delivery_target_id: Option<i32>,
    #[schema(value_type = Object)]
    destination_json: Option<Value>,
    auto_offset_reset: Option<String>,
    enabled: Option<bool>,
}

#[derive(Debug, Serialize, PartialEq, Eq, ToSchema)]
struct DeliveryTargetResponse {
    id: i32,
    target_id: String,
    name: String,
    target_type: String,
    #[schema(value_type = Object)]
    config_json: Value,
    enabled: bool,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Serialize, PartialEq, Eq, ToSchema)]
struct EventSinkResponse {
    id: i32,
    sink_id: String,
    name: String,
    delivery_target_id: i32,
    #[schema(value_type = Object)]
    destination_json: Value,
    auto_offset_reset: String,
    enabled: bool,
    created_at: i64,
    updated_at: i64,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        list_delivery_targets,
        create_delivery_target,
        update_delivery_target,
        delete_delivery_target,
        list_event_sinks,
        create_event_sink,
        update_event_sink,
        delete_event_sink
    ),
    components(
        schemas(
            CreateDeliveryTargetRequest,
            UpdateDeliveryTargetRequest,
            CreateEventSinkRequest,
            UpdateEventSinkRequest,
            DeliveryTargetResponse,
            EventSinkResponse
        )
    ),
    tags(
        (name = "admin.event_sinks", description = "Admin event sink endpoints")
    )
)]
pub struct AdminApiDoc;

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.route("/delivery-targets", web::get().to(list_delivery_targets))
        .route("/delivery-targets", web::post().to(create_delivery_target))
        .route(
            "/delivery-targets/{delivery_target_id}",
            web::put().to(update_delivery_target),
        )
        .route(
            "/delivery-targets/{delivery_target_id}",
            web::delete().to(delete_delivery_target),
        )
        .route("/event-sinks", web::get().to(list_event_sinks))
        .route("/event-sinks", web::post().to(create_event_sink))
        .route(
            "/event-sinks/{event_sink_id}",
            web::put().to(update_event_sink),
        )
        .route(
            "/event-sinks/{event_sink_id}",
            web::delete().to(delete_event_sink),
        );
}

#[utoipa::path(
    get,
    path = "/api/admin/delivery-targets",
    tag = "admin.event_sinks",
    responses(
        (status = 200, description = "List delivery targets", body = [DeliveryTargetResponse]),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn list_delivery_targets(repository: Data<EventSinkRepository>) -> HttpResponse {
    match repository.list_delivery_targets().await {
        Ok(targets) => HttpResponse::Ok().json(
            targets
                .into_iter()
                .map(DeliveryTargetResponse::from)
                .collect::<Vec<_>>(),
        ),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/api/admin/delivery-targets",
    tag = "admin.event_sinks",
    request_body = CreateDeliveryTargetRequest,
    responses(
        (status = 201, description = "Delivery target created", body = DeliveryTargetResponse),
        (status = 400, description = "Invalid JSON payload or config", body = String),
        (status = 409, description = "Delivery target already exists", body = String),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn create_delivery_target(
    repository: Data<EventSinkRepository>,
    request: Json<CreateDeliveryTargetRequest>,
) -> HttpResponse {
    match CreateDeliveryTargetInput::try_from(request.into_inner()) {
        Ok(input) => match repository.create_delivery_target(input).await {
            Ok(target) => HttpResponse::Created().json(DeliveryTargetResponse::from(target)),
            Err(error) => map_repository_error(error),
        },
        Err(error) => HttpResponse::BadRequest().body(error),
    }
}

#[utoipa::path(
    put,
    path = "/api/admin/delivery-targets/{delivery_target_id}",
    tag = "admin.event_sinks",
    params(
        ("delivery_target_id" = i32, Path, description = "Delivery target database id")
    ),
    request_body = UpdateDeliveryTargetRequest,
    responses(
        (status = 200, description = "Delivery target updated", body = DeliveryTargetResponse),
        (status = 400, description = "Invalid JSON payload or config", body = String),
        (status = 404, description = "Delivery target not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn update_delivery_target(
    delivery_target_id: Path<i32>,
    repository: Data<EventSinkRepository>,
    event_sinks: Data<EventSinkState>,
    request: Json<UpdateDeliveryTargetRequest>,
) -> HttpResponse {
    match repository
        .update_delivery_target(
            *delivery_target_id,
            UpdateDeliveryTargetInput::from(request.into_inner()),
        )
        .await
    {
        Ok(target) => finalize_event_sink_response(
            HttpResponse::Ok().json(DeliveryTargetResponse::from(target)),
            event_sinks.refresh_if_needed().await,
            "update_delivery_target",
            &delivery_target_id.to_string(),
        ),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    delete,
    path = "/api/admin/delivery-targets/{delivery_target_id}",
    tag = "admin.event_sinks",
    params(
        ("delivery_target_id" = i32, Path, description = "Delivery target database id")
    ),
    responses(
        (status = 204, description = "Delivery target deleted"),
        (status = 404, description = "Delivery target not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn delete_delivery_target(
    delivery_target_id: Path<i32>,
    repository: Data<EventSinkRepository>,
    event_sinks: Data<EventSinkState>,
) -> HttpResponse {
    match repository.delete_delivery_target(*delivery_target_id).await {
        Ok(()) => finalize_event_sink_response(
            HttpResponse::NoContent().finish(),
            event_sinks.refresh_if_needed().await,
            "delete_delivery_target",
            &delivery_target_id.to_string(),
        ),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/event-sinks",
    tag = "admin.event_sinks",
    responses(
        (status = 200, description = "List event sinks", body = [EventSinkResponse]),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn list_event_sinks(repository: Data<EventSinkRepository>) -> HttpResponse {
    match repository.list_event_sinks().await {
        Ok(sinks) => HttpResponse::Ok().json(
            sinks
                .into_iter()
                .map(EventSinkResponse::from)
                .collect::<Vec<_>>(),
        ),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/api/admin/event-sinks",
    tag = "admin.event_sinks",
    request_body = CreateEventSinkRequest,
    responses(
        (status = 201, description = "Event sink created", body = EventSinkResponse),
        (status = 400, description = "Invalid JSON payload or config", body = String),
        (status = 404, description = "Delivery target not found"),
        (status = 409, description = "Event sink already exists", body = String),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn create_event_sink(
    repository: Data<EventSinkRepository>,
    event_sinks: Data<EventSinkState>,
    request: Json<CreateEventSinkRequest>,
) -> HttpResponse {
    match CreateEventSinkInput::try_from(request.into_inner()) {
        Ok(input) => match repository.create_event_sink(input).await {
            Ok(sink) => {
                let sink_id = sink.sink_id.clone();
                finalize_event_sink_response(
                    HttpResponse::Created().json(EventSinkResponse::from(sink)),
                    event_sinks.refresh_if_needed().await,
                    "create",
                    &sink_id,
                )
            }
            Err(error) => map_repository_error(error),
        },
        Err(error) => HttpResponse::BadRequest().body(error),
    }
}

#[utoipa::path(
    put,
    path = "/api/admin/event-sinks/{event_sink_id}",
    tag = "admin.event_sinks",
    params(
        ("event_sink_id" = i32, Path, description = "Event sink database id")
    ),
    request_body = UpdateEventSinkRequest,
    responses(
        (status = 200, description = "Event sink updated", body = EventSinkResponse),
        (status = 400, description = "Invalid JSON payload or config", body = String),
        (status = 404, description = "Event sink not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn update_event_sink(
    event_sink_id: Path<i32>,
    repository: Data<EventSinkRepository>,
    event_sinks: Data<EventSinkState>,
    request: Json<UpdateEventSinkRequest>,
) -> HttpResponse {
    match UpdateEventSinkInput::try_from(request.into_inner()) {
        Ok(input) => match repository.update_event_sink(*event_sink_id, input).await {
            Ok(sink) => {
                let sink_id = sink.sink_id.clone();
                finalize_event_sink_response(
                    HttpResponse::Ok().json(EventSinkResponse::from(sink)),
                    event_sinks.refresh_if_needed().await,
                    "update",
                    &sink_id,
                )
            }
            Err(error) => map_repository_error(error),
        },
        Err(error) => HttpResponse::BadRequest().body(error),
    }
}

#[utoipa::path(
    delete,
    path = "/api/admin/event-sinks/{event_sink_id}",
    tag = "admin.event_sinks",
    params(
        ("event_sink_id" = i32, Path, description = "Event sink database id")
    ),
    responses(
        (status = 204, description = "Event sink deleted"),
        (status = 404, description = "Event sink not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn delete_event_sink(
    event_sink_id: Path<i32>,
    repository: Data<EventSinkRepository>,
    event_sinks: Data<EventSinkState>,
) -> HttpResponse {
    match repository.delete_event_sink(*event_sink_id).await {
        Ok(()) => finalize_event_sink_response(
            HttpResponse::NoContent().finish(),
            event_sinks.refresh_if_needed().await,
            "delete",
            &event_sink_id.to_string(),
        ),
        Err(error) => map_repository_error(error),
    }
}

fn finalize_event_sink_response(
    response: HttpResponse,
    refresh_result: anyhow::Result<bool>,
    operation: &str,
    sink_id: &str,
) -> HttpResponse {
    if let Err(error) = refresh_result {
        warn!("event sink router refresh failed after {operation} for '{sink_id}': {error}");
    }

    response
}

fn map_repository_error(error: EventSinkRepositoryError) -> HttpResponse {
    match error {
        EventSinkRepositoryError::DeliveryTargetNotFound { .. }
        | EventSinkRepositoryError::EventSinkNotFound { .. } => HttpResponse::NotFound().finish(),
        EventSinkRepositoryError::DeliveryTargetInUse { .. }
        | EventSinkRepositoryError::DuplicateDeliveryTarget { .. }
        | EventSinkRepositoryError::DuplicateEventSink { .. } => {
            HttpResponse::Conflict().body(error.to_string())
        }
        EventSinkRepositoryError::InvalidConfig { .. } => {
            HttpResponse::BadRequest().body(error.to_string())
        }
        _ => HttpResponse::InternalServerError().body(error.to_string()),
    }
}

fn parse_delivery_target_type(value: &str) -> Result<DeliveryTargetType, String> {
    match value {
        "kafka" => Ok(DeliveryTargetType::Kafka),
        "stdout" => Ok(DeliveryTargetType::Stdout),
        _ => Err(format!("unknown delivery target type `{value}`")),
    }
}

fn parse_auto_offset_reset(value: &str) -> Result<AutoOffsetReset, String> {
    match value {
        "earliest" => Ok(AutoOffsetReset::Earliest),
        "latest" => Ok(AutoOffsetReset::Latest),
        _ => Err(format!("unknown auto_offset_reset `{value}`")),
    }
}

fn delivery_target_type_as_str(value: DeliveryTargetType) -> &'static str {
    match value {
        DeliveryTargetType::Kafka => "kafka",
        DeliveryTargetType::Stdout => "stdout",
    }
}

fn auto_offset_reset_as_str(value: AutoOffsetReset) -> &'static str {
    match value {
        AutoOffsetReset::Earliest => "earliest",
        AutoOffsetReset::Latest => "latest",
    }
}

impl TryFrom<CreateDeliveryTargetRequest> for CreateDeliveryTargetInput {
    type Error = String;

    fn try_from(value: CreateDeliveryTargetRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            target_id: value.target_id,
            name: value.name,
            target_type: parse_delivery_target_type(&value.target_type)?,
            config_json: value.config_json,
            enabled: value.enabled,
        })
    }
}

impl From<UpdateDeliveryTargetRequest> for UpdateDeliveryTargetInput {
    fn from(value: UpdateDeliveryTargetRequest) -> Self {
        Self {
            name: value.name,
            config_json: value.config_json,
            enabled: value.enabled,
        }
    }
}

impl TryFrom<CreateEventSinkRequest> for CreateEventSinkInput {
    type Error = String;

    fn try_from(value: CreateEventSinkRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            sink_id: value.sink_id,
            name: value.name,
            delivery_target_id: value.delivery_target_id,
            destination_json: value.destination_json,
            auto_offset_reset: parse_auto_offset_reset(&value.auto_offset_reset)?,
            enabled: value.enabled,
        })
    }
}

impl TryFrom<UpdateEventSinkRequest> for UpdateEventSinkInput {
    type Error = String;

    fn try_from(value: UpdateEventSinkRequest) -> Result<Self, Self::Error> {
        let auto_offset_reset = value
            .auto_offset_reset
            .as_deref()
            .map(parse_auto_offset_reset)
            .transpose()?;

        Ok(Self {
            name: value.name,
            delivery_target_id: value.delivery_target_id,
            destination_json: value.destination_json,
            auto_offset_reset,
            enabled: value.enabled,
        })
    }
}

impl From<DeliveryTarget> for DeliveryTargetResponse {
    fn from(value: DeliveryTarget) -> Self {
        Self {
            id: value.id,
            target_id: value.target_id,
            name: value.name,
            target_type: delivery_target_type_as_str(value.target_type).to_string(),
            config_json: value.config_json,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<EventSink> for EventSinkResponse {
    fn from(value: EventSink) -> Self {
        Self {
            id: value.id,
            sink_id: value.sink_id,
            name: value.name,
            delivery_target_id: value.delivery_target_id,
            destination_json: value.destination_json,
            auto_offset_reset: auto_offset_reset_as_str(value.auto_offset_reset).to_string(),
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}
