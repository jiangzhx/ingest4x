use crate::ingest::processor::ProcessorRegistryState;
use crate::repositories::{
    CreateProcessorScriptInput, CreateProcessorScriptModuleInput, ProcessorRepository,
    ProcessorRepositoryError, ProcessorScript, ProcessorScriptModule, ProcessorScriptStatus,
    ProjectProcessor, UpdateProcessorScriptStatusInput,
};
use actix_web::web::{self, Data, Json, Path, ServiceConfig};
use actix_web::HttpResponse;
use log::warn;
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

#[derive(Debug, Deserialize, ToSchema)]
struct CreateProcessorScriptRequest {
    script_key: String,
    name: String,
    entry_module: String,
    status: String,
    modules: Vec<CreateProcessorScriptModuleRequest>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateProcessorScriptModuleRequest {
    module_name: String,
    source: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct AssignProjectProcessorRequest {
    processor_script_id: i32,
    enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateProcessorScriptStatusRequest {
    status: String,
}

#[derive(Debug, Serialize, PartialEq, Eq, ToSchema)]
struct ProcessorScriptResponse {
    id: i32,
    script_key: String,
    name: String,
    entry_module: String,
    version: i32,
    status: String,
    checksum: String,
    created_at: i64,
    updated_at: i64,
    activated_at: Option<i64>,
}

#[derive(Debug, Serialize, PartialEq, Eq, ToSchema)]
struct ProcessorScriptDetailResponse {
    #[serde(flatten)]
    script: ProcessorScriptResponse,
    modules: Vec<ProcessorScriptModuleResponse>,
}

#[derive(Debug, Serialize, PartialEq, Eq, ToSchema)]
struct ProcessorScriptModuleResponse {
    id: i32,
    processor_script_id: i32,
    module_name: String,
    source: String,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Serialize, PartialEq, Eq, ToSchema)]
struct ProjectProcessorResponse {
    id: i32,
    appid: String,
    processor_script_id: i32,
    enabled: bool,
    created_at: i64,
    updated_at: i64,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        list_processor_scripts,
        create_processor_script,
        get_processor_script,
        update_processor_script_status,
        list_project_processors,
        assign_project_processor,
        delete_project_processor
    ),
    components(
        schemas(
            CreateProcessorScriptRequest,
            CreateProcessorScriptModuleRequest,
            UpdateProcessorScriptStatusRequest,
            AssignProjectProcessorRequest,
            ProcessorScriptResponse,
            ProcessorScriptDetailResponse,
            ProcessorScriptModuleResponse,
            ProjectProcessorResponse
        )
    ),
    tags(
        (name = "admin.processors", description = "Admin Rhai processor script endpoints")
    )
)]
pub struct AdminApiDoc;

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.route("/processor-scripts", web::get().to(list_processor_scripts))
        .route(
            "/processor-scripts",
            web::post().to(create_processor_script),
        )
        .route(
            "/processor-scripts/{processor_script_id}",
            web::get().to(get_processor_script),
        )
        .route(
            "/processor-scripts/{processor_script_id}/status",
            web::put().to(update_processor_script_status),
        )
        .route(
            "/project-processors",
            web::get().to(list_project_processors),
        )
        .route(
            "/projects/{appid}/processor",
            web::put().to(assign_project_processor),
        )
        .route(
            "/projects/{appid}/processor",
            web::delete().to(delete_project_processor),
        );
}

#[utoipa::path(
    get,
    path = "/api/admin/processor-scripts",
    tag = "admin.processors",
    responses((status = 200, description = "List processor scripts", body = [ProcessorScriptResponse]))
)]
async fn list_processor_scripts(repository: Data<ProcessorRepository>) -> HttpResponse {
    match repository.list_scripts().await {
        Ok(scripts) => HttpResponse::Ok().json(
            scripts
                .into_iter()
                .map(ProcessorScriptResponse::from)
                .collect::<Vec<_>>(),
        ),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/api/admin/processor-scripts",
    tag = "admin.processors",
    request_body = CreateProcessorScriptRequest,
    responses((status = 201, description = "Processor script created", body = ProcessorScriptResponse))
)]
async fn create_processor_script(
    repository: Data<ProcessorRepository>,
    processor: Data<ProcessorRegistryState>,
    request: Json<CreateProcessorScriptRequest>,
) -> HttpResponse {
    match CreateProcessorScriptInput::try_from(request.into_inner()) {
        Ok(input) => match repository.create_script(input).await {
            Ok(script) => finalize_processor_response(
                HttpResponse::Created().json(ProcessorScriptResponse::from(script)),
                processor.refresh_if_needed().await,
                "create_processor_script",
            ),
            Err(error) => map_repository_error(error),
        },
        Err(error) => HttpResponse::BadRequest().body(error),
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/processor-scripts/{processor_script_id}",
    tag = "admin.processors",
    params(("processor_script_id" = i32, Path, description = "Processor script id")),
    responses((status = 200, description = "Processor script detail", body = ProcessorScriptDetailResponse))
)]
async fn get_processor_script(
    processor_script_id: Path<i32>,
    repository: Data<ProcessorRepository>,
) -> HttpResponse {
    match repository.get_script(*processor_script_id).await {
        Ok(Some((script, modules))) => HttpResponse::Ok().json(
            ProcessorScriptDetailResponse::from_script_and_modules(script, modules),
        ),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    put,
    path = "/api/admin/processor-scripts/{processor_script_id}/status",
    tag = "admin.processors",
    params(("processor_script_id" = i32, Path, description = "Processor script id")),
    request_body = UpdateProcessorScriptStatusRequest,
    responses((status = 200, description = "Processor script status updated", body = ProcessorScriptResponse))
)]
async fn update_processor_script_status(
    processor_script_id: Path<i32>,
    repository: Data<ProcessorRepository>,
    processor: Data<ProcessorRegistryState>,
    request: Json<UpdateProcessorScriptStatusRequest>,
) -> HttpResponse {
    match UpdateProcessorScriptStatusInput::try_from(request.into_inner()) {
        Ok(input) => match repository
            .update_script_status(*processor_script_id, input)
            .await
        {
            Ok(script) => finalize_processor_response(
                HttpResponse::Ok().json(ProcessorScriptResponse::from(script)),
                processor.refresh_if_needed().await,
                "update_processor_script_status",
            ),
            Err(error) => map_repository_error(error),
        },
        Err(error) => HttpResponse::BadRequest().body(error),
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/project-processors",
    tag = "admin.processors",
    responses((status = 200, description = "List project processor bindings", body = [ProjectProcessorResponse]))
)]
async fn list_project_processors(repository: Data<ProcessorRepository>) -> HttpResponse {
    match repository.list_project_processors().await {
        Ok(bindings) => HttpResponse::Ok().json(
            bindings
                .into_iter()
                .map(ProjectProcessorResponse::from)
                .collect::<Vec<_>>(),
        ),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    put,
    path = "/api/admin/projects/{appid}/processor",
    tag = "admin.processors",
    params(("appid" = String, Path, description = "Project appid")),
    request_body = AssignProjectProcessorRequest,
    responses((status = 204, description = "Project processor assigned"))
)]
async fn assign_project_processor(
    appid: Path<String>,
    repository: Data<ProcessorRepository>,
    processor: Data<ProcessorRegistryState>,
    request: Json<AssignProjectProcessorRequest>,
) -> HttpResponse {
    let request = request.into_inner();
    match repository
        .assign_project_processor(&appid, request.processor_script_id, request.enabled)
        .await
    {
        Ok(()) => finalize_processor_response(
            HttpResponse::NoContent().finish(),
            processor.refresh_if_needed().await,
            "assign_project_processor",
        ),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    delete,
    path = "/api/admin/projects/{appid}/processor",
    tag = "admin.processors",
    params(("appid" = String, Path, description = "Project appid")),
    responses((status = 204, description = "Project processor unassigned"))
)]
async fn delete_project_processor(
    appid: Path<String>,
    repository: Data<ProcessorRepository>,
    processor: Data<ProcessorRegistryState>,
) -> HttpResponse {
    match repository.delete_project_processor(&appid).await {
        Ok(()) => finalize_processor_response(
            HttpResponse::NoContent().finish(),
            processor.refresh_if_needed().await,
            "delete_project_processor",
        ),
        Err(error) => map_repository_error(error),
    }
}

fn finalize_processor_response(
    response: HttpResponse,
    refresh_result: anyhow::Result<bool>,
    operation: &str,
) -> HttpResponse {
    if let Err(error) = refresh_result {
        warn!("processor router refresh failed after {operation}: {error}");
    }

    response
}

fn map_repository_error(error: ProcessorRepositoryError) -> HttpResponse {
    match error {
        ProcessorRepositoryError::ProjectNotFound { .. }
        | ProcessorRepositoryError::ProcessorScriptNotFound { .. }
        | ProcessorRepositoryError::DefaultProcessorScriptMissing => {
            HttpResponse::NotFound().finish()
        }
        ProcessorRepositoryError::EntryModuleMissing { .. }
        | ProcessorRepositoryError::InvalidModuleName { .. }
        | ProcessorRepositoryError::InvalidScript { .. }
        | ProcessorRepositoryError::ProcessorScriptNotActive { .. } => {
            HttpResponse::BadRequest().body(error.to_string())
        }
        ProcessorRepositoryError::DuplicateProcessorScriptKey { .. }
        | ProcessorRepositoryError::ProcessorScriptInUse { .. } => {
            HttpResponse::Conflict().body(error.to_string())
        }
        _ => HttpResponse::InternalServerError().body(error.to_string()),
    }
}

fn parse_processor_status(value: &str) -> Result<ProcessorScriptStatus, String> {
    match value {
        "draft" => Ok(ProcessorScriptStatus::Draft),
        "active" => Ok(ProcessorScriptStatus::Active),
        "archived" => Ok(ProcessorScriptStatus::Archived),
        _ => Err(format!("unknown processor script status `{value}`")),
    }
}

fn processor_status_as_str(value: ProcessorScriptStatus) -> &'static str {
    match value {
        ProcessorScriptStatus::Draft => "draft",
        ProcessorScriptStatus::Active => "active",
        ProcessorScriptStatus::Archived => "archived",
    }
}

impl TryFrom<CreateProcessorScriptRequest> for CreateProcessorScriptInput {
    type Error = String;

    fn try_from(value: CreateProcessorScriptRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            script_key: value.script_key,
            name: value.name,
            entry_module: value.entry_module,
            status: parse_processor_status(&value.status)?,
            modules: value
                .modules
                .into_iter()
                .map(CreateProcessorScriptModuleInput::from)
                .collect(),
        })
    }
}

impl TryFrom<UpdateProcessorScriptStatusRequest> for UpdateProcessorScriptStatusInput {
    type Error = String;

    fn try_from(value: UpdateProcessorScriptStatusRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            status: parse_processor_status(&value.status)?,
        })
    }
}

impl From<CreateProcessorScriptModuleRequest> for CreateProcessorScriptModuleInput {
    fn from(value: CreateProcessorScriptModuleRequest) -> Self {
        Self {
            module_name: value.module_name,
            source: value.source,
        }
    }
}

impl From<ProcessorScript> for ProcessorScriptResponse {
    fn from(value: ProcessorScript) -> Self {
        Self {
            id: value.id,
            script_key: value.script_key,
            name: value.name,
            entry_module: value.entry_module,
            version: value.version,
            status: processor_status_as_str(value.status).to_string(),
            checksum: value.checksum,
            created_at: value.created_at,
            updated_at: value.updated_at,
            activated_at: value.activated_at,
        }
    }
}

impl ProcessorScriptDetailResponse {
    fn from_script_and_modules(
        script: ProcessorScript,
        modules: Vec<ProcessorScriptModule>,
    ) -> Self {
        Self {
            script: ProcessorScriptResponse::from(script),
            modules: modules
                .into_iter()
                .map(ProcessorScriptModuleResponse::from)
                .collect(),
        }
    }
}

impl From<ProcessorScriptModule> for ProcessorScriptModuleResponse {
    fn from(value: ProcessorScriptModule) -> Self {
        Self {
            id: value.id,
            processor_script_id: value.processor_script_id,
            module_name: value.module_name,
            source: value.source,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<ProjectProcessor> for ProjectProcessorResponse {
    fn from(value: ProjectProcessor) -> Self {
        Self {
            id: value.id,
            appid: value.appid,
            processor_script_id: value.processor_script_id,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}
