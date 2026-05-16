use crate::ingest::processor::ProcessorRegistryState as ProcessorRuntimeState;
use crate::repositories::{
    generate_ingest_token, CreateProjectInput, ProcessorRepository, Project, ProjectAuthMode,
    ProjectRepository, ProjectRepositoryError, UpdateProjectIngestSettingsInput,
    UpdateProjectInput,
};
use crate::services::ProjectRegistryState;
use actix_web::web::{self, Data, Json, Path, ServiceConfig};
use actix_web::HttpResponse;
use log::warn;
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

#[derive(Debug, Deserialize, ToSchema)]
struct CreateProjectRequest {
    name: String,
    enabled: bool,
    ingest_token: Option<String>,
    project_key: Option<String>,
    auth_mode: Option<String>,
    allowed_ips: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateProjectRequest {
    name: Option<String>,
    enabled: Option<bool>,
    ingest_token: Option<String>,
    project_key: Option<String>,
    auth_mode: Option<String>,
    allowed_ips: Option<Vec<String>>,
}

#[derive(Debug, Serialize, PartialEq, Eq, ToSchema)]
struct ProjectResponse {
    id: i32,
    project_key: String,
    name: String,
    enabled: bool,
    auth_mode: String,
    allowed_ips: Vec<String>,
    ingest_token: String,
    ingest_token_prefix: String,
    created_at: i64,
    updated_at: i64,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        list_projects,
        get_project,
        create_project,
        update_project,
        delete_project
    ),
    components(
        schemas(CreateProjectRequest, UpdateProjectRequest, ProjectResponse)
    ),
    tags(
        (name = "admin.projects", description = "Admin project CRUD endpoints")
    )
)]
pub struct AdminApiDoc;

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.service(
        web::scope("/projects")
            .route("", web::get().to(list_projects))
            .route("", web::post().to(create_project))
            .route("/{project_id}", web::get().to(get_project))
            .route("/{project_id}", web::put().to(update_project))
            .route("/{project_id}", web::delete().to(delete_project)),
    );
}

#[utoipa::path(
    get,
    path = "/api/admin/projects",
    tag = "admin.projects",
    responses(
        (status = 200, description = "List projects", body = [ProjectResponse]),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn list_projects(repository: Data<ProjectRepository>) -> HttpResponse {
    match repository.list_projects().await {
        Ok(projects) => HttpResponse::Ok().json(
            projects
                .into_iter()
                .map(ProjectResponse::from)
                .collect::<Vec<_>>(),
        ),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/projects/{project_id}",
    tag = "admin.projects",
    params(
        ("project_id" = i32, Path, description = "Project id")
    ),
    responses(
        (status = 200, description = "Project details", body = ProjectResponse),
        (status = 404, description = "Project not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn get_project(project_id: Path<i32>, repository: Data<ProjectRepository>) -> HttpResponse {
    match repository.get_project(*project_id).await {
        Ok(Some(project)) => HttpResponse::Ok().json(ProjectResponse::from(project)),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/api/admin/projects",
    tag = "admin.projects",
    request_body = CreateProjectRequest,
    responses(
        (status = 201, description = "Project created", body = ProjectResponse),
        (status = 400, description = "Invalid JSON payload"),
        (status = 409, description = "Project ingest token already exists", body = String),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn create_project(
    repository: Data<ProjectRepository>,
    processor_repository: Data<ProcessorRepository>,
    registry: Data<ProjectRegistryState>,
    processor: Data<ProcessorRuntimeState>,
    request: Json<CreateProjectRequest>,
) -> HttpResponse {
    let request = request.into_inner();
    let ingest_token = request
        .ingest_token
        .clone()
        .filter(|token| !token.trim().is_empty())
        .unwrap_or_else(generate_ingest_token);
    let ingest_settings = match UpdateProjectIngestSettingsInput::from_create_request(&request) {
        Ok(input) => input,
        Err(response) => return response,
    };

    match repository
        .create_project_with_ingest_settings(
            CreateProjectInput::from_request(request, ingest_token.clone()),
            ingest_settings,
        )
        .await
    {
        Ok(project) => {
            let project_id = project.id;
            if let Err(error) = processor_repository
                .assign_default_processor(project_id)
                .await
            {
                return HttpResponse::InternalServerError().body(error.to_string());
            }
            if let Err(error) = processor.refresh_if_needed().await {
                warn!("processor registry refresh failed after create for '{project_id}': {error}");
            }
            finalize_success_response(
                HttpResponse::Created().json(ProjectResponse::from(project)),
                registry.refresh_if_needed().await,
                "create",
                project_id,
            )
        }
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    put,
    path = "/api/admin/projects/{project_id}",
    tag = "admin.projects",
    params(
        ("project_id" = i32, Path, description = "Project id")
    ),
    request_body = UpdateProjectRequest,
    responses(
        (status = 200, description = "Project updated", body = ProjectResponse),
        (status = 400, description = "Invalid JSON payload"),
        (status = 404, description = "Project not found"),
        (status = 409, description = "Project ingest token already exists", body = String),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn update_project(
    project_id: Path<i32>,
    repository: Data<ProjectRepository>,
    registry: Data<ProjectRegistryState>,
    request: Json<UpdateProjectRequest>,
) -> HttpResponse {
    let request = request.into_inner();
    let ingest_settings = match UpdateProjectIngestSettingsInput::from_update_request(&request) {
        Ok(input) => input,
        Err(response) => return response,
    };
    let input = UpdateProjectInput::from_request(request);

    match repository
        .update_project_with_ingest_settings(*project_id, input, ingest_settings)
        .await
    {
        Ok(project) => {
            let project_id = project.id;
            finalize_success_response(
                HttpResponse::Ok().json(ProjectResponse::from(project)),
                registry.refresh_if_needed().await,
                "update",
                project_id,
            )
        }
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    delete,
    path = "/api/admin/projects/{project_id}",
    tag = "admin.projects",
    params(
        ("project_id" = i32, Path, description = "Project id")
    ),
    responses(
        (status = 204, description = "Project deleted"),
        (status = 404, description = "Project not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn delete_project(
    project_id: Path<i32>,
    repository: Data<ProjectRepository>,
    registry: Data<ProjectRegistryState>,
) -> HttpResponse {
    match repository.delete_project(*project_id).await {
        Ok(()) => finalize_success_response(
            HttpResponse::NoContent().finish(),
            registry.refresh_if_needed().await,
            "delete",
            *project_id,
        ),
        Err(error) => map_repository_error(error),
    }
}

fn finalize_success_response(
    response: HttpResponse,
    refresh_result: crate::repositories::ProjectRepositoryResult<bool>,
    operation: &str,
    project_id: i32,
) -> HttpResponse {
    if let Err(error) = refresh_result {
        warn!("project registry refresh failed after {operation} for '{project_id}': {error}");
    }

    response
}

fn map_repository_error(error: ProjectRepositoryError) -> HttpResponse {
    match error {
        ProjectRepositoryError::NotFound { .. } => HttpResponse::NotFound().finish(),
        ProjectRepositoryError::DuplicateIngestToken { .. } => {
            HttpResponse::Conflict().body(error.to_string())
        }
        ProjectRepositoryError::DuplicateProjectKey { .. } => {
            HttpResponse::Conflict().body(error.to_string())
        }
        ProjectRepositoryError::InvalidProjectKey => {
            HttpResponse::BadRequest().body(error.to_string())
        }
        _ => HttpResponse::InternalServerError().body(error.to_string()),
    }
}

impl CreateProjectInput {
    fn from_request(value: CreateProjectRequest, ingest_token: String) -> Self {
        Self {
            name: value.name,
            enabled: value.enabled,
            ingest_token,
        }
    }
}

impl UpdateProjectInput {
    fn from_request(value: UpdateProjectRequest) -> Self {
        let ingest_token = value
            .ingest_token
            .clone()
            .filter(|token| !token.trim().is_empty());

        Self {
            name: value.name,
            enabled: value.enabled,
            ingest_token,
        }
    }
}

impl From<UpdateProjectRequest> for UpdateProjectInput {
    fn from(value: UpdateProjectRequest) -> Self {
        Self::from_request(value)
    }
}

impl UpdateProjectIngestSettingsInput {
    fn from_create_request(value: &CreateProjectRequest) -> Result<Self, HttpResponse> {
        Self::from_parts(
            value.project_key.clone(),
            value.auth_mode.as_deref(),
            value.allowed_ips.clone(),
        )
    }

    fn from_update_request(value: &UpdateProjectRequest) -> Result<Self, HttpResponse> {
        Self::from_parts(
            value.project_key.clone(),
            value.auth_mode.as_deref(),
            value.allowed_ips.clone(),
        )
    }

    fn from_parts(
        project_key: Option<String>,
        auth_mode: Option<&str>,
        allowed_ips: Option<Vec<String>>,
    ) -> Result<Self, HttpResponse> {
        let auth_mode = match auth_mode {
            Some("token") => Some(ProjectAuthMode::Token),
            Some("public") => Some(ProjectAuthMode::Public),
            Some(_) => return Err(HttpResponse::BadRequest().body("invalid auth_mode")),
            None => None,
        };

        Ok(Self {
            project_key,
            auth_mode,
            allowed_ips,
        })
    }
}

impl From<Project> for ProjectResponse {
    fn from(value: Project) -> Self {
        Self {
            id: value.id,
            project_key: value.project_key,
            name: value.name,
            enabled: value.enabled,
            auth_mode: value.auth_mode.as_str().to_string(),
            allowed_ips: value.allowed_ips,
            ingest_token_prefix: crate::repositories::projects::ingest_token_prefix(
                &value.ingest_token,
            ),
            ingest_token: value.ingest_token,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalize_success_response_keeps_success_status_when_refresh_fails() {
        let response = finalize_success_response(
            HttpResponse::Created().finish(),
            Err(ProjectRepositoryError::VersionMetadataMissing),
            "create",
            1,
        );

        assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    }
}
