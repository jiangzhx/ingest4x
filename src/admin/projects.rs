use crate::ingest::processor::ProcessorRegistryState as ProcessorRuntimeState;
use crate::repositories::{
    CreateProjectInput, CreateProjectRuleSetInput, ProcessorRepository, Project, ProjectRepository,
    ProjectRepositoryError, RuleRepository, UpdateProjectInput,
};
use crate::services::ProjectRegistryState;
use actix_web::web::{self, Data, Json, Path, ServiceConfig};
use actix_web::HttpResponse;
use log::warn;
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

#[derive(Debug, Deserialize, ToSchema)]
struct CreateProjectRequest {
    appid: String,
    name: String,
    enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateProjectRequest {
    name: Option<String>,
    enabled: Option<bool>,
}

#[derive(Debug, Serialize, PartialEq, Eq, ToSchema)]
struct ProjectResponse {
    appid: String,
    name: String,
    enabled: bool,
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
            .route("/{appid}", web::get().to(get_project))
            .route("/{appid}", web::put().to(update_project))
            .route("/{appid}", web::delete().to(delete_project)),
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
    path = "/api/admin/projects/{appid}",
    tag = "admin.projects",
    params(
        ("appid" = String, Path, description = "Project appid")
    ),
    responses(
        (status = 200, description = "Project details", body = ProjectResponse),
        (status = 404, description = "Project not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn get_project(appid: Path<String>, repository: Data<ProjectRepository>) -> HttpResponse {
    match repository.get_project(&appid).await {
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
        (status = 409, description = "Project appid already exists", body = String),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn create_project(
    repository: Data<ProjectRepository>,
    rule_repository: Data<RuleRepository>,
    processor_repository: Data<ProcessorRepository>,
    registry: Data<ProjectRegistryState>,
    processor: Data<ProcessorRuntimeState>,
    request: Json<CreateProjectRequest>,
) -> HttpResponse {
    match repository
        .create_project(CreateProjectInput::from(request.into_inner()))
        .await
    {
        Ok(project) => {
            let appid = project.appid.clone();
            assign_default_rule_set_to_project(&rule_repository, &appid).await;
            if let Err(error) = processor_repository.assign_default_processor(&appid).await {
                return HttpResponse::InternalServerError().body(error.to_string());
            }
            if let Err(error) = processor.refresh_if_needed().await {
                warn!("processor registry refresh failed after create for '{appid}': {error}");
            }
            finalize_success_response(
                HttpResponse::Created().json(ProjectResponse::from(project)),
                registry.refresh_if_needed().await,
                "create",
                &appid,
            )
        }
        Err(error) => map_repository_error(error),
    }
}

async fn assign_default_rule_set_to_project(rule_repository: &RuleRepository, appid: &str) {
    let Ok(rule_sets) = rule_repository.list_rule_sets().await else {
        return;
    };

    if let Some(rule_set) = rule_sets.into_iter().find(|rule_set| rule_set.enabled) {
        let _ = rule_repository
            .assign_rule_set_to_project(
                appid,
                CreateProjectRuleSetInput {
                    rule_set_id: rule_set.id,
                    enabled: true,
                },
            )
            .await;
    }
}

#[utoipa::path(
    put,
    path = "/api/admin/projects/{appid}",
    tag = "admin.projects",
    params(
        ("appid" = String, Path, description = "Project appid")
    ),
    request_body = UpdateProjectRequest,
    responses(
        (status = 200, description = "Project updated", body = ProjectResponse),
        (status = 400, description = "Invalid JSON payload"),
        (status = 404, description = "Project not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn update_project(
    appid: Path<String>,
    repository: Data<ProjectRepository>,
    registry: Data<ProjectRegistryState>,
    request: Json<UpdateProjectRequest>,
) -> HttpResponse {
    match repository
        .update_project(&appid, UpdateProjectInput::from(request.into_inner()))
        .await
    {
        Ok(project) => {
            let appid = project.appid.clone();
            finalize_success_response(
                HttpResponse::Ok().json(ProjectResponse::from(project)),
                registry.refresh_if_needed().await,
                "update",
                &appid,
            )
        }
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    delete,
    path = "/api/admin/projects/{appid}",
    tag = "admin.projects",
    params(
        ("appid" = String, Path, description = "Project appid")
    ),
    responses(
        (status = 204, description = "Project deleted"),
        (status = 404, description = "Project not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn delete_project(
    appid: Path<String>,
    repository: Data<ProjectRepository>,
    registry: Data<ProjectRegistryState>,
) -> HttpResponse {
    match repository.delete_project(&appid).await {
        Ok(()) => finalize_success_response(
            HttpResponse::NoContent().finish(),
            registry.refresh_if_needed().await,
            "delete",
            &appid,
        ),
        Err(error) => map_repository_error(error),
    }
}

fn finalize_success_response(
    response: HttpResponse,
    refresh_result: crate::repositories::ProjectRepositoryResult<bool>,
    operation: &str,
    appid: &str,
) -> HttpResponse {
    if let Err(error) = refresh_result {
        warn!("project registry refresh failed after {operation} for '{appid}': {error}");
    }

    response
}

fn map_repository_error(error: ProjectRepositoryError) -> HttpResponse {
    match error {
        ProjectRepositoryError::NotFound { .. } => HttpResponse::NotFound().finish(),
        ProjectRepositoryError::DuplicateAppid { .. } => {
            HttpResponse::Conflict().body(error.to_string())
        }
        _ => HttpResponse::InternalServerError().body(error.to_string()),
    }
}

impl From<CreateProjectRequest> for CreateProjectInput {
    fn from(value: CreateProjectRequest) -> Self {
        Self {
            appid: value.appid,
            name: value.name,
            enabled: value.enabled,
        }
    }
}

impl From<UpdateProjectRequest> for UpdateProjectInput {
    fn from(value: UpdateProjectRequest) -> Self {
        Self {
            name: value.name,
            enabled: value.enabled,
        }
    }
}

impl From<Project> for ProjectResponse {
    fn from(value: Project) -> Self {
        Self {
            appid: value.appid,
            name: value.name,
            enabled: value.enabled,
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
            "app-1",
        );

        assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    }
}
