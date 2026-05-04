use crate::repositories::{
    CreateProjectRuleSetInput, CreateRuleInput, CreateRuleSetInput, ProjectRuleSet, Rule,
    RuleRepository, RuleRepositoryError, RuleSet, UpdateRuleInput, UpdateRuleSetInput,
};
use actix_web::web::{self, Data, Json, Path, ServiceConfig};
use actix_web::HttpResponse;
use utoipa::{OpenApi, ToSchema};

#[derive(Debug, serde::Deserialize, ToSchema)]
struct AssignProjectRuleSetRequest {
    rule_set_id: i32,
    enabled: bool,
}

#[derive(Debug, serde::Deserialize, ToSchema)]
struct CreateRuleRequest {
    parent_id: Option<i32>,
    name: String,
    xwhat: Option<String>,
    content: String,
    enabled: bool,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        list_rule_sets,
        create_rule_set,
        get_rule_set,
        update_rule_set,
        delete_rule_set,
        list_rules,
        create_rule,
        get_rule,
        update_rule,
        delete_rule,
        list_project_rule_sets,
        assign_project_rule_set,
        delete_project_rule_set
    ),
    components(
        schemas(
            RuleSet,
            Rule,
            ProjectRuleSet,
            CreateRuleSetInput,
            UpdateRuleSetInput,
            CreateRuleRequest,
            UpdateRuleInput,
            AssignProjectRuleSetRequest
        )
    ),
    tags(
        (name = "admin.rules", description = "Admin rule set and rule tree CRUD endpoints")
    )
)]
pub struct AdminApiDoc;

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.service(
        web::scope("/rule-sets")
            .route("", web::get().to(list_rule_sets))
            .route("", web::post().to(create_rule_set))
            .route("/{rule_set_id}", web::get().to(get_rule_set))
            .route("/{rule_set_id}", web::put().to(update_rule_set))
            .route("/{rule_set_id}", web::delete().to(delete_rule_set))
            .route("/{rule_set_id}/rules", web::get().to(list_rules))
            .route("/{rule_set_id}/rules", web::post().to(create_rule))
            .route("/{rule_set_id}/rules/{rule_id}", web::get().to(get_rule))
            .route("/{rule_set_id}/rules/{rule_id}", web::put().to(update_rule))
            .route(
                "/{rule_set_id}/rules/{rule_id}",
                web::delete().to(delete_rule),
            ),
    )
    .route(
        "/projects/{appid}/rule-sets",
        web::get().to(list_project_rule_sets),
    )
    .route(
        "/projects/{appid}/rule-sets",
        web::put().to(assign_project_rule_set),
    )
    .route(
        "/projects/{appid}/rule-sets/{rule_set_id}",
        web::delete().to(delete_project_rule_set),
    );
}

#[utoipa::path(
    get,
    path = "/api/admin/rule-sets",
    tag = "admin.rules",
    responses(
        (status = 200, description = "List rule sets", body = [RuleSet]),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn list_rule_sets(repository: Data<RuleRepository>) -> HttpResponse {
    match repository.list_rule_sets().await {
        Ok(rule_sets) => HttpResponse::Ok().json(rule_sets),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/api/admin/rule-sets",
    tag = "admin.rules",
    request_body = CreateRuleSetInput,
    responses(
        (status = 201, description = "Rule set created", body = RuleSet),
        (status = 409, description = "Duplicate rule set", body = String),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn create_rule_set(
    repository: Data<RuleRepository>,
    request: Json<CreateRuleSetInput>,
) -> HttpResponse {
    match repository.create_rule_set(request.into_inner()).await {
        Ok(rule_set) => HttpResponse::Created().json(rule_set),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/rule-sets/{rule_set_id}",
    tag = "admin.rules",
    params(("rule_set_id" = i32, Path, description = "Rule set id")),
    responses(
        (status = 200, description = "Rule set details", body = RuleSet),
        (status = 404, description = "Rule set not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn get_rule_set(rule_set_id: Path<i32>, repository: Data<RuleRepository>) -> HttpResponse {
    match repository.get_rule_set(rule_set_id.into_inner()).await {
        Ok(Some(rule_set)) => HttpResponse::Ok().json(rule_set),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    put,
    path = "/api/admin/rule-sets/{rule_set_id}",
    tag = "admin.rules",
    params(("rule_set_id" = i32, Path, description = "Rule set id")),
    request_body = UpdateRuleSetInput,
    responses(
        (status = 200, description = "Rule set updated", body = RuleSet),
        (status = 404, description = "Rule set not found"),
        (status = 409, description = "Duplicate rule set", body = String),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn update_rule_set(
    rule_set_id: Path<i32>,
    repository: Data<RuleRepository>,
    request: Json<UpdateRuleSetInput>,
) -> HttpResponse {
    match repository
        .update_rule_set(rule_set_id.into_inner(), request.into_inner())
        .await
    {
        Ok(rule_set) => HttpResponse::Ok().json(rule_set),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    delete,
    path = "/api/admin/rule-sets/{rule_set_id}",
    tag = "admin.rules",
    params(("rule_set_id" = i32, Path, description = "Rule set id")),
    responses(
        (status = 204, description = "Rule set deleted"),
        (status = 404, description = "Rule set not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn delete_rule_set(rule_set_id: Path<i32>, repository: Data<RuleRepository>) -> HttpResponse {
    match repository.delete_rule_set(rule_set_id.into_inner()).await {
        Ok(()) => HttpResponse::NoContent().finish(),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/rule-sets/{rule_set_id}/rules",
    tag = "admin.rules",
    params(("rule_set_id" = i32, Path, description = "Rule set id")),
    responses(
        (status = 200, description = "List rules", body = [Rule]),
        (status = 404, description = "Rule set not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn list_rules(rule_set_id: Path<i32>, repository: Data<RuleRepository>) -> HttpResponse {
    match repository.list_rules(rule_set_id.into_inner()).await {
        Ok(rules) => HttpResponse::Ok().json(rules),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/api/admin/rule-sets/{rule_set_id}/rules",
    tag = "admin.rules",
    params(("rule_set_id" = i32, Path, description = "Rule set id")),
    request_body = CreateRuleRequest,
    responses(
        (status = 201, description = "Rule created", body = Rule),
        (status = 400, description = "Invalid rule content or parent", body = String),
        (status = 404, description = "Rule set not found"),
        (status = 409, description = "Duplicate rule", body = String),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn create_rule(
    rule_set_id: Path<i32>,
    repository: Data<RuleRepository>,
    request: Json<CreateRuleRequest>,
) -> HttpResponse {
    let request = request.into_inner();
    let input = CreateRuleInput {
        rule_set_id: rule_set_id.into_inner(),
        parent_id: request.parent_id,
        name: request.name,
        xwhat: request.xwhat,
        content: request.content,
        enabled: request.enabled,
    };
    match repository.create_rule(input).await {
        Ok(rule) => HttpResponse::Created().json(rule),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/rule-sets/{rule_set_id}/rules/{rule_id}",
    tag = "admin.rules",
    params(
        ("rule_set_id" = i32, Path, description = "Rule set id"),
        ("rule_id" = i32, Path, description = "Rule id")
    ),
    responses(
        (status = 200, description = "Rule details", body = Rule),
        (status = 404, description = "Rule not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn get_rule(path: Path<(i32, i32)>, repository: Data<RuleRepository>) -> HttpResponse {
    let (rule_set_id, rule_id) = path.into_inner();
    match repository.get_rule(rule_set_id, rule_id).await {
        Ok(Some(rule)) => HttpResponse::Ok().json(rule),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    put,
    path = "/api/admin/rule-sets/{rule_set_id}/rules/{rule_id}",
    tag = "admin.rules",
    params(
        ("rule_set_id" = i32, Path, description = "Rule set id"),
        ("rule_id" = i32, Path, description = "Rule id")
    ),
    request_body = UpdateRuleInput,
    responses(
        (status = 200, description = "Rule updated", body = Rule),
        (status = 400, description = "Invalid rule content or parent", body = String),
        (status = 404, description = "Rule not found"),
        (status = 409, description = "Duplicate rule", body = String),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn update_rule(
    path: Path<(i32, i32)>,
    repository: Data<RuleRepository>,
    request: Json<UpdateRuleInput>,
) -> HttpResponse {
    let (rule_set_id, rule_id) = path.into_inner();
    match repository
        .update_rule(rule_set_id, rule_id, request.into_inner())
        .await
    {
        Ok(rule) => HttpResponse::Ok().json(rule),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    delete,
    path = "/api/admin/rule-sets/{rule_set_id}/rules/{rule_id}",
    tag = "admin.rules",
    params(
        ("rule_set_id" = i32, Path, description = "Rule set id"),
        ("rule_id" = i32, Path, description = "Rule id")
    ),
    responses(
        (status = 204, description = "Rule deleted"),
        (status = 404, description = "Rule not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn delete_rule(path: Path<(i32, i32)>, repository: Data<RuleRepository>) -> HttpResponse {
    let (rule_set_id, rule_id) = path.into_inner();
    match repository.delete_rule(rule_set_id, rule_id).await {
        Ok(()) => HttpResponse::NoContent().finish(),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/api/admin/projects/{appid}/rule-sets",
    tag = "admin.rules",
    params(("appid" = String, Path, description = "Project appid")),
    responses(
        (status = 200, description = "List project rule set assignments", body = [ProjectRuleSet]),
        (status = 404, description = "Project not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn list_project_rule_sets(
    appid: Path<String>,
    repository: Data<RuleRepository>,
) -> HttpResponse {
    match repository.list_project_rule_sets(&appid).await {
        Ok(assignments) => HttpResponse::Ok().json(assignments),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    put,
    path = "/api/admin/projects/{appid}/rule-sets",
    tag = "admin.rules",
    params(("appid" = String, Path, description = "Project appid")),
    request_body = AssignProjectRuleSetRequest,
    responses(
        (status = 200, description = "Rule set assigned to project", body = ProjectRuleSet),
        (status = 404, description = "Project or rule set not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn assign_project_rule_set(
    appid: Path<String>,
    repository: Data<RuleRepository>,
    request: Json<AssignProjectRuleSetRequest>,
) -> HttpResponse {
    let request = request.into_inner();
    match repository
        .assign_rule_set_to_project(
            &appid,
            CreateProjectRuleSetInput {
                rule_set_id: request.rule_set_id,
                enabled: request.enabled,
            },
        )
        .await
    {
        Ok(assignment) => HttpResponse::Ok().json(assignment),
        Err(error) => map_repository_error(error),
    }
}

#[utoipa::path(
    delete,
    path = "/api/admin/projects/{appid}/rule-sets/{rule_set_id}",
    tag = "admin.rules",
    params(
        ("appid" = String, Path, description = "Project appid"),
        ("rule_set_id" = i32, Path, description = "Rule set id")
    ),
    responses(
        (status = 204, description = "Rule set assignment deleted"),
        (status = 404, description = "Project or assignment not found"),
        (status = 500, description = "Repository failure", body = String)
    )
)]
async fn delete_project_rule_set(
    path: Path<(String, i32)>,
    repository: Data<RuleRepository>,
) -> HttpResponse {
    let (appid, rule_set_id) = path.into_inner();
    match repository
        .delete_project_rule_set(&appid, rule_set_id)
        .await
    {
        Ok(()) => HttpResponse::NoContent().finish(),
        Err(error) => map_repository_error(error),
    }
}

fn map_repository_error(error: RuleRepositoryError) -> HttpResponse {
    match error {
        RuleRepositoryError::ProjectNotFound { .. }
        | RuleRepositoryError::RuleSetNotFound { .. }
        | RuleRepositoryError::RuleNotFound { .. } => {
            HttpResponse::NotFound().body(error.to_string())
        }
        RuleRepositoryError::DuplicateName | RuleRepositoryError::DuplicateXwhat => {
            HttpResponse::Conflict().body(error.to_string())
        }
        RuleRepositoryError::ParentNotFound { .. }
        | RuleRepositoryError::ParentMustBeCommonRule { .. }
        | RuleRepositoryError::RuleWithChildrenCannotHaveXwhat { .. }
        | RuleRepositoryError::WildcardRuleMustNotHaveXwhat
        | RuleRepositoryError::Cycle
        | RuleRepositoryError::InvalidRuleContent { .. } => {
            HttpResponse::BadRequest().body(error.to_string())
        }
        _ => HttpResponse::InternalServerError().body(error.to_string()),
    }
}
