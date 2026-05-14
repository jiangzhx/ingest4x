use crate::current_timestamp_as_u64;
use crate::entities::{app_meta, projects};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DbErr, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, Set, SqlErr, TransactionTrait,
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use uuid::Uuid;

const PROJECTS_VERSION_KEY: &str = "projects_version";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    pub id: i32,
    pub ingest_token: String,
    pub project_key: String,
    pub auth_mode: ProjectAuthMode,
    pub allowed_ips: Vec<String>,
    pub name: String,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectInput {
    pub name: String,
    pub enabled: bool,
    pub ingest_token: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateProjectInput {
    pub name: Option<String>,
    pub enabled: Option<bool>,
    pub ingest_token: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateProjectIngestSettingsInput {
    pub project_key: Option<String>,
    pub auth_mode: Option<ProjectAuthMode>,
    pub allowed_ips: Option<Vec<String>>,
}

pub type ProjectRepositoryResult<T> = Result<T, ProjectRepositoryError>;

#[derive(Debug, PartialEq, Eq)]
pub enum ProjectRepositoryError {
    NotFound { id: i32 },
    DuplicateIngestToken { ingest_token_prefix: String },
    DuplicateProjectKey { project_key: String },
    InvalidIngestToken,
    InvalidProjectKey,
    VersionMetadataMissing,
    CorruptedVersion { value: String },
    Database(DbErr),
}

impl Display for ProjectRepositoryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound { id } => write!(f, "project '{id}' not found"),
            Self::DuplicateIngestToken {
                ingest_token_prefix,
            } => {
                write!(
                    f,
                    "project ingest token prefix '{ingest_token_prefix}' already exists"
                )
            }
            Self::DuplicateProjectKey { project_key } => {
                write!(f, "project key '{project_key}' already exists")
            }
            Self::InvalidIngestToken => write!(f, "ingest token must not be empty"),
            Self::InvalidProjectKey => write!(f, "project key must not be empty"),
            Self::VersionMetadataMissing => write!(f, "projects_version metadata is missing"),
            Self::CorruptedVersion { value } => {
                write!(f, "projects_version contains invalid value '{value}'")
            }
            Self::Database(error) => write!(f, "{error}"),
        }
    }
}

impl Error for ProjectRepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DbErr> for ProjectRepositoryError {
    fn from(value: DbErr) -> Self {
        Self::Database(value)
    }
}

#[derive(Clone)]
pub struct ProjectRepository {
    db: DatabaseConnection,
}

impl ProjectRepository {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create_project(
        &self,
        input: CreateProjectInput,
    ) -> ProjectRepositoryResult<Project> {
        self.create_project_with_ingest_settings(input, UpdateProjectIngestSettingsInput::default())
            .await
    }

    pub async fn create_project_with_ingest_settings(
        &self,
        input: CreateProjectInput,
        settings: UpdateProjectIngestSettingsInput,
    ) -> ProjectRepositoryResult<Project> {
        let token = input.ingest_token.trim();
        if token.is_empty() {
            return Err(ProjectRepositoryError::InvalidIngestToken);
        }
        let ingest_token = token.to_string();
        let ingest_token_prefix = ingest_token_prefix(token);
        let project_key = match settings.project_key {
            Some(project_key) => normalize_project_key(&project_key)
                .ok_or(ProjectRepositoryError::InvalidProjectKey)?,
            None => project_key_from_name(&input.name),
        };
        let auth_mode = settings.auth_mode.unwrap_or(ProjectAuthMode::Token);
        let allowed_ips = settings
            .allowed_ips
            .map(|allowed_ips| allowed_ips_to_storage(&allowed_ips))
            .unwrap_or_else(|| "[]".to_string());
        let txn = self.db.begin().await?;
        let result = async {
            let now = current_timestamp();

            let project = projects::ActiveModel {
                ingest_token: Set(ingest_token),
                project_key: Set(project_key.clone()),
                auth_mode: Set(auth_mode.as_str().to_string()),
                allowed_ips: Set(allowed_ips),
                name: Set(input.name),
                enabled: Set(input.enabled),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&txn)
            .await
            .map_err(|error| map_write_error(error, &ingest_token_prefix, &project_key))?;

            bump_projects_version(&txn).await?;

            Ok(project.into())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn update_project(
        &self,
        id: i32,
        input: UpdateProjectInput,
    ) -> ProjectRepositoryResult<Project> {
        let txn = self.db.begin().await?;
        let result = async {
            let existing = find_project_by_id(&txn, id).await?;
            let mut active_model = existing.into_active_model();

            if let Some(name) = input.name {
                active_model.name = Set(name);
            }
            if let Some(enabled) = input.enabled {
                active_model.enabled = Set(enabled);
            }
            let token_prefix = if let Some(ingest_token) = input.ingest_token {
                let token = ingest_token.trim();
                if token.is_empty() {
                    return Err(ProjectRepositoryError::InvalidIngestToken);
                }

                let prefix = ingest_token_prefix(token);
                active_model.ingest_token = Set(token.to_string());
                Some(prefix)
            } else {
                None
            };
            active_model.updated_at = Set(current_timestamp());

            let project =
                active_model
                    .update(&txn)
                    .await
                    .map_err(|error| match token_prefix.as_deref() {
                        Some(prefix) => map_write_error(error, prefix, ""),
                        None => ProjectRepositoryError::Database(error),
                    })?;
            bump_projects_version(&txn).await?;

            Ok(project.into())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn update_project_with_ingest_settings(
        &self,
        id: i32,
        input: UpdateProjectInput,
        settings: UpdateProjectIngestSettingsInput,
    ) -> ProjectRepositoryResult<Project> {
        let txn = self.db.begin().await?;
        let result = async {
            let existing = find_project_by_id(&txn, id).await?;
            let mut active_model = existing.into_active_model();

            if let Some(name) = input.name {
                active_model.name = Set(name);
            }
            if let Some(enabled) = input.enabled {
                active_model.enabled = Set(enabled);
            }
            let token_prefix = if let Some(ingest_token) = input.ingest_token {
                let token = ingest_token.trim();
                if token.is_empty() {
                    return Err(ProjectRepositoryError::InvalidIngestToken);
                }

                let prefix = ingest_token_prefix(token);
                active_model.ingest_token = Set(token.to_string());
                Some(prefix)
            } else {
                None
            };

            let mut project_key_for_error = None;
            if let Some(project_key) = settings.project_key {
                let project_key = normalize_project_key(&project_key)
                    .ok_or(ProjectRepositoryError::InvalidProjectKey)?;
                active_model.project_key = Set(project_key.clone());
                project_key_for_error = Some(project_key);
            }
            if let Some(auth_mode) = settings.auth_mode {
                active_model.auth_mode = Set(auth_mode.as_str().to_string());
            }
            if let Some(allowed_ips) = settings.allowed_ips {
                active_model.allowed_ips = Set(allowed_ips_to_storage(&allowed_ips));
            }
            active_model.updated_at = Set(current_timestamp());

            let project = active_model.update(&txn).await.map_err(|error| {
                match (token_prefix.as_deref(), project_key_for_error.as_deref()) {
                    (Some(prefix), Some(project_key)) => {
                        map_write_error(error, prefix, project_key)
                    }
                    (Some(prefix), None) => map_write_error(error, prefix, ""),
                    (None, Some(project_key)) => map_write_error(error, "", project_key),
                    (None, None) => ProjectRepositoryError::Database(error),
                }
            })?;
            bump_projects_version(&txn).await?;

            Ok(project.into())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn update_project_ingest_settings(
        &self,
        id: i32,
        input: UpdateProjectIngestSettingsInput,
    ) -> ProjectRepositoryResult<Project> {
        let txn = self.db.begin().await?;
        let result = async {
            let existing = find_project_by_id(&txn, id).await?;
            let mut active_model = existing.into_active_model();
            let mut project_key_for_error = None;

            if let Some(project_key) = input.project_key {
                let project_key = normalize_project_key(&project_key)
                    .ok_or(ProjectRepositoryError::InvalidProjectKey)?;
                active_model.project_key = Set(project_key.clone());
                project_key_for_error = Some(project_key);
            }
            if let Some(auth_mode) = input.auth_mode {
                active_model.auth_mode = Set(auth_mode.as_str().to_string());
            }
            if let Some(allowed_ips) = input.allowed_ips {
                active_model.allowed_ips = Set(allowed_ips_to_storage(&allowed_ips));
            }
            active_model.updated_at = Set(current_timestamp());

            let project = active_model.update(&txn).await.map_err(|error| {
                match project_key_for_error.as_deref() {
                    Some(project_key) => map_write_error(error, "", project_key),
                    None => ProjectRepositoryError::Database(error),
                }
            })?;
            bump_projects_version(&txn).await?;

            Ok(project.into())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn delete_project(&self, id: i32) -> ProjectRepositoryResult<()> {
        let txn = self.db.begin().await?;
        let result = async {
            find_project_by_id(&txn, id).await?;
            let delete_result = projects::Entity::delete_many()
                .filter(projects::Column::Id.eq(id))
                .exec(&txn)
                .await?;
            debug_assert_eq!(delete_result.rows_affected, 1);

            bump_projects_version(&txn).await?;

            Ok(())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn list_projects(&self) -> ProjectRepositoryResult<Vec<Project>> {
        let projects = projects::Entity::find()
            .order_by_asc(projects::Column::Id)
            .all(&self.db)
            .await?;

        Ok(projects.into_iter().map(Into::into).collect())
    }

    pub async fn list_enabled_projects(&self) -> ProjectRepositoryResult<Vec<Project>> {
        let projects = projects::Entity::find()
            .filter(projects::Column::Enabled.eq(true))
            .order_by_asc(projects::Column::Id)
            .all(&self.db)
            .await?;

        Ok(projects.into_iter().map(Into::into).collect())
    }

    pub async fn get_project(&self, id: i32) -> ProjectRepositoryResult<Option<Project>> {
        let project = projects::Entity::find_by_id(id).one(&self.db).await?;

        Ok(project.map(Into::into))
    }

    pub async fn find_enabled_project_by_ingest_token(
        &self,
        ingest_token: &str,
    ) -> ProjectRepositoryResult<Option<Project>> {
        let token = ingest_token.trim();
        if token.is_empty() {
            return Ok(None);
        }
        let project = projects::Entity::find()
            .filter(projects::Column::IngestToken.eq(token))
            .filter(projects::Column::Enabled.eq(true))
            .one(&self.db)
            .await?;

        Ok(project.map(Into::into))
    }

    pub async fn find_enabled_project_by_project_key(
        &self,
        project_key: &str,
    ) -> ProjectRepositoryResult<Option<Project>> {
        let key = project_key.trim();
        if key.is_empty() {
            return Ok(None);
        }
        let project = projects::Entity::find()
            .filter(projects::Column::ProjectKey.eq(key))
            .filter(projects::Column::Enabled.eq(true))
            .one(&self.db)
            .await?;

        Ok(project.map(Into::into))
    }

    pub async fn projects_version(&self) -> ProjectRepositoryResult<u64> {
        read_projects_version(&self.db).await
    }
}

async fn find_project_by_id<C>(db: &C, id: i32) -> ProjectRepositoryResult<projects::Model>
where
    C: ConnectionTrait,
{
    projects::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or(ProjectRepositoryError::NotFound { id })
}

async fn read_projects_version<C>(db: &C) -> ProjectRepositoryResult<u64>
where
    C: ConnectionTrait,
{
    let meta = load_projects_version_metadata(db).await?;

    meta.value
        .parse::<u64>()
        .map_err(|_| ProjectRepositoryError::CorruptedVersion { value: meta.value })
}

async fn bump_projects_version<C>(db: &C) -> ProjectRepositoryResult<()>
where
    C: ConnectionTrait,
{
    let meta = load_projects_version_metadata(db).await?;

    let next_version =
        meta.value
            .parse::<u64>()
            .map_err(|_| ProjectRepositoryError::CorruptedVersion {
                value: meta.value.clone(),
            })?
            + 1;

    let mut active_model = meta.into_active_model();
    active_model.value = Set(next_version.to_string());
    active_model.update(db).await?;

    Ok(())
}

async fn load_projects_version_metadata<C>(db: &C) -> ProjectRepositoryResult<app_meta::Model>
where
    C: ConnectionTrait,
{
    app_meta::Entity::find_by_id(PROJECTS_VERSION_KEY.to_string())
        .one(db)
        .await?
        .ok_or(ProjectRepositoryError::VersionMetadataMissing)
}

fn current_timestamp() -> i64 {
    current_timestamp_as_u64() as i64
}

fn map_write_error(
    error: DbErr,
    ingest_token_prefix: &str,
    project_key: &str,
) -> ProjectRepositoryError {
    match error.sql_err() {
        Some(SqlErr::UniqueConstraintViolation(message))
            if message.to_ascii_lowercase().contains("project_key") =>
        {
            ProjectRepositoryError::DuplicateProjectKey {
                project_key: project_key.to_string(),
            }
        }
        Some(SqlErr::UniqueConstraintViolation(_)) => {
            ProjectRepositoryError::DuplicateIngestToken {
                ingest_token_prefix: ingest_token_prefix.to_string(),
            }
        }
        _ => ProjectRepositoryError::Database(error),
    }
}

pub fn generate_ingest_token() -> String {
    format!("igx_{}", Uuid::new_v4().simple())
}

pub fn ingest_token_prefix(token: &str) -> String {
    let prefix = token.chars().take(12).collect::<String>();
    if token.chars().count() > 12 {
        format!("{prefix}...")
    } else {
        prefix
    }
}

fn project_key_from_name(name: &str) -> String {
    normalize_project_key(name).unwrap_or_else(|| format!("project-{}", Uuid::new_v4().simple()))
}

fn normalize_project_key(value: &str) -> Option<String> {
    let mut key = String::new();
    let mut last_was_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            key.push(ch);
            last_was_dash = false;
        } else if !last_was_dash {
            key.push('-');
            last_was_dash = true;
        }
    }

    let key = key.trim_matches('-').to_string();
    if key.is_empty() {
        None
    } else {
        Some(key)
    }
}

async fn finish_transaction<T>(
    txn: sea_orm::DatabaseTransaction,
    result: ProjectRepositoryResult<T>,
) -> ProjectRepositoryResult<T> {
    match result {
        Ok(value) => {
            txn.commit().await?;
            Ok(value)
        }
        Err(error) => {
            txn.rollback().await?;
            Err(error)
        }
    }
}

impl From<projects::Model> for Project {
    fn from(value: projects::Model) -> Self {
        Self {
            id: value.id,
            ingest_token: value.ingest_token,
            project_key: value.project_key,
            auth_mode: ProjectAuthMode::from_storage(value.auth_mode.as_str()),
            allowed_ips: parse_allowed_ips(&value.allowed_ips),
            name: value.name,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectAuthMode {
    Token,
    Public,
}

impl ProjectAuthMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Token => "token",
            Self::Public => "public",
        }
    }

    pub fn from_storage(value: &str) -> Self {
        match value {
            "public" => Self::Public,
            _ => Self::Token,
        }
    }
}

fn parse_allowed_ips(value: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(value)
        .unwrap_or_default()
        .into_iter()
        .map(|ip| ip.trim().to_string())
        .filter(|ip| !ip.is_empty())
        .collect()
}

fn allowed_ips_to_storage(allowed_ips: &[String]) -> String {
    let ips = allowed_ips
        .iter()
        .map(|ip| ip.trim().to_string())
        .filter(|ip| !ip.is_empty())
        .collect::<Vec<_>>();
    serde_json::to_string(&ips).unwrap_or_else(|_| "[]".to_string())
}
