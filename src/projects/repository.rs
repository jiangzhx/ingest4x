use crate::current_timestamp_as_u64;
use crate::db::entities::{app_meta, projects};
use crate::projects::model::{
    CreateProjectInput, Project, ProjectRepositoryError, ProjectRepositoryResult,
    UpdateProjectInput,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DbErr, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, Set, SqlErr, TransactionTrait,
};

const PROJECTS_VERSION_KEY: &str = "projects_version";

#[derive(Clone)]
pub struct ProjectRepository {
    db: DatabaseConnection,
}

impl ProjectRepository {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub fn database(&self) -> DatabaseConnection {
        self.db.clone()
    }

    pub async fn create_project(
        &self,
        input: CreateProjectInput,
    ) -> ProjectRepositoryResult<Project> {
        let txn = self.db.begin().await?;
        let result = async {
            let now = current_timestamp();
            let appid = input.appid.clone();

            let project = projects::ActiveModel {
                appid: Set(input.appid),
                name: Set(input.name),
                enabled: Set(input.enabled),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&txn)
            .await
            .map_err(|error| map_write_error(error, &appid))?;

            bump_projects_version(&txn).await?;

            Ok(project.into())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn update_project(
        &self,
        appid: &str,
        input: UpdateProjectInput,
    ) -> ProjectRepositoryResult<Project> {
        let txn = self.db.begin().await?;
        let result = async {
            let existing = find_project_by_appid(&txn, appid).await?;
            let mut active_model = existing.into_active_model();

            if let Some(name) = input.name {
                active_model.name = Set(name);
            }
            if let Some(enabled) = input.enabled {
                active_model.enabled = Set(enabled);
            }
            active_model.updated_at = Set(current_timestamp());

            let project = active_model.update(&txn).await?;
            bump_projects_version(&txn).await?;

            Ok(project.into())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn delete_project(&self, appid: &str) -> ProjectRepositoryResult<()> {
        let txn = self.db.begin().await?;
        let result = async {
            find_project_by_appid(&txn, appid).await?;
            let delete_result = projects::Entity::delete_many()
                .filter(projects::Column::Appid.eq(appid))
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

    pub async fn get_project(&self, appid: &str) -> ProjectRepositoryResult<Option<Project>> {
        let project = projects::Entity::find()
            .filter(projects::Column::Appid.eq(appid))
            .one(&self.db)
            .await?;

        Ok(project.map(Into::into))
    }

    pub async fn projects_version(&self) -> ProjectRepositoryResult<u64> {
        read_projects_version(&self.db).await
    }
}

async fn find_project_by_appid<C>(db: &C, appid: &str) -> ProjectRepositoryResult<projects::Model>
where
    C: ConnectionTrait,
{
    projects::Entity::find()
        .filter(projects::Column::Appid.eq(appid))
        .one(db)
        .await?
        .ok_or_else(|| ProjectRepositoryError::NotFound {
            appid: appid.to_string(),
        })
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

fn map_write_error(error: DbErr, appid: &str) -> ProjectRepositoryError {
    match error.sql_err() {
        Some(SqlErr::UniqueConstraintViolation(_)) => ProjectRepositoryError::DuplicateAppid {
            appid: appid.to_string(),
        },
        _ => ProjectRepositoryError::Database(error),
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
            appid: value.appid,
            name: value.name,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}
