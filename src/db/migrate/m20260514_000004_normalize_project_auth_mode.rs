use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        ensure_project_key(manager).await?;
        ensure_auth_mode(manager).await?;
        ensure_allowed_ips(manager).await?;
        ensure_project_key_index(manager).await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Ok(())
    }
}

async fn ensure_project_key(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if manager.has_column("projects", "project_key").await? {
        return Ok(());
    }

    execute_backend_sql(
        manager,
        DbBackend::Sqlite,
        "ALTER TABLE projects ADD COLUMN project_key TEXT",
    )
    .await?;
    execute_backend_sql(
        manager,
        DbBackend::MySql,
        "ALTER TABLE projects ADD COLUMN project_key VARCHAR(255)",
    )
    .await?;
    fill_project_key(manager).await
}

async fn ensure_auth_mode(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if manager.has_column("projects", "auth_mode").await? {
        return Ok(());
    }

    execute_backend_sql(
        manager,
        DbBackend::Sqlite,
        "ALTER TABLE projects ADD COLUMN auth_mode TEXT NOT NULL DEFAULT 'token'",
    )
    .await?;
    execute_backend_sql(
        manager,
        DbBackend::MySql,
        "ALTER TABLE projects ADD COLUMN auth_mode VARCHAR(32) NOT NULL DEFAULT 'token'",
    )
    .await?;

    if manager.has_column("projects", "auth_strategy").await? {
        execute_current_backend_sql(
            manager,
            "UPDATE projects SET auth_mode = CASE WHEN auth_strategy IN ('public', 'ip') THEN 'public' ELSE 'token' END",
        )
        .await?;
    }

    Ok(())
}

async fn ensure_allowed_ips(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if manager.has_column("projects", "allowed_ips").await? {
        return Ok(());
    }

    execute_backend_sql(
        manager,
        DbBackend::Sqlite,
        "ALTER TABLE projects ADD COLUMN allowed_ips TEXT NOT NULL DEFAULT '[]'",
    )
    .await?;
    execute_backend_sql(
        manager,
        DbBackend::MySql,
        "ALTER TABLE projects ADD COLUMN allowed_ips VARCHAR(2048) NOT NULL DEFAULT '[]'",
    )
    .await
}

async fn ensure_project_key_index(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    fill_project_key(manager).await?;

    match manager.get_database_backend() {
        DbBackend::Sqlite => execute_current_backend_sql(
            manager,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_project_key ON projects(project_key)",
        )
        .await,
        DbBackend::MySql => {
            if !manager
                .has_index("projects", "idx_projects_project_key")
                .await?
            {
                execute_current_backend_sql(
                    manager,
                    "CREATE UNIQUE INDEX idx_projects_project_key ON projects(project_key)",
                )
                .await?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn fill_project_key(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    execute_backend_sql(
        manager,
        DbBackend::Sqlite,
        "UPDATE projects SET project_key = 'project-' || id WHERE project_key IS NULL OR project_key = ''",
    )
    .await?;
    execute_backend_sql(
        manager,
        DbBackend::MySql,
        "UPDATE projects SET project_key = CONCAT('project-', id) WHERE project_key IS NULL OR project_key = ''",
    )
    .await
}

async fn execute_backend_sql(
    manager: &SchemaManager<'_>,
    backend: DbBackend,
    sql: &str,
) -> Result<(), DbErr> {
    if manager.get_database_backend() != backend {
        return Ok(());
    }

    execute_current_backend_sql(manager, sql).await
}

async fn execute_current_backend_sql(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    let backend = manager.get_database_backend();
    manager
        .get_connection()
        .execute(Statement::from_string(backend, sql))
        .await?;

    Ok(())
}
