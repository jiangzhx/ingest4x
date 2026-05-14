use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (sqlite_sql, mysql_sql) in [
            (
                "ALTER TABLE projects ADD COLUMN project_key TEXT",
                "ALTER TABLE projects ADD COLUMN project_key VARCHAR(255)",
            ),
            (
                "UPDATE projects SET project_key = 'project-' || id WHERE project_key IS NULL OR project_key = ''",
                "UPDATE projects SET project_key = CONCAT('project-', id) WHERE project_key IS NULL OR project_key = ''",
            ),
            (
                "ALTER TABLE projects ADD COLUMN auth_mode TEXT NOT NULL DEFAULT 'token'",
                "ALTER TABLE projects ADD COLUMN auth_mode VARCHAR(32) NOT NULL DEFAULT 'token'",
            ),
            (
                "ALTER TABLE projects ADD COLUMN allowed_ips TEXT NOT NULL DEFAULT '[]'",
                "ALTER TABLE projects ADD COLUMN allowed_ips VARCHAR(2048) NOT NULL DEFAULT '[]'",
            ),
            (
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_project_key ON projects(project_key)",
                "CREATE UNIQUE INDEX idx_projects_project_key ON projects(project_key)",
            ),
        ] {
            execute_backend_sql(manager, DbBackend::Sqlite, sqlite_sql).await?;
            execute_backend_sql(manager, DbBackend::MySql, mysql_sql).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_backend_sql(
            manager,
            DbBackend::Sqlite,
            "DROP INDEX IF EXISTS idx_projects_project_key",
        )
        .await?;
        execute_backend_sql(
            manager,
            DbBackend::MySql,
            "DROP INDEX idx_projects_project_key ON projects",
        )
        .await?;

        Ok(())
    }
}

async fn execute_backend_sql(
    manager: &SchemaManager<'_>,
    backend: DbBackend,
    sql: &str,
) -> Result<(), DbErr> {
    if manager.get_database_backend() != backend {
        return Ok(());
    }

    manager
        .get_connection()
        .execute(Statement::from_string(backend, sql))
        .await?;

    Ok(())
}
