use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.get_database_backend() != DbBackend::Sqlite {
            return Ok(());
        }
        if manager.has_table(PROJECTS_TABLE).await? {
            return Ok(());
        }
        if !manager.has_table(PROJECTS_NEW_TABLE).await? {
            return Ok(());
        }

        execute_sql(manager, "ALTER TABLE projects_new RENAME TO projects").await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Ok(())
    }
}

async fn execute_sql(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute(Statement::from_string(DbBackend::Sqlite, sql))
        .await?;

    Ok(())
}

const PROJECTS_TABLE: &str = "projects";
const PROJECTS_NEW_TABLE: &str = "projects_new";
