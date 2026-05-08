use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        match manager.get_database_backend() {
            DbBackend::MySql => {
                if manager.has_column(PROJECTS_TABLE, APPID_COLUMN).await? {
                    execute_sql(manager, "ALTER TABLE projects DROP COLUMN appid").await?;
                }
                execute_sql(
                    manager,
                    r#"
ALTER TABLE projects
    MODIFY COLUMN ingest_token_hash varchar(255) NOT NULL,
    MODIFY COLUMN ingest_token_prefix varchar(255) NOT NULL
"#,
                )
                .await?;
            }
            DbBackend::Sqlite => {
                if manager.has_column(PROJECTS_TABLE, APPID_COLUMN).await? {
                    rebuild_sqlite_projects_without_appid(manager).await?;
                }
            }
            DbBackend::Postgres => unreachable!("postgres is not enabled for ingest4x"),
        }

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Ok(())
    }
}

async fn rebuild_sqlite_projects_without_appid(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for sql in [
        "PRAGMA foreign_keys = OFF",
        "PRAGMA legacy_alter_table = ON",
        "DROP TABLE IF EXISTS projects_new",
        "DROP TABLE IF EXISTS projects_old",
        "ALTER TABLE projects RENAME TO projects_old",
        r#"
CREATE TABLE projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    ingest_token_hash TEXT NOT NULL UNIQUE,
    ingest_token_prefix TEXT NOT NULL,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
)
"#,
        r#"
INSERT INTO projects (
    id,
    ingest_token_hash,
    ingest_token_prefix,
    name,
    enabled,
    created_at,
    updated_at
)
SELECT
    id,
    ingest_token_hash,
    ingest_token_prefix,
    name,
    enabled,
    created_at,
    updated_at
FROM projects_old
"#,
        "DROP TABLE projects_old",
        "PRAGMA legacy_alter_table = OFF",
        "PRAGMA foreign_keys = ON",
    ] {
        execute_sql(manager, sql).await?;
    }

    Ok(())
}

async fn execute_sql(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    let backend = manager.get_database_backend();
    manager
        .get_connection()
        .execute(Statement::from_string(backend, sql))
        .await?;

    Ok(())
}

const PROJECTS_TABLE: &str = "projects";
const APPID_COLUMN: &str = "appid";
