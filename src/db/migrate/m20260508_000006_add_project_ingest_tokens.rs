use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement, Value};
use sea_orm_migration::prelude::*;
use sha2::{Digest, Sha256};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column(PROJECTS_TABLE, INGEST_TOKEN_HASH_COLUMN)
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Projects::Table)
                        .add_column(ColumnDef::new(Projects::IngestTokenHash).string())
                        .to_owned(),
                )
                .await?;
        }

        if !manager
            .has_column(PROJECTS_TABLE, INGEST_TOKEN_PREFIX_COLUMN)
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Projects::Table)
                        .add_column(ColumnDef::new(Projects::IngestTokenPrefix).string())
                        .to_owned(),
                )
                .await?;
        }

        if manager.has_column(PROJECTS_TABLE, APPID_COLUMN).await? {
            backfill_legacy_project_tokens(manager).await?;
        }

        manager
            .create_index(
                Index::create()
                    .name("idx_projects_ingest_token_hash")
                    .table(Projects::Table)
                    .col(Projects::IngestTokenHash)
                    .unique()
                    .if_not_exists()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_projects_ingest_token_hash")
                    .table(Projects::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        if manager
            .has_column(PROJECTS_TABLE, INGEST_TOKEN_PREFIX_COLUMN)
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Projects::Table)
                        .drop_column(Projects::IngestTokenPrefix)
                        .to_owned(),
                )
                .await?;
        }

        if manager
            .has_column(PROJECTS_TABLE, INGEST_TOKEN_HASH_COLUMN)
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Projects::Table)
                        .drop_column(Projects::IngestTokenHash)
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }
}

async fn backfill_legacy_project_tokens(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let backend = manager.get_database_backend();
    let rows = manager
        .get_connection()
        .query_all(Statement::from_string(
            backend,
            r#"
SELECT id, appid
FROM projects
WHERE ingest_token_hash IS NULL OR ingest_token_hash = ''
"#,
        ))
        .await?;

    for row in rows {
        let id = row.try_get::<i32>("", "id")?;
        let appid = row.try_get::<String>("", "appid")?;
        let token = format!("igx_{appid}");
        let hash = hash_ingest_token(&token);
        let prefix = ingest_token_prefix(&token);

        manager
            .get_connection()
            .execute(Statement::from_sql_and_values(
                backend,
                update_project_token_sql(backend),
                vec![Value::from(hash), Value::from(prefix), Value::from(id)],
            ))
            .await?;
    }

    Ok(())
}

fn update_project_token_sql(backend: DbBackend) -> &'static str {
    match backend {
        DbBackend::MySql => {
            "UPDATE projects SET ingest_token_hash = ?, ingest_token_prefix = ? WHERE id = ?"
        }
        DbBackend::Sqlite => {
            "UPDATE projects SET ingest_token_hash = ?, ingest_token_prefix = ? WHERE id = ?"
        }
        DbBackend::Postgres => unreachable!("postgres is not enabled for ingest4x"),
    }
}

fn hash_ingest_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    format!("{digest:x}")
}

fn ingest_token_prefix(token: &str) -> String {
    let prefix = token.chars().take(12).collect::<String>();
    if token.chars().count() > 12 {
        format!("{prefix}...")
    } else {
        prefix
    }
}

#[derive(DeriveIden)]
enum Projects {
    Table,
    IngestTokenHash,
    IngestTokenPrefix,
}

const PROJECTS_TABLE: &str = "projects";
const APPID_COLUMN: &str = "appid";
const INGEST_TOKEN_HASH_COLUMN: &str = "ingest_token_hash";
const INGEST_TOKEN_PREFIX_COLUMN: &str = "ingest_token_prefix";
