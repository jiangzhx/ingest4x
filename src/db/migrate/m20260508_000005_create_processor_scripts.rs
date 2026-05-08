use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ProcessorScripts::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProcessorScripts::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(ProcessorScripts::ScriptKey)
                            .string()
                            .not_null()
                            .unique_key(),
                    )
                    .col(ColumnDef::new(ProcessorScripts::Name).string().not_null())
                    .col(
                        ColumnDef::new(ProcessorScripts::EntryModule)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProcessorScripts::Version)
                            .integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ProcessorScripts::Status).string().not_null())
                    .col(
                        ColumnDef::new(ProcessorScripts::Checksum)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProcessorScripts::CreatedAt)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProcessorScripts::UpdatedAt)
                            .big_integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ProcessorScripts::ActivatedAt).big_integer())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(ProcessorScriptModules::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProcessorScriptModules::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(ProcessorScriptModules::ProcessorScriptId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProcessorScriptModules::ModuleName)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProcessorScriptModules::Source)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProcessorScriptModules::CreatedAt)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProcessorScriptModules::UpdatedAt)
                            .big_integer()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_processor_modules_script_id")
                            .from(
                                ProcessorScriptModules::Table,
                                ProcessorScriptModules::ProcessorScriptId,
                            )
                            .to(ProcessorScripts::Table, ProcessorScripts::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .index(
                        Index::create()
                            .name("idx_processor_modules_unique_script_module")
                            .col(ProcessorScriptModules::ProcessorScriptId)
                            .col(ProcessorScriptModules::ModuleName)
                            .unique(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(ProjectProcessors::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProjectProcessors::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(ProjectProcessors::ProjectId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProjectProcessors::ProcessorScriptId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProjectProcessors::Enabled)
                            .boolean()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProjectProcessors::CreatedAt)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProjectProcessors::UpdatedAt)
                            .big_integer()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_project_processors_project_id")
                            .from(ProjectProcessors::Table, ProjectProcessors::ProjectId)
                            .to(Projects::Table, Projects::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_project_processors_script_id")
                            .from(
                                ProjectProcessors::Table,
                                ProjectProcessors::ProcessorScriptId,
                            )
                            .to(ProcessorScripts::Table, ProcessorScripts::Id)
                            .on_delete(ForeignKeyAction::Restrict),
                    )
                    .index(
                        Index::create()
                            .name("idx_project_processors_unique_project")
                            .col(ProjectProcessors::ProjectId)
                            .unique(),
                    )
                    .to_owned(),
            )
            .await?;

        execute_backend_sql(
            manager,
            DbBackend::Sqlite,
            r#"
INSERT OR IGNORE INTO app_meta (key, value)
VALUES ('processor_scripts_version', '0')
"#,
        )
        .await?;

        execute_backend_sql(
            manager,
            DbBackend::MySql,
            r#"
INSERT IGNORE INTO app_meta (`key`, value)
VALUES ('processor_scripts_version', '0')
"#,
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(ProjectProcessors::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(ProcessorScriptModules::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(ProcessorScripts::Table)
                    .if_exists()
                    .to_owned(),
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

#[derive(DeriveIden)]
enum Projects {
    Table,
    Id,
}

#[derive(DeriveIden)]
enum ProcessorScripts {
    Table,
    Id,
    ScriptKey,
    Name,
    EntryModule,
    Version,
    Status,
    Checksum,
    CreatedAt,
    UpdatedAt,
    ActivatedAt,
}

#[derive(DeriveIden)]
enum ProcessorScriptModules {
    Table,
    Id,
    ProcessorScriptId,
    ModuleName,
    Source,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum ProjectProcessors {
    Table,
    Id,
    ProjectId,
    ProcessorScriptId,
    Enabled,
    CreatedAt,
    UpdatedAt,
}
