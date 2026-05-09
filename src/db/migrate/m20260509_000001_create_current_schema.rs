use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_core_tables(manager).await?;
        create_event_sink_tables(manager).await?;
        create_processor_tables(manager).await?;
        create_sqlite_indexes(manager).await?;
        seed_app_meta(manager).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for table in [
            ProjectProcessors::Table.to_string(),
            ProcessorScriptModules::Table.to_string(),
            ProcessorScripts::Table.to_string(),
            EventSinks::Table.to_string(),
            DeliveryTargets::Table.to_string(),
            ProjectRuleSets::Table.to_string(),
            Rules::Table.to_string(),
            RuleSets::Table.to_string(),
            AppMeta::Table.to_string(),
            Projects::Table.to_string(),
        ] {
            manager
                .drop_table(
                    Table::drop()
                        .table(Alias::new(&table))
                        .if_exists()
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }
}

async fn create_core_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Projects::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(Projects::Id)
                        .integer()
                        .not_null()
                        .auto_increment()
                        .primary_key(),
                )
                .col(
                    ColumnDef::new(Projects::IngestToken)
                        .string()
                        .not_null()
                        .unique_key(),
                )
                .col(ColumnDef::new(Projects::Name).string().not_null())
                .col(ColumnDef::new(Projects::Enabled).boolean().not_null())
                .col(ColumnDef::new(Projects::CreatedAt).big_integer().not_null())
                .col(ColumnDef::new(Projects::UpdatedAt).big_integer().not_null())
                .to_owned(),
        )
        .await?;

    manager
        .create_table(
            Table::create()
                .table(AppMeta::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(AppMeta::Key)
                        .string()
                        .not_null()
                        .primary_key(),
                )
                .col(ColumnDef::new(AppMeta::Value).string().not_null())
                .to_owned(),
        )
        .await?;

    manager
        .create_table(
            Table::create()
                .table(RuleSets::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(RuleSets::Id)
                        .integer()
                        .not_null()
                        .auto_increment()
                        .primary_key(),
                )
                .col(
                    ColumnDef::new(RuleSets::Name)
                        .string()
                        .not_null()
                        .unique_key(),
                )
                .col(ColumnDef::new(RuleSets::Description).string())
                .col(ColumnDef::new(RuleSets::Enabled).boolean().not_null())
                .col(ColumnDef::new(RuleSets::WildcardRuleId).integer())
                .col(ColumnDef::new(RuleSets::CreatedAt).big_integer().not_null())
                .col(ColumnDef::new(RuleSets::UpdatedAt).big_integer().not_null())
                .to_owned(),
        )
        .await?;

    manager
        .create_table(
            Table::create()
                .table(Rules::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(Rules::Id)
                        .integer()
                        .not_null()
                        .auto_increment()
                        .primary_key(),
                )
                .col(ColumnDef::new(Rules::RuleSetId).integer().not_null())
                .col(ColumnDef::new(Rules::ParentId).integer())
                .col(ColumnDef::new(Rules::Name).string().not_null())
                .col(ColumnDef::new(Rules::Xwhat).string())
                .col(ColumnDef::new(Rules::Content).text().not_null())
                .col(ColumnDef::new(Rules::Enabled).boolean().not_null())
                .col(ColumnDef::new(Rules::CreatedAt).big_integer().not_null())
                .col(ColumnDef::new(Rules::UpdatedAt).big_integer().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_rules_rule_set_id")
                        .from(Rules::Table, Rules::RuleSetId)
                        .to(RuleSets::Table, RuleSets::Id)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_rules_parent_id")
                        .from(Rules::Table, Rules::ParentId)
                        .to(Rules::Table, Rules::Id)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .to_owned(),
        )
        .await?;

    manager
        .create_table(
            Table::create()
                .table(ProjectRuleSets::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(ProjectRuleSets::Id)
                        .integer()
                        .not_null()
                        .auto_increment()
                        .primary_key(),
                )
                .col(
                    ColumnDef::new(ProjectRuleSets::ProjectId)
                        .integer()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(ProjectRuleSets::RuleSetId)
                        .integer()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(ProjectRuleSets::Enabled)
                        .boolean()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(ProjectRuleSets::CreatedAt)
                        .big_integer()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(ProjectRuleSets::UpdatedAt)
                        .big_integer()
                        .not_null(),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_project_rule_sets_project_id")
                        .from(ProjectRuleSets::Table, ProjectRuleSets::ProjectId)
                        .to(Projects::Table, Projects::Id)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_project_rule_sets_rule_set_id")
                        .from(ProjectRuleSets::Table, ProjectRuleSets::RuleSetId)
                        .to(RuleSets::Table, RuleSets::Id)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .index(
                    Index::create()
                        .name("idx_project_rule_sets_unique_project_rule_set")
                        .col(ProjectRuleSets::ProjectId)
                        .col(ProjectRuleSets::RuleSetId)
                        .unique(),
                )
                .to_owned(),
        )
        .await?;

    Ok(())
}

async fn create_event_sink_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(DeliveryTargets::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(DeliveryTargets::Id)
                        .integer()
                        .not_null()
                        .auto_increment()
                        .primary_key(),
                )
                .col(
                    ColumnDef::new(DeliveryTargets::TargetId)
                        .string()
                        .not_null()
                        .unique_key(),
                )
                .col(ColumnDef::new(DeliveryTargets::Name).string().not_null())
                .col(
                    ColumnDef::new(DeliveryTargets::TargetType)
                        .string()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeliveryTargets::ConfigJson)
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeliveryTargets::Enabled)
                        .boolean()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeliveryTargets::CreatedAt)
                        .big_integer()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(DeliveryTargets::UpdatedAt)
                        .big_integer()
                        .not_null(),
                )
                .to_owned(),
        )
        .await?;

    manager
        .create_table(
            Table::create()
                .table(EventSinks::Table)
                .if_not_exists()
                .col(
                    ColumnDef::new(EventSinks::Id)
                        .integer()
                        .not_null()
                        .auto_increment()
                        .primary_key(),
                )
                .col(
                    ColumnDef::new(EventSinks::SinkId)
                        .string()
                        .not_null()
                        .unique_key(),
                )
                .col(ColumnDef::new(EventSinks::Name).string().not_null())
                .col(
                    ColumnDef::new(EventSinks::DeliveryTargetId)
                        .integer()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(EventSinks::DestinationJson)
                        .text()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(EventSinks::AutoOffsetReset)
                        .string()
                        .not_null(),
                )
                .col(ColumnDef::new(EventSinks::Enabled).boolean().not_null())
                .col(
                    ColumnDef::new(EventSinks::CreatedAt)
                        .big_integer()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(EventSinks::UpdatedAt)
                        .big_integer()
                        .not_null(),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_event_sinks_delivery_target_id")
                        .from(EventSinks::Table, EventSinks::DeliveryTargetId)
                        .to(DeliveryTargets::Table, DeliveryTargets::Id)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await?;

    Ok(())
}

async fn create_processor_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
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

    Ok(())
}

async fn create_sqlite_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for sql in [
        r#"
CREATE UNIQUE INDEX IF NOT EXISTS rules_unique_rule_set_xwhat
ON rules(rule_set_id, xwhat)
WHERE xwhat IS NOT NULL
"#,
        r#"
CREATE UNIQUE INDEX IF NOT EXISTS rules_unique_rule_set_root_name
ON rules(rule_set_id, name)
WHERE parent_id IS NULL
"#,
        r#"
CREATE UNIQUE INDEX IF NOT EXISTS rules_unique_rule_set_child_name
ON rules(rule_set_id, parent_id, name)
WHERE parent_id IS NOT NULL
"#,
        r#"
CREATE UNIQUE INDEX IF NOT EXISTS project_rule_sets_unique_project
ON project_rule_sets(project_id)
"#,
    ] {
        execute_backend_sql(manager, DbBackend::Sqlite, sql).await?;
    }

    Ok(())
}

async fn seed_app_meta(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for (sqlite_sql, mysql_sql) in [
        (
            "INSERT OR IGNORE INTO app_meta (key, value) VALUES ('projects_version', '0')",
            "INSERT IGNORE INTO app_meta (`key`, value) VALUES ('projects_version', '0')",
        ),
        (
            "INSERT OR IGNORE INTO app_meta (key, value) VALUES ('event_sinks_version', '0')",
            "INSERT IGNORE INTO app_meta (`key`, value) VALUES ('event_sinks_version', '0')",
        ),
        (
            "INSERT OR IGNORE INTO app_meta (key, value) VALUES ('processor_scripts_version', '0')",
            "INSERT IGNORE INTO app_meta (`key`, value) VALUES ('processor_scripts_version', '0')",
        ),
    ] {
        execute_backend_sql(manager, DbBackend::Sqlite, sqlite_sql).await?;
        execute_backend_sql(manager, DbBackend::MySql, mysql_sql).await?;
    }

    Ok(())
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
    IngestToken,
    Name,
    Enabled,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum AppMeta {
    Table,
    Key,
    Value,
}

#[derive(DeriveIden)]
enum RuleSets {
    Table,
    Id,
    Name,
    Description,
    Enabled,
    WildcardRuleId,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum Rules {
    Table,
    Id,
    RuleSetId,
    ParentId,
    Name,
    Xwhat,
    Content,
    Enabled,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum ProjectRuleSets {
    Table,
    Id,
    ProjectId,
    RuleSetId,
    Enabled,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum DeliveryTargets {
    Table,
    Id,
    TargetId,
    Name,
    TargetType,
    ConfigJson,
    Enabled,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum EventSinks {
    Table,
    Id,
    SinkId,
    Name,
    DeliveryTargetId,
    DestinationJson,
    AutoOffsetReset,
    Enabled,
    CreatedAt,
    UpdatedAt,
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
