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
                        ColumnDef::new(Projects::Appid)
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
                    .index(
                        Index::create()
                            .name("idx_rules_unique_rule_set_parent_name")
                            .col(Rules::RuleSetId)
                            .col(Rules::ParentId)
                            .col(Rules::Name)
                            .unique(),
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

        execute_sqlite_only(
            manager,
            r#"
CREATE UNIQUE INDEX IF NOT EXISTS rules_unique_rule_set_xwhat
ON rules(rule_set_id, xwhat)
WHERE xwhat IS NOT NULL
"#,
        )
        .await?;

        execute_sqlite_only(
            manager,
            r#"
CREATE UNIQUE INDEX IF NOT EXISTS rules_unique_rule_set_root_name
ON rules(rule_set_id, name)
WHERE parent_id IS NULL
"#,
        )
        .await?;

        execute_sqlite_only(
            manager,
            r#"
CREATE UNIQUE INDEX IF NOT EXISTS rules_unique_rule_set_child_name
ON rules(rule_set_id, parent_id, name)
WHERE parent_id IS NOT NULL
"#,
        )
        .await?;

        execute_sqlite_only(
            manager,
            r#"
DELETE FROM project_rule_sets
WHERE id NOT IN (
    SELECT MAX(id)
    FROM project_rule_sets
    GROUP BY project_id
)
"#,
        )
        .await?;

        execute_sqlite_only(
            manager,
            r#"
CREATE UNIQUE INDEX IF NOT EXISTS project_rule_sets_unique_project
ON project_rule_sets(project_id)
"#,
        )
        .await?;

        execute_backend_sql(
            manager,
            DbBackend::Sqlite,
            r#"
INSERT OR IGNORE INTO app_meta (key, value)
VALUES ('projects_version', '0')
"#,
        )
        .await?;

        execute_backend_sql(
            manager,
            DbBackend::MySql,
            r#"
INSERT IGNORE INTO app_meta (`key`, value)
VALUES ('projects_version', '0')
"#,
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(ProjectRuleSets::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(Rules::Table).if_exists().to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(RuleSets::Table).if_exists().to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(AppMeta::Table).if_exists().to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(Projects::Table).if_exists().to_owned())
            .await?;

        Ok(())
    }
}

async fn execute_sqlite_only(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    execute_backend_sql(manager, DbBackend::Sqlite, sql).await
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
    Appid,
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
