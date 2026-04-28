use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager.has_column("rule_sets", "wildcard_rule_id").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(RuleSets::Table)
                        .add_column(ColumnDef::new(RuleSets::WildcardRuleId).integer())
                        .to_owned(),
                )
                .await?;
        }

        if manager.has_column("rules", "is_wildcard").await? {
            execute_backend_sql(
                manager,
                DbBackend::Sqlite,
                r#"
UPDATE rule_sets
SET wildcard_rule_id = (
    SELECT id
    FROM rules
    WHERE rules.rule_set_id = rule_sets.id
      AND rules.is_wildcard = 1
    ORDER BY id
    LIMIT 1
)
WHERE wildcard_rule_id IS NULL
"#,
            )
            .await?;

            execute_backend_sql(
                manager,
                DbBackend::MySql,
                r#"
UPDATE rule_sets rs
SET wildcard_rule_id = (
    SELECT id
    FROM rules r
    WHERE r.rule_set_id = rs.id
      AND r.is_wildcard = TRUE
    ORDER BY id
    LIMIT 1
)
WHERE wildcard_rule_id IS NULL
"#,
            )
            .await?;

            execute_backend_sql(
                manager,
                DbBackend::Sqlite,
                "DROP INDEX IF EXISTS rules_unique_rule_set_wildcard",
            )
            .await?;
            execute_backend_sql(
                manager,
                DbBackend::MySql,
                "DROP INDEX rules_unique_rule_set_wildcard ON rules",
            )
            .await?;
        }

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
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
enum RuleSets {
    Table,
    WildcardRuleId,
}
