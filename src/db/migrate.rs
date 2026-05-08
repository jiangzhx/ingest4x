use sea_orm::DbErr;
use sea_orm_migration::prelude::*;

mod m20260425_000001_create_initial_schema;
mod m20260427_000002_add_rule_wildcard_flag;
mod m20260427_000003_move_wildcard_to_rule_set;
mod m20260508_000004_create_event_sinks;
mod m20260508_000005_create_processor_scripts;
mod m20260508_000006_add_project_ingest_tokens;
mod m20260508_000007_drop_legacy_project_appid;
mod m20260508_000008_repair_sqlite_projects_table;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260425_000001_create_initial_schema::Migration),
            Box::new(m20260427_000002_add_rule_wildcard_flag::Migration),
            Box::new(m20260427_000003_move_wildcard_to_rule_set::Migration),
            Box::new(m20260508_000004_create_event_sinks::Migration),
            Box::new(m20260508_000005_create_processor_scripts::Migration),
            Box::new(m20260508_000006_add_project_ingest_tokens::Migration),
            Box::new(m20260508_000007_drop_legacy_project_appid::Migration),
            Box::new(m20260508_000008_repair_sqlite_projects_table::Migration),
        ]
    }
}

pub async fn run(db: &sea_orm::DatabaseConnection) -> Result<(), DbErr> {
    Migrator::up(db, None).await
}
