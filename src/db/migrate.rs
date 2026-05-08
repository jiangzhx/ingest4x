use sea_orm::DbErr;
use sea_orm_migration::prelude::*;

mod m20260425_000001_create_initial_schema;
mod m20260427_000002_add_rule_wildcard_flag;
mod m20260427_000003_move_wildcard_to_rule_set;
mod m20260508_000004_create_event_sinks;
mod m20260508_000005_create_processor_scripts;

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
        ]
    }
}

pub async fn run(db: &sea_orm::DatabaseConnection) -> Result<(), DbErr> {
    Migrator::up(db, None).await
}
