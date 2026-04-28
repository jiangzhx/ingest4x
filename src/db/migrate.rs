use sea_orm::DbErr;
use sea_orm_migration::prelude::*;

mod m20260425_000001_create_initial_schema;
mod m20260427_000002_add_rule_wildcard_flag;
mod m20260427_000003_move_wildcard_to_rule_set;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260425_000001_create_initial_schema::Migration),
            Box::new(m20260427_000002_add_rule_wildcard_flag::Migration),
            Box::new(m20260427_000003_move_wildcard_to_rule_set::Migration),
        ]
    }
}

pub async fn run(db: &sea_orm::DatabaseConnection) -> Result<(), DbErr> {
    Migrator::up(db, None).await
}
