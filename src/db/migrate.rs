use sea_orm::DbErr;
use sea_orm_migration::prelude::*;

mod m20260509_000001_create_current_schema;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(m20260509_000001_create_current_schema::Migration)]
    }
}

pub async fn run(db: &sea_orm::DatabaseConnection) -> Result<(), DbErr> {
    Migrator::up(db, None).await
}
