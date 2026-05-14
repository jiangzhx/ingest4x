use sea_orm::DbErr;
use sea_orm_migration::prelude::*;

mod m20260509_000001_create_current_schema;
mod m20260509_000002_create_service_nodes;
mod m20260513_000003_add_project_ingest_auth;
mod m20260514_000004_normalize_project_auth_mode;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260509_000001_create_current_schema::Migration),
            Box::new(m20260509_000002_create_service_nodes::Migration),
            Box::new(m20260513_000003_add_project_ingest_auth::Migration),
            Box::new(m20260514_000004_normalize_project_auth_mode::Migration),
        ]
    }
}

pub async fn run(db: &sea_orm::DatabaseConnection) -> Result<(), DbErr> {
    Migrator::up(db, None).await
}
