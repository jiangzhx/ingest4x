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

        execute_backend_sql(
            manager,
            DbBackend::Sqlite,
            r#"
INSERT OR IGNORE INTO app_meta (key, value)
VALUES ('event_sinks_version', '0')
"#,
        )
        .await?;

        execute_backend_sql(
            manager,
            DbBackend::MySql,
            r#"
INSERT IGNORE INTO app_meta (`key`, value)
VALUES ('event_sinks_version', '0')
"#,
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(EventSinks::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(DeliveryTargets::Table)
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
