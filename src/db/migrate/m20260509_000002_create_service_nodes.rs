use sea_orm::DbErr;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ServiceNodes::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ServiceNodes::NodeId)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ServiceNodes::Hostname).string())
                    .col(ColumnDef::new(ServiceNodes::MachineIp).string())
                    .col(
                        ColumnDef::new(ServiceNodes::IngestBindAddress)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ServiceNodes::ManagementBindAddress)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ServiceNodes::Version).string().not_null())
                    .col(ColumnDef::new(ServiceNodes::Status).string().not_null())
                    .col(
                        ColumnDef::new(ServiceNodes::StartedAt)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ServiceNodes::LastSeenAt)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ServiceNodes::UpdatedAt)
                            .big_integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ServiceNodes::MetadataJson).text())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_service_nodes_status")
                    .table(ServiceNodes::Table)
                    .col(ServiceNodes::Status)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_service_nodes_last_seen_at")
                    .table(ServiceNodes::Table)
                    .col(ServiceNodes::LastSeenAt)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(ServiceNodes::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum ServiceNodes {
    Table,
    NodeId,
    Hostname,
    MachineIp,
    IngestBindAddress,
    ManagementBindAddress,
    Version,
    Status,
    StartedAt,
    LastSeenAt,
    UpdatedAt,
    MetadataJson,
}
