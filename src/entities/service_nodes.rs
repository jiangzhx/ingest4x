use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "service_nodes")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub node_id: String,
    pub hostname: Option<String>,
    pub machine_ip: Option<String>,
    pub ingest_bind_address: String,
    pub management_bind_address: String,
    pub version: String,
    pub status: String,
    pub started_at: i64,
    pub last_seen_at: i64,
    pub updated_at: i64,
    pub metadata_json: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
