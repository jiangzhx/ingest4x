use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "processor_scripts")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub script_key: String,
    pub name: String,
    pub entry_module: String,
    pub version: i32,
    pub status: String,
    pub checksum: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub activated_at: Option<i64>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
