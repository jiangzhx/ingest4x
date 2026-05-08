use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "processor_script_modules")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub processor_script_id: i32,
    pub module_name: String,
    pub source: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
