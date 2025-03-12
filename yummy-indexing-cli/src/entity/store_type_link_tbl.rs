//! `SeaORM` Entity, @generated by sea-orm-codegen 1.1.5

use sea_orm::entity::prelude::*;

use crate::entity::store;
use crate::entity::store_type_link_tbl;
use crate::entity::store_type_sub;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "store_type_link_tbl")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub sub_type: i32,
    #[sea_orm(primary_key, auto_increment = false)]
    pub seq: i32,
    pub reg_dt: DateTime,
    pub chg_dt: Option<DateTime>,
    pub reg_id: String,
    pub chg_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::store::Entity",
        from = "Column::Seq",
        to = "super::store::Column::Seq"
    )]
    Store,
    #[sea_orm(
        belongs_to = "super::store_type_sub::Entity",
        from = "Column::SubType",
        to = "super::store_type_sub::Column::SubType"
    )]
    StoreTypeSub,
}

impl Related<store::Entity> for store_type_link_tbl::Entity {
    fn to() -> RelationDef {
        Relation::Store.def()
    }
}

impl Related<store_type_sub::Entity> for store_type_link_tbl::Entity {
    fn to() -> RelationDef {
        Relation::StoreTypeSub.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
