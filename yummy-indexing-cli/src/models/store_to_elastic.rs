use crate::common::*;

#[doc = "MySQL 와 맵핑할 구조체"]
#[derive(Debug, FromQueryResult)]
pub struct StoreResult {
    pub seq: i32,
    pub name: String,
    pub r#type: Option<String>,
    pub address: Option<String>,
    #[sea_orm(column_type = "Decimal(Some((10, 7)))")]
    pub lat: Decimal,
    #[sea_orm(column_type = "Decimal(Some((10, 7)))")]
    pub lng: Decimal,
    pub zero_possible: bool,
    pub recommend_name: Option<String>,
}

#[doc = "Elasticsearch 와 mapping 할 구조체"]
#[derive(Debug, Serialize, new)]
pub struct DistinctStoreResult {
    pub timestamp: String,
    pub seq: i32,
    pub name: String,
    pub r#type: Option<String>,
    pub address: Option<String>,
    pub lat: Decimal,
    pub lng: Decimal,
    pub zero_possible: bool,
    pub recommend_names: Vec<String>,
}
