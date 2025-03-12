use crate::common::*;

#[doc = "음식점 타입 정보 데이터"]
#[derive(Debug, FromQueryResult)]
pub struct StoreTypesResult {
    pub seq: i32,
    pub major_type: i32,
    pub sub_type: i32,
}

#[doc = "음식점 대분류 정보, 소분류 정보 를 가지고 있는 객체"]
#[derive(Debug, Serialize, new)]
pub struct StoreTypesMap {
    pub store_type_major_map: HashMap<i32, Vec<i32>>,
    pub store_type_sub_map: HashMap<i32, Vec<i32>>,
}
