use crate::common::*;

use crate::configuration::index_schedules_config::*;

use crate::models::store_to_elastic::*;
use crate::models::store_types::*;

use crate::repository::mysql_repository::*;

use crate::utils_module::time_utils::*;

use crate::entity::{
    elastic_index_info_tbl, recommend_tbl, store, store_location_info_tbl, store_recommend_tbl,
    store_type_link_tbl, store_type_major, store_type_sub, zero_possible_market,
};

pub trait QueryService {
    async fn get_store_by_batch(
        &self,
        batch_size: usize,
        query_filter: Condition,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<StoreResult>, anyhow::Error>;
    async fn get_all_store_table(
        &self,
        index_schedule: &IndexSchedules,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error>;
    async fn get_specific_store_table(
        &self,
        index_schedule: &IndexSchedules,
        cur_utc_date: NaiveDateTime,
        recent_datetime: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error>;
    fn get_distinct_store_table(
        &self,
        stores: &[StoreResult],
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error>;
    async fn get_recent_date_from_elastic_index_info(
        &self,
        index_schedule: &IndexSchedules,
    ) -> Result<NaiveDateTime, anyhow::Error>;
    async fn update_recent_date_to_elastic_index_info(
        &self,
        index_schedule: &IndexSchedules,
        new_datetime: NaiveDateTime,
    ) -> Result<(), anyhow::Error>;
    async fn get_store_types(
        &self,
        store_seqs: Option<Vec<i32>>,
    ) -> Result<StoreTypesMap, anyhow::Error>;
}

#[derive(Debug, new)]
pub struct QueryServicePub;

impl QueryService for QueryServicePub {
    #[doc = "store 색인 관련 배치 함수"]
    /// # Arguments
    /// * `batch_size` - 쿼리 배치 사이즈
    /// * `query_filter` - 쿼리 필터
    /// * `cur_utc_date` - 현재 시각
    ///
    /// # Returns
    /// * Result<Vec<StoreResult>, anyhow::Error>
    async fn get_store_by_batch(
        &self,
        batch_size: usize,
        query_filter: Condition,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<StoreResult>, anyhow::Error> {
        let db: &DatabaseConnection = establish_connection().await;

        let mut total_store_list: Vec<StoreResult> = Vec::new();
        let mut last_seq: Option<i32> = None;

        loop {
            let mut query: Select<store::Entity> = store::Entity::find()
                .inner_join(store_type_link_tbl::Entity)
                .inner_join(store_location_info_tbl::Entity)
                .left_join(zero_possible_market::Entity)
                .left_join(store_recommend_tbl::Entity)
                .join(
                    JoinType::LeftJoin,
                    store_recommend_tbl::Relation::RecommendTbl
                        .def()
                        .on_condition(move |_r, _| {
                            Condition::all()
                                .add(Expr::col(recommend_tbl::Column::RecommendYn).eq("Y"))
                                .add(
                                    Expr::col(store_recommend_tbl::Column::RecommendEndDt)
                                        .gt(cur_utc_date),
                                )
                        }),
                )
                .order_by_asc(store::Column::Seq)
                .limit(batch_size as u64)
                .select_only()
                .columns([store::Column::Seq, store::Column::Name, store::Column::Type])
                .expr_as(
                    Expr::case(
                        Expr::col((
                            zero_possible_market::Entity,
                            zero_possible_market::Column::UseYn,
                        ))
                        .eq("N"),
                        false,
                    )
                    .case(
                        Expr::col((
                            zero_possible_market::Entity,
                            zero_possible_market::Column::Name,
                        ))
                        .is_not_null(),
                        true,
                    )
                    .finally(false),
                    "zero_possible",
                )
                .column_as(store_location_info_tbl::Column::Address, "address")
                .column_as(store_location_info_tbl::Column::Lat, "lat")
                .column_as(store_location_info_tbl::Column::Lng, "lng")
                .column_as(recommend_tbl::Column::RecommendName, "recommend_name")
                .column_as(
                    store_location_info_tbl::Column::LocationCity,
                    "location_city",
                )
                .column_as(
                    store_location_info_tbl::Column::LocationCounty,
                    "location_county",
                )
                .column_as(
                    store_location_info_tbl::Column::LocationDistrict,
                    "location_district",
                )
                .filter(query_filter.clone());

            if let Some(seq) = last_seq {
                query = query.filter(store::Column::Seq.gt(seq)); /* `seq`가 마지막 값보다 큰 데이터 가져오기 */
            }

            let mut store_results: Vec<StoreResult> = query.into_model().all(db).await?;

            if store_results.is_empty() {
                break;
            }

            total_store_list.append(&mut store_results);
            last_seq = total_store_list.last().map(|s| s.seq);
        }

        Ok(total_store_list)
    }

    #[doc = "색인할 Store 정보를 조회해주는 함수 -> 모든 정보를 가져와준다: 정적색인 용도"]
    /// # Arguments
    /// * `index_schedule` - index_schedule 정보
    /// * `cur_utc_date` - 현재 시각
    ///
    /// # Returns
    /// * Result<Vec<DistinctStoreResult>, anyhow::Error>
    async fn get_all_store_table(
        &self,
        index_schedule: &IndexSchedules,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error> {
        let batch_size: usize = *index_schedule.es_batch_size();
        let query_filter: Condition =
            Condition::all().add(Expr::col((store::Entity, store::Column::UseYn)).eq("Y"));

        /* 중복이 존재하는 store 리스트 */
        let stores: Vec<StoreResult> = self
            .get_store_by_batch(batch_size, query_filter, cur_utc_date)
            .await?;

        /* 중복을 제외한 store 리스트 */
        let stores_distinct: Vec<DistinctStoreResult> =
            self.get_distinct_store_table(&stores, cur_utc_date)?;

        Ok(stores_distinct)
    }

    #[doc = "색인할 Store 정보를 조회해주는 함수 -> 특정 정보를 가져와준다: 증분색인 용도"]
    /// # Arguments
    /// * `index_schedule` - index_schedule 정보
    /// * `cur_utc_date` - 현재 시각정보
    /// * `recent_datetime` - 가장 최근 색인 시각정보
    ///
    /// # Returns
    /// * Result<Vec<DistinctStoreResult>, anyhow::Error>
    async fn get_specific_store_table(
        &self,
        index_schedule: &IndexSchedules,
        cur_utc_date: NaiveDateTime,
        recent_datetime: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error> {
        let batch_size: usize = *index_schedule.es_batch_size();

        let query_filter: Condition = Condition::all()
            .add(Expr::col((store::Entity, store::Column::UseYn)).eq("Y"))
            .add(
                Condition::any()
                    .add(Expr::col((store::Entity, store::Column::ChgDt)).gt(recent_datetime))
                    .add(Expr::col((store::Entity, store::Column::RegDt)).gt(recent_datetime))
                    .add(
                        Expr::col((
                            zero_possible_market::Entity,
                            zero_possible_market::Column::ChgDt,
                        ))
                        .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            zero_possible_market::Entity,
                            zero_possible_market::Column::RegDt,
                        ))
                        .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            store_recommend_tbl::Entity,
                            store_recommend_tbl::Column::ChgDt,
                        ))
                        .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            store_recommend_tbl::Entity,
                            store_recommend_tbl::Column::RegDt,
                        ))
                        .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            store_location_info_tbl::Entity,
                            store_location_info_tbl::Column::ChgDt,
                        ))
                        .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            store_location_info_tbl::Entity,
                            store_location_info_tbl::Column::RegDt,
                        ))
                        .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            store_type_link_tbl::Entity,
                            store_type_link_tbl::Column::ChgDt,
                        ))
                        .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            store_type_link_tbl::Entity,
                            store_type_link_tbl::Column::RegDt,
                        ))
                        .gt(recent_datetime),
                    ),
            );

        /* 중복이 존재하는 store 리스트 */
        let stores: Vec<StoreResult> = self
            .get_store_by_batch(batch_size, query_filter, cur_utc_date)
            .await?;

        /* 중복을 제외한 store 리스트 */
        let stores_distinct: Vec<DistinctStoreResult> =
            self.get_distinct_store_table(&stores, cur_utc_date)?;

        Ok(stores_distinct)
    }

    #[doc = "색인할 Store 정보를 조회해주는 함수 -> 중복 제거"]
    /// # Arguments
    /// * `stores` - store 데이터 객체 리스트
    /// * `cur_utc_date` - 현재 UTC 기준 시간 데이터
    ///
    /// # Returns
    /// * Result<Vec<DistinctStoreResult>, anyhow::Error>
    fn get_distinct_store_table(
        &self,
        stores: &[StoreResult],
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error> {
        let mut store_map: HashMap<i32, DistinctStoreResult> = HashMap::new();
        let cur_time_utc: String = get_str_from_naive_datetime(cur_utc_date);

        for store in stores {
            store_map
                .entry(store.seq)
                .and_modify(|existing| {
                    if let Some(recommend) = &store.recommend_name {
                        existing.recommend_names.push(recommend.to_string());
                    }
                })
                .or_insert_with(|| {
                    DistinctStoreResult::new(
                        cur_time_utc.clone(),
                        store.seq,
                        store.name.clone(),
                        store.r#type.clone(),
                        store.address.clone(),
                        store.lat,
                        store.lng,
                        store.zero_possible,
                        store.recommend_name.clone().map_or(vec![], |r| vec![r]),
                        store.location_city.clone(),
                        store.location_county.clone(),
                        store.location_district.clone(),
                        Vec::new(),
                        Vec::new(),
                    )
                });
        }

        let result: Vec<DistinctStoreResult> = store_map.into_values().collect();
        Ok(result)
    }

    #[doc = "특정 인덱스에서 가장 최근에 색인된 날짜/시간 정보를 가져와주는 함수"]
    /// # Arguments
    /// * `index_schedule` - 인덱스 스케쥴 정보
    ///
    /// # Returns
    /// * Result<NaiveDateTime, anyhow::Error>
    async fn get_recent_date_from_elastic_index_info(
        &self,
        index_schedule: &IndexSchedules,
    ) -> Result<NaiveDateTime, anyhow::Error> {
        let index_name: &String = index_schedule.index_name();

        let db: &DatabaseConnection = establish_connection().await;

        let query: Select<elastic_index_info_tbl::Entity> = elastic_index_info_tbl::Entity::find()
            .filter(elastic_index_info_tbl::Column::IndexName.eq(index_name));

        let query_results: Vec<elastic_index_info_tbl::Model> = query.all(db).await?;

        if query_results.is_empty() {
            return Err(anyhow!(
                "[Error][get_recent_date_from_elastic_index_info()] query_results is EMPTY"
            ));
        }

        let recent_datetime: NaiveDateTime = query_results
            .get(0)
            .ok_or_else(|| anyhow!("[Error][get_recent_date_from_elastic_index_info()] The first element of 'query_results' does not exist."))?
            .chg_dt;

        Ok(recent_datetime)
    }

    #[doc = "elastic_index_info 테이블의 chg_dt 데이터를 update 해주는 함수 - 색인시간 최신화"]
    /// # Arguments
    /// * `index_schedule` - 인덱스 스케쥴 정보
    /// * `new_datetime` - 새로운 날짜/시간 데이터
    ///
    /// # Returns
    /// * Result<NaiveDateTime, anyhow::Error>
    async fn update_recent_date_to_elastic_index_info(
        &self,
        index_schedule: &IndexSchedules,
        new_datetime: NaiveDateTime,
    ) -> Result<(), anyhow::Error> {
        let index_name: &String = index_schedule.index_name();

        let db: &DatabaseConnection = establish_connection().await;

        elastic_index_info_tbl::Entity::update_many()
            .col_expr(
                elastic_index_info_tbl::Column::ChgDt,
                Expr::value(new_datetime),
            )
            .filter(elastic_index_info_tbl::Column::IndexName.eq(index_name))
            .exec(db)
            .await?;

        Ok(())
    }

    #[doc = "음식점 정보와 맵핑되는 대분류, 소분류 정보를 모두 가져와 준다."]
    /// # Arguments
    /// * `store_seqs` - 상점 고유번호 리스트
    ///
    /// # Returns
    /// * Result<StoreTypesMap, anyhow::Error>
    async fn get_store_types(
        &self,
        store_seqs: Option<Vec<i32>>,
    ) -> Result<StoreTypesMap, anyhow::Error> {
        let db: &DatabaseConnection = establish_connection().await;

        let query_filter: Condition = if let Some(seqs) = store_seqs {
            Condition::any().add(store::Column::Seq.is_in(seqs))
        } else {
            Condition::all()
        };

        let query: Select<store::Entity> = store::Entity::find()
            .join(JoinType::InnerJoin, store::Relation::StoreTypeLinkTbl.def())
            .join(
                JoinType::InnerJoin,
                store_type_link_tbl::Relation::StoreTypeSub.def(),
            )
            .join(
                JoinType::InnerJoin,
                store_type_sub::Relation::StoreTypeMajor.def(),
            )
            .select_only()
            .columns([store::Column::Seq])
            .column_as(store_type_sub::Column::SubType, "sub_type")
            .column_as(store_type_major::Column::MajorType, "major_type")
            .filter(query_filter);

        let store_types_result: Vec<StoreTypesResult> = query.into_model().all(db).await?;

        let mut store_type_major_map: HashMap<i32, Vec<i32>> = HashMap::new();
        let mut store_type_sub_map: HashMap<i32, Vec<i32>> = HashMap::new();

        /* 중복체크를 위한 HashSet */
        let mut major_seen: HashMap<i32, HashSet<i32>> = HashMap::new();
        let mut sub_seen: HashMap<i32, HashSet<i32>> = HashMap::new();

        for store in &store_types_result {
            store_type_major_map
                .entry(store.seq)
                .or_insert_with(Vec::new);

            major_seen.entry(store.seq).or_insert_with(HashSet::new);

            if let Some(major_types) = store_type_major_map.get_mut(&store.seq) {
                if let Some(major_set) = major_seen.get_mut(&store.seq) {
                    if major_set.insert(store.major_type) {
                        major_types.push(store.major_type);
                    }
                }
            }

            store_type_sub_map.entry(store.seq).or_insert_with(Vec::new);

            sub_seen.entry(store.seq).or_insert_with(HashSet::new);

            if let Some(sub_types) = store_type_sub_map.get_mut(&store.seq) {
                if let Some(sub_set) = sub_seen.get_mut(&store.seq) {
                    if sub_set.insert(store.sub_type) {
                        sub_types.push(store.sub_type);
                    }
                }
            }
        }

        let store_types_map: StoreTypesMap =
            StoreTypesMap::new(store_type_major_map, store_type_sub_map);

        Ok(store_types_map)
    }
}
