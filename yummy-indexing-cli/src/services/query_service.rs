use crate::common::*;

use crate::configuration::index_schedules_config::*;

use crate::models::store_to_elastic::*;

use crate::repository::mysql_repository::*;

use crate::utils_module::time_utils::*;

use crate::entity::{
    elastic_index_info_tbl, recommend_tbl, store, store_location_info_tbl, store_recommend_tbl,
    zero_possible_market,
};

pub trait QueryService {
    async fn get_all_store_table(
        &self,
        index_schedule: &IndexSchedules,
    ) -> Result<Vec<StoreResult>, anyhow::Error>;
    fn get_distinct_store_table(
        &self,
        stores: &Vec<StoreResult>,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error>;
    async fn get_changed_store_table(
        &self,
        recent_datetime: NaiveDateTime,
        query_filter: Condition,
    ) -> Result<Vec<StoreResult>, anyhow::Error>;
    async fn get_dynamic_create_store_index(
        &self,
        recent_datetime: NaiveDateTime,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error>;
    async fn get_dynamic_update_store_index(
        &self,
        recent_datetime: NaiveDateTime,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error>;
    async fn get_dynamic_delete_store_index(
        &self,
        recent_datetime: NaiveDateTime,
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
}

#[derive(Debug, new)]
pub struct QueryServicePub;

impl QueryService for QueryServicePub {
    #[doc = "색인할 Store 정보를 조회해주는 함수 -> batch"]
    /// # Arguments
    /// * `index_schedule` - index_schedule 정보
    ///
    /// # Returns
    /// * Result<Vec<StoreResult>, anyhow::Error>
    async fn get_all_store_table(
        &self,
        index_schedule: &IndexSchedules,
    ) -> Result<Vec<StoreResult>, anyhow::Error> {
        let batch_size: usize = *index_schedule.es_batch_size();

        let db: &DatabaseConnection = establish_connection().await;

        let mut total_store_list: Vec<StoreResult> = Vec::new();
        let mut last_seq: Option<i32> = None;

        let cur_utc_date: NaiveDateTime = get_current_utc_naive_datetime();

        let query_filter: Condition =
            Condition::any().add(Expr::col((store::Entity, store::Column::UseYn)).eq("Y"));

        loop {
            let mut query: Select<store::Entity> = store::Entity::find()
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
                    Expr::col((
                        zero_possible_market::Entity,
                        zero_possible_market::Column::Name,
                    ))
                    .is_not_null(),
                    "zero_possible",
                )
                .column_as(store_location_info_tbl::Column::Address, "address")
                .column_as(store_location_info_tbl::Column::Lat, "lat")
                .column_as(store_location_info_tbl::Column::Lng, "lng")
                .column_as(recommend_tbl::Column::RecommendName, "recommend_name")
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

    #[doc = "색인할 Store 정보를 조회해주는 함수 -> 중복 제거"]
    /// # Arguments
    /// * `stores` - store 데이터 객체 리스트
    /// * `cur_utc_date` - 현재 UTC 기준 시간 데이터
    ///
    /// # Returns
    /// * Result<Vec<DistinctStoreResult>, anyhow::Error>
    fn get_distinct_store_table(
        &self,
        stores: &Vec<StoreResult>,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error> {
        let mut store_map: HashMap<i32, DistinctStoreResult> = HashMap::new();
        let cur_time_utc: String = get_str_curdatetime_utc();

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
                        cur_time_utc.to_string(),
                        store.seq,
                        store.name.clone(),
                        store.r#type.clone(),
                        store.address.clone(),
                        store.lat,
                        store.lng,
                        store.zero_possible,
                        store.recommend_name.clone().map_or(vec![], |r| vec![r]),
                    )
                });
        }

        let result: Vec<DistinctStoreResult> = store_map.into_values().collect();
        Ok(result)
    }

    #[doc = "store 데이터의 증분색인을 위한 함수"]
    /// # Arguments
    /// * `recent_datetime` - 가장 최신 날짜데이터
    /// * `cur_utc_date` - 현재 날짜 데이터
    /// * `query_filter` - 쿼리필터
    ///
    /// # Returns
    /// * Result<Vec<StoreResult>, anyhow::Error>
    async fn get_changed_store_table(
        &self,
        cur_utc_date: NaiveDateTime,
        query_filter: Condition,
    ) -> Result<Vec<StoreResult>, anyhow::Error> {
        let db: &DatabaseConnection = establish_connection().await;

        let query: Select<store::Entity> = store::Entity::find()
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
            .select_only()
            .columns([store::Column::Seq, store::Column::Name, store::Column::Type])
            .expr_as(
                Expr::col((
                    zero_possible_market::Entity,
                    zero_possible_market::Column::Name,
                ))
                .is_not_null(),
                "zero_possible",
            )
            .column_as(store_location_info_tbl::Column::Address, "address")
            .column_as(store_location_info_tbl::Column::Lat, "lat")
            .column_as(store_location_info_tbl::Column::Lng, "lng")
            .column_as(recommend_tbl::Column::RecommendName, "recommend_name")
            .filter(query_filter);

        let store_results: Vec<StoreResult> = query.into_model().all(db).await?;

        Ok(store_results)
    }

    #[doc = "동적색인 - Create 단계 함수"]
    /// # Arguments
    /// * `recent_datetime` - 가장 최신 날짜데이터
    /// * `cur_utc_date` - 현재 날짜 데이터
    ///
    /// # Returns
    /// * Result<Vec<DistinctStoreResult>, anyhow::Error>
    async fn get_dynamic_create_store_index(
        &self,
        recent_datetime: NaiveDateTime,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error> {
        let create_filter: Condition = Condition::all()
            .add(Expr::col((store::Entity, store::Column::UseYn)).eq("Y"))
            .add(
                Condition::any()
                    .add(Expr::col((store::Entity, store::Column::RegDt)).gt(recent_datetime))
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
                            store_recommend_tbl::Column::RegDt,
                        ))
                        .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((recommend_tbl::Entity, recommend_tbl::Column::RegDt))
                            .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            store_location_info_tbl::Entity,
                            store_location_info_tbl::Column::RegDt,
                        ))
                        .gt(recent_datetime),
                    ),
            );

        let stores: Vec<StoreResult> = self
            .get_changed_store_table(recent_datetime, create_filter)
            .await?;

        let distinct_result: Vec<DistinctStoreResult> =
            self.get_distinct_store_table(&stores, cur_utc_date)?;

        Ok(distinct_result)
    }

    #[doc = "동적색인 - Update 단계 함수"]
    /// # Arguments
    /// * `recent_datetime` - 가장 최신 날짜데이터
    /// * `cur_utc_date` - 현재 날짜 데이터
    ///
    /// # Returns
    /// * Result<Vec<DistinctStoreResult>, anyhow::Error>
    async fn get_dynamic_update_store_index(
        &self,
        recent_datetime: NaiveDateTime,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error> {
        let update_filter: Condition = Condition::all()
            .add(Expr::col((store::Entity, store::Column::UseYn)).eq("Y"))
            .add(
                Condition::any()
                    .add(Expr::col((store::Entity, store::Column::ChgDt)).gt(recent_datetime))
                    .add(
                        Expr::col((
                            zero_possible_market::Entity,
                            zero_possible_market::Column::ChgDt,
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
                        Expr::col((recommend_tbl::Entity, recommend_tbl::Column::ChgDt))
                            .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            store_location_info_tbl::Entity,
                            store_location_info_tbl::Column::ChgDt,
                        ))
                        .gt(recent_datetime),
                    ),
            );

        let stores: Vec<StoreResult> = self
            .get_changed_store_table(recent_datetime, update_filter)
            .await?;

        let distinct_result: Vec<DistinctStoreResult> =
            self.get_distinct_store_table(&stores, cur_utc_date)?;

        Ok(distinct_result)
    }

    #[doc = "동적색인 - Delete 단계 함수"]
    /// # Arguments
    /// * `recent_datetime` - 가장 최신 날짜데이터
    /// * `cur_utc_date` - 현재 날짜 데이터
    ///
    /// # Returns
    /// * Result<Vec<DistinctStoreResult>, anyhow::Error>    
    async fn get_dynamic_delete_store_index(
        &self,
        recent_datetime: NaiveDateTime,
        cur_utc_date: NaiveDateTime,
    ) -> Result<Vec<DistinctStoreResult>, anyhow::Error> {
        let delete_filter: Condition = Condition::all()
            .add(Expr::col((store::Entity, store::Column::UseYn)).eq("N"))
            .add(
                Condition::any()
                    .add(Expr::col((store::Entity, store::Column::ChgDt)).gt(recent_datetime))
                    .add(
                        Expr::col((
                            zero_possible_market::Entity,
                            zero_possible_market::Column::ChgDt,
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
                        Expr::col((recommend_tbl::Entity, recommend_tbl::Column::ChgDt))
                            .gt(recent_datetime),
                    )
                    .add(
                        Expr::col((
                            store_location_info_tbl::Entity,
                            store_location_info_tbl::Column::ChgDt,
                        ))
                        .gt(recent_datetime),
                    ),
            );

        let stores: Vec<StoreResult> = self
            .get_changed_store_table(recent_datetime, delete_filter)
            .await?;

        let distinct_result: Vec<DistinctStoreResult> =
            self.get_distinct_store_table(&stores, cur_utc_date)?;

        Ok(distinct_result)
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

        // let filter: Condition = Condition::all().add(
        //     Expr::col((
        //         elastic_index_info_tbl::Entity,
        //         elastic_index_info_tbl::Column::IndexName,
        //     ))
        //     .eq(index_name),
        // );

        // let filter: Condition = Condition::all().add(
        //     elastic_index_info_tbl::Column::IndexName.eq(index_name),
        // );

        let query: Select<elastic_index_info_tbl::Entity> = elastic_index_info_tbl::Entity::find()
            .filter(elastic_index_info_tbl::Column::IndexName.eq(index_name));

        let query_results: Vec<elastic_index_info_tbl::Model> = query.all(db).await?;

        if query_results.is_empty() {
            return Err(anyhow!(
                "[Error][get_recent_date_from_elastic_index_info()] query_results is EMPTY"
            ));
        }
        
        let recent_datetime: NaiveDateTime = 
            query_results
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
}
