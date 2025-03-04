use crate::common::*;

use crate::services::es_query_service::*;
use crate::services::query_service::*;

use crate::configuration::{index_schedules_config::*, system_config::*};

use crate::models::store_to_elastic::*;

use crate::utils_module::time_utils::*;

#[derive(Debug, new)]
pub struct MainController<Q: QueryService, E: EsQueryService> {
    query_service: Q,
    es_query_service: E,
}

impl<Q: QueryService, E: EsQueryService> MainController<Q, E> {
    #[doc = "메인 스케쥴러 함수"]
    /// # Arguments
    /// * `index_schedule` - 인덱스 스케쥴 객체
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    pub async fn main_schedule_task(
        &self,
        index_schedule: IndexSchedules,
    ) -> Result<(), anyhow::Error> {
        let schedule: Schedule =
            Schedule::from_str(&index_schedule.time).expect("Failed to parse CRON expression");

        let system_config: Arc<SystemConfig> = get_system_config();

        let mut interval: Interval = tokio::time::interval(tokio::time::Duration::from_millis(
            system_config.schedule_term,
        ));

        /* 한국 표준시 GMT + 9 */
        let kst_offset: FixedOffset = match FixedOffset::east_opt(9 * 3600) {
            Some(kst_offset) => kst_offset,
            None => {
                error!(
                    "[Error][main_schedule_task()] There was a problem initializing 'kst_offset'."
                );
                panic!(
                    "[Error][main_schedule_task()] There was a problem initializing 'kst_offset'."
                );
            }
        };

        loop {
            interval.tick().await;

            let now: DateTime<Utc> = Utc::now();
            let kst_now: DateTime<FixedOffset> = now.with_timezone(&kst_offset); /* Converting UTC Current Time to KST */

            if let Some(next) = schedule.upcoming(kst_offset).take(1).next() {
                if (next - kst_now).num_seconds() < 1 {
                    match self.main_task(index_schedule.clone()).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!("[Error][main_schedule_task() -> main_task()] {:?}", e);
                        }
                    }
                }
            }
        }
    }

    #[doc = "메인 작업 함수 -> 색인 진행 함수"]
    /// # Arguments
    /// * `index_schedule` - 인덱스 스케쥴 객체
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    pub async fn main_task(&self, index_schedule: IndexSchedules) -> Result<(), anyhow::Error> {
        let function_name: &str = index_schedule.function_name().as_str();

        match function_name {
            "store_static_index" => self.store_static_index(index_schedule).await?,
            "store_dynamic_index" => self.store_dynamic_index(index_schedule).await?,
            _ => {
                return Err(anyhow!(
                    "[Error][main_task()] The mapped function does not exist.: {}",
                    function_name
                ))
            }
        }
        
        Ok(())
    }

    #[doc = "Store 객체를 정적색인 해주는 함수"]
    /// # Arguments
    /// * `index_schedule` - 인덱스 스케쥴 객체
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    async fn store_static_index(
        &self,
        index_schedule: IndexSchedules,
    ) -> Result<(), anyhow::Error> {

        /* 현재기준 UTC 시간 */
        let cur_utc_date: NaiveDateTime = get_current_utc_naive_datetime();

        /* 중복이 존재하는 store 리스트 */
        let stores: Vec<StoreResult> = self
            .query_service
            .get_all_store_table(&index_schedule)
            .await?;

        /* 중복을 제외한 store 리스트 */
        let stores_distinct: Vec<DistinctStoreResult> = self
            .query_service
            .get_distinct_store_table(&stores, cur_utc_date)?;

        self.es_query_service
            .post_indexing_data_by_bulk_static::<DistinctStoreResult>(
                &index_schedule,
                &stores_distinct,
            )
            .await?;
            
        /* 색인시간 최신화 */
        self.query_service
            .update_recent_date_to_elastic_index_info(&index_schedule, cur_utc_date)
            .await?;

        info!("Store - Static Create Indexing: {}", stores_distinct.len());

        Ok(())
    }

    #[doc = "Store 객체를 증분색인 해주는 함수"]
    /// # Arguments
    /// * `index_schedule` - 인덱스 스케쥴 객체
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    async fn store_dynamic_index(
        &self,
        index_schedule: IndexSchedules,
    ) -> Result<(), anyhow::Error> {
        let cur_utc_date: NaiveDateTime = get_current_utc_naive_datetime(); /* 현재기준 UTC 시간 */

        /* 일단, 검색엔진에 색인된 정보중에 가장 최근의 timestamp 정보를 가져와 준다. -> 필요없을듯 */
        // let recent_index_datetime: NaiveDateTime = self
        //     .es_query_service
        //     .get_recent_index_datetime(&index_schedule, "timestamp")
        //     .await?;

        /* RDB 에서 검색엔진에 가장 마지막으로 색인한 날짜를 가져와준다. */
        let recent_index_datetime: NaiveDateTime = self
            .query_service
            .get_recent_date_from_elastic_index_info(&index_schedule)
            .await?;

        /* 증분색인은 Create -> Update -> Delete 세단계로 나눠준다. */
        /* 1. Create */
        let create_list: Vec<DistinctStoreResult> = self
            .query_service
            .get_dynamic_create_store_index(recent_index_datetime, cur_utc_date)
            .await?;

        if !create_list.is_empty() {
            self.es_query_service
                .post_indexing_data_by_bulk_dynamic::<DistinctStoreResult>(
                    &index_schedule,
                    &create_list,
                )
                .await
                .map_err(|e| {
                    anyhow!(
                        "[Error][store_dynamic_index()] Dynamic Index Failed (Create) : {:?}",
                        e
                    )
                })?;
        }

        info!("Dynamic Create Indexing: {}", create_list.len());

        /* 2. Update */
        let update_list: Vec<DistinctStoreResult> = self
            .query_service
            .get_dynamic_update_store_index(recent_index_datetime, cur_utc_date)
            .await?;

        if !update_list.is_empty() {
            self.es_query_service
                .update_index(&index_schedule, &update_list, "seq")
                .await?;
        }

        info!("Dynamic Update Indexing: {}", update_list.len());

        /* 3. Delete */
        let delete_list: Vec<DistinctStoreResult> = self
            .query_service
            .get_dynamic_delete_store_index(recent_index_datetime, cur_utc_date)
            .await?;

        if !delete_list.is_empty() {
            self.es_query_service
                .delete_index(&index_schedule, &delete_list, "seq")
                .await?;
        }

        info!("Dynamic Delete Indexing: {}", delete_list.len());

        if !create_list.is_empty() || !update_list.is_empty() || !delete_list.is_empty() {
            /* 색인시간 최신화 */
            self.query_service
                .update_recent_date_to_elastic_index_info(&index_schedule, cur_utc_date)
                .await?;
        }

        Ok(())
    }
    
    #[doc = "사용자의 입력을 받아서 색인을 진행시켜주는 함수"]
    /// # Arguments
    /// * `index_schedules` - 인덱스 스케쥴 객체들
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    pub async fn cli_indexing_task(&self, index_schedules: IndexSchedulesConfig) -> Result<(), anyhow::Error> {
        
        let mut stdout: io::Stdout = io::stdout();         
        
        let mut idx: i32 = 0;

        writeln!(stdout, "[================ Yummy Indexing CLI ================]").unwrap();
        writeln!(stdout, "Select the index you want to perform.").unwrap();
        
        for index in index_schedules.index() {
            idx += 1;
            writeln!(stdout, "[{}] {:?} - {:?}", idx, index.index_name(), index.indexing_type).unwrap();
        }

        loop {
            writeln!(stdout, "\n").unwrap();
            write!(stdout, "Please enter your number: ").unwrap();
            stdout.flush().unwrap();/* 즉시출력 */
            
            let mut input: String = String::new();
            io::stdin().read_line(&mut input).expect("Failed to read line");
            
            match input.trim().parse::<i32>() {
                Ok(number) => {
                    if number > 0 && number <= idx {
                        let index: &IndexSchedules = index_schedules.index().get((number - 1) as usize).unwrap();

                        /* 여기서 색인 작업을 진행해준다. */
                        match self.main_task(index.clone()).await {
                            Ok(_) => (),
                            Err(e) => {
                                error!("[Error][cli_indexing_task() -> main_task()] {:?}", e);
                                writeln!(stdout, "Index failed.").unwrap();
                                break;
                            }
                        }
                        
                        writeln!(stdout, "Indexing operation completed.").unwrap();
                        break;
                    } else {
                        writeln!(stdout, "Invalid input, please enter a number between 1 and {}.", idx).unwrap();
                    }
                } 
                Err(_) => {
                    writeln!(stdout, "Invalid input, please enter a number.").unwrap();
                }
            }
        }
        
        Ok(())
    }
}
