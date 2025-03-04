use crate::common::*;

use crate::configuration::index_schedules_config::*;

use crate::repository::es_repository::*;

use crate::utils_module::io_utils::*;
use crate::utils_module::time_utils::*;

#[async_trait]
pub trait EsQueryService {
    async fn post_indexing_data_by_bulk_static<T: Serialize + Send + Sync + Debug>(
        &self,
        index_schedule: &IndexSchedules,
        data: &Vec<T>,
    ) -> Result<(), anyhow::Error>;

    async fn post_indexing_data_by_bulk_dynamic<T: Serialize + Send + Sync + Debug>(
        &self,
        index_schedule: &IndexSchedules,
        data: &Vec<T>,
    ) -> Result<(), anyhow::Error>;

    async fn get_recent_index_datetime(
        &self,
        index_schedule: &IndexSchedules,
        timestamp_field: &str,
    ) -> Result<NaiveDateTime, anyhow::Error>;

    async fn update_index<T: Serialize + Send + Sync + Debug>(
        &self,
        index_schedule: &IndexSchedules,
        data: &Vec<T>,
        unique_field_name: &str,
    ) -> Result<(), anyhow::Error>;

    async fn delete_index<T: Serialize + Send + Sync + Debug>(
        &self,
        index_schedule: &IndexSchedules,
        data: &Vec<T>,
        unique_field_name: &str,
    ) -> Result<(), anyhow::Error>;

    async fn get_test(&self) -> Result<(), anyhow::Error>;
}

#[derive(Debug, new)]
pub struct EsQueryServicePub;

#[async_trait]
impl EsQueryService for EsQueryServicePub {
    #[doc = "Elasticsearch - static index function"]
    /// # Arguments
    /// * `index_schedule` - Index schedule information
    /// * `data` - Vector information to be indexed
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    async fn post_indexing_data_by_bulk_static<T: Serialize + Send + Sync + Debug>(
        &self,
        index_schedule: &IndexSchedules,
        data: &Vec<T>,
    ) -> Result<(), anyhow::Error> {
        /* === information of  index_schedule === */
        let index_alias_name: &String = index_schedule.index_name();
        let index_settings_path: &str = match index_schedule.setting_path() {
            Some(index_setting_path) => index_setting_path.as_str(),
            None => {
                return Err(anyhow!(
                    "[Error][store_static_index()] Please specify 'setting_path' for index"
                ))
            }
        };

        let es_batch_size: usize = *index_schedule.es_batch_size();
        /* ====================================== */
        let es_conn: ElasticConnGuard = get_elastic_guard_conn().await?;

        /* Put today's date time on the index you want to create. */
        let curr_time: String = get_current_utc_naive_datetime()
            .format("%Y%m%d%H%M%S")
            .to_string();

        let new_index_name: String = format!("{}-{}", index_alias_name, curr_time);
        let json_body: Value = match read_json_from_file(index_settings_path) {
            Ok(json_body) => json_body,
            Err(e) => {
                error!("[Error][post_indexing_data_by_bulk()] Failed to read 'index_settings' file.: {:?}", e);
                return Err(anyhow!("[Error][post_indexing_data_by_bulk()] Failed to read 'index_settings' file.: {:?}", e));
            }
        };

        es_conn.create_index(&new_index_name, &json_body).await?;
        
        /* Bulk post the data to the index above at once. */
        es_conn
            .bulk_indexing_query(&new_index_name, data, es_batch_size)
            .await?;

        /* 해당 인덱스가 있는지 없는지 확인해준다. */
        let index_exists_yn: bool = match es_conn.check_index_exist(index_alias_name).await {
            Ok(_index_exists_yn) => true,
            Err(e) => {
                error!("[Error][post_indexing_data_by_bulk()] An index starting with that name does not exist.: {}, {:?}", index_alias_name, e);
                false
            }
        };


        if index_exists_yn {
            /* 기존 인덱스가 존재하는 경우 */
            let alias_resp: Value = es_conn
                .get_indexes_mapping_by_alias(index_alias_name)
                .await?;

            let old_index_name: String;

            if let Some(first_key) = alias_resp.as_object().and_then(|map| map.keys().next()) {
                old_index_name = first_key.to_string();
            } else {
                return Err(anyhow!("[Error][post_indexing_data_by_bulk()] Failed to extract index name within 'index-alias'"));
            }

            es_conn
                .update_index_alias(index_alias_name, &new_index_name, &old_index_name)
                .await?;

            es_conn.delete_query(&old_index_name).await?;
        } else {
            /* 기존 인덱스가 존재하지 않는 경우 -> 새로운 인덱스를 생성해준다. */
            es_conn
                .create_index_alias(index_alias_name, &new_index_name)
                .await?;
        }

        /* Functions to enable search immediately after index */
        es_conn.refresh_index(index_alias_name).await?;

        Ok(())
    }

    #[doc = "Elasticsearch - dynamic index function"]
    /// # Arguments
    /// * `index_schedule` - Index schedule information
    /// * `data` - Vector information to be indexed
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    async fn post_indexing_data_by_bulk_dynamic<T: Serialize + Send + Sync + Debug>(
        &self,
        //index_alias_name: &str,
        index_schedule: &IndexSchedules,
        data: &Vec<T>,
    ) -> Result<(), anyhow::Error> {
        let index_alias_name: &String = index_schedule.index_name();
        let es_batch_size: usize = *index_schedule.es_batch_size();

        let es_conn: ElasticConnGuard = get_elastic_guard_conn().await?;
        es_conn
            .bulk_indexing_query(&index_alias_name, data, es_batch_size)
            .await?;

        Ok(())
    }

    #[doc = "특정 인덱스에서 가장 최신 날짜를 쿼리하는 함수"]
    /// # Arguments
    /// * `index_schedule` - Index schedule information
    /// * `timestamp_field` - Field name for time
    ///
    /// # Returns
    /// * Result<NaiveDateTime, anyhow::Error>
    async fn get_recent_index_datetime(
        &self,
        index_schedule: &IndexSchedules,
        timestamp_field: &str,
    ) -> Result<NaiveDateTime, anyhow::Error> {
        let index_name: &String = index_schedule.index_name();

        let es_conn: ElasticConnGuard = get_elastic_guard_conn().await?;

        let es_query: Value = json!({
            "size":1,
            "sort": [{ timestamp_field: "desc" }],
            "_source": [timestamp_field]
        });

        let response: Value = es_conn.get_search_query(&es_query, index_name).await?;

        let timestamp_str: &str = response["hits"]["hits"]
            .as_array()
            .and_then(|hits| hits.first())
            .and_then(|first_hit| first_hit["_source"][timestamp_field].as_str())
            .ok_or_else(|| {
                anyhow!("[Error][get_recent_index_datetime()] Failed to parse 'timestamp'")
            })?;

        let timestamp: NaiveDateTime =
            get_naive_datetime_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")?;

        Ok(timestamp)
    }

    #[doc = "기존의 Elasticsearch 문서들을 update 해주는 함수"]
    /// # Arguments
    /// * `index_schedule` - Index schedule information
    /// * `data` - Vector information to be indexed
    /// * `unique_field_name` - Name of a unique field
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    async fn update_index<T: Serialize + Send + Sync + Debug>(
        &self,
        index_schedule: &IndexSchedules,
        data: &Vec<T>,
        unique_field_name: &str,
    ) -> Result<(), anyhow::Error> {
        let index_name: &String = index_schedule.index_name();

        let es_conn: ElasticConnGuard = get_elastic_guard_conn().await?;

        for item in data {
            let json_value: Value = serde_json::to_value(&item)?;

            let unique_value: i32 = json_value[&unique_field_name]
                .as_i64()
                .ok_or_else(|| anyhow!("[Error][update_index()] There was a problem converting data for 'unique_value'"))?
                .try_into()?;

            /* 기존 문서 삭제 */
            es_conn
                .delete_query_where_field(index_name, unique_field_name, unique_value)
                .await?;

            /* 새로운 문서 색인 */
            es_conn.post_query_struct::<T>(item, index_name).await?;
        }

        Ok(())
    }

    #[doc = "기존의 Elasticsearch 문서들을 삭제 해주는 함수"]
    /// # Arguments
    /// * `index_schedule` - Index schedule information
    /// * `data` - Vector information to be indexed
    /// * `unique_field_name` - Name of a unique field
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    async fn delete_index<T: Serialize + Send + Sync + Debug>(
        &self,
        index_schedule: &IndexSchedules,
        data: &Vec<T>,
        unique_field_name: &str,
    ) -> Result<(), anyhow::Error> {
        let index_name: &String = index_schedule.index_name();

        let es_conn: ElasticConnGuard = get_elastic_guard_conn().await?;

        for document in data {
            let json_value: Value = serde_json::to_value(&document)?;

            let unique_value: i32 = json_value[&unique_field_name]
                .as_i64()
                .ok_or_else(|| anyhow!("[Error][delete_index()] There was a problem converting data for 'unique_value'"))?
                .try_into()?;

            /* 기존 문서 삭제 */
            es_conn
                .delete_query_where_field(index_name, unique_field_name, unique_value)
                .await?;
        }

        Ok(())
    }

    async fn get_test(&self) -> Result<(), anyhow::Error> {
        let es_conn: ElasticConnGuard = get_elastic_guard_conn().await?;

        let test = es_conn.check_index_exist("kakrftel").await?;

        Ok(())
    }
}
