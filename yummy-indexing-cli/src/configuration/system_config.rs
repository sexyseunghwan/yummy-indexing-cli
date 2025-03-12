use crate::common::*;

static SYSTEM_CONFIG: once_lazy<Arc<SystemConfig>> =
    once_lazy::new(|| Arc::new(initiate_system_config()));

#[derive(Debug, Deserialize, Serialize, Getters, Clone, new)]
#[getset(get = "pub")]
pub struct SystemConfig {
    pub schedule_term: u64,
    pub complie_type: String,
}

#[doc = "SystemConfig 객체를 초기화해주는 함수"]
pub fn initiate_system_config() -> SystemConfig {
    let schedule_term: u64 = match env::var("SCHEDULE_TERM") {
        Ok(schedule_term) => match schedule_term.parse::<u64>() {
            Ok(schedule_term) => schedule_term,
            Err(e) => {
                error!("[Error][initiate_system_config()] The 'schedule_term' information must be numeric.: {:?}", e);
                panic!("[Error][initiate_system_config()] The 'schedule_term' information must be numeric.: {:?}", e)
            }
        },
        Err(e) => {
            error!(
                "[Error][initiate_system_config()] Information 'SCHEDULE_TERM' not found. : {:?}",
                e
            );
            panic!(
                "[Error][initiate_system_config()] Information 'SCHEDULE_TERM' not found. : {:?}",
                e
            );
        }
    };

    let complie_type: String = env::var("COMPILE_TYPE")
        .expect("[Error][initiate_system_config()] Value 'COMPILE_TYPE' not found.");

    let system_config: SystemConfig = SystemConfig::new(schedule_term, complie_type);

    system_config
}

#[doc = "SystemConfig 객체를 공유해주는 함수"]
pub fn get_system_config() -> Arc<SystemConfig> {
    let system_config: Arc<SystemConfig> = Arc::clone(&SYSTEM_CONFIG);
    system_config
}
