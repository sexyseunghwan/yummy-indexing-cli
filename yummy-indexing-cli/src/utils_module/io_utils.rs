use crate::common::*;

#[doc = "Function to convert structure to JSON value"]
/// # Arguments
/// * input_struct - json 으로 변환할 구조체
///
/// # Returns
/// * Result<Value, anyhow::Error>
pub fn convert_json_from_struct<T: Serialize>(input_struct: &T) -> Result<Value, anyhow::Error> {
    serde_json::to_value(input_struct).map_err(|err| {
        anyhow!(
            "[Error][convert_json_from_struct()] Failed to serialize struct to JSON: {}",
            err
        )
    })
}

#[doc = ""]
/// # Arguments
/// * value -
///
/// # Returns
/// * String
pub fn format_number(value: i64) -> String {
    value.to_formatted_string(&Locale::en)
}

#[doc = "Functions that read the json file and return it in json value format"]
/// # Arguments
/// * `file_path` - Path the json file
///
/// # Returns
/// * Result<Value, anyhow::Error>
pub fn read_json_from_file(file_path: &str) -> Result<Value, anyhow::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let json_body: Value = serde_json::from_reader(reader)?;

    Ok(json_body)
}

#[doc = "toml 파일을 읽어서 객체로 변환해주는 함수"]
/// # Arguments
/// * `file_path` - 읽을 대상 toml 파일이 존재하는 경로
///
/// # Returns
/// * Result<T, anyhow::Error> - 성공적으로 파일을 읽었을 경우에는 json 호환 객체를 반환해준다.
pub fn read_toml_from_file<T: DeserializeOwned>(file_path: &str) -> Result<T, anyhow::Error> {
    let toml_content: String = std::fs::read_to_string(file_path)?;
    let toml: T = toml::from_str(&toml_content)?;

    Ok(toml)
}
