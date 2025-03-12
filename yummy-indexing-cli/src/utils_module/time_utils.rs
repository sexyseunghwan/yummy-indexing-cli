use crate::common::*;

/*
    Function that converts the date data 'naivedatetime' format to String format
*/
pub fn get_str_from_naive_datetime(naive_datetime: NaiveDateTime) -> String {
    naive_datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/*
    Function to change 'string' data format to 'NaiveDateTime' format
*/
pub fn get_naive_datetime_from_str(
    date: &str,
    format: &str,
) -> Result<NaiveDateTime, anyhow::Error> {
    NaiveDateTime::parse_from_str(date, format)
        .map_err(|e| anyhow!("[Datetime Parsing Error][get_naive_datetime_from_str()] Failed to parse date string: {:?} : {:?}", date, e))
}

#[doc = "Functions that make the current date (UTC time) a 'NaiveDateTime' data type"]
pub fn get_current_utc_naive_datetime() -> NaiveDateTime {
    let utc_now: DateTime<Utc> = Utc::now();
    utc_now.naive_local()
}
