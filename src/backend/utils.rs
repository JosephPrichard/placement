use std::time::{Duration, SystemTime};
use chrono::LocalResult::Single;
use chrono::{TimeZone, Utc};
use tracing::log::error;
use crate::backend::models::ServiceError;

pub fn epoch_to_iso_string(unix_epoch: i64) -> Result<String, ServiceError> {
    match Utc.timestamp_millis_opt(unix_epoch) {
        Single(time) => Ok(format!("{}", time.to_rfc3339())),
        _ => {
            error!("Failed to convert unix epoch from cassandra database into in memory datetime format");
            Err(ServiceError::Fatal)
        }
    }
}

pub fn epoch_to_day(duration: Duration) -> i64 {
    (duration.as_secs() / (60 * 60 * 24)) as i64
}