use crate::bootstrap::AppError;

pub(crate) fn now_unix_nanos_i64() -> Result<i64, AppError> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            AppError::FailedPrecondition(format!("system clock before Unix epoch: {error}"))
        })?
        .as_nanos();
    i64::try_from(nanos).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "system clock timestamp exceeds i64 nanoseconds: {error}"
        ))
    })
}
