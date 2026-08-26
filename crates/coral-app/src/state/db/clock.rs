use std::time::Duration;

use crate::bootstrap::AppError;

const TRACE_SEARCH_RESPONSE_MAX_FUTURE_SKEW: Duration = Duration::from_mins(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TraceSearchResponseRetentionBounds {
    pub(crate) oldest_inclusive_unix_nanos: i64,
    pub(crate) newest_inclusive_unix_nanos: i64,
}

pub(crate) fn trace_search_response_retention_bounds(
    now_unix_nanos: i64,
    retention: Duration,
) -> TraceSearchResponseRetentionBounds {
    let retention_nanos = i128::try_from(retention.as_nanos()).unwrap_or(i128::MAX);
    let oldest = i128::from(now_unix_nanos).saturating_sub(retention_nanos);
    let future_skew_nanos =
        i128::try_from(TRACE_SEARCH_RESPONSE_MAX_FUTURE_SKEW.as_nanos()).unwrap_or(i128::MAX);
    let newest = i128::from(now_unix_nanos).saturating_add(future_skew_nanos);

    TraceSearchResponseRetentionBounds {
        oldest_inclusive_unix_nanos: i64::try_from(oldest).unwrap_or(i64::MIN),
        newest_inclusive_unix_nanos: i64::try_from(newest).unwrap_or(i64::MAX),
    }
}

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::trace_search_response_retention_bounds;

    #[test]
    fn trace_search_response_retention_bounds_include_clock_skew() {
        let bounds = trace_search_response_retention_bounds(100, Duration::from_nanos(10));

        assert_eq!(bounds.oldest_inclusive_unix_nanos, 90);
        assert_eq!(bounds.newest_inclusive_unix_nanos, 60_000_000_100);
    }

    #[test]
    fn trace_search_response_retention_bounds_saturate() {
        let minimum = trace_search_response_retention_bounds(i64::MIN, Duration::MAX);
        assert_eq!(minimum.oldest_inclusive_unix_nanos, i64::MIN);

        let maximum = trace_search_response_retention_bounds(i64::MAX, Duration::ZERO);
        assert_eq!(maximum.oldest_inclusive_unix_nanos, i64::MAX);
        assert_eq!(maximum.newest_inclusive_unix_nanos, i64::MAX);
    }
}
