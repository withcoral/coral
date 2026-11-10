//! UTC days: the unit of a Usage Report.

use chrono::{DateTime, NaiveDate, Utc};

use crate::Error;

/// The UTC day of a unix-nanosecond time.
pub(crate) fn day_of(unix_nanos: i64) -> NaiveDate {
    DateTime::<Utc>::from_timestamp_nanos(unix_nanos).date_naive()
}

/// The unix-nanosecond start of a UTC day.
pub(crate) fn day_start(day: NaiveDate) -> i64 {
    day.and_hms_opt(0, 0, 0)
        .and_then(|start| start.and_utc().timestamp_nanos_opt())
        .unwrap_or(i64::MAX)
}

/// `YYYY-MM-DD`, as reports and storage write a day.
pub(crate) fn day_string(day: NaiveDate) -> String {
    day.format("%Y-%m-%d").to_string()
}

/// Parses a stored `YYYY-MM-DD` day.
pub(crate) fn parse_day(day: &str) -> Result<NaiveDate, Error> {
    NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .map_err(|_error| Error::CorruptData("a stored usage report day is not a date"))
}
