//! UTC days: the unit of a Usage Report.

use chrono::{DateTime, NaiveDate, Utc};
use coral_db::UnixNanos;

use crate::Error;

/// The UTC day of an instant.
pub(crate) fn day_of(at: UnixNanos) -> NaiveDate {
    DateTime::<Utc>::from_timestamp_nanos(at.get()).date_naive()
}

/// The start of a UTC day, or the last instant a row can hold for a day
/// past it.
pub(crate) fn day_start(day: NaiveDate) -> UnixNanos {
    UnixNanos::new(
        day.and_hms_opt(0, 0, 0)
            .and_then(|start| start.and_utc().timestamp_nanos_opt())
            .unwrap_or(i64::MAX),
    )
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
