//! The time conversions the link's messages and rows carry.

use std::time::Duration;

use coral_db::{Clock as _, UnixNanos};

/// Now, on the clock that rows and leases record their deadlines by across
/// Coral Instances.
pub(crate) fn unix_now() -> UnixNanos {
    coral_db::SystemClock.now()
}

/// The instant `span` before `instant`, or the first instant a row can hold
/// when the difference passes it.
pub(crate) fn before(instant: UnixNanos, span: Duration) -> UnixNanos {
    instant
        .checked_sub(span)
        .unwrap_or(UnixNanos::new(i64::MIN))
}

/// `time_left` in whole milliseconds, as a `Compile` carries it.
pub(crate) fn millis(time_left: Duration) -> u32 {
    u32::try_from(time_left.as_millis()).unwrap_or(u32::MAX)
}
