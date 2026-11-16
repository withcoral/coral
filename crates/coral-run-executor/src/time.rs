//! The time conversions the link's messages and rows carry.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Now, in Unix nanoseconds: the deadlines that rows and leases record
/// across Coral Instances.
pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, nanos)
}

/// `duration` in whole nanoseconds, as rows record it.
pub(crate) fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// `time_left` in whole milliseconds, as a `Compile` carries it.
pub(crate) fn millis(time_left: Duration) -> u32 {
    u32::try_from(time_left.as_millis()).unwrap_or(u32::MAX)
}
