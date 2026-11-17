//! Wall-clock time as the tables store it, and the one reader of it.
//!
//! Every stored instant is a count of nanoseconds since 1970 in UTC:
//! [`UnixNanos`]. A span between two instants is a [`Duration`], so the
//! compiler tells a deadline from a lifetime.
//!
//! A repository gives a statement a [`UnixNanos`] where it gave an `i64`, and
//! names it as the type of a row's field or tuple member. The column holds
//! the same integer of nanoseconds either way; an absent instant is a null.
//!
//! [`SystemClock`] is the only code in Coral that reads the host's clock;
//! `clippy.toml` refuses `SystemTime::now` everywhere else. The rule does not
//! reach a dependency that reads the clock itself.
//!
//! Code that decides on time takes the instant as a value. A module that
//! must read the time itself holds a [`Clock`], so that a test gives it one
//! it sets and drives time without waiting. Read the clock once for each
//! decision: two readings differ.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// An instant: nanoseconds since 1970-01-01T00:00:00Z.
///
/// A row decodes one from its integer column, on both databases, as it
/// decodes the `i64` inside.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct UnixNanos(i64);

impl UnixNanos {
    /// The instant `unix_nanos` nanoseconds after 1970, as a row holds it.
    #[must_use]
    pub const fn new(unix_nanos: i64) -> Self {
        Self(unix_nanos)
    }

    /// Returns the nanoseconds since 1970, as a row holds them.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Returns the whole seconds since 1970, as a token claim holds them.
    /// An instant before 1970 gives zero.
    #[must_use]
    pub const fn whole_seconds(self) -> u64 {
        if self.0 < 0 {
            0
        } else {
            (self.0 / NANOS_PER_SECOND).cast_unsigned()
        }
    }

    /// The instant `span` after this one, or `None` past the last instant a
    /// row can hold.
    #[must_use]
    pub fn checked_add(self, span: Duration) -> Option<Self> {
        // A span can be longer than any instant and the sum still fit: add
        // in a wider integer, then keep what a row can hold.
        let span = i128::try_from(span.as_nanos()).ok()?;
        i64::try_from(i128::from(self.0).checked_add(span)?)
            .ok()
            .map(Self)
    }

    /// The instant `span` after this one, or the last instant a row can hold
    /// when the sum passes it.
    #[must_use]
    pub fn saturating_add(self, span: Duration) -> Self {
        self.checked_add(span).unwrap_or(Self(i64::MAX))
    }

    /// The instant `span` before this one, or `None` before the first
    /// instant a row can hold.
    #[must_use]
    pub fn checked_sub(self, span: Duration) -> Option<Self> {
        let span = i128::try_from(span.as_nanos()).ok()?;
        i64::try_from(i128::from(self.0).checked_sub(span)?)
            .ok()
            .map(Self)
    }

    /// How long after `earlier` this instant is; zero when it is not after
    /// it.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        // Two `i64` values differ by less than 2^64, so the difference always
        // fits once it is known to be positive.
        let nanos = i128::from(self.0) - i128::from(earlier.0);
        u64::try_from(nanos).map_or(Duration::ZERO, Duration::from_nanos)
    }
}

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// A `SeaQuery` statement binds an instant as the integer its column holds.
impl From<UnixNanos> for sea_query::Value {
    fn from(instant: UnixNanos) -> Self {
        Self::BigInt(Some(instant.0))
    }
}

/// An absent instant binds as a null of the column's integer type, so a
/// statement takes an `Option<UnixNanos>`.
impl sea_query::Nullable for UnixNanos {
    fn null() -> sea_query::Value {
        sea_query::Value::BigInt(None)
    }
}

/// A source of the current instant.
///
/// Production holds [`SystemClock`]. A test holds a clock it sets, so that a
/// deadline passes when the test says so.
pub trait Clock: std::fmt::Debug + Send + Sync {
    /// Reads the current instant.
    fn now(&self) -> UnixNanos;
}

/// The host's clock.
///
/// A reading never fails. A host clock set before 1970 reads as 1970, and one
/// set past the year 2262 reads as the last instant a row can hold: neither
/// is a state a caller can act on.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> UnixNanos {
        #[expect(
            clippy::disallowed_methods,
            reason = "this is the one reader of the host's clock"
        )]
        let host_time = SystemTime::now();
        instant_of(host_time)
    }
}

/// The instant of a host clock reading, kept within what a row can hold.
fn instant_of(host_time: SystemTime) -> UnixNanos {
    let since_1970 = host_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    UnixNanos(i64::try_from(since_1970.as_nanos()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instant_moves_by_a_span_and_refuses_one_past_what_a_row_holds() {
        let start = UnixNanos::new(1_500);
        let span = Duration::from_nanos(400);

        assert_eq!(start.checked_add(span), Some(UnixNanos::new(1_900)));
        assert_eq!(start.checked_sub(span), Some(UnixNanos::new(1_100)));
        assert_eq!(start.saturating_add(span), UnixNanos::new(1_900));

        let last = UnixNanos::new(i64::MAX);
        let first = UnixNanos::new(i64::MIN);
        assert_eq!(last.checked_add(span), None);
        assert_eq!(last.saturating_add(span), last);
        assert_eq!(first.checked_sub(span), None);
        // A span longer than every instant a row can hold.
        assert_eq!(start.checked_add(Duration::MAX), None);
        assert_eq!(start.saturating_add(Duration::MAX), last);
        assert_eq!(start.checked_sub(Duration::MAX), None);
        // A span longer than the last instant, whose sum still fits.
        let past_last = Duration::from_nanos(i64::MAX.unsigned_abs() + 1);
        assert_eq!(
            UnixNanos::new(-2).checked_add(past_last),
            Some(UnixNanos::new(i64::MAX - 1))
        );
        assert_eq!(
            UnixNanos::new(1).checked_sub(past_last),
            Some(UnixNanos::new(i64::MIN + 1))
        );
        assert_eq!(
            last.checked_sub(Duration::from_nanos(u64::MAX)),
            Some(first)
        );
    }

    #[test]
    fn a_host_clock_outside_what_a_row_holds_reads_as_the_nearest_instant() {
        let before_1970 = UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(instant_of(before_1970), UnixNanos::new(0));
        assert_eq!(instant_of(UNIX_EPOCH), UnixNanos::new(0));
        assert_eq!(
            instant_of(UNIX_EPOCH + Duration::from_nanos(1_500)),
            UnixNanos::new(1_500)
        );
        // The year 2286: past 2262, the last year a row can hold.
        let past_2262 = UNIX_EPOCH + Duration::from_secs(10_000_000_000);
        assert_eq!(instant_of(past_2262), UnixNanos::new(i64::MAX));
    }

    #[test]
    fn the_span_between_two_instants_is_zero_unless_the_first_is_later() {
        let earlier = UnixNanos::new(1_000);
        let later = UnixNanos::new(4_000);

        assert_eq!(
            later.saturating_duration_since(earlier),
            Duration::from_nanos(3_000)
        );
        assert_eq!(earlier.saturating_duration_since(later), Duration::ZERO);
        assert_eq!(later.saturating_duration_since(later), Duration::ZERO);
        // The widest span two rows can hold.
        assert_eq!(
            UnixNanos::new(i64::MAX).saturating_duration_since(UnixNanos::new(i64::MIN)),
            Duration::from_nanos(u64::MAX)
        );
    }

    #[test]
    fn whole_seconds_round_down_and_stop_at_1970() {
        for (unix_nanos, seconds) in [
            (0, 0),
            (999_999_999, 0),
            (1_000_000_000, 1),
            (1_999_999_999, 1),
            (-1, 0),
            (i64::MIN, 0),
            (i64::MAX, 9_223_372_036),
        ] {
            assert_eq!(
                UnixNanos::new(unix_nanos).whole_seconds(),
                seconds,
                "{unix_nanos}"
            );
        }
    }

    #[test]
    fn the_system_clock_reads_an_instant_in_this_century() {
        let now = SystemClock.now();

        // 2020-01-01 and 2100-01-01: the reading is in nanoseconds, not in
        // seconds or milliseconds.
        assert!(now > UnixNanos::new(1_577_836_800 * NANOS_PER_SECOND));
        assert!(now < UnixNanos::new(4_102_444_800 * NANOS_PER_SECOND));
    }
}
