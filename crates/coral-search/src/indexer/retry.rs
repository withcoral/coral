//! When a reconcile pass retries a failed attempt.
//!
//! A failure that repeats until something changes, such as a write that
//! overran its limit, waits for a newer Coral version. Any other failure
//! waits at least one reconcile interval, twice as long after each such
//! failure in a row, and never more than an hour. A newer Coral version
//! retries either at once. A notified write always tries, whatever the rule.
//!
//! Only a newer version retries a failure, never an older one, so two
//! processes of different versions do not retry each other's failures pass
//! after pass.

use std::time::Duration;

use super::IndexError;

/// The Coral version that records a failure.
const CORAL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The longest a pass waits to retry a failure that may pass.
const LONGEST_WAIT: Duration = Duration::from_hours(1);

/// When a pass may retry a failed attempt, as its state row records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetryRule {
    /// The failures in a row under `coral_version` that may pass. `None` on a
    /// row written before Coral counted them.
    pub(super) attempts: Option<i64>,
    /// When a pass may retry the failure. `None` when it waits for a newer
    /// Coral version.
    pub(super) retry_after_unix_nanos: Option<i64>,
    /// The Coral version that recorded the failure. `None` on a row written
    /// before Coral recorded it.
    pub(super) coral_version: Option<String>,
}

impl RetryRule {
    /// The rule a failure attempted at `attempted_at_unix_nanos` records.
    /// `prior` is the rule of the failure before it, when the last attempt
    /// of the same target also failed. Only failures that may pass, in a row
    /// under this Coral version, double the wait.
    pub(super) fn after_failure(
        prior: Option<&Self>,
        error: &IndexError,
        attempted_at_unix_nanos: i64,
        reconcile_interval: Duration,
    ) -> Self {
        let attempts = match prior {
            Some(prior)
                if prior.retry_after_unix_nanos.is_some()
                    && prior.coral_version.as_deref() == Some(CORAL_VERSION) =>
            {
                prior.attempts.unwrap_or(1).saturating_add(1)
            }
            _ => 1,
        };
        let wait = i64::try_from(wait(attempts, reconcile_interval).as_nanos()).unwrap_or(i64::MAX);
        Self {
            attempts: Some(attempts),
            retry_after_unix_nanos: (!error.repeats_until_change())
                .then(|| attempted_at_unix_nanos.saturating_add(wait)),
            coral_version: Some(CORAL_VERSION.to_owned()),
        }
    }

    /// Whether a pass that starts at `now_unix_nanos` retries the failure:
    /// its retry time has passed, or a Coral version older than this one, or
    /// none, recorded it.
    pub(super) fn is_due(&self, now_unix_nanos: i64) -> bool {
        let recorded_by_older = self
            .coral_version
            .as_deref()
            .and_then(release)
            .is_none_or(|recorded| release(CORAL_VERSION).is_some_and(|this| recorded < this));
        recorded_by_older
            || self
                .retry_after_unix_nanos
                .is_some_and(|retry_after| retry_after < now_unix_nanos)
    }
}

/// The `major.minor.patch` of a Coral version, without any pre-release or
/// build suffix. `None` when the version does not read so.
fn release(version: &str) -> Option<(u64, u64, u64)> {
    let core = version.split(['-', '+']).next()?;
    let mut parts = core.split('.').map(str::parse);
    let release = (
        parts.next()?.ok()?,
        parts.next()?.ok()?,
        parts.next()?.ok()?,
    );
    parts.next().is_none().then_some(release)
}

/// How long a pass waits to retry the `attempts`-th failure in a row that
/// may pass.
fn wait(attempts: i64, reconcile_interval: Duration) -> Duration {
    let doublings = u32::try_from(attempts.saturating_sub(1)).unwrap_or(u32::MAX);
    reconcile_interval
        .saturating_mul(2_u32.saturating_pow(doublings))
        .min(LONGEST_WAIT)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{CORAL_VERSION, RetryRule, release, wait};

    #[test]
    fn each_failure_in_a_row_doubles_the_wait_up_to_an_hour() {
        let five_minutes = Duration::from_mins(5);
        for (attempts, expected) in [
            (1, Duration::from_mins(5)),
            (2, Duration::from_mins(10)),
            (3, Duration::from_mins(20)),
            (4, Duration::from_mins(40)),
            (5, Duration::from_hours(1)),
            (6, Duration::from_hours(1)),
            (i64::MAX, Duration::from_hours(1)),
        ] {
            assert_eq!(
                wait(attempts, five_minutes),
                expected,
                "attempts: {attempts}"
            );
        }
        assert_eq!(
            wait(1, Duration::from_hours(2)),
            Duration::from_hours(1),
            "an interval longer than the cap waits the cap"
        );
    }

    #[test]
    fn a_failure_waits_for_a_newer_version_only() {
        let (major, minor, patch) = release(CORAL_VERSION).expect("this version reads");
        let pre_release = format!("{major}.{minor}.{patch}-rc.1");
        let newer_patch = format!("{major}.{minor}.{}", patch + 1);
        let newer_major = format!("{}.0.0", major + 1);
        for (recorded, due) in [
            (None, true),
            (Some("not a version"), true),
            (Some("0.0.0"), true),
            (Some(CORAL_VERSION), false),
            (Some(pre_release.as_str()), false),
            (Some(newer_patch.as_str()), false),
            (Some(newer_major.as_str()), false),
        ] {
            let rule = RetryRule {
                attempts: Some(1),
                retry_after_unix_nanos: None,
                coral_version: recorded.map(str::to_owned),
            };
            assert_eq!(rule.is_due(i64::MAX), due, "recorded: {recorded:?}");
        }
    }
}
