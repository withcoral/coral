//! When a reconcile pass retries a failed attempt.
//!
//! A failure that repeats until something changes, such as a write that
//! overran its limit, waits for a newer Coral version. Any other failure
//! waits at least one reconcile interval, twice as long after each such
//! failure in a row, and never more than an hour. A newer Coral version
//! retries either at once. A notified write always tries, whatever the rule,
//! so removing a Provider and adding it again, or saving a Saved Function
//! again, retries it at once.
//!
//! An older version never retries a failure a newer one recorded, so two
//! processes of different versions do not retry each other's failures pass
//! after pass.

use std::cmp::Ordering;
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
    /// a Coral version older than this one, or none, recorded it, or this
    /// version recorded it and its retry time has passed. A failure a newer
    /// version recorded is left to that version: a retry here would record
    /// this older version, and the newer one would retry again at once.
    pub(super) fn is_due(&self, now_unix_nanos: i64) -> bool {
        let retry_time_passed = self
            .retry_after_unix_nanos
            .is_some_and(|retry_after| retry_after < now_unix_nanos);
        let Some(recorded) = self.coral_version.as_deref().and_then(Release::parse) else {
            return true;
        };
        match Release::parse(CORAL_VERSION).map(|this| recorded.cmp(&this)) {
            Some(Ordering::Less) => true,
            Some(Ordering::Greater) => false,
            Some(Ordering::Equal) | None => retry_time_passed,
        }
    }
}

/// A Coral version in semantic-version order: a pre-release sorts before
/// its release, and build metadata does not count.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Release {
    core: (u64, u64, u64),
    stage: Stage,
}

/// Declared in precedence order, so the derived order puts a pre-release
/// first.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    PreRelease(Vec<Identifier>),
    Final,
}

/// One dot-separated pre-release identifier. Declared in precedence order:
/// a numeric identifier sorts before a text one.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Identifier {
    Numeric(u64),
    Text(String),
}

impl Release {
    /// `None` when the version does not read as `major.minor.patch` with an
    /// optional pre-release and build suffix.
    fn parse(version: &str) -> Option<Self> {
        let version = version
            .split_once('+')
            .map_or(version, |(version, _build)| version);
        let (core, pre_release) = match version.split_once('-') {
            Some((core, pre_release)) => (core, Some(pre_release)),
            None => (version, None),
        };
        let mut parts = core.split('.').map(str::parse);
        let core = (
            parts.next()?.ok()?,
            parts.next()?.ok()?,
            parts.next()?.ok()?,
        );
        if parts.next().is_some() {
            return None;
        }
        let stage = match pre_release {
            None => Stage::Final,
            Some(pre_release) => Stage::PreRelease(
                pre_release
                    .split('.')
                    .map(|identifier| match identifier.parse() {
                        Ok(number) => Some(Identifier::Numeric(number)),
                        Err(_) if identifier.is_empty() => None,
                        Err(_) => Some(Identifier::Text(identifier.to_owned())),
                    })
                    .collect::<Option<_>>()?,
            ),
        };
        Some(Self { core, stage })
    }
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

    use super::{CORAL_VERSION, Release, RetryRule, wait};
    use crate::indexer::IndexError;

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
        let Release {
            core: (major, minor, patch),
            ..
        } = Release::parse(CORAL_VERSION).expect("this version reads");
        let pre_release = format!("{major}.{minor}.{patch}-rc.1");
        let newer_patch = format!("{major}.{minor}.{}", patch + 1);
        let newer_major = format!("{}.0.0", major + 1);
        // Whether a failure that repeats, and one whose retry time has
        // passed, are due under each recorded version.
        for (recorded, repeating_due, timed_due) in [
            (None, true, true),
            (Some("not a version"), true, true),
            (Some("0.0.0"), true, true),
            (Some(pre_release.as_str()), true, true),
            (Some(CORAL_VERSION), false, true),
            (Some(newer_patch.as_str()), false, false),
            (Some(newer_major.as_str()), false, false),
        ] {
            for (retry_after_unix_nanos, due) in [(None, repeating_due), (Some(0), timed_due)] {
                let rule = RetryRule {
                    attempts: Some(1),
                    retry_after_unix_nanos,
                    coral_version: recorded.map(str::to_owned),
                };
                assert_eq!(
                    rule.is_due(i64::MAX),
                    due,
                    "recorded: {recorded:?}, retry after: {retry_after_unix_nanos:?}"
                );
            }
        }
    }

    #[test]
    fn versions_follow_semantic_version_precedence() {
        // The precedence example of the Semantic Versioning 2.0.0 spec, then
        // a release with build metadata, which does not count.
        let ordered = [
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-alpha.beta",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0",
            "1.0.1",
            "1.1.0",
            "2.0.0",
        ]
        .map(|version| Release::parse(version).expect(version));
        assert!(ordered.is_sorted_by(|older, newer| older < newer));
        assert_eq!(Release::parse("1.0.0+build.5"), Release::parse("1.0.0"));
        for unreadable in ["1.0", "1.0.0.0", "1.0.0-", "1.0.0-rc..1", "v1.0.0"] {
            assert_eq!(Release::parse(unreadable), None, "{unreadable}");
        }
    }

    #[test]
    fn only_an_invalid_entry_or_a_write_that_overran_after_its_lock_waits_for_a_newer_version() {
        let limit = Duration::from_mins(1);
        for (error, waits_for_version) in [
            (IndexError::InvalidEntry("bad path".into()), true),
            (IndexError::Overran(limit), true),
            (IndexError::WriterBusy(limit), false),
            (IndexError::TimedOut(limit), false),
            (IndexError::Clock, false),
            (IndexError::Corrupt("bad row"), false),
        ] {
            let rule = RetryRule::after_failure(None, &error, 0, Duration::from_mins(5));
            assert_eq!(
                rule.retry_after_unix_nanos.is_none(),
                waits_for_version,
                "{error}"
            );
        }
    }
}
