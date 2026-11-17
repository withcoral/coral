//! When a device worker asks the token endpoint for its grant (RFC 8628
//! section 3.5).
//!
//! This is the timing alone, and it does no I/O. The worker reads the clock
//! once, asks what to do at that instant, and stores the timing that each
//! result gives.

use std::time::Duration;

use coral_db::UnixNanos;

use super::oauth::TOKEN_REQUEST_TIMEOUT;

/// The longest a worker sleeps before it renews its lease and looks again.
const LONGEST_WAIT: Duration = Duration::from_secs(10);
/// What `slow_down` adds to the interval.
const SLOW_DOWN: Duration = Duration::from_secs(5);

/// The timing a device job stores: the span between two polls, and the
/// instant of the next one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Timing {
    pub(crate) interval: Duration,
    pub(crate) next_poll_at: UnixNanos,
}

impl Timing {
    /// The timing whose next poll is one `interval` after `now`.
    fn after(now: UnixNanos, interval: Duration) -> Self {
        Self {
            interval,
            next_poll_at: now.saturating_add(interval),
        }
    }
}

/// What a worker does at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Step {
    /// Sleep this long, then ask again.
    Wait(Duration),
    /// Ask the token endpoint now.
    Poll,
    /// The device code has expired. Ask the token endpoint nothing more.
    Expired,
}

/// How a poll ended when it left the grant undecided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome {
    /// `authorization_pending`: the User has not finished.
    Pending,
    /// `slow_down`: the service wants longer between polls.
    SlowDown,
    /// No answer came before the request's timeout. The service may have
    /// answered anything.
    NoAnswer,
}

/// One device code's polling: the timing in force, and the instant the code
/// expires.
pub(super) struct PollSchedule {
    timing: Timing,
    expires_at: UnixNanos,
}

impl PollSchedule {
    /// The schedule of a device code that the service issued at `now`. The
    /// first poll is one `interval` later.
    pub(super) fn issued(now: UnixNanos, interval: Duration, expires_at: UnixNanos) -> Self {
        Self {
            timing: Timing::after(now, interval),
            expires_at,
        }
    }

    /// The schedule that goes on from the timing a job stored.
    pub(super) const fn resumed(timing: Timing, expires_at: UnixNanos) -> Self {
        Self { timing, expires_at }
    }

    /// The timing in force, as a job stores it.
    pub(super) const fn timing(&self) -> Timing {
        self.timing
    }

    /// What the worker does at `now`. A code expires at its expiry instant,
    /// not after it. A poll that falls due at or after the expiry never
    /// leaves: the worker wakes at the expiry instead.
    pub(super) fn step(&self, now: UnixNanos) -> Step {
        if now >= self.expires_at {
            return Step::Expired;
        }
        let wait = self
            .timing
            .next_poll_at
            .min(self.expires_at)
            .saturating_duration_since(now);
        if wait.is_zero() {
            Step::Poll
        } else {
            Step::Wait(wait.min(LONGEST_WAIT))
        }
    }

    /// The timing to store before the request of a poll at `now` leaves
    /// Coral. A worker that dies has lost the answer, which may have been
    /// `slow_down`, and the request may have run to its timeout. So the
    /// stored timing assumes the longer interval of the two and a request
    /// that took its whole timeout. The schedule itself keeps its timing
    /// until the poll ends.
    pub(super) fn before_request(&self, now: UnixNanos) -> Timing {
        let interval = self
            .interval_after(Outcome::SlowDown)
            .max(self.interval_after(Outcome::NoAnswer));
        Timing {
            interval,
            next_poll_at: now.saturating_add(TOKEN_REQUEST_TIMEOUT.saturating_add(interval)),
        }
    }

    /// Takes the outcome of the poll that ended at `now` and returns the
    /// timing to store: the next poll is one interval later. `slow_down`
    /// adds five seconds to the interval and a request with no answer
    /// doubles it, for this poll and every later one.
    pub(super) fn after_poll(&mut self, now: UnixNanos, outcome: Outcome) -> Timing {
        self.timing = Timing::after(now, self.interval_after(outcome));
        self.timing
    }

    fn interval_after(&self, outcome: Outcome) -> Duration {
        let interval = self.timing.interval;
        match outcome {
            Outcome::Pending => interval,
            Outcome::SlowDown => interval.saturating_add(SLOW_DOWN),
            Outcome::NoAnswer => interval.saturating_mul(2),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The instant `seconds` after the device code was issued.
    fn at(seconds: u64) -> UnixNanos {
        UnixNanos::new(1_700_000_000_000_000_000)
            .checked_add(Duration::from_secs(seconds))
            .unwrap()
    }

    /// The timing with `interval` seconds between polls.
    const fn timing(interval: u64, next_poll_at: UnixNanos) -> Timing {
        Timing {
            interval: Duration::from_secs(interval),
            next_poll_at,
        }
    }

    /// The schedule that goes on from `timing` with a code that expires ten
    /// minutes after it was issued.
    const fn resumed(timing: Timing) -> PollSchedule {
        PollSchedule::resumed(timing, UnixNanos::new(1_700_000_600_000_000_000))
    }

    /// The last instant a row can hold.
    const LAST: UnixNanos = UnixNanos::new(i64::MAX);

    #[test]
    fn the_first_poll_is_one_interval_after_the_code_is_issued() {
        for (case, interval, first) in [
            ("the default interval", 5, timing(5, at(105))),
            ("the shortest interval", 1, timing(1, at(101))),
            (
                "the longest interval a service may ask",
                3600,
                timing(3600, at(3700)),
            ),
            (
                "an interval past what a row holds",
                u64::MAX,
                timing(u64::MAX, LAST),
            ),
        ] {
            let schedule = PollSchedule::issued(at(100), Duration::from_secs(interval), at(600));
            assert_eq!(schedule.timing(), first, "{case}");
        }
    }

    #[test]
    fn a_worker_waits_for_the_next_poll_and_stops_at_the_expiry() {
        let nanosecond = Duration::from_nanos(1);
        for (case, next_poll, expires, now, step) in [
            (
                "before the next poll",
                at(5),
                at(600),
                at(2),
                Step::Wait(Duration::from_secs(3)),
            ),
            (
                "one nanosecond before the next poll",
                at(5),
                at(600),
                at(5).checked_sub(nanosecond).unwrap(),
                Step::Wait(nanosecond),
            ),
            ("at the next poll", at(5), at(600), at(5), Step::Poll),
            ("after the next poll", at(5), at(600), at(9), Step::Poll),
            (
                "a wait longer than the cap",
                at(50),
                at(600),
                at(2),
                Step::Wait(LONGEST_WAIT),
            ),
            (
                "a wait of exactly the cap",
                at(12),
                at(600),
                at(2),
                Step::Wait(LONGEST_WAIT),
            ),
            (
                "one nanosecond before the expiry, with a poll due",
                at(5),
                at(60),
                at(60).checked_sub(nanosecond).unwrap(),
                Step::Poll,
            ),
            ("at the expiry", at(5), at(60), at(60), Step::Expired),
            ("after the expiry", at(5), at(60), at(61), Step::Expired),
            (
                "a poll due after the expiry",
                at(64),
                at(60),
                at(58),
                Step::Wait(Duration::from_secs(2)),
            ),
            (
                "a poll due at the expiry",
                at(60),
                at(60),
                at(60),
                Step::Expired,
            ),
            (
                "a poll due after the expiry, more than the cap away",
                at(900),
                at(600),
                at(2),
                Step::Wait(LONGEST_WAIT),
            ),
        ] {
            let schedule = PollSchedule::resumed(timing(5, next_poll), expires);
            assert_eq!(schedule.step(now), step, "{case}");
        }
    }

    #[test]
    fn each_outcome_sets_the_interval_and_the_next_poll() {
        for (case, interval, outcome, stored) in [
            (
                "pending keeps the interval",
                5,
                Outcome::Pending,
                timing(5, at(105)),
            ),
            (
                "slow_down adds five seconds",
                5,
                Outcome::SlowDown,
                timing(10, at(110)),
            ),
            (
                "slow_down adds five seconds to a short interval",
                1,
                Outcome::SlowDown,
                timing(6, at(106)),
            ),
            (
                "no answer doubles the interval",
                5,
                Outcome::NoAnswer,
                timing(10, at(110)),
            ),
            (
                "no answer doubles a long interval",
                40,
                Outcome::NoAnswer,
                timing(80, at(180)),
            ),
        ] {
            let mut schedule = resumed(timing(interval, at(100)));
            assert_eq!(schedule.after_poll(at(100), outcome), stored, "{case}");
            assert_eq!(schedule.timing(), stored, "{case}");
            // The schedule polls by the timing it returned.
            let before = stored
                .next_poll_at
                .checked_sub(Duration::from_nanos(1))
                .unwrap();
            assert_eq!(
                schedule.step(before),
                Step::Wait(Duration::from_nanos(1)),
                "{case}"
            );
            assert_eq!(schedule.step(stored.next_poll_at), Step::Poll, "{case}");
        }
    }

    #[test]
    fn an_interval_stops_at_the_longest_span_and_its_poll_at_the_last_instant() {
        for (case, seconds, outcome) in [
            ("slow_down", u64::MAX - 2, Outcome::SlowDown),
            ("no answer", u64::MAX / 2 + 1, Outcome::NoAnswer),
        ] {
            let mut schedule = resumed(timing(seconds, at(100)));
            let stored = schedule.after_poll(at(100), outcome);
            // A job stores the interval in whole seconds.
            assert_eq!(
                (stored.interval.as_secs(), stored.next_poll_at),
                (u64::MAX, LAST),
                "{case}"
            );
        }
    }

    #[test]
    fn a_changed_interval_stays_for_the_polls_after_it() {
        let mut schedule = PollSchedule::issued(at(0), Duration::from_secs(5), at(600));
        for (now, outcome, stored) in [
            (at(5), Outcome::SlowDown, timing(10, at(15))),
            (at(15), Outcome::Pending, timing(10, at(25))),
            (at(26), Outcome::NoAnswer, timing(20, at(46))),
            (at(46), Outcome::SlowDown, timing(25, at(71))),
            (at(71), Outcome::Pending, timing(25, at(96))),
        ] {
            assert_eq!(
                schedule.after_poll(now, outcome),
                stored,
                "{outcome:?} at {now:?}"
            );
        }
    }

    #[test]
    fn the_timing_stored_before_a_request_assumes_a_lost_answer() {
        for (case, interval, stored) in [
            // Up to five seconds, slow_down gives the longer interval.
            ("a one-second interval", 1, timing(6, at(136))),
            ("a four-second interval", 4, timing(9, at(139))),
            ("a five-second interval", 5, timing(10, at(140))),
            // Past five seconds, the doubled interval is the longer one.
            ("a six-second interval", 6, timing(12, at(142))),
            ("a one-minute interval", 60, timing(120, at(250))),
        ] {
            let mut schedule = resumed(timing(interval, at(100)));
            assert_eq!(schedule.before_request(at(100)), stored, "{case}");
            // The stored timing is for a worker that never sees the answer.
            // This one does, and goes on from the interval it had.
            assert_eq!(schedule.timing(), timing(interval, at(100)), "{case}");
            assert_eq!(
                schedule.after_poll(at(101), Outcome::Pending),
                timing(interval, at(101 + interval)),
                "{case}"
            );
        }
    }
}
