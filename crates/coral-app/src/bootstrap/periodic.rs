//! Periodic passes: the work the clock starts, not a request
//! ([ADR 0036](../../../../docs/adr/0036-periodic-work-runs-as-idempotent-passes-on-every-coral-instance.md)).
//!
//! # Interface
//!
//! A module exposes a pass as one function. Bootstrap wraps it in a closure
//! and gives it to [`Passes::start`] with its [`Schedule`]; the pass then
//! runs in this Coral Instance until [`Passes::stop`], or until the set is
//! dropped.
//!
//! # Invariants
//!
//! - Every Coral Instance runs every pass, so a pass is safe to run at the
//!   same moment in two of them: the rows it works on settle each unit of
//!   work.
//! - One pass never overlaps itself in one Coral Instance: the next run
//!   starts after the last returned.
//! - A pass keeps a fixed cadence anchored at its first run. A run that went
//!   through later ticks gets one run as soon as it ends, and the ticks it
//!   went through are skipped, not replayed.
//! - A failed or panicked run is logged, and the next one starts at the
//!   cadence.
//! - Stopping a pass cancels a run in flight at its next await.

use std::future::Future;
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::num::NonZeroU64;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use coral_telemetry::safe;
use tokio::task::AbortHandle;

/// What a pass asks of its schedule when it returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Next {
    /// The pass found nothing more to do: run again at the cadence.
    AtCadence,
    /// The pass stopped at its bound with work left: run again after
    /// [`SOON`], or at the next tick when that comes first.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the pass that expires old history (#2430) is the first with a bound"
        )
    )]
    Soon,
}

/// How long after a pass that stopped at its bound the next one starts.
pub(crate) const SOON: Duration = Duration::from_secs(60);

/// When a pass runs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Schedule {
    cadence: Duration,
    first_after: Duration,
    jitter: Duration,
}

impl Schedule {
    /// The first pass at once, then one each `cadence`.
    pub(crate) const fn every(cadence: Duration) -> Self {
        Self {
            cadence,
            first_after: Duration::ZERO,
            jitter: Duration::ZERO,
        }
    }

    /// The first pass after `delay`, and the cadence from there: for a pass
    /// that bootstrap already ran once.
    pub(crate) const fn first_after(mut self, delay: Duration) -> Self {
        self.first_after = delay;
        self
    }

    /// Delays the first pass by a further part of `window` that differs
    /// between Coral Instances, so that Coral Instances that started together
    /// do not run the pass at the same moment.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the pass that expires old history (#2430) is the first to take jitter"
        )
    )]
    pub(crate) const fn jittered(mut self, window: Duration) -> Self {
        self.jitter = window;
        self
    }
}

/// The periodic passes of one Coral Instance.
pub(crate) struct Passes {
    instance: String,
    running: Vec<AbortHandle>,
}

impl Passes {
    /// No passes yet, for the Coral Instance with this id.
    pub(crate) fn new(instance: &str) -> Self {
        Self {
            instance: instance.to_owned(),
            running: Vec::new(),
        }
    }

    /// Runs `pass` on `schedule` until [`Self::stop`], or until the returned
    /// handle aborts it alone. `name` is the pass's fixed name in logs; a
    /// run fails with its error's stable code.
    ///
    /// # Panics
    ///
    /// Panics when the schedule's cadence is zero.
    pub(crate) fn start<P, F>(
        &mut self,
        name: &'static str,
        schedule: Schedule,
        mut pass: P,
    ) -> AbortHandle
    where
        P: FnMut() -> F + Send + 'static,
        F: Future<Output = Result<Next, &'static str>> + Send + 'static,
    {
        assert!(
            !schedule.cadence.is_zero(),
            "a pass's cadence must be positive"
        );
        let first = tokio::time::Instant::now()
            + schedule.first_after
            + self.offset_within(schedule.jitter, name);
        let handle = tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(first, schedule.cadence);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut next = Next::AtCadence;
            loop {
                match next {
                    Next::AtCadence => drop(ticks.tick().await),
                    Next::Soon => tokio::select! {
                        () = tokio::time::sleep(SOON) => {}
                        _ = ticks.tick() => {}
                    },
                }
                next = run(name, &mut pass).await;
            }
        })
        .abort_handle();
        self.running.push(handle.clone());
        handle
    }

    /// An offset into `window` that is fixed for this Coral Instance and
    /// pass, and spread evenly over Coral Instances: their ids are random.
    fn offset_within(&self, window: Duration, name: &str) -> Duration {
        let Some(window) = NonZeroU64::new(u64::try_from(window.as_nanos()).unwrap_or(u64::MAX))
        else {
            return Duration::ZERO;
        };
        let mut hasher = DefaultHasher::new();
        (&self.instance, name).hash(&mut hasher);
        Duration::from_nanos(hasher.finish() % window)
    }

    /// Stops every pass. Idempotent.
    pub(crate) fn stop(&self) {
        for pass in &self.running {
            pass.abort();
        }
    }
}

impl Drop for Passes {
    /// A pass does not outlive its set.
    fn drop(&mut self) {
        self.stop();
    }
}

/// One run of `pass`, to its end. A failure or a panic is logged here, and
/// the schedule goes on at the cadence.
async fn run<P, F>(name: &'static str, pass: &mut P) -> Next
where
    P: FnMut() -> F,
    F: Future<Output = Result<Next, &'static str>>,
{
    // The call is inside the caught future too: a pass may panic before it
    // returns its future.
    let outcome = CaughtPanic(Box::pin(async { pass().await })).await;
    match outcome {
        Some(Ok(next)) => next,
        Some(Err(code)) => {
            tracing::warn!(
                coral.periodic.pass = safe::kind(name),
                error.type = safe::kind(code),
                "periodic pass failed; the next one runs at the cadence"
            );
            Next::AtCadence
        }
        None => {
            tracing::error!(
                coral.periodic.pass = safe::kind(name),
                error.type = safe::kind("panicked"),
                "periodic pass panicked; the next one runs at the cadence"
            );
            Next::AtCadence
        }
    }
}

/// A future that ends with `None` when its inner future panics, in place of
/// unwinding through the task that polls it. It runs the inner future in
/// that same task, so aborting the task cancels the inner future directly.
struct CaughtPanic<F>(Pin<Box<F>>);

impl<F: Future> Future for CaughtPanic<F> {
    type Output = Option<F::Output>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.0.as_mut();
        // As after any task that panics, what the pass shares with the rest
        // of the process is left as the panic found it. The inner future is
        // never polled again: this one is done.
        match std::panic::catch_unwind(AssertUnwindSafe(|| inner.poll(context))) {
            Ok(poll) => poll.map(Some),
            Err(_panic) => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::task::AbortHandle;

    use super::{Next, Passes, Schedule};

    const HOUR: Duration = Duration::from_hours(1);
    const MINUTE: Duration = Duration::from_mins(1);
    const SECOND: Duration = Duration::from_secs(1);

    /// What one scripted pass does.
    #[derive(Debug, Clone, Copy)]
    enum Step {
        Done,
        /// Takes this long, then finds nothing more to do.
        Slow(Duration),
        Soon,
        Fail,
        Panic,
        /// Panics as it is called, before it returns a future.
        PanicAtTheCall,
    }

    /// When each pass started, as time since `Passes::start`.
    type Starts = Arc<Mutex<Vec<Duration>>>;

    /// Starts a pass on `passes` that follows `script`, then finds nothing
    /// more to do.
    fn scripted(passes: &mut Passes, schedule: Schedule, script: &[Step]) -> (Starts, AbortHandle) {
        let starts = Starts::default();
        let began = tokio::time::Instant::now();
        let script = Arc::new(Mutex::new(script.iter().copied().collect::<VecDeque<_>>()));
        let seen = Arc::clone(&starts);
        let handle = passes.start("scripted", schedule, move || {
            seen.lock().unwrap().push(began.elapsed());
            let step = script.lock().unwrap().pop_front().unwrap_or(Step::Done);
            assert!(
                !matches!(step, Step::PanicAtTheCall),
                "scripted panic at the call"
            );
            async move {
                match step {
                    Step::Done => Ok(Next::AtCadence),
                    Step::Slow(took) => {
                        tokio::time::sleep(took).await;
                        Ok(Next::AtCadence)
                    }
                    Step::Soon => Ok(Next::Soon),
                    Step::Fail => Err("refused"),
                    Step::Panic | Step::PanicAtTheCall => panic!("scripted panic"),
                }
            }
        });
        (starts, handle)
    }

    #[tokio::test(start_paused = true)]
    async fn a_handle_stops_its_own_pass_and_stop_ends_them_all() {
        let mut passes = Passes::new("instance");
        let (stopped_alone, handle) = scripted(&mut passes, Schedule::every(HOUR), &[]);
        let (stopped_with_all, _) = scripted(&mut passes, Schedule::every(HOUR), &[]);
        let count = |starts: &Starts| starts.lock().unwrap().len();
        // A pass that is still in flight when everything stops.
        let ended = Arc::new(AtomicBool::new(false));
        let in_flight = Arc::clone(&ended);
        passes.start(
            "in flight",
            Schedule::every(HOUR).first_after(HOUR / 2),
            move || {
                let in_flight = Arc::clone(&in_flight);
                async move {
                    tokio::time::sleep(HOUR).await;
                    in_flight.store(true, Ordering::SeqCst);
                    Ok(Next::AtCadence)
                }
            },
        );

        tokio::time::sleep(SECOND).await;
        handle.abort();
        tokio::time::sleep(HOUR).await;
        assert_eq!((count(&stopped_alone), count(&stopped_with_all)), (1, 2));

        passes.stop();
        tokio::time::sleep(HOUR * 2).await;
        assert_eq!((count(&stopped_alone), count(&stopped_with_all)), (1, 2));
        assert!(!ended.load(Ordering::SeqCst), "stop ends a pass in flight");
    }

    #[test]
    #[should_panic(expected = "cadence must be positive")]
    fn a_zero_cadence_is_refused_when_the_pass_starts() {
        Passes::new("instance").start("zero", Schedule::every(Duration::ZERO), || async {
            Ok(Next::AtCadence)
        });
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_set_stops_its_passes() {
        let mut passes = Passes::new("instance");
        let (starts, _) = scripted(&mut passes, Schedule::every(HOUR), &[]);
        tokio::time::sleep(SECOND).await;
        drop(passes);
        tokio::time::sleep(HOUR * 2).await;
        assert_eq!(*starts.lock().unwrap(), [Duration::ZERO]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_pass_runs_on_its_schedule() {
        struct Case {
            name: &'static str,
            schedule: Schedule,
            script: Vec<Step>,
            watch: Duration,
            starts: Vec<Duration>,
        }
        let cases = [
            Case {
                name: "the first pass runs at once, then one each cadence",
                schedule: Schedule::every(HOUR),
                script: vec![],
                watch: HOUR * 2,
                starts: vec![Duration::ZERO, HOUR, HOUR * 2],
            },
            Case {
                name: "a first pass after a delay keeps the cadence from there",
                schedule: Schedule::every(HOUR).first_after(HOUR / 2),
                script: vec![],
                watch: HOUR * 2,
                starts: vec![HOUR / 2, HOUR * 3 / 2],
            },
            Case {
                name: "a pass that ran through two ticks gets one pass as it ends, then the next tick",
                schedule: Schedule::every(HOUR),
                script: vec![Step::Done, Step::Slow(HOUR * 2 + SECOND)],
                watch: HOUR * 4,
                starts: vec![Duration::ZERO, HOUR, HOUR * 3 + SECOND, HOUR * 4],
            },
            Case {
                name: "the pass after a failed one runs at the cadence",
                schedule: Schedule::every(HOUR),
                script: vec![Step::Fail],
                watch: HOUR,
                starts: vec![Duration::ZERO, HOUR],
            },
            Case {
                name: "the pass after a panicked one runs at the cadence",
                schedule: Schedule::every(HOUR),
                script: vec![Step::Panic],
                watch: HOUR,
                starts: vec![Duration::ZERO, HOUR],
            },
            Case {
                name: "the pass after one that panicked at its call runs at the cadence",
                schedule: Schedule::every(HOUR),
                script: vec![Step::PanicAtTheCall],
                watch: HOUR,
                starts: vec![Duration::ZERO, HOUR],
            },
            Case {
                name: "a pass that asks to run again soon runs each time it asks, then at the next tick",
                schedule: Schedule::every(HOUR),
                script: vec![Step::Soon, Step::Soon],
                watch: HOUR,
                starts: vec![Duration::ZERO, MINUTE, MINUTE * 2, HOUR],
            },
            Case {
                name: "soon is never later than the next tick",
                schedule: Schedule::every(MINUTE / 2),
                script: vec![Step::Soon],
                watch: MINUTE,
                starts: vec![Duration::ZERO, MINUTE / 2, MINUTE],
            },
        ];
        for case in cases {
            let mut passes = Passes::new("instance");
            let (starts, _) = scripted(&mut passes, case.schedule, &case.script);
            // One second past the last tick, so that its pass has started.
            tokio::time::sleep(case.watch + SECOND).await;
            assert_eq!(*starts.lock().unwrap(), case.starts, "{}", case.name);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn jitter_delays_the_first_pass_by_a_part_of_its_window_that_differs_between_instances() {
        let window = MINUTE * 5;
        let mut firsts = Vec::new();
        for instance in ["a", "b", "c", "d"] {
            let mut passes = Passes::new(instance);
            let schedule = Schedule::every(HOUR).jittered(window);
            let (starts, _) = scripted(&mut passes, schedule, &[]);
            tokio::time::sleep(HOUR + window).await;
            passes.stop();

            let starts = starts.lock().unwrap().clone();
            let [first, second] = starts[..] else {
                panic!("two passes in an hour and the window: {starts:?}");
            };
            assert!(first < window, "{instance}: {first:?}");
            assert_eq!(second, first + HOUR, "the cadence starts at the first pass");
            firsts.push(first);
        }
        firsts.sort();
        firsts.dedup();
        assert_eq!(firsts.len(), 4, "each instance gets its own delay");
    }

    #[tokio::test(start_paused = true)]
    async fn a_pass_that_failed_or_panicked_is_logged_by_name() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let logs = coral_telemetry::testing::ExportCapture::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(logs.log_layer()));
        let mut passes = Passes::new("instance");
        scripted(
            &mut passes,
            Schedule::every(HOUR),
            &[Step::Fail, Step::Panic],
        );
        tokio::time::sleep(HOUR + SECOND).await;

        let exported = logs.text();
        for expected in [
            "periodic pass failed",
            "refused",
            "periodic pass panicked",
            "coral.periodic.pass",
            "scripted",
        ] {
            assert!(exported.contains(expected), "{expected}: {exported}");
        }
    }
}
