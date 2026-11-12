//! Periodic passes: the work the clock starts, not a request.
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
//! - Each Coral Instance runs its own passes. A pass that must not run in
//!   two of them at once settles that itself.
//! - One pass never overlaps itself in one Coral Instance: the next run
//!   starts after the last returned.
//! - A pass keeps a fixed cadence anchored at its first run. A run that went
//!   through later ticks gets one run as soon as it ends, and the ticks it
//!   went through are skipped, not replayed.
//! - A failed or panicked run is logged, and the next one starts at the
//!   cadence.
//! - Stopping a pass cancels a run in flight at its next await.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use coral_telemetry::safe;
use tokio::task::AbortHandle;

/// When a pass runs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Schedule {
    cadence: Duration,
    first_after: Duration,
}

impl Schedule {
    /// The first pass at once, then one each `cadence`.
    pub(crate) const fn every(cadence: Duration) -> Self {
        Self {
            cadence,
            first_after: Duration::ZERO,
        }
    }

    /// The first pass after `delay`, and the cadence from there: for a pass
    /// that bootstrap already ran once.
    pub(crate) const fn first_after(mut self, delay: Duration) -> Self {
        self.first_after = delay;
        self
    }
}

/// The periodic passes of one Coral Instance.
#[derive(Default)]
pub(crate) struct Passes {
    running: Vec<AbortHandle>,
}

impl Passes {
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
        F: Future<Output = Result<(), &'static str>> + Send + 'static,
    {
        assert!(
            !schedule.cadence.is_zero(),
            "a pass's cadence must be positive"
        );
        let first = tokio::time::Instant::now() + schedule.first_after;
        let handle = tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(first, schedule.cadence);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                run(name, &mut pass).await;
            }
        })
        .abort_handle();
        self.running.push(handle.clone());
        handle
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
async fn run<P, F>(name: &'static str, pass: &mut P)
where
    P: FnMut() -> F,
    F: Future<Output = Result<(), &'static str>>,
{
    // The call is inside the caught future too: a pass may panic before it
    // returns its future.
    let outcome = CaughtPanic(Box::pin(async { pass().await })).await;
    match outcome {
        Some(Ok(())) => {}
        Some(Err(code)) => tracing::warn!(
            coral.periodic.pass = safe::kind(name),
            error.type = safe::kind(code),
            "periodic pass failed; the next one runs at the cadence"
        ),
        None => tracing::error!(
            coral.periodic.pass = safe::kind(name),
            error.type = safe::kind("panicked"),
            "periodic pass panicked; the next one runs at the cadence"
        ),
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

    use super::{Passes, Schedule};

    const HOUR: Duration = Duration::from_hours(1);
    const SECOND: Duration = Duration::from_secs(1);

    /// What one scripted run does.
    #[derive(Debug, Clone, Copy)]
    enum Step {
        Done,
        /// Takes this long, then ends.
        Slow(Duration),
        Fail,
        Panic,
        /// Panics as it is called, before it returns a future.
        PanicAtTheCall,
    }

    /// When each run started, as time since `Passes::start`.
    type Starts = Arc<Mutex<Vec<Duration>>>;

    /// Starts a pass on `passes` whose runs follow `script`, then just end.
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
                    Step::Done => Ok(()),
                    Step::Slow(took) => {
                        tokio::time::sleep(took).await;
                        Ok(())
                    }
                    Step::Fail => Err("refused"),
                    Step::Panic | Step::PanicAtTheCall => panic!("scripted panic"),
                }
            }
        });
        (starts, handle)
    }

    #[tokio::test(start_paused = true)]
    async fn a_handle_stops_its_own_pass_and_stop_ends_them_all() {
        let mut passes = Passes::default();
        let (stopped_alone, handle) = scripted(&mut passes, Schedule::every(HOUR), &[]);
        let (stopped_with_all, _) = scripted(&mut passes, Schedule::every(HOUR), &[]);
        let count = |starts: &Starts| starts.lock().unwrap().len();
        // A run that is still in flight when everything stops.
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
                    Ok(())
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
        assert!(!ended.load(Ordering::SeqCst), "stop ends a run in flight");
    }

    #[test]
    #[should_panic(expected = "cadence must be positive")]
    fn a_zero_cadence_is_refused_when_the_pass_starts() {
        Passes::default().start("zero", Schedule::every(Duration::ZERO), || async { Ok(()) });
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_set_stops_its_passes() {
        let mut passes = Passes::default();
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
                name: "the first run is at once, then one each cadence",
                schedule: Schedule::every(HOUR),
                script: vec![],
                watch: HOUR * 2,
                starts: vec![Duration::ZERO, HOUR, HOUR * 2],
            },
            Case {
                name: "a first run after a delay keeps the cadence from there",
                schedule: Schedule::every(HOUR).first_after(HOUR / 2),
                script: vec![],
                watch: HOUR * 2,
                starts: vec![HOUR / 2, HOUR * 3 / 2],
            },
            Case {
                name: "a run that went through two ticks gets one run as it ends, then the next tick",
                schedule: Schedule::every(HOUR),
                script: vec![Step::Done, Step::Slow(HOUR * 2 + SECOND)],
                watch: HOUR * 4,
                starts: vec![Duration::ZERO, HOUR, HOUR * 3 + SECOND, HOUR * 4],
            },
            Case {
                name: "the run after a failed one starts at the cadence",
                schedule: Schedule::every(HOUR),
                script: vec![Step::Fail],
                watch: HOUR,
                starts: vec![Duration::ZERO, HOUR],
            },
            Case {
                name: "the run after a panicked one starts at the cadence",
                schedule: Schedule::every(HOUR),
                script: vec![Step::Panic],
                watch: HOUR,
                starts: vec![Duration::ZERO, HOUR],
            },
            Case {
                name: "the run after one that panicked at its call starts at the cadence",
                schedule: Schedule::every(HOUR),
                script: vec![Step::PanicAtTheCall],
                watch: HOUR,
                starts: vec![Duration::ZERO, HOUR],
            },
        ];
        for case in cases {
            let mut passes = Passes::default();
            let (starts, _) = scripted(&mut passes, case.schedule, &case.script);
            // One second past the last tick, so that its run has started.
            tokio::time::sleep(case.watch + SECOND).await;
            assert_eq!(*starts.lock().unwrap(), case.starts, "{}", case.name);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_run_that_failed_or_panicked_is_logged_by_pass_name() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let logs = coral_telemetry::testing::ExportCapture::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(logs.log_layer()));
        let mut passes = Passes::default();
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
