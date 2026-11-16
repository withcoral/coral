//! The execution isolates of a Run Executor's Runs. One that hosts more
//! than one Run keeps one fresh isolate warm, so a package does not wait to
//! make one.

use std::sync::{Mutex, MutexGuard, PoisonError};

use coral_v8::FreshIsolate;
use tokio::sync::oneshot;

use super::RunLimits;

/// What one Run does on its isolate's thread.
pub(super) type Job = Box<dyn FnOnce(Result<FreshIsolate, coral_v8::Error>) + Send>;

/// What happens on an isolate's thread, so a test can follow it.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IsolateEvent {
    /// The thread made its isolate.
    Made(std::thread::ThreadId),
    /// The thread's isolate took a Run.
    Ran(std::thread::ThreadId),
}

/// A Run Executor's execution isolates. Each lives on a thread of its own,
/// since an isolate stays on the thread that made it, and runs one Run.
pub(crate) struct Isolates {
    limits: RunLimits,
    /// Whether one isolate is kept warm.
    keeps_warm: bool,
    /// The warm isolate's thread, waiting for its Run.
    warm: Mutex<Option<oneshot::Sender<Job>>>,
}

impl Isolates {
    /// Keeps one isolate warm when the Run Executor holds more than one Run
    /// at once.
    pub(crate) fn new(limits: RunLimits, max_runs_held: u32) -> Self {
        let keeps_warm = max_runs_held > 1;
        let isolates = Self {
            limits,
            keeps_warm,
            warm: Mutex::new(None),
        };
        if keeps_warm {
            *isolates.warm() = isolates.warm_next();
        }
        isolates
    }

    /// Runs `job` in the warm isolate and makes the next one, or runs it in
    /// a new isolate when none is warm.
    pub(super) fn run(&self, job: Job) -> std::io::Result<()> {
        let warm = if self.keeps_warm {
            let next = self.warm_next();
            std::mem::replace(&mut *self.warm(), next)
        } else {
            None
        };
        // A warm thread that ended, such as one that panicked while V8 made
        // its isolate, hands the job back.
        let job = match warm {
            Some(warm) => match warm.send(job) {
                Ok(()) => return Ok(()),
                Err(job) => job,
            },
            None => job,
        };
        self.spawn()?
            .send(job)
            .map_err(|_job| std::io::Error::other("the Run's thread ended before its Run"))
    }

    /// What each Run runs within.
    pub(super) const fn limits(&self) -> &RunLimits {
        &self.limits
    }

    fn warm(&self) -> MutexGuard<'_, Option<oneshot::Sender<Job>>> {
        self.warm.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The next warm isolate's thread; `None`, and the next Run makes its
    /// own, when no thread starts.
    fn warm_next(&self) -> Option<oneshot::Sender<Job>> {
        self.spawn()
            .inspect_err(|_error| tracing::warn!("the warm isolate's thread could not start"))
            .ok()
    }

    /// Starts a thread that makes an isolate and runs in it the one job it
    /// is sent. It ends unused when the sender drops.
    fn spawn(&self) -> std::io::Result<oneshot::Sender<Job>> {
        let (sender, receiver) = oneshot::channel::<Job>();
        let limits = self.limits.isolate;
        #[cfg(test)]
        let on_isolate = self.limits.on_isolate;
        std::thread::Builder::new()
            .name("coral-run".to_owned())
            .spawn(move || {
                let isolate = FreshIsolate::new(limits);
                #[cfg(test)]
                tell(on_isolate, IsolateEvent::Made);
                let Ok(job) = receiver.blocking_recv() else {
                    return;
                };
                #[cfg(test)]
                tell(on_isolate, IsolateEvent::Ran);
                job(isolate);
            })?;
        Ok(sender)
    }
}

#[cfg(test)]
fn tell(
    on_isolate: Option<&'static tokio::sync::mpsc::UnboundedSender<IsolateEvent>>,
    event: fn(std::thread::ThreadId) -> IsolateEvent,
) {
    if let Some(on_isolate) = on_isolate {
        let _unwatched = on_isolate.send(event(std::thread::current().id())).is_err();
    }
}
