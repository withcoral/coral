//! The execution isolates of a Run Executor's Runs. One that hosts more
//! than one Run keeps one fresh isolate warm, so a package does not wait to
//! make one.

use std::sync::{Mutex, MutexGuard, PoisonError};

use coral_v8::FreshIsolate;
use tokio::sync::oneshot;

use super::RunLimits;

/// What one Run does on its isolate's thread.
pub(super) type Job = Box<dyn FnOnce(Result<FreshIsolate, coral_v8::Error>) + Send>;

/// What happens to an isolate's thread, so a test can follow it.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IsolateEvent {
    /// The thread started. Told before its starter goes on, so every
    /// thread a load starts is told before the Run is handed over.
    Started(std::thread::ThreadId),
    /// The thread took a Run.
    Took(std::thread::ThreadId),
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
            *isolates.lock_warm() = isolates.warm_next();
        }
        isolates
    }

    /// Runs `job` in the warm isolate and makes the next one, or runs it in
    /// a new isolate when none is warm.
    pub(super) fn run(&self, job: Job) -> std::io::Result<()> {
        let warm = if self.keeps_warm {
            let next = self.warm_next();
            std::mem::replace(&mut *self.lock_warm(), next)
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

    fn lock_warm(&self) -> MutexGuard<'_, Option<oneshot::Sender<Job>>> {
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
        let thread = std::thread::Builder::new()
            .name("coral-run".to_owned())
            .spawn(move || {
                let isolate = FreshIsolate::new(limits);
                let Ok(job) = receiver.blocking_recv() else {
                    return;
                };
                #[cfg(test)]
                tell(on_isolate, IsolateEvent::Took(std::thread::current().id()));
                job(isolate);
            })?;
        #[cfg(test)]
        tell(on_isolate, IsolateEvent::Started(thread.thread().id()));
        // Detached: the thread ends with its Run, or unused.
        drop(thread);
        Ok(sender)
    }
}

#[cfg(test)]
fn tell(
    on_isolate: Option<&'static tokio::sync::mpsc::UnboundedSender<IsolateEvent>>,
    event: IsolateEvent,
) {
    if let Some(on_isolate) = on_isolate {
        let _unwatched = on_isolate.send(event).is_err();
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::run::tests::next;

    /// Catches: a Run lost because the warm isolate's thread ended before
    /// it, such as one that panicked while V8 made its isolate.
    #[tokio::test]
    async fn a_run_whose_warm_thread_ended_runs_in_a_new_isolate() {
        let (on_isolate, mut events) = mpsc::unbounded_channel();
        let limits = RunLimits {
            on_isolate: Some(Box::leak(Box::new(on_isolate))),
            ..RunLimits::default()
        };
        let isolates = Isolates::new(limits, 2);
        assert!(matches!(events.try_recv(), Ok(IsolateEvent::Started(_))));
        // A thread that ended takes the warm one's place, which ends unused.
        let (ended, receiver) = oneshot::channel();
        drop(receiver);
        drop(isolates.lock_warm().replace(ended));
        let (ran, fresh) = oneshot::channel();
        isolates
            .run(Box::new(move |isolate| {
                let _awaited = ran.send(isolate.is_ok()).is_ok();
            }))
            .expect("a thread starts for the Run");
        let IsolateEvent::Started(replacement) = next(&mut events, "the replacement starts").await
        else {
            panic!("the replacement starts first");
        };
        let IsolateEvent::Started(cold) = next(&mut events, "a new thread starts").await else {
            panic!("a new thread starts for the Run");
        };
        assert_ne!(cold, replacement, "the Run leaves the warm isolate warm");
        let took = next(&mut events, "the new thread takes the Run").await;
        assert_eq!(took, IsolateEvent::Took(cold));
        assert!(fresh.await.expect("the Run runs"), "in a fresh isolate");
    }
}
