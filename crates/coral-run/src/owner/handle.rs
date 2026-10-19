//! The Run Owner's mailbox, its handle, and the registry of live owners.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, mpsc, oneshot};

use crate::engine::{ExecutionCompletion, ProgramPackage};
use crate::execution::ExecutionStop;
use crate::termination::ShutdownAction;
use crate::{Error, ProgramLogLevel, ProgramValue, RunId};

use super::chunk::ChunkWork;

/// The answer to one Program request, at Applied or at Recorded.
pub(crate) type Reply = oneshot::Sender<Result<(), Error>>;

/// Everything the Run Owner's loop decides on: Run Requests from the Program
/// host, the outcomes of the effects the hosting task ran, and the two signals
/// the hosting task adds (the drained mailbox and the idle tick).
pub(crate) enum Message {
    /// The owner exists; commit its first chunk.
    Launch,
    /// The first chunk's outcome: the package to execute, or nothing to run.
    Started(Result<Option<ProgramPackage>, Error>),
    Emit {
        value: ProgramValue,
        reply: Reply,
    },
    Log {
        level: ProgramLogLevel,
        message: String,
        reply: Reply,
    },
    /// Admit one Invocation: its row and `invocation.started` ride the next chunk.
    Admit {
        work: Box<dyn ChunkWork>,
        reply: Reply,
    },
    /// One Invocation's upstream result, decided against the Run Output Budget.
    InvocationResult {
        bytes: usize,
        work: Box<dyn ChunkWork>,
        reply: Reply,
    },
    /// Row work whose outcome is known only once its chunk is recorded.
    Work(Box<dyn ChunkWork>),
    /// The Program's execution ended.
    ProgramEnded(ExecutionCompletion),
    /// The chunk in flight landed.
    Committed,
    /// The chunk in flight did not land.
    CommitRejected(CommitRejection),
    /// The terminal write ran; a shutdown settle may still be due.
    Completed(Result<Option<ShutdownAction>, Error>),
    /// The recorded termination is settled: the Run is terminal.
    Settled,
    /// An outside writer recorded a termination and wants execution to stop.
    Stop,
    /// Half a lease passed since the last commit.
    IdleTick,
    /// The mailbox is empty: commit when free.
    Drained,
}

impl fmt::Debug for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Launch => formatter.write_str("Launch"),
            Self::Started(result) => formatter
                .debug_tuple("Started")
                .field(&result.as_ref().map(Option::is_some))
                .finish(),
            Self::Emit { value, .. } => formatter
                .debug_struct("Emit")
                .field("value", value)
                .finish(),
            Self::Log { level, message, .. } => formatter
                .debug_struct("Log")
                .field("level", level)
                .field("message", message)
                .finish(),
            Self::Admit { work, .. } => {
                formatter.debug_struct("Admit").field("work", work).finish()
            }
            Self::InvocationResult { bytes, work, .. } => formatter
                .debug_struct("InvocationResult")
                .field("bytes", bytes)
                .field("work", work)
                .finish(),
            Self::Work(work) => formatter.debug_tuple("Work").field(work).finish(),
            Self::ProgramEnded(completion) => formatter
                .debug_tuple("ProgramEnded")
                .field(completion)
                .finish(),
            Self::Committed => formatter.write_str("Committed"),
            Self::CommitRejected(rejection) => formatter
                .debug_tuple("CommitRejected")
                .field(rejection)
                .finish(),
            Self::Completed(result) => formatter
                .debug_tuple("Completed")
                .field(&result.as_ref().map(Option::is_some))
                .finish(),
            Self::Settled => formatter.write_str("Settled"),
            Self::Stop => formatter.write_str("Stop"),
            Self::IdleTick => formatter.write_str("IdleTick"),
            Self::Drained => formatter.write_str("Drained"),
        }
    }
}

/// Why a chunk commit did not land, by failure class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitRejection {
    /// The Run row holds another execution claim, or no row: definitive, the
    /// owner has lost the Run.
    ClaimMismatch,
    /// The Run ended under another writer; no Event follows `run.ended`.
    RunEnded,
    /// The Run row no longer takes Program work: its deadline passed, or an
    /// outside writer released the worker claim. The terminal write decides.
    Refused,
    /// The commit failed operationally; the terminal write decides what the
    /// Run records, and nothing is invented.
    Failed,
}

impl CommitRejection {
    /// The answer every request lost with the chunk receives.
    pub(crate) fn lost(self) -> Error {
        match self {
            Self::ClaimMismatch => Error::OwnerLost,
            Self::RunEnded | Self::Refused | Self::Failed => Error::ProgramHostTerminated,
        }
    }
}

impl From<Error> for CommitRejection {
    fn from(error: Error) -> Self {
        match error {
            Error::ClaimMismatch => Self::ClaimMismatch,
            Error::RunEnded => Self::RunEnded,
            _operational => Self::Failed,
        }
    }
}

/// The receiving end of one owner's mailbox. It is unbounded: a Program
/// request or row work is awaited by the task that queued it, so the queue
/// never holds more than the Program's outstanding calls (one Emission or Log
/// at a time; one message per Invocation in flight, which admission does not
/// cap) plus the owner's own few messages, and the owner applies each one in
/// memory without I/O. The owner's own messages must never wait.
pub(crate) type Mailbox = mpsc::UnboundedReceiver<Message>;

struct Shared {
    run_id: RunId,
    claim_id: String,
    sender: mpsc::UnboundedSender<Message>,
    stop: Arc<ExecutionStop>,
    stopped: AtomicBool,
    stopped_notification: Notify,
}

/// A handle on one live Run Owner: the Program host and the Invocation tasks
/// send it Run Requests; outside writers ask it to stop.
#[derive(Clone)]
pub(crate) struct OwnerHandle {
    shared: Arc<Shared>,
}

impl OwnerHandle {
    pub(crate) fn new(run_id: RunId, claim_id: String) -> (Self, Mailbox) {
        let (sender, mailbox) = mpsc::unbounded_channel();
        let handle = Self {
            shared: Arc::new(Shared {
                run_id,
                claim_id,
                sender,
                stop: Arc::new(ExecutionStop::new()),
                stopped: AtomicBool::new(false),
                stopped_notification: Notify::new(),
            }),
        };
        (handle, mailbox)
    }

    pub(crate) fn run_id(&self) -> &RunId {
        &self.shared.run_id
    }

    pub(crate) fn claim_id(&self) -> &str {
        &self.shared.claim_id
    }

    /// The stop shared with the Program and its dispatches.
    pub(crate) fn stop(&self) -> Arc<ExecutionStop> {
        self.shared.stop.clone()
    }

    /// Terminates the Program at once and tells the owner no more Program
    /// requests are accepted. The owner still records what it applied.
    pub(crate) fn request_stop(&self) {
        self.shared.stop.request();
        self.send(Message::Stop);
    }

    /// Whether a stop was requested: the owner's terminal state as a dispatch
    /// checks it before sending.
    pub(crate) fn is_stopping(&self) -> bool {
        self.shared.stop.is_requested()
    }

    pub(crate) fn mark_stopped(&self) {
        if !self.shared.stopped.swap(true, Ordering::SeqCst) {
            self.shared.stopped_notification.notify_waiters();
        }
    }

    /// Returns once the owner has retired: no chunk commit of its own follows.
    pub(crate) async fn wait_stopped(&self) {
        if self.shared.stopped.load(Ordering::SeqCst) {
            return;
        }
        let notified = self.shared.stopped_notification.notified();
        if self.shared.stopped.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    /// Delivers one message; a retired owner has dropped its mailbox and
    /// nothing is left to tell it.
    pub(crate) fn send(&self, message: Message) {
        let _retired = self.shared.sender.send(message);
    }

    pub(crate) async fn emit(&self, value: ProgramValue) -> Result<(), Error> {
        self.ask(|reply| Message::Emit { value, reply }).await
    }

    pub(crate) async fn log(&self, level: ProgramLogLevel, message: String) -> Result<(), Error> {
        self.ask(|reply| Message::Log {
            level,
            message,
            reply,
        })
        .await
    }

    pub(crate) async fn admit(&self, work: Box<dyn ChunkWork>) -> Result<(), Error> {
        self.ask(|reply| Message::Admit { work, reply }).await
    }

    pub(crate) async fn invocation_result(
        &self,
        bytes: usize,
        work: Box<dyn ChunkWork>,
    ) -> Result<(), Error> {
        self.ask(|reply| Message::InvocationResult { bytes, work, reply })
            .await
    }

    /// Queues row work; its own outcome arrives through the work.
    pub(crate) fn work(&self, work: Box<dyn ChunkWork>) -> Result<(), Error> {
        self.shared
            .sender
            .send(Message::Work(work))
            .map_err(|_retired| Error::OwnerLost)
    }

    async fn ask(&self, build: impl FnOnce(Reply) -> Message) -> Result<(), Error> {
        let (reply, answer) = oneshot::channel();
        self.shared
            .sender
            .send(build(reply))
            .map_err(|_retired| Error::OwnerLost)?;
        answer.await.map_err(|_retired| Error::OwnerLost)?
    }
}

/// The live Run Owners of this process, by Run.
#[derive(Clone, Default)]
pub(crate) struct Owners {
    owners: Arc<Mutex<HashMap<RunId, OwnerHandle>>>,
}

impl Owners {
    /// Registers `handle` as its Run's owner for the registration's lifetime.
    pub(crate) fn register(&self, handle: OwnerHandle) -> OwnerRegistration {
        let replaced = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(handle.run_id().clone(), handle.clone());
        if let Some(replaced) = replaced {
            replaced.request_stop();
        }
        OwnerRegistration {
            owners: self.clone(),
            handle,
        }
    }

    /// The owner holding `claim_id` for `run_id`, when one is live here.
    pub(crate) fn find(&self, run_id: &RunId, claim_id: &str) -> Option<OwnerHandle> {
        self.owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(run_id)
            .filter(|handle| handle.claim_id() == claim_id)
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, run_id: &RunId) -> bool {
        self.owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(run_id)
    }

    fn remove(&self, handle: &OwnerHandle) {
        let mut owners = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if owners
            .get(handle.run_id())
            .is_some_and(|registered| registered.claim_id() == handle.claim_id())
        {
            owners.remove(handle.run_id());
        }
    }
}

/// Holds the registry entry; dropping it, also on an unwinding or cancelled
/// owner task, retires the owner: the Program and its dispatches stop, and
/// everything waiting for the owner to stop wakes.
pub(crate) struct OwnerRegistration {
    owners: Owners,
    handle: OwnerHandle,
}

impl Drop for OwnerRegistration {
    fn drop(&mut self) {
        self.handle.shared.stop.request();
        self.handle.mark_stopped();
        self.owners.remove(&self.handle);
    }
}
