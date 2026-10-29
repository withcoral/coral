//! The Run Owner's mailbox, its handle, and the registry of live owners.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use coral_provider::account::ConnectionRequestId;
use coral_workspace::{Principal, TraceId};
use tokio::sync::{mpsc, oneshot, watch};

use crate::approval::{ApprovalDecision, ApprovalId};
use crate::engine::{ExecutionCompletion, ProgramPackage};
use crate::execution::ExecutionStop;
use crate::invocation::{FenceRequest, ParkedInvocation};
use crate::run_inbox::InboxRow;
use crate::termination::ShutdownAction;
use crate::types::HeldRunId;
use crate::{Error, ProgramLogLevel, ProgramValue, RunId, SourceLocation};

use super::chunk::ChunkWork;

/// The answer to one Program request, at Applied or at Recorded.
pub(crate) type Reply = oneshot::Sender<Result<(), Error>>;

/// A timer the owner arms (an [`Effect::ArmTimer`] the host runs as a sleep
/// on its own clock). It fires back into the mailbox as [`Message::Timer`],
/// applied in arrival order with every other message, so the clock decides
/// nothing outside `apply`: a passed deadline is one ordered decision
/// against the cancel or the decision that races it.
///
/// [`Effect::ArmTimer`]: super::state::Effect::ArmTimer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Timer {
    /// The Run deadline: the Run times out.
    RunDeadline,
    /// One Approval Request's deadline: the request expires and its
    /// Invocation ends.
    Approval(ApprovalId),
    /// One connection request's deadline: the Invocations parked on it stop
    /// waiting for it.
    Connection(ConnectionRequestId),
    /// Half a lease: an owner that committed nothing meanwhile renews its
    /// lease with an empty chunk. Armed again on every fire.
    IdleTick,
    /// The shutdown grace since the Program was stopped is out.
    Grace,
    /// The periodic precheck of the parked waits is due.
    Precheck,
    /// The gap since the last connection check after a change is out.
    WakeGap,
}

/// What a parked Invocation waits for, as the owner keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParkedWait {
    /// A decision on its Approval Request, which expires at the deadline.
    Approval {
        approval: ApprovalId,
        deadline_at_unix_nanos: i64,
    },
    /// Its User's connection request to end, which expires at the deadline.
    Connection {
        request: ConnectionRequestId,
        expires_at_unix_nanos: i64,
    },
}

impl ParkedWait {
    /// The owner's timer for the wait's deadline, and the deadline.
    pub(crate) fn deadline(&self) -> (Timer, i64) {
        match self {
            Self::Approval {
                approval,
                deadline_at_unix_nanos,
            } => (Timer::Approval(approval.clone()), *deadline_at_unix_nanos),
            Self::Connection {
                request,
                expires_at_unix_nanos,
            } => (Timer::Connection(*request), *expires_at_unix_nanos),
        }
    }

    pub(crate) const fn on_connection(&self) -> bool {
        matches!(self, Self::Connection { .. })
    }
}

/// One read of the parked waits in a snapshot, held nowhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Check {
    /// After the change signal rang: the connection requests the parked
    /// waits are on, and nothing else.
    Connections,
    /// The periodic precheck: every parked wait against the Dispatch Fence's
    /// facts, for a wait that ended or became moot without a wake-up.
    Periodic,
}

/// What the owner's first chunk started: the package to execute, and the
/// Run Ledger head after the execution-start mark, which the owner tracks
/// from here on.
pub(crate) struct StartedExecution {
    pub(crate) package: ProgramPackage,
    pub(crate) head: u64,
}

/// One outside request's identity: the entity it decides. The first request
/// for an entity wins; a repeat changes nothing, and its requester reads the
/// record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RequestKey {
    /// A decision on one Approval Request.
    Approval(ApprovalId),
    /// The Run's termination intent: one cancellation, or the deadline.
    Termination,
}

/// A decision an outside requester asks the owner to record on one Approval
/// Request of its Run. Authorized and concealed before it reaches the Run
/// Inbox.
#[derive(Debug, Clone)]
pub(crate) struct DecisionRequest {
    pub(crate) approval: ApprovalId,
    /// The request's deadline as the requester read it: a decision applied
    /// past it on the owner's clock is judged as the request's timer would
    /// have been first.
    pub(crate) deadline_at_unix_nanos: i64,
    pub(crate) decision: ApprovalDecision,
    pub(crate) actor: Principal,
    pub(crate) comment: Option<String>,
    pub(crate) trace: TraceId,
}

/// Everything the Run Owner's loop decides on: Run Requests from the Program
/// host and from outside, the outcomes of the effects the hosting task ran
/// (a chunk's answer and what its work recorded or parked, a snapshot check
/// of the parked waits), and the signals the hosting task adds (the drained
/// mailbox, the timers, and the change signal's rings).
pub(crate) enum Message {
    /// The owner exists; commit its first chunk.
    Launch,
    /// The first chunk's outcome: the execution to start, or nothing to run.
    Started(Result<Option<StartedExecution>, Error>),
    Emit {
        value: ProgramValue,
        location: Option<SourceLocation>,
        reply: Reply,
    },
    Log {
        level: ProgramLogLevel,
        message: String,
        location: Option<SourceLocation>,
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
    /// One ready Invocation asks for the Dispatch Fence: it joins the batch
    /// the next chunk commit decides, and answers once that chunk is recorded.
    Fence(Box<FenceRequest>),
    /// A fenced Invocation gave back its slot under the Fan-out Limit
    /// without an upstream result: the fence denied it, another writer ended
    /// it, it stopped to refresh a token, its dispatch failed before a result
    /// was recorded (the credentials, the send, a Built-in Tool), or the
    /// Run is ending. An Invocation that does record a result frees its slot
    /// through [`Self::InvocationResult`] instead.
    DispatchEnded,
    /// A decision on one Approval Request from the Run Inbox, with the row
    /// that carried it. The first decision on a request rides the next chunk,
    /// which deletes the row; a repeat, or one the expiry came before,
    /// changes nothing but has its row deleted too, and a row delivered
    /// again changes nothing.
    Decide {
        request: DecisionRequest,
        row: InboxRow,
    },
    /// A cancellation from the Run Inbox, with the row that carried it: the
    /// termination intent and `cancellation.requested` ride the next chunk,
    /// and the Program is terminated at once. That chunk deletes the row; a
    /// repeat changes nothing but has its row deleted too, and a row
    /// delivered again changes nothing.
    Cancel { requester: Principal, row: InboxRow },
    /// A timer the owner armed fired: the Run deadline, an Approval
    /// Request's or a connection request's deadline, the idle tick, the
    /// shutdown grace, the periodic precheck, or the end of the wake gap.
    Timer(Timer),
    /// A chunk that landed parked an Invocation: its held recheck found it
    /// waiting on an Approval Request or a connection request. Reaches the
    /// owner before that chunk's `Committed`.
    Parked(Box<ParkedInvocation>),
    /// The content-free change signal rang while the owner is subscribed: a
    /// hint that a connection request may have ended.
    Changed,
    /// A check of the parked waits read its snapshot: the parks (by the
    /// serial the owner gave each) whose wait it found ended or moot.
    Prechecked {
        check: Check,
        found: Result<Vec<u64>, Error>,
    },
    /// The Program's execution ended.
    ProgramEnded(ExecutionCompletion),
    /// The chunk in flight landed.
    Committed,
    /// The chunk in flight did not land, definitively: an answer the database
    /// did not give is resolved by the hosting task before it reaches here.
    CommitRejected(CommitRejection),
    /// The terminal write ran; a shutdown settle may still be due.
    Completed(Result<Option<ShutdownAction>, Error>),
    /// One settle attempt of the recorded termination ended: `Ok(true)` the
    /// Run is terminal; `Ok(false)` a dispatch is still in flight within the
    /// shutdown grace, and the owner tries again.
    SettleAttempted(Result<bool, Error>),
    /// The self-interrupt's one chunk, or a recovered owner's, is recorded,
    /// or another writer ended or took the Run first.
    Interrupted(Result<(), CommitRejection>),
    /// A recovered owner has applied every timer that passed and every
    /// request waiting in the Run Inbox: end the Run in one chunk. The sweep
    /// queues it last.
    Recover,
    /// The process interrupts the Run: its Shutdown Drain is out, or another
    /// owner registered for the Run.
    Stop,
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
            Self::Fence(request) => formatter.debug_tuple("Fence").field(request).finish(),
            Self::DispatchEnded => formatter.write_str("DispatchEnded"),
            Self::Decide { request, row } => formatter
                .debug_struct("Decide")
                .field("approval", &request.approval)
                .field("decision", &request.decision)
                .field("row", row)
                .finish(),
            Self::Cancel { requester, row } => formatter
                .debug_struct("Cancel")
                .field("requester", requester)
                .field("row", row)
                .finish(),
            Self::Timer(timer) => formatter.debug_tuple("Timer").field(timer).finish(),
            Self::Parked(parked) => formatter.debug_tuple("Parked").field(parked).finish(),
            Self::Changed => formatter.write_str("Changed"),
            Self::Prechecked { check, found } => formatter
                .debug_struct("Prechecked")
                .field("check", check)
                .field("found", found)
                .finish(),
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
            Self::SettleAttempted(result) => formatter
                .debug_tuple("SettleAttempted")
                .field(result)
                .finish(),
            Self::Interrupted(result) => {
                formatter.debug_tuple("Interrupted").field(result).finish()
            }
            Self::Recover => formatter.write_str("Recover"),
            Self::Stop => formatter.write_str("Stop"),
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
    /// The commit failed on a definite answer that is neither the Run's
    /// (a clock the host cannot read, a domain rule a work item met); the
    /// terminal write decides what the Run records, and nothing is invented.
    /// An answer the database did not give is not a rejection: the hosting
    /// task rereads the Run and recommits until it has one.
    Failed,
    /// The Run row contradicts the owner's own history: a head below the one
    /// the chunk was built on, or a head mismatch under a matching claim,
    /// which one serial owner cannot cause. Loud, never treated as a lost
    /// claim, and nothing more is written.
    Corrupt,
}

impl CommitRejection {
    /// The rejection's kind, for an event's `error.type`.
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::ClaimMismatch => "claim_mismatch",
            Self::RunEnded => "run_ended",
            Self::Refused => "refused",
            Self::Failed => "failed",
            Self::Corrupt => "corrupt",
        }
    }

    /// The answer every Program request lost with the chunk receives.
    pub(crate) fn lost(self) -> Error {
        match self {
            Self::ClaimMismatch => Error::OwnerLost,
            Self::RunEnded | Self::Refused | Self::Failed => Error::ProgramHostTerminated,
            Self::Corrupt => Error::CorruptData(
                "Run Ledger head contradicts its Run Owner's history".to_string(),
            ),
        }
    }
}

impl From<Error> for CommitRejection {
    /// The rejection a definite answer means. The owner is the only writer
    /// of a nonterminal Run's ledger, so a head mismatch under a matching
    /// claim is corruption. A row that does not decode is a failure like any
    /// other definite answer, with its own reason.
    fn from(error: Error) -> Self {
        match error {
            Error::ClaimMismatch => Self::ClaimMismatch,
            Error::RunEnded => Self::RunEnded,
            Error::HeadMismatch => Self::Corrupt,
            _definite => Self::Failed,
        }
    }
}

/// The receiving end of one owner's mailbox. It is unbounded: a Program
/// request or row work is awaited by the task that queued it, so the queue
/// never holds more than the Program's outstanding calls (one Emission or Log
/// at a time; one message per Invocation in flight, which admission does not
/// cap) plus the outside requests in flight and the owner's own few
/// messages, and the owner applies each one in memory without I/O. The
/// owner's own messages must never wait.
pub(crate) type Mailbox = mpsc::UnboundedReceiver<Message>;

struct Shared {
    run_id: HeldRunId,
    claim_id: String,
    sender: mpsc::UnboundedSender<Message>,
    stop: Arc<ExecutionStop>,
    stopped: AtomicBool,
}

/// A handle on one live Run Owner: the Program host and the Invocation tasks
/// send it Program requests, the Run Inbox reader the outside requests; the
/// Shutdown Drain asks it to stop.
#[derive(Clone)]
pub(crate) struct OwnerHandle {
    shared: Arc<Shared>,
}

impl OwnerHandle {
    pub(crate) fn new(run_id: HeldRunId, claim_id: String) -> (Self, Mailbox) {
        let (sender, mailbox) = mpsc::unbounded_channel();
        let handle = Self {
            shared: Arc::new(Shared {
                run_id,
                claim_id,
                sender,
                stop: Arc::new(ExecutionStop::new()),
                stopped: AtomicBool::new(false),
            }),
        };
        (handle, mailbox)
    }

    pub(crate) fn run_id(&self) -> &RunId {
        self.shared.run_id.run_id()
    }

    /// The Run's ID as a span field.
    pub(crate) fn held_id(&self) -> &HeldRunId {
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
        self.shared.stopped.store(true, Ordering::SeqCst);
    }

    /// Whether the owner has retired (or its task ended): nothing it started
    /// needs to keep trying on its behalf.
    pub(crate) fn is_retired(&self) -> bool {
        self.shared.stopped.load(Ordering::SeqCst)
    }

    /// Delivers one message; a retired owner has dropped its mailbox and
    /// nothing is left to tell it.
    pub(crate) fn send(&self, message: Message) {
        let _retired = self.shared.sender.send(message);
    }

    pub(crate) async fn emit(
        &self,
        value: ProgramValue,
        location: Option<SourceLocation>,
    ) -> Result<(), Error> {
        self.ask(|reply| Message::Emit {
            value,
            location,
            reply,
        })
        .await
    }

    pub(crate) async fn log(
        &self,
        level: ProgramLogLevel,
        message: String,
        location: Option<SourceLocation>,
    ) -> Result<(), Error> {
        self.ask(|reply| Message::Log {
            level,
            message,
            location,
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

    /// Queues a request for the Dispatch Fence; its outcome arrives through
    /// the request.
    pub(crate) fn fence(&self, request: FenceRequest) -> Result<(), Error> {
        self.shared
            .sender
            .send(Message::Fence(Box::new(request)))
            .map_err(|_retired| Error::OwnerLost)
    }

    /// Tells the owner that the dispatch one of its fences decided is over,
    /// so the slot it held under the Fan-out Limit is free.
    pub(crate) fn dispatch_ended(&self) {
        self.send(Message::DispatchEnded);
    }

    /// Sends one request and waits for its answer; a retired owner, or one
    /// that dropped the reply, answers [`Error::OwnerLost`].
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
    /// How many owners are live, for the Shutdown Drain to wait on.
    live: Arc<watch::Sender<usize>>,
}

impl Owners {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RunId, OwnerHandle>> {
        self.owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Registers `handle` as its Run's owner for the registration's lifetime.
    pub(crate) fn register(&self, handle: OwnerHandle) -> OwnerRegistration {
        let (registration, replaced) = self.insert(&mut self.lock(), handle);
        if let Some(replaced) = replaced {
            replaced.request_stop();
        }
        registration
    }

    /// Registers a recovered owner, unless an owner for its Run is live here
    /// already: a local owner is never swept, it continues while its commits
    /// find its claim.
    pub(crate) fn register_recovered(&self, handle: OwnerHandle) -> Option<OwnerRegistration> {
        let mut owners = self.lock();
        if owners.contains_key(handle.run_id()) {
            return None;
        }
        Some(self.insert(&mut owners, handle).0)
    }

    fn insert(
        &self,
        owners: &mut HashMap<RunId, OwnerHandle>,
        handle: OwnerHandle,
    ) -> (OwnerRegistration, Option<OwnerHandle>) {
        let replaced = owners.insert(handle.run_id().clone(), handle.clone());
        self.live.send_replace(owners.len());
        (
            OwnerRegistration {
                owners: self.clone(),
                handle,
            },
            replaced,
        )
    }

    /// The owner holding `claim_id` for `run_id`, when one is live here.
    #[cfg(test)]
    pub(crate) fn find(&self, run_id: &RunId, claim_id: &str) -> Option<OwnerHandle> {
        self.lock()
            .get(run_id)
            .filter(|handle| handle.claim_id() == claim_id)
            .cloned()
    }

    /// The owner live here for `run_id`, whatever its claim.
    pub(crate) fn get(&self, run_id: &RunId) -> Option<OwnerHandle> {
        self.lock().get(run_id).cloned()
    }

    /// Whether an owner for `run_id` is live here, whatever its claim.
    pub(crate) fn contains(&self, run_id: &RunId) -> bool {
        self.lock().contains_key(run_id)
    }

    /// Every owner live here now.
    pub(crate) fn live(&self) -> Vec<OwnerHandle> {
        self.lock().values().cloned().collect()
    }

    /// Returns once no owner is live here.
    pub(crate) async fn retired(&self) {
        let mut live = self.live.subscribe();
        // The sender lives as long as `self`.
        let _none_live = live.wait_for(|count| *count == 0).await;
    }

    fn remove(&self, handle: &OwnerHandle) {
        let mut owners = self.lock();
        if owners
            .get(handle.run_id())
            .is_some_and(|registered| registered.claim_id() == handle.claim_id())
        {
            owners.remove(handle.run_id());
            self.live.send_replace(owners.len());
        }
    }
}

/// Holds the registry entry; dropping it, also on an unwinding or cancelled
/// owner task, retires the owner: the Program and its dispatches stop, and
/// every write still retrying on its behalf gives up.
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
