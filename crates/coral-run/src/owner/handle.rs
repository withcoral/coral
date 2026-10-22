//! The Run Owner's mailbox, its handle, and the registry of live owners.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use coral_provider::account::ConnectionRequestId;
use coral_workspace::{Principal, TraceId};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::approval::{ApprovalDecision, ApprovalId, ApprovalState};
use crate::engine::{ExecutionCompletion, ProgramPackage};
use crate::execution::ExecutionStop;
use crate::invocation::{FenceRequest, ParkedInvocation};
use crate::termination::ShutdownAction;
use crate::{Error, ProgramLogLevel, ProgramValue, RunId, SourceLocation};

use super::chunk::ChunkWork;

/// The answer to one Program request, at Applied or at Recorded.
pub(crate) type Reply = oneshot::Sender<Result<(), Error>>;

/// The answer to one outside request (a decision, a cancellation), at
/// Recorded: what its chunk recorded for it.
pub(crate) type RequestReply = oneshot::Sender<Result<DecisionAnswer, Error>>;

/// What a decision request learns once it is recorded: what its chunk's
/// work wrote for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecisionAnswer {
    /// This request's decision is the one recorded, as its chunk wrote it.
    Recorded {
        state: ApprovalState,
        decided_at_unix_nanos: i64,
    },
    /// The record answers: an earlier decision or a closed request for a
    /// decision, the recorded intent for a cancellation.
    FromRecord,
}

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
/// for an entity wins; a repeat receives the recorded answer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RequestKey {
    /// A decision on one Approval Request.
    Approval(ApprovalId),
    /// The Run's termination intent: one cancellation, or the deadline.
    Termination,
}

/// A decision an outside requester asks the owner to record on one Approval
/// Request of its Run. Authorized and concealed before it reaches the owner.
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
/// host and from outside, the outcomes of the effects the hosting task ran,
/// and the two signals the hosting task adds (the drained mailbox and the
/// idle tick).
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
    /// was recorded (the credentials, the send, a Built-in Operation), or the
    /// Run is ending. An Invocation that does record a result frees its slot
    /// through [`Self::InvocationResult`] instead.
    DispatchEnded,
    /// An outside requester decides one Approval Request. The first decision
    /// on a request wins and rides the next chunk; a repeat receives the
    /// recorded answer. Answered once recorded.
    Decide {
        request: DecisionRequest,
        reply: RequestReply,
    },
    /// An outside requester cancels the Run: the termination intent and
    /// `cancellation.requested` ride the next chunk, and the Program is
    /// terminated at once. A repeat receives the recorded answer. Answered
    /// once recorded.
    Cancel {
        requester: Principal,
        reply: RequestReply,
    },
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
    /// What an outside request's work recorded in the chunk in flight.
    /// Reaches the owner before that chunk's `Committed`, which answers the
    /// requester with it.
    RequestRecorded {
        key: RequestKey,
        answer: DecisionAnswer,
    },
    /// The chunk in flight did not land, definitively: an answer the database
    /// did not give is resolved by the hosting task before it reaches here.
    CommitRejected(CommitRejection),
    /// The terminal write ran; a shutdown settle may still be due.
    Completed(Result<Option<ShutdownAction>, Error>),
    /// One settle attempt of the recorded termination ended: `Ok(true)` the
    /// Run is terminal; `Ok(false)` a dispatch is still in flight within the
    /// shutdown grace, and the owner tries again.
    SettleAttempted(Result<bool, Error>),
    /// The self-interrupt's one chunk is recorded, or another writer ended or
    /// took the Run first.
    Interrupted(Result<(), CommitRejection>),
    /// An outside writer recorded a termination and wants execution to stop.
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
            Self::Decide { request, .. } => formatter
                .debug_struct("Decide")
                .field("approval", &request.approval)
                .field("decision", &request.decision)
                .finish(),
            Self::Cancel { requester, .. } => formatter
                .debug_struct("Cancel")
                .field("requester", requester)
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
            Self::RequestRecorded { key, answer } => formatter
                .debug_struct("RequestRecorded")
                .field("key", key)
                .field("answer", answer)
                .finish(),
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
    run_id: RunId,
    claim_id: String,
    sender: mpsc::UnboundedSender<Message>,
    stop: Arc<ExecutionStop>,
    stopped: AtomicBool,
    stopped_notification: Notify,
}

/// A handle on one live Run Owner: the Program host and the Invocation tasks
/// send it Program requests, outside requesters their decisions and
/// cancellations; recovery asks it to stop.
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

    /// Whether the owner has retired (or its task ended): nothing it started
    /// needs to keep trying on its behalf.
    pub(crate) fn is_retired(&self) -> bool {
        self.shared.stopped.load(Ordering::SeqCst)
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

    /// Asks the owner to decide one Approval Request; answers once the
    /// decision, or the one that won before it, is recorded.
    ///
    /// # Errors
    ///
    /// [`Error::OwnerLost`] when the owner stopped first: read the record.
    pub(crate) async fn decide(&self, request: DecisionRequest) -> Result<DecisionAnswer, Error> {
        self.ask(|reply| Message::Decide { request, reply }).await
    }

    /// Asks the owner to cancel its Run; answers once the termination intent
    /// is recorded, or the one that won before it.
    ///
    /// # Errors
    ///
    /// [`Error::OwnerLost`] when the owner stopped first: read the record.
    pub(crate) async fn cancel(&self, requester: Principal) -> Result<(), Error> {
        self.ask(|reply| Message::Cancel { requester, reply })
            .await
            .map(drop)
    }

    /// Sends one request and waits for its answer; a retired owner, or one
    /// that dropped the reply, answers [`Error::OwnerLost`].
    async fn ask<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, Error>>) -> Message,
    ) -> Result<T, Error> {
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
