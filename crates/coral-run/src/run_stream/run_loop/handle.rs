//! A Run stream loop's mailbox, its handle, and the registry of live loops.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use coral_provider::account::ConnectionRequestId;
use coral_workspace::{Principal, TraceId, WorkspaceId};
use tokio::sync::{mpsc, oneshot, watch};

use crate::approval::{ApprovalDecision, ApprovalId};
use crate::engine::ExecutionCompletion;
use crate::execution::ExecutionStop;
use crate::invocation::{EnforcementRequest, ParkedInvocation};
use crate::types::HeldRunId;
use crate::{Error, ProgramLogLevel, ProgramValue, RunId, SourceLocation};

use super::chunk::ChunkWork;

/// The answer to one Program request, at Applied or at Recorded.
pub(crate) type Reply = oneshot::Sender<Result<(), Error>>;

/// A timer the loop arms (an [`Effect::ArmTimer`] the host runs as a sleep
/// on its own clock). It fires back into the mailbox as [`Message::Timer`],
/// applied in arrival order with every other message, so the clock decides
/// nothing outside `apply`: a passed deadline is one ordered decision
/// against the Program request that races it. An outside writer's decision
/// or cancellation races it through the database instead, ordered by the
/// held rows and the Run's head.
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
    /// The periodic precheck of the parked waits is due.
    Precheck,
    /// The gap since the last connection check after a change is out.
    WakeGap,
}

/// What a parked Invocation waits for, as the loop keeps it.
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
    /// The loop's timer for the wait's deadline, and the deadline.
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
    /// The periodic precheck: every parked wait against the Policy Enforcement
    /// Point's facts, for a wait that ended or became moot without a wake-up.
    Periodic,
    /// After a writer other than the loop moved the head of a Run a Run
    /// Executor holds: the Approval Requests the parked waits are on, which
    /// that writer may have decided or expired, and nothing else.
    Approvals,
}

/// What the stream's start left: the Run Ledger head after the
/// execution-start mark, which the loop tracks from here on.
#[derive(Clone, Copy)]
pub(crate) struct StartedExecution {
    pub(crate) head: u64,
    /// The Run deadline the start mark recorded.
    pub(crate) deadline_at_unix_nanos: i64,
}

/// One outside request's identity: the entity it decides. The first request
/// for an entity wins; a repeat changes nothing, and its requester reads the
/// record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RequestKey {
    /// A decision on one Approval Request.
    Approval(ApprovalId),
}

/// A decision an outside requester records on one Approval Request of a
/// Run, authorized and concealed before it is committed.
#[derive(Debug, Clone)]
pub(crate) struct DecisionRequest {
    pub(crate) approval: ApprovalId,
    pub(crate) decision: ApprovalDecision,
    pub(crate) actor: Principal,
    pub(crate) comment: Option<String>,
    pub(crate) trace: TraceId,
}

/// Everything a Run stream's loop decides on: the stream's requests, the
/// wake-up reader's wake-ups, the outcomes of the effects the hosting task ran
/// (a chunk's answer and what its work recorded or parked, a snapshot check
/// of the parked waits), and the signals the hosting task adds (the drained
/// mailbox, the timers, and the change signal's rings).
pub(crate) enum Message {
    /// The stream's start: the execution to start, or nothing to run.
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
    /// Applied, it frees the slot the Invocation held under the Fan-out Limit
    /// when `frees_slot`: an Invocation Coral performs in the Program held none.
    InvocationResult {
        bytes: usize,
        work: Box<dyn ChunkWork>,
        frees_slot: bool,
        reply: Reply,
    },
    /// Row work whose outcome is known only once its chunk is recorded.
    Work(Box<dyn ChunkWork>),
    /// One ready Invocation asks for the Policy Enforcement Point: it joins the
    /// batch the next chunk commit decides, and answers once that chunk is
    /// recorded.
    Enforce(Box<EnforcementRequest>),
    /// An Invocation past the enforcement point gave back its slot under the
    /// Fan-out Limit without an upstream result: the enforcement point denied
    /// it, another writer ended it, it stopped to refresh a token, its dispatch
    /// failed before a result was recorded (the credentials, the send, a
    /// Built-in Tool), or the Run is ending. An Invocation that does record a
    /// result frees its slot through [`Self::InvocationResult`] instead.
    DispatchEnded,
    /// A timer the loop armed fired: the Run deadline, an Approval
    /// Request's or a connection request's deadline, the periodic precheck,
    /// or the end of the wake gap.
    Timer(Timer),
    /// A chunk that landed parked an Invocation: its held recheck found it
    /// waiting on an Approval Request or a connection request. Reaches the
    /// loop before that chunk's `Committed`.
    Parked(Box<ParkedInvocation>),
    /// The content-free change signal rang while the loop is subscribed: a
    /// hint that a connection request may have ended.
    Changed,
    /// A check of the parked waits read its snapshot: the parks (by the
    /// serial the loop gave each) whose wait it found ended or moot.
    Prechecked {
        check: Check,
        found: Result<Vec<u64>, Error>,
    },
    /// The Program's execution ended.
    ProgramEnded(ExecutionCompletion),
    /// The Run Executor ended the Program at the Run's deadline (a Run
    /// stream's `Finish`).
    DeadlineReached,
    /// The chunk in flight landed.
    Committed,
    /// The chunk in flight did not land, definitively: an answer the database
    /// did not give is resolved by the hosting task before it reaches here.
    CommitRejected(CommitRejection),
    /// The terminal write ran.
    Completed(Result<(), Error>),
    /// The mailbox is empty: commit when free.
    Drained,
    /// Run stream request `number`: `message` is what it asks of the loop,
    /// or `None` for a request that leaves nothing to record here. The
    /// loop applies requests in number order, so each chunk's watermark
    /// covers every lower request.
    Request {
        number: u64,
        message: Option<Box<Message>>,
    },
    /// The chunk in flight lands on a head another writer moved, which now
    /// records `recorded_total` output bytes; its `Committed` follows.
    Rebased { head: u64, recorded_total: usize },
    /// The Run stream is gone: the loop records what it applied and
    /// retires, and the Run stays as it is.
    Detached,
    /// The wake-up reader found a Run stream's Run ended or moved past what
    /// its loop wrote, as the Run row shows it: another writer committed on
    /// it.
    Woken(crate::run_repository::RunHead),
    /// A Run stream's chunk found its Run ended by another writer, with
    /// this termination: the reason its `Terminate` gives. Its
    /// `CommitRejected` follows.
    EndedOutside(Option<crate::lifecycle::RunTerminationKind>),
    /// A step of the Shutdown Drain for a Run stream loop.
    Drain(DrainStep),
}

/// How the Shutdown Drain moves a Run stream (ADR 0027).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainStep {
    /// Start no new dispatch; keep committing the stream's requests.
    Hold,
    /// Move the stream once no dispatch is in flight.
    Move,
    /// The drain is out: move the stream now.
    MoveNow,
    /// The drain is out for a local child's stream, which cannot move: end
    /// the Run interrupted.
    Interrupt,
}

impl fmt::Debug for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::InvocationResult {
                bytes,
                work,
                frees_slot,
                ..
            } => formatter
                .debug_struct("InvocationResult")
                .field("bytes", bytes)
                .field("work", work)
                .field("frees_slot", frees_slot)
                .finish(),
            Self::Work(work) => formatter.debug_tuple("Work").field(work).finish(),
            Self::Enforce(request) => formatter.debug_tuple("Enforce").field(request).finish(),
            Self::DispatchEnded => formatter.write_str("DispatchEnded"),
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
            Self::DeadlineReached => formatter.write_str("DeadlineReached"),
            Self::Committed => formatter.write_str("Committed"),
            Self::CommitRejected(rejection) => formatter
                .debug_tuple("CommitRejected")
                .field(rejection)
                .finish(),
            Self::Completed(result) => formatter.debug_tuple("Completed").field(result).finish(),
            Self::Drained => formatter.write_str("Drained"),
            Self::Request { number, message } => formatter
                .debug_struct("Request")
                .field("number", number)
                .field("message", message)
                .finish(),
            Self::Rebased {
                head,
                recorded_total,
            } => formatter
                .debug_struct("Rebased")
                .field("head", head)
                .field("recorded_total", recorded_total)
                .finish(),
            Self::Detached => formatter.write_str("Detached"),
            Self::Woken(run) => formatter.debug_tuple("Woken").field(run).finish(),
            Self::EndedOutside(kind) => formatter.debug_tuple("EndedOutside").field(kind).finish(),
            Self::Drain(step) => formatter.debug_tuple("Drain").field(step).finish(),
        }
    }
}

/// Why a chunk commit did not land, by failure class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitRejection {
    /// The Run row holds another execution claim, or no row: definitive, the
    /// loop has lost the Run.
    ClaimMismatch,
    /// The Run ended under another writer; no Event follows `run.ended`.
    RunEnded,
    /// The Run row no longer takes the loop's chunks though its claim
    /// matches. The terminal write decides.
    Refused,
    /// The commit failed on a definite answer that is neither the Run's
    /// (a clock the host cannot read, a domain rule a work item met); the
    /// terminal write decides what the Run records, and nothing is invented.
    /// An answer the database did not give is not a rejection: the hosting
    /// task rereads the Run and recommits until it has one.
    Failed,
    /// The Run row contradicts the loop's own history: a head below the one
    /// the chunk was built on. Loud, never treated as a lost claim, and
    /// nothing more is written.
    Corrupt,
}

impl CommitRejection {
    /// The answer every Program request lost with the chunk receives.
    pub(crate) fn lost(self) -> Error {
        match self {
            Self::ClaimMismatch => Error::OwnerLost,
            Self::RunEnded | Self::Refused | Self::Failed => Error::ProgramHostTerminated,
            Self::Corrupt => {
                Error::CorruptData("Run Ledger head contradicts its loop's history".to_string())
            }
        }
    }
}

impl From<Error> for CommitRejection {
    /// The rejection a definite answer means. A head mismatch under a
    /// matching claim reaches here only once the loop has rebuilt on the
    /// moved head, so it is corruption. A row that does not decode is a
    /// failure like any other definite answer, with its own reason.
    fn from(error: Error) -> Self {
        match error {
            Error::ClaimMismatch => Self::ClaimMismatch,
            Error::RunEnded => Self::RunEnded,
            Error::HeadMismatch => Self::Corrupt,
            _definite => Self::Failed,
        }
    }
}

/// The receiving end of one loop's mailbox. It is unbounded: a Program
/// request or row work is awaited by the task that queued it, so the queue
/// never holds more than the Program's outstanding calls (one Emission or Log
/// at a time; one message per Invocation in flight, which admission does not
/// cap) plus the wake-ups and the loop's own few messages, and the loop
/// applies each one in memory without I/O. The loop's own messages must
/// never wait.
pub(crate) type Mailbox = mpsc::UnboundedReceiver<Message>;

struct Shared {
    run_id: HeldRunId,
    claim_id: String,
    sender: mpsc::UnboundedSender<Message>,
    stop: Arc<ExecutionStop>,
    stopped: AtomicBool,
    /// Its Run's stream ended, or a reopen of it on this Coral Instance
    /// replaced it: the Run goes on without it.
    detached: AtomicBool,
    /// Its Run stream: the Shutdown Drain moves it, and the wake-up reader
    /// compares its head.
    stream: StreamHead,
    /// The number of the Run stream request being handed to the Program's
    /// host, until the call it makes takes it.
    request: std::sync::Mutex<Option<u64>>,
}

/// What a Run stream's loop publishes for its Coral Instance's wake-up
/// reader: its Run's Workspace, and the head it last wrote with
/// whether a chunk of its own is in flight.
struct StreamHead {
    workspace_id: WorkspaceId,
    /// A local child of this Coral Instance holds the stream. It reopens
    /// only here, so the Shutdown Drain lets its Run go on instead of
    /// moving it, and ends it at the drain's length.
    local_child: bool,
    /// The head shifted left by two, with [`IN_FLIGHT`] set while a chunk
    /// is in flight and [`LISTENING`] while the loop acts on a wake-up:
    /// one value, read whole.
    published: AtomicU64,
}

/// [`StreamHead::published`]: a chunk of the loop's is in flight.
const IN_FLIGHT: u64 = 1;
/// [`StreamHead::published`]: the loop acts on a wake-up. Clear before
/// its first publish, once it stopped, and while it ends its Run itself.
const LISTENING: u64 = 2;

/// The number of one Run stream request until its message reaches the
/// loop. Dropped first, it tells the loop the request leaves nothing to
/// record, so later requests do not wait for it.
pub(crate) struct RequestTicket(Option<(u64, LoopHandle)>);

impl RequestTicket {
    pub(crate) fn number(&self) -> Option<u64> {
        self.0.as_ref().map(|(number, _owner)| *number)
    }

    /// `message` as the request it answers, once.
    fn carry(&mut self, message: Message) -> Message {
        match self.0.take() {
            Some((number, _owner)) => Message::Request {
                number,
                message: Some(Box::new(message)),
            },
            None => message,
        }
    }
}

impl Drop for RequestTicket {
    fn drop(&mut self) {
        if let Some((number, run_loop)) = self.0.take() {
            run_loop.send(Message::Request {
                number,
                message: None,
            });
        }
    }
}

/// A handle on one live Run stream loop: the stream's reader and the
/// Invocation tasks send it Program requests, the wake-up reader its
/// wake-ups, and the Shutdown Drain its steps.
#[derive(Clone)]
pub(crate) struct LoopHandle {
    shared: Arc<Shared>,
}

impl LoopHandle {
    /// The handle on the loop that hosts a Run stream's Run, which the
    /// Workspace `workspace_id` owns.
    pub(crate) fn for_stream(
        run_id: HeldRunId,
        claim_id: String,
        workspace_id: WorkspaceId,
        local_child: bool,
    ) -> (Self, Mailbox) {
        let stream = StreamHead {
            workspace_id,
            local_child,
            published: AtomicU64::new(0),
        };
        let (sender, mailbox) = mpsc::unbounded_channel();
        let handle = Self {
            shared: Arc::new(Shared {
                run_id,
                claim_id,
                sender,
                stop: Arc::new(ExecutionStop::new()),
                stopped: AtomicBool::new(false),
                detached: AtomicBool::new(false),
                stream,
                request: std::sync::Mutex::new(None),
            }),
        };
        (handle, mailbox)
    }

    pub(crate) fn run_id(&self) -> &RunId {
        self.shared.run_id.run_id()
    }

    /// The Run's Workspace.
    pub(crate) fn stream_workspace(&self) -> &WorkspaceId {
        &self.shared.stream.workspace_id
    }

    /// A Run stream loop publishes `head`, the one it last wrote, whether
    /// a chunk of its own is `in_flight`, and whether it is `listening`:
    /// acting on a wake-up. `true` when it began listening with this
    /// publish.
    pub(crate) fn publish_head(&self, head: u64, in_flight: bool, listening: bool) -> bool {
        let stream = &self.shared.stream;
        let flags = if in_flight { IN_FLIGHT } else { 0 } | if listening { LISTENING } else { 0 };
        let before = stream.published.swap(head << 2 | flags, Ordering::SeqCst);
        listening && before & LISTENING == 0
    }

    /// Whether a Run row at `head` that is still `running` shows a write
    /// this listening Run stream loop has not seen: the Run ended, or its
    /// head moved past the one the loop wrote, its chunk in flight counted.
    pub(crate) fn behind(&self, head: u64, running: bool) -> bool {
        let published = self.shared.stream.published.load(Ordering::SeqCst);
        let written = (published >> 2) + (published & IN_FLIGHT);
        published & LISTENING != 0 && (!running || head > written)
    }

    /// Whether `other` is a handle on this very loop.
    pub(crate) fn is(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
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

    /// Whether a stop was requested: the loop's terminal state as a dispatch
    /// checks it before sending.
    pub(crate) fn is_stopping(&self) -> bool {
        self.shared.stop.is_requested()
    }

    pub(crate) fn mark_stopped(&self) {
        self.shared.stopped.store(true, Ordering::SeqCst);
    }

    /// Whether the loop has retired (or its task ended): nothing it started
    /// needs to keep trying on its behalf.
    pub(crate) fn is_retired(&self) -> bool {
        self.shared.stopped.load(Ordering::SeqCst)
    }

    /// Whether its Run's stream left this loop ([`Self::detach`]): it
    /// commits only on the head it tracks, and ends no call for it.
    pub(crate) fn is_detached(&self) -> bool {
        self.shared.detached.load(Ordering::SeqCst)
    }

    /// A step of the Shutdown Drain. A loop that hosts a dialed Run
    /// Executor's stream moves the stream. One that hosts a local child's
    /// stream keeps dispatching, and at the drain's length ends its Run
    /// interrupted, or timed out past its deadline.
    pub(crate) fn drain(&self, step: DrainStep) {
        match (self.shared.stream.local_child, step) {
            (false, step) => self.send(Message::Drain(step)),
            (true, DrainStep::MoveNow) => self.send(Message::Drain(DrainStep::Interrupt)),
            (true, _) => {}
        }
    }

    /// Its Run's stream ended, or reopens elsewhere: the Run goes on
    /// without this loop, which commits what it applied and retires.
    pub(crate) fn detach(&self) {
        self.shared.detached.store(true, Ordering::SeqCst);
        self.send(Message::Detached);
    }

    /// Delivers one message; a retired loop has dropped its mailbox and
    /// nothing is left to tell it.
    pub(crate) fn send(&self, message: Message) {
        let _retired = self.shared.sender.send(message);
    }

    /// Hands Run stream request `number` to the next Program call made on
    /// this thread.
    pub(crate) fn hand_request(&self, number: u64) {
        *self.request_cell() = Some(number);
    }

    /// The request number handed and not yet taken, as a ticket.
    pub(crate) fn take_request(&self) -> RequestTicket {
        RequestTicket(
            self.request_cell()
                .take()
                .map(|number| (number, self.clone())),
        )
    }

    fn request_cell(&self) -> std::sync::MutexGuard<'_, Option<u64>> {
        self.shared
            .request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) async fn emit(
        &self,
        value: ProgramValue,
        location: Option<SourceLocation>,
    ) -> Result<(), Error> {
        let mut ticket = self.take_request();
        self.ask(|reply| {
            ticket.carry(Message::Emit {
                value,
                location,
                reply,
            })
        })
        .await
    }

    pub(crate) async fn log(
        &self,
        level: ProgramLogLevel,
        message: String,
        location: Option<SourceLocation>,
    ) -> Result<(), Error> {
        let mut ticket = self.take_request();
        self.ask(|reply| {
            ticket.carry(Message::Log {
                level,
                message,
                location,
                reply,
            })
        })
        .await
    }

    pub(crate) async fn admit(
        &self,
        work: Box<dyn ChunkWork>,
        mut ticket: RequestTicket,
    ) -> Result<(), Error> {
        self.ask(|reply| ticket.carry(Message::Admit { work, reply }))
            .await
    }

    pub(crate) async fn invocation_result(
        &self,
        bytes: usize,
        work: Box<dyn ChunkWork>,
        frees_slot: bool,
    ) -> Result<(), Error> {
        self.ask(|reply| Message::InvocationResult {
            bytes,
            work,
            frees_slot,
            reply,
        })
        .await
    }

    /// Queues row work; its own outcome arrives through the work.
    pub(crate) fn work(&self, work: Box<dyn ChunkWork>) -> Result<(), Error> {
        self.shared
            .sender
            .send(Message::Work(work))
            .map_err(|_retired| Error::OwnerLost)
    }

    /// Queues a request for the Policy Enforcement Point; its outcome arrives
    /// through the request.
    pub(crate) fn enforce(&self, request: EnforcementRequest) -> Result<(), Error> {
        self.shared
            .sender
            .send(Message::Enforce(Box::new(request)))
            .map_err(|_retired| Error::OwnerLost)
    }

    /// Tells the loop that the dispatch one of its enforcement checks decided
    /// is over, so the slot it held under the Fan-out Limit is free.
    pub(crate) fn dispatch_ended(&self) {
        self.send(Message::DispatchEnded);
    }

    /// Sends one request and waits for its answer; a retired loop, or one
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

/// The live loops of this process, by Run.
#[derive(Clone, Default)]
pub(crate) struct Loops {
    loops: Arc<Mutex<HashMap<RunId, LoopHandle>>>,
    /// How many loops are live, for the Shutdown Drain to wait on.
    live: Arc<watch::Sender<usize>>,
}

impl Loops {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RunId, LoopHandle>> {
        self.loops
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Registers the loop of a Run stream: one for its Run that is live
    /// here still, whose stream ended or never reached it, is detached, not
    /// stopped, since the Run goes on.
    pub(crate) fn register_stream(&self, handle: LoopHandle) -> LoopRegistration {
        let (registration, replaced) = self.insert(&mut self.lock(), handle);
        if let Some(replaced) = replaced {
            replaced.detach();
        }
        registration
    }

    /// Detaches the loop of `run_id` live here, if any: its Run's stream
    /// reopens here, and it commits nothing more on a moved head.
    pub(crate) fn supersede(&self, run_id: &RunId) {
        if let Some(replaced) = self.get(run_id) {
            replaced.detach();
        }
    }

    fn insert(
        &self,
        loops: &mut HashMap<RunId, LoopHandle>,
        handle: LoopHandle,
    ) -> (LoopRegistration, Option<LoopHandle>) {
        let replaced = loops.insert(handle.run_id().clone(), handle.clone());
        self.live.send_replace(loops.len());
        (
            LoopRegistration {
                loops: self.clone(),
                handle,
                _active: coral_telemetry::metric::run_active(),
            },
            replaced,
        )
    }

    /// The loop holding `claim_id` for `run_id`, when one is live here.
    #[cfg(test)]
    pub(crate) fn find(&self, run_id: &RunId, claim_id: &str) -> Option<LoopHandle> {
        self.lock()
            .get(run_id)
            .filter(|handle| handle.claim_id() == claim_id)
            .cloned()
    }

    /// The loop live here for `run_id`, whatever its claim.
    pub(crate) fn get(&self, run_id: &RunId) -> Option<LoopHandle> {
        self.lock().get(run_id).cloned()
    }

    /// Whether a loop for `run_id` is live here, whatever its claim.
    pub(crate) fn contains(&self, run_id: &RunId) -> bool {
        self.lock().contains_key(run_id)
    }

    /// Every loop live here now.
    pub(crate) fn live(&self) -> Vec<LoopHandle> {
        self.lock().values().cloned().collect()
    }

    /// Every Run stream's loop live here now, with its Run's Workspace.
    pub(crate) fn streams(&self) -> Vec<(WorkspaceId, LoopHandle)> {
        self.lock()
            .values()
            .map(|run_loop| (run_loop.stream_workspace().clone(), run_loop.clone()))
            .collect()
    }

    /// Returns once no loop is live here.
    pub(crate) async fn retired(&self) {
        let mut live = self.live.subscribe();
        // The sender lives as long as `self`.
        let _none_live = live.wait_for(|count| *count == 0).await;
    }

    fn remove(&self, handle: &LoopHandle) {
        let mut loops = self.lock();
        // Only itself: a reopened Run stream's loop replaces one with the
        // same claim.
        if loops
            .get(handle.run_id())
            .is_some_and(|registered| registered.is(handle))
        {
            loops.remove(handle.run_id());
            self.live.send_replace(loops.len());
        }
    }
}

/// Holds the registry entry; dropping it, also on an unwinding or cancelled
/// loop task, retires the loop: the Program and its dispatches stop, and
/// every write still retrying on its behalf gives up.
pub(crate) struct LoopRegistration {
    loops: Loops,
    handle: LoopHandle,
    /// Counts the loop in `coral.run.active` while it is live.
    _active: coral_telemetry::metric::LevelGuard,
}

impl Drop for LoopRegistration {
    fn drop(&mut self) {
        self.handle.shared.stop.request();
        self.handle.mark_stopped();
        self.loops.remove(&self.handle);
    }
}
