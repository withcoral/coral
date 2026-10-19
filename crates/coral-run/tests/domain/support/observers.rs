//! Invocation and execution observers that gate and record Run timing.
use crate::tests::*;

#[derive(Default)]
pub(crate) struct ClaimGate {
    pub(crate) claims: AtomicUsize,
    pub(crate) finished: AtomicUsize,
    pub(crate) entered: AtomicBool,
    pub(crate) release: AtomicBool,
}

#[async_trait]
impl ExecutionObserver for ClaimGate {
    async fn owner_starting(&self, _run_id: &crate::RunId, _claim_id: &str) {
        if self.claims.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        }
    }

    fn execution_attempt_finished(&self) {
        self.finished.fetch_add(1, Ordering::SeqCst);
    }
}

/// Holds the first caller that reaches it until released; every later caller
/// passes. The gates below embed it at the hook they hold.
pub(crate) struct BlockingGate {
    pub(crate) entered: AtomicBool,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl BlockingGate {
    pub(crate) fn new() -> (Self, oneshot::Sender<()>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                entered: AtomicBool::new(false),
                release: Mutex::new(Some(receiver)),
            },
            sender,
        )
    }

    /// Marks the gate entered and, for the first caller, waits for the
    /// release. The result says whether the sender released it or was dropped.
    pub(crate) async fn hold(&self) -> Result<(), oneshot::error::RecvError> {
        let release = self.release.lock().expect("gate lock").take();
        self.entered.store(true, Ordering::SeqCst);
        match release {
            Some(release) => release.await,
            None => Ok(()),
        }
    }
}

/// Holds the Run Owner's first chunk commit (after its start mark) until
/// released; every later commit passes.
pub(crate) struct CommitGate(BlockingGate);

impl CommitGate {
    pub(crate) fn blocking() -> (Arc<Self>, oneshot::Sender<()>) {
        let (gate, release) = BlockingGate::new();
        (Arc::new(Self(gate)), release)
    }

    pub(crate) fn entered(&self) -> bool {
        self.0.entered.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ExecutionObserver for CommitGate {
    async fn before_chunk_commit(&self) {
        // A dropped sender releases the commit too.
        let _released = self.0.hold().await;
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FenceObservation {
    pub(crate) permit_returned: bool,
    pub(crate) durable_dispatch_start: bool,
}

#[derive(Default)]
pub(crate) struct ObservedFences(pub(crate) Mutex<Vec<FenceObservation>>);

impl ObservedFences {
    pub(crate) fn record(&self, permit_returned: bool, durable_dispatch_start: bool) {
        self.0.lock().unwrap().push(FenceObservation {
            permit_returned,
            durable_dispatch_start,
        });
    }

    pub(crate) fn assert_no_dispatch(&self) {
        let attempts = self.0.lock().unwrap();
        assert!(
            attempts
                .iter()
                .all(|attempt| !attempt.permit_returned && !attempt.durable_dispatch_start),
            "losing attempts must return no permit and write no dispatch-start: {attempts:?}"
        );
    }
}

pub(crate) struct AdvanceFenceTime {
    pub(crate) clock: Arc<TestClock>,
    pub(crate) now: i64,
    pub(crate) fences: ObservedFences,
}

impl AdvanceFenceTime {
    pub(crate) fn new(clock: Arc<TestClock>, now: i64) -> Arc<Self> {
        Arc::new(Self {
            clock,
            now,
            fences: ObservedFences::default(),
        })
    }
}

#[async_trait]
impl InvocationObserver for AdvanceFenceTime {
    fn fence_checks_loaded(&self) {
        self.clock.next.store(self.now, Ordering::SeqCst);
    }

    fn fence_attempt_finished(&self, permit: bool, durable_start: bool) {
        self.fences.record(permit, durable_start);
    }
}

/// Holds a Run at its approval wait until the gate opens.
pub(crate) struct ApprovalWaitGate(pub(crate) Arc<FenceGate>);

#[async_trait]
impl InvocationObserver for ApprovalWaitGate {
    async fn approval_waiting(&self) -> Result<(), Error> {
        self.0.before_fence().await
    }
}

pub(crate) struct FenceGate {
    pub(crate) fences: ObservedFences,
    gate: BlockingGate,
    pub(crate) failure_code: Mutex<Option<&'static str>>,
}

impl FenceGate {
    pub(crate) fn blocking() -> (Arc<Self>, oneshot::Sender<()>) {
        let (gate, release) = BlockingGate::new();
        (
            Arc::new(Self {
                fences: ObservedFences::default(),
                gate,
                failure_code: Mutex::new(None),
            }),
            release,
        )
    }

    pub(crate) fn entered(&self) -> bool {
        self.gate.entered.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
pub(crate) struct TerminalCommitFault {
    pub(crate) attempted: AtomicBool,
    pub(crate) completion_checked: AtomicBool,
}

#[async_trait]
impl InvocationObserver for TerminalCommitFault {
    async fn before_terminal_commit(&self) -> Result<(), String> {
        self.attempted.store(true, Ordering::SeqCst);
        Err("scripted terminal commit failure".to_string())
    }

    fn run_completion_checked(&self, has_nonterminal_invocation: bool) {
        self.completion_checked
            .store(has_nonterminal_invocation, Ordering::SeqCst);
    }
}

#[async_trait]
impl InvocationObserver for FenceGate {
    fn fence_attempt_finished(&self, permit: bool, durable_start: bool) {
        self.fences.record(permit, durable_start);
    }

    async fn before_fence(&self) -> Result<(), Error> {
        // Unwinding drops the sender. Exit the blocking Program worker without
        // further database work, even when the test runtime is shutting down.
        self.gate
            .hold()
            .await
            .map_err(|_closed| Error::ProgramHostTerminated)
    }

    fn invocation_failure_committed(&self, code: &'static str) {
        *self
            .failure_code
            .lock()
            .expect("Invocation failure observation lock") = Some(code);
    }
}

/// Counts held approval rechecks and the times a waiting Invocation parks
/// until a commit, an expiry, or the fallback poll.
pub(crate) struct ApprovalWaitProbe {
    gate: Option<Arc<FenceGate>>,
    held: watch::Sender<usize>,
    parked: watch::Sender<usize>,
}

impl ApprovalWaitProbe {
    pub(crate) fn new() -> Arc<Self> {
        Self::with_gate(None)
    }

    /// Holds the first approval wait before its first snapshot until the sender fires.
    pub(crate) fn gated() -> (Arc<Self>, oneshot::Sender<()>) {
        let (gate, release) = FenceGate::blocking();
        (Self::with_gate(Some(gate)), release)
    }

    fn with_gate(gate: Option<Arc<FenceGate>>) -> Arc<Self> {
        Arc::new(Self {
            gate,
            held: watch::channel(0).0,
            parked: watch::channel(0).0,
        })
    }

    pub(crate) fn held(&self) -> usize {
        *self.held.borrow()
    }

    pub(crate) fn parked(&self) -> usize {
        *self.parked.borrow()
    }

    /// Returns once the wait has parked `count` times in total.
    pub(crate) async fn until_parked(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut parked = self.parked.subscribe();
            parked.wait_for(|parked| *parked >= count).await.map(|_| ())
        })
        .await
        .expect("the Invocation parks in its wait")
        .expect("probe is alive");
    }

    /// Returns once `count` held rechecks have run in total, or fails after `within`.
    pub(crate) async fn until_held(&self, count: usize, within: Duration) {
        tokio::time::timeout(within, async {
            let mut held = self.held.subscribe();
            held.wait_for(|held| *held >= count).await.map(|_| ())
        })
        .await
        .expect("the wait runs a held recheck in time")
        .expect("probe is alive");
    }
}

#[async_trait]
impl InvocationObserver for ApprovalWaitProbe {
    fn approval_authority_checked(&self) {
        self.held.send_modify(|held| *held += 1);
    }

    async fn approval_waiting(&self) -> Result<(), Error> {
        match &self.gate {
            Some(gate) => gate.before_fence().await,
            None => Ok(()),
        }
    }

    fn change_wait_started(&self) {
        self.parked.send_modify(|parked| *parked += 1);
    }
}

/// Watches the Run Owner's chunk commits: counts them, holds the next one
/// when armed, loses the next one's answer when armed, and panics the
/// owner's task on its next message when armed.
pub(crate) struct CommitProbe {
    commits: AtomicUsize,
    /// Attempts still to fail as unanswered before they write.
    unanswered: AtomicUsize,
    hold: Mutex<Option<oneshot::Receiver<()>>>,
    held: AtomicBool,
    lose: Mutex<Option<oneshot::Receiver<()>>>,
    lost: AtomicBool,
    panic_on_next_message: AtomicBool,
}

impl CommitProbe {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            commits: AtomicUsize::new(0),
            unanswered: AtomicUsize::new(0),
            hold: Mutex::new(None),
            held: AtomicBool::new(false),
            lose: Mutex::new(None),
            lost: AtomicBool::new(false),
            panic_on_next_message: AtomicBool::new(false),
        })
    }

    /// Chunk commit attempts so far, recommits included.
    pub(crate) fn commits(&self) -> usize {
        self.commits.load(Ordering::SeqCst)
    }

    /// Fails the next `count` chunk commit attempts as unanswered before
    /// they write, as a database that drops the connection would.
    pub(crate) fn unanswer_next_attempts(&self, count: usize) {
        self.unanswered.store(count, Ordering::SeqCst);
    }

    /// Holds the next chunk commit before its transaction until the sender
    /// fires (or is dropped).
    pub(crate) fn hold_next_commit(&self) -> oneshot::Sender<()> {
        let (release, receiver) = oneshot::channel();
        self.held.store(false, Ordering::SeqCst);
        *self.hold.lock().expect("hold lock") = Some(receiver);
        release
    }

    pub(crate) fn held(&self) -> bool {
        self.held.load(Ordering::SeqCst)
    }

    /// Loses the next chunk commit's answer: once that chunk has landed, the
    /// commit waits here until the sender fires (or is dropped), then
    /// resolves as one the database never answered.
    pub(crate) fn lose_next_answer(&self) -> oneshot::Sender<()> {
        let (release, receiver) = oneshot::channel();
        self.lost.store(false, Ordering::SeqCst);
        *self.lose.lock().expect("lose lock") = Some(receiver);
        release
    }

    pub(crate) fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }

    /// Panics the owner's task when it dequeues its next message.
    pub(crate) fn panic_on_next_message(&self) {
        self.panic_on_next_message.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ExecutionObserver for CommitProbe {
    async fn before_chunk_commit(&self) {
        self.commits.fetch_add(1, Ordering::SeqCst);
        let release = self.hold.lock().expect("hold lock").take();
        if let Some(release) = release {
            self.held.store(true, Ordering::SeqCst);
            let _released = release.await;
        }
    }

    async fn chunk_commit_unanswered(&self) -> bool {
        self.unanswered
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
    }

    async fn chunk_commit_answer_lost(&self) -> bool {
        let release = self.lose.lock().expect("lose lock").take();
        match release {
            Some(release) => {
                self.lost.store(true, Ordering::SeqCst);
                let _released = release.await;
                true
            }
            None => false,
        }
    }

    fn owner_message_dequeued(&self) {
        assert!(
            !self.panic_on_next_message.swap(false, Ordering::SeqCst),
            "scripted Run Owner task panic"
        );
    }
}
