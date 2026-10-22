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

/// Records every timer the Run Owner armed, with how long after its arming
/// it fires.
#[derive(Default)]
pub(crate) struct TimerProbe {
    armed: Mutex<Vec<(crate::owner::Timer, i64)>>,
}

impl TimerProbe {
    /// How long after its last arming `timer` fires, once armed.
    pub(crate) fn armed(&self, timer: &crate::owner::Timer) -> Option<i64> {
        self.armed
            .lock()
            .expect("armed lock")
            .iter()
            .rev()
            .find(|(armed, _)| armed == timer)
            .map(|(_, after_nanos)| *after_nanos)
    }
}

#[async_trait]
impl ExecutionObserver for TimerProbe {
    fn timer_armed(&self, timer: &crate::owner::Timer, after_nanos: i64) {
        self.armed
            .lock()
            .expect("armed lock")
            .push((timer.clone(), after_nanos));
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

/// Holds a recovered owner's one chunk until released.
pub(crate) struct RecoveryGate(BlockingGate);

impl RecoveryGate {
    pub(crate) fn blocking() -> (Arc<Self>, oneshot::Sender<()>) {
        let (gate, release) = BlockingGate::new();
        (Arc::new(Self(gate)), release)
    }

    pub(crate) fn entered(&self) -> bool {
        self.0.entered.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ExecutionObserver for RecoveryGate {
    async fn before_recovery_write(&self) {
        let _released = self.0.hold().await;
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

    /// Fence attempts that ended, permitted or not.
    pub(crate) fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    /// Every attempt as `(permit returned, durable dispatch start)`, in order.
    pub(crate) fn pairs(&self) -> Vec<(bool, bool)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|attempt| (attempt.permit_returned, attempt.durable_dispatch_start))
            .collect()
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
    async fn fence_checks_loaded(&self) {
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

/// Parks every Invocation that reaches the fence until released, so a batch
/// of ready Invocations asks the owner for its fence together; and parks
/// every fence again after it has read its facts under their holds, so a
/// test can see what else proceeds while those holds are in place. Each
/// park is released on its own; a latch that holds neither only records
/// each Invocation's fence attempt.
pub(crate) struct FenceLatch {
    pub(crate) fences: ObservedFences,
    /// Invocations that reached the fence.
    pub(crate) arrived: AtomicUsize,
    /// Fences that have read their facts and hold them.
    pub(crate) loaded: AtomicUsize,
    arrivals: watch::Sender<bool>,
    /// How many loaded fences may go on, in the order they loaded.
    loaded_permits: watch::Sender<usize>,
    failures: watch::Sender<bool>,
}

impl FenceLatch {
    /// Holds Invocations before their fence; fences pass once loaded.
    pub(crate) fn holding_arrivals() -> Arc<Self> {
        Self::new(true, false, false)
    }

    /// Holds Invocations before their fence, and fences once loaded.
    pub(crate) fn holding_both() -> Arc<Self> {
        Self::new(true, true, false)
    }

    /// Holds Invocations before their fence, and the commit of every known
    /// failure, so a fence's own chunk can be observed alone.
    pub(crate) fn holding_arrivals_and_failures() -> Arc<Self> {
        Self::new(true, false, true)
    }

    /// Holds nothing: counts the Invocations that reach the fence and the
    /// fence attempts that ended.
    pub(crate) fn counting() -> Arc<Self> {
        Self::new(false, false, false)
    }

    fn new(hold_arrivals: bool, hold_loaded_facts: bool, hold_failures: bool) -> Arc<Self> {
        Arc::new(Self {
            fences: ObservedFences::default(),
            arrived: AtomicUsize::new(0),
            loaded: AtomicUsize::new(0),
            arrivals: watch::channel(!hold_arrivals).0,
            loaded_permits: watch::channel(if hold_loaded_facts { 0 } else { usize::MAX }).0,
            failures: watch::channel(!hold_failures).0,
        })
    }

    /// Lets every known failure waiting to be recorded go on.
    pub(crate) fn release_failures(&self) {
        self.failures.send_replace(true);
    }

    /// Lets every Invocation parked before its fence go on, and later ones pass.
    pub(crate) fn release_arrivals(&self) {
        self.arrivals.send_replace(true);
    }

    /// Lets every fence parked on its loaded facts go on, and later ones pass.
    pub(crate) fn release_loaded(&self) {
        self.loaded_permits.send_replace(usize::MAX);
    }

    /// Lets the one fence that loaded its facts earliest and still parks go on.
    pub(crate) fn release_one_loaded(&self) {
        self.loaded_permits.send_modify(|permits| *permits += 1);
    }

    pub(crate) async fn until_arrived(&self, count: usize) {
        wait_until(|| self.arrived.load(Ordering::SeqCst) >= count).await;
    }

    /// Like [`Self::until_arrived`] for an arrival that may take `within` of
    /// waiting, such as one behind a parked wait's wake-up.
    pub(crate) async fn until_arrived_within(&self, count: usize, within: Duration) {
        wait_until_async_within(within, async || {
            self.arrived.load(Ordering::SeqCst) >= count
        })
        .await;
    }

    pub(crate) async fn until_loaded(&self, count: usize) {
        wait_until(|| self.loaded.load(Ordering::SeqCst) >= count).await;
    }

    async fn park(gate: &watch::Sender<bool>) {
        let mut released = gate.subscribe();
        // A dropped sender never happens: the latch outlives the test.
        let _released = released.wait_for(|released| *released).await;
    }
}

#[async_trait]
impl InvocationObserver for FenceLatch {
    async fn before_fence(&self) -> Result<(), Error> {
        self.arrived.fetch_add(1, Ordering::SeqCst);
        Self::park(&self.arrivals).await;
        Ok(())
    }

    async fn fence_checks_loaded(&self) {
        let ordinal = self.loaded.fetch_add(1, Ordering::SeqCst) + 1;
        let mut permits = self.loaded_permits.subscribe();
        // A dropped sender never happens: the latch outlives the test.
        let _permitted = permits.wait_for(|permits| *permits >= ordinal).await;
    }

    async fn before_failure_commit(&self) -> Result<(), Error> {
        Self::park(&self.failures).await;
        Ok(())
    }

    fn fence_attempt_finished(&self, permit: bool, durable_start: bool) {
        self.fences.record(permit, durable_start);
    }
}

/// Watches parked waits from both sides: counts the held rechecks, the
/// times an Invocation parks, its Dispatch Fence attempts, and the owner's
/// snapshot checks of its parked waits with what each found.
pub(crate) struct WaitProbe {
    held: watch::Sender<usize>,
    parked: watch::Sender<usize>,
    fences: ObservedFences,
    /// Each check of the parked waits: its kind, what it found, and when it
    /// finished.
    prechecks: Mutex<Vec<(crate::owner::Check, usize, tokio::time::Instant)>>,
}

impl WaitProbe {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            held: watch::channel(0).0,
            parked: watch::channel(0).0,
            fences: ObservedFences::default(),
            prechecks: Mutex::new(Vec::new()),
        })
    }

    /// Held rechecks: the approval gate's and the credential selection's.
    pub(crate) fn held(&self) -> usize {
        *self.held.borrow()
    }

    pub(crate) fn parked(&self) -> usize {
        *self.parked.borrow()
    }

    /// Dispatch Fence attempts that returned.
    pub(crate) fn fences(&self) -> usize {
        self.fences.len()
    }

    /// The owner's checks of its parked waits, in order, each with how many
    /// waits it found ended or moot.
    pub(crate) fn prechecks(&self) -> Vec<(crate::owner::Check, usize)> {
        self.prechecks
            .lock()
            .unwrap()
            .iter()
            .map(|(check, found, _)| (*check, *found))
            .collect()
    }

    /// When the first check of `check`'s kind that found a wait ended or
    /// moot finished.
    pub(crate) fn first_found(&self, check: crate::owner::Check) -> Option<tokio::time::Instant> {
        self.prechecks
            .lock()
            .unwrap()
            .iter()
            .find(|(kind, found, _)| *kind == check && *found > 0)
            .map(|(_, _, at)| *at)
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
}

#[async_trait]
impl InvocationObserver for WaitProbe {
    fn approval_authority_checked(&self) {
        self.held.send_modify(|held| *held += 1);
    }

    fn wait_parked(&self) {
        self.parked.send_modify(|parked| *parked += 1);
    }

    fn fence_attempt_finished(&self, permit: bool, durable_start: bool) {
        self.fences.record(permit, durable_start);
    }
}

#[async_trait]
impl ExecutionObserver for WaitProbe {
    fn precheck_finished(&self, check: crate::owner::Check, found: usize) {
        self.prechecks
            .lock()
            .unwrap()
            .push((check, found, tokio::time::Instant::now()));
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
    /// Blocks the owner's task on its next dequeued message until released:
    /// everything sent meanwhile is applied in one mailbox drain after it.
    hold_message: Mutex<Option<mpsc::Receiver<()>>>,
    message_held: AtomicBool,
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
            hold_message: Mutex::new(None),
            message_held: AtomicBool::new(false),
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

    /// Holds the owner's task at its next dequeued message until the sender
    /// fires (or is dropped): the messages that arrive meanwhile are applied
    /// in one mailbox drain with it, in arrival order.
    pub(crate) fn hold_next_message(&self) -> mpsc::Sender<()> {
        let (release, receiver) = mpsc::channel();
        self.message_held.store(false, Ordering::SeqCst);
        *self.hold_message.lock().expect("hold lock") = Some(receiver);
        release
    }

    pub(crate) fn message_held(&self) -> bool {
        self.message_held.load(Ordering::SeqCst)
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
        let release = self.hold_message.lock().expect("hold lock").take();
        if let Some(release) = release {
            self.message_held.store(true, Ordering::SeqCst);
            // Blocks the owner's thread on purpose; the test runs on more
            // than one worker thread.
            let _released = release.recv();
        }
    }
}
