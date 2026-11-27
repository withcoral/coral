//! Invocation and execution observers that gate and record Run timing.
use crate::tests::*;

/// Returns once `gate` is open: at once when the test has opened it already.
pub(crate) async fn until_released(gate: &watch::Sender<bool>) {
    let mut released = gate.subscribe();
    // A dropped sender never happens: a gate outlives the calls it holds.
    let _released = released.wait_for(|released| *released).await;
}

/// Records every timer the loop armed, with how long after its arming
/// it fires.
#[derive(Default)]
pub(crate) struct TimerProbe {
    armed: Mutex<Vec<(crate::run_stream::run_loop::Timer, i64)>>,
}

impl TimerProbe {
    /// How long after its last arming `timer` fires, once armed.
    pub(crate) fn armed(&self, timer: &crate::run_stream::run_loop::Timer) -> Option<i64> {
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
    fn timer_armed(&self, timer: &crate::run_stream::run_loop::Timer, after_nanos: i64) {
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

/// Holds a Run stream loop's first chunk commit until released; every
/// later commit passes. Counts the messages the loop dequeued.
pub(crate) struct CommitGate {
    gate: BlockingGate,
    dequeued: AtomicUsize,
}

impl CommitGate {
    pub(crate) fn blocking() -> (Arc<Self>, oneshot::Sender<()>) {
        let (gate, release) = BlockingGate::new();
        let dequeued = AtomicUsize::new(0);
        (Arc::new(Self { gate, dequeued }), release)
    }

    pub(crate) fn entered(&self) -> bool {
        self.gate.entered.load(Ordering::SeqCst)
    }

    pub(crate) fn dequeued(&self) -> usize {
        self.dequeued.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ExecutionObserver for CommitGate {
    async fn before_chunk_commit(&self) {
        // A dropped sender releases the commit too.
        let _released = self.gate.hold().await;
    }

    fn loop_message_dequeued(&self) {
        self.dequeued.fetch_add(1, Ordering::SeqCst);
    }
}

/// Records what started each read in which the wake-up reader woke a Run
/// stream's loop; holds the reader's reads while asked, and the next chunk
/// commit once armed.
pub(crate) struct WakeProbe {
    turns: Mutex<Vec<crate::observation::Turn>>,
    reads_open: watch::Sender<bool>,
    commit: Mutex<Option<BlockingGate>>,
}

impl WakeProbe {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            turns: Mutex::default(),
            reads_open: watch::channel(true).0,
            commit: Mutex::default(),
        })
    }

    pub(crate) fn turns(&self) -> Vec<crate::observation::Turn> {
        self.turns.lock().expect("turns lock").clone()
    }

    /// Holds every read of the wake-up reader from the next one on.
    pub(crate) fn hold_reads(&self) {
        self.reads_open.send_replace(false);
    }

    /// Holds the next chunk commit until the returned sender is used.
    pub(crate) fn hold_next_commit(&self) -> oneshot::Sender<()> {
        let (gate, release) = BlockingGate::new();
        *self.commit.lock().expect("commit lock") = Some(gate);
        release
    }

    /// Whether the held chunk commit reached its hold.
    pub(crate) fn commit_held(&self) -> bool {
        self.commit.lock().expect("commit lock").as_ref().is_none()
    }
}

#[async_trait]
impl ExecutionObserver for WakeProbe {
    async fn before_chunk_commit(&self) {
        let gate = self.commit.lock().expect("commit lock").take();
        if let Some(gate) = gate {
            let _released = gate.hold().await;
        }
    }

    fn wake_delivered(&self, turn: crate::observation::Turn) {
        self.turns.lock().expect("turns lock").push(turn);
    }

    async fn before_wake_read(&self) {
        until_released(&self.reads_open).await;
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct EnforcementObservation {
    pub(crate) permit_returned: bool,
    pub(crate) durable_dispatch_start: bool,
}

#[derive(Default)]
pub(crate) struct ObservedEnforcementChecks(pub(crate) Mutex<Vec<EnforcementObservation>>);

impl ObservedEnforcementChecks {
    pub(crate) fn record(&self, permit_returned: bool, durable_dispatch_start: bool) {
        self.0.lock().unwrap().push(EnforcementObservation {
            permit_returned,
            durable_dispatch_start,
        });
    }

    /// Enforcement checks that ended, permitted or not.
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

pub(crate) struct AdvanceEnforcementTime {
    pub(crate) clock: Arc<TestClock>,
    pub(crate) now: i64,
    pub(crate) enforcement_checks: ObservedEnforcementChecks,
}

impl AdvanceEnforcementTime {
    pub(crate) fn new(clock: Arc<TestClock>, now: i64) -> Arc<Self> {
        Arc::new(Self {
            clock,
            now,
            enforcement_checks: ObservedEnforcementChecks::default(),
        })
    }
}

#[async_trait]
impl InvocationObserver for AdvanceEnforcementTime {
    async fn enforcement_checks_loaded(&self) {
        self.clock.next.store(self.now, Ordering::SeqCst);
    }

    fn enforcement_check_finished(&self, permit: bool, durable_start: bool) {
        self.enforcement_checks.record(permit, durable_start);
    }
}

/// Holds a Run at its approval wait until the gate opens.
pub(crate) struct ApprovalWaitGate(pub(crate) Arc<EnforcementGate>);

#[async_trait]
impl InvocationObserver for ApprovalWaitGate {
    async fn approval_waiting(&self) -> Result<(), Error> {
        self.0.before_enforcement().await
    }
}

pub(crate) struct EnforcementGate {
    pub(crate) enforcement_checks: ObservedEnforcementChecks,
    gate: BlockingGate,
    holds: GatePoint,
    pub(crate) failure_code: Mutex<Option<&'static str>>,
}

/// Where an [`EnforcementGate`] holds a Tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GatePoint {
    /// After the call resolved its Tool, before its Invocation is opened or
    /// admitted: no held recheck has run yet.
    Resolved,
    /// After the Invocation's held rechecks, before it asks for the Policy
    /// Enforcement Point.
    BeforeEnforcement,
}

impl EnforcementGate {
    pub(crate) fn blocking() -> (Arc<Self>, oneshot::Sender<()>) {
        Self::blocking_at(GatePoint::BeforeEnforcement)
    }

    pub(crate) fn blocking_at(holds: GatePoint) -> (Arc<Self>, oneshot::Sender<()>) {
        let (gate, release) = BlockingGate::new();
        (
            Arc::new(Self {
                enforcement_checks: ObservedEnforcementChecks::default(),
                gate,
                holds,
                failure_code: Mutex::new(None),
            }),
            release,
        )
    }

    pub(crate) fn entered(&self) -> bool {
        self.gate.entered.load(Ordering::SeqCst)
    }

    /// Holds the call that reached `point` when this gate holds there.
    async fn hold_at(&self, point: GatePoint) -> Result<(), Error> {
        if self.holds != point {
            return Ok(());
        }
        // Unwinding drops the sender. Exit the blocking Program worker without
        // further database work, even when the test runtime is shutting down.
        self.gate
            .hold()
            .await
            .map_err(|_closed| Error::ProgramHostTerminated)
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
impl InvocationObserver for EnforcementGate {
    async fn tool_resolved(&self) -> Result<(), Error> {
        self.hold_at(GatePoint::Resolved).await
    }

    fn enforcement_check_finished(&self, permit: bool, durable_start: bool) {
        self.enforcement_checks.record(permit, durable_start);
    }

    async fn before_enforcement(&self) -> Result<(), Error> {
        self.hold_at(GatePoint::BeforeEnforcement).await
    }

    fn invocation_failure_committed(&self, code: &'static str) {
        *self
            .failure_code
            .lock()
            .expect("Invocation failure observation lock") = Some(code);
    }
}

/// Parks every Invocation that reaches the enforcement point until released, so
/// a batch of ready Invocations asks the loop for its enforcement point
/// together; and parks every enforcement check again after it has read its
/// facts under their holds, so a test can see what else proceeds while those
/// holds are in place. Each park is released on its own; a latch that holds
/// neither only records each Invocation's enforcement check.
pub(crate) struct EnforcementLatch {
    pub(crate) enforcement_checks: ObservedEnforcementChecks,
    /// Invocations that reached the enforcement point.
    pub(crate) arrived: AtomicUsize,
    /// Enforcement checks that have read their facts and hold them.
    pub(crate) loaded: AtomicUsize,
    arrivals: watch::Sender<bool>,
    /// How many loaded enforcement checks may go on, in the order they loaded.
    loaded_permits: watch::Sender<usize>,
    failures: watch::Sender<bool>,
}

impl EnforcementLatch {
    /// Holds Invocations before their enforcement point; enforcement checks
    /// pass once loaded.
    pub(crate) fn holding_arrivals() -> Arc<Self> {
        Self::new(true, false, false)
    }

    /// Holds Invocations before their enforcement point, and enforcement checks
    /// once loaded.
    pub(crate) fn holding_both() -> Arc<Self> {
        Self::new(true, true, false)
    }

    /// Holds Invocations before their enforcement point, and the commit of
    /// every known failure, so an enforcement check's own chunk can be observed
    /// alone.
    pub(crate) fn holding_arrivals_and_failures() -> Arc<Self> {
        Self::new(true, false, true)
    }

    /// Holds nothing: counts the Invocations that reach the enforcement point
    /// and the enforcement checks that ended.
    pub(crate) fn counting() -> Arc<Self> {
        Self::new(false, false, false)
    }

    fn new(hold_arrivals: bool, hold_loaded_facts: bool, hold_failures: bool) -> Arc<Self> {
        Arc::new(Self {
            enforcement_checks: ObservedEnforcementChecks::default(),
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

    /// Lets every Invocation parked before its enforcement point go on, and
    /// later ones pass.
    pub(crate) fn release_arrivals(&self) {
        self.arrivals.send_replace(true);
    }

    /// Lets every enforcement check parked on its loaded facts go on, and later
    /// ones pass.
    pub(crate) fn release_loaded(&self) {
        self.loaded_permits.send_replace(usize::MAX);
    }

    /// Lets the one enforcement check that loaded its facts earliest and still
    /// parks go on.
    pub(crate) fn release_one_loaded(&self) {
        self.loaded_permits.send_modify(|permits| *permits += 1);
    }

    pub(crate) async fn until_arrived(&self, count: usize) {
        wait_until(|| self.arrived.load(Ordering::SeqCst) >= count).await;
    }

    pub(crate) async fn until_loaded(&self, count: usize) {
        wait_until(|| self.loaded.load(Ordering::SeqCst) >= count).await;
    }
}

#[async_trait]
impl InvocationObserver for EnforcementLatch {
    async fn before_enforcement(&self) -> Result<(), Error> {
        self.arrived.fetch_add(1, Ordering::SeqCst);
        until_released(&self.arrivals).await;
        Ok(())
    }

    async fn enforcement_checks_loaded(&self) {
        let ordinal = self.loaded.fetch_add(1, Ordering::SeqCst) + 1;
        let mut permits = self.loaded_permits.subscribe();
        // A dropped sender never happens: the latch outlives the test.
        let _permitted = permits.wait_for(|permits| *permits >= ordinal).await;
    }

    async fn before_failure_commit(&self) -> Result<(), Error> {
        until_released(&self.failures).await;
        Ok(())
    }

    fn enforcement_check_finished(&self, permit: bool, durable_start: bool) {
        self.enforcement_checks.record(permit, durable_start);
    }
}

/// Watches parked waits from both sides: counts the held rechecks, the
/// times an Invocation parks, its enforcement checks, and the loop's
/// snapshot checks of its parked waits with what each found.
pub(crate) struct WaitProbe {
    held: watch::Sender<usize>,
    parked: watch::Sender<usize>,
    enforcement_checks: ObservedEnforcementChecks,
    /// Each check of the parked waits: its kind, what it found, and when it
    /// finished.
    prechecks: Mutex<
        Vec<(
            crate::run_stream::run_loop::Check,
            usize,
            tokio::time::Instant,
        )>,
    >,
}

impl WaitProbe {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            held: watch::channel(0).0,
            parked: watch::channel(0).0,
            enforcement_checks: ObservedEnforcementChecks::default(),
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

    /// Enforcement checks that returned.
    pub(crate) fn enforcement_checks(&self) -> usize {
        self.enforcement_checks.len()
    }

    /// The loop's checks of its parked waits, in order, each with how many
    /// waits it found ended or moot.
    pub(crate) fn prechecks(&self) -> Vec<(crate::run_stream::run_loop::Check, usize)> {
        self.prechecks
            .lock()
            .unwrap()
            .iter()
            .map(|(check, found, _)| (*check, *found))
            .collect()
    }

    /// When the first check of `check`'s kind that found a wait ended or
    /// moot finished.
    pub(crate) fn first_found(
        &self,
        check: crate::run_stream::run_loop::Check,
    ) -> Option<tokio::time::Instant> {
        self.prechecks
            .lock()
            .unwrap()
            .iter()
            .find(|(kind, found, _)| *kind == check && *found > 0)
            .map(|(_, _, at)| *at)
    }

    /// Returns once the wait has parked `count` times in total.
    pub(crate) async fn until_parked(&self, count: usize) {
        tokio::time::timeout(EVENTUALLY, async {
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

    fn enforcement_check_finished(&self, permit: bool, durable_start: bool) {
        self.enforcement_checks.record(permit, durable_start);
    }
}

#[async_trait]
impl ExecutionObserver for WaitProbe {
    fn precheck_finished(&self, check: crate::run_stream::run_loop::Check, found: usize) {
        self.prechecks
            .lock()
            .unwrap()
            .push((check, found, tokio::time::Instant::now()));
    }
}

/// Watches a Run stream loop's chunk commits: counts them, holds the next
/// one when armed, and loses the next one's answer when armed.
pub(crate) struct CommitProbe {
    commits: AtomicUsize,
    /// Attempts still to fail as unanswered before they write.
    unanswered: AtomicUsize,
    hold: Mutex<Option<oneshot::Receiver<()>>>,
    held: AtomicBool,
    lose: Mutex<Option<oneshot::Receiver<()>>>,
    lost: AtomicBool,
    lose_start: AtomicBool,
    start_lost: AtomicBool,
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
            lose_start: AtomicBool::new(false),
            start_lost: AtomicBool::new(false),
        })
    }

    /// Loses the answer of the next Run stream start, once its start mark
    /// has landed.
    pub(crate) fn lose_next_start_answer(&self) {
        self.lose_start.store(true, Ordering::SeqCst);
    }

    pub(crate) fn start_lost(&self) -> bool {
        self.start_lost.load(Ordering::SeqCst)
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
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
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

    async fn start_commit_answer_lost(&self) -> bool {
        let lose = self.lose_start.swap(false, Ordering::SeqCst);
        self.start_lost.fetch_or(lose, Ordering::SeqCst);
        lose
    }
}
