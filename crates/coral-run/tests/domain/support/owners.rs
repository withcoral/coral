//! Run Owner test doubles: a Program that emits a burst and then waits, row
//! work that only records how it was settled, and waits on the owner's
//! ledger.
use crate::tests::*;

use crate::owner::ChunkWork;
use coral_db::Transaction;

/// Waits for `go`, emits `values` in one burst recording each acknowledgement,
/// then holds the Program until `release`, so the test can look at the ledger
/// while the Program is still running.
pub(crate) struct BurstEngine {
    pub(crate) values: Vec<String>,
    /// The Program is executing: the owner has applied its start.
    pub(crate) started: AtomicBool,
    pub(crate) acknowledged: AtomicUsize,
    pub(crate) rejected: AtomicUsize,
    go: Mutex<Option<mpsc::Receiver<()>>>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
}

pub(crate) struct Gates {
    pub(crate) go: mpsc::Sender<()>,
    pub(crate) release: mpsc::Sender<()>,
}

impl BurstEngine {
    pub(crate) fn new(values: Vec<String>) -> (Arc<Self>, Gates) {
        let (go, go_receiver) = mpsc::channel();
        let (release, release_receiver) = mpsc::channel();
        (
            Arc::new(Self {
                values,
                started: AtomicBool::new(false),
                acknowledged: AtomicUsize::new(0),
                rejected: AtomicUsize::new(0),
                go: Mutex::new(Some(go_receiver)),
                release: Mutex::new(Some(release_receiver)),
            }),
            Gates { go, release },
        )
    }

    fn wait(gate: &Mutex<Option<mpsc::Receiver<()>>>) -> bool {
        let receiver = gate.lock().expect("gate lock").take();
        receiver.is_none_or(|receiver| receiver.recv().is_ok())
    }
}

impl ProgramEngine for BurstEngine {
    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, CompileFailure> {
        Ok(ProgramPackage::scripted(source))
    }

    fn execute(
        &self,
        _package: ProgramPackage,
        host: Arc<dyn coral_v8::ProgramHost>,
        stop: Arc<ExecutionStop>,
    ) -> ExecutionCompletion {
        self.started.store(true, Ordering::SeqCst);
        if !Self::wait(&self.go) {
            return ExecutionCompletion::ProgramHostTerminated;
        }
        for value in &self.values {
            match host.emit(value, None) {
                Ok(()) => self.acknowledged.fetch_add(1, Ordering::SeqCst),
                Err(_rejected) => self.rejected.fetch_add(1, Ordering::SeqCst),
            };
        }
        if !Self::wait(&self.release) || stop.is_requested() {
            return ExecutionCompletion::ProgramHostTerminated;
        }
        ExecutionCompletion::Succeeded(FinalValue::Absent)
    }
}

/// How a [`ProbeWork`] was settled, once it was.
pub(crate) type Settled = Arc<Mutex<Option<Result<(), Error>>>>;

/// Row work that only records how it was settled.
#[derive(Debug)]
pub(crate) struct ProbeWork {
    settled: Settled,
}

#[async_trait]
impl ChunkWork for ProbeWork {
    async fn run(
        &mut self,
        _transaction: &mut Transaction<'_>,
        _run: &crate::lifecycle::RunRecord,
        _now_unix_nanos: i64,
    ) -> Result<Vec<crate::run_repository::OwnedEvent>, Error> {
        Ok(Vec::new())
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        *self.settled.lock().expect("settle lock") = Some(recorded);
    }
}

pub(crate) fn probe_work() -> (Box<dyn ChunkWork>, Settled) {
    let settled = Arc::new(Mutex::new(None));
    (
        Box::new(ProbeWork {
            settled: settled.clone(),
        }),
        settled,
    )
}

pub(crate) async fn wait_until_started(
    fixture: &TestModules,
    workspace_id: &coral_workspace::WorkspaceId,
    run_id: &crate::RunId,
) {
    wait_until_async(async || {
        fixture
            .run_record(workspace_id, run_id)
            .await
            .execution_started()
    })
    .await;
}

pub(crate) async fn wait_until_recorded(
    fixture: &TestModules,
    workspace_id: &coral_workspace::WorkspaceId,
    run_id: &crate::RunId,
    sequence: u64,
) {
    wait_until_async(async || {
        last_sequence(&fixture.run_record(workspace_id, run_id).await) >= sequence
    })
    .await;
}
