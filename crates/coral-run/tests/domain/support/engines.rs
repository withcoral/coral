//! Scripted Program engines that gate or observe execution.
use crate::tests::*;

#[derive(Clone)]
pub(crate) struct ClosureReadGateEngine {
    pub(crate) reading_root: Arc<AtomicBool>,
    pub(crate) release: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl ClosureReadGateEngine {
    pub(crate) fn new() -> (Self, mpsc::Sender<()>) {
        let (sender, receiver) = mpsc::channel();
        (
            Self {
                reading_root: Arc::new(AtomicBool::new(false)),
                release: Arc::new(Mutex::new(receiver)),
            },
            sender,
        )
    }
}

impl ProgramEngine for ClosureReadGateEngine {
    fn program_saved_function_references(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
    ) -> Result<Vec<SavedFunctionReference>, String> {
        test_saved_function_reference("closure.root").map(|reference| vec![reference])
    }

    fn saved_function_dependency_references(
        &self,
        _deadline: std::time::Instant,
        _path: &SavedFunctionPath,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, String> {
        if !source.contains("root-v1") {
            return Ok(Vec::new());
        }
        self.reading_root.store(true, Ordering::SeqCst);
        self.release
            .lock()
            .map_err(|error| error.to_string())?
            .recv()
            .map_err(|error| error.to_string())?;
        test_saved_function_reference("closure.dependency").map(|reference| vec![reference])
    }

    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn compile_with_saved_functions(
        &self,
        _deadline: std::time::Instant,
        source: &str,
        _linked: &[SavedFunctionSelection],
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn execute(
        &self,
        _package: ProgramPackage,
        _host: Arc<dyn coral_v8::ProgramHost>,
        _stop: Arc<crate::execution::ExecutionStop>,
    ) -> ExecutionCompletion {
        ExecutionCompletion::Succeeded(FinalValue::Absent)
    }
}

#[derive(Clone)]
pub(crate) struct SnapshotEngine {
    pub(crate) started: Arc<AtomicBool>,
    pub(crate) release: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl SnapshotEngine {
    pub(crate) fn new() -> (Self, mpsc::Sender<()>) {
        let (sender, receiver) = mpsc::channel();
        (
            Self {
                started: Arc::new(AtomicBool::new(false)),
                release: Arc::new(Mutex::new(receiver)),
            },
            sender,
        )
    }
}

impl ProgramEngine for SnapshotEngine {
    fn program_saved_function_references(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
    ) -> Result<Vec<SavedFunctionReference>, String> {
        test_saved_function_reference("snapshots.value").map(|reference| vec![reference])
    }

    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn compile_with_saved_functions(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
        linked: &[SavedFunctionSelection],
    ) -> Result<ProgramPackage, String> {
        let linked = linked
            .first()
            .ok_or_else(|| "Saved Function snapshot is missing".to_string())?;
        Ok(ProgramPackage::scripted(linked.source().as_str()))
    }

    fn execute(
        &self,
        package: ProgramPackage,
        _host: Arc<dyn coral_v8::ProgramHost>,
        _stop: Arc<crate::execution::ExecutionStop>,
    ) -> ExecutionCompletion {
        self.started.store(true, Ordering::SeqCst);
        if self
            .release
            .lock()
            .expect("snapshot release lock")
            .recv()
            .is_err()
        {
            return ExecutionCompletion::Failed(crate::RunFailure::new(
                RunFailureCode::ProgramExecutionFailed,
                "Snapshot test release failed".to_string(),
            ));
        }
        ExecutionCompletion::Succeeded(FinalValue::Present(
            ProgramValue::from_json(&serde_json::json!({"linked_source": package.javascript}))
                .expect("snapshot Program Value"),
        ))
    }
}

#[derive(Clone, Default)]
pub(crate) struct ImmediateEngine {
    pub(crate) executions: Arc<AtomicUsize>,
}

impl ProgramEngine for ImmediateEngine {
    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        if source == "compile-error" {
            return Err("scripted compilation failure".to_string());
        }
        let mut package = ProgramPackage::scripted(source);
        if source == "oversized-package" {
            package.javascript = "x".repeat(crate::EMITTED_JAVASCRIPT_MAX_BYTES + 1);
        } else if source == "oversized-source-map" {
            package.source_map = "x".repeat(crate::SOURCE_MAP_MAX_BYTES + 1);
        } else if source == "invalid-source-map" {
            package.source_map = "{}".to_string();
        }
        Ok(package)
    }

    fn execute(
        &self,
        package: ProgramPackage,
        _host: Arc<dyn coral_v8::ProgramHost>,
        _stop: Arc<crate::execution::ExecutionStop>,
    ) -> ExecutionCompletion {
        self.executions.fetch_add(1, Ordering::SeqCst);
        match package.javascript.as_str() {
            "null" => ExecutionCompletion::Succeeded(FinalValue::Present(
                ProgramValue::from_json(&serde_json::Value::Null).expect("Program Value"),
            )),
            _ => ExecutionCompletion::Succeeded(FinalValue::Absent),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InvokingEngine {
    pub(crate) call_path: String,
}

impl InvokingEngine {
    pub(crate) fn new(call_path: &str) -> Self {
        Self {
            call_path: call_path.to_string(),
        }
    }
}

impl ProgramEngine for InvokingEngine {
    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn execute(
        &self,
        _package: ProgramPackage,
        host: Arc<dyn coral_v8::ProgramHost>,
        _stop: Arc<crate::execution::ExecutionStop>,
    ) -> ExecutionCompletion {
        match tokio::runtime::Handle::current()
            .block_on(host.invoke(self.call_path.clone(), "{}".to_string()))
        {
            Ok(result_json) => match ProgramValue::from_encoded_json(result_json) {
                Ok(value) => ExecutionCompletion::Succeeded(FinalValue::Present(value)),
                Err(error) => ExecutionCompletion::Failed(crate::RunFailure::new(
                    RunFailureCode::InvalidProgramValue,
                    error.to_string(),
                )),
            },
            Err(
                coral_v8::OperationHostError::Rejected { .. }
                | coral_v8::OperationHostError::Upstream { .. },
            ) => ExecutionCompletion::Failed(crate::RunFailure::new(
                RunFailureCode::ProgramRejected,
                "Program rejected".to_string(),
            )),
            Err(coral_v8::OperationHostError::Fatal) => ExecutionCompletion::ProgramHostTerminated,
        }
    }
}

#[derive(Clone)]
pub(crate) struct StaleClaimEngine {
    pub(crate) call_path: String,
    pub(crate) started: Arc<AtomicBool>,
    pub(crate) attempted_late_work: Arc<AtomicBool>,
    pub(crate) emission_succeeded: Arc<AtomicBool>,
    pub(crate) invocation_succeeded: Arc<AtomicBool>,
    pub(crate) release: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl StaleClaimEngine {
    pub(crate) fn new(call_path: &str) -> (Self, mpsc::Sender<()>) {
        let (sender, receiver) = mpsc::channel();
        (
            Self {
                call_path: call_path.to_string(),
                started: Arc::new(AtomicBool::new(false)),
                attempted_late_work: Arc::new(AtomicBool::new(false)),
                emission_succeeded: Arc::new(AtomicBool::new(false)),
                invocation_succeeded: Arc::new(AtomicBool::new(false)),
                release: Arc::new(Mutex::new(receiver)),
            },
            sender,
        )
    }
}

impl ProgramEngine for StaleClaimEngine {
    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn execute(
        &self,
        _package: ProgramPackage,
        host: Arc<dyn coral_v8::ProgramHost>,
        _stop: Arc<crate::execution::ExecutionStop>,
    ) -> ExecutionCompletion {
        self.started.store(true, Ordering::SeqCst);
        if self
            .release
            .lock()
            .expect("stale execution release lock")
            .recv()
            .is_err()
        {
            return ExecutionCompletion::ProgramHostTerminated;
        }
        self.emission_succeeded
            .store(host.emit("42").is_ok(), Ordering::SeqCst);
        self.invocation_succeeded.store(
            tokio::runtime::Handle::current()
                .block_on(host.invoke(self.call_path.clone(), "{}".to_string()))
                .is_ok(),
            Ordering::SeqCst,
        );
        self.attempted_late_work.store(true, Ordering::SeqCst);
        ExecutionCompletion::Succeeded(FinalValue::Absent)
    }
}

pub(crate) struct CompletionAttemptingEngine {
    pub(crate) call_path: String,
    pub(crate) finished: AtomicBool,
}

impl CompletionAttemptingEngine {
    pub(crate) fn new(call_path: &str) -> Self {
        Self {
            call_path: call_path.to_string(),
            finished: AtomicBool::new(false),
        }
    }
}

impl ProgramEngine for CompletionAttemptingEngine {
    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn execute(
        &self,
        _package: ProgramPackage,
        host: Arc<dyn coral_v8::ProgramHost>,
        _stop: Arc<crate::execution::ExecutionStop>,
    ) -> ExecutionCompletion {
        let _host_result = tokio::runtime::Handle::current()
            .block_on(host.invoke(self.call_path.clone(), "{}".to_string()));
        self.finished.store(true, Ordering::SeqCst);
        ExecutionCompletion::Succeeded(FinalValue::Absent)
    }
}

#[derive(Clone, Default)]
pub(crate) struct StopAwareEngine {
    pub(crate) started: Arc<AtomicBool>,
    pub(crate) stopped: Arc<AtomicBool>,
    pub(crate) executions: Arc<AtomicUsize>,
}

impl ProgramEngine for StopAwareEngine {
    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn execute(
        &self,
        _package: ProgramPackage,
        _host: Arc<dyn coral_v8::ProgramHost>,
        stop: Arc<ExecutionStop>,
    ) -> ExecutionCompletion {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.started.store(true, Ordering::SeqCst);
        while !stop.is_requested() {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.stopped.store(true, Ordering::SeqCst);
        ExecutionCompletion::ProgramHostTerminated
    }
}

#[derive(Clone)]
pub(crate) struct BlockingEngine {
    pub(crate) started: Arc<AtomicBool>,
    pub(crate) finished: Arc<AtomicBool>,
    pub(crate) executions: Arc<AtomicUsize>,
    pub(crate) completion: Arc<Mutex<mpsc::Receiver<ExecutionCompletion>>>,
}

impl BlockingEngine {
    pub(crate) fn new() -> (Self, mpsc::Sender<ExecutionCompletion>) {
        let (sender, receiver) = mpsc::channel();
        (
            Self {
                started: Arc::new(AtomicBool::new(false)),
                finished: Arc::new(AtomicBool::new(false)),
                executions: Arc::new(AtomicUsize::new(0)),
                completion: Arc::new(Mutex::new(receiver)),
            },
            sender,
        )
    }
}

impl ProgramEngine for BlockingEngine {
    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn execute(
        &self,
        _package: ProgramPackage,
        _host: Arc<dyn coral_v8::ProgramHost>,
        _stop: Arc<crate::execution::ExecutionStop>,
    ) -> ExecutionCompletion {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.started.store(true, Ordering::SeqCst);
        let completion = self
            .completion
            .lock()
            .expect("completion lock")
            .recv()
            .expect("scripted completion");
        self.finished.store(true, Ordering::SeqCst);
        completion
    }
}

#[derive(Clone)]
pub(crate) struct CompileGateEngine {
    pub(crate) compiling: Arc<AtomicBool>,
    pub(crate) executions: Arc<AtomicUsize>,
    pub(crate) release: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl CompileGateEngine {
    pub(crate) fn new() -> (Self, mpsc::Sender<()>) {
        let (sender, receiver) = mpsc::channel();
        (
            Self {
                compiling: Arc::new(AtomicBool::new(false)),
                executions: Arc::new(AtomicUsize::new(0)),
                release: Arc::new(Mutex::new(receiver)),
            },
            sender,
        )
    }
}

impl ProgramEngine for CompileGateEngine {
    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        self.compiling.store(true, Ordering::SeqCst);
        self.release
            .lock()
            .map_err(|error| error.to_string())?
            .recv()
            .map_err(|error| error.to_string())?;
        Ok(ProgramPackage::scripted(source))
    }

    fn execute(
        &self,
        _package: ProgramPackage,
        _host: Arc<dyn coral_v8::ProgramHost>,
        _stop: Arc<crate::execution::ExecutionStop>,
    ) -> ExecutionCompletion {
        self.executions.fetch_add(1, Ordering::SeqCst);
        ExecutionCompletion::Succeeded(FinalValue::Absent)
    }
}

/// Finds `coral.functions.<path>` references by scanning source text, so Saved
/// Function closure rules run without TypeScript compilation time. A linked
/// Program returns how many Saved Functions it linked.
#[derive(Clone, Default)]
pub(crate) struct ReferenceScanningEngine;

impl ReferenceScanningEngine {
    fn references(source: &str) -> Result<Vec<SavedFunctionReference>, String> {
        const PREFIX: &str = "coral.functions.";
        source
            .match_indices(PREFIX)
            .map(|(at, _)| {
                let path = source
                    .get(at + PREFIX.len()..)
                    .unwrap_or_default()
                    .split(|character: char| {
                        !(character.is_ascii_alphanumeric() || matches!(character, '.' | '_'))
                    })
                    .next()
                    .unwrap_or_default();
                test_saved_function_reference(path)
            })
            .collect()
    }
}

impl ProgramEngine for ReferenceScanningEngine {
    fn program_saved_function_references(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, String> {
        Self::references(source)
    }

    fn saved_function_dependency_references(
        &self,
        _deadline: std::time::Instant,
        _path: &SavedFunctionPath,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, String> {
        Self::references(source)
    }

    fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(source))
    }

    fn compile_with_saved_functions(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
        linked: &[SavedFunctionSelection],
    ) -> Result<ProgramPackage, String> {
        Ok(ProgramPackage::scripted(&linked.len().to_string()))
    }

    fn execute(
        &self,
        package: ProgramPackage,
        _host: Arc<dyn coral_v8::ProgramHost>,
        _stop: Arc<ExecutionStop>,
    ) -> ExecutionCompletion {
        let linked = serde_json::from_str(&package.javascript).unwrap_or_default();
        ExecutionCompletion::Succeeded(FinalValue::Present(
            ProgramValue::from_json(&linked).expect("linked count"),
        ))
    }
}

pub(crate) fn test_saved_function_reference(path: &str) -> Result<SavedFunctionReference, String> {
    Ok(SavedFunctionReference {
        path: SavedFunctionPath::parse(path).map_err(|error| error.to_string())?,
        line: 1,
        column: 1,
    })
}
