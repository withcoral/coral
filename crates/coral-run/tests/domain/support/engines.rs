//! Scripted Program engines that gate or observe execution.
use crate::tests::*;
use coral_tools::{AuthoredToolPath, LinkedToolReference, LinkedToolSelection};

/// The real Program engine, for a Program that runs on the test's Run
/// Executor.
pub(crate) fn v8_engine() -> Arc<dyn ProgramEngine> {
    Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap())
}

/// A Program that calls the fixture's weather Tool once and returns its
/// result.
pub(crate) const INVOKE_WEATHER: &str = "return await coral.providers.weather.getForecast({});";

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

#[async_trait::async_trait]
impl ProgramEngine for ClosureReadGateEngine {
    async fn analyze(
        &self,
        _deadline: std::time::Instant,
        sources: &[AdmissionSource],
    ) -> Result<Vec<AdmissionReferences>, CompileFailure> {
        let root_v1 = |source: &AdmissionSource| matches!(source, AdmissionSource::Linked { source, .. } if source.contains("root-v1"));
        if sources.iter().any(root_v1) {
            self.reading_root.store(true, Ordering::SeqCst);
            wait_for_release(&self.release).await?;
        }
        analyze_each(sources, |source| match source {
            AdmissionSource::Program(_) => {
                test_private_tool_reference("closure.root").map(|reference| vec![reference])
            }
            AdmissionSource::Linked { source, .. } if source.contains("root-v1") => {
                test_private_tool_reference("closure.dependency").map(|reference| vec![reference])
            }
            AdmissionSource::Linked { .. } => Ok(Vec::new()),
        })
    }

    async fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
        _linked: &[LinkedToolSelection],
        _builtin_tools: &coral_v8::BuiltinTools,
        _callable: Option<&[String]>,
    ) -> Result<ProgramPackage, PackageFailure> {
        Ok(ProgramPackage::scripted(source))
    }
}

#[derive(Clone)]
pub(crate) struct ImmediateEngine;

#[async_trait::async_trait]
impl ProgramEngine for ImmediateEngine {
    async fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
        _linked: &[LinkedToolSelection],
        _builtin_tools: &coral_v8::BuiltinTools,
        _callable: Option<&[String]>,
    ) -> Result<ProgramPackage, PackageFailure> {
        if source == "compile-error" {
            return Err(
                CompileFailure::Rejected("scripted compilation failure".to_string()).into(),
            );
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
}

/// Fails the compile stage a source names as `<stage>:<failure>`.
pub(crate) struct FailingCompileEngine;

impl FailingCompileEngine {
    fn fail_at(stage: &str, source: &str) -> Result<(), CompileFailure> {
        let Some(failure) = source
            .strip_prefix(stage)
            .and_then(|rest| rest.strip_prefix(':'))
        else {
            return Ok(());
        };
        Err(match failure {
            "busy" => CompileFailure::Busy,
            "timed-out" => CompileFailure::TimedOut,
            "stopped" => CompileFailure::Stopped,
            "no-compiler" => CompileFailure::NoCompiler,
            "failed" => CompileFailure::Failed("engine"),
            _ => CompileFailure::Rejected("scripted rejection".to_string()),
        })
    }
}

#[async_trait::async_trait]
impl ProgramEngine for FailingCompileEngine {
    async fn analyze(
        &self,
        _deadline: std::time::Instant,
        sources: &[AdmissionSource],
    ) -> Result<Vec<AdmissionReferences>, CompileFailure> {
        analyze_each(sources, |source| {
            if let AdmissionSource::Program(source) = source {
                Self::fail_at("analysis", source)?;
            }
            Ok(Vec::new())
        })
    }

    async fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
        _linked: &[LinkedToolSelection],
        _builtin_tools: &coral_v8::BuiltinTools,
        _callable: Option<&[String]>,
    ) -> Result<ProgramPackage, PackageFailure> {
        Self::fail_at("compile", source)?;
        Ok(ProgramPackage::scripted(source))
    }
}

/// Waits until `release` takes a message. The wait blocks, so it runs on a
/// blocking worker, as a compiler's own work does.
async fn wait_for_release(release: &Arc<Mutex<mpsc::Receiver<()>>>) -> Result<(), CompileFailure> {
    let release = Arc::clone(release);
    tokio::task::spawn_blocking(move || {
        release
            .lock()
            .map_err(|error| error.to_string())?
            .recv()
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|_join_error| CompileFailure::Stopped)?
    .map_err(CompileFailure::from)
}

#[derive(Clone)]
pub(crate) struct CompileGateEngine {
    pub(crate) compiling: Arc<AtomicBool>,
    pub(crate) release: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl CompileGateEngine {
    pub(crate) fn new() -> (Self, mpsc::Sender<()>) {
        let (sender, receiver) = mpsc::channel();
        (
            Self {
                compiling: Arc::new(AtomicBool::new(false)),
                release: Arc::new(Mutex::new(receiver)),
            },
            sender,
        )
    }
}

#[async_trait::async_trait]
impl ProgramEngine for CompileGateEngine {
    async fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
        _linked: &[LinkedToolSelection],
        _builtin_tools: &coral_v8::BuiltinTools,
        _callable: Option<&[String]>,
    ) -> Result<ProgramPackage, PackageFailure> {
        self.compiling.store(true, Ordering::SeqCst);
        wait_for_release(&self.release).await?;
        Ok(ProgramPackage::scripted(source))
    }
}

/// Finds `coral.private.<path>` references by scanning source text, so Private
/// Tool closure rules run without TypeScript compilation time. A linked
/// Program returns how many Private Tools it linked.
#[derive(Clone, Default)]
pub(crate) struct ReferenceScanningEngine;

impl ReferenceScanningEngine {
    fn references(source: &str) -> Result<AdmissionReferences, CompileFailure> {
        const PREFIX: &str = "coral.private.";
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
                test_private_tool_reference(path)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(AdmissionReferences::linking)
    }
}

#[async_trait::async_trait]
impl ProgramEngine for ReferenceScanningEngine {
    async fn analyze(
        &self,
        _deadline: std::time::Instant,
        sources: &[AdmissionSource],
    ) -> Result<Vec<AdmissionReferences>, CompileFailure> {
        sources
            .iter()
            .map(|source| match source {
                AdmissionSource::Program(source) | AdmissionSource::Linked { source, .. } => {
                    Self::references(source)
                }
            })
            .collect()
    }

    async fn compile(
        &self,
        _deadline: std::time::Instant,
        source: &str,
        linked: &[LinkedToolSelection],
        _builtin_tools: &coral_v8::BuiltinTools,
        _callable: Option<&[String]>,
    ) -> Result<ProgramPackage, PackageFailure> {
        Ok(if linked.is_empty() {
            ProgramPackage::scripted(source)
        } else {
            ProgramPackage::scripted(&linked.len().to_string())
        })
    }
}

/// Each source's linked Tools from `references`, as an engine that finds no
/// Tool Call Paths names them.
pub(crate) fn analyze_each(
    sources: &[AdmissionSource],
    mut references: impl FnMut(&AdmissionSource) -> Result<Vec<LinkedToolReference>, CompileFailure>,
) -> Result<Vec<AdmissionReferences>, CompileFailure> {
    sources
        .iter()
        .map(|source| Ok(AdmissionReferences::linking(references(source)?)))
        .collect()
}

pub(crate) fn test_private_tool_reference(
    path: &str,
) -> Result<LinkedToolReference, CompileFailure> {
    let path = PrivateToolPath::parse(path).map_err(|error| error.to_string())?;
    Ok(LinkedToolReference::new(
        AuthoredToolPath::Private(path),
        1,
        1,
    ))
}
