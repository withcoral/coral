use async_trait::async_trait;
use coral_catalog::{ProviderSetupInput, ToolPathAssignment};

use crate::configuration::preparation::SourceAnalysis;

pub(crate) use coral_catalog::MaterializationOutcome;

#[derive(Debug)]
pub(crate) struct OperationalMaterializationError {
    message: String,
}

impl OperationalMaterializationError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub(super) fn message(&self) -> &str {
        &self.message
    }
}

#[async_trait]
pub(crate) trait Materializer: Send + Sync {
    async fn materialize(
        &self,
        setup_input: &ProviderSetupInput,
        previous_paths: &[ToolPathAssignment],
    ) -> Result<MaterializationOutcome, OperationalMaterializationError>;

    /// Analyzes an OpenAPI Setup source off the async runtime.
    async fn analyze(
        &self,
        setup_input: ProviderSetupInput,
    ) -> Result<Option<SourceAnalysis>, OperationalMaterializationError> {
        blocking(move || SourceAnalysis::of(setup_input)).await
    }
}

// Bound CPU work across all Provider Modules in this process. The permit lives
// inside the blocking closure: cancellation cannot release capacity while work runs.
static CPU_WORK: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, OperationalMaterializationError> {
    let permit = CPU_WORK.acquire().await.map_err(|_closed| {
        OperationalMaterializationError::new("preparation executor unavailable")
    })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|_panic| OperationalMaterializationError::new("preparation worker failed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn canceled_callers_do_not_release_running_cpu_work() {
        let (entered, mut entries) = tokio::sync::mpsc::unbounded_channel();
        let mut tasks = Vec::new();
        let mut releases = Vec::new();
        for _ in 0..2 {
            let (release, wait) = std::sync::mpsc::channel::<()>();
            releases.push(release);
            let entered = entered.clone();
            tasks.push(tokio::spawn(async move {
                blocking(move || {
                    entered.send(()).unwrap();
                    // A failed assertion cannot leave the runtime waiting on a thread forever.
                    let _released = wait.recv_timeout(Duration::from_secs(10));
                })
                .await
            }));
        }
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(5), entries.recv())
                .await
                .unwrap()
                .unwrap();
        }
        let canceled = tasks.pop().unwrap();
        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());
        assert_eq!(
            CPU_WORK.available_permits(),
            0,
            "the running closures, not their callers, own both permits"
        );
        for release in releases {
            release.send(()).unwrap();
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(blocking(|| 42).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn worker_panics_fail_without_leaking_capacity() {
        assert!(blocking(|| panic!("worker failure fixture")).await.is_err());
        assert_eq!(blocking(|| 42).await.unwrap(), 42);
    }
}

/// One configurable Materializer for tests. It counts calls and materializes
/// the real catalog, optionally after failing its first call, holding its
/// first call until released, or meeting other callers at a barrier.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestMaterializer {
    calls: std::sync::atomic::AtomicUsize,
    analyses: std::sync::atomic::AtomicUsize,
    fail_first: bool,
    gate: Option<(tokio::sync::Notify, tokio::sync::Notify)>,
    barrier: Option<std::sync::Arc<tokio::sync::Barrier>>,
}

#[cfg(test)]
impl TestMaterializer {
    /// Counts calls and materializes each one.
    pub(crate) fn counting() -> std::sync::Arc<Self> {
        std::sync::Arc::default()
    }

    /// Fails the first call as an operational failure.
    pub(crate) fn failing_once() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            fail_first: true,
            ..Self::default()
        })
    }

    /// Holds the first call until [`Self::release`].
    pub(crate) fn gated() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            gate: Some(Default::default()),
            ..Self::default()
        })
    }

    /// Makes every call wait at `barrier` first.
    pub(crate) fn meeting_at(
        barrier: std::sync::Arc<tokio::sync::Barrier>,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            barrier: Some(barrier),
            ..Self::default()
        })
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Source analyses run through this Materializer.
    pub(crate) fn analyses(&self) -> usize {
        self.analyses.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Waits until the gated first call is held.
    pub(crate) async fn held(&self) {
        self.gate
            .as_ref()
            .expect("a gated Materializer")
            .0
            .notified()
            .await;
    }

    /// Lets the gated first call continue.
    pub(crate) fn release(&self) {
        self.gate
            .as_ref()
            .expect("a gated Materializer")
            .1
            .notify_one();
    }
}

#[cfg(test)]
#[async_trait]
impl Materializer for TestMaterializer {
    async fn materialize(
        &self,
        setup_input: &ProviderSetupInput,
        previous_paths: &[ToolPathAssignment],
    ) -> Result<MaterializationOutcome, OperationalMaterializationError> {
        let first = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        if first && self.fail_first {
            return Err(OperationalMaterializationError::new(
                "materializer was temporarily unavailable",
            ));
        }
        if first && let Some((held, release)) = &self.gate {
            held.notify_one();
            release.notified().await;
        }
        CatalogMaterializer(
            coral_outbound::DestinationPolicy::default()
                .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        )
        .materialize(setup_input, previous_paths)
        .await
    }

    async fn analyze(
        &self,
        setup_input: ProviderSetupInput,
    ) -> Result<Option<SourceAnalysis>, OperationalMaterializationError> {
        self.analyses
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        CatalogMaterializer::default().analyze(setup_input).await
    }
}

#[derive(Default)]
pub(crate) struct CatalogMaterializer(pub(crate) coral_outbound::DestinationPolicy);

#[async_trait]
impl Materializer for CatalogMaterializer {
    async fn materialize(
        &self,
        setup_input: &ProviderSetupInput,
        previous_paths: &[ToolPathAssignment],
    ) -> Result<MaterializationOutcome, OperationalMaterializationError> {
        let policy = self.0;
        let input = setup_input.clone();
        let paths = previous_paths.to_vec();
        blocking(move || coral_catalog::materialize(policy, &input, &paths)).await
    }
}
