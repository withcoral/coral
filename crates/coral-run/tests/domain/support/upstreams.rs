//! Scripted upstream senders.
use crate::tests::*;

pub(crate) struct ScriptedUpstream {
    pub(crate) sends: AtomicUsize,
    pub(crate) outcome: HttpOutcome,
}

impl ScriptedUpstream {
    pub(crate) const fn json(status: u16, data: serde_json::Value) -> Self {
        Self {
            sends: AtomicUsize::new(0),
            outcome: HttpOutcome::new(status, data),
        }
    }
}

#[async_trait]
impl UpstreamSender for ScriptedUpstream {
    async fn send(
        &self,
        _dispatch: ReadyDispatch,
        _stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, crate::invocation::UpstreamSendError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(UpstreamOutcome::Http(self.outcome.clone()))
    }
}

pub(crate) struct DefectiveUpstream {
    pub(crate) panic: bool,
}

#[async_trait]
impl UpstreamSender for DefectiveUpstream {
    async fn send(
        &self,
        _dispatch: ReadyDispatch,
        _stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, crate::invocation::UpstreamSendError> {
        assert!(!self.panic, "private upstream defect canary");
        Err(crate::invocation::UpstreamSendError::Fatal(
            coral_upstream::Error::InvalidLimit,
        ))
    }
}

pub(crate) struct FailingUpstream {
    pub(crate) sends: AtomicUsize,
    failure: fn() -> crate::invocation::UpstreamSendError,
}

impl FailingUpstream {
    pub(crate) const fn new(failure: fn() -> crate::invocation::UpstreamSendError) -> Self {
        Self {
            sends: AtomicUsize::new(0),
            failure,
        }
    }
}

impl Default for FailingUpstream {
    fn default() -> Self {
        Self::new(|| crate::invocation::UpstreamSendError::Incomplete)
    }
}

#[async_trait]
impl UpstreamSender for FailingUpstream {
    async fn send(
        &self,
        _dispatch: ReadyDispatch,
        _stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, crate::invocation::UpstreamSendError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Err((self.failure)())
    }
}

pub(crate) struct BlockingUpstream {
    pub(crate) sends: AtomicUsize,
    pub(crate) entered: AtomicBool,
    pub(crate) release: AtomicBool,
    pub(crate) exited: AtomicBool,
    pub(crate) outcome: HttpOutcome,
    pub(crate) result_too_large: bool,
}

impl BlockingUpstream {
    pub(crate) const fn new(outcome: HttpOutcome) -> Self {
        Self {
            sends: AtomicUsize::new(0),
            entered: AtomicBool::new(false),
            release: AtomicBool::new(false),
            exited: AtomicBool::new(false),
            outcome,
            result_too_large: false,
        }
    }
}

#[async_trait]
impl UpstreamSender for BlockingUpstream {
    async fn send(
        &self,
        _dispatch: ReadyDispatch,
        _stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, crate::invocation::UpstreamSendError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.entered.store(true, Ordering::SeqCst);
        while !self.release.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        self.exited.store(true, Ordering::SeqCst);
        if self.result_too_large {
            Err(crate::invocation::UpstreamSendError::TooLarge)
        } else {
            Ok(UpstreamOutcome::Http(self.outcome.clone()))
        }
    }
}

pub(crate) struct RejectingUpstream;

#[async_trait]
impl UpstreamSender for RejectingUpstream {
    async fn send(
        &self,
        _dispatch: ReadyDispatch,
        _stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, crate::invocation::UpstreamSendError> {
        Err(crate::invocation::UpstreamSendError::Incomplete)
    }
}

pub(crate) fn scripted_http_result(data: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "status": 200,
        "data": data,
        "metadata": {
            "kind": "http",
            "headers": [],
            "mediaType": null,
            "charset": null,
            "finalUrl": "",
            "redirects": [],
            "decodeDiagnostics": []
        }
    })
}

/// Holds the send until the Run's stop is requested, then reports it
/// canceled, as the production sender does for a dispatch in flight.
pub(crate) struct StopAwareUpstream {
    pub(crate) entered: AtomicBool,
    pub(crate) exited: AtomicBool,
}

impl StopAwareUpstream {
    pub(crate) const fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            exited: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl UpstreamSender for StopAwareUpstream {
    async fn send(
        &self,
        _dispatch: ReadyDispatch,
        stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, crate::invocation::UpstreamSendError> {
        self.entered.store(true, Ordering::SeqCst);
        stop.requested().await;
        self.exited.store(true, Ordering::SeqCst);
        Err(crate::invocation::UpstreamSendError::Canceled)
    }
}

/// Holds every send until the test lets sends go, one at a time in send
/// order, and reports a held send canceled once the Run's stop is requested,
/// as the production sender does for a dispatch in flight.
pub(crate) struct SequencedUpstream {
    pub(crate) sends: AtomicUsize,
    /// How many sends may go on, in the order they were sent.
    permits: watch::Sender<usize>,
    outcome: HttpOutcome,
}

impl SequencedUpstream {
    pub(crate) fn new(outcome: HttpOutcome) -> Arc<Self> {
        Arc::new(Self {
            sends: AtomicUsize::new(0),
            permits: watch::channel(0).0,
            outcome,
        })
    }

    /// Lets the earliest send still held go on.
    pub(crate) fn release_one(&self) {
        self.permits.send_modify(|permits| *permits += 1);
    }

    /// Lets every send, held or later, go on.
    pub(crate) fn release_all(&self) {
        self.permits.send_replace(usize::MAX);
    }

    /// Waits until `count` sends have reached the upstream.
    pub(crate) async fn until_sent(&self, count: usize) {
        wait_until_async_within(Duration::from_secs(10), async || {
            self.sends.load(Ordering::SeqCst) >= count
        })
        .await;
    }
}

#[async_trait]
impl UpstreamSender for SequencedUpstream {
    async fn send(
        &self,
        _dispatch: ReadyDispatch,
        stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, crate::invocation::UpstreamSendError> {
        let ordinal = self.sends.fetch_add(1, Ordering::SeqCst) + 1;
        let mut permits = self.permits.subscribe();
        tokio::select! {
            // A dropped sender never happens: the upstream outlives the test.
            _permitted = permits.wait_for(|permits| *permits >= ordinal) => {
                Ok(UpstreamOutcome::Http(self.outcome.clone()))
            }
            () = stop.requested() => Err(crate::invocation::UpstreamSendError::Canceled),
        }
    }
}
