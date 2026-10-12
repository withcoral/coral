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

#[derive(Default)]
pub(crate) struct FailingUpstream {
    pub(crate) sends: AtomicUsize,
}

#[async_trait]
impl UpstreamSender for FailingUpstream {
    async fn send(
        &self,
        _dispatch: ReadyDispatch,
        _stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, crate::invocation::UpstreamSendError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Err(crate::invocation::UpstreamSendError::Incomplete)
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
        "data": data,
        "metadata": {
            "kind": "http",
            "status": 200,
            "headers": [],
            "mediaType": null,
            "charset": null,
            "finalUrl": "",
            "redirects": [],
            "decodeDiagnostics": []
        }
    })
}
