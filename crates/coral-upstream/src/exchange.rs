//! The measure of one dispatch to an Upstream Service.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use coral_telemetry::metric::{self, ProviderProtocol, UpstreamOutcome};

use crate::Error;

/// One dispatch to an Upstream Service over its protocol, measured from its
/// start until [`Self::end`]: the body bytes it sent and received, and how
/// it ended. A dispatch dropped before its end, such as one a Run stop
/// cancels, ended without a complete response.
#[derive(Debug)]
pub(crate) struct ExchangeMeasure {
    protocol: ProviderProtocol,
    started: Instant,
    bytes: Arc<ExchangeBytes>,
    outcome: UpstreamOutcome,
}

/// The body bytes one dispatch sent and received, which its transport adds.
#[derive(Debug, Default)]
pub(crate) struct ExchangeBytes {
    sent: AtomicUsize,
    received: AtomicUsize,
}

impl ExchangeMeasure {
    pub(crate) fn start(protocol: ProviderProtocol) -> Self {
        Self {
            protocol,
            started: Instant::now(),
            bytes: Arc::default(),
            outcome: UpstreamOutcome::Failed,
        }
    }

    pub(crate) const fn bytes(&self) -> &Arc<ExchangeBytes> {
        &self.bytes
    }

    /// Records how the dispatch ended: with a complete response, a JSON-RPC
    /// error among them, one too large, or none.
    pub(crate) fn end(mut self, ended: Result<(), &Error>) {
        self.outcome = match ended {
            Ok(()) | Err(Error::McpRejected(_)) => UpstreamOutcome::Response,
            Err(Error::ResultTooLarge) => UpstreamOutcome::TooLarge,
            Err(_) => UpstreamOutcome::Failed,
        };
    }
}

impl Drop for ExchangeMeasure {
    /// A direction that carried no body records no size.
    fn drop(&mut self) {
        let size =
            |bytes: &AtomicUsize| Some(bytes.load(Ordering::Relaxed)).filter(|bytes| *bytes > 0);
        metric::upstream_exchanged(
            self.protocol,
            self.outcome,
            self.started.elapsed(),
            size(&self.bytes.sent),
            size(&self.bytes.received),
        );
    }
}

impl ExchangeBytes {
    /// Adds `bytes` of request body the dispatch sent.
    pub(crate) fn sent(&self, bytes: usize) {
        self.sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Adds `bytes` of response body the dispatch received.
    pub(crate) fn received(&self, bytes: usize) {
        self.received.fetch_add(bytes, Ordering::Relaxed);
    }
}
