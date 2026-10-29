//! The measure of one dispatch to an Upstream Service.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use coral_telemetry::metric::{self, ProviderProtocol, UpstreamOutcome};

use crate::Error;

/// One dispatch to an Upstream Service over its protocol, measured from its
/// start until [`Self::end`]: the body bytes it sent and received, and how
/// it ended.
#[derive(Debug)]
pub(crate) struct ExchangeMeasure {
    protocol: ProviderProtocol,
    started: Instant,
    sent: AtomicUsize,
    received: AtomicUsize,
}

impl ExchangeMeasure {
    pub(crate) fn start(protocol: ProviderProtocol) -> Self {
        Self {
            protocol,
            started: Instant::now(),
            sent: AtomicUsize::new(0),
            received: AtomicUsize::new(0),
        }
    }

    /// Adds `bytes` of request body the dispatch sent.
    pub(crate) fn sent(&self, bytes: usize) {
        self.sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Adds `bytes` of response body the dispatch received.
    pub(crate) fn received(&self, bytes: usize) {
        self.received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records how the dispatch ended: with a complete response, one too
    /// large, or none. A direction that carried no body records no size.
    pub(crate) fn end(&self, ended: Result<(), &Error>) {
        let outcome = match ended {
            Ok(()) => UpstreamOutcome::Response,
            Err(Error::ResultTooLarge) => UpstreamOutcome::TooLarge,
            Err(_) => UpstreamOutcome::Failed,
        };
        let size =
            |bytes: &AtomicUsize| Some(bytes.load(Ordering::Relaxed)).filter(|bytes| *bytes > 0);
        metric::upstream_exchanged(
            self.protocol,
            outcome,
            self.started.elapsed(),
            size(&self.sent),
            size(&self.received),
        );
    }
}
