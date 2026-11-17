//! Coral's side of a Run stream, behind a seam: the link authenticates the
//! Run Executor and hands each Run stream to what this Coral Instance serves
//! them with, which owns the Run's rows (ADR 0027).

use std::pin::Pin;

use tokio::sync::mpsc;
use tonic::Status;

use crate::wire::v1::{RunRequest, RunResponse};

/// One Run stream a Run Executor opened, as Coral serves it.
pub struct ServedRunStream {
    /// The Run Executor's messages, in order; the stream ends when the Run
    /// Executor's side does, or with a message the link cannot decode.
    pub requests: Pin<Box<dyn tokio_stream::Stream<Item = RunRequest> + Send>>,
    /// Coral's messages. A [`Closing`] closes the stream with its status.
    pub responses: mpsc::UnboundedSender<Result<RunResponse, Closing>>,
}

/// Why Coral closes a Run stream without `Ended`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Closing {
    /// The Run Executor broke the protocol.
    InvalidArgument(&'static str),
    /// The protocol levels are more than one apart.
    FailedPrecondition(&'static str),
    /// Coral cannot serve the stream now; the Run Executor opens it again.
    Unavailable(&'static str),
}

impl From<Closing> for Status {
    fn from(closing: Closing) -> Self {
        match closing {
            Closing::InvalidArgument(message) => Self::invalid_argument(message),
            Closing::FailedPrecondition(message) => Self::failed_precondition(message),
            Closing::Unavailable(message) => Self::unavailable(message),
        }
    }
}

/// What serves this Coral Instance's Run streams.
#[tonic::async_trait]
pub trait RunStreamServer: Send + Sync + 'static {
    /// Serves `stream` until it ends.
    async fn serve(&self, stream: ServedRunStream);
}
