//! Coral's side of a Run stream, behind a seam: the link authenticates the
//! Run Executor and hands each Run stream to what this Coral Instance serves
//! them with, which owns the Run's rows.

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

/// What a Coral Instance in its Shutdown Drain answers a new stream with.
pub const SHUTTING_DOWN: &str = "this Coral Instance is shutting down";

/// Why Coral closes a Run stream without `Ended`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Closing {
    /// The Run Executor broke the protocol.
    InvalidArgument(&'static str),
    /// The protocol levels are more than one apart.
    FailedPrecondition(&'static str),
    /// Coral cannot serve the stream now; the Run Executor opens it again.
    Unavailable(&'static str),
    /// Coral moves the stream: it ends with an OK status and no message, and
    /// the Run Executor reopens it at once on its current connection.
    Moved,
}

impl Closing {
    /// The status the stream ends with; none for a move, which ends it OK.
    #[must_use]
    pub fn status(self) -> Option<Status> {
        match self {
            Self::InvalidArgument(message) => Some(Status::invalid_argument(message)),
            Self::FailedPrecondition(message) => Some(Status::failed_precondition(message)),
            Self::Unavailable(message) => Some(Status::unavailable(message)),
            Self::Moved => None,
        }
    }
}

/// What serves this Coral Instance's Run streams.
#[tonic::async_trait]
pub trait RunStreamServer: Send + Sync + 'static {
    /// Serves `stream` until it ends.
    async fn serve(&self, stream: ServedRunStream);
}
