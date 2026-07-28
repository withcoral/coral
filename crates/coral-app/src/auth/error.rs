//! Errors surfaced by Coral's authorization server and its configuration.

use std::io;
use std::net::SocketAddr;

/// Failure preparing, starting, or stopping Coral's authorization server.
///
/// Callers distinguish a listener failure ([`Self::Bind`]) from an invalid
/// configuration ([`Self::Config`]) by matching, rather than by inspecting
/// message text.
#[derive(Debug, thiserror::Error)]
pub enum AuthServerError {
    /// An `[auth]` field or cross-section relationship is invalid.
    ///
    /// The message names the offending section or setting and is safe to show
    /// an operator; it never contains secret values.
    #[error("{0}")]
    Config(String),
    /// The resolved session-token issuer disagrees with the settings it was
    /// resolved from.
    #[error("resolved session-token issuer does not match authorization-server settings")]
    SessionIssuerMismatch,
    /// The TCP listener could not bind.
    #[error("failed to bind authorization server to {address}")]
    Bind {
        /// Requested bind address.
        address: SocketAddr,
        /// Listener error.
        #[source]
        source: io::Error,
    },
    /// The bound listener's local address could not be read.
    #[error("failed to read authorization server address")]
    LocalAddr(#[source] io::Error),
    /// The HTTP server exited with an I/O error.
    #[error("authorization server failed")]
    Server(#[source] io::Error),
    /// The HTTP server task could not be joined.
    #[error("authorization server task failed")]
    Join(#[source] tokio::task::JoinError),
    /// Graceful shutdown exceeded its deadline and the task was aborted.
    #[error("authorization server shutdown timed out; task aborted")]
    ShutdownTimedOut,
}
