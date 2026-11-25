//! A test of a model before a Workspace depends on it.

use coral_credentials::Secret;

use crate::settings::ModelSettings;

/// Sends a model one fixed request, with no Workspace data. The Sub-agent's
/// crate implements it, and has the one model client.
#[async_trait::async_trait]
pub trait ModelTester: Send + Sync {
    /// Asks the model for one reply, once.
    ///
    /// # Errors
    ///
    /// Returns what kind of failure it met, and never the server's text.
    async fn test(&self, settings: &ModelSettings, key: Option<Secret>) -> Result<(), TestFailure>;
}

/// Why a model did not answer a test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestFailure {
    /// The server could not be reached, or did not answer in time.
    Unreachable,
    /// The server answered with this HTTP status.
    Refused(u16),
    /// The operator's destination policy forbids the server.
    Forbidden,
    /// The server's answer does not read as a reply of the protocol.
    Unreadable,
}

/// The API key to test a model with.
#[derive(Debug)]
pub enum TestKey {
    /// The stored one, where the base URL is the one it is stored for.
    Stored,
    /// This one.
    Entered(Secret),
    /// None.
    Absent,
}
