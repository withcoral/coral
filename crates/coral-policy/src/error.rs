/// Operation Policy Module failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Batches contain 1–100 distinct exact targets in one Workspace.
    #[error("invalid Operation policy batch")]
    InvalidBatch,
    /// The Provider or current Operation is absent from the exact Workspace target.
    #[error("Operation was not found")]
    OperationNotFound,
    /// No Operation of the Provider's current catalog has these Call Paths.
    #[error("no current Operation has the Call Paths {}", .0.join(", "))]
    OperationPathsNotFound(Vec<String>),
    /// A persisted row does not decode to the closed domain model.
    #[error("database contains corrupt Operation policy data: {0}")]
    CorruptData(String),
    /// Provider or Catalog evidence could not be resolved.
    #[error(transparent)]
    Provider(#[from] coral_provider::Error),
    /// Workspace authority or lifecycle rejected the request.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// The persistence Adapter rejected the operation.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
    /// Encoding or decoding safe lifecycle-event field paths failed.
    #[error("database contains invalid Operation-policy lifecycle-event fields: {0}")]
    EventFields(#[from] serde_json::Error),
}
