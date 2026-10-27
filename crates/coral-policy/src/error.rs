/// Tool Policy Module failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Batches contain 1–100 distinct exact targets in one Workspace.
    #[error("invalid Tool policy batch")]
    InvalidBatch,
    /// The Provider or current Tool is absent from the exact Workspace target.
    #[error("Tool was not found")]
    ToolNotFound,
    /// No Tool of the Provider's current catalog has these Call Paths.
    #[error("no current Tool has the Call Paths {}", .0.join(", "))]
    ToolPathsNotFound(Vec<String>),
    /// A persisted row does not decode to the closed domain model.
    #[error("database contains corrupt Tool policy data: {0}")]
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
    #[error("database contains invalid Tool-policy lifecycle-event fields: {0}")]
    EventFields(#[from] serde_json::Error),
}
