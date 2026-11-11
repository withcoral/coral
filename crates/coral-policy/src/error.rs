/// Tool Policy Module failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Batches contain 1–100 distinct exact targets in one Workspace.
    #[error("invalid Tool policy batch")]
    InvalidBatch,
    /// The Provider or current Tool is absent from the exact Workspace target.
    #[error("Tool was not found")]
    ToolNotFound,
    /// Tool Call Paths a change refused: each that names no current Tool
    /// the actor can set, and each that names a Built-in Tool, whose policy
    /// a Program cannot change.
    #[error("Tool Call Paths refused: {}", refused_paths(.missing, .builtin))]
    RefusedToolPaths {
        /// Each path that names no Tool the actor can set.
        missing: Vec<String>,
        /// Each path that names a Built-in Tool.
        builtin: Vec<String>,
    },
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

/// Names each kind of refused path that has any, such as
/// `not found coral.providers.github.x; Built-in coral.search`.
fn refused_paths(missing: &[String], builtin: &[String]) -> String {
    [("not found", missing), ("Built-in", builtin)]
        .into_iter()
        .filter(|(_kind, paths)| !paths.is_empty())
        .map(|(kind, paths)| format!("{kind} {}", paths.join(", ")))
        .collect::<Vec<_>>()
        .join("; ")
}
