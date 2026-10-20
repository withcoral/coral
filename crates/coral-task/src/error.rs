/// Task Module failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Task is absent or concealed from this Principal.
    #[error("Task was not found")]
    TaskNotFound,
    /// The first terminal goal outcome rejects new attempts.
    #[error("Task is closed to new attempts")]
    TaskClosed,
    /// Historical detail is absent or unavailable.
    #[error("Task Interaction was not found")]
    InteractionNotFound,
    /// A complete snapshot cannot fit its finite bound.
    #[error("complete discovery snapshot exceeds its limit")]
    SnapshotTooLarge,
    /// Search selected a discovery failure.
    #[error(transparent)]
    Search(#[from] coral_search::Error),
    /// Functions selected an authoring failure.
    #[error(transparent)]
    Functions(#[from] coral_functions::Error),
    /// Run selected an admission or observation failure.
    #[error(transparent)]
    Run(#[from] coral_run::Error),
    /// The host cannot obtain durable time evidence.
    #[error("Task clock is unavailable")]
    Clock,
    /// Coral could not issue an unused Task identifier.
    #[error("Task identifier could not be issued: {0}")]
    IdentifierIssuance(String),
    /// The Workspace is absent or inaccessible to this Principal.
    #[error("Workspace was not found")]
    WorkspaceNotFound,
    /// The authenticated Principal cannot perform this action.
    #[error("permission denied: {0}")]
    PermissionDenied(&'static str),
    /// Workspace authority or persistence rejected the operation.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// A persisted row does not decode to the closed domain model.
    #[error("database contains corrupt Task data: {0}")]
    CorruptData(String),
    /// The persistence Adapter rejected the operation.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}
