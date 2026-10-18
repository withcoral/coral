use thiserror::Error;

/// Agent Module failures. Messages are safe to show the caller unless noted.
#[derive(Debug, Error)]
pub enum Error {
    /// The caller sent invalid input.
    #[error("{0}")]
    InvalidInput(String),
    /// The turn does not exist or belongs to someone else.
    #[error("Agent Turn was not found")]
    TurnNotFound,
    /// The Task's thread already has a turn running.
    #[error("A turn is already running in this Task; wait for it or cancel it")]
    TurnInProgress,
    /// The Task rejected the turn.
    #[error(transparent)]
    Task(#[from] coral_task::Error),
    /// Workspace authority rejected the caller.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// Persistence failed. Not safe to show.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
    /// Stored state is inconsistent. Not safe to show.
    #[error("Agent state is corrupt: {0}")]
    CorruptData(String),
    /// The system clock is unusable.
    #[error("system clock is unavailable")]
    Clock,
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(coral_db::Error::from(error))
    }
}
