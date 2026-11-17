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
    /// Tools selected an authoring failure.
    #[error(transparent)]
    Tools(#[from] coral_tools::Error),
    /// Run selected an admission or observation failure.
    #[error(transparent)]
    Run(#[from] coral_run::Error),
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

impl Error {
    /// The database error behind this one, directly or through the module
    /// error that wraps it.
    #[must_use]
    pub const fn database_error(&self) -> Option<&coral_db::Error> {
        match self {
            Self::Database(error) => Some(error),
            Self::Workspace(error) => error.database_error(),
            Self::Search(error) => error.database_error(),
            Self::Tools(error) => error.database_error(),
            Self::Run(error) => error.database_error(),
            // Name every variant so a new one that holds a database error
            // is found here.
            Self::InvalidInput(_)
            | Self::TaskNotFound
            | Self::TaskClosed
            | Self::InteractionNotFound
            | Self::SnapshotTooLarge
            | Self::IdentifierIssuance(_)
            | Self::WorkspaceNotFound
            | Self::PermissionDenied(_)
            | Self::CorruptData(_) => None,
        }
    }

    /// The database error behind this one, by value, or this error back
    /// when it holds none. [`Self::database_error`] says which it is.
    ///
    /// # Errors
    ///
    /// Returns the error itself when no database error is behind it.
    pub fn into_database_error(self) -> Result<coral_db::Error, Self> {
        match self {
            Self::Database(error) => Ok(error),
            Self::Workspace(error) => error.into_database_error().map_err(Self::Workspace),
            Self::Search(error) => error.into_database_error().map_err(Self::Search),
            Self::Tools(error) => error.into_database_error().map_err(Self::Tools),
            Self::Run(error) => error.into_database_error().map_err(Self::Run),
            other => {
                debug_assert!(other.database_error().is_none());
                Err(other)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    // Catches: a database error that a caller of Task cannot find through
    // the Run, Search or authored Tool error that holds it.
    #[test]
    fn a_task_error_states_the_database_error_behind_it() {
        type Wrap = fn(coral_db::Error) -> Error;
        let wrapped: [(Wrap, &str); 6] = [
            (Error::Database, "task"),
            (
                |error| Error::Workspace(coral_workspace::Error::Database(error)),
                "workspace",
            ),
            (|error| Error::Run(coral_run::Error::Database(error)), "run"),
            (
                |error| {
                    Error::Run(coral_run::Error::Workspace(
                        coral_workspace::Error::Database(error),
                    ))
                },
                "run workspace",
            ),
            (
                |error| {
                    Error::Search(coral_search::Error::Tools(coral_tools::Error::Database(
                        error,
                    )))
                },
                "search tools",
            ),
            (
                |error| {
                    Error::Tools(coral_tools::Error::Workspace(
                        coral_workspace::Error::Database(error),
                    ))
                },
                "tools workspace",
            ),
        ];
        for (wrap, label) in wrapped {
            assert!(
                wrap(coral_db::Error::WriterBusy)
                    .database_error()
                    .is_some_and(coral_db::Error::is_unanswered),
                "{label}"
            );
            assert!(
                matches!(
                    wrap(coral_db::Error::WriterBusy).into_database_error(),
                    Ok(coral_db::Error::WriterBusy)
                ),
                "{label}"
            );
        }
        for error in [
            Error::TaskNotFound,
            Error::CorruptData("row".into()),
            Error::Run(coral_run::Error::RunNotFound),
            Error::Search(coral_search::Error::ServiceUnavailable),
        ] {
            assert!(error.database_error().is_none(), "{error:?}");
            error.into_database_error().expect_err("no database error");
        }
    }
}
