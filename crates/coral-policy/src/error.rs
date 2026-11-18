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

impl Error {
    /// The database error behind this one, directly or through the Provider
    /// or Workspace error that wraps it. A module that wraps this error asks
    /// here, so it never lists another module's variants.
    #[must_use]
    pub const fn database_error(&self) -> Option<&coral_db::Error> {
        match self {
            Self::Database(error) => Some(error),
            Self::Provider(error) => error.database_error(),
            Self::Workspace(error) => error.database_error(),
            // Name every variant so a new one that holds a database error
            // is found here.
            Self::InvalidBatch
            | Self::ToolNotFound
            | Self::RefusedToolPaths { .. }
            | Self::CorruptData(_)
            | Self::EventFields(_) => None,
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
            Self::Provider(error) => error.into_database_error().map_err(Self::Provider),
            Self::Workspace(error) => error.into_database_error().map_err(Self::Workspace),
            other => {
                debug_assert!(other.database_error().is_none());
                Err(other)
            }
        }
    }

    /// The Workspace refusal this error wraps, directly or through another
    /// module's error. A refusal reads the same whichever module wrapped
    /// it, so a caller reports it without naming that module's variants.
    #[must_use]
    pub const fn workspace_refusal(&self) -> Option<coral_workspace::AuthorityRefusal> {
        match self {
            Self::Workspace(error) => error.refusal(),
            Self::Provider(error) => error.workspace_refusal(),
            // Name every variant so a new one that wraps a Workspace error
            // is found here.
            Self::Database(_)
            | Self::InvalidBatch
            | Self::ToolNotFound
            | Self::RefusedToolPaths { .. }
            | Self::CorruptData(_)
            | Self::EventFields(_) => None,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::Error;

    // Catches: a database error that a caller of Policy cannot find, so an
    // unanswered write reads as a definite answer.
    #[test]
    fn a_policy_error_states_the_database_error_behind_it() {
        let unanswered = || coral_db::Error::WriterBusy;
        let found: [(Error, &str); 4] = [
            (Error::Database(unanswered()), "policy"),
            (
                Error::Workspace(coral_workspace::Error::Database(unanswered())),
                "workspace",
            ),
            (
                Error::Provider(coral_provider::Error::Database(unanswered())),
                "provider",
            ),
            (
                Error::Provider(coral_provider::Error::Catalog(
                    coral_catalog::Error::Database(unanswered()),
                )),
                "provider catalog",
            ),
        ];
        for (error, label) in found {
            assert!(
                error
                    .database_error()
                    .is_some_and(coral_db::Error::is_unanswered),
                "{label}"
            );
            assert!(
                matches!(error.into_database_error(), Ok(coral_db::Error::WriterBusy)),
                "{label}"
            );
        }
        for error in [
            Error::ToolNotFound,
            Error::CorruptData("row".into()),
            Error::Workspace(coral_workspace::Error::WorkspaceArchived),
            Error::Provider(coral_provider::Error::ProviderNotFound),
        ] {
            assert!(error.database_error().is_none(), "{error:?}");
            let label = format!("{error:?}");
            let back = error.into_database_error().expect_err("no database error");
            assert_eq!(format!("{back:?}"), label);
        }
    }

    // Catches: a database failure inside a Provider or authored Tool read
    // reported as corrupt policy data, which reads as a definite answer.
    #[test]
    fn a_read_keeps_a_wrapped_database_error_and_calls_the_rest_corrupt() {
        let unanswered = || coral_db::Error::WriterBusy;
        let kept = [
            crate::tools_error(coral_tools::Error::Workspace(
                coral_workspace::Error::Database(unanswered()),
            )),
            crate::tools_error(coral_tools::Error::Database(unanswered())),
            crate::provider_read_error(coral_provider::Error::Catalog(
                coral_catalog::Error::Database(unanswered()),
            )),
            crate::provider_read_error(coral_provider::Error::Workspace(
                coral_workspace::Error::Database(unanswered()),
            )),
        ];
        for error in kept {
            assert!(
                matches!(error, Error::Database(coral_db::Error::WriterBusy)),
                "{error:?}"
            );
        }
        for error in [
            crate::tools_error(coral_tools::Error::PrivateToolExists),
            crate::provider_read_error(coral_provider::Error::ProviderNotFound),
        ] {
            assert!(matches!(error, Error::CorruptData(_)), "{error:?}");
        }
    }

    // Catches: a Workspace refusal that a caller of Policy cannot find
    // because a Provider error holds it.
    #[test]
    fn a_policy_error_states_the_workspace_refusal_it_wraps() {
        use coral_workspace::AuthorityRefusal as Refusal;
        for (error, refusal) in [
            (
                Error::Workspace(coral_workspace::Error::WorkspaceArchived),
                Some(Refusal::WorkspaceArchived),
            ),
            (
                Error::Provider(coral_provider::Error::Workspace(
                    coral_workspace::Error::AgentDisabled,
                )),
                Some(Refusal::AgentDisabled),
            ),
            (Error::Workspace(coral_workspace::Error::EtagMismatch), None),
            (
                Error::Provider(coral_provider::Error::ProviderNotFound),
                None,
            ),
            (Error::ToolNotFound, None),
        ] {
            assert_eq!(error.workspace_refusal(), refusal, "{error:?}");
        }
    }
}
