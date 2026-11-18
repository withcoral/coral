use crate::ProviderCallRoot;

/// Universal Search Module failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied discovery input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// A Provider filter value names no Provider the caller can search.
    #[error("no Provider the caller can search has the requested Call Root")]
    UnknownProviderCallRoot {
        /// The Call Roots the caller can search, in order.
        searchable: Vec<ProviderCallRoot>,
        /// The positions of the filter values that name none of them, in
        /// order.
        unmatched: Vec<usize>,
    },
    /// Search results could not be ranked, or a result page could not be
    /// produced.
    #[error("Universal Search is temporarily unavailable")]
    ServiceUnavailable,
    /// The exact Call Path is absent or concealed from this Principal.
    #[error("Authoring Description was not found")]
    AuthoringDescriptionNotFound,
    /// `describe` was asked for more than [`crate::DESCRIBE_TYPES_MAX`]
    /// types, or a name longer than [`crate::DESCRIBE_TYPE_NAME_MAX_BYTES`].
    #[error("types exceeds its limits")]
    TypesLimit,
    /// `describe` was asked for types its declaration does not declare.
    #[error("types names a type the declaration does not declare")]
    UndeclaredTypes {
        /// The requested names the declaration does not declare, in the
        /// order requested.
        undeclared: Vec<String>,
        /// Every type the declaration declares, in its order.
        declared: Vec<String>,
    },
    /// This discovery slice does not yet support the authenticated Principal.
    #[error("permission denied: {0}")]
    PermissionDenied(&'static str),
    /// Provider authoring material could not be resolved.
    #[error(transparent)]
    Provider(#[from] coral_provider::Error),
    /// Exact Tool policy could not be resolved.
    #[error(transparent)]
    Policy(#[from] coral_policy::Error),
    /// Private or Shared Tool authoring material could not be resolved.
    #[error(transparent)]
    Tools(#[from] coral_tools::Error),
    /// Workspace authority rejected discovery.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// Persistence rejected a coherent discovery read.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

impl Error {
    /// Whether the caller lost the Workspace: it is gone, or the caller may
    /// no longer inspect it. Every later request of this caller fails the
    /// same way, so a failed [`crate::UniversalSearchModule::searchable_providers`]
    /// with it denies the request instead of serving it without the list.
    #[must_use]
    pub const fn lost_workspace_authority(&self) -> bool {
        use coral_workspace::AuthorityRefusal as Refusal;
        if matches!(self, Self::PermissionDenied(_)) {
            return true;
        }
        match self.workspace_refusal() {
            Some(
                Refusal::WorkspaceNotFound
                | Refusal::PermissionDenied
                | Refusal::AgentNotFound
                | Refusal::InvalidAgentCredential
                | Refusal::AgentDisabled
                | Refusal::AgentRegistrationExpired,
            ) => true,
            // An archived Workspace stays open to inspection.
            Some(Refusal::WorkspaceArchived) | None => false,
        }
    }

    /// The database error behind this one, directly or through the module
    /// error that wraps it.
    #[must_use]
    pub const fn database_error(&self) -> Option<&coral_db::Error> {
        match self {
            Self::Database(error) => Some(error),
            Self::Workspace(error) => error.database_error(),
            Self::Provider(error) => error.database_error(),
            Self::Policy(error) => error.database_error(),
            Self::Tools(error) => error.database_error(),
            // Name every variant so a new one that holds a database error
            // is found here.
            Self::InvalidInput(_)
            | Self::UnknownProviderCallRoot { .. }
            | Self::ServiceUnavailable
            | Self::AuthoringDescriptionNotFound
            | Self::TypesLimit
            | Self::UndeclaredTypes { .. }
            | Self::PermissionDenied(_) => None,
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
            Self::Provider(error) => error.into_database_error().map_err(Self::Provider),
            Self::Policy(error) => error.into_database_error().map_err(Self::Policy),
            Self::Tools(error) => error.into_database_error().map_err(Self::Tools),
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
            Self::Policy(error) => error.workspace_refusal(),
            Self::Tools(error) => error.workspace_refusal(),
            // Name every variant so a new one that wraps a Workspace error
            // is found here.
            Self::InvalidInput(_)
            | Self::UnknownProviderCallRoot { .. }
            | Self::ServiceUnavailable
            | Self::AuthoringDescriptionNotFound
            | Self::TypesLimit
            | Self::UndeclaredTypes { .. }
            | Self::PermissionDenied(_)
            | Self::Database(_) => None,
        }
    }

    /// Returns a safe definite rejection, excluding uncertain persistence or host errors.
    #[must_use]
    pub fn safe_rejection(&self) -> Option<(&'static str, String)> {
        if let Some(refusal) = self.workspace_refusal() {
            return Some((refusal.code(), refusal.message().to_owned()));
        }
        if let Self::UnknownProviderCallRoot { searchable, .. } = self {
            return Some(("invalid_input", unknown_call_root_message(searchable)));
        }
        if let Self::TypesLimit = self {
            return Some((
                "types_limit",
                format!(
                    "types has at most {} names of at most {} UTF-8 bytes each.",
                    crate::DESCRIBE_TYPES_MAX,
                    crate::DESCRIBE_TYPE_NAME_MAX_BYTES
                ),
            ));
        }
        if let Self::UndeclaredTypes {
            undeclared,
            declared,
        } = self
        {
            return Some((
                "undeclared_types",
                undeclared_types_message(undeclared, declared),
            ));
        }
        let (code, message) = match self {
            Self::InvalidInput(_) => ("invalid_input", "Discovery input is invalid"),
            Self::ServiceUnavailable => (
                "service_unavailable",
                "Universal Search is temporarily unavailable",
            ),
            Self::AuthoringDescriptionNotFound => (
                "authoring_description_not_found",
                "Authoring Description was not found",
            ),
            _ => return None,
        };
        Some((code, message.to_owned()))
    }
}

/// Why one Provider filter value is refused, without the namespaces to use
/// and without the value.
pub const UNKNOWN_NAMESPACE_REASON: &str = "No Provider you can search has this namespace";

/// Names only what the caller can search, never the value it asked for.
fn unknown_call_root_message(searchable: &[ProviderCallRoot]) -> String {
    format!(
        "{UNKNOWN_NAMESPACE_REASON}. {}",
        searchable_namespaces(searchable)
    )
}

/// The namespaces a refused Provider filter can use instead, as one
/// sentence, sorted as `exec`'s description lists them.
#[must_use]
pub fn searchable_namespaces(searchable: &[ProviderCallRoot]) -> String {
    if searchable.is_empty() {
        return "You can search no Provider.".to_owned();
    }
    let mut namespaces = searchable
        .iter()
        .map(crate::provider_filter_value)
        .collect::<Vec<_>>();
    namespaces.sort_unstable();
    format!("Use one of: {}.", namespaces.join(", "))
}

/// The most declared names an `undeclared_types` rejection lists.
const DECLARED_NAMES_SHOWN: usize = 50;

fn undeclared_types_message(undeclared: &[String], declared: &[String]) -> String {
    let quoted = |names: &[String]| {
        names
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let missing = match undeclared {
        [name] => format!("Type \"{name}\" is not declared by this path."),
        names => format!("Types {} are not declared by this path.", quoted(names)),
    };
    let shown = quoted(
        &declared
            .iter()
            .take(DECLARED_NAMES_SHOWN)
            .cloned()
            .collect::<Vec<_>>(),
    );
    match declared.len().saturating_sub(DECLARED_NAMES_SHOWN) {
        _ if declared.is_empty() => format!("{missing} Coral cannot list the types it declares."),
        0 => format!("{missing} It declares: {shown}."),
        more => format!("{missing} It declares: {shown}, and {more} more."),
    }
}

#[cfg(test)]
mod tests;
