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
    /// Saved Function authoring material could not be resolved.
    #[error(transparent)]
    Functions(#[from] coral_functions::Error),
    /// Workspace authority rejected discovery.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// Persistence rejected a coherent discovery read.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

impl Error {
    /// Returns a safe definite rejection, excluding uncertain persistence or host errors.
    #[must_use]
    pub fn safe_rejection(&self) -> Option<(&'static str, String)> {
        if let Self::Workspace(error) = self {
            return error
                .safe_rejection()
                .map(|(code, message)| (code, message.to_owned()));
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
/// sentence.
#[must_use]
pub fn searchable_namespaces(searchable: &[ProviderCallRoot]) -> String {
    if searchable.is_empty() {
        return "You can search no Provider.".to_owned();
    }
    format!(
        "Use one of: {}.",
        searchable
            .iter()
            .map(crate::provider_filter_value)
            .collect::<Vec<_>>()
            .join(", ")
    )
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
