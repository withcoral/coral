use coral_workspace::{AuthenticatedPrincipal, TraceId, WorkspaceId};
use uuid::Uuid;

use crate::Error;

/// Longest Middleware name, in bytes.
pub const MIDDLEWARE_NAME_MAX_BYTES: usize = 64;
/// Longest Middleware description, in bytes.
pub const MIDDLEWARE_DESCRIPTION_MAX_BYTES: usize = 1024;
/// Largest Middleware source, in bytes. It matches one Saved Function source.
pub const MIDDLEWARE_SOURCE_MAX_BYTES: usize = 64 * 1024;
/// Most Middleware one Workspace can hold. Every enabled one joins each Run.
pub const MIDDLEWARE_MAX_COUNT: usize = 16;
/// Most advisory diagnostics retained with one accepted source.
pub const MIDDLEWARE_DIAGNOSTIC_MAX_COUNT: usize = 50;

/// Stable, non-authorizing Middleware identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MiddlewareId(Uuid);

impl MiddlewareId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses a canonical lowercase UUID.
    ///
    /// # Errors
    ///
    /// Rejects malformed or noncanonical identifiers.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value)
            .map_err(|_error| Error::InvalidInput("invalid Middleware id".to_string()))?;
        if id.to_string() != value {
            return Err(Error::InvalidInput("invalid Middleware id".to_string()));
        }
        Ok(Self(id))
    }
}

impl std::fmt::Display for MiddlewareId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Workspace-unique Middleware name: a lowercase letter, then up to 63
/// lowercase letters, digits, hyphens, or underscores.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MiddlewareName(String);

impl MiddlewareName {
    /// Parses one name.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, or non-slug names.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let mut characters = value.chars();
        let valid = value.len() <= MIDDLEWARE_NAME_MAX_BYTES
            && characters
                .next()
                .is_some_and(|first| first.is_ascii_lowercase())
            && characters.all(|character| {
                character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || matches!(character, '-' | '_')
            });
        if valid {
            Ok(Self(value.to_string()))
        } else {
            Err(Error::InvalidInput(format!(
                "Middleware name must start with a lowercase letter and use at most {MIDDLEWARE_NAME_MAX_BYTES} lowercase letters, digits, hyphens, or underscores"
            )))
        }
    }

    /// Returns the name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Owner-facing explanation of what one Middleware does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MiddlewareDescription(String);

impl MiddlewareDescription {
    /// Parses one nonblank bounded description.
    ///
    /// # Errors
    ///
    /// Rejects blank or oversized text.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.trim().is_empty() || value.len() > MIDDLEWARE_DESCRIPTION_MAX_BYTES {
            return Err(Error::InvalidInput(format!(
                "Middleware description must be 1 to {MIDDLEWARE_DESCRIPTION_MAX_BYTES} bytes"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the description.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One Middleware TypeScript module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MiddlewareSource(String);

impl MiddlewareSource {
    /// Parses one nonblank bounded source.
    ///
    /// # Errors
    ///
    /// Rejects blank or oversized source.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.trim().is_empty() || value.len() > MIDDLEWARE_SOURCE_MAX_BYTES {
            return Err(Error::InvalidInput(format!(
                "Middleware source must be 1 to {MIDDLEWARE_SOURCE_MAX_BYTES} bytes"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the source.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Invocation levels one Middleware wraps. Scopes select execution points;
/// every enabled Middleware has its own Run and Saved Function contexts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MiddlewareScopes {
    /// Wrap the Program Run.
    pub run: bool,
    /// Wrap each Saved Function call.
    pub function: bool,
    /// Wrap each Operation Invocation.
    pub operation: bool,
}

/// One accepted save: name, description and source. A save never changes
/// scopes or order and always leaves the Middleware disabled.
#[derive(Debug, Clone)]
pub struct MiddlewareSave {
    name: MiddlewareName,
    description: MiddlewareDescription,
    source: MiddlewareSource,
}

impl MiddlewareSave {
    /// Builds one validated save request.
    #[must_use]
    pub const fn new(
        name: MiddlewareName,
        description: MiddlewareDescription,
        source: MiddlewareSource,
    ) -> Self {
        Self {
            name,
            description,
            source,
        }
    }

    /// Returns the target name.
    #[must_use]
    pub const fn name(&self) -> &MiddlewareName {
        &self.name
    }

    /// Returns the description.
    #[must_use]
    pub const fn description(&self) -> &MiddlewareDescription {
        &self.description
    }

    /// Returns the source.
    #[must_use]
    pub const fn source(&self) -> &MiddlewareSource {
        &self.source
    }
}

/// Explicit change attribution supplied by an Adapter.
#[derive(Debug, Clone)]
pub struct MiddlewareChangeContext {
    actor: AuthenticatedPrincipal,
    occurred_at_unix_nanos: i64,
    trace_id: TraceId,
}

impl MiddlewareChangeContext {
    /// Builds change attribution. Only a Workspace Owner User can change Middleware.
    #[must_use]
    pub const fn new(
        actor: AuthenticatedPrincipal,
        occurred_at_unix_nanos: i64,
        trace_id: TraceId,
    ) -> Self {
        Self {
            actor,
            occurred_at_unix_nanos,
            trace_id,
        }
    }

    /// Returns the accountable Principal.
    #[must_use]
    pub const fn actor(&self) -> &AuthenticatedPrincipal {
        &self.actor
    }

    /// Returns the occurrence time.
    #[must_use]
    pub const fn occurred_at_unix_nanos(&self) -> i64 {
        self.occurred_at_unix_nanos
    }

    /// Returns the trace identity.
    #[must_use]
    pub const fn trace_id(&self) -> &TraceId {
        &self.trace_id
    }
}

/// Advisory TypeScript diagnostic retained with an accepted source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MiddlewareDiagnostic {
    /// TypeScript diagnostic category.
    pub category: coral_v8::DiagnosticCategory,
    /// Official TypeScript code.
    pub code: u64,
    /// Flattened message.
    pub message: String,
    /// One-based line.
    pub line: Option<u64>,
    /// One-based column.
    pub column: Option<u64>,
}

/// One current Workspace Middleware.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Middleware {
    pub(crate) id: MiddlewareId,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) name: MiddlewareName,
    pub(crate) description: MiddlewareDescription,
    pub(crate) source: MiddlewareSource,
    pub(crate) diagnostics: Vec<MiddlewareDiagnostic>,
    pub(crate) scopes: MiddlewareScopes,
    pub(crate) enabled: bool,
    pub(crate) position: u32,
    pub(crate) revision: i64,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) updated_at_unix_nanos: i64,
}

impl Middleware {
    /// Returns the stable identity.
    #[must_use]
    pub const fn id(&self) -> &MiddlewareId {
        &self.id
    }

    /// Returns the owning Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the Workspace-unique name.
    #[must_use]
    pub const fn name(&self) -> &MiddlewareName {
        &self.name
    }

    /// Returns the description.
    #[must_use]
    pub const fn description(&self) -> &MiddlewareDescription {
        &self.description
    }

    /// Returns the accepted source.
    #[must_use]
    pub const fn source(&self) -> &MiddlewareSource {
        &self.source
    }

    /// Returns advisory diagnostics from the accepted source.
    #[must_use]
    pub fn diagnostics(&self) -> &[MiddlewareDiagnostic] {
        &self.diagnostics
    }

    /// Returns the wrapped invocation levels.
    #[must_use]
    pub const fn scopes(&self) -> MiddlewareScopes {
        self.scopes
    }

    /// Returns whether new Runs pin this Middleware.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the zero-based chain position, outermost first.
    #[must_use]
    pub const fn position(&self) -> u32 {
        self.position
    }

    /// Returns the revision. Every accepted change increments it.
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }

    /// Returns when the Middleware was first saved.
    #[must_use]
    pub const fn created_at_unix_nanos(&self) -> i64 {
        self.created_at_unix_nanos
    }

    /// Returns when the Middleware last changed.
    #[must_use]
    pub const fn updated_at_unix_nanos(&self) -> i64 {
        self.updated_at_unix_nanos
    }
}

/// One enabled Middleware pinned by Run admission, in chain order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MiddlewareSelection {
    pub(crate) id: MiddlewareId,
    pub(crate) name: MiddlewareName,
    pub(crate) source: MiddlewareSource,
    pub(crate) scopes: MiddlewareScopes,
    pub(crate) revision: i64,
}

impl MiddlewareSelection {
    /// Returns the stable identity.
    #[must_use]
    pub const fn id(&self) -> &MiddlewareId {
        &self.id
    }

    /// Returns the name.
    #[must_use]
    pub const fn name(&self) -> &MiddlewareName {
        &self.name
    }

    /// Returns the exact source pinned by the Run.
    #[must_use]
    pub const fn source(&self) -> &MiddlewareSource {
        &self.source
    }

    /// Returns the pinned scopes.
    #[must_use]
    pub const fn scopes(&self) -> MiddlewareScopes {
        self.scopes
    }

    /// Returns the selected revision.
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }

    /// Restores one pinned selection retained by a Run.
    ///
    /// # Errors
    ///
    /// Rejects retained values that no longer parse.
    pub fn restore(
        id: &str,
        name: &str,
        source: &str,
        scopes: MiddlewareScopes,
        revision: i64,
    ) -> Result<Self, Error> {
        Ok(Self {
            id: MiddlewareId::parse(id)?,
            name: MiddlewareName::parse(name)?,
            source: MiddlewareSource(source.to_string()),
            scopes,
            revision,
        })
    }
}

/// Direction of one reorder step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveDirection {
    /// Toward the outermost position.
    Up,
    /// Toward the innermost position.
    Down,
}

/// Safe Middleware lifecycle action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleAction {
    /// A save created the Middleware, disabled.
    Created,
    /// A save replaced its description and source and disabled it.
    Updated,
    /// An Owner enabled it.
    Enabled,
    /// An Owner disabled it.
    Disabled,
    /// An Owner changed its scopes, which disabled it.
    ScopesUpdated,
    /// An Owner moved it, which disabled it.
    Moved,
    /// An Owner deleted it.
    Deleted,
}

impl LifecycleAction {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Created => "middleware.created",
            Self::Updated => "middleware.updated",
            Self::Enabled => "middleware.enabled",
            Self::Disabled => "middleware.disabled",
            Self::ScopesUpdated => "middleware.scopes_updated",
            Self::Moved => "middleware.moved",
            Self::Deleted => "middleware.deleted",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        Ok(match value {
            "middleware.created" => Self::Created,
            "middleware.updated" => Self::Updated,
            "middleware.enabled" => Self::Enabled,
            "middleware.disabled" => Self::Disabled,
            "middleware.scopes_updated" => Self::ScopesUpdated,
            "middleware.moved" => Self::Moved,
            "middleware.deleted" => Self::Deleted,
            _ => {
                return Err(Error::CorruptData(
                    "Middleware lifecycle Event has an unknown action".to_string(),
                ));
            }
        })
    }
}

/// One safe Administrative Lifecycle Event; it never contains source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdministrativeLifecycleEvent {
    /// Accepted action.
    pub action: LifecycleAction,
    /// Target identity.
    pub target_id: MiddlewareId,
    /// Target name when the change happened.
    pub target_name: MiddlewareName,
    /// Accountable User.
    pub actor_user_id: String,
    /// Changed field paths.
    pub changed_field_paths: Vec<String>,
    /// Occurrence time.
    pub occurred_at_unix_nanos: i64,
}
