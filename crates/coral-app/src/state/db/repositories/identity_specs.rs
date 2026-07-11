#![expect(
    dead_code,
    reason = "identity repositories land before their read and write behavior in the B1 stack"
)]

use crate::bootstrap::AppError;
use crate::state::db::{DbError, DbSession};
use crate::workspaces::WorkspaceName;
use coral_spec::validate_identity_spec_name;

const GLOBAL_SCOPE_KIND: &str = "global";
const GLOBAL_SCOPE_ID: &str = "__global__";
const WORKSPACE_SCOPE_KIND: &str = "workspace";

/// Definition scope for one durable identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdentitySpecScope {
    /// A globally installed identity spec definition.
    Global,
    /// An identity spec definition scoped to one workspace.
    Workspace(WorkspaceName),
}

impl IdentitySpecScope {
    /// Build the global identity-spec scope.
    pub(crate) fn global() -> Self {
        Self::Global
    }

    /// Build a workspace identity-spec scope.
    pub(crate) fn workspace(workspace_name: WorkspaceName) -> Self {
        Self::Workspace(workspace_name)
    }

    pub(super) fn kind(&self) -> &'static str {
        match self {
            Self::Global => GLOBAL_SCOPE_KIND,
            Self::Workspace(_workspace_name) => WORKSPACE_SCOPE_KIND,
        }
    }

    pub(super) fn scope_id(&self) -> &str {
        match self {
            Self::Global => GLOBAL_SCOPE_ID,
            Self::Workspace(workspace_name) => workspace_name.as_str(),
        }
    }

    pub(super) fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::Global => None,
            Self::Workspace(workspace_name) => Some(workspace_name.as_str()),
        }
    }
}

/// Portable primary key for one global or workspace-scoped identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecKey {
    /// Scope that owns this identity spec definition.
    scope: IdentitySpecScope,
    /// Identity spec name unique within the scope.
    name: String,
}

impl IdentitySpecKey {
    /// Build an identity-spec key from a scope and validated name.
    pub(crate) fn new(scope: IdentitySpecScope, name: &str) -> Result<Self, AppError> {
        Ok(Self {
            scope,
            name: parse_identity_spec_name(name)?,
        })
    }

    /// Build a global identity-spec key.
    pub(crate) fn global(name: &str) -> Result<Self, AppError> {
        Self::new(IdentitySpecScope::global(), name)
    }

    /// Build a workspace-scoped identity-spec key.
    pub(crate) fn workspace(workspace_name: WorkspaceName, name: &str) -> Result<Self, AppError> {
        Self::new(IdentitySpecScope::workspace(workspace_name), name)
    }

    /// Borrow the scope selected for this identity spec.
    pub(crate) fn scope(&self) -> &IdentitySpecScope {
        &self.scope
    }

    /// Borrow the validated identity-spec name.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn from_spec_storage_parts(
        scope_kind: &str,
        scope_id: &str,
        workspace_id: Option<&str>,
        name: &str,
    ) -> Result<Self, DbError> {
        let scope = match (scope_kind, workspace_id) {
            (GLOBAL_SCOPE_KIND, None) if scope_id == GLOBAL_SCOPE_ID => IdentitySpecScope::Global,
            (GLOBAL_SCOPE_KIND, _) => {
                return Err(DbError::CorruptData(
                    "global identity spec row has invalid scope columns".to_string(),
                ));
            }
            (WORKSPACE_SCOPE_KIND, Some(workspace_id)) if scope_id == workspace_id => {
                IdentitySpecScope::Workspace(parse_workspace_name(workspace_id)?)
            }
            (WORKSPACE_SCOPE_KIND, _) => {
                return Err(DbError::CorruptData(
                    "workspace identity spec row has invalid scope columns".to_string(),
                ));
            }
            (other, _) => {
                return Err(DbError::CorruptData(format!(
                    "identity spec row has invalid scope kind '{other}'"
                )));
            }
        };
        Ok(Self {
            scope,
            name: parse_persisted_identity_spec_name(name)?,
        })
    }

    pub(super) fn from_document_storage_parts(
        scope_kind: &str,
        scope_id: &str,
        name: &str,
    ) -> Result<Self, DbError> {
        let scope = match scope_kind {
            GLOBAL_SCOPE_KIND if scope_id == GLOBAL_SCOPE_ID => IdentitySpecScope::Global,
            GLOBAL_SCOPE_KIND => {
                return Err(DbError::CorruptData(
                    "global identity spec document row has invalid scope columns".to_string(),
                ));
            }
            WORKSPACE_SCOPE_KIND => IdentitySpecScope::Workspace(parse_workspace_name(scope_id)?),
            other => {
                return Err(DbError::CorruptData(format!(
                    "identity spec document row has invalid scope kind '{other}'"
                )));
            }
        };
        Ok(Self {
            scope,
            name: parse_persisted_identity_spec_name(name)?,
        })
    }
}

/// Repository shell for durable DSL v4 identity spec definitions.
pub(crate) struct IdentitySpecsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> IdentitySpecsRepo<'a, S>
where
    S: DbSession,
{
    /// Create an identity-spec repository over an existing DB session.
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }
}

/// Repository shell for encrypted setup-input documents owned by identity specs.
pub(crate) struct IdentitySpecDocumentsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> IdentitySpecDocumentsRepo<'a, S>
where
    S: DbSession,
{
    /// Create an identity-spec document repository over an existing DB session.
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }
}

fn parse_identity_spec_name(name: &str) -> Result<String, AppError> {
    validate_identity_spec_name(name).map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(name.to_string())
}

fn parse_workspace_name(workspace_id: &str) -> Result<WorkspaceName, DbError> {
    let workspace_name = WorkspaceName::parse(workspace_id).map_err(|error| {
        DbError::CorruptData(format!("invalid workspace id '{workspace_id}': {error}"))
    })?;
    if workspace_name.as_str() != workspace_id {
        return Err(DbError::CorruptData(format!(
            "workspace id '{workspace_id}' is not normalized"
        )));
    }
    Ok(workspace_name)
}

fn parse_persisted_identity_spec_name(name: &str) -> Result<String, DbError> {
    let parsed = parse_identity_spec_name(name).map_err(|error| {
        DbError::CorruptData(format!("invalid identity spec name '{name}': {error}"))
    })?;
    if parsed != name {
        return Err(DbError::CorruptData(format!(
            "identity spec name '{name}' is not normalized"
        )));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::IdentitySpecKey;
    use crate::bootstrap::AppError;
    use crate::state::db::DbError;

    #[test]
    fn caller_names_keep_invalid_input_classification() {
        for name in [
            "bad/name",
            "github-oauth",
            "github oauth",
            "9github",
            " github",
            "github ",
        ] {
            assert!(matches!(
                IdentitySpecKey::global(name),
                Err(AppError::InvalidInput(_))
            ));
        }

        let key = IdentitySpecKey::global("github_oauth2").expect("valid identity spec name");
        assert_eq!(key.name(), "github_oauth2");
        assert!(matches!(key.scope(), super::IdentitySpecScope::Global));
    }

    #[test]
    fn persisted_scope_keys_reject_non_normalized_identifiers() {
        for result in [
            IdentitySpecKey::from_spec_storage_parts("global", "__global__", None, " github"),
            IdentitySpecKey::from_spec_storage_parts(
                "workspace",
                " default",
                Some(" default"),
                "github",
            ),
            IdentitySpecKey::from_document_storage_parts("global", "__global__", "github "),
            IdentitySpecKey::from_document_storage_parts("workspace", " default", "github"),
            IdentitySpecKey::from_spec_storage_parts("global", "__global__", None, "github-oauth"),
        ] {
            assert!(matches!(result, Err(DbError::CorruptData(_))));
        }
    }
}
