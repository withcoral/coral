#![expect(
    dead_code,
    reason = "identity repositories land before their read and write behavior in the B1 stack"
)]

use crate::bootstrap::AppError;
use crate::state::db::{DbError, DbSession};
use crate::workspaces::WorkspaceName;
use coral_spec::validate_identity_spec_name;
use uuid::{Uuid, Variant, Version};

/// Opaque database identity for one persisted identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecId(String);

impl IdentitySpecId {
    pub(super) fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub(super) fn from_storage(value: String) -> Result<Self, DbError> {
        let parsed = Uuid::parse_str(&value).map_err(|error| {
            DbError::CorruptData(format!("invalid identity spec id '{value}': {error}"))
        })?;
        if parsed.get_version() != Some(Version::Random) || parsed.get_variant() != Variant::RFC4122
        {
            return Err(DbError::CorruptData(format!(
                "identity spec id '{value}' is not an RFC 4122 UUID v4"
            )));
        }
        if parsed.to_string() != value {
            return Err(DbError::CorruptData(format!(
                "identity spec id '{value}' is not canonical"
            )));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

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

    pub(super) fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::Global => None,
            Self::Workspace(workspace_name) => Some(workspace_name.as_str()),
        }
    }
}

/// Logical lookup key for one global or workspace-scoped identity spec.
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
        workspace_id: Option<&str>,
        name: &str,
    ) -> Result<Self, DbError> {
        let scope = match workspace_id {
            None => IdentitySpecScope::Global,
            Some(workspace_id) => IdentitySpecScope::Workspace(parse_workspace_name(workspace_id)?),
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
    use super::{IdentitySpecId, IdentitySpecKey};
    use crate::bootstrap::AppError;
    use crate::state::db::DbError;
    use uuid::Uuid;

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
            IdentitySpecKey::from_spec_storage_parts(None, " github"),
            IdentitySpecKey::from_spec_storage_parts(Some(" default"), "github"),
            IdentitySpecKey::from_spec_storage_parts(None, "github "),
            IdentitySpecKey::from_spec_storage_parts(None, "github-oauth"),
        ] {
            assert!(matches!(result, Err(DbError::CorruptData(_))));
        }
    }

    #[test]
    fn persisted_identity_spec_ids_must_be_canonical_rfc_4122_uuid_v4_values() {
        let id = IdentitySpecId::new();
        assert_eq!(
            IdentitySpecId::from_storage(id.as_str().to_string()).expect("canonical id"),
            id
        );

        for invalid in [
            "not-a-uuid".to_string(),
            Uuid::nil().to_string(),
            Uuid::new_v4().simple().to_string(),
            Uuid::new_v4().to_string().to_uppercase(),
            "00000000-0000-4000-c000-000000000000".to_string(),
        ] {
            assert!(matches!(
                IdentitySpecId::from_storage(invalid),
                Err(DbError::CorruptData(_))
            ));
        }
    }
}
