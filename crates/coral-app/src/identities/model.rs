#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Identity persistence consumers land in the next stack units."
    )
)]

use std::fmt;

use crate::bootstrap::AppError;
use crate::identity::{UserPrincipal, parse_path_segment};
use crate::state::db::{IdentitySpecKey, IdentitySpecScope};
use crate::workspaces::WorkspaceName;

const USER_OWNER_KIND: &str = "user";
const WORKSPACE_OWNER_KIND: &str = "workspace";

/// Validated name of one identity instance within an owner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IdentityName(String);

impl IdentityName {
    /// Parse and normalize an identity name.
    pub(crate) fn parse(name: &str) -> Result<Self, AppError> {
        parse_path_segment("identity", name).map(Self)
    }

    /// Borrow the normalized identity name.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdentityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// User or workspace that owns one identity instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdentityOwner {
    User(UserPrincipal),
    Workspace(WorkspaceName),
}

impl IdentityOwner {
    /// Build a user owner from an already validated request principal.
    pub(crate) fn for_user(principal: UserPrincipal) -> Self {
        Self::User(principal)
    }

    /// Build a workspace owner from an already validated workspace name.
    pub(crate) fn workspace(workspace: WorkspaceName) -> Self {
        Self::Workspace(workspace)
    }

    /// Storage discriminator for this owner.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::User(_) => USER_OWNER_KIND,
            Self::Workspace(_) => WORKSPACE_OWNER_KIND,
        }
    }

    /// Stable storage key within the owner kind.
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::User(principal) => principal.user_id(),
            Self::Workspace(workspace) => workspace.as_str(),
        }
    }

    /// Workspace foreign-key value, absent for user-owned identities.
    pub(crate) fn workspace_name(&self) -> Option<&WorkspaceName> {
        match self {
            Self::User(_) => None,
            Self::Workspace(workspace) => Some(workspace),
        }
    }
}

/// Exact identity-spec version selected when an identity is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecReference {
    key: IdentitySpecKey,
    fingerprint: String,
    issuer: String,
    identity_type: String,
}

impl IdentitySpecReference {
    /// Validate an exact spec reference against the identity owner.
    pub(crate) fn new(
        owner: &IdentityOwner,
        key: IdentitySpecKey,
        fingerprint: impl Into<String>,
        issuer: impl Into<String>,
        identity_type: impl Into<String>,
    ) -> Result<Self, AppError> {
        validate_scope(owner, key.scope())?;
        let reference = Self {
            key,
            fingerprint: fingerprint.into(),
            issuer: issuer.into(),
            identity_type: identity_type.into(),
        };
        for (field, value) in [
            ("identity spec fingerprint", reference.fingerprint.as_str()),
            ("identity spec issuer", reference.issuer.as_str()),
            ("identity spec type", reference.identity_type.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(AppError::InvalidInput(format!("missing {field}")));
            }
        }
        Ok(reference)
    }

    pub(crate) fn key(&self) -> &IdentitySpecKey {
        &self.key
    }

    pub(crate) fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub(crate) fn issuer(&self) -> &str {
        &self.issuer
    }

    pub(crate) fn identity_type(&self) -> &str {
        &self.identity_type
    }
}

fn validate_scope(owner: &IdentityOwner, scope: &IdentitySpecScope) -> Result<(), AppError> {
    let valid = match (owner, scope) {
        (_, IdentitySpecScope::Global) => true,
        (IdentityOwner::Workspace(owner), IdentitySpecScope::Workspace(spec)) => owner == spec,
        (IdentityOwner::User(_), IdentitySpecScope::Workspace(_)) => false,
    };
    if valid {
        Ok(())
    } else {
        Err(AppError::InvalidInput(
            "identity spec scope is incompatible with its owner".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{IdentityName, IdentityOwner, IdentitySpecReference};
    use crate::identity::UserPrincipal;
    use crate::state::db::IdentitySpecKey;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn identity_names_are_safe_normalized_and_orderable() {
        let alpha = IdentityName::parse("  alpha  ").expect("normalized name");
        let beta = IdentityName::parse("beta").expect("second name");
        assert_eq!(alpha.as_str(), "alpha");
        assert_eq!(alpha.to_string(), "alpha");
        assert!(alpha < beta);
        for invalid in ["", "  ", ".", "..", "bad/name", r"bad\name"] {
            assert!(
                IdentityName::parse(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn owners_expose_stable_storage_values() {
        let local = IdentityOwner::for_user(UserPrincipal::local());
        assert_eq!((local.kind(), local.key()), ("user", "local"));
        assert!(local.workspace_name().is_none());

        let user = IdentityOwner::for_user(UserPrincipal::for_user("member-1").expect("user"));
        assert_eq!((user.kind(), user.key()), ("user", "member-1"));

        let workspace = WorkspaceName::parse("team-1").expect("workspace");
        let owner = IdentityOwner::workspace(workspace.clone());
        assert_eq!((owner.kind(), owner.key()), ("workspace", "team-1"));
        assert_eq!(owner.workspace_name(), Some(&workspace));
    }

    #[test]
    fn spec_references_enforce_the_full_owner_scope_matrix() {
        let user = IdentityOwner::for_user(UserPrincipal::local());
        let alpha = WorkspaceName::parse("alpha").expect("alpha");
        let beta = WorkspaceName::parse("beta").expect("beta");
        let workspace = IdentityOwner::workspace(alpha.clone());
        let reference = |owner: &IdentityOwner, key| {
            IdentitySpecReference::new(owner, key, "fingerprint", "issuer", "fixed_token")
        };

        reference(&user, IdentitySpecKey::global("token").expect("global"))
            .expect("user may reference a global spec");
        reference(
            &user,
            IdentitySpecKey::workspace(alpha.clone(), "token").expect("scoped"),
        )
        .expect_err("user must not reference a workspace spec");
        let global = reference(
            &workspace,
            IdentitySpecKey::global("token").expect("global"),
        )
        .expect("workspace global fallback");
        assert_eq!(global.key().name(), "token");
        assert_eq!(global.fingerprint(), "fingerprint");
        assert_eq!(global.issuer(), "issuer");
        assert_eq!(global.identity_type(), "fixed_token");
        reference(
            &workspace,
            IdentitySpecKey::workspace(alpha, "token").expect("same workspace"),
        )
        .expect("workspace may reference its own spec");
        reference(
            &workspace,
            IdentitySpecKey::workspace(beta, "token").expect("other workspace"),
        )
        .expect_err("workspace must not reference another workspace's spec");
    }

    #[test]
    fn spec_references_reject_blank_required_fields() {
        let owner = IdentityOwner::for_user(UserPrincipal::local());
        for fields in [
            (" ", "issuer", "fixed_token"),
            ("fingerprint", "\t", "fixed_token"),
            ("fingerprint", "issuer", "\n"),
        ] {
            IdentitySpecReference::new(
                &owner,
                IdentitySpecKey::global("token").expect("key"),
                fields.0,
                fields.1,
                fields.2,
            )
            .expect_err("blank required spec-reference field must be rejected");
        }
    }
}
