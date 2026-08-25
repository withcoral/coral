use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU16;

use serde_json::Value;

use crate::bootstrap::AppError;
use crate::identity::{LOCAL_PRINCIPAL_ID, Principal, PrincipalKind, parse_path_segment};
use crate::state::db::{DbError, IdentitySpecKey, IdentitySpecScope};
use crate::workspaces::WorkspaceName;

const USER_OWNER_KIND: &str = "user";
const WORKSPACE_OWNER_KIND: &str = "workspace";

/// Normalized provider audience pinned when an identity is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentityAudience {
    host: String,
    port: Option<NonZeroU16>,
}

impl IdentityAudience {
    /// Parse and normalize one typed host and optional non-zero port.
    pub(crate) fn new(host: &str, port: Option<u16>) -> Result<Self, AppError> {
        if host.trim().is_empty() {
            return Err(invalid_audience());
        }
        let host = url::Host::parse(host)
            .map_err(|_error| invalid_audience())?
            .to_string();
        let port = port
            .map(|port| NonZeroU16::new(port).ok_or_else(invalid_audience))
            .transpose()?;
        Ok(Self { host, port })
    }

    /// Extract an already normalized audience from a validated manifest.
    pub(crate) fn from_manifest(audience: &BTreeMap<String, Value>) -> Result<Self, AppError> {
        if audience.keys().any(|key| key != "host" && key != "port") {
            return Err(invalid_audience());
        }
        let host = audience
            .get("host")
            .and_then(Value::as_str)
            .ok_or_else(invalid_audience)?;
        let port = audience
            .get("port")
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|port| u16::try_from(port).ok())
                    .ok_or_else(invalid_audience)
            })
            .transpose()?;
        let normalized = Self::new(host, port)?;
        if normalized.host != host {
            return Err(invalid_audience());
        }
        Ok(normalized)
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) fn port(&self) -> Option<u16> {
        self.port.map(NonZeroU16::get)
    }

    fn from_storage_parts(
        host: Option<String>,
        port: Option<i64>,
    ) -> Result<Option<Self>, DbError> {
        let Some(host) = host else {
            return if port.is_none() {
                Ok(None)
            } else {
                Err(corrupt_audience())
            };
        };
        let port = port
            .map(|port| u16::try_from(port).map_err(|_error| corrupt_audience()))
            .transpose()?;
        let audience = Self::new(&host, port).map_err(|_error| corrupt_audience())?;
        if audience.host != host {
            return Err(corrupt_audience());
        }
        Ok(Some(audience))
    }
}

fn invalid_audience() -> AppError {
    AppError::InvalidInput(
        "identity audience must contain a normalized host and optional port from 1 through 65535"
            .to_string(),
    )
}

fn corrupt_audience() -> DbError {
    DbError::CorruptData("persisted identity audience is invalid or not normalized".to_string())
}

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

    pub(crate) fn from_storage(value: &str) -> Result<Self, DbError> {
        let name = Self::parse(value).map_err(|error| {
            DbError::CorruptData(format!(
                "invalid persisted identity name '{value}': {error}"
            ))
        })?;
        if name.as_str() != value {
            return Err(DbError::CorruptData(format!(
                "persisted identity name '{value}' is not normalized"
            )));
        }
        Ok(name)
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
    User(Principal),
    Workspace(WorkspaceName),
}

impl IdentityOwner {
    /// Build a user owner from an already validated request principal.
    pub(crate) fn for_user(principal: Principal) -> Self {
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
            Self::User(principal) => principal.id().as_str(),
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

    pub(crate) fn from_storage_parts(
        owner_kind: &str,
        owner_key: &str,
        workspace_id: Option<&str>,
    ) -> Result<Self, DbError> {
        match (owner_kind, workspace_id) {
            (USER_OWNER_KIND, None) => {
                let principal = if owner_key == LOCAL_PRINCIPAL_ID {
                    Principal::local()
                } else {
                    Principal::parse(owner_key, PrincipalKind::User).map_err(|error| {
                        DbError::CorruptData(format!(
                            "invalid persisted identity user owner '{owner_key}': {error}"
                        ))
                    })?
                };
                if principal.id().as_str() != owner_key {
                    return Err(DbError::CorruptData(format!(
                        "persisted identity user owner '{owner_key}' is not normalized"
                    )));
                }
                Ok(Self::for_user(principal))
            }
            (WORKSPACE_OWNER_KIND, Some(workspace_id)) if owner_key == workspace_id => {
                let workspace = WorkspaceName::parse(workspace_id).map_err(|error| {
                    DbError::CorruptData(format!(
                        "invalid persisted identity workspace owner '{workspace_id}': {error}"
                    ))
                })?;
                if workspace.as_str() != workspace_id {
                    return Err(DbError::CorruptData(format!(
                        "persisted identity workspace owner '{workspace_id}' is not normalized"
                    )));
                }
                Ok(Self::workspace(workspace))
            }
            _ => Err(DbError::CorruptData(
                "persisted identity row has invalid owner columns".to_string(),
            )),
        }
    }

    /// Reconstruct an owner from a child table whose exact key omits workspace metadata.
    pub(crate) fn from_key_storage_parts(
        owner_kind: &str,
        owner_key: &str,
    ) -> Result<Self, DbError> {
        let workspace_id = (owner_kind == WORKSPACE_OWNER_KIND).then_some(owner_key);
        Self::from_storage_parts(owner_kind, owner_key, workspace_id)
    }
}

/// Exact identity-spec version selected when an identity is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentitySpecReference {
    key: IdentitySpecKey,
    fingerprint: String,
    issuer: String,
    identity_type: String,
    audience: Option<IdentityAudience>,
}

impl IdentitySpecReference {
    /// Validate an exact spec reference against the identity owner.
    pub(crate) fn new(
        owner: &IdentityOwner,
        key: IdentitySpecKey,
        fingerprint: impl Into<String>,
        issuer: impl Into<String>,
        identity_type: impl Into<String>,
        audience: IdentityAudience,
    ) -> Result<Self, AppError> {
        validate_scope(owner, key.scope())?;
        let reference = Self {
            key,
            fingerprint: fingerprint.into(),
            issuer: issuer.into(),
            identity_type: identity_type.into(),
            audience: Some(audience),
        };
        reference.validate_required_fields()?;
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

    /// Return the pinned audience, absent only for legacy persisted rows.
    pub(crate) fn audience(&self) -> Option<&IdentityAudience> {
        self.audience.as_ref()
    }

    pub(crate) fn validate_for_owner(&self, owner: &IdentityOwner) -> Result<(), AppError> {
        validate_scope(owner, self.key.scope())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "validates one complete persisted identity spec reference"
    )]
    pub(crate) fn from_storage_parts(
        owner: &IdentityOwner,
        workspace_id: Option<&str>,
        name: &str,
        fingerprint: String,
        issuer: String,
        identity_type: String,
        audience_host: Option<String>,
        audience_port: Option<i64>,
    ) -> Result<Self, DbError> {
        let key = IdentitySpecKey::from_reference_storage_parts(workspace_id, name)?;
        validate_scope(owner, key.scope()).map_err(|error| {
            DbError::CorruptData(format!(
                "invalid persisted identity spec reference: {error}"
            ))
        })?;
        let reference = Self {
            key,
            fingerprint,
            issuer,
            identity_type,
            audience: IdentityAudience::from_storage_parts(audience_host, audience_port)?,
        };
        reference.validate_required_fields().map_err(|error| {
            DbError::CorruptData(format!(
                "invalid persisted identity spec reference: {error}"
            ))
        })?;
        Ok(reference)
    }

    fn validate_required_fields(&self) -> Result<(), AppError> {
        for (field, value) in [
            ("identity spec fingerprint", self.fingerprint.as_str()),
            ("identity spec issuer", self.issuer.as_str()),
            ("identity spec type", self.identity_type.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(AppError::InvalidInput(format!("missing {field}")));
            }
        }
        if !matches!(self.identity_type.as_str(), "oauth" | "fixed_token") {
            return Err(AppError::InvalidInput(
                "identity spec type must be 'oauth' or 'fixed_token'".to_string(),
            ));
        }
        Ok(())
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
    use super::{IdentityAudience, IdentityName, IdentityOwner, IdentitySpecReference};
    use crate::identity::{Principal, PrincipalKind};
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
        let local = IdentityOwner::for_user(Principal::local());
        assert_eq!((local.kind(), local.key()), ("user", "coral:local"));
        assert!(local.workspace_name().is_none());

        let user = IdentityOwner::for_user(
            Principal::parse("member-1", PrincipalKind::User).expect("user"),
        );
        assert_eq!((user.kind(), user.key()), ("user", "member-1"));

        let workspace = WorkspaceName::parse("team-1").expect("workspace");
        let owner = IdentityOwner::workspace(workspace.clone());
        assert_eq!((owner.kind(), owner.key()), ("workspace", "team-1"));
        assert_eq!(owner.workspace_name(), Some(&workspace));
        assert_eq!(
            IdentityOwner::from_key_storage_parts("workspace", "team-1")
                .expect("stored workspace owner"),
            owner
        );
        assert_eq!(
            IdentityOwner::from_key_storage_parts("user", "coral:local")
                .expect("stored local owner"),
            local
        );
        IdentityOwner::from_key_storage_parts("unknown", "team-1")
            .expect_err("unknown owner kind must fail");
    }

    #[test]
    fn spec_references_enforce_the_full_owner_scope_matrix() {
        let user = IdentityOwner::for_user(Principal::local());
        let alpha = WorkspaceName::parse("alpha").expect("alpha");
        let beta = WorkspaceName::parse("beta").expect("beta");
        let workspace = IdentityOwner::workspace(alpha.clone());
        let reference = |owner: &IdentityOwner, key| {
            IdentitySpecReference::new(
                owner,
                key,
                "fingerprint",
                "issuer",
                "fixed_token",
                audience(),
            )
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
        let owner = IdentityOwner::for_user(Principal::local());
        for fields in [
            (" ", "issuer", "fixed_token"),
            ("fingerprint", "\t", "fixed_token"),
            ("fingerprint", "issuer", "\n"),
            ("fingerprint", "issuer", "unknown"),
        ] {
            IdentitySpecReference::new(
                &owner,
                IdentitySpecKey::global("token").expect("key"),
                fields.0,
                fields.1,
                fields.2,
                audience(),
            )
            .expect_err("blank required spec-reference field must be rejected");
        }
    }

    #[test]
    fn identity_audiences_are_typed_normalized_and_legacy_aware() {
        let normalized = IdentityAudience::new("API.EXAMPLE.COM", Some(443)).expect("audience");
        assert_eq!(normalized.host(), "api.example.com");
        assert_eq!(normalized.port(), Some(443));
        for (host, port) in [
            ("", None),
            ("https://api.example.com", None),
            ("api.example.com", Some(0)),
        ] {
            IdentityAudience::new(host, port).expect_err("invalid audience");
        }

        let owner = IdentityOwner::for_user(Principal::local());
        let stored = |host: Option<&str>, port| {
            IdentitySpecReference::from_storage_parts(
                &owner,
                None,
                "token",
                "fingerprint".to_string(),
                "issuer".to_string(),
                "fixed_token".to_string(),
                host.map(str::to_string),
                port,
            )
        };
        assert!(
            stored(None, None)
                .expect("legacy reference")
                .audience()
                .is_none()
        );
        let known = stored(Some("api.example.com"), Some(8443)).expect("known reference");
        assert_eq!(known.audience().expect("audience").port(), Some(8443));
        for (host, port) in [
            (None, Some(443)),
            (Some("API.EXAMPLE.COM"), None),
            (Some("bad host"), None),
            (Some("api.example.com"), Some(0)),
            (Some("api.example.com"), Some(-1)),
            (Some("api.example.com"), Some(65_536)),
        ] {
            stored(host, port).expect_err("corrupt stored audience");
        }
    }

    #[test]
    fn persisted_identity_parts_reject_corrupt_or_non_normalized_values() {
        IdentityName::from_storage(" alpha").expect_err("non-normalized name");
        IdentityName::from_storage("bad/name").expect_err("unsafe name");
        IdentityOwner::from_storage_parts("unknown", "local", None).expect_err("unknown owner");
        IdentityOwner::from_storage_parts("user", " member", None)
            .expect_err("non-normalized user");
        IdentityOwner::from_storage_parts("user", "local", Some("local"))
            .expect_err("user workspace column");
        IdentityOwner::from_storage_parts("workspace", "alpha", Some("beta"))
            .expect_err("mismatched workspace columns");

        let owner = IdentityOwner::for_user(Principal::local());
        IdentitySpecReference::from_storage_parts(
            &owner,
            Some("alpha"),
            "token",
            "fingerprint".to_string(),
            "issuer".to_string(),
            "fixed_token".to_string(),
            None,
            None,
        )
        .expect_err("user cannot reference a workspace spec");
    }

    fn audience() -> IdentityAudience {
        IdentityAudience::new("api.example.com", Some(443)).expect("audience")
    }
}
