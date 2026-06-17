//! App-owned identity context, binding, and runtime identity helpers.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use coral_engine::{RequestIdentityResolutionContext, RequestIdentityResolverError};
use coral_spec::v4::IdentityRequirements;
use reqwest::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bootstrap::AppError;

pub(crate) fn parse_path_segment(kind: &str, value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidInput(format!("missing {kind} name")));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(AppError::InvalidInput(format!(
            "{kind} name must not contain '/' or '\\\\'"
        )));
    }
    if trimmed == "." || trimmed == ".." {
        return Err(AppError::InvalidInput(format!(
            "{kind} name must not be '.' or '..'"
        )));
    }
    Ok(trimmed.to_string())
}

fn parse_accepted_identity_id(value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidInput(
            "missing accepted identity id".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

/// Stable local user used by single-user local mode.
pub(crate) const LOCAL_MEMBER_ID: &str = "local";

/// Request-scoped user principal selected by the app transport layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPrincipal {
    user_id: String,
}

impl UserPrincipal {
    /// Builds the default single-user local principal.
    #[must_use]
    pub fn local() -> Self {
        Self {
            user_id: LOCAL_MEMBER_ID.to_string(),
        }
    }

    /// Builds a principal for a validated user id.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the user id is empty, contains leading or
    /// trailing whitespace, contains path separators, or aliases the reserved
    /// local single-user sentinel.
    pub fn for_user(user_id: &str) -> Result<Self, AppError> {
        if user_id != user_id.trim() {
            return Err(AppError::InvalidInput(
                "user id must not contain leading or trailing whitespace".to_string(),
            ));
        }
        let user_id = parse_path_segment("user", user_id)?;
        if user_id == LOCAL_MEMBER_ID {
            return Err(AppError::InvalidInput(format!(
                "user id '{LOCAL_MEMBER_ID}' is reserved for single-user local mode"
            )));
        }
        Ok(Self { user_id })
    }

    /// Returns the validated user id.
    #[must_use]
    pub fn user_id(&self) -> &str {
        &self.user_id
    }
}

/// Server-side provider for request user principals.
///
/// The OSS provider always returns [`UserPrincipal::local`]. Product runtimes
/// can install a provider that authenticates inbound metadata and returns the
/// corresponding user principal.
#[tonic::async_trait]
pub trait UserPrincipalProvider: Send + Sync + std::fmt::Debug {
    /// Returns the user principal for one inbound gRPC request.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when transport metadata is malformed, the provider
    /// cannot authenticate the request, or principal selection fails.
    async fn principal_for_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, AppError>;
}

/// Default OSS principal provider for single-user local mode.
#[derive(Debug, Default)]
pub struct SingleUserPrincipalProvider;

#[tonic::async_trait]
impl UserPrincipalProvider for SingleUserPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, AppError> {
        Ok(UserPrincipal::local())
    }
}

/// Scope that owns configured provider-facing source identity material.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceIdentityOwner {
    /// Identity material is owned by the current Coral user principal.
    User,
    /// Identity material is owned by the workspace and independent of a user principal.
    Workspace,
}

impl SourceIdentityOwner {
    /// Returns the stable config representation for this owner.
    #[must_use]
    pub const fn as_config_value(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Workspace => "workspace",
        }
    }
}

/// Subject used to select provider-facing source identity material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceIdentitySubject {
    /// Identity material is owned by the request user principal.
    User(String),
    /// Identity material is owned by the workspace, not a user principal.
    Workspace,
}

impl SourceIdentitySubject {
    /// Selects the runtime subject for a configured source identity owner.
    #[must_use]
    pub fn for_binding_owner(owner: SourceIdentityOwner, user_principal: &UserPrincipal) -> Self {
        match owner {
            SourceIdentityOwner::User => Self::User(user_principal.user_id().to_string()),
            SourceIdentityOwner::Workspace => Self::Workspace,
        }
    }

    /// Returns the selected user id for user-owned identity material.
    #[must_use]
    pub fn user_id(&self) -> Option<&str> {
        match self {
            Self::User(user_id) => Some(user_id),
            Self::Workspace => None,
        }
    }

    /// Returns whether identity material is workspace-owned.
    #[must_use]
    pub const fn is_workspace(&self) -> bool {
        matches!(self, Self::Workspace)
    }
}

/// Workspace config binding from one source-local surface to an identity slot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceIdentityBinding {
    /// Whether the identity is user-specific or workspace-owned.
    pub owner: SourceIdentityOwner,
    /// Workspace-owned identity reference understood by installed identity
    /// providers.
    ///
    /// User-owned bindings intentionally leave this empty because the concrete
    /// identity selection is per Coral user principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    /// Workspace-owned accepted identity id from the surface's
    /// `identity_requirements`.
    ///
    /// User-owned bindings intentionally leave this empty because the concrete
    /// accepted branch is per Coral user principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_identity: Option<String>,
}

impl SourceIdentityBinding {
    /// Builds one validated user-owned source identity slot.
    #[must_use]
    pub const fn user_owned() -> Self {
        Self {
            owner: SourceIdentityOwner::User,
            identity: None,
            accepted_identity: None,
        }
    }

    /// Builds one validated workspace-owned source identity binding.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the identity reference is empty or contains a
    /// path separator, or if the accepted identity id is empty.
    pub fn workspace_owned(
        identity: impl Into<String>,
        accepted_identity: Option<String>,
    ) -> Result<Self, AppError> {
        let identity = parse_path_segment("identity", &identity.into())?;
        let accepted_identity = accepted_identity
            .map(|accepted_identity| parse_accepted_identity_id(&accepted_identity))
            .transpose()?;
        Ok(Self {
            owner: SourceIdentityOwner::Workspace,
            identity: Some(identity),
            accepted_identity,
        })
    }

    /// Validates a binding loaded from config.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the identity reference is empty or contains a
    /// path separator, or if the accepted identity id is empty.
    pub fn validate(&self) -> Result<(), AppError> {
        match self.owner {
            SourceIdentityOwner::User => {
                if self.identity.is_some() || self.accepted_identity.is_some() {
                    return Err(AppError::InvalidInput(
                        "user-owned source identity bindings store only owner; identity and accepted_identity are selected per user".to_string(),
                    ));
                }
            }
            SourceIdentityOwner::Workspace => {
                let Some(identity) = &self.identity else {
                    return Err(AppError::InvalidInput(
                        "workspace-owned source identity binding is missing identity".to_string(),
                    ));
                };
                parse_path_segment("identity", identity)?;
                if let Some(accepted_identity) = &self.accepted_identity {
                    parse_accepted_identity_id(accepted_identity)?;
                }
            }
        }
        Ok(())
    }
}

/// Concrete identity selected for one source-local surface.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceIdentitySelection {
    /// Stable identity reference understood by installed identity providers.
    pub identity: String,
    /// Optional accepted identity id from the surface's `identity_requirements`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_identity: Option<String>,
}

impl SourceIdentitySelection {
    /// Builds one validated source identity selection.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the identity reference is empty or contains a
    /// path separator, or if the accepted identity id is empty.
    pub fn new(
        identity: impl Into<String>,
        accepted_identity: Option<String>,
    ) -> Result<Self, AppError> {
        let identity = parse_path_segment("identity", &identity.into())?;
        let accepted_identity = accepted_identity
            .map(|accepted_identity| parse_accepted_identity_id(&accepted_identity))
            .transpose()?;
        Ok(Self {
            identity,
            accepted_identity,
        })
    }

    /// Validates a selection loaded from storage.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the identity reference is empty or contains a
    /// path separator, or if the accepted identity id is empty.
    pub fn validate(&self) -> Result<(), AppError> {
        parse_path_segment("identity", &self.identity)?;
        if let Some(accepted_identity) = &self.accepted_identity {
            parse_accepted_identity_id(accepted_identity)?;
        }
        Ok(())
    }
}

/// Request to resolve a user-specific source identity selection.
#[derive(Debug, Clone)]
pub struct SourceIdentitySelectionRequest {
    /// Workspace selected by the request.
    pub workspace_name: String,
    /// Subject selected by Coral for this binding; user-owned bindings carry
    /// the request user id (`subject.user_id()`).
    pub subject: SourceIdentitySubject,
    /// Source/schema name whose surface needs identity material.
    pub source_name: String,
    /// DSL v4 surface id.
    pub surface_id: String,
    /// Workspace source-surface identity binding.
    pub binding: SourceIdentityBinding,
}

/// Request to resolve one configured source identity binding into runtime material.
#[derive(Debug, Clone)]
pub struct SourceIdentityResolutionRequest {
    /// Workspace selected by the request.
    pub workspace_name: String,
    /// Subject selected by Coral for this binding.
    ///
    /// User-owned bindings carry the request user id. Workspace-owned bindings
    /// intentionally carry no request user id, so providers cannot accidentally
    /// select user-scoped material for a workspace identity.
    pub subject: SourceIdentitySubject,
    /// Source/schema name whose surface needs identity material.
    pub source_name: String,
    /// DSL v4 surface id.
    pub surface_id: String,
    /// Configured source-surface identity binding.
    pub binding: SourceIdentityBinding,
    /// Concrete source-surface identity selection.
    pub selection: SourceIdentitySelection,
    /// The workspace binding's selected identity requirements.
    pub identity_requirements: IdentityRequirements,
}

/// Provider that resolves configured Coral identity bindings into runtime material.
#[tonic::async_trait]
pub trait SourceIdentityProvider: Send + Sync + fmt::Debug {
    /// Resolves one user-owned source identity selection.
    ///
    /// Providers return `Ok(None)` when they do not own the requested
    /// selection scope. Workspace-owned bindings are resolved directly from the
    /// workspace binding and do not call providers for selection.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the provider owns the requested selection but
    /// cannot load or validate the selected identity.
    async fn resolve_source_identity_selection(
        &self,
        _request: &SourceIdentitySelectionRequest,
    ) -> Result<Option<SourceIdentitySelection>, AppError> {
        Ok(None)
    }

    /// Resolves one source identity binding.
    ///
    /// Providers return `Ok(None)` when they do not own the requested identity
    /// reference. Coral tries providers in registration order and fails if none
    /// can resolve a required binding.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the provider owns the requested identity but
    /// cannot load, validate, or materialize it.
    async fn resolve_source_identity(
        &self,
        request: &SourceIdentityResolutionRequest,
    ) -> Result<Option<Arc<dyn RuntimeSourceIdentity>>, AppError>;
}

/// Runtime identity selected by Coral for one source surface.
#[tonic::async_trait]
pub trait RuntimeSourceIdentity: Send + Sync + fmt::Debug {
    /// Installed identity spec id that describes this identity.
    fn identity_spec_id(&self) -> &str;

    /// Candidate audience claims for requirement matching.
    fn audience(&self) -> &BTreeMap<String, Value>;

    /// Returns headers to append to the outbound HTTP request.
    ///
    /// # Errors
    ///
    /// Returns [`RequestIdentityResolverError`] when request-scoped input is
    /// invalid or identity material cannot satisfy the request.
    async fn resolve_headers(
        &self,
        identity: &RequestIdentityResolutionContext,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError>;
}

#[cfg(test)]
mod tests {
    use super::{
        LOCAL_MEMBER_ID, SourceIdentityBinding, SourceIdentityOwner, SourceIdentitySelection,
        SourceIdentitySubject, UserPrincipal, parse_path_segment,
    };

    #[test]
    fn rejects_empty_names() {
        let error = parse_path_segment("source", "   ").expect_err("empty name should fail");
        assert!(error.to_string().contains("missing source name"));
    }

    #[test]
    fn rejects_path_separators() {
        let error = parse_path_segment("workspace", r"bad\name").expect_err("slash should fail");
        assert!(
            error
                .to_string()
                .contains("workspace name must not contain '/' or '\\\\'")
        );
    }

    #[test]
    fn rejects_dot_segments() {
        let error = parse_path_segment("source", "..").expect_err("dot segment should fail");
        assert!(
            error
                .to_string()
                .contains("source name must not be '.' or '..'")
        );
    }

    #[test]
    fn user_principal_rejects_surrounding_whitespace() {
        let error = UserPrincipal::for_user(" saul").expect_err("leading whitespace should fail");

        assert!(
            error
                .to_string()
                .contains("user id must not contain leading or trailing whitespace")
        );
    }

    #[test]
    fn user_principal_preserves_valid_id() {
        let principal = UserPrincipal::for_user("saul").expect("valid user");

        assert_eq!(principal.user_id(), "saul");
    }

    #[test]
    fn user_principal_rejects_reserved_local_sentinel() {
        let principal = UserPrincipal::for_user("saul").expect("named user");
        assert_eq!(principal.user_id(), "saul");

        let error = UserPrincipal::for_user(LOCAL_MEMBER_ID).expect_err("local id is reserved");
        assert!(
            error
                .to_string()
                .contains("reserved for single-user local mode"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn source_identity_subject_carries_user_only_for_user_owned_bindings() {
        let user_principal = UserPrincipal::for_user("saul").expect("named user");

        assert_eq!(
            SourceIdentitySubject::for_binding_owner(SourceIdentityOwner::User, &user_principal),
            SourceIdentitySubject::User("saul".to_string())
        );
        assert_eq!(
            SourceIdentitySubject::for_binding_owner(
                SourceIdentityOwner::Workspace,
                &user_principal,
            ),
            SourceIdentitySubject::Workspace
        );
    }

    #[test]
    fn source_identity_binding_allows_manifest_accepted_identity_ids() {
        let binding =
            SourceIdentityBinding::workspace_owned("github-workspace", Some("oauth/github".into()))
                .expect("manifest accepted identity ids are not path segments");

        assert_eq!(binding.identity.as_deref(), Some("github-workspace"));
        assert_eq!(binding.accepted_identity.as_deref(), Some("oauth/github"));
        binding
            .validate()
            .expect("accepted identity id should validate");
    }

    #[test]
    fn source_identity_selection_allows_manifest_accepted_identity_ids() {
        let selection = SourceIdentitySelection::new("github-user", Some("oauth/github".into()))
            .expect("manifest accepted identity ids are not path segments");

        assert_eq!(selection.identity, "github-user");
        assert_eq!(selection.accepted_identity.as_deref(), Some("oauth/github"));
        selection
            .validate()
            .expect("accepted identity id should validate");
    }

    #[test]
    fn source_identity_binding_rejects_empty_accepted_identity_ids() {
        let error = SourceIdentityBinding::workspace_owned("github-workspace", Some("   ".into()))
            .expect_err("accepted identity id should be non-empty");

        assert!(
            error.to_string().contains("missing accepted identity id"),
            "unexpected error: {error}"
        );
    }
}
