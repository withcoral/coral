//! App-owned identity context, binding, and runtime identity helpers.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use coral_engine::{RequestIdentityHttpAuthenticatorError, SelectedRequestIdentity};
use coral_spec::v4::IdentityRequirements;
use reqwest::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bootstrap::AppError;

/// Collects `(key, value)` inputs into a map, validating each key as a path
/// segment and rejecting duplicates. `label` names the input kind in errors.
pub(crate) fn unique_input_map(
    inputs: impl IntoIterator<Item = (String, String)>,
    label: &str,
) -> Result<BTreeMap<String, String>, AppError> {
    let mut values = BTreeMap::new();
    for (key, value) in inputs {
        let key = parse_path_segment(label, &key)?;
        if values.insert(key.clone(), value).is_some() {
            return Err(AppError::InvalidInput(format!(
                "{label} '{key}' is repeated"
            )));
        }
    }
    Ok(values)
}

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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum IdentityOwnerKind {
    /// Identity material is owned by the current Coral user principal.
    User,
    /// Identity material is owned by the workspace and independent of a user principal.
    Workspace,
}

impl IdentityOwnerKind {
    /// Returns the stable config representation for this owner.
    #[must_use]
    pub const fn as_config_value(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Workspace => "workspace",
        }
    }
}

/// Workspace config binding from one source-local surface to an identity slot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceIdentityBinding {
    /// Whether the identity is user-specific or workspace-owned.
    pub owner: IdentityOwnerKind,
    /// Workspace-owned identity reference understood by installed identity
    /// providers.
    ///
    /// User-owned bindings intentionally leave this empty because the concrete
    /// identity selection is per Coral user principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
}

impl SourceIdentityBinding {
    /// Builds one validated user-owned source identity slot.
    #[must_use]
    pub const fn user_owned() -> Self {
        Self {
            owner: IdentityOwnerKind::User,
            identity: None,
        }
    }

    /// Builds one validated workspace-owned source identity binding.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the identity reference is empty or contains a
    /// path separator.
    pub fn workspace_owned(identity: impl Into<String>) -> Result<Self, AppError> {
        let identity = parse_path_segment("identity", &identity.into())?;
        Ok(Self {
            owner: IdentityOwnerKind::Workspace,
            identity: Some(identity),
        })
    }

    /// Validates a binding loaded from config.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the identity reference is empty or contains a
    /// path separator.
    pub fn validate(&self) -> Result<(), AppError> {
        match self.owner {
            IdentityOwnerKind::User => {
                if self.identity.is_some() {
                    return Err(AppError::InvalidInput(
                        "user-owned source identity bindings store only owner; identity is selected per user".to_string(),
                    ));
                }
            }
            IdentityOwnerKind::Workspace => {
                let Some(identity) = &self.identity else {
                    return Err(AppError::InvalidInput(
                        "workspace-owned source identity binding is missing identity".to_string(),
                    ));
                };
                parse_path_segment("identity", identity)?;
            }
        }
        Ok(())
    }

    fn workspace_selection(&self) -> Result<SourceIdentitySelection, AppError> {
        if self.owner != IdentityOwnerKind::Workspace {
            return Err(AppError::InvalidInput(
                "user-owned source identity selection must be resolved per user".to_string(),
            ));
        }
        SourceIdentitySelection::new(self.identity.clone().ok_or_else(|| {
            AppError::InvalidInput(
                "workspace-owned source identity binding is missing identity".to_string(),
            )
        })?)
    }
}

/// Concrete identity selected for one source-local surface.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceIdentitySelection {
    /// Stable identity reference understood by installed identity providers.
    pub identity: String,
}

impl SourceIdentitySelection {
    /// Builds one validated source identity selection.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the identity reference is empty or contains a
    /// path separator.
    pub fn new(identity: impl Into<String>) -> Result<Self, AppError> {
        let identity = parse_path_segment("identity", &identity.into())?;
        Ok(Self { identity })
    }

    /// Validates a selection loaded from storage.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the identity reference is empty or contains a
    /// path separator.
    pub fn validate(&self) -> Result<(), AppError> {
        parse_path_segment("identity", &self.identity).map(|_| ())
    }
}

/// Request to resolve a user-specific source identity selection.
#[derive(Debug, Clone)]
pub struct SourceIdentitySelectionRequest {
    /// Workspace selected by the request.
    pub workspace_name: String,
    /// Request user id whose user-owned source identity selection is needed.
    pub user_id: String,
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
    /// Request user id selected by Coral for user-owned bindings.
    ///
    /// Workspace-owned bindings intentionally carry no request user id, so
    /// providers cannot accidentally select user-scoped material for a workspace
    /// identity.
    pub user_id: Option<String>,
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
    /// Returns [`RequestIdentityHttpAuthenticatorError`] when request-scoped
    /// input is invalid or identity material cannot satisfy the request.
    async fn resolve_headers(
        &self,
        identity: &SelectedRequestIdentity,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityHttpAuthenticatorError>;
}

/// App-owned source identity manager. It owns binding resolution policy;
/// providers only supply material for an already-selected binding.
#[derive(Clone, Default)]
pub(crate) struct IdentityManager {
    providers: Arc<Vec<Arc<dyn SourceIdentityProvider>>>,
}

impl IdentityManager {
    pub(crate) fn new(providers: Vec<Arc<dyn SourceIdentityProvider>>) -> Self {
        Self {
            providers: Arc::new(providers),
        }
    }

    pub(crate) async fn resolve_source_identity_selection(
        &self,
        request: SourceIdentitySelectionRequest,
    ) -> Result<SourceIdentitySelection, AppError> {
        if request.binding.owner == IdentityOwnerKind::Workspace {
            return request.binding.workspace_selection();
        }
        for provider in self.providers.iter() {
            if let Some(selection) = provider.resolve_source_identity_selection(&request).await? {
                selection.validate()?;
                return Ok(selection);
            }
        }
        Err(AppError::FailedPrecondition(format!(
            "no source identity provider resolved user-owned selection for source '{}' surface '{}' and user '{}'",
            request.source_name, request.surface_id, request.user_id
        )))
    }

    pub(crate) async fn resolve_source_identity(
        &self,
        request: SourceIdentityResolutionRequest,
    ) -> Result<Arc<dyn RuntimeSourceIdentity>, AppError> {
        for provider in self.providers.iter() {
            if let Some(identity) = provider.resolve_source_identity(&request).await? {
                return Ok(identity);
            }
        }
        Err(AppError::FailedPrecondition(format!(
            "no source identity provider resolved identity '{}' for source '{}' surface '{}'",
            request.selection.identity, request.source_name, request.surface_id
        )))
    }
}

impl fmt::Debug for IdentityManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdentityManager")
            .field("provider_count", &self.providers.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bootstrap::AppError;

    use super::{
        IdentityManager, LOCAL_MEMBER_ID, RuntimeSourceIdentity, SourceIdentityBinding,
        SourceIdentityProvider, SourceIdentityResolutionRequest, SourceIdentitySelection,
        SourceIdentitySelectionRequest, UserPrincipal, parse_path_segment,
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
    fn source_identity_binding_accepts_workspace_identity_reference() {
        let binding = SourceIdentityBinding::workspace_owned("github-workspace")
            .expect("workspace identity reference should validate");

        assert_eq!(binding.identity.as_deref(), Some("github-workspace"));
        binding.validate().expect("binding should validate");
    }

    #[test]
    fn source_identity_selection_accepts_identity_reference() {
        let selection = SourceIdentitySelection::new("github-user")
            .expect("identity reference should validate");

        assert_eq!(selection.identity, "github-user");
        selection.validate().expect("selection should validate");
    }

    #[derive(Debug)]
    struct InvalidSelectionProvider;

    #[tonic::async_trait]
    impl SourceIdentityProvider for InvalidSelectionProvider {
        async fn resolve_source_identity_selection(
            &self,
            _request: &SourceIdentitySelectionRequest,
        ) -> Result<Option<SourceIdentitySelection>, AppError> {
            Ok(Some(SourceIdentitySelection {
                identity: "../github".to_string(),
            }))
        }

        async fn resolve_source_identity(
            &self,
            _request: &SourceIdentityResolutionRequest,
        ) -> Result<Option<Arc<dyn RuntimeSourceIdentity>>, AppError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn identity_manager_rejects_invalid_provider_selection() {
        let manager = IdentityManager::new(vec![Arc::new(InvalidSelectionProvider)]);

        let error = manager
            .resolve_source_identity_selection(SourceIdentitySelectionRequest {
                workspace_name: "default".to_string(),
                user_id: "saul".to_string(),
                source_name: "github_v4".to_string(),
                surface_id: "rest".to_string(),
                binding: SourceIdentityBinding::user_owned(),
            })
            .await
            .expect_err("invalid provider selection should fail");

        assert!(
            error
                .to_string()
                .contains("identity name must not contain '/' or '\\\\'"),
            "unexpected error: {error}"
        );
    }
}
