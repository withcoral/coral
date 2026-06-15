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

/// Collects `(key, value)` inputs into a map, validating each key and rejecting
/// duplicates. `label` names the input kind in errors.
pub(crate) fn unique_input_map(
    inputs: impl IntoIterator<Item = (String, String)>,
    label: &str,
) -> Result<BTreeMap<String, String>, AppError> {
    let mut values = BTreeMap::new();
    for (key, value) in inputs {
        let key = normalize_input_key(label, &key)?;
        if values.insert(key.clone(), value).is_some() {
            return Err(AppError::InvalidInput(format!(
                "{label} '{key}' is repeated"
            )));
        }
    }
    Ok(values)
}

fn normalize_input_key(label: &str, value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidInput(format!("missing {label} key")));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(AppError::InvalidInput(format!(
            "{label} key must not contain '/' or '\\\\'"
        )));
    }
    if trimmed.contains('=') || trimmed.contains('\n') || trimmed.contains('\r') {
        return Err(AppError::InvalidInput(format!(
            "{label} key must not contain '=', '\\n', or '\\r'"
        )));
    }
    if trimmed.starts_with('#') {
        return Err(AppError::InvalidInput(format!(
            "{label} key must not start with '#'"
        )));
    }
    Ok(trimmed.to_string())
}

pub(crate) fn parse_path_segment(kind: &str, value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidInput(format!("missing {kind} name")));
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains(':') {
        return Err(AppError::InvalidInput(format!(
            "{kind} name must not contain '/', '\\\\', or ':'"
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
    /// Returns [`AppError`] if the user id is empty, contains path separators,
    /// or aliases the reserved local single-user sentinel.
    pub fn for_user(user_id: &str) -> Result<Self, AppError> {
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

impl Default for UserPrincipal {
    fn default() -> Self {
        Self::local()
    }
}

/// Errors raised while authenticating or selecting a request user principal.
#[derive(Debug, thiserror::Error)]
pub enum UserPrincipalError {
    /// The request did not present valid authentication.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    /// The request presented malformed identity metadata.
    #[error("invalid user principal metadata: {0}")]
    InvalidInput(String),
    /// The principal provider failed unexpectedly.
    #[error("user principal provider failed: {0}")]
    Internal(String),
}

impl UserPrincipalError {
    /// Builds an unauthenticated principal error.
    #[must_use]
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::Unauthenticated(message.into())
    }

    /// Builds an invalid-input principal error.
    #[must_use]
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    /// Builds an internal principal provider error.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

/// Server-side provider for request user principals.
///
/// The OSS provider always returns [`UserPrincipal::local`]. Product runtimes
/// can install a provider that authenticates inbound metadata and returns the
/// corresponding user principal.
#[tonic::async_trait]
pub trait UserPrincipalProvider: Send + Sync + std::fmt::Debug {
    /// Returns true for Coral's default local single-user provider.
    ///
    /// Server startup uses this to prevent accidental public network exposure
    /// with OSS local defaults. Product providers should use the default
    /// implementation.
    fn is_default_single_user_provider(&self) -> bool {
        false
    }

    /// Returns the user principal for one inbound gRPC request.
    ///
    /// # Errors
    ///
    /// Returns [`UserPrincipalError`] when transport metadata is malformed or
    /// the provider cannot authenticate the request.
    async fn principal_for_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalError>;
}

/// Default OSS principal provider for single-user local mode.
#[derive(Debug, Default)]
pub struct SingleUserPrincipalProvider;

#[tonic::async_trait]
impl UserPrincipalProvider for SingleUserPrincipalProvider {
    fn is_default_single_user_provider(&self) -> bool {
        true
    }

    async fn principal_for_metadata(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalError> {
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
    pub(crate) fn as_config_value(self) -> &'static str {
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
    pub(crate) fn for_binding_owner(
        owner: SourceIdentityOwner,
        user_principal: &UserPrincipal,
    ) -> Self {
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
    /// Returns [`AppError`] if the identity reference or accepted identity id is
    /// empty or contains a path separator.
    pub fn workspace_owned(
        identity: impl Into<String>,
        accepted_identity: Option<String>,
    ) -> Result<Self, AppError> {
        let identity = parse_path_segment("identity", &identity.into())?;
        let accepted_identity = accepted_identity
            .map(|accepted_identity| parse_path_segment("accepted identity", &accepted_identity))
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
    /// Returns [`AppError`] if the identity reference or accepted identity id is
    /// empty or contains a path separator.
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
                    parse_path_segment("accepted identity", accepted_identity)?;
                }
            }
        }
        Ok(())
    }

    fn workspace_selection(&self) -> Result<SourceIdentitySelection, AppError> {
        if self.owner != SourceIdentityOwner::Workspace {
            return Err(AppError::InvalidInput(
                "user-owned source identity selection must be resolved per user".to_string(),
            ));
        }
        SourceIdentitySelection::new(
            self.identity.clone().ok_or_else(|| {
                AppError::InvalidInput(
                    "workspace-owned source identity binding is missing identity".to_string(),
                )
            })?,
            self.accepted_identity.clone(),
        )
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
    /// Returns [`AppError`] if the identity reference or accepted identity id is
    /// empty or contains a path separator.
    pub fn new(
        identity: impl Into<String>,
        accepted_identity: Option<String>,
    ) -> Result<Self, AppError> {
        let identity = parse_path_segment("identity", &identity.into())?;
        let accepted_identity = accepted_identity
            .map(|accepted_identity| parse_path_segment("accepted identity", &accepted_identity))
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
    /// Returns [`AppError`] if the identity reference or accepted identity id is
    /// empty or contains a path separator.
    pub fn validate(&self) -> Result<(), AppError> {
        parse_path_segment("identity", &self.identity)?;
        if let Some(accepted_identity) = &self.accepted_identity {
            parse_path_segment("accepted identity", accepted_identity)?;
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
    async fn resolve_headers(
        &self,
        identity: &RequestIdentityResolutionContext,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError>;
}

/// App-owned identity manager. It owns binding resolution policy; providers
/// only supply material for an already-selected binding.
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

    pub(crate) async fn resolve_source_identity_selection(
        &self,
        request: SourceIdentitySelectionRequest,
    ) -> Result<SourceIdentitySelection, AppError> {
        if request.binding.owner == SourceIdentityOwner::Workspace {
            return request.binding.workspace_selection();
        }
        for provider in self.providers.iter() {
            if let Some(selection) = provider.resolve_source_identity_selection(&request).await? {
                return Ok(selection);
            }
        }
        Err(AppError::FailedPrecondition(format!(
            "no source identity provider resolved user-owned selection for source '{}' surface '{}' and user '{}'",
            request.source_name,
            request.surface_id,
            request.subject.user_id().unwrap_or("<unresolved>")
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
    use super::{
        LOCAL_MEMBER_ID, SourceIdentityOwner, SourceIdentitySubject, UserPrincipal,
        parse_path_segment, unique_input_map,
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
                .contains("workspace name must not contain '/', '\\\\', or ':'")
        );
    }

    #[test]
    fn rejects_windows_drive_prefixes() {
        let error = parse_path_segment("identity", "C:foo")
            .expect_err("windows drive-prefixed segment should fail");
        assert!(
            error
                .to_string()
                .contains("identity name must not contain '/', '\\\\', or ':'")
        );
    }

    #[test]
    fn input_keys_allow_colons_but_reject_duplicates() {
        let values = unique_input_map(
            [("tenant:id".to_string(), "tenant-a".to_string())],
            "credential input",
        )
        .expect("colon input key");
        assert_eq!(
            values.get("tenant:id").map(String::as_str),
            Some("tenant-a")
        );

        let error = unique_input_map(
            [
                ("tenant:id".to_string(), "tenant-a".to_string()),
                ("tenant:id".to_string(), "tenant-b".to_string()),
            ],
            "credential input",
        )
        .expect_err("duplicate input should fail");
        assert!(
            error
                .to_string()
                .contains("credential input 'tenant:id' is repeated")
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
}
