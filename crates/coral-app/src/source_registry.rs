//! Public source registry extension seam.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::bootstrap::AppError;
use crate::credentials::CredentialStorageKind;
use crate::identity::SourceIdentityBinding;
use crate::sources::SourceName;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::workspaces::WorkspaceName;

/// Validated identity of one source-spec manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpecManifestMetadata {
    /// Stable source-spec id declared by the manifest.
    pub source_spec_id: String,
    /// Declared source-spec DSL version.
    pub dsl_version: u32,
    /// Authored source-spec version, when present.
    pub version: Option<String>,
}

/// Validates one source-spec manifest and returns its registry identity.
///
/// # Errors
///
/// Returns [`AppError`] when the manifest is invalid.
pub fn source_spec_manifest_metadata(
    manifest_yaml: &str,
) -> Result<SourceSpecManifestMetadata, AppError> {
    let manifest = coral_spec::parse_source_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(SourceSpecManifestMetadata {
        source_spec_id: manifest.schema_name().to_string(),
        dsl_version: manifest.dsl_version(),
        version: manifest.source_version().map(str::to_string),
    })
}

/// Parsed source/identity spec bundle ready for global registry import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecBundleManifest {
    /// Source spec document from the bundle.
    pub source_spec: SpecBundleSourceSpec,
    /// Identity spec documents from the bundle.
    pub identity_specs: Vec<SpecBundleIdentitySpec>,
}

/// Source spec document in a parsed spec bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecBundleSourceSpec {
    /// Stable source-spec id declared by the manifest.
    pub source_spec_id: String,
    /// Declared source-spec DSL version.
    pub dsl_version: u32,
    /// Authored source-spec version, when present.
    pub version: Option<String>,
    /// Canonical source manifest YAML.
    pub manifest_yaml: String,
}

/// Identity spec document in a parsed spec bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecBundleIdentitySpec {
    /// Stable identity-spec id declared by the manifest.
    pub identity_spec_id: String,
    /// Authored identity-spec version.
    pub version: String,
    /// Canonical identity manifest YAML.
    pub manifest_yaml: String,
}

/// Kind of identity-spec setup input declared by an imported bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleIdentityInputKind {
    /// Non-secret input material.
    Variable,
    /// Secret input material.
    Secret,
}

/// Promptable identity-spec setup input declared by an imported bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleIdentityInputSpec {
    /// Identity spec that owns the input.
    pub identity_spec_id: String,
    /// Input key declared by the identity spec.
    pub key: String,
    /// Input kind.
    pub kind: BundleIdentityInputKind,
    /// Whether this input must be present after defaults and existing material are resolved.
    pub required: bool,
    /// Authored default value for variable inputs, if any.
    pub default_value: String,
    /// Optional authored prompt hint.
    pub hint: Option<String>,
}

/// Error returned while discovering identity-spec inputs from a bundle.
#[derive(Debug, thiserror::Error)]
pub enum BundleIdentityInputDiscoveryError {
    /// Bundle YAML failed to parse or validate.
    #[error("spec bundle is invalid: {0}")]
    Invalid(String),
}

/// Parses a source/identity spec bundle and returns canonical documents.
///
/// # Errors
///
/// Returns [`AppError`] when the bundle is invalid.
pub fn parse_spec_bundle_manifest_yaml(raw: &str) -> Result<SpecBundleManifest, AppError> {
    let bundle = coral_spec::parse_manifest_bundle_yaml(raw)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(SpecBundleManifest {
        source_spec: SpecBundleSourceSpec {
            source_spec_id: bundle.source_manifest.schema_name().to_string(),
            dsl_version: bundle.source_manifest.dsl_version(),
            version: bundle.source_manifest.source_version().map(str::to_string),
            manifest_yaml: bundle.source_manifest_yaml,
        },
        identity_specs: bundle
            .identity_manifests
            .into_iter()
            .map(|identity| SpecBundleIdentitySpec {
                identity_spec_id: identity.manifest.name,
                version: identity.manifest.version,
                manifest_yaml: identity.manifest_yaml,
            })
            .collect(),
    })
}

/// Discover identity-spec setup inputs declared by a source/identity bundle.
///
/// # Errors
///
/// Returns [`BundleIdentityInputDiscoveryError`] when the bundle YAML is invalid.
pub fn bundle_identity_inputs_from_yaml(
    bundle_yaml: &str,
) -> Result<Vec<BundleIdentityInputSpec>, BundleIdentityInputDiscoveryError> {
    let bundle = coral_spec::parse_manifest_bundle_yaml(bundle_yaml)
        .map_err(|error| BundleIdentityInputDiscoveryError::Invalid(error.to_string()))?;
    Ok(bundle
        .identity_manifests
        .into_iter()
        .flat_map(|identity| {
            let identity_spec_id = identity.manifest.name;
            identity.manifest.inputs.into_iter().map(move |input| {
                let kind = match input.kind {
                    coral_spec::ManifestInputKind::Variable => BundleIdentityInputKind::Variable,
                    coral_spec::ManifestInputKind::Secret => BundleIdentityInputKind::Secret,
                };
                BundleIdentityInputSpec {
                    identity_spec_id: identity_spec_id.clone(),
                    key: input.key,
                    kind,
                    required: input.required,
                    default_value: input.default_value,
                    hint: input.hint,
                }
            })
        })
        .collect())
}

/// Durable storage backend for workspace-installed sources.
///
/// The default implementation persists source records in local `config.toml`.
/// Product runtimes can install an implementation backed by their own durable
/// control-plane store.
pub trait SourceRegistry: fmt::Debug + Send + Sync + 'static {
    /// Lists all sources installed in one workspace.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the registry cannot be read or contains invalid
    /// source records.
    fn list_workspace_sources(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<SourceRegistryRecord>, AppError>;

    /// Fetches one installed source, returning `None` when it is absent.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the registry cannot be read or contains an
    /// invalid source record.
    fn get_source(
        &self,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<Option<SourceRegistryRecord>, AppError>;

    /// Inserts or replaces one installed source.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the record is invalid or cannot be persisted.
    fn upsert_source(&self, record: SourceRegistryRecord) -> Result<(), AppError>;

    /// Removes one installed source if present.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the source cannot be removed.
    fn remove_source(&self, workspace_id: &str, source_name: &str) -> Result<(), AppError>;
}

/// One source installed in a workspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRegistryRecord {
    /// Workspace id that owns this source.
    pub workspace_id: String,
    /// Installed source name.
    pub source_name: String,
    /// Persisted source version, when applicable.
    pub version: Option<String>,
    /// Imported source manifest YAML.
    ///
    /// Bundled sources may leave this empty because their manifest is resolved
    /// from the bundled catalog. Imported DSL v4 manifests are not secret
    /// material and product registries can persist them here.
    pub manifest_yaml: Option<String>,
    /// Configured non-secret variable values.
    pub variables: BTreeMap<String, String>,
    /// Source-owned secret keys for legacy source specs.
    ///
    /// DSL v4 sources should leave this empty and express credentials through
    /// identity requirements.
    pub secrets: Vec<String>,
    /// Credential storage backend for legacy source-owned secrets.
    pub credential_storage: Option<SourceRegistryCredentialStorage>,
    /// Source-surface identity slot configuration.
    pub identity_bindings: BTreeMap<String, SourceIdentityBinding>,
    /// How this source entered the registry.
    pub origin: SourceRegistryOrigin,
}

/// Source origin stored by a source registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRegistryOrigin {
    /// Source was installed from the bundled source catalog.
    Bundled,
    /// Source was imported from a user-provided manifest.
    Imported,
}

/// Source-owned credential storage backend for legacy source specs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRegistryCredentialStorage {
    /// Credential material is stored in the local file store.
    File,
    /// Credential material is stored in the operating-system keychain.
    Keychain,
}

pub(crate) fn record_from_installed_source(
    workspace_name: &WorkspaceName,
    source: InstalledSource,
) -> SourceRegistryRecord {
    SourceRegistryRecord {
        workspace_id: workspace_name.as_str().to_string(),
        source_name: source.name.as_str().to_string(),
        version: source.version,
        manifest_yaml: None,
        variables: source.variables,
        secrets: source.secrets,
        credential_storage: source.credential_storage.map(Into::into),
        identity_bindings: source.identity_bindings,
        origin: source.origin.into(),
    }
}

pub(crate) fn installed_source_from_record(
    expected_workspace: &WorkspaceName,
    record: SourceRegistryRecord,
) -> Result<InstalledSource, AppError> {
    let workspace_name = WorkspaceName::parse(&record.workspace_id)?;
    if &workspace_name != expected_workspace {
        return Err(AppError::InvalidInput(format!(
            "source registry returned workspace '{workspace_name}' while loading workspace '{expected_workspace}'"
        )));
    }
    let source_name = SourceName::parse(&record.source_name)?;
    validate_registry_identity_bindings(source_name.as_str(), &record.identity_bindings)?;
    Ok(InstalledSource {
        name: source_name,
        version: record.version,
        variables: record.variables,
        secrets: record.secrets,
        credential_storage: record.credential_storage.map(Into::into),
        identity_bindings: record.identity_bindings,
        origin: record.origin.into(),
    })
}

fn validate_registry_identity_bindings(
    source_name: &str,
    bindings: &BTreeMap<String, SourceIdentityBinding>,
) -> Result<(), AppError> {
    for (surface_id, binding) in bindings {
        let mut chars = surface_id.chars();
        let valid = matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
            && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !valid {
            return Err(AppError::InvalidInput(format!(
                "source '{source_name}' identity binding surface '{surface_id}' must match [a-z][a-z0-9_]*"
            )));
        }
        binding.validate().map_err(|error| {
            AppError::InvalidInput(format!(
                "source '{source_name}' identity binding for surface '{surface_id}' is invalid: {error}"
            ))
        })?;
    }
    Ok(())
}

impl From<SourceOrigin> for SourceRegistryOrigin {
    fn from(value: SourceOrigin) -> Self {
        match value {
            SourceOrigin::Bundled => Self::Bundled,
            SourceOrigin::Imported => Self::Imported,
        }
    }
}

impl From<SourceRegistryOrigin> for SourceOrigin {
    fn from(value: SourceRegistryOrigin) -> Self {
        match value {
            SourceRegistryOrigin::Bundled => Self::Bundled,
            SourceRegistryOrigin::Imported => Self::Imported,
        }
    }
}

impl From<CredentialStorageKind> for SourceRegistryCredentialStorage {
    fn from(value: CredentialStorageKind) -> Self {
        match value {
            CredentialStorageKind::File => Self::File,
            CredentialStorageKind::Keychain => Self::Keychain,
        }
    }
}

impl From<SourceRegistryCredentialStorage> for CredentialStorageKind {
    fn from(value: SourceRegistryCredentialStorage) -> Self {
        match value {
            SourceRegistryCredentialStorage::File => Self::File,
            SourceRegistryCredentialStorage::Keychain => Self::Keychain,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BundleIdentityInputKind, bundle_identity_inputs_from_yaml};

    #[test]
    fn bundle_identity_inputs_from_yaml_discovers_identity_inputs() {
        let inputs = bundle_identity_inputs_from_yaml(&format!(
            "---\n{}---\n{}",
            source_yaml("demo"),
            oauth_identity_yaml()
        ))
        .expect("bundle inputs");

        let [tenant, client_secret] = inputs.as_slice() else {
            panic!("expected two inputs, got {inputs:?}");
        };
        assert_eq!(tenant.identity_spec_id, "demo_oauth");
        assert_eq!(tenant.key, "DEMO_TENANT");
        assert_eq!(tenant.kind, BundleIdentityInputKind::Variable);
        assert_eq!(tenant.default_value, "oauth2");
        assert_eq!(client_secret.identity_spec_id, "demo_oauth");
        assert_eq!(client_secret.key, "DEMO_OAUTH_CLIENT_SECRET");
        assert_eq!(client_secret.kind, BundleIdentityInputKind::Secret);
        assert!(client_secret.required);
    }

    fn source_yaml(name: &str) -> String {
        format!(
            "name: {name}\nversion: 0.1.0\ndsl_version: 4\nsurfaces:\n  - id: rest\n    type: openapi\n    file: /tmp/openapi.yaml\n    sha256: 0000000000000000000000000000000000000000000000000000000000000000\n"
        )
    }

    fn oauth_identity_yaml() -> &'static str {
        r"
kind: identity
spec_version: 1
name: demo_oauth
version: 0.1.0
description: Demo OAuth identity.
issuer: demo
type: oauth
audience:
  host: api.example.test
inputs:
  DEMO_TENANT:
    kind: variable
    default: oauth2
  DEMO_OAUTH_CLIENT_SECRET:
    kind: secret
    required: true
oauth:
  method:
    label: Demo OAuth
    flow:
      type: authorization_code
      pkce: required
    redirect_uri: http://127.0.0.1:53682/callback
    endpoints:
      authorization_url: https://auth.example.test/authorize
      token_url: https://auth.example.test/token
    client:
      id:
        default: demo-client
      secret:
        input: DEMO_OAUTH_CLIENT_SECRET
        transport: request_body
"
    }
}
