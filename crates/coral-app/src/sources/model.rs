//! Installed-source domain model for the application management plane.

use std::collections::{BTreeMap, BTreeSet};

use coral_spec::{AuthSpec, ManifestInputSpec, ValidatedSourceManifest, ValueSourceSpec};
use serde::{Deserialize, Serialize};

use crate::credentials::CredentialStorageKind;
use crate::sources::SourceName;

/// App-owned description of a source candidate that can be installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateSource {
    pub(crate) name: SourceName,
    pub(crate) description: String,
    pub(crate) version: String,
    pub(crate) inputs: Vec<ManifestInputSpec>,
    pub(crate) auth_one_of_secret_requirements: Vec<AuthOneOfSecretRequirement>,
    pub(crate) installed: bool,
    pub(crate) origin: SourceOrigin,
    pub(crate) credential_storage: Option<CredentialStorageKind>,
}

/// Install-time invariant for an auth `from: one_of` whose branches are source
/// secrets. At least one key must be supplied or already stored for auth to be
/// usable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthOneOfSecretRequirement {
    /// Human-readable auth location for error messages.
    pub(crate) context: String,
    /// Secret input keys that can satisfy the auth choice, in runtime order.
    pub(crate) keys: Vec<String>,
}

impl AuthOneOfSecretRequirement {
    pub(crate) fn from_manifest(manifest: &ValidatedSourceManifest) -> Vec<Self> {
        let declared_secret_names = manifest.declared_secret_names();
        let Some(http) = manifest.as_http() else {
            return Vec::new();
        };
        let AuthSpec::HeaderAuth(auth) = &http.auth else {
            return Vec::new();
        };

        auth.headers
            .iter()
            .filter_map(|header| {
                Self::from_value_source(
                    format!("auth header '{}'", header.name),
                    &header.value,
                    &declared_secret_names,
                )
            })
            .collect()
    }

    fn from_value_source(
        context: String,
        value: &ValueSourceSpec,
        declared_secret_names: &BTreeSet<String>,
    ) -> Option<Self> {
        let ValueSourceSpec::OneOf { values } = value else {
            return None;
        };
        if values.is_empty() {
            return None;
        }

        let mut keys = Vec::new();
        for value in values {
            let key = match value {
                ValueSourceSpec::Input { key } | ValueSourceSpec::Bearer { key }
                    if declared_secret_names.contains(key) =>
                {
                    key
                }
                _ => return None,
            };
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
        if keys.is_empty() {
            return None;
        }
        Some(Self { context, keys })
    }
}

/// App-owned model for one source installed in a workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstalledSource {
    /// Bare source name. This is also the visible SQL schema name.
    pub(crate) name: SourceName,
    /// Persisted manifest version when it should live in app state.
    ///
    /// Bundled sources resolve their manifest directly from the compiled-in
    /// catalog, so they do not persist a duplicate version string in config.
    #[serde(default)]
    pub(crate) version: Option<String>,
    /// Configured non-secret variable bindings.
    #[serde(default)]
    pub(crate) variables: BTreeMap<String, String>,
    /// Logical secret keys referenced by this source.
    #[serde(default)]
    pub(crate) secrets: Vec<String>,
    /// Storage backend that owns this source's credential material.
    ///
    /// `None` means a legacy pre-keychain install, which is treated as file
    /// storage until the source is removed and re-added.
    #[serde(default)]
    pub(crate) credential_storage: Option<CredentialStorageKind>,
    /// Where this installed source came from.
    pub(crate) origin: SourceOrigin,
}

impl InstalledSource {
    pub(crate) fn effective_credential_storage(&self) -> CredentialStorageKind {
        self.credential_storage
            .unwrap_or(CredentialStorageKind::File)
    }

    pub(crate) fn credential_storage_for_material(&self) -> Option<CredentialStorageKind> {
        if self.secrets.is_empty() {
            None
        } else {
            Some(self.effective_credential_storage())
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceOrigin {
    Bundled,
    Imported,
}

impl SourceOrigin {
    pub(crate) fn as_config_value(self) -> &'static str {
        match self {
            Self::Bundled => "bundled",
            Self::Imported => "imported",
        }
    }
}
