//! Installed-source domain model for the application management plane.

use std::collections::BTreeMap;

use coral_spec::ManifestInputSpec;
use serde::{Deserialize, Serialize};

use crate::credentials::CredentialStorageKind;
use crate::identity::SourceIdentityBinding;
use crate::sources::SourceName;

/// App-owned description of a source candidate that can be installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateSource {
    pub(crate) name: SourceName,
    pub(crate) description: String,
    pub(crate) version: Option<String>,
    pub(crate) inputs: Vec<ManifestInputSpec>,
    pub(crate) installed: bool,
    pub(crate) origin: SourceOrigin,
    pub(crate) credential_storage: Option<CredentialStorageKind>,
}

/// App-owned model for one source installed in a workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstalledSource {
    /// Bare source name. This is also the visible SQL schema name.
    pub(crate) name: SourceName,
    /// Authored source-spec id declared by the imported manifest.
    ///
    /// When absent, the installed source name is also the source-spec id. This
    /// preserves older local config records and bundled sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source_spec_id: Option<String>,
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
    /// Source-local DSL v4 surface identity bindings for this workspace.
    #[serde(default)]
    pub(crate) identity_bindings: BTreeMap<String, SourceIdentityBinding>,
    /// Where this installed source came from.
    pub(crate) origin: SourceOrigin,
}

impl InstalledSource {
    pub(crate) fn source_spec_id(&self) -> &str {
        self.source_spec_id
            .as_deref()
            .unwrap_or_else(|| self.name.as_str())
    }

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
