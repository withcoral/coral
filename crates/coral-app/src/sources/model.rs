//! Installed-source domain model for the application management plane.

use std::collections::{BTreeMap, BTreeSet};

use coral_spec::ManifestInputSpec;
use serde::{Deserialize, Serialize};

use crate::credentials::CredentialStorageKind;
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
    /// Immutable app-assigned generated capability source id.
    #[serde(default = "default_source_id")]
    pub(crate) source_id: String,
    /// Mutable display name surfaced in discovery.
    #[serde(default)]
    pub(crate) display_name: String,
    /// Immutable source key used for generated SQL schemas and TypeScript namespaces.
    #[serde(default)]
    pub(crate) source_key: String,
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
    /// `None` is treated as file storage until the source is removed and
    /// re-added.
    #[serde(default)]
    pub(crate) credential_storage: Option<CredentialStorageKind>,
    /// Where this installed source came from.
    pub(crate) origin: SourceOrigin,
}

impl InstalledSource {
    pub(crate) fn identity_for_name(source_name: &SourceName) -> InstalledSourceIdentity {
        let source_key = source_key_seed(source_name.as_str());
        InstalledSourceIdentity {
            source_id: format!("src_{source_key}"),
            display_name: source_name.as_str().to_string(),
            source_key,
        }
    }

    pub(crate) fn allocate_identity_for_name<'a>(
        source_name: &SourceName,
        existing_sources: impl IntoIterator<Item = &'a InstalledSource>,
    ) -> InstalledSourceIdentity {
        let existing_sources = existing_sources.into_iter().collect::<Vec<_>>();
        let existing_source_keys = existing_sources
            .iter()
            .map(|source| source.source_key.as_str())
            .filter(|key| !key.is_empty())
            .collect::<BTreeSet<_>>();
        let source_key = unique_identity_component(
            &source_key_seed(source_name.as_str()),
            &existing_source_keys,
        );
        let existing_source_ids = existing_sources
            .iter()
            .map(|source| source.source_id.as_str())
            .filter(|source_id| !source_id.is_empty())
            .collect::<BTreeSet<_>>();
        let source_id =
            unique_identity_component(&format!("src_{source_key}"), &existing_source_ids);
        InstalledSourceIdentity {
            source_id,
            display_name: source_name.as_str().to_string(),
            source_key,
        }
    }

    pub(crate) fn identity(&self) -> InstalledSourceIdentity {
        InstalledSourceIdentity {
            source_id: self.source_id.clone(),
            display_name: self.display_name.clone(),
            source_key: self.source_key.clone(),
        }
    }

    pub(crate) fn with_allocated_missing_identity<'a>(
        mut self,
        existing_sources: impl IntoIterator<Item = &'a InstalledSource>,
    ) -> Self {
        if self.source_id.is_empty() || self.display_name.is_empty() || self.source_key.is_empty() {
            let identity = Self::allocate_identity_for_name(&self.name, existing_sources);
            if self.source_id.is_empty() {
                self.source_id = identity.source_id;
            }
            if self.display_name.is_empty() {
                self.display_name = identity.display_name;
            }
            if self.source_key.is_empty() {
                self.source_key = identity.source_key;
            }
        }
        self
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InstalledSourceIdentity {
    pub(crate) source_id: String,
    pub(crate) display_name: String,
    pub(crate) source_key: String,
}

fn default_source_id() -> String {
    String::new()
}

fn unique_identity_component(base: &str, existing: &BTreeSet<&str>) -> String {
    if !existing.contains(base) {
        return base.to_string();
    }
    for suffix in 2_u64.. {
        let candidate = format!("{base}_{suffix}");
        if !existing.contains(candidate.as_str()) {
            return candidate;
        }
    }
    unreachable!("unbounded suffix search must eventually find a free identity component")
}

fn source_key_seed(raw: &str) -> String {
    let mut seed = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    while seed.contains("__") {
        seed = seed.replace("__", "_");
    }
    let seed = seed.trim_matches('_');
    let seed = if seed.is_empty() { "source" } else { seed };
    if seed
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
    {
        seed.to_string()
    } else {
        format!("source_{seed}")
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

#[cfg(test)]
mod tests {
    use super::{InstalledSource, SourceOrigin};
    use crate::sources::SourceName;

    fn installed_source(name: &str, source_id: &str, source_key: &str) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("source name"),
            source_id: source_id.to_string(),
            display_name: name.to_string(),
            source_key: source_key.to_string(),
            version: None,
            variables: std::collections::BTreeMap::default(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Imported,
        }
    }

    #[test]
    fn allocated_identity_suffixes_source_key_and_id_collisions() {
        let existing = [
            installed_source("work", "src_github", "github"),
            installed_source("work_2", "src_github_2", "github_2"),
        ];
        let name = SourceName::parse("github").expect("source name");
        let identity = InstalledSource::allocate_identity_for_name(&name, existing.iter());
        assert_eq!(identity.display_name, "github");
        assert_eq!(identity.source_key, "github_3");
        assert_eq!(identity.source_id, "src_github_3");
    }

    #[test]
    fn identity_sanitizes_source_key_seed_for_generated_refs() {
        let name = SourceName::parse("Foo.Bar-2026").expect("source name");
        let identity = InstalledSource::identity_for_name(&name);
        assert_eq!(identity.display_name, "Foo.Bar-2026");
        assert_eq!(identity.source_key, "foo_bar_2026");
        assert_eq!(identity.source_id, "src_foo_bar_2026");
    }
}
