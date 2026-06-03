//! Installed-source domain model for the application management plane.

use std::collections::{BTreeMap, BTreeSet};

use coral_spec::{AuthSpec, ManifestInputSpec, ValidatedSourceManifest, ValueSourceSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::credentials::CredentialStorageKind;
use crate::sources::SourceName;

/// App-owned description of a source candidate that can be installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateSource {
    pub(crate) name: SourceName,
    pub(crate) description: String,
    pub(crate) version: Option<String>,
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

        let mut keys = Vec::new();
        for value in values {
            match value {
                ValueSourceSpec::Input { key } | ValueSourceSpec::Bearer { key }
                    if declared_secret_names.contains(key) && !keys.contains(key) =>
                {
                    keys.push(key.clone());
                }
                _ if value_source_always_resolves(value) => return None,
                _ => {}
            }
        }

        if keys.is_empty() {
            return None;
        }
        Some(Self { context, keys })
    }
}

fn value_source_always_resolves(value: &ValueSourceSpec) -> bool {
    match value {
        ValueSourceSpec::Literal { value } => !literal_renders_empty(value),
        ValueSourceSpec::NowEpochMinusSeconds { .. } => true,
        _ => false,
    }
}

fn literal_renders_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(text) => text.is_empty(),
        _ => false,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn declared_secrets(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn requirement(
        value: &ValueSourceSpec,
        secrets: &[&str],
    ) -> Option<AuthOneOfSecretRequirement> {
        AuthOneOfSecretRequirement::from_value_source(
            "auth header 'Authorization'".to_string(),
            value,
            &declared_secrets(secrets),
        )
    }

    #[test]
    fn one_of_secret_branches_require_one_of_them() {
        let value = ValueSourceSpec::OneOf {
            values: vec![
                ValueSourceSpec::Bearer {
                    key: "OAUTH_TOKEN".to_string(),
                },
                ValueSourceSpec::Input {
                    key: "API_KEY".to_string(),
                },
            ],
        };
        let requirement = requirement(&value, &["OAUTH_TOKEN", "API_KEY"]).expect("requirement");
        assert_eq!(
            requirement.keys,
            vec!["OAUTH_TOKEN".to_string(), "API_KEY".to_string()]
        );
    }

    #[test]
    fn one_of_mixing_secret_with_runtime_branch_still_requires_the_secret() {
        let value = ValueSourceSpec::OneOf {
            values: vec![
                ValueSourceSpec::Bearer {
                    key: "OAUTH_TOKEN".to_string(),
                },
                ValueSourceSpec::State {
                    key: "SESSION".to_string(),
                },
            ],
        };
        let requirement = requirement(&value, &["OAUTH_TOKEN"]).expect("requirement");
        assert_eq!(requirement.keys, vec!["OAUTH_TOKEN".to_string()]);
    }

    #[test]
    fn one_of_with_guaranteed_literal_fallback_requires_nothing() {
        let value = ValueSourceSpec::OneOf {
            values: vec![
                ValueSourceSpec::Bearer {
                    key: "OAUTH_TOKEN".to_string(),
                },
                ValueSourceSpec::Literal {
                    value: Value::String("anonymous".to_string()),
                },
            ],
        };
        assert!(requirement(&value, &["OAUTH_TOKEN"]).is_none());
    }

    #[test]
    fn one_of_with_empty_literal_fallback_still_requires_the_secret() {
        let value = ValueSourceSpec::OneOf {
            values: vec![
                ValueSourceSpec::Bearer {
                    key: "OAUTH_TOKEN".to_string(),
                },
                ValueSourceSpec::Literal {
                    value: Value::String(String::new()),
                },
            ],
        };
        let requirement = requirement(&value, &["OAUTH_TOKEN"]).expect("requirement");
        assert_eq!(requirement.keys, vec!["OAUTH_TOKEN".to_string()]);
    }

    #[test]
    fn one_of_without_any_secret_branch_requires_nothing() {
        let value = ValueSourceSpec::OneOf {
            values: vec![ValueSourceSpec::State {
                key: "SESSION".to_string(),
            }],
        };
        assert!(requirement(&value, &["OAUTH_TOKEN"]).is_none());
    }

    #[test]
    fn one_of_deduplicates_repeated_secret_keys_in_runtime_order() {
        let value = ValueSourceSpec::OneOf {
            values: vec![
                ValueSourceSpec::Bearer {
                    key: "OAUTH_TOKEN".to_string(),
                },
                ValueSourceSpec::Input {
                    key: "API_KEY".to_string(),
                },
                ValueSourceSpec::Bearer {
                    key: "OAUTH_TOKEN".to_string(),
                },
            ],
        };
        let requirement = requirement(&value, &["OAUTH_TOKEN", "API_KEY"]).expect("requirement");
        assert_eq!(
            requirement.keys,
            vec!["OAUTH_TOKEN".to_string(), "API_KEY".to_string()]
        );
    }

    #[test]
    fn non_one_of_value_source_requires_nothing() {
        let value = ValueSourceSpec::Bearer {
            key: "OAUTH_TOKEN".to_string(),
        };
        assert!(requirement(&value, &["OAUTH_TOKEN"]).is_none());
    }
}
