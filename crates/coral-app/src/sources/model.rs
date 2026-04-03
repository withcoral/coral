//! Managed-source domain model for the application management plane.
//!
//! This file is the reviewable seam between persisted source state in
//! `coral-app` and the installed source resources exposed over gRPC.

use std::collections::BTreeMap;

use coral_api::v1::{Source, SourceOrigin, SourceSecret, SourceVariable, Workspace};
use serde::{Deserialize, Serialize};

/// App-owned persisted representation of one managed source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ManagedSource {
    /// Owning workspace for this source.
    pub(crate) workspace: Workspace,
    /// Bare source name. This is also the visible SQL schema name.
    pub(crate) name: String,
    /// Manifest version from the installed source spec.
    #[serde(default)]
    pub(crate) version: String,
    /// Configured non-secret variable bindings.
    #[serde(default)]
    pub(crate) variables: BTreeMap<String, String>,
    /// Logical secret keys referenced by this source.
    #[serde(default)]
    pub(crate) secrets: Vec<String>,
    /// Where this installed source came from.
    pub(crate) origin: ManagedSourceOrigin,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManagedSourceOrigin {
    Bundled,
    Imported,
}

impl ManagedSource {
    #[must_use]
    /// Returns the logical secret keys owned by this source.
    pub(crate) fn secrets(&self) -> Vec<String> {
        self.secrets.clone()
    }

    #[must_use]
    /// Returns the installed source resource exposed by the management plane.
    pub(crate) fn to_source_resource(&self) -> Source {
        Source {
            workspace: Some(self.workspace.clone()),
            name: self.name.clone(),
            version: self.version.clone(),
            secrets: self
                .secrets()
                .into_iter()
                .map(|key| SourceSecret {
                    key,
                    value: String::new(),
                })
                .collect(),
            variables: self
                .variables
                .iter()
                .map(|(key, value)| SourceVariable {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            origin: self.origin.to_proto() as i32,
        }
    }
}

impl ManagedSourceOrigin {
    pub(crate) fn to_proto(self) -> SourceOrigin {
        match self {
            Self::Bundled => SourceOrigin::Bundled,
            Self::Imported => SourceOrigin::Imported,
        }
    }

    pub(crate) fn as_config_value(self) -> &'static str {
        match self {
            Self::Bundled => "bundled",
            Self::Imported => "imported",
        }
    }
}
