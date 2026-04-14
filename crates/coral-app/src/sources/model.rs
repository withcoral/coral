//! Installed-source domain model for the application management plane.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::sources::SourceName;

/// App-owned description of a source candidate that can be installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateSource {
    pub(crate) name: SourceName,
    pub(crate) description: String,
    pub(crate) version: String,
    pub(crate) inputs: Vec<CandidateSourceInput>,
    pub(crate) installed: bool,
    pub(crate) origin: SourceOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateSourceInput {
    pub(crate) key: String,
    pub(crate) kind: CandidateSourceInputKind,
    pub(crate) required: bool,
    pub(crate) default_value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CandidateSourceInputKind {
    Variable,
    Secret,
}

/// App-owned model for one source installed in a workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstalledSource {
    /// Bare source name. This is also the visible SQL schema name.
    pub(crate) name: SourceName,
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
    pub(crate) origin: SourceOrigin,
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
