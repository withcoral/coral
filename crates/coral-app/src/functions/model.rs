//! Installed function domain model for the application management plane.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::bootstrap::AppError;
use crate::identity::parse_path_segment;

/// App-owned identity for one installed function.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FunctionName(String);

impl FunctionName {
    /// Parse and validate a function name for app-internal use.
    pub(crate) fn parse(name: &str) -> Result<Self, AppError> {
        parse_path_segment("function", name).map(Self)
    }

    /// Borrow the normalized function name at string boundaries such as paths,
    /// config rendering, or protobuf mapping.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FunctionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for FunctionName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for FunctionName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for FunctionName {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// App-owned model for one function installed in a workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstalledFunction {
    /// Stable function name.
    pub(crate) name: FunctionName,
    /// Coral surface that wrote the current function definition.
    #[serde(default)]
    pub(crate) write_surface: FunctionWriteSurface,
}

/// Coral surface that wrote the current installed function definition.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FunctionWriteSurface {
    /// The function predates write-surface metadata or its source is unknown.
    #[default]
    Unknown,
    /// Coral's command-line interface wrote the function.
    Cli,
    /// Coral's MCP server wrote the function.
    Mcp,
}

impl FunctionWriteSurface {
    pub(crate) fn as_config_value(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Cli => "cli",
            Self::Mcp => "mcp",
        }
    }
}
