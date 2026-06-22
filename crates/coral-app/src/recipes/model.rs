//! Installed-recipe domain model for the application management plane.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::bootstrap::AppError;
use crate::identity::parse_path_segment;

/// App-owned identity for one installed recipe.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RecipeName(String);

impl RecipeName {
    /// Parse and validate a recipe name for app-internal use.
    pub(crate) fn parse(name: &str) -> Result<Self, AppError> {
        parse_path_segment("recipe", name).map(Self)
    }

    /// Borrow the normalized recipe name at string boundaries such as paths,
    /// config rendering, or protobuf mapping.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RecipeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for RecipeName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RecipeName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

impl FromStr for RecipeName {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// App-owned model for one recipe installed in a workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstalledRecipe {
    /// Stable recipe name.
    pub(crate) name: RecipeName,
}
