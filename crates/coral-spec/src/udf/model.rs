use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ManifestDataType;

/// Validated function artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionSpec {
    pub(super) name: String,
    pub(super) group: String,
    pub(super) description: String,
    pub(super) guide: String,
    pub(super) implementation: FunctionImplementationSpec,
    pub(super) signature: Option<FunctionDeclaredSignature>,
}

/// The executable body behind a function.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum FunctionImplementationSpec {
    /// Read-only Coral SQL using `DataFusion` value parameters like `$argument`.
    CoralSql(FunctionCoralSqlImplementationSpec),
    /// TypeScript source for a future function executor.
    TypeScript(FunctionTypeScriptImplementationSpec),
}

impl FunctionImplementationSpec {
    /// Returns the authored implementation language.
    #[must_use]
    pub fn language(&self) -> FunctionLanguage {
        match self {
            Self::CoralSql(_) => FunctionLanguage::Sql,
            Self::TypeScript(_) => FunctionLanguage::TypeScript,
        }
    }
}

/// Read-only Coral SQL implementation for a function.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FunctionCoralSqlImplementationSpec {
    /// SQL query executed by Coral after typed argument binding.
    pub query: String,
}

/// TypeScript implementation for a function.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FunctionTypeScriptImplementationSpec {
    /// TypeScript source supplied by the author.
    pub source: String,
}

/// Authored implementation language.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FunctionLanguage {
    /// Coral SQL body.
    #[default]
    #[serde(rename = "sql")]
    Sql,
    /// TypeScript body.
    #[serde(rename = "typescript")]
    TypeScript,
}

/// Signature declared in a function artifact.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct FunctionDeclaredSignature {
    /// Typed arguments accepted by the function.
    pub arguments: Vec<FunctionDeclaredArgument>,
    /// Typed columns returned by the function.
    pub result_columns: Vec<FunctionDeclaredResultColumn>,
}

/// One declared function argument.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FunctionDeclaredArgument {
    /// Argument name.
    pub name: String,
    /// Argument type in manifest spelling.
    pub data_type: ManifestDataType,
}

/// One declared function result column.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FunctionDeclaredResultColumn {
    /// Column name.
    pub name: String,
    /// Column type in manifest spelling.
    pub data_type: ManifestDataType,
    /// Whether the column can contain null values.
    #[serde(default)]
    pub nullable: bool,
}

impl FunctionSpec {
    /// Returns the stable function id within one workspace.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the generic group for this function.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the user-facing function description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns query guidance for the published table function.
    #[must_use]
    pub fn guide(&self) -> &str {
        &self.guide
    }

    /// Returns the executable function implementation.
    #[must_use]
    pub fn implementation(&self) -> &FunctionImplementationSpec {
        &self.implementation
    }

    /// Returns the authored implementation language.
    #[must_use]
    pub fn language(&self) -> FunctionLanguage {
        self.implementation.language()
    }

    /// Returns the declared signature, when one is present.
    #[must_use]
    pub fn declared_signature(&self) -> Option<&FunctionDeclaredSignature> {
        self.signature.as_ref()
    }
}
