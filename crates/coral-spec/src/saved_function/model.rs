use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Validated saved function artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionSpec {
    pub(super) name: String,
    pub(super) description: String,
    pub(super) arguments: Vec<SavedFunctionArgumentSpec>,
    pub(super) implementation: SavedFunctionImplementationSpec,
    pub(super) validation: SavedFunctionValidationSpec,
    pub(super) publish: SavedFunctionPublishSpec,
}

/// One typed saved function argument.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SavedFunctionArgumentSpec {
    /// Argument name used in saved function SQL placeholders and published call schemas.
    pub name: String,
    /// Scalar argument type.
    #[serde(rename = "type")]
    pub data_type: SavedFunctionArgumentType,
    /// Whether callers must provide this argument.
    #[serde(default)]
    pub required: bool,
    /// Optional human-readable argument description.
    #[serde(default)]
    pub description: String,
}

/// Scalar argument types supported by v1 saved functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SavedFunctionArgumentType {
    /// UTF-8 string value.
    String,
    /// Signed 64-bit integer value.
    Integer,
    /// Boolean value.
    Boolean,
}

/// Runtime validation inputs for one saved function.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SavedFunctionValidationSpec {
    /// Concrete argument values Coral uses when validating the saved function at install time.
    #[serde(default)]
    pub args: BTreeMap<String, SavedFunctionValidationValue>,
}

/// One scalar validation argument value.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum SavedFunctionValidationValue {
    /// UTF-8 string value.
    String(String),
    /// Signed 64-bit integer value.
    Integer(i64),
    /// Boolean value.
    Boolean(bool),
    /// Explicit null value.
    Null(()),
}

/// The executable body behind a saved function.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SavedFunctionImplementationSpec {
    /// Read-only Coral SQL using `DataFusion` value parameters like `$argument`.
    CoralSql {
        /// SQL query executed by Coral after typed argument binding.
        query: String,
    },
}

/// Public surfaces a saved function should publish.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SavedFunctionPublishSpec {
    /// Required SQL table-function surface.
    pub table_function: SavedFunctionTableFunctionPublishSpec,
    /// Optional MCP tool surface.
    #[serde(default)]
    pub mcp: Option<SavedFunctionMcpPublishSpec>,
}

/// SQL table-function surface published by a saved function.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SavedFunctionTableFunctionPublishSpec {
    /// SQL schema where the public table function is exposed.
    pub schema: String,
    /// Public table-function name within `schema`.
    pub name: String,
    /// Optional publish-target-specific description.
    #[serde(default)]
    pub description: String,
}

/// Optional MCP tool surface published by a saved function.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SavedFunctionMcpPublishSpec {
    /// MCP tool name.
    pub name: String,
    /// Optional publish-target-specific description.
    #[serde(default)]
    pub description: String,
}

impl SavedFunctionSpec {
    /// Returns the stable saved function id within one workspace.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the user-facing saved function description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns declared saved function arguments in authored order.
    #[must_use]
    pub fn arguments(&self) -> &[SavedFunctionArgumentSpec] {
        &self.arguments
    }

    /// Returns the executable saved function implementation.
    #[must_use]
    pub fn implementation(&self) -> &SavedFunctionImplementationSpec {
        &self.implementation
    }

    /// Returns the install-time validation invocation.
    #[must_use]
    pub fn validation(&self) -> &SavedFunctionValidationSpec {
        &self.validation
    }

    /// Returns public surfaces the saved function asks Coral to publish.
    #[must_use]
    pub fn publish(&self) -> &SavedFunctionPublishSpec {
        &self.publish
    }
}
