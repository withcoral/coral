use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Validated function artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionSpec {
    pub(super) name: String,
    pub(super) schema: String,
    pub(super) description: String,
    pub(super) implementation: FunctionImplementationSpec,
}

/// The executable body behind a function.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FunctionImplementationSpec {
    /// Read-only Coral SQL using `DataFusion` value parameters like `$argument`.
    pub coral_sql: FunctionCoralSqlImplementationSpec,
}

/// Read-only Coral SQL implementation for a function.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FunctionCoralSqlImplementationSpec {
    /// SQL query executed by Coral after typed argument binding.
    pub query: String,
}

impl FunctionSpec {
    /// Returns the stable function id within one workspace.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the SQL schema where this function is published.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the user-facing function description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the executable function implementation.
    #[must_use]
    pub fn implementation(&self) -> &FunctionImplementationSpec {
        &self.implementation
    }
}
