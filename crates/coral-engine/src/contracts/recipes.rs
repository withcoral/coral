//! Runtime recipe contracts supplied by the app layer.

/// One validated recipe made available to the query runtime.
#[derive(Debug, Clone)]
pub struct RecipeRuntimeDefinition {
    /// Stable recipe id within one workspace.
    pub name: String,
    /// User-facing recipe description.
    pub description: String,
    /// Typed arguments accepted by the recipe.
    pub arguments: Vec<RecipeRuntimeArgument>,
    /// Executable recipe implementation.
    pub implementation: RecipeRuntimeImplementation,
    /// Public surfaces requested by the recipe.
    pub publish: Vec<RecipeRuntimePublish>,
    /// Result columns inferred during app/runtime validation.
    pub result_columns: Vec<RecipeRuntimeResultColumn>,
}

/// One trusted recipe invocation.
#[derive(Debug, Clone)]
pub struct RecipeRuntimeCall {
    /// Recipe id to call.
    pub recipe_name: String,
    /// Argument values keyed by recipe argument name.
    pub arguments: std::collections::BTreeMap<String, RecipeRuntimeArgumentValue>,
}

/// One typed recipe argument.
#[derive(Debug, Clone)]
pub struct RecipeRuntimeArgument {
    /// Argument name.
    pub name: String,
    /// Scalar argument type.
    pub data_type: RecipeRuntimeArgumentType,
    /// Whether callers must provide this argument.
    pub required: bool,
    /// Optional user-facing description.
    pub description: String,
}

/// Scalar argument types supported by v1 recipe execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipeRuntimeArgumentType {
    /// UTF-8 string.
    String,
    /// Signed 64-bit integer.
    Integer,
    /// Boolean.
    Boolean,
}

/// Concrete argument value supplied to one recipe call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecipeRuntimeArgumentValue {
    /// UTF-8 string.
    String(String),
    /// Signed 64-bit integer.
    Integer(i64),
    /// Boolean.
    Boolean(bool),
    /// Explicit SQL NULL.
    Null,
}

/// Executable recipe implementation.
#[derive(Debug, Clone)]
pub enum RecipeRuntimeImplementation {
    /// Read-only Coral SQL using `DataFusion` value parameters like `$argument`.
    CoralSql {
        /// SQL query executed by Coral after typed argument binding.
        query: String,
    },
}

/// Public surface requested by one recipe.
#[derive(Debug, Clone)]
pub enum RecipeRuntimePublish {
    /// Public SQL table-function wrapper.
    TableFunction {
        /// SQL schema.
        schema: String,
        /// SQL function name.
        name: String,
        /// Optional publish-target-specific description.
        description: String,
    },
    /// Public MCP tool wrapper.
    McpTool {
        /// MCP tool name.
        name: String,
        /// Optional publish-target-specific description.
        description: String,
    },
}

/// One column returned by a recipe.
#[derive(Debug, Clone)]
pub struct RecipeRuntimeResultColumn {
    /// Column name.
    pub name: String,
    /// Arrow/DataFusion type rendered as text.
    pub data_type: String,
    /// Whether the column can contain null values.
    pub nullable: bool,
    /// Optional user-facing description.
    pub description: String,
}
