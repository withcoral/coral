//! Engine runtime saved function contracts supplied by the app layer.
//!
//! These are not the user-authored saved function file format. The app/spec layers
//! parse and validate authored saved functions, then supply these runtime definitions
//! to the engine for planning and execution.

use crate::QueryParameterValue;

/// One validated saved function made available to the query runtime.
#[derive(Debug, Clone)]
pub struct SavedFunctionRuntimeDefinition {
    /// Stable saved function id within one workspace.
    pub name: String,
    /// Typed arguments accepted by the saved function.
    pub arguments: Vec<SavedFunctionRuntimeArgument>,
    /// Executable saved function implementation.
    pub implementation: SavedFunctionRuntimeImplementation,
}

/// One typed saved function argument.
#[derive(Debug, Clone)]
pub struct SavedFunctionRuntimeArgument {
    /// Argument name.
    pub name: String,
    /// Scalar argument type.
    pub data_type: SavedFunctionRuntimeArgumentType,
    /// Whether callers must provide this argument.
    pub required: bool,
}

/// Scalar argument types supported by v1 saved function execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SavedFunctionRuntimeArgumentType {
    /// UTF-8 string.
    String,
    /// Signed 64-bit integer.
    Integer,
    /// Boolean.
    Boolean,
}

impl SavedFunctionRuntimeArgumentType {
    /// Returns the argument type name used in validation diagnostics.
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
        }
    }

    /// Returns whether a query parameter value can satisfy this saved function argument type.
    #[must_use]
    pub(crate) fn accepts(self, value: &QueryParameterValue) -> bool {
        matches!(
            (self, value),
            (Self::String, QueryParameterValue::String(_))
                | (Self::Integer, QueryParameterValue::Integer(_))
                | (Self::Boolean, QueryParameterValue::Boolean(_))
        )
    }

    /// Builds a typed SQL NULL for this argument type.
    #[must_use]
    pub(crate) fn typed_null_value(self) -> QueryParameterValue {
        match self {
            Self::String => QueryParameterValue::String(None),
            Self::Integer => QueryParameterValue::Integer(None),
            Self::Boolean => QueryParameterValue::Boolean(None),
        }
    }
}

/// Executable saved function implementation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SavedFunctionRuntimeImplementation {
    /// Read-only Coral SQL using `DataFusion` value parameters like `$argument`.
    CoralSql {
        /// SQL query executed by Coral after typed argument binding.
        query: String,
    },
}
