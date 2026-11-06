//! `coral.deleteFunction`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. It deletes one of the caller's own Saved
//! Functions.

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ArgumentProblem, BuiltinTool, Declared, Definition, derived_schema, tool_result};
use crate::{ToolHints, ToolPresentation};

/// Every Principal authors its own Saved Functions.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Author;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::DeleteFunction.key(),
        summary: "Delete one of your Saved Functions",
        presentation: ToolPresentation::new(
            Some("Delete function".to_string()),
            "Delete one of your Saved Functions.".to_string(),
            false,
            vec!["functions".to_string()],
            Some(
                "A path that names none of your functions fails with \
                 saved_function_not_found. A Program already running keeps the version it \
                 linked."
                    .to_string(),
            ),
        ),
        // Deleting again fails, so it is not idempotent.
        hints: ToolHints::new(Some(false), Some(true), Some(false), Some(false)),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<DeleteFunctionResult>(Contract::Serialize),
        declared: Declared::Once,
        // It changes only the caller's own Saved Functions, so it needs no
        // approval unless an Owner sets an exact rule (ADR 0031).
        enabled_by_default: true,
    }
}

/// `deleteFunction`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
struct Arguments {
    #[schemars(
        length(min = 1),
        description = "Dotted path under coral.functions, without that prefix, such as \
                       github.pulls.listStale."
    )]
    path: String,
}

/// `deleteFunction`'s arguments in the shape the schema allows.
#[derive(Debug, Clone)]
pub struct DeleteFunctionArguments {
    path: String,
}

impl DeleteFunctionArguments {
    /// The path relative to `coral.private`.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Reads the arguments' shape.
    ///
    /// # Errors
    ///
    /// Returns the argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        if arguments.path.is_empty() {
            return Err(vec![ArgumentProblem::new(
                "path",
                "pass a value that is not empty",
            )]);
        }
        Ok(Self {
            path: arguments.path,
        })
    }
}

/// The only arguments `deleteFunction` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![serde_json::json!({ "path": "github.pulls.listStale" })]
}

/// What `deleteFunction` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct DeleteFunctionResult {
    /// The complete Call Path that no longer names a function.
    #[schemars(description = "The complete Call Path of the deleted function.")]
    pub call_path: String,
}

impl DeleteFunctionResult {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}
