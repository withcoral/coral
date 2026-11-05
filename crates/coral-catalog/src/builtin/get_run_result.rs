//! `coral.workspace.getRunResult`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. The result holds an earlier Run's state, its
//! Final Value or failure, and the Tool Results it retained, so a later
//! Program can slice data an earlier one fetched without calling the
//! Provider again.

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ArgumentProblem, BuiltinTool, Declared, Definition, derived_schema, tool_result};
use crate::{ToolHints, ToolPresentation};

/// Any Principal that can inspect the Workspace reads its own earlier Runs.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Inspect;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::GetRunResult.key(),
        summary: "Read an earlier Run's Final Value and Tool Results",
        presentation: ToolPresentation::new(
            Some("Get run result".to_string()),
            "Read an earlier Run of this Task: its state, its Final Value or failure, and the \
             Tool Results its Program received, oldest first. Fetch provider data once, \
             return a small summary, then slice the full data here in later Programs \
             without calling providers again."
                .to_string(),
            false,
            vec!["runs".to_string()],
            Some(
                "Pass the run_id that exec returned for an earlier Run of this Task. A runId \
                 that names no such Run fails with invalid_input. A running Run has no \
                 finalValue yet. toolResults stops at 4 MiB in total and sets truncated when \
                 it leaves some out; search and getRunResult calls are left out."
                    .to_string(),
            ),
        ),
        // Read-only: the default Tool Access State is enabled.
        hints: ToolHints::new(Some(true), None, Some(true), None),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<GetRunResultResult>(Contract::Serialize),
        declared: Declared::Once,
    }
}

/// `getRunResult`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(description = "")]
struct Arguments {
    #[schemars(
        length(min = 1),
        description = "The run_id that exec returned for an earlier Run of this Task."
    )]
    run_id: String,
}

/// `getRunResult`'s arguments, checked by every rule that needs no
/// Workspace state.
#[derive(Debug, Clone)]
pub struct GetRunResultArguments {
    run_id: String,
}

impl GetRunResultArguments {
    /// The Run id as the Program wrote it; coral-run checks its syntax.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Reads the arguments.
    ///
    /// # Errors
    ///
    /// Returns the argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        if arguments.run_id.is_empty() {
            return Err(vec![ArgumentProblem::new(
                "runId",
                "pass the run_id that exec returned",
            )]);
        }
        Ok(Self {
            run_id: arguments.run_id,
        })
    }
}

/// The only arguments `getRunResult` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![serde_json::json!({ "runId": "run_0123" })]
}

/// What `getRunResult` returns: one earlier Run's outcome and Tool Results.
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(deny_unknown_fields, description = "")]
pub struct GetRunResultResult {
    /// The Run id.
    #[schemars(description = "The run_id you passed.")]
    pub run_id: String,
    /// The Run state.
    #[schemars(description = "running, succeeded, failed, canceled, timed_out or interrupted.")]
    pub state: String,
    /// The Final Value of a succeeded Run that returned one.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(description = "What the Program returned, when the Run succeeded and \
                       returned a value.")]
    pub final_value: Option<Value>,
    /// The failure of a failed Run.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(description = "Why the Run failed, when it failed.")]
    pub failure: Option<RunResultFailure>,
    /// The retained Tool Results, oldest first.
    #[schemars(
        description = "The Tool Results the Run's Program received, oldest first, \
                       without search and getRunResult calls."
    )]
    pub tool_results: Vec<RunToolResult>,
    /// Whether the byte cap left some Tool Results out.
    #[schemars(
        description = "True when toolResults leaves out results that did not fit \
                       in 4 MiB."
    )]
    pub truncated: bool,
}

impl GetRunResultResult {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}

/// Why an earlier Run failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(rename = "RunFailure", deny_unknown_fields, description = "")]
pub struct RunResultFailure {
    /// The failure code.
    #[schemars(description = "The failure code, such as program_rejected.")]
    pub code: String,
    /// The safe diagnostic message.
    #[schemars(description = "What went wrong.")]
    pub message: String,
}

/// One Tool Result an earlier Run's Program received.
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "RunToolResult", deny_unknown_fields, description = "")]
pub struct RunToolResult {
    /// The Call Path the Program called.
    #[schemars(description = "The Call Path the Program called.")]
    pub call_path: String,
    /// The arguments it passed.
    #[schemars(description = "The arguments the Program passed.")]
    pub arguments: Value,
    /// The Tool Result's data.
    #[schemars(description = "The data the call returned, as the Program received it.")]
    pub data: Value,
}
