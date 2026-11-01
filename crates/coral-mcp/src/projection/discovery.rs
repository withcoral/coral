//! Saved Function results.
use coral_functions::SavedFunction;
use schemars::JsonSchema;
use serde::Serialize;

use super::ToolFailure;
use super::diagnostics::{Diagnostic, saved_diagnostics};

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SavedFunctionResult<'a> {
    call_path: String,
    description: &'a str,
    declaration: &'a str,
    diagnostics: Vec<Diagnostic>,
    diagnostics_truncated: bool,
}

pub(crate) fn saved_function(
    function: &SavedFunction,
) -> Result<SavedFunctionResult<'_>, ToolFailure> {
    Ok(SavedFunctionResult {
        call_path: format!("coral.functions.{}", function.path().as_str()),
        description: function.description().as_str(),
        declaration: function.declaration().as_str(),
        diagnostics: saved_diagnostics(function.diagnostics())?,
        diagnostics_truncated: function.diagnostics_truncated(),
    })
}
