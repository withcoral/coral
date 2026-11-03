//! Search Hits and complete public TypeScript descriptions, not authoritative Provider
//! schemas. coral-search builds both as the result types coral-catalog declares
//! for `coral.workspace.search`, so MCP, the Built-in Tool and Task history
//! show the same fields.
use coral_catalog::{Description, SearchResult};
use coral_functions::SavedFunction;
use coral_search::{ExactAuthoringDescription, SearchPage};
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

pub(crate) fn search(page: &SearchPage) -> SearchResult {
    coral_search::projection::search_result(page)
}

/// A Saved Function's detail is absent when `describe` returned only the
/// `types` it was asked for.
pub(crate) fn exact_description(exact: &ExactAuthoringDescription) -> Description {
    coral_search::projection::description(exact)
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
