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
use super::diagnostics::{Diagnostic, SafeInteger, saved_diagnostics};

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
pub(crate) fn exact_description(
    exact: &ExactAuthoringDescription,
) -> Result<Description, ToolFailure> {
    exact_in_json(coral_search::projection::description(exact))
}

/// The description, or an internal failure when a saved diagnostic carries
/// an integer that a JSON reader cannot hold exactly. The shared type
/// serializes a plain `u64`, so this adapter keeps the check that its own
/// [`SafeInteger`] makes for `save_function`.
fn exact_in_json(description: Description) -> Result<Description, ToolFailure> {
    if let Description::SavedFunction {
        detail: Some(detail),
        ..
    } = &description
    {
        for diagnostic in &detail.diagnostics {
            SafeInteger::new(diagnostic.code)?;
            for value in [diagnostic.line, diagnostic.column].into_iter().flatten() {
                SafeInteger::new(value)?;
            }
        }
    }
    Ok(description)
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

#[cfg(test)]
mod tests {
    use coral_catalog::{
        Description, DescriptionAuthoring, SavedDetail, SavedDiagnostic, SavedDiagnosticCategory,
    };

    fn saved_function(code: u64, line: Option<u64>, column: Option<u64>) -> Description {
        Description::SavedFunction {
            authoring: DescriptionAuthoring {
                call_path: "coral.functions.weekly".to_owned(),
                description: "The weekly report.".to_owned(),
                typescript_declaration: "export default function (): Promise<string>;".to_owned(),
            },
            detail: Some(SavedDetail {
                source: "export default async function weekly() {}".to_owned(),
                diagnostics: vec![SavedDiagnostic {
                    category: SavedDiagnosticCategory::Warning,
                    code,
                    message: "unused".to_owned(),
                    line,
                    column,
                }],
                diagnostics_truncated: false,
            }),
        }
    }

    // Catches: a saved diagnostic integer that a JSON reader cannot hold
    // exactly reaching the agent as a rounded number instead of failing.
    #[test]
    fn an_exact_description_fails_rather_than_round_a_diagnostic_integer() {
        const SAFE: u64 = 9_007_199_254_740_991;
        assert!(super::exact_in_json(saved_function(SAFE, Some(SAFE), Some(SAFE))).is_ok());
        for (code, line, column) in [
            (SAFE + 1, None, None),
            (6133, Some(SAFE + 1), None),
            (6133, Some(1), Some(SAFE + 1)),
        ] {
            assert!(
                super::exact_in_json(saved_function(code, line, column)).is_err(),
                "{code} {line:?} {column:?}"
            );
        }
    }
}
