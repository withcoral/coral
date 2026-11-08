//! `coral.saveFunction`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. It creates or replaces one of the caller's own
//! Saved Functions; coral-functions checks the path, description and source.

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::private_tools::{empty, missing_source};
use super::{ArgumentProblem, BuiltinTool, Declared, Definition, derived_schema, tool_result};
use crate::{SavedDiagnostic, ToolHints, ToolPresentation};

/// Every Principal authors its own Saved Functions.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Author;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::SaveFunction.key(),
        summary: "Save one of your Saved Functions",
        presentation: ToolPresentation::new(
            Some("Save function".to_string()),
            "Create or replace one of your Saved Functions. Later Programs call it as \
             coral.private.<path>(...)."
                .to_string(),
            false,
            vec!["functions".to_string()],
            Some(
                "Coral saves the function with the top-level declarations it uses. Call it \
                 before saving, and call the saved function by its path in later Programs."
                    .to_string(),
            ),
        ),
        // A replace discards the previous source; saving the same input again
        // changes nothing more. It writes only the caller's own functions.
        hints: ToolHints::new(Some(false), Some(true), Some(true), Some(false)),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<SaveFunctionResult>(Contract::Serialize),
        declared: Declared::Once,
        // It changes only the caller's own Saved Functions, so it needs no
        // approval unless an Owner sets an exact rule (ADR 0031).
        enabled_by_default: true,
    }
}

/// `saveFunction`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
struct Arguments {
    #[schemars(
        length(min = 1),
        description = "Dotted path under coral.private, without that prefix, such as \
                       github.pulls.listStale."
    )]
    path: String,
    #[schemars(
        length(min = 1),
        description = "One line, one or two sentences: what the function does and when to use \
                       it. Use words a search would match. Do not restate input or return \
                       types; the declaration is shown beside it."
    )]
    description: String,
    #[schemars(schema_with = "super::private_tools::source_schema")]
    source: String,
}

/// `saveFunction`'s arguments in the shape the schema allows.
/// coral-functions checks each value itself.
#[derive(Debug, Clone)]
pub struct SaveFunctionArguments {
    path: String,
    description: String,
    source: String,
}

impl SaveFunctionArguments {
    /// The path relative to `coral.private`.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The description search shows beside the declaration.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// The TypeScript module source admission extracted.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Reads the arguments' shape.
    ///
    /// # Errors
    ///
    /// Returns the argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        if let Some(problem) = missing_source(arguments, BuiltinTool::SaveFunction) {
            return Err(vec![problem]);
        }
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        let problems = empty([
            ("path", arguments.path.as_str()),
            ("description", arguments.description.as_str()),
            ("source", arguments.source.as_str()),
        ]);
        if !problems.is_empty() {
            return Err(problems);
        }
        Ok(Self {
            path: arguments.path,
            description: arguments.description,
            source: arguments.source,
        })
    }
}

/// The only arguments `saveFunction` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![serde_json::json!({
        "path": "github.pulls.listStale",
        "description": "List open pull requests with no update in a week.",
        "source": "export default async function () { return []; }",
    })]
}

/// What `saveFunction` returns: the function as later Programs see it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct SaveFunctionResult {
    /// The complete Call Path.
    #[schemars(description = "The complete Call Path a later Program calls, such as \
                       coral.private.github.pulls.listStale.")]
    pub call_path: String,
    /// The saved description.
    #[schemars(description = "The description search shows.")]
    pub description: String,
    /// The emitted declaration.
    #[schemars(description = "The TypeScript declaration later Programs are typed from.")]
    pub declaration: String,
    /// The advisory diagnostics saved with it.
    #[schemars(
        description = "Advisory TypeScript diagnostics. The function is saved even \
                       when one is an error."
    )]
    pub diagnostics: Vec<SavedDiagnostic>,
    /// Whether more diagnostics were found than saved.
    #[schemars(description = "True when Coral found more diagnostics than it kept.")]
    pub diagnostics_truncated: bool,
}

impl SaveFunctionResult {
    /// Whether a JSON reader holds every diagnostic's integers exactly.
    #[must_use]
    pub fn is_exact_in_json(&self) -> bool {
        self.diagnostics
            .iter()
            .all(SavedDiagnostic::is_exact_in_json)
    }

    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}
