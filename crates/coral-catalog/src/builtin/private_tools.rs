//! `coral.addTool`, `coral.updateTool` and `coral.removeTool`: the schemas a
//! Program is typed from, the arguments Coral reads against the input
//! schemas, and the results it writes against the output schemas. They
//! create, replace and remove one of the caller's own Private Tools;
//! coral-tools checks each path, description and source (ADR 0031,
//! ADR 0033).

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ArgumentProblem, BuiltinTool, Declared, Definition, derived_schema, tool_result};
use crate::{SavedDiagnostic, ToolAccessState, ToolHints, ToolPresentation};

/// Every Principal authors its own Private Tools.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Author;

const PATH_DESCRIPTION: &str = "Dotted path under coral.private, without that prefix, such as \
                                github.pulls.listStale.";

const DESCRIPTION_DESCRIPTION: &str = "One line, one or two sentences: what the tool does and \
                                       when to use it. Use words a search would match. Do not \
                                       restate input or return types; the declaration is shown \
                                       beside it.";

/// The description of `policy`, ending with what omitting it does.
macro_rules! policy_description {
    ($omitted:literal) => {
        concat!(
            "The tool's default policy: enabled runs each call, approval_required waits for a \
             person to approve each call, and disabled refuses each call. An owner's rule \
             overrides it. ",
            $omitted
        )
    };
}

const ADD_POLICY_DESCRIPTION: &str = policy_description!("Omit it for enabled.");

const UPDATE_POLICY_DESCRIPTION: &str = policy_description!("Omit it to keep the current one.");

const SAVE_NOTES: &str = "Pass source as the name of a function declared at the top level of \
                          this Program, with function or const: not a string, an inline \
                          function or any other expression. The function takes one object, \
                          such as ({ id }: { id: string }), and returns JSON data: convert a \
                          Date, Map or Set first. Coral saves the function with the \
                          top-level declarations it uses. Call it before saving, and call the \
                          saved tool by its path in later Programs. Type errors do not stop the \
                          save: read diagnostics in the result, and fix an error with \
                          coral.updateTool.";

pub(super) fn add_definition() -> Definition {
    Definition {
        key: BuiltinTool::AddTool.key(),
        summary: "Add one of your Private Tools",
        presentation: ToolPresentation::new(
            Some("Add tool".to_string()),
            "Create one of your Private Tools. Later Programs call it as \
             coral.private.<path>(...)."
                .to_string(),
            false,
            vec!["tools".to_string()],
            Some(format!(
                "{SAVE_NOTES} A path that already names one of your tools fails with \
                 private_tool_exists; change that tool with coral.updateTool."
            )),
        ),
        // A second add at the same path fails, so it is not idempotent, and
        // it replaces nothing.
        hints: ToolHints::new(Some(false), Some(false), Some(false), Some(false)),
        input: derived_schema::<AddArguments>(Contract::Deserialize),
        output: derived_schema::<SavedToolResult>(Contract::Serialize),
        declared: Declared::Once,
        // It changes only the caller's own Private Tools, so it needs no
        // approval unless an Owner sets an exact rule (ADR 0031).
        enabled_by_default: true,
    }
}

pub(super) fn update_definition() -> Definition {
    Definition {
        key: BuiltinTool::UpdateTool.key(),
        summary: "Update one of your Private Tools",
        presentation: ToolPresentation::new(
            Some("Update tool".to_string()),
            "Replace the description and source of one of your Private Tools.".to_string(),
            false,
            vec!["tools".to_string()],
            Some(format!(
                "{SAVE_NOTES} A path that names none of your tools fails with \
                 private_tool_not_found. Pass expectedRevision, the revision addTool or \
                 updateTool returned, to fail with private_tool_revision_conflict when the \
                 tool changed since. A Program already running keeps the version it linked."
            )),
        ),
        // A replace discards the previous source, and each one adds a
        // revision, so repeating it is not idempotent.
        hints: ToolHints::new(Some(false), Some(true), Some(false), Some(false)),
        input: derived_schema::<UpdateArguments>(Contract::Deserialize),
        output: derived_schema::<SavedToolResult>(Contract::Serialize),
        declared: Declared::Once,
        enabled_by_default: true,
    }
}

pub(super) fn remove_definition() -> Definition {
    Definition {
        key: BuiltinTool::RemoveTool.key(),
        summary: "Remove one of your Private Tools",
        presentation: ToolPresentation::new(
            Some("Remove tool".to_string()),
            "Remove one of your Private Tools.".to_string(),
            false,
            vec!["tools".to_string()],
            Some(
                "A path that names none of your tools fails with private_tool_not_found. A \
                 Program already running keeps the version it linked."
                    .to_string(),
            ),
        ),
        // Removing again fails, so it is not idempotent.
        hints: ToolHints::new(Some(false), Some(true), Some(false), Some(false)),
        input: derived_schema::<RemoveArguments>(Contract::Deserialize),
        output: derived_schema::<RemovedToolResult>(Contract::Serialize),
        declared: Declared::Once,
        enabled_by_default: true,
    }
}

/// `addTool`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
struct AddArguments {
    #[schemars(length(min = 1), description = PATH_DESCRIPTION)]
    path: String,
    #[schemars(length(min = 1), description = DESCRIPTION_DESCRIPTION)]
    description: String,
    #[schemars(schema_with = "source_schema")]
    source: String,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(with = "ToolAccessState", description = ADD_POLICY_DESCRIPTION)]
    policy: Option<ToolAccessState>,
}

/// `updateTool`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(description = "")]
struct UpdateArguments {
    #[schemars(length(min = 1), description = PATH_DESCRIPTION)]
    path: String,
    #[schemars(length(min = 1), description = DESCRIPTION_DESCRIPTION)]
    description: String,
    #[schemars(schema_with = "source_schema")]
    source: String,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "u64",
        range(min = 1),
        description = "The revision you expect the tool to be at, as addTool or updateTool \
                       returned it. Omit it to replace whatever is current."
    )]
    expected_revision: Option<u64>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(with = "ToolAccessState", description = UPDATE_POLICY_DESCRIPTION)]
    policy: Option<ToolAccessState>,
}

/// `removeTool`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
struct RemoveArguments {
    #[schemars(length(min = 1), description = PATH_DESCRIPTION)]
    path: String,
}

/// The opaque type of `source`: a Program passes a function, and admission
/// replaces it with the module it extracts, which the Program host encodes
/// as the string this schema describes.
pub(super) const SOURCE_OPAQUE_TYPE: &str = "AuthoredToolSource";

/// Accepts any function a Program declares.
pub(super) const SOURCE_TYPESCRIPT_TYPE: &str = "(...args: never[]) => unknown";

pub(super) fn source_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string",
        "minLength": 1,
        "x-coral-opaque-type": SOURCE_OPAQUE_TYPE,
        "description": "A function declared at the top level of this Program, after you have \
                        called it.",
    })
}

/// What to correct when no extracted module reached `tool`: a call
/// admission could not read, such as one through a computed path, has none.
fn missing_source(arguments: &Value, tool: BuiltinTool) -> Option<ArgumentProblem> {
    arguments
        .as_object()
        .is_none_or(|fields| !fields.contains_key("source"))
        .then(|| {
            ArgumentProblem::new(
                "source",
                format!(
                    "pass a function declared at the top level, through a literal coral.{} call",
                    tool.key()
                ),
            )
        })
}

/// A problem for each of `fields` whose value is empty.
fn empty<'a>(fields: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<ArgumentProblem> {
    fields
        .into_iter()
        .filter(|(_field, value)| value.is_empty())
        .map(|(field, _value)| ArgumentProblem::new(field, "pass a value that is not empty"))
        .collect()
}

/// `addTool`'s or `updateTool`'s arguments in the shape the schema allows.
/// coral-tools checks each value itself.
#[derive(Debug, Clone)]
pub struct ToolSaveArguments {
    path: String,
    description: String,
    source: String,
    expected_revision: Option<u64>,
    policy: Option<ToolAccessState>,
}

impl ToolSaveArguments {
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

    /// The revision `updateTool` expects the tool to be at; always `None`
    /// for `addTool`.
    #[must_use]
    pub const fn expected_revision(&self) -> Option<u64> {
        self.expected_revision
    }

    /// The default Tool Access State the save declares; `None` when the
    /// Program omits it.
    #[must_use]
    pub const fn policy(&self) -> Option<ToolAccessState> {
        self.policy
    }

    /// Reads `addTool`'s arguments.
    ///
    /// # Errors
    ///
    /// Returns the argument to correct.
    pub fn parse_add(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        if let Some(problem) = missing_source(arguments, BuiltinTool::AddTool) {
            return Err(vec![problem]);
        }
        let AddArguments {
            path,
            description,
            source,
            policy,
        } = super::read(arguments).map_err(|problem| vec![problem])?;
        Self::checked(path, description, source, None, policy)
    }

    /// Reads `updateTool`'s arguments.
    ///
    /// # Errors
    ///
    /// Returns the argument to correct.
    pub fn parse_update(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        if let Some(problem) = missing_source(arguments, BuiltinTool::UpdateTool) {
            return Err(vec![problem]);
        }
        let UpdateArguments {
            path,
            description,
            source,
            expected_revision,
            policy,
        } = super::read(arguments).map_err(|problem| vec![problem])?;
        let mut problems = Vec::new();
        if expected_revision == Some(0) {
            problems.push(ArgumentProblem::new(
                "expectedRevision",
                "pass a revision of 1 or more",
            ));
        }
        match Self::checked(path, description, source, expected_revision, policy) {
            Ok(arguments) if problems.is_empty() => Ok(arguments),
            Ok(_arguments) => Err(problems),
            Err(mut found) => {
                found.append(&mut problems);
                Err(found)
            }
        }
    }

    fn checked(
        path: String,
        description: String,
        source: String,
        expected_revision: Option<u64>,
        policy: Option<ToolAccessState>,
    ) -> Result<Self, Vec<ArgumentProblem>> {
        let problems = empty([
            ("path", path.as_str()),
            ("description", description.as_str()),
            ("source", source.as_str()),
        ]);
        if !problems.is_empty() {
            return Err(problems);
        }
        Ok(Self {
            path,
            description,
            source,
            expected_revision,
            policy,
        })
    }
}

/// `removeTool`'s arguments in the shape the schema allows.
#[derive(Debug, Clone)]
pub struct RemoveToolArguments {
    path: String,
}

impl RemoveToolArguments {
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
        let RemoveArguments { path } = super::read(arguments).map_err(|problem| vec![problem])?;
        let problems = empty([("path", path.as_str())]);
        if !problems.is_empty() {
            return Err(problems);
        }
        Ok(Self { path })
    }
}

/// The only arguments `addTool` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_add_arguments() -> Vec<Value> {
    vec![
        serde_json::json!({
            "path": "github.pulls.listStale",
            "description": "List open pull requests with no update in a week.",
            "source": "export default async function () { return []; }",
        }),
        serde_json::json!({
            "path": "github.pulls.listStale",
            "description": "List open pull requests with no update in a week.",
            "source": "export default async function () { return []; }",
            "policy": "approval_required",
        }),
    ]
}

/// The arguments `updateTool` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_update_arguments() -> Vec<Value> {
    let path = "github.pulls.listStale";
    let description = "List open pull requests with no update in a week.";
    let source = "export default async function () { return []; }";
    vec![
        serde_json::json!({ "path": path, "description": description, "source": source }),
        serde_json::json!({
            "path": path,
            "description": description,
            "source": source,
            "expectedRevision": 2,
            "policy": "disabled",
        }),
    ]
}

/// The only arguments `removeTool` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_remove_arguments() -> Vec<Value> {
    vec![serde_json::json!({ "path": "github.pulls.listStale" })]
}

/// What `addTool` and `updateTool` return: the tool as later Programs see
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct SavedToolResult {
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
        description = "Advisory TypeScript diagnostics. The tool is saved even when one is \
                       an error."
    )]
    pub diagnostics: Vec<SavedDiagnostic>,
    /// Whether more diagnostics were found than saved.
    #[schemars(description = "True when Coral found more diagnostics than it kept.")]
    pub diagnostics_truncated: bool,
    /// The revision this save made.
    #[schemars(
        description = "The tool's revision after this save: 1 when added, and one \
                       more at each update. Pass it to updateTool as expectedRevision."
    )]
    pub revision: u64,
}

impl SavedToolResult {
    /// Whether a JSON reader holds every integer exactly.
    #[must_use]
    pub fn is_exact_in_json(&self) -> bool {
        super::search::exact_in_json(self.revision)
            && self
                .diagnostics
                .iter()
                .all(SavedDiagnostic::is_exact_in_json)
    }

    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}

/// What `removeTool` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct RemovedToolResult {
    /// The complete Call Path that no longer names a tool.
    #[schemars(description = "The complete Call Path of the removed tool.")]
    pub call_path: String,
}

impl RemovedToolResult {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}
