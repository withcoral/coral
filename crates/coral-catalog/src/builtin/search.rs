//! `coral.workspace.search`: Universal Search from inside a Program. The
//! schemas a Program is typed from and the arguments Coral reads against the
//! input schema. coral-search owns the page and the descriptions, so the output
//! schema states their shape loosely and the result is that JSON unchanged.

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{ArgumentProblem, BuiltinTool, Definition, derived_schema, tool_result};
use crate::{ToolHints, ToolPresentation};

/// Any Principal that can inspect the Workspace discovers what it can call.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Inspect;

/// The most Call Paths one exact lookup names.
pub const SEARCH_PATHS_MAX: usize = 10;
/// The longest Call Path an exact lookup accepts, in UTF-8 bytes.
const SEARCH_PATH_MAX_BYTES: usize = 1024;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::Search.key(),
        summary: "Search provider tools and Saved Functions, or read their declarations",
        presentation: ToolPresentation::new(
            Some("Search".to_string()),
            "Search the provider tools and Saved Functions you can call, or read the full \
             declaration of exact Call Paths."
                .to_string(),
            false,
            vec!["discovery".to_string()],
            Some(
                "Pass query to rank results: the top 3 hits carry a compact declaration, and \
                 declarations: true adds full descriptions instead. Or pass paths to read exact Call Paths; a path nobody can \
                 reach comes back in missing."
                    .to_string(),
            ),
        ),
        // Read-only: the default Tool Access State is enabled.
        hints: ToolHints::new(Some(true), None, Some(true), None),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: output_schema(),
    }
}

/// The page coral-search projects, with the description this Tool adds to a
/// hit and the paths an exact lookup could not find.
fn output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "hits": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "call_path": { "type": "string" },
                        "signature_line": { "type": "string" },
                        "description_snippet": { "type": "string" },
                        "declaration": {
                            "type": "string",
                            "description": "The inputs and the top of the result. Present on a \
                                            query's top 3 hits."
                        },
                        "description": {
                            "type": "object",
                            "description": "The full description, with typescript_declaration. \
                                            Present for an exact lookup, and for the top hits of \
                                            a query with declarations: true.",
                            "properties": {
                                "typescript_declaration": { "type": "string" }
                            },
                            "required": ["typescript_declaration"],
                            "additionalProperties": true
                        }
                    },
                    "required": ["call_path"],
                    "additionalProperties": true
                }
            },
            "has_more": { "type": "boolean" },
            "next_cursor": { "type": "string" },
            "retriever_statuses": {
                "type": "array",
                "items": { "type": "object", "additionalProperties": true }
            },
            "missing": {
                "type": "array",
                "description": "The paths of an exact lookup that name nothing you can call.",
                "items": { "type": "string" }
            }
        },
        "required": ["hits", "has_more"],
        "additionalProperties": false
    })
}

/// `search`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "", extend("dependentRequired" = { "types": ["paths"] }))]
struct Arguments {
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "String",
        description = "Keywords for the capability you need, such as list pull requests. Omit \
                       it, and paths, to page through what you can call."
    )]
    query: Option<String>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "String",
        description = "A namespace from coral.providers.<namespace>, as start_task lists them, \
                       or coral.workspace. Omit when you cannot tell which service holds the data."
    )]
    provider: Option<String>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "u16",
        range(min = 1, max = 50),
        description = "Maximum results. Default 10."
    )]
    limit: Option<u16>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "String",
        description = "The next_cursor from the previous page, with the same query and provider."
    )]
    cursor: Option<String>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "bool",
        description = "Add the full description to each hit, best first, until the page reaches \
                       32 KiB, instead of the compact declaration on the top 3 hits."
    )]
    declarations: Option<bool>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "Vec<String>",
        length(min = 1, max = 10),
        description = "Exact Call Paths to read in full, instead of a query."
    )]
    paths: Option<Vec<String>>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "Vec<String>",
        length(max = 32),
        description = "With one path: type names from its declaration, returned whole instead \
                       of the inputs and the function."
    )]
    types: Option<Vec<String>>,
}

/// `search`'s arguments, checked by every rule that needs no Workspace state.
#[derive(Debug, Clone)]
pub enum SearchArguments {
    /// A ranked page, or a listing without a query.
    Ranked {
        /// Keywords, or `None` to list.
        query: Option<String>,
        /// The Provider filter.
        provider: Option<String>,
        /// The page size.
        limit: Option<u16>,
        /// The continuation of an earlier page.
        cursor: Option<String>,
        /// Whether to add full descriptions to the top hits.
        declarations: bool,
    },
    /// Full descriptions of exact Call Paths.
    Exact {
        /// 1 to [`SEARCH_PATHS_MAX`] Call Paths.
        paths: Vec<String>,
        /// Type names of the one path, or empty.
        types: Vec<String>,
    },
}

impl SearchArguments {
    /// Reads the arguments: the shape first, then which ones go together.
    ///
    /// # Errors
    ///
    /// Returns every argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        // serde also reads a struct from a sequence, which the schema forbids.
        if !arguments.is_object() {
            return Err(vec![ArgumentProblem::new("", "pass an object")]);
        }
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        let Some(paths) = arguments.paths else {
            if arguments.types.is_some() {
                return Err(vec![ArgumentProblem::new("types", "pass types with paths")]);
            }
            return Ok(Self::Ranked {
                query: arguments.query,
                provider: arguments.provider,
                limit: arguments.limit,
                cursor: arguments.cursor,
                declarations: arguments.declarations.unwrap_or(false),
            });
        };
        let mut problems: Vec<ArgumentProblem> = [
            ("query", arguments.query.is_some()),
            ("provider", arguments.provider.is_some()),
            ("limit", arguments.limit.is_some()),
            ("cursor", arguments.cursor.is_some()),
            ("declarations", arguments.declarations.is_some()),
        ]
        .into_iter()
        .filter(|(_field, present)| *present)
        .map(|(field, _present)| ArgumentProblem::new(field, "leave it out with paths"))
        .collect();
        if paths.is_empty() || paths.len() > SEARCH_PATHS_MAX {
            problems.push(ArgumentProblem::new(
                "paths",
                format!("pass 1 to {SEARCH_PATHS_MAX} Call Paths"),
            ));
        }
        if paths.iter().any(|path| path.len() > SEARCH_PATH_MAX_BYTES) {
            problems.push(ArgumentProblem::new(
                "paths",
                format!("a Call Path is at most {SEARCH_PATH_MAX_BYTES} UTF-8 bytes"),
            ));
        }
        let types = arguments.types.unwrap_or_default();
        if !types.is_empty() && paths.len() != 1 {
            problems.push(ArgumentProblem::new("types", "pass types with one path"));
        }
        if !problems.is_empty() {
            return Err(problems);
        }
        Ok(Self::Exact { paths, types })
    }
}

/// The Tool Result holding `data`, the page this Tool built.
#[must_use]
pub fn search_tool_result(data: &Value) -> Value {
    tool_result(data)
}

/// Arguments that use every part of `search`'s input schema.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![
        json!({}),
        json!({
            "query": "list pull requests",
            "provider": "github",
            "limit": 3,
            "cursor": "next",
            "declarations": true
        }),
        json!({ "paths": ["coral.providers.github.repos.get"] }),
        json!({ "paths": ["coral.providers.github.repos.get"], "types": ["Repository"] }),
    ]
}
