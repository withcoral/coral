//! `coral.workspace.search`: Universal Search from inside a Program. The
//! schemas a Program is typed from, the arguments Coral reads against the
//! input schema, and the result types Coral writes against the output schema.
//!
//! The result types are also the shape MCP `search` and `describe` return
//! and the shape Task history retains, so every surface that shows a search
//! page or an exact description shows the same fields. coral-search builds
//! them from its own page and descriptions.

use std::borrow::Cow;

use coral_workspace::WorkspaceAction;
use schemars::generate::Contract;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ArgumentProblem, BuiltinTool, Definition, derived_schema, tool_result, words_schema};
use crate::{ToolHints, ToolPresentation};

/// Any Principal that can inspect the Workspace discovers what it can call.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Inspect;

/// The most Call Paths one exact lookup names.
pub const SEARCH_PATHS_MAX: usize = 10;
/// The longest Call Path an exact lookup accepts, in UTF-8 bytes.
pub const SEARCH_PATH_MAX_BYTES: usize = 1024;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::Search.key(),
        summary: "Search the provider tools and Saved Functions you can call",
        presentation: ToolPresentation::new(
            Some("Search".to_string()),
            "Find the provider tools and Saved Functions you can call, ranked by a query, or \
             read the exact descriptions of the Call Paths you name."
                .to_string(),
            false,
            vec!["discovery".to_string()],
            Some(
                "Pass query for hits, best first, or paths for exact descriptions. Call a hit \
                 by its complete literal call_path. A path that names nothing you can call \
                 comes back in missing."
                    .to_string(),
            ),
        ),
        // Read-only: the default Tool Access State is enabled.
        hints: ToolHints::new(Some(true), None, Some(true), None),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<SearchOutput>(Contract::Serialize),
    }
}

/// `search`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "", extend("dependentRequired" = { "types": ["paths"] }))]
struct Arguments {
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "String",
        length(min = 1),
        description = "Keywords for the capability you need, such as list pull requests. Leave \
                       out people, record titles, IDs, dates and other record values. Omit it, \
                       and paths, to list what you can call."
    )]
    query: Option<String>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "String",
        description = "A namespace from coral.providers.<namespace>, or coral.workspace for \
                       Coral's own tools. It narrows provider tools to one Provider; Saved \
                       Functions stay. Omit it unless you know which service holds the data."
    )]
    provider: Option<String>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "u16",
        range(min = 1, max = 50),
        description = "Maximum hits. Default 10."
    )]
    limit: Option<u16>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "String",
        description = "The next_cursor of the previous page, with the same query and provider."
    )]
    cursor: Option<String>,
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "Vec<String>",
        length(min = 1, max = 10),
        description = "1 to 10 complete Call Paths to read exactly, instead of a query, such as \
                       coral.providers.github.repos.get."
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

/// `search`'s arguments, checked by every rule that needs no Workspace
/// state. Universal Search checks the values themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchArguments {
    /// A ranked page, or a listing without a query.
    Query {
        /// Keywords, or `None` to list.
        query: Option<String>,
        /// The Provider filter.
        provider: Option<String>,
        /// The page size.
        limit: Option<u16>,
        /// The continuation of an earlier page.
        cursor: Option<String>,
    },
    /// The exact descriptions of named Call Paths.
    Paths {
        /// 1 to [`SEARCH_PATHS_MAX`] Call Paths, as the Program wrote them.
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
            return Ok(Self::Query {
                query: arguments.query,
                provider: arguments.provider,
                limit: arguments.limit,
                cursor: arguments.cursor,
            });
        };
        let mut problems: Vec<ArgumentProblem> = [
            ("query", arguments.query.is_some()),
            ("provider", arguments.provider.is_some()),
            ("limit", arguments.limit.is_some()),
            ("cursor", arguments.cursor.is_some()),
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
        Ok(Self::Paths { paths, types })
    }
}

/// What `search` returns: a page of hits for a query, or the exact
/// descriptions of the paths it named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(untagged)]
#[schemars(description = "")]
pub enum SearchOutput {
    /// A ranked page, or a listing.
    #[schemars(description = "")]
    Page(SearchResult),
    /// The exact descriptions of the named Call Paths.
    #[schemars(description = "")]
    Paths(PathsResult),
}

impl SearchOutput {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}

/// One page of Search Hits, and only what the caller acts on besides: the
/// cursor while more remain, and the status of each retriever whose results
/// may be missing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct SearchResult {
    /// The hits, best first; in Call Path order for a listing.
    #[schemars(description = "The hits, best first. Without a query, in Call Path order.")]
    pub hits: Vec<SearchHit>,
    /// Whether another page follows.
    #[schemars(description = "")]
    pub has_more: bool,
    /// The cursor of the next page, while `has_more` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        with = "String",
        description = "Present only while `has_more` is true."
    )]
    pub next_cursor: Option<String>,
    /// The retrievers whose results may be missing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(description = "The retrievers whose results may be missing; absent when none are.")]
    pub retriever_statuses: Vec<SearchRetrieverStatus>,
}

/// One Search Hit, cut to what the caller needs to choose and call it. The
/// Call Path already names the kind and the Provider, the rank reflects the
/// evidence, and an exact lookup gives the rest. A flag appears only when
/// set, a hint only when declared, and a match only where a query term
/// matched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(
    deny_unknown_fields,
    description = "One Search Hit. Call a read from it when its signature line is complete: \
                   no cut marker (`…`) and no type named in its input that the line does not \
                   show. Read its exact description first otherwise, and before a write."
)]
pub struct SearchHit {
    /// The complete Call Path.
    #[schemars(description = "`coral.providers.<namespace>.…` for a provider tool, \
                       `coral.workspace.…` for one of Coral's own, `coral.functions.…` for a \
                       Saved Function.")]
    pub call_path: String,
    /// The callable's signature, cut to its byte limit.
    #[schemars(
        description = "The callable's input members one level deep; a provider tool's result \
                       data types by name, an unnamed object as `{…}`; a Saved Function's result \
                       members one level deep. At most 512 UTF-8 bytes."
    )]
    pub signature_line: String,
    /// The description around the first matched term.
    #[schemars(
        description = "At most 512 UTF-8 bytes of the description, around the first \
                       matched term; `…` marks cut text."
    )]
    pub description_snippet: String,
    /// The guidance around a matched term.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(description = "Present only when a query term matched the guidance.")]
    pub guidance_snippet: Option<String>,
    /// The tags a query term matched.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(description = "The tags a query term matched; absent when none did.")]
    pub matched_tags: Vec<String>,
    /// The top-level arguments a query term matched that the signature
    /// line does not show.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(
        description = "The arguments a query term matched that the signature line does \
                       not show; absent when there are none."
    )]
    pub matched_inputs: Vec<String>,
    /// The hints the Provider declared, when it declared any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "DeclaredHints", description = "")]
    pub hints: Option<DeclaredHints>,
    /// Whether a person must approve each call.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    #[schemars(description = "Present, and true, only when a person must approve each call.")]
    pub approval_required: bool,
    /// Whether the provider tool is deprecated.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    #[schemars(description = "Present, and true, only when the provider tool is deprecated.")]
    pub deprecated: bool,
}

/// The effect hints a Provider declared. An absent field is undeclared, not
/// a protocol default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(
    rename = "Hints",
    deny_unknown_fields,
    description = "Effect hints the Provider declared. An absent field is undeclared, not a \
                   protocol default."
)]
pub struct DeclaredHints {
    /// Whether the Tool only reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "bool", description = "")]
    pub read_only: Option<bool>,
    /// Whether the Tool may destroy data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "bool", description = "")]
    pub destructive: Option<bool>,
    /// Whether repeating a call changes nothing more.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "bool", description = "")]
    pub idempotent: Option<bool>,
    /// Whether the Tool reaches beyond its own service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "bool", description = "")]
    pub open_world: Option<bool>,
}

impl From<ToolHints> for DeclaredHints {
    fn from(hints: ToolHints) -> Self {
        Self {
            read_only: hints.read_only(),
            destructive: hints.destructive(),
            idempotent: hints.idempotent(),
            open_world: hints.open_world(),
        }
    }
}

/// One retriever whose results may be missing from a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(rename = "RetrieverStatus", deny_unknown_fields, description = "")]
pub struct SearchRetrieverStatus {
    /// The kind of Search Entity it ranks.
    #[schemars(description = "")]
    pub retriever: SearchRetriever,
    /// Why its results may be missing.
    #[schemars(description = "")]
    pub state: SearchRetrieverState,
    /// How much it examined.
    #[schemars(description = "")]
    pub coverage: SearchRetrieverCoverage,
}

/// The kind of Search Entity a retriever ranks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchRetriever {
    /// Provider tools.
    ProviderTool,
    /// Saved Functions.
    SavedFunction,
}

impl JsonSchema for SearchRetriever {
    fn schema_name() -> Cow<'static, str> {
        "Retriever".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        words_schema(&["provider_tool", "saved_function"])
    }
}

/// Why a retriever's results may be missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchRetrieverState {
    /// Some units are not indexed yet, or failed.
    Partial,
    /// The retriever could not rank.
    Error,
}

impl JsonSchema for SearchRetrieverState {
    fn schema_name() -> Cow<'static, str> {
        "RetrieverState".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        let mut schema = words_schema(&["partial", "error"]);
        schema.insert(
            "description".to_owned(),
            "- `partial`: some Providers or Saved Functions are not indexed yet, or failed; \
             results may be missing.\n- `error`: the retriever could not rank; its results are \
             missing.\n\nRetry shortly or list without a query."
                .into(),
        );
        schema
    }
}

/// How much of the caller's Search Entities one retriever examined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(rename = "RetrieverCoverage", deny_unknown_fields, description = "")]
pub struct SearchRetrieverCoverage {
    /// The units it could search.
    #[schemars(description = "")]
    pub eligible_units: u64,
    /// The units it searched.
    #[schemars(description = "")]
    pub searched_units: u64,
    /// The units that failed.
    #[schemars(description = "")]
    pub failed_units: u64,
    /// How many Entities it returned.
    #[schemars(description = "")]
    pub returned_count: u64,
    /// Whether it had more than it returned.
    #[schemars(description = "")]
    pub has_more: bool,
}

/// The exact descriptions of the Call Paths a lookup named, and the paths
/// that name nothing the caller can call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct PathsResult {
    /// One description for each path the caller can call, in the order named.
    #[schemars(
        description = "The exact description of each path you can call, in the order \
                       you named them."
    )]
    pub descriptions: Vec<Description>,
    /// Each path that names nothing the caller can call.
    #[schemars(description = "The paths that name nothing you can call.")]
    pub missing: Vec<MissingPath>,
}

/// A named Call Path that is absent, or that the caller cannot discover.
/// The two are indistinguishable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct MissingPath {
    /// The path as the caller wrote it.
    #[schemars(description = "The path as you wrote it.")]
    pub call_path: String,
    /// Up to three Call Paths of the same Call Root that the caller can
    /// call, nearest first.
    #[schemars(
        description = "Up to 3 call paths you can call under the same root, nearest first."
    )]
    pub suggestions: Vec<String>,
}

/// One exact description: the callable's current contract, with the
/// declaration abridged, or only the types the caller named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schemars(description = "")]
pub enum Description {
    /// A provider tool, or one of Coral's own.
    #[schemars(description = "")]
    ProviderTool {
        /// What every kind has.
        #[serde(flatten)]
        authoring: DescriptionAuthoring,
        /// The `provider` filter value of its Provider.
        #[schemars(description = "")]
        provider_access: String,
        /// Its Provider's id.
        #[schemars(
            description = "",
            extend("format" = "uuid", "minLength" = 36, "maxLength" = 36)
        )]
        provider_id: String,
        /// Its Provider's display name.
        #[schemars(description = "")]
        provider_display_name: String,
        /// Its title.
        #[schemars(description = "")]
        title: Option<String>,
        /// Whether it is deprecated.
        #[schemars(description = "")]
        deprecated: bool,
        /// Its tags.
        #[schemars(description = "")]
        tags: Vec<String>,
        /// Its guidance.
        #[schemars(description = "")]
        guidance: Option<String>,
        /// The hints its Provider declared.
        #[schemars(description = "")]
        hints: DeclaredHints,
        /// Whether a person must approve each call.
        #[schemars(description = "")]
        approval_required: bool,
    },
    /// A Saved Function.
    #[schemars(description = "")]
    SavedFunction {
        /// What every kind has.
        #[serde(flatten)]
        authoring: DescriptionAuthoring,
        /// The source and saved diagnostics of the caller's own Saved
        /// Function; absent when the lookup named `types`.
        #[serde(flatten)]
        detail: Option<SavedDetail>,
    },
}

/// What every exact description has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(rename = "Authoring", description = "")]
pub struct DescriptionAuthoring {
    /// The complete Call Path.
    #[schemars(description = "")]
    pub call_path: String,
    /// The description.
    #[schemars(description = "")]
    pub description: String,
    /// The TypeScript declaration, abridged, or the named types.
    #[schemars(description = "")]
    pub typescript_declaration: String,
}

/// A Saved Function's current source and the diagnostics saved with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(description = "")]
pub struct SavedDetail {
    /// The current source.
    #[schemars(description = "")]
    pub source: String,
    /// The advisory diagnostics saved with it.
    #[schemars(description = "")]
    pub diagnostics: Vec<SavedDiagnostic>,
    /// Whether more diagnostics were found than saved.
    #[schemars(description = "")]
    pub diagnostics_truncated: bool,
}

/// The largest integer a JSON reader holds exactly.
const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

/// One advisory TypeScript diagnostic saved with a Saved Function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(rename = "Diagnostic", deny_unknown_fields, description = "")]
pub struct SavedDiagnostic {
    /// How severe it is.
    #[schemars(description = "")]
    pub category: SavedDiagnosticCategory,
    /// The TypeScript diagnostic code.
    #[schemars(description = "", range(max = JSON_SAFE_INTEGER_MAX))]
    pub code: u64,
    /// The compiler's message.
    #[schemars(description = "")]
    pub message: String,
    /// The one-based line, when known.
    #[schemars(description = "", range(max = JSON_SAFE_INTEGER_MAX))]
    pub line: Option<u64>,
    /// The one-based column, when known.
    #[schemars(description = "", range(max = JSON_SAFE_INTEGER_MAX))]
    pub column: Option<u64>,
}

/// How severe a saved diagnostic is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SavedDiagnosticCategory {
    /// An informational message.
    Message,
    /// A warning.
    Warning,
    /// An error, saved without rejecting the save.
    Error,
    /// A suggestion.
    Suggestion,
}

impl JsonSchema for SavedDiagnosticCategory {
    fn schema_name() -> Cow<'static, str> {
        "DiagnosticCategory".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        words_schema(&["message", "warning", "error", "suggestion"])
    }
}

#[cfg(test)]
pub(super) mod tests {
    use serde_json::{Value, json};

    use super::{
        DeclaredHints, Description, DescriptionAuthoring, MissingPath, PathsResult, SavedDetail,
        SavedDiagnostic, SavedDiagnosticCategory, SearchHit, SearchOutput, SearchResult,
        SearchRetriever, SearchRetrieverCoverage, SearchRetrieverState, SearchRetrieverStatus,
    };

    /// Arguments that use every part of `search`'s input schema.
    pub(in crate::builtin) fn valid_arguments() -> Vec<Value> {
        vec![
            json!({}),
            json!({
                "query": "list pull requests",
                "provider": "github",
                "limit": 3,
                "cursor": "next"
            }),
            json!({ "paths": ["coral.providers.github.repos.get", "coral.functions.weekly"] }),
            json!({ "paths": ["coral.providers.github.repos.get"], "types": ["Repository"] }),
        ]
    }

    fn authoring(call_path: &str) -> DescriptionAuthoring {
        DescriptionAuthoring {
            call_path: call_path.to_owned(),
            description: "Get a repository.".to_owned(),
            typescript_declaration: "declare function get(input: {}): Promise<{}>;".to_owned(),
        }
    }

    /// A page with every optional part, an empty page, and an exact lookup
    /// with each kind of description and a missing path.
    #[expect(
        clippy::too_many_lines,
        reason = "one value for each part of the output schema"
    )]
    pub(in crate::builtin) fn results() -> Vec<Value> {
        let hit = SearchHit {
            call_path: "coral.providers.github.repos.get".to_owned(),
            signature_line: "get(input: { owner: string }): Promise<{ data: Repository }>"
                .to_owned(),
            description_snippet: "Get a repository.".to_owned(),
            guidance_snippet: Some("Pass the owner.".to_owned()),
            matched_tags: vec!["repos".to_owned()],
            matched_inputs: vec!["repo".to_owned()],
            hints: Some(DeclaredHints {
                read_only: Some(true),
                destructive: None,
                idempotent: Some(true),
                open_world: Some(false),
            }),
            approval_required: true,
            deprecated: true,
        };
        let bare = SearchHit {
            call_path: "coral.functions.weekly".to_owned(),
            signature_line: "weekly(input: {}): Promise<string>".to_owned(),
            description_snippet: "The weekly report.".to_owned(),
            guidance_snippet: None,
            matched_tags: Vec::new(),
            matched_inputs: Vec::new(),
            hints: None,
            approval_required: false,
            deprecated: false,
        };
        let status = |retriever, state| SearchRetrieverStatus {
            retriever,
            state,
            coverage: SearchRetrieverCoverage {
                eligible_units: 3,
                searched_units: 2,
                failed_units: 1,
                returned_count: 1,
                has_more: false,
            },
        };
        [
            SearchOutput::Page(SearchResult {
                hits: vec![hit, bare],
                has_more: true,
                next_cursor: Some("next".to_owned()),
                retriever_statuses: vec![
                    status(SearchRetriever::ProviderTool, SearchRetrieverState::Partial),
                    status(SearchRetriever::SavedFunction, SearchRetrieverState::Error),
                ],
            }),
            SearchOutput::Page(SearchResult {
                hits: Vec::new(),
                has_more: false,
                next_cursor: None,
                retriever_statuses: Vec::new(),
            }),
            SearchOutput::Paths(PathsResult {
                descriptions: vec![
                    Description::ProviderTool {
                        authoring: authoring("coral.providers.github.repos.get"),
                        provider_access: "github".to_owned(),
                        provider_id: "0190f1a2-0000-7000-8000-000000000000".to_owned(),
                        provider_display_name: "GitHub".to_owned(),
                        title: None,
                        deprecated: false,
                        tags: vec!["repos".to_owned()],
                        guidance: Some("Pass the owner.".to_owned()),
                        hints: DeclaredHints::from(crate::ToolHints::default()),
                        approval_required: false,
                    },
                    Description::SavedFunction {
                        authoring: authoring("coral.functions.weekly"),
                        detail: Some(SavedDetail {
                            source: "export default async function weekly() {}".to_owned(),
                            diagnostics: vec![SavedDiagnostic {
                                category: SavedDiagnosticCategory::Warning,
                                code: 6133,
                                message: "unused".to_owned(),
                                line: Some(1),
                                column: None,
                            }],
                            diagnostics_truncated: false,
                        }),
                    },
                    Description::SavedFunction {
                        authoring: authoring("coral.functions.daily"),
                        detail: None,
                    },
                ],
                missing: vec![MissingPath {
                    call_path: "coral.providers.notion.retrievePage".to_owned(),
                    suggestions: vec!["coral.providers.notion.retrieveAPage".to_owned()],
                }],
            }),
            SearchOutput::Paths(PathsResult {
                descriptions: Vec::new(),
                missing: Vec::new(),
            }),
        ]
        .into_iter()
        .map(SearchOutput::into_tool_result)
        .collect()
    }
}
