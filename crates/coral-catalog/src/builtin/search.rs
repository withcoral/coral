//! `coral.workspace.search`: Universal Search from inside a Program. The
//! schemas a Program is typed from, the arguments Coral reads against the
//! input schema, and the result types Coral writes against the output schema.
//!
//! The result types are also the shape of the `search` and `describe`
//! records Task history retains from before discovery moved into Programs,
//! so a retained page or description shows the same fields a Program reads.
//! coral-search builds them from its own page and descriptions.

use std::borrow::Cow;

use coral_workspace::WorkspaceAction;
use schemars::generate::Contract;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    ArgumentProblem, BuiltinTool, Declared, Definition, derived_schema, tool_result, words_schema,
};
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
        input: derived_schema::<Input>(Contract::Deserialize),
        output: derived_schema::<SearchOutput>(Contract::Serialize),
        declared: Declared::Overloads(OVERLOADS),
        enabled_by_default: false,
    }
}

/// `search`'s input as a Program is typed from it: one mode per call. Each
/// branch is one overload of the declaration, so a Program that passes
/// `query` gets a page and one that passes `paths` gets exact descriptions
/// (see [`OVERLOADS`]). Coral reads [`Arguments`], which accepts the same
/// values, so a call that mixes the modes gets a reason for each argument
/// to leave out.
#[derive(JsonSchema)]
#[schemars(untagged, description = "")]
#[expect(
    dead_code,
    reason = "the schema is derived from these types; Coral reads Arguments"
)]
enum Input {
    Query(QueryInput),
    Paths(PathsInput),
    Types(TypesInput),
}

/// The result branch of [`SearchOutput`] that each branch of [`Input`]
/// returns, in order: a page for a query, exact descriptions for paths.
pub(super) const OVERLOADS: &[usize] = &[0, 1, 1];

/// A ranked page, or a listing without a query.
#[derive(JsonSchema)]
#[schemars(inline, deny_unknown_fields, description = "")]
#[expect(
    dead_code,
    reason = "the schema is derived from these types; Coral reads Arguments"
)]
struct QueryInput {
    #[serde(default)]
    #[schemars(
        with = "String",
        length(min = 1),
        description = "Keywords for the capability you need, such as list pull requests. Leave \
                       out people, record titles, IDs, dates and other record values. Omit it \
                       to list what you can call."
    )]
    query: Option<String>,
    #[serde(default)]
    #[schemars(
        with = "String",
        description = "A namespace from coral.providers.<namespace>, or coral.workspace for \
                       Coral's own tools. It narrows provider tools to one Provider; Saved \
                       Functions stay. Omit it unless you know which service holds the data."
    )]
    provider: Option<String>,
    #[serde(default)]
    #[schemars(
        with = "u16",
        range(min = 1, max = 50),
        description = "Maximum hits. Default 10."
    )]
    limit: Option<u16>,
    #[serde(default)]
    #[schemars(
        with = "u32",
        description = "The next_offset of the previous page, with the same query and provider."
    )]
    offset: Option<u32>,
}

/// The exact descriptions of 1 to 10 Call Paths.
#[derive(JsonSchema)]
#[schemars(inline, deny_unknown_fields, description = "")]
#[expect(
    dead_code,
    reason = "the schema is derived from these types; Coral reads Arguments"
)]
struct PathsInput {
    #[schemars(
        length(min = 1, max = 10),
        inner(length(max = SEARCH_PATH_MAX_BYTES)),
        description = "1 to 10 complete Call Paths to read exactly, each at most 1024 UTF-8 \
                       bytes, such as coral.providers.github.repos.get."
    )]
    paths: Vec<String>,
}

/// Named types of one Call Path's declaration.
#[derive(JsonSchema)]
#[schemars(inline, deny_unknown_fields, description = "")]
#[expect(
    dead_code,
    reason = "the schema is derived from these types; Coral reads Arguments"
)]
struct TypesInput {
    #[schemars(
        schema_with = "one_path_schema",
        description = "The one complete Call Path whose types to read, at most 1024 UTF-8 \
                       bytes."
    )]
    paths: (String,),
    #[schemars(
        length(max = 32),
        description = "Type names from its declaration, returned whole instead of the inputs \
                       and the function."
    )]
    types: Vec<String>,
}

/// Exactly one Call Path, which TypeScript reads as `[string]`.
fn one_path_schema(_generator: &mut SchemaGenerator) -> Schema {
    schemars::json_schema!({
        "type": "array",
        "prefixItems": [{ "type": "string", "maxLength": SEARCH_PATH_MAX_BYTES }],
        "items": false,
        "minItems": 1,
    })
}

/// `search`'s arguments as Coral reads them: every argument of every mode,
/// so a call that mixes modes reads, and [`SearchArguments::parse`] names
/// each argument to leave out.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    #[serde(default, deserialize_with = "super::present")]
    query: Option<String>,
    #[serde(default, deserialize_with = "super::present")]
    provider: Option<String>,
    #[serde(default, deserialize_with = "super::present")]
    limit: Option<u16>,
    #[serde(default, deserialize_with = "super::present")]
    offset: Option<u32>,
    #[serde(default, deserialize_with = "super::present")]
    paths: Option<Vec<String>>,
    #[serde(default, deserialize_with = "super::present")]
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
        /// How many ranked results to skip: the `next_offset` of an earlier
        /// page.
        offset: Option<u32>,
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
                offset: arguments.offset,
            });
        };
        let mut problems: Vec<ArgumentProblem> = [
            ("query", arguments.query.is_some()),
            ("provider", arguments.provider.is_some()),
            ("limit", arguments.limit.is_some()),
            ("offset", arguments.offset.is_some()),
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
        // As the schema reads it: `types`, even empty, names one path's types.
        if arguments.types.is_some() && paths.len() != 1 {
            problems.push(ArgumentProblem::new("types", "pass types with one path"));
        }
        let types = arguments.types.unwrap_or_default();
        if !problems.is_empty() {
            return Err(problems);
        }
        Ok(Self::Paths { paths, types })
    }
}

/// What `search` returns: a page of hits for a query, or the exact
/// descriptions of the paths it named. [`OVERLOADS`] names each variant by
/// its position, so the declaration gives each input mode its own result.
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

    /// Whether a JSON reader holds every integer in it exactly. One past
    /// 2^53 − 1 comes only from corrupt stored data, and a reader such as V8
    /// would round it, so a surface fails rather than return it.
    #[must_use]
    pub fn is_exact_in_json(&self) -> bool {
        match self {
            Self::Page(page) => page.retriever_statuses.iter().all(|status| {
                let coverage = &status.coverage;
                [
                    coverage.eligible_units,
                    coverage.searched_units,
                    coverage.failed_units,
                    coverage.returned_count,
                ]
                .into_iter()
                .all(exact_in_json)
            }),
            Self::Paths(paths) => paths.descriptions.iter().all(Description::is_exact_in_json),
        }
    }
}

/// Whether a JSON reader holds `value` exactly.
const fn exact_in_json(value: u64) -> bool {
    value <= JSON_SAFE_INTEGER_MAX
}

/// One page of Search Hits, and only what the caller acts on besides: the
/// next offset while more remain, and the status of each retriever whose
/// results may be missing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct SearchResult {
    /// The hits, best first; in Call Path order for a listing.
    #[schemars(description = "The hits, best first. Without a query, in Call Path order.")]
    pub hits: Vec<SearchHit>,
    /// The `offset` of the next page; `None` on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        with = "u32",
        description = "The `offset` of the next page; absent on the last page."
    )]
    pub next_offset: Option<u32>,
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
    description = "One Search Hit. Call a read from it when it is complete. Read its exact \
                   description first otherwise, and before a write."
)]
pub struct SearchHit {
    /// The complete Call Path.
    #[schemars(description = "`coral.providers.<namespace>.…` for a provider tool, \
                       `coral.workspace.…` for one of Coral's own, `coral.functions.…` for a \
                       Saved Function.")]
    pub call_path: String,
    /// The callable's signature, within its byte limit.
    #[schemars(
        description = "The callable's input members one level deep, required ones first, then \
                       its result, which the line always keeps: a provider tool's result data as its \
                       top-level field names when they fit, or by type name; for an MCP tool that \
                       declares no output, a note that the result is text, usually JSON, in \
                       `data.content[0].text`; and a Saved Function's result members one level \
                       deep. `{…}` marks an object the line does not show, a bare `…` a type it \
                       cannot read, and a last `…` member or final `…` what it cut to fit. At most \
                       512 UTF-8 bytes."
    )]
    pub signature_line: String,
    /// Whether the signature line's input is complete enough to call from.
    #[schemars(
        description = "Whether you can call from the signature line: its input has no `…`, \
                       names no type that the line does not show, and is the callable's only \
                       overload. What the line leaves out of the result does not count."
    )]
    pub complete: bool,
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
        length(max = 3),
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

impl Description {
    /// Whether a JSON reader holds every integer in it exactly: a saved
    /// diagnostic's code, line and column. See
    /// [`SearchOutput::is_exact_in_json`].
    #[must_use]
    pub fn is_exact_in_json(&self) -> bool {
        match self {
            Self::ProviderTool { .. } | Self::SavedFunction { detail: None, .. } => true,
            Self::SavedFunction {
                detail: Some(detail),
                ..
            } => detail
                .diagnostics
                .iter()
                .all(SavedDiagnostic::is_exact_in_json),
        }
    }
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

impl SavedDiagnostic {
    /// Whether a JSON reader holds its code, line and column exactly. Only
    /// corrupt stored data holds one it would round.
    #[must_use]
    pub fn is_exact_in_json(&self) -> bool {
        [Some(self.code), self.line, self.column]
            .into_iter()
            .flatten()
            .all(exact_in_json)
    }
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
        DeclaredHints, Description, DescriptionAuthoring, MissingPath, PathsResult,
        SEARCH_PATH_MAX_BYTES, SavedDetail, SavedDiagnostic, SavedDiagnosticCategory,
        SearchArguments, SearchHit, SearchOutput, SearchResult, SearchRetriever,
        SearchRetrieverCoverage, SearchRetrieverState, SearchRetrieverStatus,
    };

    /// Arguments that use every part of `search`'s input schema.
    pub(in crate::builtin) fn valid_arguments() -> Vec<Value> {
        vec![
            json!({}),
            json!({
                "query": "list pull requests",
                "provider": "github",
                "limit": 3,
                "offset": 20
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

    /// A hit with every optional part, and one with none.
    fn hits() -> Vec<SearchHit> {
        let hit = SearchHit {
            call_path: "coral.providers.github.repos.get".to_owned(),
            signature_line: "get(input: { owner: string }): Promise<{ data: Repository }>"
                .to_owned(),
            complete: true,
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
            complete: false,
            description_snippet: "The weekly report.".to_owned(),
            guidance_snippet: None,
            matched_tags: Vec::new(),
            matched_inputs: Vec::new(),
            hints: None,
            approval_required: false,
            deprecated: false,
        };
        vec![hit, bare]
    }

    /// A page with every optional part, an empty page, and an exact lookup
    /// with each kind of description and a missing path.
    pub(in crate::builtin) fn results() -> Vec<Value> {
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
                hits: hits(),
                next_offset: Some(20),
                retriever_statuses: vec![
                    status(SearchRetriever::ProviderTool, SearchRetrieverState::Partial),
                    status(SearchRetriever::SavedFunction, SearchRetrieverState::Error),
                ],
            }),
            SearchOutput::Page(SearchResult {
                hits: Vec::new(),
                next_offset: None,
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

    /// Asserts that the input schema and Coral's reading both accept, or
    /// both refuse, each of `cases`.
    fn schema_and_parser_agree(cases: &[(Value, bool)]) {
        let schema = jsonschema::validator_for(&super::definition().input).expect("schema");
        for (arguments, accepted) in cases {
            assert_eq!(schema.is_valid(arguments), *accepted, "schema: {arguments}");
            assert_eq!(
                SearchArguments::parse(arguments).is_ok(),
                *accepted,
                "parser: {arguments}"
            );
        }
    }

    // Catches: Coral reading an empty `types` beside several paths, which a
    // typed Program cannot send. The one-change mutations of
    // `valid_arguments` never reach it.
    #[test]
    fn types_go_with_one_path_even_when_empty() {
        schema_and_parser_agree(&[
            (json!({ "paths": ["a", "b"], "types": [] }), false),
            (json!({ "paths": ["a"], "types": [] }), true),
        ]);
    }

    // Catches: a schema that accepts a path longer than Coral reads, so a
    // typed Program learns the limit only from a refusal.
    #[test]
    fn the_schema_bounds_each_path_as_coral_reads_it() {
        let at_limit = "a".repeat(SEARCH_PATH_MAX_BYTES);
        let past_limit = "a".repeat(SEARCH_PATH_MAX_BYTES + 1);
        schema_and_parser_agree(&[
            (json!({ "paths": [at_limit] }), true),
            (json!({ "paths": ["a", past_limit] }), false),
            (json!({ "paths": [at_limit], "types": ["T"] }), true),
            (json!({ "paths": [past_limit], "types": ["T"] }), false),
        ]);
    }

    // Catches: a hit that calls every `…` a cut, so a caller cannot tell an
    // object the line leaves out, or a type it cannot read, from cut text; or
    // one that lets a `…` in the result stop a call the input allows.
    #[test]
    fn a_hit_says_what_each_signature_marker_means() {
        let schema = serde_json::to_value(schemars::schema_for!(SearchHit)).expect("schema");
        let text = |pointer: &str| {
            schema
                .pointer(pointer)
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("{pointer} in {schema}"))
                .to_owned()
        };
        let hit = text("/description");
        assert!(!hit.contains("cut marker"), "{hit}");
        let line = text("/properties/signature_line/description");
        for meaning in [
            "`{…}` marks an object the line does not show",
            "a bare `…` a type it cannot read",
            "a last `…` member or final `…` what it cut to fit",
        ] {
            assert!(line.contains(meaning), "{line} lacks {meaning}");
        }
        let complete = text("/properties/complete/description");
        assert!(
            complete.contains("its input has no `…`")
                && complete.contains("What the line leaves out of the result does not count"),
            "{complete}"
        );
    }

    // Catches: an integer past 2^53 − 1, in a page's coverage or a saved
    // diagnostic, that a surface would hand a JSON reader to round.
    #[test]
    fn an_output_is_exact_in_json_only_within_the_safe_range() {
        const SAFE: u64 = 9_007_199_254_740_991;
        let page = |units| {
            SearchOutput::Page(SearchResult {
                hits: Vec::new(),
                next_offset: None,
                retriever_statuses: vec![SearchRetrieverStatus {
                    retriever: SearchRetriever::SavedFunction,
                    state: SearchRetrieverState::Partial,
                    coverage: SearchRetrieverCoverage {
                        eligible_units: units,
                        searched_units: 0,
                        failed_units: 0,
                        returned_count: 0,
                        has_more: false,
                    },
                }],
            })
        };
        let paths = |code, line, column| {
            SearchOutput::Paths(PathsResult {
                descriptions: vec![Description::SavedFunction {
                    authoring: authoring("coral.functions.weekly"),
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
                }],
                missing: Vec::new(),
            })
        };
        for (output, exact) in [
            (page(SAFE), true),
            (page(SAFE + 1), false),
            (paths(SAFE, Some(SAFE), Some(SAFE)), true),
            (paths(SAFE + 1, None, None), false),
            (paths(6133, Some(SAFE + 1), None), false),
            (paths(6133, Some(1), Some(SAFE + 1)), false),
        ] {
            assert_eq!(output.is_exact_in_json(), exact, "{output:?}");
        }
    }
}
