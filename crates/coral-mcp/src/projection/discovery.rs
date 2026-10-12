//! Search Hits and complete public TypeScript descriptions, not authoritative Provider
//! schemas.
use coral_functions::SavedFunction;
use coral_search::{ExactAuthoringDescription, SearchPage};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::ToolFailure;
use super::diagnostics::{Diagnostic, saved_diagnostics};

#[derive(Deserialize, Serialize, JsonSchema)]
pub(crate) struct SearchResult {
    hits: Vec<SearchHit>,
    next_cursor: Option<String>,
    has_more: bool,
    /// One status per retriever: whether it ranked, and how much it searched.
    retriever_statuses: Vec<RetrieverStatus>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RetrieverStatus {
    retriever: Retriever,
    state: RetrieverState,
    coverage: RetrieverCoverage,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Retriever {
    Operation,
    SavedFunction,
}

/// `partial` and `error` mean results may be missing: retry shortly or list
/// without a query. Only a search that an earlier Coral retained holds
/// `skipped`: its Provider filter left this kind out.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RetrieverState {
    ResultsFound,
    Empty,
    Skipped,
    Partial,
    Error,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RetrieverCoverage {
    eligible_units: u64,
    searched_units: u64,
    failed_units: u64,
    returned_count: u64,
    has_more: bool,
}

/// One Search Hit. Call a read from it when its signature line is complete:
/// no cut marker (`…`) and no type named in its input that the line does not
/// show. Describe first otherwise, and before a write.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SearchHit {
    Operation {
        #[serde(flatten)]
        common: HitCommon,
        provider_display_name: String,
        title: Option<String>,
        hints: Hints,
        approval_required: bool,
        deprecated: bool,
        /// Present only when a query term matched the guidance.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        guidance_snippet: Option<String>,
        /// The tags a query term matched; absent when none did.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        matched_tags: Vec<String>,
    },
    SavedFunction {
        #[serde(flatten)]
        common: HitCommon,
    },
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct HitCommon {
    call_path: String,
    /// The callable's input members one level deep and its result type by
    /// name, at most 512 UTF-8 bytes.
    signature_line: String,
    /// At most 512 UTF-8 bytes of the description, around the first matched
    /// term; `…` marks cut text.
    description_snippet: String,
    evidence: Evidence,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Evidence {
    fields: Vec<String>,
    paths: Vec<String>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Description<Detail> {
    Operation {
        #[serde(flatten)]
        authoring: Authoring,
        provider_access: String,
        #[schemars(schema_with = "crate::arguments::uuid_schema")]
        provider_id: String,
        provider_display_name: String,
        title: Option<String>,
        deprecated: bool,
        tags: Vec<String>,
        guidance: Option<String>,
        hints: Hints,
        approval_required: bool,
    },
    SavedFunction {
        #[serde(flatten)]
        authoring: Authoring,
        #[serde(flatten)]
        detail: Detail,
    },
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub(crate) struct Authoring {
    call_path: String,
    description: String,
    typescript_declaration: String,
}

/// Effect hints the Provider declared. An absent field is undeclared, not a
/// protocol default.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Hints {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "bool")]
    read_only: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "bool")]
    destructive: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "bool")]
    idempotent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "bool")]
    open_world: Option<bool>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub(crate) struct SavedDetail {
    source: String,
    diagnostics: Vec<Diagnostic>,
    diagnostics_truncated: bool,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SavedFunctionResult<'a> {
    call_path: String,
    description: &'a str,
    declaration: &'a str,
    diagnostics: Vec<Diagnostic>,
    diagnostics_truncated: bool,
}

pub(crate) fn search(page: &SearchPage) -> Result<SearchResult, ToolFailure> {
    from_snapshot(&coral_search::projection::search(page))
}

/// A Saved Function's detail is absent when `describe` returned only the
/// `types` it was asked for.
pub(crate) fn exact_description(
    exact: &ExactAuthoringDescription,
) -> Result<Description<Option<SavedDetail>>, ToolFailure> {
    from_snapshot(&coral_search::projection::exact_description(exact))
}

// Task history and MCP must expose the same complete discovery response. Keep
// domain-to-public field mapping in the existing shared snapshot projection.
// These MCP-owned types drive both serialization and schemas; reject drift
// rather than silently discarding a field that history retained (serde flatten
// cannot be combined reliably with deny_unknown_fields).
fn from_snapshot<T: DeserializeOwned + Serialize>(snapshot: &Value) -> Result<T, ToolFailure> {
    let projected = T::deserialize(snapshot).map_err(|_error| ToolFailure::internal())?;
    let serialized = serde_json::to_value(&projected).map_err(|_error| ToolFailure::internal())?;
    if &serialized != snapshot {
        return Err(ToolFailure::internal());
    }
    Ok(projected)
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
