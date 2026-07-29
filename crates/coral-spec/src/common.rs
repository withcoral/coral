#![allow(
    missing_docs,
    reason = "This module defines many field-heavy declarative source-spec types."
)]

//! Shared source-spec DSL types and helpers.
//!
//! These types model the backend-agnostic parts of the Coral source-spec DSL:
//! source identity, filters, request templating, response extraction, typed
//! columns, and pagination.

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::de::Error as _;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::{ManifestError, ParsedTemplate, Result};

/// Source SQL names the runtime owns. Kept in step with `RESERVED_SCHEMA_NAMES`
/// in `coral-engine`'s registry: a name the engine refuses at registration has
/// to fail manifest validation too, or the manifest validator accepts a source
/// that can never register.
const RESERVED_SOURCE_SCHEMA_NAMES: &[&str] = &["coral", "coral_admin", "datafusion", "public"];

/// Arrow field metadata key marking a source-authored column as excluded from
/// observed-value indexing.
pub const DO_NOT_INDEX_COLUMN_METADATA_KEY: &str = "coral.do_not_index";

/// Common top-level source metadata shared by every backend source spec.
#[derive(Debug, Clone, Serialize)]
pub struct SourceManifestCommon {
    pub dsl_version: u32,
    pub name: String,
    pub version: String,
    pub description: String,
    pub test_queries: Vec<String>,
}

impl SourceManifestCommon {
    pub(crate) fn new(
        dsl_version: u32,
        name: String,
        version: String,
        description: String,
        test_queries: Vec<String>,
    ) -> Self {
        Self {
            dsl_version,
            name,
            version,
            description,
            test_queries,
        }
    }
}

pub(crate) fn validate_source_name(name: &str) -> Result<()> {
    validate_reserved_source_schema_name(name, "source name")
}

pub(crate) fn validate_reserved_source_schema_name(name: &str, label: &str) -> Result<()> {
    // Case-insensitive: legacy (pre-v4) manifests are not held to v4's
    // `[a-z][a-z0-9_]*` rule, so `DataFusion` would otherwise pass validation
    // while the runtime still treats that spelling as its default catalog.
    if RESERVED_SOURCE_SCHEMA_NAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(name))
    {
        return Err(ManifestError::validation(format!(
            "{label} '{name}' is reserved and cannot be used by manifests"
        )));
    }
    Ok(())
}

pub(crate) fn validate_test_queries(source_name: &str, test_queries: &[String]) -> Result<()> {
    for (index, query) in test_queries.iter().enumerate() {
        if query.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' test_queries[{index}] must not be empty"
            )));
        }
    }
    Ok(())
}

/// Supported source-spec backends.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceBackend {
    Http,
    File,
    Mcp,
}

/// The normalized scalar type vocabulary shared by source specs, the query
/// runtime, and catalog surfaces.
///
/// This is the hub every other scalar vocabulary converts through: v4 IR
/// scalars lower into it, and the engine maps it into runtime-specific
/// (Arrow) types. The variant spellings ("Utf8", "Int64", ...) are a wire
/// contract pinned by the `PascalCase` serde representation and the manifest
/// JSON schema.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "PascalCase")]
pub enum ManifestDataType {
    Utf8,
    Int64,
    Boolean,
    Float64,
    Timestamp,
    /// Stored as UTF-8 containing valid JSON. Hints to users and tooling
    /// that the column is queryable with JSON functions (`json_get`,
    /// `json_get_str`, `json_as_text`, etc.); the JSON functions also
    /// work on plain `Utf8` columns whose values happen to be JSON.
    Json,
}

impl ManifestDataType {
    /// Every manifest data type, in canonical declaration order.
    ///
    /// [`FromStr`](std::str::FromStr) and the lattice enforcement tests
    /// treat this array as the source of truth for the variant set.
    pub const ALL: [Self; 6] = {
        // Exhaustiveness witness: adding a variant breaks this match, and
        // the new variant must be added to the array below in the same
        // edit.
        const fn witness(data_type: ManifestDataType) {
            match data_type {
                ManifestDataType::Utf8
                | ManifestDataType::Int64
                | ManifestDataType::Boolean
                | ManifestDataType::Float64
                | ManifestDataType::Timestamp
                | ManifestDataType::Json => {}
            }
        }
        let _ = witness;
        [
            Self::Utf8,
            Self::Int64,
            Self::Boolean,
            Self::Float64,
            Self::Timestamp,
            Self::Json,
        ]
    };

    /// Returns the source-manifest spelling for this data type.
    ///
    /// This must stay aligned with the enum's `PascalCase` serde
    /// representation and the `manifest_data_type` definition in the manifest
    /// JSON schema.
    #[must_use]
    pub fn as_manifest_str(self) -> &'static str {
        match self {
            Self::Utf8 => "Utf8",
            Self::Int64 => "Int64",
            Self::Boolean => "Boolean",
            Self::Float64 => "Float64",
            Self::Timestamp => "Timestamp",
            Self::Json => "Json",
        }
    }
}

impl std::fmt::Display for ManifestDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_manifest_str())
    }
}

impl std::str::FromStr for ManifestDataType {
    type Err = ManifestError;

    fn from_str(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|data_type| data_type.as_manifest_str() == s)
            .ok_or_else(|| {
                let expected = Self::ALL.map(Self::as_manifest_str).join(", ");
                ManifestError::validation(format!(
                    "unsupported data type '{s}' in source manifest; expected one of: {expected}"
                ))
            })
    }
}

/// One request or auth header declared in the source spec.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct HeaderSpec {
    pub name: String,
    #[serde(flatten)]
    pub value: ValueSourceSpec,
}

/// Shared table metadata used by all backend-specific table specs.
#[derive(Debug, Clone, Serialize)]
pub struct TableCommon {
    pub name: String,
    pub description: String,
    pub guide: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub require_guide_read: bool,
    pub filters: Vec<FilterSpec>,
    pub fetch_limit_default: Option<usize>,
    pub search_limits: Option<SearchLimitsSpec>,
    pub detail_hints: Vec<DetailHintSpec>,
    pub columns: Vec<ColumnSpec>,
}

impl TableCommon {
    #[expect(
        clippy::too_many_arguments,
        reason = "Field-heavy source-spec table metadata stays explicit at construction sites."
    )]
    pub(crate) fn new(
        name: String,
        description: String,
        guide: String,
        require_guide_read: bool,
        filters: Vec<FilterSpec>,
        fetch_limit_default: Option<usize>,
        search_limits: Option<SearchLimitsSpec>,
        detail_hints: Vec<DetailHintSpec>,
        columns: Vec<ColumnSpec>,
    ) -> Self {
        Self {
            name,
            description,
            guide,
            require_guide_read,
            filters,
            fetch_limit_default,
            search_limits,
            detail_hints,
            columns,
        }
    }
}

/// How a filter value is matched against `SQL` predicates.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum FilterMode {
    /// Pushes down `=` only (current behaviour for all existing providers).
    #[default]
    Equality,
    /// Compatibility-only virtual-filter search mode for existing table
    /// manifests. New provider-native search surfaces should use
    /// [`SourceTableFunctionKind::Search`] functions instead.
    Search,
    /// Pushes down `LIKE` as a substring/contains filter.
    Contains,
}

/// One declared filter that can be used as a complete exact lookup from SQL.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FilterSpec {
    pub name: String,
    #[serde(rename = "type", default = "default_filter_data_type")]
    pub data_type: ManifestDataType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub mode: FilterMode,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub lookup_key: bool,
}

fn default_filter_data_type() -> ManifestDataType {
    ManifestDataType::Utf8
}

/// Source-scoped table-function semantic class.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceTableFunctionKind {
    /// Generic row-returning provider operation.
    #[default]
    Table,
    /// Provider-native retrieval/search operation that returns ranked candidates.
    Search,
}

impl SourceTableFunctionKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Search => "search",
        }
    }
}

impl FilterMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Equality => "equality",
            Self::Search => "search",
            Self::Contains => "contains",
        }
    }
}

/// Highest per-call result count a source-spec search surface may request.
pub(crate) const MAX_SEARCH_TOP_K: usize = 1_000;
/// Highest number of provider search calls a single query may make.
pub(crate) const MAX_SEARCH_CALLS_PER_QUERY: usize = 100;
/// Highest aggregate candidate budget for one query across repeated search calls.
pub(crate) const MAX_SEARCH_CANDIDATES_PER_QUERY: usize = 10_000;

/// Bounded retrieval settings for search-like provider surfaces.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SearchLimitsSpec {
    pub default_top_k: usize,
    pub max_top_k: usize,
    pub max_calls_per_query: usize,
}

impl SearchLimitsSpec {
    pub fn validate(&self, context: &str) -> Result<()> {
        if self.default_top_k == 0 {
            return Err(ManifestError::validation(format!(
                "{context}.default_top_k must be > 0"
            )));
        }
        if self.max_top_k == 0 {
            return Err(ManifestError::validation(format!(
                "{context}.max_top_k must be > 0"
            )));
        }
        if self.max_top_k > MAX_SEARCH_TOP_K {
            return Err(ManifestError::validation(format!(
                "{context}.max_top_k must be <= {MAX_SEARCH_TOP_K}"
            )));
        }
        if self.default_top_k > self.max_top_k {
            return Err(ManifestError::validation(format!(
                "{context}.default_top_k must be <= max_top_k"
            )));
        }
        if self.max_calls_per_query == 0 {
            return Err(ManifestError::validation(format!(
                "{context}.max_calls_per_query must be > 0"
            )));
        }
        if self.max_calls_per_query > MAX_SEARCH_CALLS_PER_QUERY {
            return Err(ManifestError::validation(format!(
                "{context}.max_calls_per_query must be <= {MAX_SEARCH_CALLS_PER_QUERY}"
            )));
        }
        let Some(candidate_budget) = self.max_top_k.checked_mul(self.max_calls_per_query) else {
            return Err(ManifestError::validation(format!(
                "{context}.max_top_k * max_calls_per_query exceeds supported range"
            )));
        };
        if candidate_budget > MAX_SEARCH_CANDIDATES_PER_QUERY {
            return Err(ManifestError::validation(format!(
                "{context}.max_top_k * max_calls_per_query must be <= {MAX_SEARCH_CANDIDATES_PER_QUERY}"
            )));
        }
        Ok(())
    }
}

/// Machine-readable path from a search candidate row to a detail table.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DetailHintSpec {
    pub table: String,
    pub search_result_column: String,
    pub detail_filter: String,
    pub purpose: String,
}

/// Declarative source-scoped table-valued function.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceTableFunctionSpec {
    pub name: String,
    #[serde(default)]
    pub kind: SourceTableFunctionKind,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub guide: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub require_guide_read: bool,
    #[serde(default)]
    pub fetch_limit_default: Option<usize>,
    #[serde(default)]
    pub search_limits: Option<SearchLimitsSpec>,
    #[serde(default)]
    pub detail_hints: Vec<DetailHintSpec>,
    #[serde(default)]
    pub args: Vec<TableFunctionArgSpec>,
    #[serde(default)]
    pub request: RequestSpec,
    #[serde(default)]
    pub response: ResponseSpec,
    #[serde(default)]
    pub pagination: PaginationSpec,
    #[serde(default)]
    pub columns: Vec<ColumnSpec>,
}

/// One argument accepted by a source-scoped table-valued function.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableFunctionArgSpec {
    pub name: String,
    #[serde(rename = "type", default = "default_table_function_arg_data_type")]
    pub data_type: ManifestDataType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub values: Vec<String>,
    /// A typed runtime default imported for a generated DSL v4 argument.
    ///
    /// DSL v3 manifests cannot author this field. The outer option records
    /// whether an imported default was present, so an explicit JSON `null`
    /// remains distinct from no default.
    #[serde(
        default,
        deserialize_with = "deserialize_declared_default",
        skip_serializing_if = "Option::is_none"
    )]
    pub default: Option<DeclaredDefaultValue>,
    pub bind: FunctionArgBinding,
}

/// A type-preserving imported DSL v4 table-function argument default.
///
/// This wrapper deliberately preserves JSON `null`: using `Option<Value>`
/// directly would collapse an explicit null into the same state as a missing
/// field during deserialization.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(transparent)]
pub struct DeclaredDefaultValue(Value);

impl DeclaredDefaultValue {
    #[must_use]
    pub fn new(value: Value) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn value(&self) -> &Value {
        &self.0
    }

    #[must_use]
    pub fn into_value(self) -> Value {
        self.0
    }
}

pub(crate) fn deserialize_declared_default<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<DeclaredDefaultValue>, D::Error>
where
    D: Deserializer<'de>,
{
    Value::deserialize(deserializer).map(|value| Some(DeclaredDefaultValue::new(value)))
}

/// Result-display and entity-identity mapping for a DSL v4 Universal Search
/// route.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UniversalSearchResultMappingSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identity_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attributes: Vec<String>,
}

fn default_table_function_arg_data_type() -> ManifestDataType {
    ManifestDataType::Utf8
}

/// How a table function argument contributes to the provider request.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct FunctionArgBinding {
    pub arg: String,
}

/// The base request template for one HTTP table or request route.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RequestSpec {
    #[serde(default)]
    pub method: HttpMethod,
    #[serde(default)]
    pub path: ParsedTemplate,
    #[serde(default)]
    pub query: Vec<QueryParamSpec>,
    #[serde(default)]
    pub body: BodySpec,
    #[serde(default)]
    pub headers: Vec<HeaderSpec>,
}

/// A conditional request override selected when the listed filters are present.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RequestRouteSpec {
    pub when_filters: Vec<String>,
    #[serde(flatten)]
    pub request: RequestSpec,
}

/// Supported HTTP methods in the source-spec DSL.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    #[default]
    GET,
    POST,
}

/// One query parameter emitted into an HTTP request.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryParamSpec {
    pub name: String,
    #[serde(flatten)]
    pub value: ValueSourceSpec,
}

/// One body field emitted into an HTTP request payload.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BodyFieldSpec {
    pub path: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when_arg: Option<String>,
    #[serde(flatten)]
    pub value: ValueSourceSpec,
}

/// How the request body is shaped before being sent.
///
/// Accepts two YAML forms for backwards compatibility:
/// - The legacy array form (`body: [{ path, from, ... }]`) is treated as the
///   `Json` variant.
/// - The tagged object form (`body: { format: json|text, ... }`) opts into a
///   specific shape.
#[derive(Debug, Clone)]
pub enum BodySpec {
    /// Build a JSON object from a list of path-addressed fields.
    Json { fields: Vec<BodyFieldSpec> },
    /// Send a raw text body rendered from a single value source. Intended for
    /// SQL-over-HTTP and similar APIs that accept a free-form string body.
    Text { content: ValueSourceSpec },
}

impl Default for BodySpec {
    fn default() -> Self {
        Self::Json { fields: Vec::new() }
    }
}

impl BodySpec {
    /// Returns true when this body has no content to send.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Json { fields } => fields.is_empty(),
            Self::Text { .. } => false,
        }
    }

    /// Returns the JSON body fields if this is a JSON body, otherwise empty.
    #[must_use]
    pub fn json_fields(&self) -> &[BodyFieldSpec] {
        match self {
            Self::Json { fields } => fields,
            Self::Text { .. } => &[],
        }
    }
}

impl<'de> Deserialize<'de> for BodySpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Array(Vec<BodyFieldSpec>),
            Tagged(TaggedBody),
        }

        #[derive(Deserialize)]
        #[serde(tag = "format", rename_all = "snake_case", deny_unknown_fields)]
        enum TaggedBody {
            Json {
                #[serde(default)]
                fields: Vec<BodyFieldSpec>,
            },
            Text {
                content: ValueSourceSpec,
            },
        }

        match Raw::deserialize(deserializer).map_err(D::Error::custom)? {
            Raw::Array(fields) | Raw::Tagged(TaggedBody::Json { fields }) => {
                Ok(BodySpec::Json { fields })
            }
            Raw::Tagged(TaggedBody::Text { content }) => Ok(BodySpec::Text { content }),
        }
    }
}

impl Serialize for BodySpec {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Json { fields } => {
                let mut state = serializer.serialize_struct("BodySpec", 2)?;
                state.serialize_field("format", "json")?;
                state.serialize_field("fields", fields)?;
                state.end()
            }
            Self::Text { content } => {
                let mut state = serializer.serialize_struct("BodySpec", 2)?;
                state.serialize_field("format", "text")?;
                state.serialize_field("content", content)?;
                state.end()
            }
        }
    }
}

/// How a source-spec request value is populated at runtime.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum ValueSourceSpec {
    Template {
        template: ParsedTemplate,
    },
    OneOf {
        values: Vec<ValueSourceSpec>,
    },
    Literal {
        value: Value,
    },
    Filter {
        key: String,
        #[serde(default)]
        default: Option<Value>,
    },
    FilterInt {
        key: String,
        #[serde(default)]
        default: Option<i64>,
    },
    FilterBool {
        key: String,
        #[serde(default)]
        default: Option<bool>,
    },
    FilterStringArray {
        key: String,
        #[serde(default)]
        default: Option<Vec<String>>,
    },
    FilterSplit {
        key: String,
        separator: String,
        part: usize,
    },
    FilterSplitInt {
        key: String,
        separator: String,
        part: usize,
    },
    Arg {
        key: String,
        #[serde(default)]
        default: Option<Value>,
    },
    ArgInt {
        key: String,
        #[serde(default)]
        default: Option<i64>,
    },
    ArgBool {
        key: String,
        #[serde(default)]
        default: Option<bool>,
    },
    ArgSplit {
        key: String,
        separator: String,
        part: usize,
    },
    ArgSplitInt {
        key: String,
        separator: String,
        part: usize,
    },
    Input {
        key: String,
    },
    Bearer {
        key: String,
    },
    State {
        key: String,
    },
    NowEpochMinusSeconds {
        seconds: i64,
    },
}

/// Rules for interpreting the response payload returned by one HTTP table.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ResponseSpec {
    #[serde(default)]
    pub format: ResponseBodyFormat,
    #[serde(default)]
    pub rows_path: Vec<String>,
    #[serde(default)]
    pub ok_path: Vec<String>,
    #[serde(default)]
    pub error_path: Vec<String>,
    #[serde(default)]
    pub allow_404_empty: bool,
    #[serde(default)]
    pub row_strategy: RowStrategy,
}

/// How the raw response body is decoded before row extraction runs.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResponseBodyFormat {
    /// Standard JSON document (the response is parsed once).
    #[default]
    Json,
    /// Newline-delimited JSON (e.g. `ClickHouse`'s `JSONEachRow` format).
    /// Each non-empty line is parsed as one JSON value and collected into an
    /// array before row extraction.
    JsonEachRow,
}

/// How the engine converts a selected response value into logical rows.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RowStrategy {
    #[default]
    Direct,
    SeriesPointList,
    DictEntries,
}

/// Pagination configuration for one HTTP table.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaginationSpec {
    #[serde(default)]
    pub mode: PaginationMode,
    #[serde(default)]
    pub page_size: Option<PageSizeSpec>,
    #[serde(default)]
    pub cursor_param: Option<String>,
    #[serde(default)]
    pub cursor_body_path: Vec<String>,
    #[serde(default)]
    pub response_cursor_path: Vec<String>,
    #[serde(default)]
    pub response_cursor_header: Option<String>,
    #[serde(default)]
    pub page_param: Option<String>,
    #[serde(default)]
    pub page_start: i64,
    #[serde(default = "default_page_step")]
    pub page_step: i64,
    #[serde(default)]
    pub offset_param: Option<String>,
    #[serde(default)]
    pub offset_start: i64,
    #[serde(default)]
    pub offset_step: Option<i64>,
    #[serde(default)]
    pub link_header_require_results: bool,
    #[serde(default)]
    pub next_url_header: Option<String>,
    /// Path to a response-body property holding the complete URL of the next
    /// page, as `OData`'s `@odata.nextLink` does.
    ///
    /// Distinct from [`Self::response_cursor_path`]: that path yields a token
    /// to place into a request parameter, this one yields a URL to request as
    /// it stands.
    #[serde(default)]
    pub next_url_path: Vec<String>,
    #[serde(default)]
    pub max_pages: Option<usize>,
}

impl Default for PaginationSpec {
    fn default() -> Self {
        Self {
            mode: PaginationMode::default(),
            page_size: None,
            cursor_param: None,
            cursor_body_path: Vec::new(),
            response_cursor_path: Vec::new(),
            response_cursor_header: None,
            page_param: None,
            page_start: 0,
            page_step: default_page_step(),
            offset_param: None,
            offset_start: 0,
            offset_step: None,
            link_header_require_results: false,
            next_url_header: None,
            next_url_path: Vec::new(),
            max_pages: None,
        }
    }
}

/// Fully validated pagination configuration ready for engine use.
#[derive(Debug, Clone)]
pub struct ValidatedPagination {
    pub mode: ValidatedPaginationMode,
    pub page_size: Option<PageSizeSpec>,
    pub cursor_param: Option<String>,
    pub cursor_body_path: Vec<String>,
    pub response_cursor_path: Vec<String>,
    pub response_cursor_header: Option<String>,
    pub page_param: Option<String>,
    pub page_start: i64,
    pub page_step: i64,
    pub link_header_require_results: bool,
    pub next_url_header: Option<String>,
    pub max_pages: Option<usize>,
}

/// The validated pagination mode selected for one HTTP table.
#[derive(Debug, Clone)]
pub enum ValidatedPaginationMode {
    None,
    Auto,
    CursorQuery,
    CursorBody,
    Page,
    Offset(OffsetPagination),
    LinkHeader,
    NextUrlBody(NextUrlBodyPagination),
}

impl ValidatedPaginationMode {
    /// Whether this mode advances by requesting a complete URL the response
    /// supplied, rather than by mutating the request Coral built.
    ///
    /// The fetch loop asks this instead of testing variants, so that a mode
    /// added later either follows its next URL or is a deliberate omission at
    /// this one site — a `matches!` per call site would silently answer "no"
    /// and quietly stop after page one.
    #[must_use]
    pub fn follows_response_next_url(&self) -> bool {
        match self {
            Self::LinkHeader | Self::Auto | Self::NextUrlBody(_) => true,
            Self::None | Self::CursorQuery | Self::CursorBody | Self::Page | Self::Offset(_) => {
                false
            }
        }
    }
}

/// Validated settings for following a next-page URL out of a response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextUrlBodyPagination {
    path: Vec<String>,
}

impl NextUrlBodyPagination {
    /// Path from the response root to the property holding the next-page URL.
    #[must_use]
    pub fn path(&self) -> &[String] {
        &self.path
    }
}

/// Validated typed offset-pagination settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetPagination {
    pub param: String,
    pub start: i64,
    step: OffsetStep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OffsetStep {
    Explicit(i64),
    PageSize,
}

impl PaginationSpec {
    pub(crate) fn validate(&self, schema: &str, table: &str) -> Result<()> {
        self.validated(schema, table)?;
        Ok(())
    }

    pub fn validated(&self, schema: &str, table: &str) -> Result<ValidatedPagination> {
        let page_size = self.validated_page_size(schema, table)?;
        if matches!(self.max_pages, Some(0)) {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} pagination.max_pages must be > 0"
            )));
        }
        let mode = self.validated_mode(schema, table, page_size.is_some())?;
        Ok(ValidatedPagination {
            mode,
            page_size,
            cursor_param: self.cursor_param.clone(),
            cursor_body_path: self.cursor_body_path.clone(),
            response_cursor_path: self.response_cursor_path.clone(),
            response_cursor_header: self.response_cursor_header.clone(),
            page_param: self.page_param.clone(),
            page_start: self.page_start,
            page_step: self.page_step,
            link_header_require_results: self.link_header_require_results,
            next_url_header: self.next_url_header.clone(),
            max_pages: self.max_pages,
        })
    }

    fn validated_mode(
        &self,
        schema: &str,
        table: &str,
        has_page_size: bool,
    ) -> Result<ValidatedPaginationMode> {
        if self
            .response_cursor_header
            .as_deref()
            .is_some_and(|header| header.trim().is_empty())
        {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} pagination.response_cursor_header must not be empty"
            )));
        }
        if self
            .next_url_header
            .as_deref()
            .is_some_and(|header| header.trim().is_empty())
        {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} pagination.next_url_header must not be empty"
            )));
        }

        match self.mode {
            PaginationMode::None => Ok(ValidatedPaginationMode::None),
            PaginationMode::Auto => Ok(ValidatedPaginationMode::Auto),
            PaginationMode::CursorQuery => {
                if self.cursor_param.is_none() {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table} pagination.mode=cursor_query requires cursor_param"
                    )));
                }
                if self.response_cursor_path.is_empty() && self.response_cursor_header.is_none() {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table} pagination.mode=cursor_query requires response_cursor_path or response_cursor_header"
                    )));
                }
                Ok(ValidatedPaginationMode::CursorQuery)
            }
            PaginationMode::CursorBody => {
                if self.cursor_body_path.is_empty() {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table} pagination.mode=cursor_body requires cursor_body_path"
                    )));
                }
                if self.response_cursor_path.is_empty() && self.response_cursor_header.is_none() {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table} pagination.mode=cursor_body requires response_cursor_path or response_cursor_header"
                    )));
                }
                Ok(ValidatedPaginationMode::CursorBody)
            }
            PaginationMode::Page => {
                if self.page_param.is_none() {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table} pagination.mode=page requires page_param"
                    )));
                }
                if self.page_step <= 0 {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table} pagination.page_step must be > 0"
                    )));
                }
                Ok(ValidatedPaginationMode::Page)
            }
            PaginationMode::Offset => {
                let param = self.offset_param.clone().ok_or_else(|| {
                    ManifestError::validation(format!(
                        "{schema}.{table} pagination.mode=offset requires offset_param"
                    ))
                })?;
                let step = match self.offset_step {
                    Some(offset_step) if offset_step > 0 => OffsetStep::Explicit(offset_step),
                    Some(_) => {
                        return Err(ManifestError::validation(format!(
                            "{schema}.{table} pagination.offset_step must be > 0"
                        )));
                    }
                    None if has_page_size => OffsetStep::PageSize,
                    None => {
                        return Err(ManifestError::validation(format!(
                            "{schema}.{table} pagination.mode=offset requires offset_step or page_size"
                        )));
                    }
                };
                Ok(ValidatedPaginationMode::Offset(OffsetPagination {
                    param,
                    start: self.offset_start,
                    step,
                }))
            }
            PaginationMode::LinkHeader => Ok(ValidatedPaginationMode::LinkHeader),
            PaginationMode::NextUrlBody => self.validated_next_url_body_mode(schema, table),
        }
    }

    fn validated_next_url_body_mode(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<ValidatedPaginationMode> {
        if self.next_url_path.is_empty() {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} pagination.mode=next_url_body requires next_url_path"
            )));
        }
        if self
            .next_url_path
            .iter()
            .any(|segment| segment.trim().is_empty() || segment.trim() != segment)
        {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} pagination.next_url_path segments must not be blank or padded"
            )));
        }
        Ok(ValidatedPaginationMode::NextUrlBody(
            NextUrlBodyPagination {
                path: self.next_url_path.clone(),
            },
        ))
    }

    fn validated_page_size(&self, schema: &str, table: &str) -> Result<Option<PageSizeSpec>> {
        let Some(page_size) = &self.page_size else {
            return Ok(None);
        };

        if page_size.default == 0 {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} pagination.page_size.default must be > 0"
            )));
        }
        if page_size.max == 0 {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} pagination.page_size.max must be > 0"
            )));
        }
        if page_size.query_param.is_none() && page_size.body_path.is_empty() {
            return Err(ManifestError::validation(format!(
                "{schema}.{table} pagination.page_size must define query_param or body_path"
            )));
        }

        Ok(Some(page_size.clone()))
    }
}

impl OffsetPagination {
    pub fn resolve_step(&self, page_size: Option<usize>, schema: &str, table: &str) -> Result<i64> {
        match self.step {
            OffsetStep::Explicit(step) => Ok(step),
            OffsetStep::PageSize => i64::try_from(page_size.ok_or_else(|| {
                ManifestError::validation(format!(
                    "{schema}.{table} offset pagination requires page_size"
                ))
            })?)
            .map_err(|_err| {
                ManifestError::validation(format!(
                    "{schema}.{table} page_size exceeds supported i64 range"
                ))
            }),
        }
    }
}

fn default_page_step() -> i64 {
    1
}

/// Supported pagination modes in the source-spec DSL.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PaginationMode {
    #[default]
    None,
    Auto,
    CursorQuery,
    CursorBody,
    Page,
    Offset,
    LinkHeader,
    NextUrlBody,
}

/// Page-size settings shared by several pagination modes.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PageSizeSpec {
    pub default: usize,
    pub max: usize,
    #[serde(default)]
    pub query_param: Option<String>,
    #[serde(default)]
    pub body_path: Vec<String>,
}

/// One declared output column for a manifest table.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: ManifestDataType,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    #[serde(rename = "virtual")]
    pub r#virtual: bool,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub expr: Option<ExprSpec>,
    /// Excludes this column from observed-value indexing when true.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub do_not_index: bool,
}

impl ColumnSpec {
    #[must_use]
    pub fn resolved_expr(&self) -> ExprSpec {
        self.expr.clone().unwrap_or_else(|| ExprSpec::Path {
            path: vec![self.name.clone()],
        })
    }
}

fn default_nullable() -> bool {
    true
}

/// Column expressions supported by the source-spec DSL.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExprSpec {
    Path {
        path: Vec<String>,
    },
    Coalesce {
        exprs: Vec<ExprSpec>,
    },
    FromFilter {
        key: String,
    },
    FromArg {
        key: String,
    },
    Literal {
        value: Value,
    },
    Null,
    JoinArray {
        path: Vec<String>,
        #[serde(default = "default_separator")]
        separator: String,
    },
    JoinArrayPath {
        path: Vec<String>,
        item_path: Vec<String>,
        #[serde(default = "default_separator")]
        separator: String,
    },
    TagValue {
        path: Vec<String>,
        key: String,
        #[serde(default = "default_key_field")]
        key_field: String,
        #[serde(default = "default_value_field")]
        value_field: String,
    },
    IfPresent {
        check: Box<ExprSpec>,
        then_value: String,
    },
    JoinTagValues {
        path: Vec<String>,
        key: String,
        #[serde(default = "default_key_field")]
        key_field: String,
        #[serde(default = "default_value_field")]
        value_field: String,
        #[serde(default = "default_separator")]
        separator: String,
    },
    FirstArrayItemPath {
        path: Vec<String>,
        item_path: Vec<String>,
    },
    ObjectFilterPath {
        path: Vec<String>,
        filter_key: String,
        item_path: Vec<String>,
    },
    CurrentRow,
    FormatTimestamp {
        expr: Box<ExprSpec>,
        #[serde(default)]
        input: TimestampInput,
    },
    Base64Decode {
        expr: Box<ExprSpec>,
    },
    Replace {
        expr: Box<ExprSpec>,
        from: String,
        to: String,
    },
    Template {
        template: ParsedTemplate,
        values: HashMap<String, ExprSpec>,
    },
}

/// Declares how to interpret the raw value before formatting as ISO-8601.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampInput {
    /// Seconds since Unix epoch (integer or float).
    #[default]
    Seconds,
    /// Milliseconds since Unix epoch.
    Milliseconds,
    /// ISO 8601 / RFC 3339 timestamp string.
    Iso8601,
}

fn default_separator() -> String {
    ",".to_string()
}

fn default_key_field() -> String {
    "key".to_string()
}

fn default_value_field() -> String {
    "value".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::http::test_http_table_spec;
    use std::collections::HashSet;

    #[test]
    fn runtime_table_function_arg_default_distinguishes_missing_from_explicit_null() {
        let missing = TableFunctionArgSpec {
            name: "options".to_string(),
            data_type: ManifestDataType::Json,
            required: false,
            values: Vec::new(),
            default: None,
            bind: FunctionArgBinding {
                arg: "options".to_string(),
            },
        };
        assert!(missing.default.is_none());

        let explicit_null = TableFunctionArgSpec {
            default: Some(DeclaredDefaultValue::new(serde_json::Value::Null)),
            ..missing
        };
        assert_eq!(
            explicit_null
                .default
                .as_ref()
                .map(DeclaredDefaultValue::value),
            Some(&serde_json::Value::Null)
        );
        let encoded =
            serde_json::to_value(explicit_null).expect("serialize explicit null runtime default");
        assert_eq!(encoded.get("default"), Some(&serde_json::Value::Null));

        let decoded = serde_json::from_value::<TableFunctionArgSpec>(encoded)
            .expect("deserialize explicit null runtime default");
        assert_eq!(
            decoded.default.as_ref().map(DeclaredDefaultValue::value),
            Some(&serde_json::Value::Null)
        );
    }

    #[test]
    fn resolve_request_returns_default_when_no_routes() {
        let table = test_http_table_spec(
            "items",
            vec![],
            vec![],
            RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items").expect("template"),
                query: vec![],
                body: BodySpec::default(),
                headers: vec![],
            },
        );
        let filters = HashSet::new();
        assert_eq!(table.resolve_request(&filters).path, "/items");
    }

    #[test]
    fn resolve_request_picks_matching_route() {
        let mut table = test_http_table_spec(
            "items",
            vec![],
            vec![FilterSpec {
                name: "id".into(),
                data_type: ManifestDataType::Utf8,
                required: false,
                mode: FilterMode::default(),
                description: String::new(),
                lookup_key: false,
            }],
            RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items").expect("template"),
                query: vec![],
                body: BodySpec::default(),
                headers: vec![],
            },
        );
        table.requests = vec![RequestRouteSpec {
            when_filters: vec!["id".into()],
            request: RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items/{{filter.id}}").expect("template"),
                query: vec![],
                body: BodySpec::default(),
                headers: vec![],
            },
        }];
        let mut filters = HashSet::new();
        assert_eq!(table.resolve_request(&filters).path, "/items");
        filters.insert("id".to_string());
        assert_eq!(table.resolve_request(&filters).path, "/items/{{filter.id}}");
    }

    #[test]
    fn resolve_request_prefers_most_specific_matching_route() {
        let mut table = test_http_table_spec(
            "items",
            vec![],
            vec![
                FilterSpec {
                    name: "id".into(),
                    data_type: ManifestDataType::Utf8,
                    required: false,
                    mode: FilterMode::default(),
                    description: String::new(),
                    lookup_key: false,
                },
                FilterSpec {
                    name: "org".into(),
                    data_type: ManifestDataType::Utf8,
                    required: false,
                    mode: FilterMode::default(),
                    description: String::new(),
                    lookup_key: false,
                },
            ],
            RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items").expect("template"),
                query: vec![],
                body: BodySpec::default(),
                headers: vec![],
            },
        );
        table.requests = vec![
            RequestRouteSpec {
                when_filters: vec!["id".into()],
                request: RequestSpec {
                    method: HttpMethod::GET,
                    path: ParsedTemplate::parse("/items/by-id/{{filter.id}}").expect("template"),
                    query: vec![],
                    body: BodySpec::default(),
                    headers: vec![],
                },
            },
            RequestRouteSpec {
                when_filters: vec!["id".into(), "org".into()],
                request: RequestSpec {
                    method: HttpMethod::GET,
                    path: ParsedTemplate::parse("/orgs/{{filter.org}}/items/{{filter.id}}")
                        .expect("template"),
                    query: vec![],
                    body: BodySpec::default(),
                    headers: vec![],
                },
            },
        ];

        let filters = HashSet::from(["id".to_string(), "org".to_string()]);
        assert_eq!(
            table.resolve_request(&filters).path,
            "/orgs/{{filter.org}}/items/{{filter.id}}"
        );
    }

    #[test]
    fn body_spec_legacy_array_deserializes_as_json_variant() {
        let spec: BodySpec = serde_json::from_value(serde_json::json!([
            { "path": ["query"], "from": "literal", "value": "x" }
        ]))
        .unwrap();
        let BodySpec::Json { fields } = spec else {
            panic!("expected legacy array to deserialize as Json variant");
        };
        assert_eq!(fields.len(), 1);
        let field = fields.first().expect("legacy body field");
        assert_eq!(field.path, vec!["query".to_string()]);
    }

    #[test]
    fn body_spec_tagged_text_deserializes() {
        let spec: BodySpec = serde_json::from_value(serde_json::json!({
            "format": "text",
            "content": { "from": "literal", "value": "SELECT 1" }
        }))
        .unwrap();
        match spec {
            BodySpec::Text {
                content: ValueSourceSpec::Literal { value },
            } => assert_eq!(value, serde_json::json!("SELECT 1")),
            other => panic!("expected text body, got {other:?}"),
        }
    }

    #[test]
    fn body_spec_tagged_json_with_empty_fields_defaults() {
        let spec: BodySpec = serde_json::from_value(serde_json::json!({
            "format": "json"
        }))
        .unwrap();
        let BodySpec::Json { fields } = spec else {
            panic!("expected json body");
        };
        assert!(fields.is_empty());
    }

    #[test]
    fn response_body_format_defaults_to_json() {
        let spec: ResponseSpec =
            serde_json::from_value(serde_json::json!({ "rows_path": ["data"] })).unwrap();
        assert_eq!(spec.format, ResponseBodyFormat::Json);
    }

    #[test]
    fn response_body_format_parses_json_each_row() {
        let spec: ResponseSpec =
            serde_json::from_value(serde_json::json!({ "format": "json_each_row" })).unwrap();
        assert_eq!(spec.format, ResponseBodyFormat::JsonEachRow);
    }

    #[test]
    fn filter_mode_defaults_to_equality() {
        let spec: FilterSpec = serde_json::from_value(serde_json::json!({
            "name": "org"
        }))
        .unwrap();
        assert_eq!(spec.mode, FilterMode::Equality);
        assert!(!spec.lookup_key);
    }

    #[test]
    fn filter_mode_deserializes_contains() {
        let spec: FilterSpec = serde_json::from_value(serde_json::json!({
            "name": "q",
            "mode": "contains"
        }))
        .unwrap();
        assert_eq!(spec.mode, FilterMode::Contains);
    }

    #[test]
    fn filter_mode_rejects_unknown_value() {
        let result = serde_json::from_value::<FilterSpec>(serde_json::json!({
            "name": "q",
            "mode": "fuzzy"
        }));
        let error = result.expect_err("unknown filter mode should fail");
        assert!(error.to_string().contains("unknown variant"));
    }

    #[test]
    fn filter_mode_deserializes_legacy_search_value() {
        let spec: FilterSpec = serde_json::from_value(serde_json::json!({
            "name": "q",
            "mode": "search"
        }))
        .unwrap();
        assert_eq!(spec.mode, FilterMode::Search);
    }

    #[test]
    fn filter_metadata_defaults_to_utf8_with_empty_description() {
        let spec: FilterSpec = serde_json::from_value(serde_json::json!({
            "name": "q"
        }))
        .unwrap();
        assert_eq!(spec.data_type, ManifestDataType::Utf8);
        assert_eq!(spec.description, "");
    }

    #[test]
    fn table_function_kind_defaults_to_table() {
        let spec: SourceTableFunctionSpec = serde_json::from_value(serde_json::json!({
            "name": "issues",
            "request": { "path": "/issues" }
        }))
        .unwrap();
        assert_eq!(spec.kind, SourceTableFunctionKind::Table);
    }

    #[test]
    fn table_function_kind_deserializes_search() {
        let spec: SourceTableFunctionSpec = serde_json::from_value(serde_json::json!({
            "name": "search_issues",
            "kind": "search",
            "request": { "path": "/search/issues" },
            "search_limits": {
                "default_top_k": 10,
                "max_top_k": 100,
                "max_calls_per_query": 1
            }
        }))
        .unwrap();
        assert_eq!(spec.kind, SourceTableFunctionKind::Search);
        assert_eq!(spec.search_limits.unwrap().default_top_k, 10);
    }

    #[test]
    fn table_function_arg_data_type_defaults_to_utf8_and_deserializes() {
        let spec: SourceTableFunctionSpec = serde_json::from_value(serde_json::json!({
            "name": "search_issues",
            "args": [
                { "name": "q", "bind": { "arg": "query" } },
                { "name": "include_archived", "type": "Boolean", "bind": { "arg": "archived" } }
            ],
            "request": { "path": "/search/issues" }
        }))
        .unwrap();

        let [query, include_archived] = spec.args.as_slice() else {
            panic!("expected two table function args");
        };
        assert_eq!(query.data_type, ManifestDataType::Utf8);
        assert_eq!(include_archived.data_type, ManifestDataType::Boolean);
    }

    #[test]
    fn filter_lookup_key_field_deserializes() {
        let spec: FilterSpec = serde_json::from_value(serde_json::json!({
            "name": "repo",
            "lookup_key": true
        }))
        .unwrap();

        assert!(spec.lookup_key);
    }

    #[test]
    fn pagination_validated_builds_typed_offset_mode_with_explicit_step() {
        let pagination = PaginationSpec {
            mode: PaginationMode::Offset,
            offset_param: Some("offset".to_string()),
            offset_start: 50,
            offset_step: Some(25),
            ..PaginationSpec::default()
        };

        let validated = pagination.validated("demo", "items").unwrap();
        let ValidatedPaginationMode::Offset(offset) = validated.mode else {
            panic!("expected typed offset pagination");
        };

        assert_eq!(offset.param, "offset");
        assert_eq!(offset.start, 50);
        assert_eq!(offset.resolve_step(None, "demo", "items").unwrap(), 25);
        assert!(validated.page_size.is_none());
    }

    #[test]
    fn pagination_validated_builds_typed_offset_mode_from_page_size() {
        let pagination = PaginationSpec {
            mode: PaginationMode::Offset,
            page_size: Some(PageSizeSpec {
                default: 20,
                max: 100,
                query_param: Some("limit".to_string()),
                body_path: vec![],
            }),
            offset_param: Some("start".to_string()),
            ..PaginationSpec::default()
        };

        let validated = pagination.validated("demo", "items").unwrap();
        let ValidatedPaginationMode::Offset(offset) = validated.mode else {
            panic!("expected typed offset pagination");
        };

        assert_eq!(offset.param, "start");
        assert_eq!(offset.start, 0);
        assert_eq!(offset.resolve_step(Some(20), "demo", "items").unwrap(), 20);
        assert_eq!(validated.page_size.unwrap().default, 20);
    }

    #[test]
    fn pagination_offset_without_step_or_page_size_is_rejected() {
        let pagination = PaginationSpec {
            mode: PaginationMode::Offset,
            offset_param: Some("offset".to_string()),
            ..PaginationSpec::default()
        };

        let err = pagination.validated("demo", "items").unwrap_err();
        assert!(
            err.to_string()
                .contains("demo.items pagination.mode=offset requires offset_step or page_size")
        );
    }

    #[test]
    fn pagination_max_pages_zero_is_rejected() {
        let pagination = PaginationSpec {
            max_pages: Some(0),
            ..PaginationSpec::default()
        };

        let err = pagination.validated("demo", "items").unwrap_err();
        assert!(
            err.to_string()
                .contains("demo.items pagination.max_pages must be > 0")
        );
    }

    #[test]
    fn cursor_query_pagination_rejects_empty_response_cursor_header() {
        let pagination = PaginationSpec {
            mode: PaginationMode::CursorQuery,
            cursor_param: Some("cursor".to_string()),
            response_cursor_path: vec!["meta".to_string(), "next_cursor".to_string()],
            response_cursor_header: Some(String::new()),
            ..PaginationSpec::default()
        };

        let err = pagination.validated("demo", "items").unwrap_err();
        assert!(
            err.to_string()
                .contains("demo.items pagination.response_cursor_header must not be empty")
        );
    }

    #[test]
    fn cursor_body_pagination_rejects_blank_response_cursor_header() {
        let pagination = PaginationSpec {
            mode: PaginationMode::CursorBody,
            cursor_body_path: vec!["cursor".to_string()],
            response_cursor_path: vec!["meta".to_string(), "next_cursor".to_string()],
            response_cursor_header: Some("   ".to_string()),
            ..PaginationSpec::default()
        };

        let err = pagination.validated("demo", "items").unwrap_err();
        assert!(
            err.to_string()
                .contains("demo.items pagination.response_cursor_header must not be empty")
        );
    }

    #[test]
    fn link_pagination_rejects_blank_next_url_header() {
        let pagination = PaginationSpec {
            mode: PaginationMode::LinkHeader,
            next_url_header: Some("   ".to_string()),
            ..PaginationSpec::default()
        };

        let err = pagination.validated("demo", "items").unwrap_err();
        assert!(
            err.to_string()
                .contains("demo.items pagination.next_url_header must not be empty")
        );
    }

    #[test]
    fn next_url_body_pagination_requires_a_usable_path() {
        for (next_url_path, expected) in [
            (
                Vec::new(),
                "demo.items pagination.mode=next_url_body requires next_url_path",
            ),
            (
                vec!["  ".to_string()],
                "demo.items pagination.next_url_path segments must not be blank or padded",
            ),
            (
                vec![" @odata.nextLink".to_string()],
                "demo.items pagination.next_url_path segments must not be blank or padded",
            ),
        ] {
            let pagination = PaginationSpec {
                mode: PaginationMode::NextUrlBody,
                next_url_path,
                ..PaginationSpec::default()
            };

            let err = pagination.validated("demo", "items").unwrap_err();
            assert!(
                err.to_string().contains(expected),
                "expected {expected:?}, got {err}"
            );
        }
    }

    #[test]
    fn next_url_body_pagination_carries_its_path_into_the_validated_mode() {
        let pagination = PaginationSpec {
            mode: PaginationMode::NextUrlBody,
            next_url_path: vec!["@odata.nextLink".to_string()],
            ..PaginationSpec::default()
        };

        let validated = pagination.validated("demo", "items").expect("validated");
        let ValidatedPaginationMode::NextUrlBody(next_url_body) = &validated.mode else {
            panic!("expected next_url_body mode, got {:?}", validated.mode);
        };
        assert_eq!(next_url_body.path(), ["@odata.nextLink"]);
        assert!(
            validated.mode.follows_response_next_url(),
            "the fetch loop must be told to request the URL rather than rebuild the request"
        );
    }

    #[test]
    fn manifest_data_type_all_round_trips_through_spelling_and_serde() {
        for data_type in ManifestDataType::ALL {
            let spelled = data_type.to_string();
            assert_eq!(
                spelled.parse::<ManifestDataType>().expect("round trip"),
                data_type,
                "Display/FromStr round trip failed for {spelled}"
            );
            assert_eq!(
                serde_json::to_value(data_type).expect("serialize"),
                Value::String(spelled.clone()),
                "serde spelling diverged from as_manifest_str for {spelled}"
            );
        }
    }

    #[test]
    fn manifest_data_type_rejects_unknown_spelling() {
        let error = "Banana"
            .parse::<ManifestDataType>()
            .expect_err("unknown spelling should fail");
        assert!(
            error.to_string().contains("unsupported data type 'Banana'"),
            "unexpected error: {error}"
        );
    }
}
