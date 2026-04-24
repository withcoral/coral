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

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ManifestError, ParsedTemplate, Result};

/// Common top-level source metadata shared by every backend source spec.
#[derive(Debug, Clone)]
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
    Parquet,
    Jsonl,
}

/// Normalized scalar data types supported by the source-spec DSL.
///
/// The engine is responsible for mapping these into runtime-specific types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Source-level authentication requirements for HTTP-backed source specs.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthSpec {
    #[serde(default)]
    pub headers: Vec<HeaderSpec>,
}

/// One request or auth header declared in the source spec.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HeaderSpec {
    pub name: String,
    #[serde(flatten)]
    pub value: ValueSourceSpec,
}

/// Shared table metadata used by all backend-specific table specs.
#[derive(Debug, Clone)]
pub struct TableCommon {
    pub name: String,
    pub description: String,
    pub guide: String,
    pub filters: Vec<FilterSpec>,
    pub fetch_limit_default: Option<usize>,
    pub columns: Vec<ColumnSpec>,
}

impl TableCommon {
    pub(crate) fn new(
        name: String,
        description: String,
        guide: String,
        filters: Vec<FilterSpec>,
        fetch_limit_default: Option<usize>,
        columns: Vec<ColumnSpec>,
    ) -> Self {
        Self {
            name,
            description,
            guide,
            filters,
            fetch_limit_default,
            columns,
        }
    }
}

/// How a filter value is matched against `SQL` predicates.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum FilterMode {
    /// Pushes down `=` only (current behaviour for all existing providers).
    #[default]
    Equality,
    /// Pushes down `LIKE` as a search API call; results may be relevance-ordered.
    Search,
    /// Pushes down `LIKE` as a substring/contains filter.
    Contains,
}

/// One declared filter that can be bound from SQL into a backend request.
#[derive(Debug, Clone, Deserialize)]
pub struct FilterSpec {
    pub name: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub mode: FilterMode,
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
    pub body: Vec<BodyFieldSpec>,
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
#[allow(
    clippy::upper_case_acronyms,
    reason = "The manifest format uses conventional HTTP method spellings."
)]
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
    #[serde(flatten)]
    pub value: ValueSourceSpec,
}

/// How a source-spec request value is populated at runtime.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum ValueSourceSpec {
    Template {
        template: ParsedTemplate,
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
    Input {
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
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ResponseSpec {
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

/// How the engine converts a selected response value into logical rows.
#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RowStrategy {
    #[default]
    Direct,
    SeriesPointList,
    DictEntries,
}

/// Pagination configuration for one HTTP table.
#[derive(Debug, Clone, Deserialize)]
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
            page_param: None,
            page_start: 0,
            page_step: default_page_step(),
            offset_param: None,
            offset_start: 0,
            offset_step: None,
            link_header_require_results: false,
            max_pages: None,
        }
    }
}

/// Fully validated pagination configuration ready for engine use.
#[derive(Debug, Clone)]
pub struct ValidatedPagination {
    pub mode: ValidatedPaginationMode,
    pub page_size: Option<PageSizeSpec>,
    pub link_header_require_results: bool,
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
        let mode = self.validated_mode(schema, table, page_size.is_some())?;
        Ok(ValidatedPagination {
            mode,
            page_size,
            link_header_require_results: self.link_header_require_results,
        })
    }

    fn validated_mode(
        &self,
        schema: &str,
        table: &str,
        has_page_size: bool,
    ) -> Result<ValidatedPaginationMode> {
        match self.mode {
            PaginationMode::None => Ok(ValidatedPaginationMode::None),
            PaginationMode::Auto => Ok(ValidatedPaginationMode::Auto),
            PaginationMode::CursorQuery => {
                if self.cursor_param.is_none() {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table} pagination.mode=cursor_query requires cursor_param"
                    )));
                }
                if self.response_cursor_path.is_empty() {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table} pagination.mode=cursor_query requires response_cursor_path"
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
                if self.response_cursor_path.is_empty() {
                    return Err(ManifestError::validation(format!(
                        "{schema}.{table} pagination.mode=cursor_body requires response_cursor_path"
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
        }
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
            .map_err(|_| {
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
#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
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
}

/// Page-size settings shared by several pagination modes.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PageSizeSpec {
    pub default: usize,
    pub max: usize,
    #[serde(default)]
    pub query_param: Option<String>,
    #[serde(default)]
    pub body_path: Vec<String>,
}

/// One declared output column for a manifest table.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    #[serde(rename = "virtual")]
    pub r#virtual: bool,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub expr: Option<ExprSpec>,
}

impl ColumnSpec {
    /// Convert this manifest type into a normalized manifest data type.
    ///
    /// # Errors
    ///
    /// Returns a [`ManifestError`] if the manifest references an unsupported
    /// data type.
    pub fn manifest_data_type(&self) -> Result<ManifestDataType> {
        parse_manifest_data_type(&self.data_type)
    }

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
#[derive(Debug, Clone, Deserialize)]
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
    Literal {
        value: Value,
    },
    Null,
    JoinArray {
        path: Vec<String>,
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
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampInput {
    /// Seconds since Unix epoch (integer or float).
    #[default]
    Seconds,
    /// Milliseconds since Unix epoch.
    Milliseconds,
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

/// Parse a manifest data type name into a normalized manifest data type.
///
/// # Errors
///
/// Returns a [`ManifestError`] if `s` is not one of the supported manifest
/// data type names.
pub(crate) fn parse_manifest_data_type(s: &str) -> Result<ManifestDataType> {
    match s {
        "Utf8" => Ok(ManifestDataType::Utf8),
        "Int64" => Ok(ManifestDataType::Int64),
        "Boolean" => Ok(ManifestDataType::Boolean),
        "Float64" => Ok(ManifestDataType::Float64),
        "Timestamp" => Ok(ManifestDataType::Timestamp),
        "Json" => Ok(ManifestDataType::Json),
        other => Err(ManifestError::validation(format!(
            "unsupported data type '{other}' in source manifest"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::http::test_http_table_spec;
    use std::collections::HashSet;

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
                body: vec![],
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
                required: false,
                mode: FilterMode::default(),
            }],
            RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items").expect("template"),
                query: vec![],
                body: vec![],
                headers: vec![],
            },
        );
        table.requests = vec![RequestRouteSpec {
            when_filters: vec!["id".into()],
            request: RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items/{{filter.id}}").expect("template"),
                query: vec![],
                body: vec![],
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
                    required: false,
                    mode: FilterMode::default(),
                },
                FilterSpec {
                    name: "org".into(),
                    required: false,
                    mode: FilterMode::default(),
                },
            ],
            RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items").expect("template"),
                query: vec![],
                body: vec![],
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
                    body: vec![],
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
                    body: vec![],
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
    fn filter_mode_defaults_to_equality() {
        let spec: FilterSpec = serde_json::from_value(serde_json::json!({
            "name": "org"
        }))
        .unwrap();
        assert_eq!(spec.mode, FilterMode::Equality);
    }

    #[test]
    fn filter_mode_deserializes_search() {
        let spec: FilterSpec = serde_json::from_value(serde_json::json!({
            "name": "q",
            "mode": "search"
        }))
        .unwrap();
        assert_eq!(spec.mode, FilterMode::Search);
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
        assert!(result.is_err());
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
}
