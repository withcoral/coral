//! Shared Universal Search response rendering for thin clients.

use std::fmt::Write as _;

use coral_api::v1::{
    CatalogItem, CatalogMetadata, ColumnHint, GetSearchCapabilitiesResponse, NativeSearchAttribute,
    NativeSearchDiagnostic, NativeSearchDiagnosticReason, NativeSearchDiagnosticState,
    NativeSearchResult, ObservedValue, SearchFieldRole, SearchLimits, SearchProvider,
    SearchProviderCoverage, SearchProviderState, SearchResponse, SearchResult,
    SearchResultTruncation, SearchRouteIdentity as ProtoSearchRouteIdentity, SearchSurfaceKind,
    SearchTableColumnPreview, SearchTableColumnPreviewColumn, TableFunction, TableFunctionArgument,
    TableFunctionKind, TableFunctionResultColumn, TableSummary, catalog_item, search_result,
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

const MAX_SEARCH_CAPABILITY_ROUTES: usize = 16;

/// Effective Universal Search behavior reported by the local Coral server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCapabilities {
    /// Whether bounded provider fanout is effective for this process.
    pub provider_fanout_enabled: bool,
    /// Bounded identities of source-authorised routes visible in the workspace.
    pub eligible_routes: Vec<SearchRouteIdentity>,
    /// Whether the complete eligible route inventory was truncated.
    pub truncated: bool,
    /// Number of eligible routes omitted from this decoded inventory.
    pub omitted_route_count: u32,
}

/// Safe identity of one source-authorised Universal Search route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRouteIdentity {
    /// Installed source that owns the route.
    pub installed_source_name: String,
    /// Query-visible schema containing the resolved function.
    pub schema_name: String,
    /// Query-visible function authorised for Universal Search.
    pub function_name: String,
    /// Authored route identifier when this route was explicit.
    pub authored_route_id: Option<String>,
}

/// Decodes and defensively bounds a Search capabilities response.
///
/// A newer or faulty server cannot make a thin adapter render an unbounded
/// route inventory: entries beyond the public cap are counted as omitted.
#[must_use]
pub fn decode_search_capabilities_response(
    response: GetSearchCapabilitiesResponse,
) -> SearchCapabilities {
    let GetSearchCapabilitiesResponse {
        provider_fanout_enabled,
        mut eligible_routes,
        truncated,
        omitted_route_count,
    } = response;
    if !provider_fanout_enabled {
        return SearchCapabilities {
            provider_fanout_enabled: false,
            eligible_routes: Vec::new(),
            truncated: false,
            omitted_route_count: 0,
        };
    }
    let client_omitted = eligible_routes
        .len()
        .saturating_sub(MAX_SEARCH_CAPABILITY_ROUTES);
    eligible_routes.truncate(MAX_SEARCH_CAPABILITY_ROUTES);
    let omitted_route_count =
        omitted_route_count.saturating_add(u32::try_from(client_omitted).unwrap_or(u32::MAX));

    SearchCapabilities {
        provider_fanout_enabled,
        eligible_routes: eligible_routes
            .into_iter()
            .map(search_route_identity_from_proto)
            .collect(),
        truncated: truncated || omitted_route_count != 0,
        omitted_route_count,
    }
}

fn search_route_identity_from_proto(route: ProtoSearchRouteIdentity) -> SearchRouteIdentity {
    SearchRouteIdentity {
        installed_source_name: route.installed_source_name,
        schema_name: route.schema_name,
        function_name: route.function_name,
        authored_route_id: route.authored_route_id,
    }
}

/// Shared machine-readable Universal Search response shape used by local
/// adapters.
#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SearchResponseValue<'a> {
    results: Vec<SearchResultValue<'a>>,
    provider_statuses: Vec<SearchProviderStatusValue<'a>>,
    truncation: Option<SearchTruncationValue<'a>>,
}

impl<'a> From<&'a SearchResponse> for SearchResponseValue<'a> {
    fn from(response: &'a SearchResponse) -> Self {
        Self {
            results: response
                .results
                .iter()
                .map(SearchResultValue::from)
                .collect(),
            provider_statuses: response
                .provider_statuses
                .iter()
                .map(SearchProviderStatusValue::from)
                .collect(),
            truncation: response
                .truncation
                .as_ref()
                .map(SearchTruncationValue::from),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[serde(untagged)]
enum SearchResultValue<'a> {
    CatalogMetadata {
        provider: &'static str,
        kind: &'static str,
        catalog_metadata: CatalogMetadataValue<'a>,
    },
    ColumnHint {
        provider: &'static str,
        kind: &'static str,
        column_hint: ColumnHintValue<'a>,
    },
    ObservedValue {
        provider: &'static str,
        kind: &'static str,
        observed_value: ObservedValueValue<'a>,
    },
    NativeResult {
        provider: &'static str,
        kind: &'static str,
        native_result: NativeSearchResultValue<'a>,
    },
    Unknown {
        provider: &'static str,
        kind: &'static str,
    },
}

impl<'a> From<&'a SearchResult> for SearchResultValue<'a> {
    fn from(result: &'a SearchResult) -> Self {
        let provider = provider_name(result.provider);
        match result.payload.as_ref() {
            Some(search_result::Payload::CatalogMetadata(metadata)) => Self::CatalogMetadata {
                provider,
                kind: "catalog_metadata",
                catalog_metadata: CatalogMetadataValue::from(metadata),
            },
            Some(search_result::Payload::ColumnHint(hint)) => Self::ColumnHint {
                provider,
                kind: "column_hint",
                column_hint: ColumnHintValue::from(hint),
            },
            Some(search_result::Payload::ObservedValue(observed)) => Self::ObservedValue {
                provider,
                kind: "observed_value",
                observed_value: ObservedValueValue::from(observed),
            },
            Some(search_result::Payload::NativeResult(native)) => Self::NativeResult {
                provider,
                kind: "native_result",
                native_result: NativeSearchResultValue::from(native),
            },
            None => Self::Unknown {
                provider,
                kind: "unknown",
            },
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogMetadataValue<'a> {
    item: Option<CatalogItemValue<'a>>,
    matched_fields: &'a [String],
    table_column_preview: Option<TableColumnPreviewValue<'a>>,
}

impl<'a> From<&'a CatalogMetadata> for CatalogMetadataValue<'a> {
    fn from(metadata: &'a CatalogMetadata) -> Self {
        Self {
            item: metadata
                .item
                .as_ref()
                .and_then(CatalogItemValue::from_catalog_item),
            matched_fields: &metadata.matched_fields,
            table_column_preview: metadata
                .table_column_preview
                .as_ref()
                .map(TableColumnPreviewValue::from),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[serde(untagged)]
enum CatalogItemValue<'a> {
    Table(TableSummaryValue<'a>),
    TableFunction(TableFunctionValue<'a>),
}

impl<'a> CatalogItemValue<'a> {
    fn from_catalog_item(item: &'a CatalogItem) -> Option<Self> {
        match item.item.as_ref()? {
            catalog_item::Item::Table(table) => Some(Self::Table(TableSummaryValue::from(table))),
            catalog_item::Item::TableFunction(function) => {
                Some(Self::TableFunction(TableFunctionValue::from(function)))
            }
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableSummaryValue<'a> {
    kind: &'static str,
    catalog_name: &'a str,
    schema_name: &'a str,
    name: String,
    sql_reference: String,
    description: &'a str,
    table: TableSummaryDetailsValue<'a>,
}

impl<'a> From<&'a TableSummary> for TableSummaryValue<'a> {
    fn from(table: &'a TableSummary) -> Self {
        Self {
            kind: "table",
            catalog_name: &table.catalog_name,
            schema_name: &table.schema_name,
            name: format_table_name(
                optional_catalog_name(&table.catalog_name),
                &table.schema_name,
                &table.name,
            ),
            sql_reference: format_schema_table_equivalent(
                optional_catalog_name(&table.catalog_name),
                &table.schema_name,
                &table.name,
            ),
            description: &table.description,
            table: TableSummaryDetailsValue::from(table),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableSummaryDetailsValue<'a> {
    table_name: &'a str,
    guide: &'a str,
    required_filters: &'a [String],
}

impl<'a> From<&'a TableSummary> for TableSummaryDetailsValue<'a> {
    fn from(table: &'a TableSummary) -> Self {
        Self {
            table_name: &table.name,
            guide: &table.guide,
            required_filters: &table.required_filters,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableFunctionValue<'a> {
    kind: &'static str,
    schema_name: &'a str,
    name: String,
    sql_reference: String,
    sql_call_example: String,
    description: &'a str,
    table_function: TableFunctionDetailsValue<'a>,
}

impl<'a> From<&'a TableFunction> for TableFunctionValue<'a> {
    fn from(function: &'a TableFunction) -> Self {
        Self {
            kind: "table_function",
            schema_name: &function.schema_name,
            name: format!("{}.{}", function.schema_name, function.name),
            sql_reference: format_schema_table_equivalent(
                None,
                &function.schema_name,
                &function.name,
            ),
            sql_call_example: minimal_table_function_call_example(function),
            description: &function.description,
            table_function: TableFunctionDetailsValue::from(function),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableFunctionDetailsValue<'a> {
    function_name: &'a str,
    function_kind: &'static str,
    guide: &'a str,
    arguments: Vec<TableFunctionArgumentValue<'a>>,
    result_columns: Vec<TableFunctionResultColumnValue<'a>>,
    search_limits: Option<SearchLimitsValue>,
}

impl<'a> From<&'a TableFunction> for TableFunctionDetailsValue<'a> {
    fn from(function: &'a TableFunction) -> Self {
        Self {
            function_name: &function.name,
            function_kind: table_function_kind_name(function.kind),
            guide: &function.guide,
            arguments: function
                .arguments
                .iter()
                .map(TableFunctionArgumentValue::from)
                .collect(),
            result_columns: function
                .result_columns
                .iter()
                .map(TableFunctionResultColumnValue::from)
                .collect(),
            search_limits: function.search_limits.as_ref().map(SearchLimitsValue::from),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableFunctionArgumentValue<'a> {
    name: &'a str,
    required: bool,
    values: &'a [String],
}

impl<'a> From<&'a TableFunctionArgument> for TableFunctionArgumentValue<'a> {
    fn from(argument: &'a TableFunctionArgument) -> Self {
        Self {
            name: &argument.name,
            required: argument.required,
            values: &argument.values,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableFunctionResultColumnValue<'a> {
    column_name: &'a str,
    data_type: &'a str,
    is_nullable: bool,
    description: &'a str,
}

impl<'a> From<&'a TableFunctionResultColumn> for TableFunctionResultColumnValue<'a> {
    fn from(column: &'a TableFunctionResultColumn) -> Self {
        Self {
            column_name: &column.name,
            data_type: &column.data_type,
            is_nullable: column.nullable,
            description: &column.description,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct SearchLimitsValue {
    default_top_k: u32,
    max_top_k: u32,
    max_calls_per_query: u32,
}

impl From<&SearchLimits> for SearchLimitsValue {
    fn from(limits: &SearchLimits) -> Self {
        Self {
            default_top_k: limits.default_top_k,
            max_top_k: limits.max_top_k,
            max_calls_per_query: limits.max_calls_per_query,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableColumnPreviewValue<'a> {
    column_count: u32,
    columns: Vec<TableColumnPreviewColumnValue<'a>>,
    omitted_column_count: u32,
}

impl<'a> From<&'a SearchTableColumnPreview> for TableColumnPreviewValue<'a> {
    fn from(preview: &'a SearchTableColumnPreview) -> Self {
        Self {
            column_count: preview.column_count,
            columns: preview
                .columns
                .iter()
                .map(TableColumnPreviewColumnValue::from)
                .collect(),
            omitted_column_count: preview.omitted_column_count,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableColumnPreviewColumnValue<'a> {
    column_name: &'a str,
    data_type: &'a str,
    is_required_filter: bool,
    description: &'a str,
    matched_fields: &'a [String],
}

impl<'a> From<&'a SearchTableColumnPreviewColumn> for TableColumnPreviewColumnValue<'a> {
    fn from(column: &'a SearchTableColumnPreviewColumn) -> Self {
        Self {
            column_name: &column.name,
            data_type: &column.data_type,
            is_required_filter: column.is_required_filter,
            description: &column.description,
            matched_fields: &column.matched_fields,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct ColumnHintValue<'a> {
    schema_name: &'a str,
    surface_name: &'a str,
    surface_kind: &'static str,
    surface_sql_reference: String,
    column_name: &'a str,
    data_type: &'a str,
    required: bool,
    description: &'a str,
    matched_fields: &'a [String],
    field_role: &'static str,
}

impl<'a> From<&'a ColumnHint> for ColumnHintValue<'a> {
    fn from(hint: &'a ColumnHint) -> Self {
        Self {
            schema_name: &hint.schema_name,
            surface_name: &hint.surface_name,
            surface_kind: surface_kind_name(hint.surface_kind),
            surface_sql_reference: format_schema_table_equivalent(
                None,
                &hint.schema_name,
                &hint.surface_name,
            ),
            column_name: &hint.name,
            data_type: &hint.data_type,
            required: hint.required,
            description: &hint.description,
            matched_fields: &hint.matched_fields,
            field_role: field_role_name(hint.field_role),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct ObservedValueValue<'a> {
    value: &'a str,
    schema_name: &'a str,
    surface_name: &'a str,
    surface_kind: &'static str,
    surface_sql_reference: String,
    column_name: &'a str,
    field_path: &'a str,
    observed_count: u64,
    last_observed_at: &'a str,
}

impl<'a> From<&'a ObservedValue> for ObservedValueValue<'a> {
    fn from(observed: &'a ObservedValue) -> Self {
        Self {
            value: &observed.value,
            schema_name: &observed.schema_name,
            surface_name: &observed.surface_name,
            surface_kind: surface_kind_name(observed.surface_kind),
            surface_sql_reference: format_schema_table_equivalent(
                None,
                &observed.schema_name,
                &observed.surface_name,
            ),
            column_name: &observed.column_name,
            field_path: &observed.field_path,
            observed_count: observed.observed_count,
            last_observed_at: &observed.last_observed_at,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct NativeSearchResultValue<'a> {
    schema_name: &'a str,
    function_name: &'a str,
    row_ordinal: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    entity_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snippet: Option<&'a str>,
    attributes: Vec<NativeSearchAttributeValue<'a>>,
    omitted_attribute_count: u32,
    content_truncated: bool,
}

impl<'a> From<&'a NativeSearchResult> for NativeSearchResultValue<'a> {
    fn from(result: &'a NativeSearchResult) -> Self {
        Self {
            schema_name: &result.schema_name,
            function_name: &result.function_name,
            row_ordinal: result.row_ordinal,
            entity_type: result.entity_type.as_deref(),
            provider_id: result.provider_id.as_deref(),
            title: result.title.as_deref(),
            url: result.url.as_deref(),
            snippet: result.snippet.as_deref(),
            attributes: result
                .attributes
                .iter()
                .map(NativeSearchAttributeValue::from)
                .collect(),
            omitted_attribute_count: result.omitted_attribute_count,
            content_truncated: result.content_truncated,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct NativeSearchAttributeValue<'a> {
    name: &'a str,
    display_value: &'a str,
}

impl<'a> From<&'a NativeSearchAttribute> for NativeSearchAttributeValue<'a> {
    fn from(attribute: &'a NativeSearchAttribute) -> Self {
        Self {
            name: &attribute.name,
            display_value: &attribute.display_value,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct SearchProviderStatusValue<'a> {
    provider: &'static str,
    state: &'static str,
    note: &'a str,
    coverage: Option<SearchProviderCoverageValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    diagnostics: Vec<NativeSearchDiagnosticValue<'a>>,
    #[serde(default, skip_serializing_if = "is_false")]
    diagnostics_truncated: bool,
    #[serde(default, skip_serializing_if = "is_zero")]
    omitted_diagnostic_count: u32,
}

impl<'a> From<&'a coral_api::v1::SearchProviderStatus> for SearchProviderStatusValue<'a> {
    fn from(status: &'a coral_api::v1::SearchProviderStatus) -> Self {
        Self {
            provider: provider_name(status.provider),
            state: provider_state_name(status.state),
            note: &status.note,
            coverage: status
                .coverage
                .as_ref()
                .map(SearchProviderCoverageValue::from),
            diagnostics: status
                .diagnostics
                .iter()
                .map(NativeSearchDiagnosticValue::from)
                .collect(),
            diagnostics_truncated: status.diagnostics_truncated,
            omitted_diagnostic_count: status.omitted_diagnostic_count,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct NativeSearchDiagnosticValue<'a> {
    source_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    function_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    authored_route_id: Option<&'a str>,
    state: &'static str,
    reason: &'static str,
    elapsed_ms: u64,
    safe_candidate_count: u32,
    has_more: bool,
}

impl<'a> From<&'a NativeSearchDiagnostic> for NativeSearchDiagnosticValue<'a> {
    fn from(diagnostic: &'a NativeSearchDiagnostic) -> Self {
        Self {
            source_name: &diagnostic.source_name,
            function_name: diagnostic.function_name.as_deref(),
            authored_route_id: diagnostic.authored_route_id.as_deref(),
            state: native_diagnostic_state_name(diagnostic.state),
            reason: native_diagnostic_reason_name(diagnostic.reason),
            elapsed_ms: diagnostic.elapsed_ms,
            safe_candidate_count: diagnostic.safe_candidate_count,
            has_more: diagnostic.has_more,
        }
    }
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if callbacks receive a reference"
)]
fn is_false(value: &bool) -> bool {
    !value
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if callbacks receive a reference"
)]
fn is_zero(value: &u32) -> bool {
    *value == 0
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "typed response value mirrors the public SearchProviderCoverage response shape"
)]
struct SearchProviderCoverageValue {
    eligible_units: u32,
    searched_units: u32,
    failed_units: u32,
    returned_count: u32,
    has_more: bool,
    budget_exhausted: bool,
    timed_out: bool,
    stale_index: bool,
}

impl From<&SearchProviderCoverage> for SearchProviderCoverageValue {
    fn from(coverage: &SearchProviderCoverage) -> Self {
        Self {
            eligible_units: coverage.eligible_units,
            searched_units: coverage.searched_units,
            failed_units: coverage.failed_units,
            returned_count: coverage.returned_count,
            has_more: coverage.has_more,
            budget_exhausted: coverage.budget_exhausted,
            timed_out: coverage.timed_out,
            stale_index: coverage.stale_index,
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct SearchTruncationValue<'a> {
    truncated: bool,
    returned_count: u32,
    max_results: u32,
    note: &'a str,
}

impl<'a> From<&'a SearchResultTruncation> for SearchTruncationValue<'a> {
    fn from(truncation: &'a SearchResultTruncation) -> Self {
        Self {
            truncated: truncation.truncated,
            returned_count: truncation.returned_count,
            max_results: truncation.max_results,
            note: &truncation.note,
        }
    }
}

/// Converts a Universal Search response into the shared JSON shape used by
/// local adapters.
///
/// # Panics
///
/// Panics if the shared typed response value cannot be serialized to JSON.
/// The value currently contains only strings, numbers, booleans, arrays, and
/// optional nested structs, so this indicates a bug in the renderer contract.
#[must_use]
pub fn search_response_json_value(response: &SearchResponse) -> Value {
    serde_json::to_value(SearchResponseValue::from(response))
        .expect("shared search response value serializes")
}

/// Formats a Universal Search response as pretty JSON.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] if the shared response value cannot be
/// serialized.
pub fn format_search_response_json(response: &SearchResponse) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&SearchResponseValue::from(response))
}

/// Formats a Universal Search response for terminal display.
#[must_use]
pub fn format_search_response_text(response: &SearchResponse) -> String {
    let mut lines = Vec::new();
    lines.push("Results".to_string());
    if response.results.is_empty() {
        lines.push("No results.".to_string());
    } else {
        for (idx, result) in response.results.iter().enumerate() {
            lines.extend(result_text_lines(idx + 1, result));
        }
    }

    lines.push(String::new());
    lines.push("Provider statuses".to_string());
    if response.provider_statuses.is_empty() {
        lines.push("- none reported".to_string());
    } else {
        lines.extend(response.provider_statuses.iter().map(provider_status_text));
    }

    if let Some(truncation) = response.truncation.as_ref() {
        lines.push(String::new());
        lines.push(format!(
            "Returned {} of {} requested result(s){}.",
            truncation.returned_count,
            truncation.max_results,
            if truncation.truncated {
                " (truncated)"
            } else {
                ""
            }
        ));
        if !truncation.note.is_empty() {
            lines.push(format!("Note: {}", truncation.note));
        }
    }

    format!("{}\n", lines.join("\n"))
}

fn result_text_lines(index: usize, result: &SearchResult) -> Vec<String> {
    let provider = provider_name(result.provider);
    match result.payload.as_ref() {
        Some(search_result::Payload::CatalogMetadata(metadata)) => {
            catalog_metadata_text_lines(index, provider, metadata)
        }
        Some(search_result::Payload::ColumnHint(hint)) => {
            column_hint_text_lines(index, provider, hint)
        }
        Some(search_result::Payload::ObservedValue(observed)) => {
            observed_value_text_lines(index, provider, observed)
        }
        Some(search_result::Payload::NativeResult(native)) => {
            native_result_text_lines(index, provider, native)
        }
        None => vec![format!("{index}. [{provider}] unknown result payload")],
    }
}

fn catalog_metadata_text_lines(
    index: usize,
    provider: &str,
    metadata: &CatalogMetadata,
) -> Vec<String> {
    let mut lines = match metadata.item.as_ref().and_then(|item| item.item.as_ref()) {
        Some(catalog_item::Item::Table(table)) => vec![
            format!(
                "{index}. [{provider}] table {}",
                format_table_name(
                    optional_catalog_name(&table.catalog_name),
                    &table.schema_name,
                    &table.name,
                )
            ),
            format!(
                "   SQL: {}",
                format_schema_table_equivalent(
                    optional_catalog_name(&table.catalog_name),
                    &table.schema_name,
                    &table.name
                )
            ),
        ],
        Some(catalog_item::Item::TableFunction(function)) => {
            let mut lines = vec![
                format!(
                    "{index}. [{provider}] table function {}.{}",
                    function.schema_name, function.name
                ),
                format!(
                    "   SQL reference: {}",
                    format_schema_table_equivalent(None, &function.schema_name, &function.name)
                ),
                format!("   Call: {}", minimal_table_function_call_example(function)),
            ];
            if !function.guide.is_empty() {
                lines.push(format!("   Guide: {}", function.guide));
            }
            lines
        }
        None => vec![format!("{index}. [{provider}] catalog metadata")],
    };
    push_optional_fields_line(&mut lines, "   matched", &metadata.matched_fields);
    if let Some(preview) = metadata
        .table_column_preview
        .as_ref()
        .and_then(preview_text)
    {
        lines.push(format!("   columns: {preview}"));
    }
    lines
}

fn column_hint_text_lines(index: usize, provider: &str, hint: &ColumnHint) -> Vec<String> {
    let mut lines = vec![
        format!(
            "{index}. [{provider}] {} {}.{}.{}",
            field_role_name(hint.field_role),
            hint.schema_name,
            hint.surface_name,
            hint.name
        ),
        format!(
            "   Surface: {} {}",
            surface_kind_name(hint.surface_kind),
            format_schema_table_equivalent(None, &hint.schema_name, &hint.surface_name)
        ),
    ];
    if !hint.data_type.is_empty() {
        lines.push(format!("   Type: {}", hint.data_type));
    }
    if hint.required {
        lines.push("   Required: true".to_string());
    }
    push_optional_fields_line(&mut lines, "   matched", &hint.matched_fields);
    lines
}

fn observed_value_text_lines(
    index: usize,
    provider: &str,
    observed: &ObservedValue,
) -> Vec<String> {
    let mut lines = vec![
        format!(
            "{index}. [{provider}] observed value {}.{}.{}",
            observed.schema_name, observed.surface_name, observed.column_name
        ),
        format!("   Value: {}", observed.value),
    ];
    if !observed.field_path.is_empty() {
        lines.push(format!("   Field path: {}", observed.field_path));
    }
    lines.push(format!(
        "   Last observed: {} ({} observation(s))",
        observed.last_observed_at, observed.observed_count
    ));
    lines
}

fn native_result_text_lines(
    index: usize,
    provider: &str,
    result: &NativeSearchResult,
) -> Vec<String> {
    let mut lines = vec![format!(
        "{index}. [{provider}] native result {}.{} row {}",
        result.schema_name, result.function_name, result.row_ordinal
    )];
    for (label, value) in [
        ("Entity type", result.entity_type.as_deref()),
        ("Provider id", result.provider_id.as_deref()),
        ("Title", result.title.as_deref()),
        ("URL", result.url.as_deref()),
        ("Snippet", result.snippet.as_deref()),
    ] {
        if let Some(value) = value {
            lines.push(format!("   {label}: {value}"));
        }
    }
    if !result.attributes.is_empty() {
        lines.push(format!(
            "   Attributes: {}",
            result
                .attributes
                .iter()
                .map(|attribute| format!("{}={}", attribute.name, attribute.display_value))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if result.omitted_attribute_count > 0 {
        lines.push(format!(
            "   Omitted attributes: {}",
            result.omitted_attribute_count
        ));
    }
    if result.content_truncated {
        lines.push("   Content truncated: true".to_string());
    }
    lines
}

fn provider_status_text(status: &coral_api::v1::SearchProviderStatus) -> String {
    let mut line = format!(
        "- {}: {}",
        provider_name(status.provider),
        provider_state_name(status.state)
    );
    if let Some(coverage) = status.coverage.as_ref() {
        write!(
            line,
            " (eligible {}, searched {}, returned {}, failed {})",
            coverage.eligible_units,
            coverage.searched_units,
            coverage.returned_count,
            coverage.failed_units
        )
        .expect("writing to string should not fail");
    }
    if !status.note.is_empty() {
        line.push_str(": ");
        line.push_str(&status.note);
    }
    for diagnostic in &status.diagnostics {
        write!(
            line,
            "\n  - {}: {}/{} ({} ms, {} candidate(s){}{})",
            diagnostic_identity_text(diagnostic),
            native_diagnostic_state_name(diagnostic.state),
            native_diagnostic_reason_name(diagnostic.reason),
            diagnostic.elapsed_ms,
            diagnostic.safe_candidate_count,
            if diagnostic.has_more {
                ", has more"
            } else {
                ""
            },
            diagnostic
                .authored_route_id
                .as_deref()
                .map_or_else(String::new, |route_id| format!(", route {route_id}"))
        )
        .expect("writing to string should not fail");
    }
    if status.diagnostics_truncated || status.omitted_diagnostic_count > 0 {
        write!(
            line,
            "\n  Diagnostics truncated: {} omitted",
            status.omitted_diagnostic_count
        )
        .expect("writing to string should not fail");
    }
    line
}

fn diagnostic_identity_text(diagnostic: &NativeSearchDiagnostic) -> String {
    diagnostic.function_name.as_deref().map_or_else(
        || diagnostic.source_name.clone(),
        |function| format!("{}.{function}", diagnostic.source_name),
    )
}

fn push_optional_fields_line(lines: &mut Vec<String>, label: &str, fields: &[String]) {
    if !fields.is_empty() {
        lines.push(format!("{label}: {}", fields.join(", ")));
    }
}

fn preview_text(preview: &SearchTableColumnPreview) -> Option<String> {
    if preview.columns.is_empty() {
        return None;
    }
    let mut parts = preview
        .columns
        .iter()
        .map(|column| {
            if column.data_type.is_empty() {
                column.name.clone()
            } else {
                format!("{} ({})", column.name, column.data_type)
            }
        })
        .collect::<Vec<_>>();
    if preview.omitted_column_count > 0 {
        parts.push(format!("+{} more", preview.omitted_column_count));
    }
    Some(parts.join(", "))
}

/// Formats the shortest SQL call example for a table function.
#[must_use]
pub fn minimal_table_function_call_example(function: &TableFunction) -> String {
    let reference = format_schema_table_equivalent(None, &function.schema_name, &function.name);
    let required_arguments = function
        .arguments
        .iter()
        .filter(|argument| argument.required)
        .map(|argument| format!("{} => '<value>'", format_sql_identifier(&argument.name)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{reference}({required_arguments})")
}

/// Formats a display table name with its query-visible schema and optional catalog.
#[must_use]
pub fn format_table_name(
    catalog_name: Option<&str>,
    schema_name: &str,
    table_name: &str,
) -> String {
    match catalog_name {
        Some(catalog_name) => format!("{catalog_name}.{schema_name}.{table_name}"),
        None => format!("{schema_name}.{table_name}"),
    }
}

/// Formats a SQL table or table-function reference, qualified by schema and, when
/// `catalog_name` is `Some`, by catalog. Pass `None` for a two-part reference —
/// table functions and the surfaces whose protos carry no catalog field.
#[must_use]
pub fn format_schema_table_equivalent(
    catalog_name: Option<&str>,
    schema_name: &str,
    table_name: &str,
) -> String {
    match catalog_name {
        Some(catalog_name) => format!(
            "{}.{}.{}",
            format_sql_identifier(catalog_name),
            format_sql_identifier(schema_name),
            format_sql_identifier(table_name)
        ),
        None => format!(
            "{}.{}",
            format_sql_identifier(schema_name),
            format_sql_identifier(table_name)
        ),
    }
}

/// Reads a catalog qualifier off a proto message, where an empty field means the
/// surface is two-part and carries no catalog.
#[must_use]
pub fn optional_catalog_name(catalog_name: &str) -> Option<&str> {
    (!catalog_name.is_empty()).then_some(catalog_name)
}

/// Formats one SQL identifier, quoting it only when required.
#[must_use]
pub fn format_sql_identifier(identifier: &str) -> String {
    if identifier_needs_quotes(identifier) {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    } else {
        identifier.to_string()
    }
}

fn identifier_needs_quotes(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return true;
    };
    if !(first.is_ascii_lowercase() || first == '_') {
        return true;
    }
    !chars.all(|char| char.is_ascii_lowercase() || char.is_ascii_digit() || char == '_')
}

fn provider_name(provider: i32) -> &'static str {
    match SearchProvider::try_from(provider) {
        Ok(SearchProvider::CatalogMetadata) => "catalog_metadata",
        Ok(SearchProvider::ObservedValues) => "observed_values",
        Ok(SearchProvider::NativeFanout) => "native_fanout",
        Ok(SearchProvider::Unspecified) | Err(_) => "unspecified",
    }
}

fn provider_state_name(state: i32) -> &'static str {
    match SearchProviderState::try_from(state) {
        Ok(SearchProviderState::ResultsFound) => "results_found",
        Ok(SearchProviderState::Empty) => "empty",
        Ok(SearchProviderState::NotEnabled) => "not_enabled",
        Ok(SearchProviderState::Skipped) => "skipped",
        Ok(SearchProviderState::Partial) => "partial",
        Ok(SearchProviderState::Error) => "error",
        Ok(SearchProviderState::Unspecified) | Err(_) => "unspecified",
    }
}

fn native_diagnostic_state_name(state: i32) -> &'static str {
    match NativeSearchDiagnosticState::try_from(state) {
        Ok(NativeSearchDiagnosticState::ResultsFound) => "results_found",
        Ok(NativeSearchDiagnosticState::Empty) => "empty",
        Ok(NativeSearchDiagnosticState::Skipped) => "skipped",
        Ok(NativeSearchDiagnosticState::TimedOut) => "timed_out",
        Ok(NativeSearchDiagnosticState::Cancelled) => "cancelled",
        Ok(NativeSearchDiagnosticState::Error) => "error",
        Ok(NativeSearchDiagnosticState::Unspecified) | Err(_) => "unspecified",
    }
}

fn native_diagnostic_reason_name(reason: i32) -> &'static str {
    match NativeSearchDiagnosticReason::try_from(reason) {
        Ok(NativeSearchDiagnosticReason::NotAuthorized) => "not_authorized",
        Ok(NativeSearchDiagnosticReason::AmbiguousRoute) => "ambiguous_route",
        Ok(NativeSearchDiagnosticReason::InvalidSearchLimits) => "invalid_search_limits",
        Ok(NativeSearchDiagnosticReason::QueryInputUnmappable) => "query_input_unmappable",
        Ok(NativeSearchDiagnosticReason::MissingArgumentDefault) => "missing_argument_default",
        Ok(NativeSearchDiagnosticReason::RouteStale) => "route_stale",
        Ok(NativeSearchDiagnosticReason::UnsafeOperation) => "unsafe_operation",
        Ok(NativeSearchDiagnosticReason::NoSafeDisplayFields) => "no_safe_display_fields",
        Ok(NativeSearchDiagnosticReason::FanoutLimitReached) => "fanout_limit_reached",
        Ok(NativeSearchDiagnosticReason::InsufficientBudget) => "insufficient_budget",
        Ok(NativeSearchDiagnosticReason::GlobalBudgetExhausted) => "global_budget_exhausted",
        Ok(NativeSearchDiagnosticReason::CallTimeout) => "call_timeout",
        Ok(NativeSearchDiagnosticReason::Cancelled) => "cancelled",
        Ok(NativeSearchDiagnosticReason::RateLimited) => "rate_limited",
        Ok(NativeSearchDiagnosticReason::AuthOrPermissionFailed) => "auth_or_permission_failed",
        Ok(NativeSearchDiagnosticReason::UpstreamUnavailable) => "upstream_unavailable",
        Ok(NativeSearchDiagnosticReason::InvalidResponse) => "invalid_response",
        Ok(NativeSearchDiagnosticReason::ExecutionFailed) => "execution_failed",
        Ok(NativeSearchDiagnosticReason::UnsupportedCancellation) => "unsupported_cancellation",
        Ok(NativeSearchDiagnosticReason::InternalError) => "internal_error",
        Ok(NativeSearchDiagnosticReason::Unspecified) | Err(_) => "unspecified",
    }
}

fn surface_kind_name(kind: i32) -> &'static str {
    match SearchSurfaceKind::try_from(kind) {
        Ok(SearchSurfaceKind::Table) => "table",
        Ok(SearchSurfaceKind::TableFunction) => "table_function",
        Ok(SearchSurfaceKind::Unspecified) | Err(_) => "unspecified",
    }
}

fn field_role_name(role: i32) -> &'static str {
    match SearchFieldRole::try_from(role) {
        Ok(SearchFieldRole::TableColumn) => "table_column",
        Ok(SearchFieldRole::TableFilter) => "table_filter",
        Ok(SearchFieldRole::TableFunctionArgument) => "table_function_argument",
        Ok(SearchFieldRole::TableFunctionResultColumn) => "table_function_result_column",
        Ok(SearchFieldRole::Unspecified) | Err(_) => "unspecified",
    }
}

fn table_function_kind_name(kind: i32) -> &'static str {
    match TableFunctionKind::try_from(kind) {
        Ok(TableFunctionKind::Table) => "table",
        Ok(TableFunctionKind::Search) => "search",
        Ok(TableFunctionKind::Unspecified) | Err(_) => "unspecified",
    }
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    reason = "unit test assertions use fixed fixture indexes for readability"
)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_decoder_defensively_caps_routes() {
        let response = GetSearchCapabilitiesResponse {
            provider_fanout_enabled: true,
            eligible_routes: (0..20)
                .map(|index| ProtoSearchRouteIdentity {
                    installed_source_name: "source".to_string(),
                    schema_name: "schema".to_string(),
                    function_name: format!("search_{index:02}"),
                    authored_route_id: Some(format!("route-{index:02}")),
                })
                .collect(),
            truncated: false,
            omitted_route_count: 2,
        };

        let capabilities = decode_search_capabilities_response(response);

        assert!(capabilities.provider_fanout_enabled);
        assert_eq!(capabilities.eligible_routes.len(), 16);
        assert!(capabilities.truncated);
        assert_eq!(capabilities.omitted_route_count, 6);
        assert_eq!(
            capabilities.eligible_routes[15]
                .authored_route_id
                .as_deref(),
            Some("route-15")
        );
    }

    #[test]
    fn capabilities_decoder_ignores_routes_when_fanout_is_disabled() {
        let capabilities = decode_search_capabilities_response(GetSearchCapabilitiesResponse {
            provider_fanout_enabled: false,
            eligible_routes: vec![ProtoSearchRouteIdentity {
                installed_source_name: "source".to_string(),
                schema_name: "schema".to_string(),
                function_name: "search".to_string(),
                authored_route_id: None,
            }],
            truncated: true,
            omitted_route_count: 3,
        });

        assert!(!capabilities.provider_fanout_enabled);
        assert!(capabilities.eligible_routes.is_empty());
        assert!(!capabilities.truncated);
        assert_eq!(capabilities.omitted_route_count, 0);
    }

    fn native_response() -> SearchResponse {
        SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::NativeFanout as i32,
                payload: Some(search_result::Payload::NativeResult(NativeSearchResult {
                    schema_name: "github".to_string(),
                    function_name: "search_issues".to_string(),
                    row_ordinal: 1,
                    entity_type: Some("issue".to_string()),
                    provider_id: None,
                    title: Some("Fix native search".to_string()),
                    url: None,
                    snippet: Some("Compact preview".to_string()),
                    attributes: vec![
                        NativeSearchAttribute {
                            name: "state".to_string(),
                            display_value: "open".to_string(),
                        },
                        NativeSearchAttribute {
                            name: "author".to_string(),
                            display_value: "octocat".to_string(),
                        },
                    ],
                    omitted_attribute_count: 2,
                    content_truncated: true,
                })),
            }],
            provider_statuses: vec![coral_api::v1::SearchProviderStatus {
                provider: SearchProvider::NativeFanout as i32,
                state: SearchProviderState::Partial as i32,
                note: "one route was skipped".to_string(),
                coverage: None,
                diagnostics: vec![NativeSearchDiagnostic {
                    source_name: "github".to_string(),
                    function_name: None,
                    authored_route_id: Some("issues".to_string()),
                    state: NativeSearchDiagnosticState::Skipped as i32,
                    reason: NativeSearchDiagnosticReason::InsufficientBudget as i32,
                    elapsed_ms: 19,
                    safe_candidate_count: 0,
                    has_more: false,
                }],
                diagnostics_truncated: true,
                omitted_diagnostic_count: 3,
            }],
            truncation: None,
        }
    }

    #[test]
    fn text_output_renders_table_function_guide() {
        let response = SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::CatalogMetadata as i32,
                payload: Some(search_result::Payload::CatalogMetadata(CatalogMetadata {
                    item: Some(CatalogItem {
                        item: Some(catalog_item::Item::TableFunction(TableFunction {
                            schema_name: "github".to_string(),
                            name: "search_issues".to_string(),
                            guide: "Use this function for issue lookup.".to_string(),
                            ..Default::default()
                        })),
                    }),
                    ..Default::default()
                })),
            }],
            provider_statuses: Vec::new(),
            truncation: None,
        };

        let text = format_search_response_text(&response);

        assert!(
            text.contains("Guide: Use this function for issue lookup."),
            "table-function text should include its guide: {text}"
        );
    }

    #[test]
    fn text_output_renders_observed_field_path() {
        let response = SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::ObservedValues as i32,
                payload: Some(search_result::Payload::ObservedValue(ObservedValue {
                    value: "urgent".to_string(),
                    schema_name: "github".to_string(),
                    surface_name: "issues".to_string(),
                    surface_kind: SearchSurfaceKind::Table as i32,
                    column_name: "labels".to_string(),
                    field_path: "labels.name".to_string(),
                    observed_count: 2,
                    last_observed_at: "2026-07-03T10:00:00Z".to_string(),
                })),
            }],
            provider_statuses: Vec::new(),
            truncation: None,
        };

        let text = format_search_response_text(&response);

        assert!(
            text.contains("Field path: labels.name"),
            "observed value text should include nested field path: {text}"
        );
    }

    #[test]
    fn native_json_omits_absent_fields_and_preserves_attribute_order() {
        let json = search_response_json_value(&native_response());
        let result = &json["results"][0]["native_result"];

        assert_eq!(result["schema_name"], "github");
        assert_eq!(result["function_name"], "search_issues");
        assert_eq!(result["row_ordinal"], 1);
        assert_eq!(result["entity_type"], "issue");
        assert!(result.get("provider_id").is_none());
        assert!(result.get("url").is_none());
        assert_eq!(result["attributes"][0]["name"], "state");
        assert_eq!(result["attributes"][1]["name"], "author");

        let diagnostic = &json["provider_statuses"][0]["diagnostics"][0];
        assert_eq!(diagnostic["source_name"], "github");
        assert!(diagnostic.get("installed_source_name").is_none());
        assert!(diagnostic.get("schema_name").is_none());
        assert!(diagnostic.get("function_name").is_none());
        assert_eq!(diagnostic["authored_route_id"], "issues");
        assert_eq!(diagnostic["state"], "skipped");
        assert_eq!(diagnostic["reason"], "insufficient_budget");
        assert_eq!(diagnostic["elapsed_ms"], 19);
        assert_eq!(json["provider_statuses"][0]["omitted_diagnostic_count"], 3);
    }

    #[test]
    fn native_text_renders_only_present_display_fields_and_diagnostics() {
        let text = format_search_response_text(&native_response());

        assert!(text.contains("native result github.search_issues row 1"));
        assert!(text.contains("Entity type: issue"));
        assert!(text.contains("Title: Fix native search"));
        assert!(!text.contains("Provider id:"));
        assert!(!text.contains("URL:"));
        assert!(text.contains("Attributes: state=open, author=octocat"));
        assert!(
            text.contains(
                "github: skipped/insufficient_budget (19 ms, 0 candidate(s), route issues)"
            )
        );
        assert!(text.contains("Diagnostics truncated: 3 omitted"));
    }

    #[test]
    fn native_text_renders_resolved_diagnostic_as_source_and_function() {
        let mut response = native_response();
        response.provider_statuses[0].diagnostics[0].function_name =
            Some("search_pull_requests".to_string());

        let text = format_search_response_text(&response);

        assert!(text.contains(
            "github.search_pull_requests: skipped/insufficient_budget \
             (19 ms, 0 candidate(s), route issues)"
        ));
        assert!(!text.contains("github (github."));
    }

    #[test]
    fn native_diagnostic_enum_names_cover_every_stable_wire_value() {
        let states = [
            (NativeSearchDiagnosticState::ResultsFound, "results_found"),
            (NativeSearchDiagnosticState::Empty, "empty"),
            (NativeSearchDiagnosticState::Skipped, "skipped"),
            (NativeSearchDiagnosticState::TimedOut, "timed_out"),
            (NativeSearchDiagnosticState::Cancelled, "cancelled"),
            (NativeSearchDiagnosticState::Error, "error"),
        ];
        for (state, expected) in states {
            assert_eq!(native_diagnostic_state_name(state as i32), expected);
        }

        let reasons = [
            (
                NativeSearchDiagnosticReason::NotAuthorized,
                "not_authorized",
            ),
            (
                NativeSearchDiagnosticReason::AmbiguousRoute,
                "ambiguous_route",
            ),
            (
                NativeSearchDiagnosticReason::InvalidSearchLimits,
                "invalid_search_limits",
            ),
            (
                NativeSearchDiagnosticReason::QueryInputUnmappable,
                "query_input_unmappable",
            ),
            (
                NativeSearchDiagnosticReason::MissingArgumentDefault,
                "missing_argument_default",
            ),
            (NativeSearchDiagnosticReason::RouteStale, "route_stale"),
            (
                NativeSearchDiagnosticReason::UnsafeOperation,
                "unsafe_operation",
            ),
            (
                NativeSearchDiagnosticReason::NoSafeDisplayFields,
                "no_safe_display_fields",
            ),
            (
                NativeSearchDiagnosticReason::FanoutLimitReached,
                "fanout_limit_reached",
            ),
            (
                NativeSearchDiagnosticReason::InsufficientBudget,
                "insufficient_budget",
            ),
            (
                NativeSearchDiagnosticReason::GlobalBudgetExhausted,
                "global_budget_exhausted",
            ),
            (NativeSearchDiagnosticReason::CallTimeout, "call_timeout"),
            (NativeSearchDiagnosticReason::Cancelled, "cancelled"),
            (NativeSearchDiagnosticReason::RateLimited, "rate_limited"),
            (
                NativeSearchDiagnosticReason::AuthOrPermissionFailed,
                "auth_or_permission_failed",
            ),
            (
                NativeSearchDiagnosticReason::UpstreamUnavailable,
                "upstream_unavailable",
            ),
            (
                NativeSearchDiagnosticReason::InvalidResponse,
                "invalid_response",
            ),
            (
                NativeSearchDiagnosticReason::ExecutionFailed,
                "execution_failed",
            ),
            (
                NativeSearchDiagnosticReason::UnsupportedCancellation,
                "unsupported_cancellation",
            ),
            (
                NativeSearchDiagnosticReason::InternalError,
                "internal_error",
            ),
        ];
        for (reason, expected) in reasons {
            assert_eq!(native_diagnostic_reason_name(reason as i32), expected);
        }
    }

    #[test]
    fn unknown_native_diagnostic_enums_are_rendered_as_unspecified() {
        let mut response = native_response();
        response.provider_statuses[0].diagnostics[0].state = 999;
        response.provider_statuses[0].diagnostics[0].reason = 998;

        let json = search_response_json_value(&response);
        let diagnostic = &json["provider_statuses"][0]["diagnostics"][0];
        assert_eq!(diagnostic["state"], "unspecified");
        assert_eq!(diagnostic["reason"], "unspecified");
    }

    #[test]
    fn feature_off_rendering_omits_all_new_diagnostic_fields() {
        let response = SearchResponse {
            results: Vec::new(),
            provider_statuses: vec![coral_api::v1::SearchProviderStatus {
                provider: SearchProvider::NativeFanout as i32,
                state: SearchProviderState::NotEnabled as i32,
                note: "search provider fanout disabled".to_string(),
                coverage: None,
                diagnostics: Vec::new(),
                diagnostics_truncated: false,
                omitted_diagnostic_count: 0,
            }],
            truncation: None,
        };

        let json = format_search_response_json(&response).expect("JSON response");
        assert_eq!(
            json,
            r#"{
  "results": [],
  "provider_statuses": [
    {
      "provider": "native_fanout",
      "state": "not_enabled",
      "note": "search provider fanout disabled",
      "coverage": null
    }
  ],
  "truncation": null
}"#
        );
        assert_eq!(
            format_search_response_text(&response),
            "Results\nNo results.\n\nProvider statuses\n- native_fanout: not_enabled: search provider fanout disabled\n"
        );
    }

    #[test]
    fn native_rendering_does_not_fabricate_internal_or_provider_details() {
        let rendered = format_search_response_json(&native_response()).expect("JSON response");

        for forbidden in [
            "internal_key",
            "rendered_sql",
            "query_text",
            "arguments",
            "raw_error",
            "response_body",
            "request_url",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "native rendering leaked forbidden field {forbidden}: {rendered}"
            );
        }
    }
}
