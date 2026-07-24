//! Shared Universal Search response rendering for thin clients.

use std::fmt::Write as _;

use coral_api::v1::{
    CatalogItem, CatalogMetadata, ColumnHint, ObservedValue, SearchFieldRole, SearchLimits,
    SearchProvider, SearchProviderCoverage, SearchProviderState, SearchResponse, SearchResult,
    SearchResultTruncation, SearchSurfaceKind, SearchTableColumnPreview,
    SearchTableColumnPreviewColumn, TableFunction, TableFunctionArgument, TableFunctionKind,
    TableFunctionResultColumn, TableSummary, catalog_item, search_result,
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

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
            schema_name: &table.schema_name,
            name: format!("{}.{}", table.schema_name, table.name),
            sql_reference: format_schema_table_equivalent(&table.schema_name, &table.name),
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
    require_guide_read: bool,
    required_filters: &'a [String],
}

impl<'a> From<&'a TableSummary> for TableSummaryDetailsValue<'a> {
    fn from(table: &'a TableSummary) -> Self {
        Self {
            table_name: &table.name,
            guide: &table.guide,
            require_guide_read: table.require_guide_read,
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
            sql_reference: format_schema_table_equivalent(&function.schema_name, &function.name),
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
    require_guide_read: bool,
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
            require_guide_read: function.require_guide_read,
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
struct SearchProviderStatusValue<'a> {
    provider: &'static str,
    state: &'static str,
    note: &'a str,
    coverage: Option<SearchProviderCoverageValue>,
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
        }
    }
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
                "{index}. [{provider}] table {}.{}",
                table.schema_name, table.name
            ),
            format!(
                "   SQL: {}",
                format_schema_table_equivalent(&table.schema_name, &table.name)
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
                    format_schema_table_equivalent(&function.schema_name, &function.name)
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
            format_schema_table_equivalent(&hint.schema_name, &hint.surface_name)
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
    line
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
    let reference = format_schema_table_equivalent(&function.schema_name, &function.name);
    let required_arguments = function
        .arguments
        .iter()
        .filter(|argument| argument.required)
        .map(|argument| format!("{} => '<value>'", format_sql_identifier(&argument.name)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{reference}({required_arguments})")
}

/// Formats a schema-qualified SQL table or table-function reference.
#[must_use]
pub fn format_schema_table_equivalent(schema_name: &str, table_name: &str) -> String {
    format!(
        "{}.{}",
        format_sql_identifier(schema_name),
        format_sql_identifier(table_name)
    )
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
mod tests {
    use super::*;

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
}
