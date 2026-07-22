//! Shared Universal Search response rendering for thin clients.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use coral_api::v1::{
    CatalogMetadata, ObservedValue, SearchFieldRole, SearchProvider, SearchProviderCoverage,
    SearchProviderState, SearchResponse, SearchResult, SearchResultTruncation, SearchSurfaceKind,
    TableFunction, catalog_item, search_result,
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
#[serde(untagged)]
enum CatalogMetadataValue<'a> {
    Table(TableCatalogMetadataValue<'a>),
    TableFunction(TableFunctionCatalogMetadataValue<'a>),
    Unknown(UnknownCatalogMetadataValue<'a>),
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableCatalogMetadataValue<'a> {
    item: CatalogItemValue<'a>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    fields: BTreeMap<&'a str, Option<&'a str>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    required_filters: Vec<&'a str>,
    #[serde(skip_serializing_if = "is_default")]
    omitted_matching_field_count: u32,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableFunctionCatalogMetadataValue<'a> {
    item: CatalogItemValue<'a>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    arguments: Vec<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    required_arguments: Vec<&'a str>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    argument_values: BTreeMap<&'a str, Vec<&'a str>>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    returns: BTreeMap<&'a str, Option<&'a str>>,
    #[serde(skip_serializing_if = "is_default")]
    omitted_matching_field_count: u32,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct UnknownCatalogMetadataValue<'a> {
    item: Option<CatalogItemValue<'a>>,
}

impl<'a> From<&'a CatalogMetadata> for CatalogMetadataValue<'a> {
    fn from(metadata: &'a CatalogMetadata) -> Self {
        match metadata.item.as_ref().and_then(|item| item.item.as_ref()) {
            Some(catalog_item::Item::Table(table)) => Self::Table(TableCatalogMetadataValue {
                item: CatalogItemValue::from_table(table),
                fields: surface_field_map(
                    metadata,
                    &[SearchFieldRole::TableColumn, SearchFieldRole::TableFilter],
                ),
                required_filters: required_surface_field_names(
                    metadata,
                    SearchFieldRole::TableFilter,
                ),
                omitted_matching_field_count: metadata.omitted_matching_field_count,
            }),
            Some(catalog_item::Item::TableFunction(function)) => {
                let arguments =
                    surface_field_names(metadata, SearchFieldRole::TableFunctionArgument);
                Self::TableFunction(TableFunctionCatalogMetadataValue {
                    item: CatalogItemValue::from_table_function(function),
                    argument_values: argument_values(function, &arguments),
                    arguments,
                    required_arguments: required_surface_field_names(
                        metadata,
                        SearchFieldRole::TableFunctionArgument,
                    ),
                    returns: surface_field_map(
                        metadata,
                        &[SearchFieldRole::TableFunctionResultColumn],
                    ),
                    omitted_matching_field_count: metadata.omitted_matching_field_count,
                })
            }
            None => Self::Unknown(UnknownCatalogMetadataValue { item: None }),
        }
    }
}

fn surface_field_names(metadata: &CatalogMetadata, role: SearchFieldRole) -> Vec<&str> {
    let mut names = metadata
        .surface_fields
        .iter()
        .filter(|field| SearchFieldRole::try_from(field.role) == Ok(role))
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    names
}

fn argument_values<'a>(
    function: &'a TableFunction,
    included_arguments: &[&str],
) -> BTreeMap<&'a str, Vec<&'a str>> {
    function
        .arguments
        .iter()
        .filter(|argument| {
            !argument.values.is_empty() && included_arguments.contains(&argument.name.as_str())
        })
        .map(|argument| {
            (
                argument.name.as_str(),
                argument.values.iter().map(String::as_str).collect(),
            )
        })
        .collect()
}

fn surface_field_map<'a>(
    metadata: &'a CatalogMetadata,
    roles: &[SearchFieldRole],
) -> BTreeMap<&'a str, Option<&'a str>> {
    metadata
        .surface_fields
        .iter()
        .filter(|field| {
            SearchFieldRole::try_from(field.role).is_ok_and(|role| roles.contains(&role))
        })
        .map(|field| {
            (
                field.name.as_str(),
                (!field.data_type.is_empty()).then_some(field.data_type.as_str()),
            )
        })
        .collect()
}

fn required_surface_field_names(metadata: &CatalogMetadata, role: SearchFieldRole) -> Vec<&str> {
    let mut names = metadata
        .surface_fields
        .iter()
        .filter(|field| {
            field.required && SearchFieldRole::try_from(field.role).is_ok_and(|value| value == role)
        })
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    names
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    value == &T::default()
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogItemValue<'a> {
    kind: &'static str,
    name: String,
    sql_reference: String,
    description: &'a str,
}

impl<'a> CatalogItemValue<'a> {
    fn from_table(table: &'a coral_api::v1::TableSummary) -> Self {
        Self {
            kind: "table",
            name: format!("{}.{}", table.schema_name, table.name),
            sql_reference: format_schema_table_equivalent(&table.schema_name, &table.name),
            description: &table.description,
        }
    }

    fn from_table_function(function: &'a TableFunction) -> Self {
        Self {
            kind: "table_function",
            name: format!("{}.{}", function.schema_name, function.name),
            sql_reference: format_schema_table_equivalent(&function.schema_name, &function.name),
            description: &function.description,
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
        Some(catalog_item::Item::Table(table)) => {
            let mut lines = vec![
                format!(
                    "{index}. [{provider}] table {}.{}",
                    table.schema_name, table.name
                ),
                format!(
                    "   SQL: {}",
                    format_schema_table_equivalent(&table.schema_name, &table.name)
                ),
            ];
            push_surface_fields_line(
                &mut lines,
                "fields",
                metadata,
                &[SearchFieldRole::TableColumn, SearchFieldRole::TableFilter],
            );
            push_surface_field_names_line(
                &mut lines,
                "required filters",
                &required_surface_field_names(metadata, SearchFieldRole::TableFilter),
            );
            lines
        }
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
            push_surface_fields_line(
                &mut lines,
                "arguments",
                metadata,
                &[SearchFieldRole::TableFunctionArgument],
            );
            push_surface_field_names_line(
                &mut lines,
                "required arguments",
                &required_surface_field_names(metadata, SearchFieldRole::TableFunctionArgument),
            );
            push_surface_fields_line(
                &mut lines,
                "returns",
                metadata,
                &[SearchFieldRole::TableFunctionResultColumn],
            );
            lines
        }
        None => vec![format!("{index}. [{provider}] catalog metadata")],
    };
    if metadata.omitted_matching_field_count > 0 {
        lines.push(format!(
            "   omitted matching fields: {}",
            metadata.omitted_matching_field_count
        ));
    }
    lines
}

fn push_surface_fields_line(
    lines: &mut Vec<String>,
    label: &str,
    metadata: &CatalogMetadata,
    roles: &[SearchFieldRole],
) {
    let fields = metadata
        .surface_fields
        .iter()
        .filter(|field| {
            SearchFieldRole::try_from(field.role).is_ok_and(|role| roles.contains(&role))
        })
        .map(|field| {
            if field.data_type.is_empty() {
                field.name.clone()
            } else {
                format!("{} ({})", field.name, field.data_type)
            }
        })
        .collect::<Vec<_>>();
    if !fields.is_empty() {
        lines.push(format!("   {label}: {}", fields.join(", ")));
    }
}

fn push_surface_field_names_line(lines: &mut Vec<String>, label: &str, fields: &[&str]) {
    if !fields.is_empty() {
        lines.push(format!("   {label}: {}", fields.join(", ")));
    }
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

#[cfg(test)]
mod tests {
    use coral_api::v1::{
        CatalogItem, SearchSurfaceField, TableFunctionArgument, TableFunctionResultColumn,
    };

    use super::*;

    #[test]
    fn table_function_json_keeps_arguments_and_returns_separate() {
        let metadata = CatalogMetadata {
            item: Some(CatalogItem {
                item: Some(catalog_item::Item::TableFunction(TableFunction {
                    workspace: None,
                    schema_name: "notion".to_string(),
                    name: "search_data_source_templates".to_string(),
                    description: "Search data source templates".to_string(),
                    arguments: vec![TableFunctionArgument {
                        name: "name".to_string(),
                        required: true,
                        values: vec!["project".to_string(), "meeting".to_string()],
                    }],
                    result_columns: vec![TableFunctionResultColumn {
                        name: "name".to_string(),
                        data_type: "Utf8".to_string(),
                        nullable: false,
                        description: "Template name".to_string(),
                    }],
                    kind: 0,
                    search_limits: None,
                })),
            }),
            surface_fields: vec![
                SearchSurfaceField {
                    name: "name".to_string(),
                    data_type: String::new(),
                    required: true,
                    role: SearchFieldRole::TableFunctionArgument as i32,
                },
                SearchSurfaceField {
                    name: "name".to_string(),
                    data_type: "Utf8".to_string(),
                    required: false,
                    role: SearchFieldRole::TableFunctionResultColumn as i32,
                },
            ],
            omitted_matching_field_count: 0,
        };

        let value = serde_json::to_value(CatalogMetadataValue::from(&metadata)).expect("serialize");

        assert_eq!(value.get("arguments"), Some(&serde_json::json!(["name"])));
        assert_eq!(
            value.get("required_arguments"),
            Some(&serde_json::json!(["name"]))
        );
        assert_eq!(
            value.pointer("/argument_values/name"),
            Some(&serde_json::json!(["project", "meeting"]))
        );
        assert_eq!(
            value.pointer("/returns/name").and_then(Value::as_str),
            Some("Utf8")
        );
        assert!(value.get("fields").is_none());
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
