//! Shared Universal Search response rendering for thin clients.

use std::fmt::Write as _;

use std::collections::BTreeMap;

use coral_api::v1::{
    SearchField, SearchFunctionShape, SearchProvider, SearchProviderCoverage, SearchProviderState,
    SearchResponse, SearchResult, SearchResultTruncation, SearchSurfaceRef, SearchTableShape,
    TableFunction, search_result,
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

/// One queryable catalog entry rendered for a thin client.
///
/// Tables and functions differ in shape: a table's columns are both selectable
/// and filterable, so they share one map, while a function separates what you
/// supply from what you get back. Empty collections are omitted so a bare entry
/// hit stays short.
#[derive(Serialize, JsonSchema)]
#[serde(untagged)]
enum SearchResultValue<'a> {
    Table(TableResultValue<'a>),
    Function(FunctionResultValue<'a>),
    Unknown(UnknownResultValue),
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct TableResultValue<'a> {
    kind: &'static str,
    sql_reference: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    providers: Vec<&'static str>,
    description: &'a str,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    guide: &'a str,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    fields: BTreeMap<&'a str, &'a str>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required: Vec<&'a str>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    matching_values: BTreeMap<&'a str, Vec<&'a str>>,
    #[serde(default, skip_serializing_if = "is_default")]
    omitted_matching_field_count: u32,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct FunctionResultValue<'a> {
    kind: &'static str,
    sql_reference: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    providers: Vec<&'static str>,
    description: &'a str,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    guide: &'a str,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    arguments: BTreeMap<&'a str, &'a str>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required: Vec<&'a str>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    returns: BTreeMap<&'a str, &'a str>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    matching_values: BTreeMap<&'a str, Vec<&'a str>>,
    #[serde(default, skip_serializing_if = "is_default")]
    omitted_matching_field_count: u32,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct UnknownResultValue {
    kind: &'static str,
}

impl<'a> From<&'a SearchResult> for SearchResultValue<'a> {
    fn from(result: &'a SearchResult) -> Self {
        let Some(entry) = result.surface.as_ref() else {
            return Self::Unknown(UnknownResultValue { kind: "unknown" });
        };
        let sql_reference = entry_sql_reference(entry);
        let matching_values = matching_values(result);
        match result.shape.as_ref() {
            Some(search_result::Shape::Table(table)) => Self::Table(TableResultValue {
                kind: "table",
                sql_reference,
                providers: result_provider_names(result),
                description: &result.description,
                guide: &result.guide,
                fields: field_map(&table.fields),
                required: required_names(&table.fields),
                matching_values,
                omitted_matching_field_count: result.omitted_matching_field_count,
            }),
            Some(search_result::Shape::Function(function)) => Self::Function(FunctionResultValue {
                kind: "function",
                sql_reference,
                providers: result_provider_names(result),
                description: &result.description,
                guide: &result.guide,
                arguments: field_map(&function.arguments),
                required: required_names(&function.arguments),
                returns: field_map(&function.returns),
                matching_values,
                omitted_matching_field_count: result.omitted_matching_field_count,
            }),
            None => Self::Unknown(UnknownResultValue { kind: "unknown" }),
        }
    }
}

fn entry_sql_reference(entry: &SearchSurfaceRef) -> String {
    format_schema_table_equivalent(
        optional_catalog_name(&entry.catalog_name),
        &entry.schema_name,
        &entry.name,
    )
}

fn result_provider_names(result: &SearchResult) -> Vec<&'static str> {
    result
        .providers
        .iter()
        .map(|provider| provider_name(*provider))
        .collect()
}

fn field_map(fields: &[SearchField]) -> BTreeMap<&str, &str> {
    fields
        .iter()
        .map(|field| (field.name.as_str(), field.data_type.as_str()))
        .collect()
}

fn required_names(fields: &[SearchField]) -> Vec<&str> {
    fields
        .iter()
        .filter(|field| field.required)
        .map(|field| field.name.as_str())
        .collect()
}

fn matching_values(result: &SearchResult) -> BTreeMap<&str, Vec<&str>> {
    result
        .matching_values
        .iter()
        .map(|values| {
            (
                values.field.as_str(),
                values.values.iter().map(String::as_str).collect(),
            )
        })
        .collect()
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    value == &T::default()
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
    let Some(entry) = result.surface.as_ref() else {
        return vec![format!("{index}. unknown result")];
    };
    let reference = entry_sql_reference(entry);
    let kind = match result.shape.as_ref() {
        Some(search_result::Shape::Table(_)) => "table",
        Some(search_result::Shape::Function(_)) => "function",
        None => "unknown",
    };
    let mut lines = vec![format!("{index}. [{kind}] {reference}")];
    if !result.description.is_empty() {
        lines.push(format!("   {}", result.description));
    }
    match result.shape.as_ref() {
        Some(search_result::Shape::Table(table)) => push_table_lines(&mut lines, table),
        Some(search_result::Shape::Function(function)) => {
            push_function_lines(&mut lines, function);
        }
        None => {}
    }
    for values in &result.matching_values {
        lines.push(format!(
            "   matched {} = {}",
            values.field,
            values.values.join(", ")
        ));
    }
    if result.omitted_matching_field_count > 0 {
        lines.push(format!(
            "   {} more matching field(s) not shown",
            result.omitted_matching_field_count
        ));
    }
    if !result.guide.is_empty() {
        lines.push(format!("   {}", result.guide));
    }
    lines
}

fn push_table_lines(lines: &mut Vec<String>, table: &SearchTableShape) {
    push_field_line(lines, "required", &required_names(&table.fields));
    let optional = optional_names(&table.fields);
    push_field_line(lines, "fields", &optional);
}

fn push_function_lines(lines: &mut Vec<String>, function: &SearchFunctionShape) {
    push_field_line(lines, "required", &required_names(&function.arguments));
    push_field_line(lines, "arguments", &optional_names(&function.arguments));
    push_field_line(lines, "returns", &optional_names(&function.returns));
}

fn optional_names(fields: &[SearchField]) -> Vec<&str> {
    fields
        .iter()
        .filter(|field| !field.required)
        .map(|field| field.name.as_str())
        .collect()
}

fn push_field_line(lines: &mut Vec<String>, label: &str, names: &[&str]) {
    if names.is_empty() {
        return;
    }
    lines.push(format!("   {label}: {}", names.join(", ")));
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

#[cfg(test)]
mod tests {
    use super::*;
    use coral_api::v1::{SearchFieldValues, SearchSurfaceRef};

    fn table_result() -> SearchResult {
        SearchResult {
            surface: Some(SearchSurfaceRef {
                catalog_name: String::new(),
                schema_name: "github".to_string(),
                name: "repo_action_jobs".to_string(),
            }),
            description: "Action jobs for a repository".to_string(),
            guide: "Filter by owner, repo and job_id.".to_string(),
            shape: Some(search_result::Shape::Table(SearchTableShape {
                fields: vec![
                    SearchField {
                        name: "owner".to_string(),
                        data_type: "Utf8".to_string(),
                        required: true,
                    },
                    SearchField {
                        name: "conclusion".to_string(),
                        data_type: "Utf8".to_string(),
                        required: false,
                    },
                ],
            })),
            matching_values: vec![SearchFieldValues {
                field: "owner".to_string(),
                values: vec!["acme".to_string()],
            }],
            omitted_matching_field_count: 2,
            providers: vec![SearchProvider::CatalogMetadata as i32],
        }
    }

    fn function_result() -> SearchResult {
        SearchResult {
            surface: Some(SearchSurfaceRef {
                catalog_name: String::new(),
                schema_name: "github".to_string(),
                name: "search_issues".to_string(),
            }),
            description: "Search issues".to_string(),
            guide: "Supply a query.".to_string(),
            shape: Some(search_result::Shape::Function(SearchFunctionShape {
                arguments: vec![
                    SearchField {
                        name: "query".to_string(),
                        data_type: "Utf8".to_string(),
                        required: true,
                    },
                    SearchField {
                        name: "limit".to_string(),
                        data_type: "Int64".to_string(),
                        required: false,
                    },
                ],
                returns: vec![SearchField {
                    name: "title".to_string(),
                    data_type: "Utf8".to_string(),
                    required: false,
                }],
            })),
            matching_values: Vec::new(),
            omitted_matching_field_count: 0,
            providers: vec![SearchProvider::CatalogMetadata as i32],
        }
    }

    fn response(results: Vec<SearchResult>) -> SearchResponse {
        SearchResponse {
            results,
            provider_statuses: Vec::new(),
            truncation: None,
        }
    }

    fn first_result(value: &Value) -> Value {
        value
            .get("results")
            .and_then(|results| results.get(0))
            .cloned()
            .expect("one result")
    }

    #[test]
    fn rendered_results_are_numbered_from_one() {
        // The caller already passes a 1-based position, so incrementing again
        // labels the first result "2." and shifts every rank the agent reads.
        let text = format_search_response_text(&response(vec![table_result(), table_result()]));

        let numbered = text
            .lines()
            .filter(|line| line.starts_with(char::is_numeric))
            .collect::<Vec<_>>();
        let prefixes = numbered
            .iter()
            .map(|line| line.split_once(' ').map_or("", |(prefix, _)| prefix))
            .collect::<Vec<_>>();
        assert_eq!(
            prefixes,
            ["1.", "2."],
            "results must be numbered from one, got {numbered:?}"
        );
    }

    #[test]
    fn json_nests_fields_and_values_under_the_entry_that_owns_them() {
        let value = search_response_json_value(&response(vec![table_result()]));

        let result = first_result(&value);
        assert_eq!(result.get("kind").and_then(Value::as_str), Some("table"));
        assert_eq!(
            result.get("sql_reference").and_then(Value::as_str),
            Some("github.repo_action_jobs")
        );
        assert_eq!(
            result.pointer("/fields/conclusion").and_then(Value::as_str),
            Some("Utf8")
        );
        assert_eq!(
            result.pointer("/required/0").and_then(Value::as_str),
            Some("owner")
        );
        assert_eq!(
            result
                .pointer("/matching_values/owner/0")
                .and_then(Value::as_str),
            Some("acme")
        );
        assert_eq!(
            result
                .get("omitted_matching_field_count")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            result.pointer("/providers/0").and_then(Value::as_str),
            Some("catalog_metadata")
        );
    }

    #[test]
    fn json_omits_empty_collections_so_a_bare_entry_stays_short() {
        let bare = SearchResult {
            providers: Vec::new(),
            matching_values: Vec::new(),
            omitted_matching_field_count: 0,
            guide: String::new(),
            shape: Some(search_result::Shape::Table(SearchTableShape {
                fields: Vec::new(),
            })),
            ..table_result()
        };

        let value = search_response_json_value(&response(vec![bare]));

        let result = first_result(&value);
        assert!(result.get("fields").is_none());
        assert!(result.get("required").is_none());
        assert!(result.get("matching_values").is_none());
        assert!(result.get("guide").is_none());
        assert!(result.get("omitted_matching_field_count").is_none());
        assert!(result.get("providers").is_none());
    }

    #[test]
    fn text_output_labels_the_entry_and_its_matched_values() {
        let text = format_search_response_text(&response(vec![table_result()]));

        assert!(
            text.contains("[table] github.repo_action_jobs"),
            "text should lead with the queryable reference: {text}"
        );
        assert!(
            text.contains("matched owner = acme"),
            "text should show the literals to filter by: {text}"
        );
        assert!(
            text.contains("required: owner"),
            "text should show what must be constrained: {text}"
        );
    }

    #[test]
    fn json_renders_function_arguments_and_returns() {
        let value = search_response_json_value(&response(vec![function_result()]));

        let result = first_result(&value);
        assert_eq!(result.get("kind").and_then(Value::as_str), Some("function"));
        assert_eq!(
            result.pointer("/arguments/query").and_then(Value::as_str),
            Some("Utf8")
        );
        assert_eq!(
            result.pointer("/required/0").and_then(Value::as_str),
            Some("query")
        );
        assert_eq!(
            result.pointer("/returns/title").and_then(Value::as_str),
            Some("Utf8")
        );
    }

    #[test]
    fn text_renders_function_arguments_and_returns() {
        let text = format_search_response_text(&response(vec![function_result()]));

        assert!(text.contains("[function] github.search_issues"));
        assert!(text.contains("required: query"));
        assert!(text.contains("arguments: limit"));
        assert!(text.contains("returns: title"));
    }

    #[test]
    fn a_source_name_needing_quotes_stays_valid_sql() {
        let mut result = table_result();
        result.surface = Some(SearchSurfaceRef {
            catalog_name: String::new(),
            schema_name: "my-source".to_string(),
            name: "jobs".to_string(),
        });

        let value = search_response_json_value(&response(vec![result]));

        assert_eq!(
            first_result(&value)
                .get("sql_reference")
                .and_then(Value::as_str),
            Some("\"my-source\".jobs")
        );
    }

    #[test]
    fn catalog_qualified_table_reference_preserves_all_three_parts() {
        let mut result = table_result();
        result.surface = Some(SearchSurfaceRef {
            catalog_name: "warehouse".to_string(),
            schema_name: "analytics".to_string(),
            name: "events".to_string(),
        });

        let value = search_response_json_value(&response(vec![result]));

        assert_eq!(
            first_result(&value)
                .get("sql_reference")
                .and_then(Value::as_str),
            Some("warehouse.analytics.events")
        );
    }
}
