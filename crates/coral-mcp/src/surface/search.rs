use std::sync::Arc;

use coral_api::v1::search_result::Payload;
use coral_api::v1::{
    ColumnHint, NativeSearchPath, SearchProvider, SearchProviderState, SearchResponse,
    SearchResult, SearchResultTruncation, SearchSurfaceKind,
};
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::catalog::catalog_item_value;
use super::values::format_schema_table_equivalent;

pub(crate) fn search_value(response: &SearchResponse) -> Value {
    serde_json::to_value(SearchValue {
        provider_statuses: response
            .provider_statuses
            .iter()
            .map(ProviderStatusValue::from)
            .collect(),
        truncation: response
            .truncation
            .as_ref()
            .map(TruncationValue::from)
            .unwrap_or_default(),
        results: response
            .results
            .iter()
            .filter_map(search_result_value)
            .collect(),
    })
    .expect("search value serializes")
}

pub(crate) fn search_output_schema() -> Arc<Map<String, Value>> {
    Arc::new(
        json!({
            "type": "object",
            "required": ["provider_statuses", "truncation", "results"],
            "additionalProperties": false,
            "properties": {
                "provider_statuses": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["provider", "state", "note"],
                        "additionalProperties": false,
                        "properties": {
                            "provider": {
                                "type": "string",
                                "enum": ["catalog_metadata", "observed_values", "unknown"]
                            },
                            "state": {
                                "type": "string",
                                "enum": [
                                    "results_found",
                                    "empty",
                                    "not_enabled",
                                    "skipped",
                                    "partial",
                                    "error",
                                    "unknown"
                                ]
                            },
                            "note": { "type": "string" }
                        }
                    }
                },
                "truncation": {
                    "type": "object",
                    "required": ["truncated", "returned_count", "max_results", "note"],
                    "additionalProperties": false,
                    "properties": {
                        "truncated": { "type": "boolean" },
                        "returned_count": { "type": "integer", "minimum": 0 },
                        "max_results": { "type": "integer", "minimum": 1 },
                        "note": { "type": "string" }
                    }
                },
                "results": {
                    "type": "array",
                    "items": {
                        "oneOf": [
                            catalog_item_result_schema(),
                            column_hint_result_schema(),
                            native_search_path_result_schema(),
                            observed_value_result_schema()
                        ]
                    }
                }
            }
        })
        .as_object()
        .cloned()
        .expect("search output schema should be an object"),
    )
}

#[derive(Serialize)]
struct SearchValue<'a> {
    provider_statuses: Vec<ProviderStatusValue<'a>>,
    truncation: TruncationValue<'a>,
    results: Vec<Value>,
}

#[derive(Serialize)]
struct ProviderStatusValue<'a> {
    provider: &'static str,
    state: &'static str,
    note: &'a str,
}

impl<'a> From<&'a coral_api::v1::SearchProviderStatus> for ProviderStatusValue<'a> {
    fn from(status: &'a coral_api::v1::SearchProviderStatus) -> Self {
        Self {
            provider: provider_name(SearchProvider::try_from(status.provider).ok()),
            state: provider_state(SearchProviderState::try_from(status.state).ok()),
            note: &status.note,
        }
    }
}

#[derive(Default, Serialize)]
struct TruncationValue<'a> {
    truncated: bool,
    returned_count: u32,
    max_results: u32,
    note: &'a str,
}

impl<'a> From<&'a SearchResultTruncation> for TruncationValue<'a> {
    fn from(truncation: &'a SearchResultTruncation) -> Self {
        Self {
            truncated: truncation.truncated,
            returned_count: truncation.returned_count,
            max_results: truncation.max_results,
            note: &truncation.note,
        }
    }
}

fn search_result_value(result: &SearchResult) -> Option<Value> {
    match result.payload.as_ref()? {
        Payload::CatalogItem(item) => catalog_item_result_value(item),
        Payload::ColumnHint(hint) => Some(column_hint_value(hint)),
        Payload::ObservedValue(value) => serde_json::to_value(ObservedValueResult {
            r#type: "observed_value",
            value: &value.value,
            schema_name: &value.schema_name,
            surface_name: &value.surface_name,
            column_name: &value.column_name,
        })
        .ok(),
        Payload::NativeSearchPath(path) => native_search_path_value(path),
    }
}

fn catalog_item_result_value(item: &coral_api::v1::CatalogItem) -> Option<Value> {
    let mut value = catalog_item_value(item)?;
    value
        .as_object_mut()?
        .insert("type".to_string(), Value::from("catalog_item"));
    Some(value)
}

fn column_hint_value(hint: &ColumnHint) -> Value {
    serde_json::to_value(ColumnHintResult {
        r#type: "column_hint",
        schema_name: &hint.schema_name,
        surface_name: &hint.surface_name,
        surface_kind: surface_kind(SearchSurfaceKind::try_from(hint.surface_kind).ok()),
        name: &hint.name,
        data_type: &hint.data_type,
        required: hint.required,
        description: &hint.description,
        matched_fields: &hint.matched_fields,
    })
    .expect("column hint value serializes")
}

fn native_search_path_value(path: &NativeSearchPath) -> Option<Value> {
    let function = path.table_function.as_ref()?;
    serde_json::to_value(NativeSearchPathResult {
        r#type: "native_search_path",
        schema_name: &function.schema_name,
        name: format!("{}.{}", function.schema_name, function.name),
        sql_reference: format_schema_table_equivalent(&function.schema_name, &function.name),
        sql_call_example: &path.sql_call_example,
        description: &function.description,
        arguments: function
            .arguments
            .iter()
            .map(FunctionArgumentValue::from)
            .collect(),
        result_columns: function
            .result_columns
            .iter()
            .map(FunctionResultColumnValue::from)
            .collect(),
        matched_fields: &path.matched_fields,
    })
    .ok()
}

#[derive(Serialize)]
struct ColumnHintResult<'a> {
    r#type: &'static str,
    schema_name: &'a str,
    surface_name: &'a str,
    surface_kind: &'static str,
    name: &'a str,
    data_type: &'a str,
    required: bool,
    description: &'a str,
    matched_fields: &'a [String],
}

#[derive(Serialize)]
struct ObservedValueResult<'a> {
    r#type: &'static str,
    value: &'a str,
    schema_name: &'a str,
    surface_name: &'a str,
    column_name: &'a str,
}

#[derive(Serialize)]
struct NativeSearchPathResult<'a> {
    r#type: &'static str,
    schema_name: &'a str,
    name: String,
    sql_reference: String,
    sql_call_example: &'a str,
    description: &'a str,
    arguments: Vec<FunctionArgumentValue<'a>>,
    result_columns: Vec<FunctionResultColumnValue<'a>>,
    matched_fields: &'a [String],
}

#[derive(Serialize)]
struct FunctionArgumentValue<'a> {
    name: &'a str,
    required: bool,
    values: &'a [String],
}

impl<'a> From<&'a coral_api::v1::TableFunctionArgument> for FunctionArgumentValue<'a> {
    fn from(argument: &'a coral_api::v1::TableFunctionArgument) -> Self {
        Self {
            name: &argument.name,
            required: argument.required,
            values: &argument.values,
        }
    }
}

#[derive(Serialize)]
struct FunctionResultColumnValue<'a> {
    column_name: &'a str,
    data_type: &'a str,
    is_nullable: bool,
    description: &'a str,
}

impl<'a> From<&'a coral_api::v1::TableFunctionResultColumn> for FunctionResultColumnValue<'a> {
    fn from(column: &'a coral_api::v1::TableFunctionResultColumn) -> Self {
        Self {
            column_name: &column.name,
            data_type: &column.data_type,
            is_nullable: column.nullable,
            description: &column.description,
        }
    }
}

fn provider_name(provider: Option<SearchProvider>) -> &'static str {
    match provider {
        Some(SearchProvider::CatalogMetadata) => "catalog_metadata",
        Some(SearchProvider::ObservedValues) => "observed_values",
        Some(SearchProvider::Unspecified) | None => "unknown",
    }
}

fn provider_state(state: Option<SearchProviderState>) -> &'static str {
    match state {
        Some(SearchProviderState::ResultsFound) => "results_found",
        Some(SearchProviderState::Empty) => "empty",
        Some(SearchProviderState::NotEnabled) => "not_enabled",
        Some(SearchProviderState::Skipped) => "skipped",
        Some(SearchProviderState::Partial) => "partial",
        Some(SearchProviderState::Error) => "error",
        Some(SearchProviderState::Unspecified) | None => "unknown",
    }
}

fn surface_kind(kind: Option<SearchSurfaceKind>) -> &'static str {
    match kind {
        Some(SearchSurfaceKind::Table) => "table",
        Some(SearchSurfaceKind::TableFunction) => "table_function",
        Some(SearchSurfaceKind::Unspecified) | None => "unknown",
    }
}

fn catalog_item_result_schema() -> Value {
    json!({
        "type": "object",
        "required": ["type", "kind", "schema_name", "name", "sql_reference", "description"],
        "additionalProperties": true,
        "properties": {
            "type": { "enum": ["catalog_item"] },
            "kind": { "type": "string", "enum": ["table", "table_function"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "description": { "type": "string" }
        }
    })
}

fn column_hint_result_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "type",
            "schema_name",
            "surface_name",
            "surface_kind",
            "name",
            "data_type",
            "required",
            "description",
            "matched_fields"
        ],
        "additionalProperties": false,
        "properties": {
            "type": { "enum": ["column_hint"] },
            "schema_name": { "type": "string" },
            "surface_name": { "type": "string" },
            "surface_kind": {
                "type": "string",
                "enum": ["table", "table_function", "unknown"]
            },
            "name": { "type": "string" },
            "data_type": { "type": "string" },
            "required": { "type": "boolean" },
            "description": { "type": "string" },
            "matched_fields": {
                "type": "array",
                "items": { "type": "string" }
            }
        }
    })
}

fn native_search_path_result_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "type",
            "schema_name",
            "name",
            "sql_reference",
            "sql_call_example",
            "description",
            "arguments",
            "result_columns",
            "matched_fields"
        ],
        "additionalProperties": false,
        "properties": {
            "type": { "enum": ["native_search_path"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "sql_call_example": { "type": "string" },
            "description": { "type": "string" },
            "arguments": {
                "type": "array",
                "items": function_argument_schema()
            },
            "result_columns": {
                "type": "array",
                "items": result_column_schema()
            },
            "matched_fields": {
                "type": "array",
                "items": { "type": "string" }
            }
        }
    })
}

fn observed_value_result_schema() -> Value {
    json!({
        "type": "object",
        "required": ["type", "value", "schema_name", "surface_name", "column_name"],
        "additionalProperties": false,
        "properties": {
            "type": { "enum": ["observed_value"] },
            "value": { "type": "string" },
            "schema_name": { "type": "string" },
            "surface_name": { "type": "string" },
            "column_name": { "type": "string" }
        }
    })
}

fn function_argument_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name", "required", "values"],
        "additionalProperties": false,
        "properties": {
            "name": { "type": "string" },
            "required": { "type": "boolean" },
            "values": {
                "type": "array",
                "items": { "type": "string" }
            }
        }
    })
}

fn result_column_schema() -> Value {
    json!({
        "type": "object",
        "required": ["column_name", "data_type", "is_nullable", "description"],
        "additionalProperties": false,
        "properties": {
            "column_name": { "type": "string" },
            "data_type": { "type": "string" },
            "is_nullable": { "type": "boolean" },
            "description": { "type": "string" }
        }
    })
}

#[cfg(test)]
mod tests {
    use coral_api::v1::{
        SearchProvider, SearchProviderState, SearchProviderStatus, SearchResponse,
        SearchResultTruncation,
    };
    use serde_json::Value;

    use super::search_value;

    #[test]
    fn search_value_renders_provider_status_names() {
        let value = search_value(&SearchResponse {
            results: Vec::new(),
            provider_statuses: vec![SearchProviderStatus {
                provider: SearchProvider::ObservedValues as i32,
                state: SearchProviderState::NotEnabled as i32,
                note: "disabled".to_string(),
            }],
            truncation: Some(SearchResultTruncation {
                truncated: false,
                returned_count: 0,
                max_results: 10,
                note: String::new(),
            }),
        });

        let statuses = value
            .get("provider_statuses")
            .and_then(Value::as_array)
            .expect("provider statuses");
        let status = statuses.first().expect("first provider status");
        assert_eq!(
            status.get("provider").and_then(Value::as_str),
            Some("observed_values")
        );
        assert_eq!(
            status.get("state").and_then(Value::as_str),
            Some("not_enabled")
        );
    }
}
