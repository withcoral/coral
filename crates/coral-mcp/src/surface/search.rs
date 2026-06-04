use std::sync::Arc;

use coral_api::v1::provider_search_result_value;
use coral_api::v1::search_result::Payload;
use coral_api::v1::{
    CatalogMetadata, ColumnHint, NativeSearchPath, ProviderSearchResult, ProviderSearchResultValue,
    SearchFieldRole, SearchProvider, SearchProviderState, SearchResponse, SearchResult,
    SearchResultTruncation, SearchSurfaceKind, SearchTableColumnPreviewColumn,
};
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::catalog::catalog_item_value;
use super::values::format_schema_table_equivalent;

pub(crate) fn search_value(response: &SearchResponse) -> Value {
    let truncation = match response.truncation.as_ref() {
        Some(truncation) => truncation_value(truncation),
        None => TruncationValue::empty(),
    };
    serde_json::to_value(SearchValue {
        provider_statuses: response
            .provider_statuses
            .iter()
            .map(ProviderStatusValue::from)
            .collect(),
        truncation,
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
                    "description": "Per-provider coverage and error metadata computed before final global ranking and truncation. A provider state of results_found means that provider produced candidates, not that one of those candidates appears in the returned results window.",
                    "items": {
                        "type": "object",
                        "required": ["provider", "state", "note"],
                        "additionalProperties": false,
                        "properties": {
                            "provider": {
                                "type": "string",
                                "enum": ["catalog_metadata", "observed_values", "source_native", "unknown"]
                            },
                            "state": {
                                "type": "string",
                                "description": "Provider run state for this query before final result truncation. Treat error and partial states as diagnostics; treat results_found without a returned result from that provider as a signal to retry with a more targeted query.",
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
                    "description": "Final global result-window metadata after provider candidates are merged and ranked. Search has no MCP pagination; if truncated, retry with a more targeted keyword/identifier query.",
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
                            provider_search_result_schema(),
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
            provider: SearchProvider::try_from(status.provider)
                .unwrap_or(SearchProvider::Unspecified)
                .label(),
            state: SearchProviderState::try_from(status.state)
                .unwrap_or(SearchProviderState::Unspecified)
                .label(),
            note: &status.note,
        }
    }
}

#[derive(Serialize)]
struct TruncationValue<'a> {
    truncated: bool,
    returned_count: u32,
    max_results: u32,
    note: &'a str,
}

impl TruncationValue<'_> {
    fn empty() -> Self {
        Self {
            truncated: false,
            returned_count: 0,
            max_results: 1,
            note: "",
        }
    }
}

fn truncation_value(truncation: &SearchResultTruncation) -> TruncationValue<'_> {
    TruncationValue {
        truncated: truncation.truncated,
        returned_count: truncation.returned_count,
        max_results: truncation.max_results,
        note: &truncation.note,
    }
}

fn search_result_value(result: &SearchResult) -> Option<Value> {
    let provider = SearchProvider::try_from(result.provider)
        .unwrap_or(SearchProvider::Unspecified)
        .label();
    match result.payload.as_ref()? {
        Payload::CatalogMetadata(metadata) => catalog_metadata_result_value(metadata, provider),
        Payload::ColumnHint(hint) => Some(column_hint_value(hint, provider)),
        Payload::ObservedValue(value) => serde_json::to_value(ObservedValueResult {
            provider,
            r#type: "observed_value",
            value: &value.value,
            schema_name: &value.schema_name,
            surface_name: &value.surface_name,
            surface_kind: SearchSurfaceKind::try_from(value.surface_kind)
                .unwrap_or(SearchSurfaceKind::Unspecified)
                .label(),
            column_name: &value.column_name,
            field_path: &value.field_path,
            observed_count: value.observed_count,
            last_observed_at: &value.last_observed_at,
        })
        .ok(),
        Payload::NativeSearchPath(path) => native_search_path_value(path, provider),
        Payload::ProviderSearchResult(result) => {
            provider_search_result_json_value(result, provider)
        }
    }
}

fn catalog_metadata_result_value(
    metadata: &CatalogMetadata,
    provider: &'static str,
) -> Option<Value> {
    let mut value = catalog_item_value(metadata.item.as_ref()?)?;
    let object = value.as_object_mut()?;
    object.insert("provider".to_string(), Value::from(provider));
    object.insert("type".to_string(), Value::from("catalog_item"));
    object.insert(
        "matched_fields".to_string(),
        serde_json::to_value(&metadata.matched_fields).ok()?,
    );

    if let Some(preview) = &metadata.table_column_preview {
        let table = object.get_mut("table").and_then(Value::as_object_mut)?;
        table.insert(
            "column_count".to_string(),
            Value::from(preview.column_count),
        );
        table.insert(
            "column_preview".to_string(),
            serde_json::to_value(
                preview
                    .columns
                    .iter()
                    .map(TableColumnPreviewColumnValue::from)
                    .collect::<Vec<_>>(),
            )
            .ok()?,
        );
        table.insert(
            "omitted_column_count".to_string(),
            Value::from(preview.omitted_column_count),
        );
    }

    Some(value)
}

fn column_hint_value(hint: &ColumnHint, provider: &'static str) -> Value {
    serde_json::to_value(ColumnHintResult {
        provider,
        r#type: "column_hint",
        schema_name: &hint.schema_name,
        surface_name: &hint.surface_name,
        surface_kind: SearchSurfaceKind::try_from(hint.surface_kind)
            .unwrap_or(SearchSurfaceKind::Unspecified)
            .label(),
        field_role: SearchFieldRole::try_from(hint.field_role)
            .unwrap_or(SearchFieldRole::Unspecified)
            .label(),
        name: &hint.name,
        data_type: &hint.data_type,
        required: hint.required,
        description: &hint.description,
        matched_fields: &hint.matched_fields,
    })
    .expect("column hint value serializes")
}

fn native_search_path_value(path: &NativeSearchPath, provider: &'static str) -> Option<Value> {
    let function = path.table_function.as_ref()?;
    serde_json::to_value(NativeSearchPathResult {
        provider,
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

fn provider_search_result_json_value(
    result: &ProviderSearchResult,
    provider: &'static str,
) -> Option<Value> {
    let function = result.table_function.as_ref()?;
    let mut value = Map::from_iter([
        ("provider".to_string(), json!(provider)),
        ("type".to_string(), json!("provider_search_result")),
        ("schema_name".to_string(), json!(function.schema_name)),
        (
            "name".to_string(),
            json!(format!("{}.{}", function.schema_name, function.name)),
        ),
        (
            "sql_reference".to_string(),
            json!(format_schema_table_equivalent(
                &function.schema_name,
                &function.name
            )),
        ),
        ("sql_call".to_string(), json!(result.sql_call)),
        ("row_ordinal".to_string(), json!(result.row_ordinal)),
        ("row".to_string(), provider_row_value(&result.row)),
    ]);
    insert_optional_provider_field(&mut value, "id", result.id.as_deref());
    insert_optional_provider_field(&mut value, "title", result.title.as_deref());
    insert_optional_provider_field(&mut value, "url", result.url.as_deref());
    insert_optional_provider_field(&mut value, "snippet", result.snippet.as_deref());
    if let Some(score) = result.score {
        value.insert("score".to_string(), json!(score));
    }
    Some(Value::Object(value))
}

fn insert_optional_provider_field(value: &mut Map<String, Value>, key: &str, field: Option<&str>) {
    if let Some(field) = field {
        value.insert(key.to_string(), json!(field));
    }
}

#[derive(Serialize)]
struct ColumnHintResult<'a> {
    provider: &'static str,
    r#type: &'static str,
    schema_name: &'a str,
    surface_name: &'a str,
    surface_kind: &'static str,
    field_role: &'static str,
    name: &'a str,
    data_type: &'a str,
    required: bool,
    description: &'a str,
    matched_fields: &'a [String],
}

#[derive(Serialize)]
struct ObservedValueResult<'a> {
    provider: &'static str,
    r#type: &'static str,
    value: &'a str,
    schema_name: &'a str,
    surface_name: &'a str,
    surface_kind: &'static str,
    column_name: &'a str,
    field_path: &'a str,
    observed_count: u64,
    last_observed_at: &'a str,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
struct NativeSearchPathResult<'a> {
    provider: &'static str,
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
    default_json: &'a str,
}

impl<'a> From<&'a coral_api::v1::TableFunctionArgument> for FunctionArgumentValue<'a> {
    fn from(argument: &'a coral_api::v1::TableFunctionArgument) -> Self {
        Self {
            name: &argument.name,
            required: argument.required,
            values: &argument.values,
            default_json: &argument.default_json,
        }
    }
}

fn provider_row_value(row: &std::collections::HashMap<String, ProviderSearchResultValue>) -> Value {
    let mut object = Map::new();
    for (key, value) in row {
        object.insert(key.clone(), provider_scalar_value(value));
    }
    Value::Object(object)
}

fn provider_scalar_value(value: &ProviderSearchResultValue) -> Value {
    match value.value.as_ref() {
        Some(provider_search_result_value::Value::StringValue(value)) => {
            Value::String(value.clone())
        }
        Some(provider_search_result_value::Value::NumberValue(value)) => json!(value),
        Some(provider_search_result_value::Value::BoolValue(value)) => json!(value),
        Some(provider_search_result_value::Value::JsonValue(value)) => {
            serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.clone()))
        }
        Some(provider_search_result_value::Value::NullValue(_)) | None => Value::Null,
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

fn catalog_item_result_schema() -> Value {
    json!({
        "oneOf": [
            catalog_table_result_schema(),
            catalog_table_function_result_schema()
        ]
    })
}

fn catalog_table_result_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "provider",
            "type",
            "kind",
            "schema_name",
            "name",
            "sql_reference",
            "description",
            "matched_fields",
            "table"
        ],
        "additionalProperties": false,
        "properties": {
            "provider": result_provider_schema(),
            "type": { "enum": ["catalog_item"] },
            "kind": { "enum": ["table"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "description": { "type": "string" },
            "matched_fields": {
                "type": "array",
                "items": { "type": "string" }
            },
            "table": {
                "type": "object",
                "required": ["table_name", "guide", "required_filters"],
                "additionalProperties": false,
                "properties": {
                    "table_name": { "type": "string" },
                    "guide": { "type": "string" },
                    "required_filters": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "column_count": {
                        "type": "integer",
                        "minimum": 0
                    },
                    "column_preview": {
                        "type": "array",
                        "items": table_column_preview_column_schema()
                    },
                    "omitted_column_count": {
                        "type": "integer",
                        "minimum": 0
                    }
                }
            }
        }
    })
}

fn catalog_table_function_result_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "provider",
            "type",
            "kind",
            "schema_name",
            "name",
            "sql_reference",
            "sql_call_example",
            "description",
            "matched_fields",
            "table_function"
        ],
        "additionalProperties": false,
        "properties": {
            "provider": result_provider_schema(),
            "type": { "enum": ["catalog_item"] },
            "kind": { "enum": ["table_function"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "sql_call_example": { "type": "string" },
            "description": { "type": "string" },
            "matched_fields": {
                "type": "array",
                "items": { "type": "string" }
            },
            "table_function": {
                "type": "object",
                "required": ["function_name", "arguments", "result_columns"],
                "additionalProperties": false,
                "properties": {
                    "function_name": { "type": "string" },
                    "arguments": {
                        "type": "array",
                        "items": function_argument_schema()
                    },
                    "result_columns": {
                        "type": "array",
                        "items": result_column_schema()
                    }
                }
            }
        }
    })
}

fn table_column_preview_column_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "column_name",
            "data_type",
            "is_required_filter",
            "description",
            "matched_fields"
        ],
        "additionalProperties": false,
        "properties": {
            "column_name": { "type": "string" },
            "data_type": { "type": "string" },
            "is_required_filter": { "type": "boolean" },
            "description": { "type": "string" },
            "matched_fields": {
                "type": "array",
                "items": { "type": "string" }
            }
        }
    })
}

fn column_hint_result_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "provider",
            "type",
            "schema_name",
            "surface_name",
            "surface_kind",
            "field_role",
            "name",
            "data_type",
            "required",
            "description",
            "matched_fields"
        ],
        "additionalProperties": false,
        "properties": {
            "provider": result_provider_schema(),
            "type": { "enum": ["column_hint"] },
            "schema_name": { "type": "string" },
            "surface_name": { "type": "string" },
            "surface_kind": {
                "type": "string",
                "enum": ["table", "table_function", "unknown"]
            },
            "field_role": {
                "type": "string",
                "enum": [
                    "table_column",
                    "table_filter",
                    "table_function_argument",
                    "table_function_result_column",
                    "unknown"
                ]
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
            "provider",
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
            "provider": result_provider_schema(),
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
        "required": [
            "provider",
            "type",
            "value",
            "schema_name",
            "surface_name",
            "surface_kind",
            "column_name",
            "field_path",
            "observed_count",
            "last_observed_at"
        ],
        "additionalProperties": false,
        "properties": {
            "provider": result_provider_schema(),
            "type": { "enum": ["observed_value"] },
            "value": { "type": "string" },
            "schema_name": { "type": "string" },
            "surface_name": { "type": "string" },
            "surface_kind": {
                "type": "string",
                "enum": ["table", "table_function", "unknown"]
            },
            "column_name": { "type": "string" },
            "field_path": { "type": "string" },
            "observed_count": {
                "type": "integer",
                "minimum": 0
            },
            "last_observed_at": { "type": "string" }
        }
    })
}

fn provider_search_result_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "provider",
            "type",
            "schema_name",
            "name",
            "sql_reference",
            "sql_call",
            "row_ordinal",
            "row"
        ],
        "additionalProperties": false,
        "properties": {
            "provider": result_provider_schema(),
            "type": { "enum": ["provider_search_result"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "sql_call": { "type": "string" },
            "row_ordinal": { "type": "integer", "minimum": 0 },
            "id": { "type": "string" },
            "title": { "type": "string" },
            "url": { "type": "string" },
            "snippet": { "type": "string" },
            "score": { "type": "number" },
            "row": {
                "type": "object",
                "additionalProperties": true
            }
        }
    })
}

fn result_provider_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["catalog_metadata", "observed_values", "source_native", "unknown"]
    })
}

fn function_argument_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name", "required", "values", "default_json"],
        "additionalProperties": false,
        "properties": {
            "name": { "type": "string" },
            "required": { "type": "boolean" },
            "values": {
                "type": "array",
                "items": { "type": "string" }
            },
            "default_json": { "type": "string" }
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
    use std::collections::HashMap;

    use coral_api::v1::catalog_item;
    use coral_api::v1::provider_search_result_value;
    use coral_api::v1::search_result::Payload;
    use coral_api::v1::{
        CatalogItem, CatalogMetadata, ObservedValue, ProviderSearchResult,
        ProviderSearchResultValue, SearchProvider, SearchProviderState, SearchProviderStatus,
        SearchResponse, SearchResult, SearchResultTruncation, SearchSurfaceKind,
        SearchTableColumnPreview, SearchTableColumnPreviewColumn, TableFunction,
        TableFunctionArgument, TableFunctionKind, TableFunctionResultColumn, TableSummary,
        Workspace,
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

    #[test]
    fn search_value_renders_catalog_metadata_preview() {
        let value = search_value(&SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::CatalogMetadata as i32,
                payload: Some(Payload::CatalogMetadata(CatalogMetadata {
                    item: Some(CatalogItem {
                        item: Some(catalog_item::Item::Table(TableSummary {
                            workspace: Some(Workspace {
                                name: "default".to_string(),
                            }),
                            schema_name: "github".to_string(),
                            name: "issues".to_string(),
                            description: "GitHub issues".to_string(),
                            required_filters: vec!["repo".to_string()],
                            guide: "Query GitHub issues.".to_string(),
                        })),
                    }),
                    matched_fields: vec!["source_name".to_string(), "table_name".to_string()],
                    table_column_preview: Some(SearchTableColumnPreview {
                        column_count: 9,
                        columns: vec![SearchTableColumnPreviewColumn {
                            name: "repo".to_string(),
                            data_type: "Utf8".to_string(),
                            is_required_filter: true,
                            description: "Repository name".to_string(),
                            matched_fields: vec!["column_name".to_string()],
                        }],
                        omitted_column_count: 8,
                    }),
                })),
            }],
            provider_statuses: Vec::new(),
            truncation: None,
        });

        let result = value.pointer("/results/0").expect("first result");
        assert_eq!(
            result.pointer("/provider").and_then(Value::as_str),
            Some("catalog_metadata")
        );
        assert_eq!(
            result.pointer("/type").and_then(Value::as_str),
            Some("catalog_item")
        );
        assert_eq!(
            result.pointer("/name").and_then(Value::as_str),
            Some("github.issues")
        );
        assert_eq!(
            result.pointer("/matched_fields/0").and_then(Value::as_str),
            Some("source_name")
        );
        assert_eq!(
            result
                .pointer("/table/column_count")
                .and_then(Value::as_u64),
            Some(9)
        );
        assert_eq!(
            result
                .pointer("/table/omitted_column_count")
                .and_then(Value::as_u64),
            Some(8)
        );
        assert_eq!(
            result
                .pointer("/table/column_preview/0/column_name")
                .and_then(Value::as_str),
            Some("repo")
        );
        assert_eq!(
            result
                .pointer("/table/column_preview/0/is_required_filter")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            result
                .pointer("/table/column_preview/0/matched_fields/0")
                .and_then(Value::as_str),
            Some("column_name")
        );
    }

    #[test]
    fn search_value_renders_observed_value_metadata() {
        let value = search_value(&SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::ObservedValues as i32,
                payload: Some(Payload::ObservedValue(ObservedValue {
                    value: "timeout".to_string(),
                    schema_name: "datadog".to_string(),
                    surface_name: "logs".to_string(),
                    column_name: "payload".to_string(),
                    surface_kind: SearchSurfaceKind::Table as i32,
                    field_path: "payload.error".to_string(),
                    observed_count: 4,
                    last_observed_at: "2026-06-04T10:00:00.000Z".to_string(),
                })),
            }],
            provider_statuses: Vec::new(),
            truncation: None,
        });

        let result = value.pointer("/results/0").expect("first result");
        assert_eq!(
            result.pointer("/provider").and_then(Value::as_str),
            Some("observed_values")
        );
        assert_eq!(
            result.pointer("/type").and_then(Value::as_str),
            Some("observed_value")
        );
        assert_eq!(
            result.pointer("/column_name").and_then(Value::as_str),
            Some("payload")
        );
        assert_eq!(
            result.pointer("/field_path").and_then(Value::as_str),
            Some("payload.error")
        );
        assert_eq!(
            result.pointer("/observed_count").and_then(Value::as_u64),
            Some(4)
        );
        assert_eq!(
            result.pointer("/last_observed_at").and_then(Value::as_str),
            Some("2026-06-04T10:00:00.000Z")
        );
    }

    fn provider_row_value(value: provider_search_result_value::Value) -> ProviderSearchResultValue {
        ProviderSearchResultValue { value: Some(value) }
    }

    fn provider_search_result_row() -> HashMap<String, ProviderSearchResultValue> {
        let mut row = HashMap::new();
        row.insert(
            "title".to_string(),
            provider_row_value(provider_search_result_value::Value::StringValue(
                "Fix search".to_string(),
            )),
        );
        row.insert(
            "rank".to_string(),
            provider_row_value(provider_search_result_value::Value::NumberValue(3.0)),
        );
        row.insert(
            "labels".to_string(),
            provider_row_value(provider_search_result_value::Value::JsonValue(
                r#"["search","bug"]"#.to_string(),
            )),
        );
        row.insert(
            "archived".to_string(),
            provider_row_value(provider_search_result_value::Value::BoolValue(false)),
        );
        row.insert(
            "milestone".to_string(),
            provider_row_value(provider_search_result_value::Value::NullValue(true)),
        );
        row
    }

    fn provider_search_table_function() -> TableFunction {
        TableFunction {
            workspace: Some(Workspace {
                name: "default".to_string(),
            }),
            schema_name: "github".to_string(),
            name: "search_issues".to_string(),
            description: "Search GitHub issues".to_string(),
            arguments: vec![TableFunctionArgument {
                name: "q".to_string(),
                required: true,
                values: Vec::new(),
                default_json: String::new(),
            }],
            result_columns: vec![TableFunctionResultColumn {
                name: "title".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
                description: "Issue title".to_string(),
            }],
            kind: TableFunctionKind::Search as i32,
            search_limits: None,
            universal_search: None,
        }
    }

    fn provider_search_response() -> SearchResponse {
        SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::SourceNative as i32,
                payload: Some(Payload::ProviderSearchResult(ProviderSearchResult {
                    table_function: Some(provider_search_table_function()),
                    sql_call: r#"SELECT * FROM "github"."search_issues"("q" => 'search') LIMIT 10"#
                        .to_string(),
                    row_ordinal: 2,
                    row: provider_search_result_row(),
                    id: Some("123".to_string()),
                    title: Some("Fix search".to_string()),
                    url: None,
                    snippet: Some("Search result snippet".to_string()),
                    score: Some(0.75),
                })),
            }],
            provider_statuses: vec![SearchProviderStatus {
                provider: SearchProvider::SourceNative as i32,
                state: SearchProviderState::ResultsFound as i32,
                note: "Source-native search returned 1 provider-ranked rows".to_string(),
            }],
            truncation: None,
        }
    }

    #[test]
    fn search_value_renders_provider_search_result_metadata() {
        let value = search_value(&provider_search_response());

        let status = value
            .pointer("/provider_statuses/0/provider")
            .and_then(Value::as_str);
        assert_eq!(status, Some("source_native"));

        let result = value.pointer("/results/0").expect("first result");
        assert_eq!(
            result.pointer("/provider").and_then(Value::as_str),
            Some("source_native")
        );
        assert_eq!(
            result.pointer("/type").and_then(Value::as_str),
            Some("provider_search_result")
        );
        assert_eq!(
            result.pointer("/name").and_then(Value::as_str),
            Some("github.search_issues")
        );
        assert_eq!(
            result.pointer("/sql_reference").and_then(Value::as_str),
            Some("\"github\".\"search_issues\"")
        );
        assert_eq!(
            result.pointer("/row_ordinal").and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(result.pointer("/id").and_then(Value::as_str), Some("123"));
        assert_eq!(
            result.pointer("/title").and_then(Value::as_str),
            Some("Fix search")
        );
        assert_eq!(result.pointer("/url"), None);
        assert_eq!(
            result.pointer("/snippet").and_then(Value::as_str),
            Some("Search result snippet")
        );
        assert_eq!(result.pointer("/score").and_then(Value::as_f64), Some(0.75));
        assert_eq!(
            result.pointer("/row/title").and_then(Value::as_str),
            Some("Fix search")
        );
        assert_eq!(
            result.pointer("/row/rank").and_then(Value::as_f64),
            Some(3.0)
        );
        assert_eq!(
            result.pointer("/row/labels/0").and_then(Value::as_str),
            Some("search")
        );
        assert_eq!(
            result.pointer("/row/archived").and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(result.pointer("/row/milestone"), Some(&Value::Null));
    }
}
