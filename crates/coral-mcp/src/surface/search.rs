use std::sync::Arc;

use coral_api::v1::search_result::Payload;
use coral_api::v1::{
    CatalogMetadata, ColumnHint, NativeSearchPath, SearchFieldRole, SearchProvider,
    SearchProviderState, SearchResponse, SearchResult, SearchResultTruncation, SearchSurfaceKind,
    SearchTableColumnPreviewColumn,
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
    let provider = provider_name(SearchProvider::try_from(result.provider).ok());
    match result.payload.as_ref()? {
        Payload::CatalogMetadata(metadata) => catalog_metadata_result_value(metadata, provider),
        Payload::ColumnHint(hint) => Some(column_hint_value(hint, provider)),
        Payload::ObservedValue(value) => serde_json::to_value(ObservedValueResult {
            provider,
            r#type: "observed_value",
            value: &value.value,
            schema_name: &value.schema_name,
            surface_name: &value.surface_name,
            surface_kind: surface_kind(SearchSurfaceKind::try_from(value.surface_kind).ok()),
            column_name: &value.column_name,
            field_path: &value.field_path,
            observed_count: value.observed_count,
            last_observed_at: &value.last_observed_at,
        })
        .ok(),
        Payload::NativeSearchPath(path) => native_search_path_value(path, provider),
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
        surface_kind: surface_kind(SearchSurfaceKind::try_from(hint.surface_kind).ok()),
        field_role: field_role(SearchFieldRole::try_from(hint.field_role).ok()),
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

fn field_role(role: Option<SearchFieldRole>) -> &'static str {
    match role {
        Some(SearchFieldRole::TableColumn) => "table_column",
        Some(SearchFieldRole::TableFilter) => "table_filter",
        Some(SearchFieldRole::TableFunctionArgument) => "table_function_argument",
        Some(SearchFieldRole::TableFunctionResultColumn) => "table_function_result_column",
        Some(SearchFieldRole::Unspecified) | None => "unknown",
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

fn result_provider_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["catalog_metadata", "observed_values", "unknown"]
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
    use coral_api::v1::catalog_item;
    use coral_api::v1::search_result::Payload;
    use coral_api::v1::{
        CatalogItem, CatalogMetadata, ObservedValue, SearchProvider, SearchProviderState,
        SearchProviderStatus, SearchResponse, SearchResult, SearchResultTruncation,
        SearchSurfaceKind, SearchTableColumnPreview, SearchTableColumnPreviewColumn, TableSummary,
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
}
