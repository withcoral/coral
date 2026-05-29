//! Rendering helpers for `coral search`.

use coral_api::v1::catalog_item;
use coral_api::v1::search_result::Payload;
use coral_api::v1::{
    CatalogItem, ColumnHint, NativeSearchPath, ObservedValue, SearchProvider, SearchProviderState,
    SearchProviderStatus, SearchResponse, SearchResult, SearchResultTruncation, SearchResultType,
    SearchSurfaceKind, TableFunction, TableFunctionArgument, TableFunctionResultColumn,
    TableSummary,
};
use serde_json::{Value, json};

pub(crate) fn search_rows(response: &SearchResponse) -> Vec<[String; 3]> {
    response
        .results
        .iter()
        .filter_map(search_result_row)
        .collect()
}

pub(crate) fn search_json(response: &SearchResponse) -> Result<String, serde_json::Error> {
    serde_json::to_string(&search_value(response))
}

fn search_value(response: &SearchResponse) -> Value {
    json!({
        "provider_statuses": response
            .provider_statuses
            .iter()
            .map(provider_status_value)
            .collect::<Vec<_>>(),
        "truncation": response
            .truncation
            .as_ref()
            .map_or(Value::Null, truncation_value),
        "results": response
            .results
            .iter()
            .filter_map(search_result_value)
            .collect::<Vec<_>>()
    })
}

fn search_result_row(result: &SearchResult) -> Option<[String; 3]> {
    match result.payload.as_ref()? {
        Payload::CatalogItem(item) => catalog_item_row(item),
        Payload::ColumnHint(hint) => Some([
            search_result_type_name(SearchResultType::ColumnHint).to_string(),
            qualified_field_name(&hint.schema_name, &hint.surface_name, &hint.name),
            field_details(&hint.data_type, hint.required, &hint.description),
        ]),
        Payload::ObservedValue(value) => Some([
            search_result_type_name(SearchResultType::ObservedValue).to_string(),
            qualified_field_name(&value.schema_name, &value.surface_name, &value.column_name),
            value.value.clone(),
        ]),
        Payload::NativeSearchPath(path) => native_search_path_row(path),
    }
}

fn catalog_item_row(item: &CatalogItem) -> Option<[String; 3]> {
    match item.item.as_ref()? {
        catalog_item::Item::Table(table) => Some([
            search_result_type_name(SearchResultType::CatalogItem).to_string(),
            qualified_name(&table.schema_name, &table.name),
            compact_details("table", &table.description),
        ]),
        catalog_item::Item::TableFunction(function) => Some([
            search_result_type_name(SearchResultType::CatalogItem).to_string(),
            qualified_name(&function.schema_name, &function.name),
            compact_details("table_function", &function.description),
        ]),
    }
}

fn native_search_path_row(path: &NativeSearchPath) -> Option<[String; 3]> {
    let function = path.table_function.as_ref()?;
    Some([
        search_result_type_name(SearchResultType::NativeSearchPath).to_string(),
        qualified_name(&function.schema_name, &function.name),
        if path.sql_call_example.is_empty() {
            function.description.clone()
        } else {
            path.sql_call_example.clone()
        },
    ])
}

fn field_details(data_type: &str, required: bool, description: &str) -> String {
    let mut parts = Vec::new();
    if !data_type.is_empty() {
        parts.push(data_type.to_string());
    }
    if required {
        parts.push("required".to_string());
    }
    if !description.is_empty() {
        parts.push(description.to_string());
    }
    parts.join("; ")
}

fn compact_details(kind: &str, description: &str) -> String {
    if description.is_empty() {
        kind.to_string()
    } else {
        format!("{kind}; {description}")
    }
}

fn provider_status_value(status: &SearchProviderStatus) -> Value {
    json!({
        "provider": provider_name(SearchProvider::try_from(status.provider).ok()),
        "state": provider_state(SearchProviderState::try_from(status.state).ok()),
        "note": status.note
    })
}

fn truncation_value(truncation: &SearchResultTruncation) -> Value {
    json!({
        "truncated": truncation.truncated,
        "returned_count": truncation.returned_count,
        "max_results": truncation.max_results,
        "note": truncation.note
    })
}

fn search_result_value(result: &SearchResult) -> Option<Value> {
    match result.payload.as_ref()? {
        Payload::CatalogItem(item) => catalog_item_value(item),
        Payload::ColumnHint(hint) => Some(column_hint_value(hint)),
        Payload::ObservedValue(value) => Some(observed_value(value)),
        Payload::NativeSearchPath(path) => native_search_path_value(path),
    }
}

fn catalog_item_value(item: &CatalogItem) -> Option<Value> {
    match item.item.as_ref()? {
        catalog_item::Item::Table(table) => Some(table_summary_value(table)),
        catalog_item::Item::TableFunction(function) => Some(table_function_value(function)),
    }
}

fn table_summary_value(table: &TableSummary) -> Value {
    json!({
        "type": search_result_type_name(SearchResultType::CatalogItem),
        "kind": "table",
        "schema_name": table.schema_name,
        "name": qualified_name(&table.schema_name, &table.name),
        "sql_reference": format_sql_reference(&table.schema_name, &table.name),
        "description": table.description,
        "table": {
            "table_name": table.name,
            "guide": table.guide,
            "required_filters": table.required_filters
        }
    })
}

fn table_function_value(function: &TableFunction) -> Value {
    json!({
        "type": search_result_type_name(SearchResultType::CatalogItem),
        "kind": "table_function",
        "schema_name": function.schema_name,
        "name": qualified_name(&function.schema_name, &function.name),
        "sql_reference": format_sql_reference(&function.schema_name, &function.name),
        "description": function.description,
        "table_function": {
            "function_name": function.name,
            "kind": function.kind,
            "arguments": function.arguments.iter().map(argument_value).collect::<Vec<_>>(),
            "result_columns": function
                .result_columns
                .iter()
                .map(result_column_value)
                .collect::<Vec<_>>()
        }
    })
}

fn column_hint_value(hint: &ColumnHint) -> Value {
    json!({
        "type": search_result_type_name(SearchResultType::ColumnHint),
        "schema_name": hint.schema_name,
        "surface_name": hint.surface_name,
        "surface_kind": surface_kind(SearchSurfaceKind::try_from(hint.surface_kind).ok()),
        "name": hint.name,
        "data_type": hint.data_type,
        "required": hint.required,
        "description": hint.description,
        "matched_fields": hint.matched_fields
    })
}

fn observed_value(value: &ObservedValue) -> Value {
    json!({
        "type": search_result_type_name(SearchResultType::ObservedValue),
        "value": value.value,
        "schema_name": value.schema_name,
        "surface_name": value.surface_name,
        "column_name": value.column_name
    })
}

fn native_search_path_value(path: &NativeSearchPath) -> Option<Value> {
    let function = path.table_function.as_ref()?;
    Some(json!({
        "type": search_result_type_name(SearchResultType::NativeSearchPath),
        "schema_name": function.schema_name,
        "name": qualified_name(&function.schema_name, &function.name),
        "sql_reference": format_sql_reference(&function.schema_name, &function.name),
        "sql_call_example": path.sql_call_example,
        "description": function.description,
        "arguments": function.arguments.iter().map(argument_value).collect::<Vec<_>>(),
        "result_columns": function
            .result_columns
            .iter()
            .map(result_column_value)
            .collect::<Vec<_>>(),
        "matched_fields": path.matched_fields
    }))
}

fn argument_value(argument: &TableFunctionArgument) -> Value {
    json!({
        "name": argument.name,
        "required": argument.required,
        "values": argument.values
    })
}

fn result_column_value(column: &TableFunctionResultColumn) -> Value {
    json!({
        "column_name": column.name,
        "data_type": column.data_type,
        "is_nullable": column.nullable,
        "description": column.description
    })
}

fn search_result_type_name(result_type: SearchResultType) -> &'static str {
    match result_type {
        SearchResultType::CatalogItem => "catalog_item",
        SearchResultType::ColumnHint => "column_hint",
        SearchResultType::ObservedValue => "observed_value",
        SearchResultType::NativeSearchPath => "native_search_path",
        SearchResultType::Unspecified => "unknown",
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

fn qualified_name(schema_name: &str, name: &str) -> String {
    format!("{schema_name}.{name}")
}

fn qualified_field_name(schema_name: &str, surface_name: &str, field_name: &str) -> String {
    format!("{schema_name}.{surface_name}.{field_name}")
}

fn format_sql_reference(schema_name: &str, name: &str) -> String {
    format!(
        "{}.{}",
        format_sql_identifier(schema_name),
        format_sql_identifier(name)
    )
}

fn format_sql_identifier(identifier: &str) -> String {
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

#[cfg(test)]
mod tests {
    use coral_api::v1::search_result::Payload;
    use coral_api::v1::{
        ColumnHint, SearchProvider, SearchProviderState, SearchProviderStatus, SearchResponse,
        SearchResult, SearchResultTruncation, SearchResultType, SearchSurfaceKind,
    };
    use serde_json::Value;

    use super::{search_json, search_rows};

    #[test]
    fn search_output_renders_rows_and_named_json() {
        let response = SearchResponse {
            results: vec![SearchResult {
                r#type: SearchResultType::ColumnHint as i32,
                payload: Some(Payload::ColumnHint(ColumnHint {
                    workspace: None,
                    schema_name: "github".to_string(),
                    surface_name: "issues".to_string(),
                    surface_kind: SearchSurfaceKind::Table as i32,
                    name: "title".to_string(),
                    data_type: "Utf8".to_string(),
                    required: false,
                    description: "Issue title".to_string(),
                    matched_fields: vec!["description".to_string()],
                })),
            }],
            provider_statuses: vec![SearchProviderStatus {
                provider: SearchProvider::CatalogMetadata as i32,
                state: SearchProviderState::ResultsFound as i32,
                note: "1 result".to_string(),
            }],
            truncation: Some(SearchResultTruncation {
                truncated: false,
                returned_count: 1,
                max_results: 10,
                note: String::new(),
            }),
        };

        assert_eq!(
            search_rows(&response),
            vec![[
                "column_hint".to_string(),
                "github.issues.title".to_string(),
                "Utf8; Issue title".to_string()
            ]]
        );

        let json: Value =
            serde_json::from_str(&search_json(&response).expect("json")).expect("parse json");
        assert_eq!(
            json.pointer("/provider_statuses/0/provider")
                .and_then(Value::as_str),
            Some("catalog_metadata")
        );
        assert_eq!(
            json.pointer("/results/0/type").and_then(Value::as_str),
            Some("column_hint")
        );
    }
}
