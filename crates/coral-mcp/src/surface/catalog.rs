use coral_api::v1::{
    ColumnSearchResult, DescribeTableResponse, ListColumnsResponse, SearchTablesResponse,
    Table as ProtoTable, TableSearchResult as ProtoTableSearchResult,
    TableSummary as ProtoTableSummary,
};
use serde_json::{Map, Value};

use super::values::{
    insert_pagination_fields, missing_table_summary_value, paged_collection_value,
    queryable_table_summary_value,
};

pub(crate) fn describe_table_value(
    schema: &str,
    table: &str,
    response: &DescribeTableResponse,
) -> Value {
    if let Some(table) = &response.table {
        return describe_found_table_value(table);
    }
    describe_missing_table_value(
        schema,
        table,
        &response.available_schemas,
        &response.same_schema_tables,
        &response.suggestions,
    )
}

fn describe_found_table_value(table: &ProtoTable) -> Value {
    serde_json::json!({
        "found": true,
        "schema_name": table.schema_name,
        "table_name": table.name,
        "name": format!("{}.{}", table.schema_name, table.name),
        "description": table.description,
        "guide": table.guide,
        "required_filters": table.required_filters,
        "column_count": table.columns.len(),
        "columns_hint": "Use list_columns with this schema/table to inspect columns.",
    })
}

fn describe_missing_table_value(
    schema: &str,
    table: &str,
    available_schemas: &[String],
    same_schema_tables: &[ProtoTableSummary],
    suggestions: &[ProtoTableSummary],
) -> Value {
    let same_schema_tables = same_schema_tables
        .iter()
        .map(missing_table_summary_value)
        .collect::<Vec<_>>();
    let suggestions = suggestions
        .iter()
        .map(missing_table_summary_value)
        .collect::<Vec<_>>();
    let escaped_table = regex::escape(table);
    let search_arguments = if same_schema_tables.is_empty() {
        serde_json::json!({
            "pattern": escaped_table,
        })
    } else {
        serde_json::json!({
            "pattern": escaped_table,
            "schema": schema,
        })
    };
    let mut suggested_calls = vec![serde_json::json!({
        "tool": "search_tables",
        "arguments": search_arguments,
    })];
    if !same_schema_tables.is_empty() {
        suggested_calls.push(serde_json::json!({
            "tool": "list_tables",
            "arguments": {
                "schema": schema,
                "limit": 10,
            }
        }));
    }
    serde_json::json!({
        "found": false,
        "requested": {
            "schema": schema,
            "table": table,
        },
        "available_schemas": available_schemas,
        "same_schema_tables": same_schema_tables,
        "suggestions": suggestions,
        "suggested_calls": suggested_calls,
    })
}

pub(crate) fn search_tables_value(response: &SearchTablesResponse) -> Value {
    let pagination = response.pagination.unwrap_or_default();
    let tables = response
        .tables
        .iter()
        .filter_map(table_search_result_value)
        .collect::<Vec<_>>();
    paged_collection_value("tables", tables, &pagination)
}

fn table_search_result_value(result: &ProtoTableSearchResult) -> Option<Value> {
    result.table.as_ref().map(|table| {
        let mut value = queryable_table_summary_value(table);
        value
            .as_object_mut()
            .expect("table summary value is initialized as a JSON object")
            .insert(
                "matched_fields".to_string(),
                serde_json::json!(result.matched_fields),
            );
        value
    })
}

pub(crate) fn list_columns_value(
    schema: &str,
    table: &str,
    response: &ListColumnsResponse,
) -> Value {
    let pagination = response.pagination.unwrap_or_default();
    let columns = response
        .columns
        .iter()
        .filter_map(column_search_result_value)
        .collect::<Vec<_>>();
    let mut value = Map::from_iter([
        ("schema_name".to_string(), serde_json::json!(schema)),
        ("table_name".to_string(), serde_json::json!(table)),
        ("columns".to_string(), serde_json::json!(columns)),
    ]);
    insert_pagination_fields(&mut value, &pagination);
    Value::Object(value)
}

fn column_search_result_value(result: &ColumnSearchResult) -> Option<Value> {
    let column = result.column.as_ref()?;
    let mut value = serde_json::Map::from_iter([
        ("column_name".to_string(), serde_json::json!(column.name)),
        ("data_type".to_string(), serde_json::json!(column.data_type)),
        (
            "is_nullable".to_string(),
            serde_json::json!(column.nullable),
        ),
        (
            "is_virtual".to_string(),
            serde_json::json!(column.is_virtual),
        ),
        (
            "is_required_filter".to_string(),
            serde_json::json!(column.is_required_filter),
        ),
        (
            "description".to_string(),
            serde_json::json!(column.description),
        ),
        (
            "ordinal_position".to_string(),
            serde_json::json!(column.ordinal_position),
        ),
    ]);
    if !result.matched_fields.is_empty() {
        value.insert(
            "matched_fields".to_string(),
            serde_json::json!(result.matched_fields),
        );
    }
    Some(Value::Object(value))
}
