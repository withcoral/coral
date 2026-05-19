use coral_api::v1::{PaginationResponse, Table, TableSummary};
use serde_json::{Map, Value, json};

pub(crate) fn queryable_table_summary_value(table: &TableSummary) -> Value {
    json!({
        "schema_name": table.schema_name,
        "table_name": table.name,
        "name": format!("{}.{}", table.schema_name, table.name),
        "sql_reference": format_schema_table_equivalent(&table.schema_name, &table.name),
        "description": table.description,
        "guide": table.guide,
        "required_filters": table.required_filters,
    })
}

pub(crate) fn missing_table_summary_value(table: &TableSummary) -> Value {
    json!({
        "schema_name": table.schema_name,
        "table_name": table.name,
        "name": format!("{}.{}", table.schema_name, table.name),
        "description": table.description,
        "required_filters": table.required_filters,
    })
}

pub(crate) fn queryable_table_summary_values(tables: &[TableSummary]) -> Vec<Value> {
    let mut summaries = tables
        .iter()
        .map(queryable_table_summary_value)
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        left.get("name")
            .and_then(Value::as_str)
            .cmp(&right.get("name").and_then(Value::as_str))
    });
    summaries
}

pub(crate) fn table_to_summary(table: &Table) -> TableSummary {
    TableSummary {
        workspace: table.workspace.clone(),
        schema_name: table.schema_name.clone(),
        name: table.name.clone(),
        description: table.description.clone(),
        required_filters: table.required_filters.clone(),
        guide: table.guide.clone(),
    }
}

pub(crate) fn paged_collection_value(
    collection_key: &str,
    items: Vec<Value>,
    pagination: &PaginationResponse,
) -> Value {
    let mut value = Map::from_iter([(collection_key.to_string(), Value::Array(items))]);
    insert_pagination_fields(&mut value, pagination);
    Value::Object(value)
}

pub(crate) fn insert_pagination_fields(
    value: &mut Map<String, Value>,
    pagination: &PaginationResponse,
) {
    value.insert("total".to_string(), json!(pagination.total_count));
    value.insert("limit".to_string(), json!(pagination.limit));
    value.insert("offset".to_string(), json!(pagination.offset));
    value.insert("has_more".to_string(), json!(pagination.has_more));
    if pagination.has_more {
        value.insert("next_offset".to_string(), json!(pagination.next_offset));
    }
}

pub(crate) fn format_schema_table_equivalent(schema_name: &str, table_name: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier_if_needed(schema_name),
        quote_identifier_if_needed(table_name)
    )
}

fn quote_identifier_if_needed(identifier: &str) -> String {
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
