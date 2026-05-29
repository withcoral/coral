use coral_api::v1::{PaginationResponse, TableSummary};
use coral_client::format_sql_reference;
use serde::Serialize;
use serde_json::{Map, Value};

pub(crate) use coral_client::format_sql_identifier;

pub(crate) fn queryable_table_summary_value(table: &TableSummary) -> Value {
    serde_json::to_value(QueryableTableSummaryValue::from(table))
        .expect("queryable table summary value serializes")
}

pub(crate) fn missing_table_summary_value(table: &TableSummary) -> Value {
    serde_json::to_value(MissingTableSummaryValue::from(table))
        .expect("missing table summary value serializes")
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
    value.insert("total".to_string(), Value::from(pagination.total_count));
    value.insert("limit".to_string(), Value::from(pagination.limit));
    value.insert("offset".to_string(), Value::from(pagination.offset));
    value.insert("has_more".to_string(), Value::from(pagination.has_more));
    if pagination.has_more {
        value.insert(
            "next_offset".to_string(),
            Value::from(pagination.next_offset),
        );
    }
}

#[derive(Serialize)]
struct QueryableTableSummaryValue<'a> {
    schema_name: &'a str,
    table_name: &'a str,
    name: String,
    sql_reference: String,
    description: &'a str,
    guide: &'a str,
    required_filters: &'a [String],
}

impl<'a> From<&'a TableSummary> for QueryableTableSummaryValue<'a> {
    fn from(table: &'a TableSummary) -> Self {
        Self {
            schema_name: &table.schema_name,
            table_name: &table.name,
            name: format!("{}.{}", table.schema_name, table.name),
            sql_reference: format_schema_table_equivalent(&table.schema_name, &table.name),
            description: &table.description,
            guide: &table.guide,
            required_filters: &table.required_filters,
        }
    }
}

#[derive(Serialize)]
struct MissingTableSummaryValue<'a> {
    schema_name: &'a str,
    table_name: &'a str,
    name: String,
    description: &'a str,
    required_filters: &'a [String],
}

impl<'a> From<&'a TableSummary> for MissingTableSummaryValue<'a> {
    fn from(table: &'a TableSummary) -> Self {
        Self {
            schema_name: &table.schema_name,
            table_name: &table.name,
            name: format!("{}.{}", table.schema_name, table.name),
            description: &table.description,
            required_filters: &table.required_filters,
        }
    }
}

pub(crate) fn format_schema_table_equivalent(schema_name: &str, table_name: &str) -> String {
    format_sql_reference(schema_name, table_name)
}
