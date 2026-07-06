use coral_api::v1::TableSummary;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

pub(crate) fn queryable_table_summary_value(table: &TableSummary) -> Value {
    serde_json::to_value(QueryableTableSummaryValue::from(table))
        .expect("queryable table summary value serializes")
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

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct MissingTableSummaryValue<'a> {
    pub(crate) schema_name: &'a str,
    pub(crate) table_name: &'a str,
    pub(crate) name: String,
    pub(crate) description: &'a str,
    pub(crate) required_filters: &'a [String],
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
    format!(
        "{}.{}",
        format_sql_identifier(schema_name),
        format_sql_identifier(table_name)
    )
}

pub(crate) fn format_sql_identifier(identifier: &str) -> String {
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
