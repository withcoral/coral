use coral_api::v1::TableSummary;
pub(crate) use coral_client::format_schema_table_equivalent;
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
    require_guide_read: bool,
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
            require_guide_read: table.require_guide_read,
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
