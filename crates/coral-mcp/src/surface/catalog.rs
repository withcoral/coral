use coral_api::v1::{
    ColumnSearchResult, DescribeTableResponse, ListCatalogResponse, ListColumnsResponse,
    Table as ProtoTable, TableFunction as ProtoTableFunction,
    TableFunctionArgument as ProtoTableFunctionArgument,
    TableFunctionResultColumn as ProtoTableFunctionResultColumn, TableSummary as ProtoTableSummary,
    catalog_item,
};
use serde::Serialize;
use serde_json::{Map, Value};

use super::values::{
    format_schema_table_equivalent, format_sql_identifier, insert_pagination_fields,
    missing_table_summary_value, paged_collection_value,
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
    serde_json::to_value(FoundTableValue::from(table)).expect("found table value serializes")
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
    let suggested_calls = vec![SuggestedCall {
        tool: "list_catalog",
        arguments: SuggestedCallArguments {
            schema: (!same_schema_tables.is_empty()).then_some(schema),
            kind: Some("table"),
            limit: Some(10),
        },
    }];
    serde_json::to_value(MissingTableValue {
        found: false,
        requested: RequestedTable { schema, table },
        available_schemas,
        same_schema_tables,
        suggestions,
        suggested_calls,
    })
    .expect("missing table value serializes")
}

pub(crate) fn list_catalog_value(response: &ListCatalogResponse) -> Value {
    let pagination = response.pagination.unwrap_or_default();
    let items = response
        .items
        .iter()
        .filter_map(catalog_item_value)
        .collect::<Vec<_>>();
    paged_collection_value("items", items, &pagination)
}

pub(crate) fn catalog_item_value(item: &coral_api::v1::CatalogItem) -> Option<Value> {
    match item.item.as_ref()? {
        catalog_item::Item::Table(table) => {
            serde_json::to_value(CatalogTableItemValue::from(table)).ok()
        }
        catalog_item::Item::TableFunction(function) => {
            serde_json::to_value(CatalogTableFunctionItemValue::from(function)).ok()
        }
    }
}

fn minimal_table_function_call_example(function: &ProtoTableFunction) -> String {
    let reference = format_schema_table_equivalent(&function.schema_name, &function.name);
    let required_arguments = function
        .arguments
        .iter()
        .filter(|argument| argument.required)
        .map(|argument| format!("{} => '<value>'", format_sql_identifier(&argument.name)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{reference}({required_arguments})")
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
        ("schema_name".to_string(), Value::from(schema)),
        ("table_name".to_string(), Value::from(table)),
        ("columns".to_string(), Value::Array(columns)),
    ]);
    insert_pagination_fields(&mut value, &pagination);
    Value::Object(value)
}

fn column_search_result_value(result: &ColumnSearchResult) -> Option<Value> {
    let column = result.column.as_ref()?;
    let Value::Object(mut value) = serde_json::to_value(ColumnValue::from(column)).ok()? else {
        return None;
    };
    if !result.matched_fields.is_empty() {
        value.insert(
            "matched_fields".to_string(),
            serde_json::to_value(&result.matched_fields).ok()?,
        );
    }
    Some(Value::Object(value))
}

#[derive(Serialize)]
struct FoundTableValue<'a> {
    found: bool,
    schema_name: &'a str,
    table_name: &'a str,
    name: String,
    description: &'a str,
    guide: &'a str,
    required_filters: &'a [String],
    column_count: usize,
    columns_hint: &'static str,
}

impl<'a> From<&'a ProtoTable> for FoundTableValue<'a> {
    fn from(table: &'a ProtoTable) -> Self {
        Self {
            found: true,
            schema_name: &table.schema_name,
            table_name: &table.name,
            name: format!("{}.{}", table.schema_name, table.name),
            description: &table.description,
            guide: &table.guide,
            required_filters: &table.required_filters,
            column_count: table.columns.len(),
            columns_hint: "Use list_columns with this schema/table to inspect columns.",
        }
    }
}

#[derive(Serialize)]
struct MissingTableValue<'a> {
    found: bool,
    requested: RequestedTable<'a>,
    available_schemas: &'a [String],
    same_schema_tables: Vec<Value>,
    suggestions: Vec<Value>,
    suggested_calls: Vec<SuggestedCall<'a>>,
}

#[derive(Serialize)]
struct RequestedTable<'a> {
    schema: &'a str,
    table: &'a str,
}

#[derive(Serialize)]
struct SuggestedCall<'a> {
    tool: &'static str,
    arguments: SuggestedCallArguments<'a>,
}

#[derive(Serialize)]
struct SuggestedCallArguments<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
}

#[derive(Serialize)]
struct CatalogTableItemValue<'a> {
    kind: &'static str,
    schema_name: &'a str,
    name: String,
    sql_reference: String,
    description: &'a str,
    table: CatalogTableValue<'a>,
}

impl<'a> From<&'a ProtoTableSummary> for CatalogTableItemValue<'a> {
    fn from(table: &'a ProtoTableSummary) -> Self {
        Self {
            kind: "table",
            schema_name: &table.schema_name,
            name: format!("{}.{}", table.schema_name, table.name),
            sql_reference: format_schema_table_equivalent(&table.schema_name, &table.name),
            description: &table.description,
            table: CatalogTableValue {
                table_name: &table.name,
                guide: &table.guide,
                required_filters: &table.required_filters,
            },
        }
    }
}

#[derive(Serialize)]
struct CatalogTableValue<'a> {
    table_name: &'a str,
    guide: &'a str,
    required_filters: &'a [String],
}

#[derive(Serialize)]
struct CatalogTableFunctionItemValue<'a> {
    kind: &'static str,
    schema_name: &'a str,
    name: String,
    sql_reference: String,
    sql_call_example: String,
    description: &'a str,
    table_function: CatalogTableFunctionValue<'a>,
}

impl<'a> From<&'a ProtoTableFunction> for CatalogTableFunctionItemValue<'a> {
    fn from(function: &'a ProtoTableFunction) -> Self {
        Self {
            kind: "table_function",
            schema_name: &function.schema_name,
            name: format!("{}.{}", function.schema_name, function.name),
            sql_reference: format_schema_table_equivalent(&function.schema_name, &function.name),
            sql_call_example: minimal_table_function_call_example(function),
            description: &function.description,
            table_function: CatalogTableFunctionValue {
                function_name: &function.name,
                arguments: function
                    .arguments
                    .iter()
                    .map(TableFunctionArgumentValue::from)
                    .collect(),
                result_columns: function
                    .result_columns
                    .iter()
                    .map(TableFunctionResultColumnValue::from)
                    .collect(),
            },
        }
    }
}

#[derive(Serialize)]
struct CatalogTableFunctionValue<'a> {
    function_name: &'a str,
    arguments: Vec<TableFunctionArgumentValue<'a>>,
    result_columns: Vec<TableFunctionResultColumnValue<'a>>,
}

#[derive(Serialize)]
struct TableFunctionArgumentValue<'a> {
    name: &'a str,
    required: bool,
    values: &'a [String],
}

impl<'a> From<&'a ProtoTableFunctionArgument> for TableFunctionArgumentValue<'a> {
    fn from(argument: &'a ProtoTableFunctionArgument) -> Self {
        Self {
            name: &argument.name,
            required: argument.required,
            values: &argument.values,
        }
    }
}

#[derive(Serialize)]
struct TableFunctionResultColumnValue<'a> {
    column_name: &'a str,
    data_type: &'a str,
    is_nullable: bool,
    description: &'a str,
}

impl<'a> From<&'a ProtoTableFunctionResultColumn> for TableFunctionResultColumnValue<'a> {
    fn from(column: &'a ProtoTableFunctionResultColumn) -> Self {
        Self {
            column_name: &column.name,
            data_type: &column.data_type,
            is_nullable: column.nullable,
            description: &column.description,
        }
    }
}

#[derive(Serialize)]
struct ColumnValue<'a> {
    column_name: &'a str,
    data_type: &'a str,
    is_nullable: bool,
    is_virtual: bool,
    is_required_filter: bool,
    description: &'a str,
    ordinal_position: u32,
}

impl<'a> From<&'a coral_api::v1::Column> for ColumnValue<'a> {
    fn from(column: &'a coral_api::v1::Column) -> Self {
        Self {
            column_name: &column.name,
            data_type: &column.data_type,
            is_nullable: column.nullable,
            is_virtual: column.is_virtual,
            is_required_filter: column.is_required_filter,
            description: &column.description,
            ordinal_position: column.ordinal_position,
        }
    }
}
