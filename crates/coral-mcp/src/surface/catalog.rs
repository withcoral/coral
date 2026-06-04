use coral_api::v1::{
    ColumnSearchResult, DescribeTableResponse, ListCatalogResponse, ListColumnsResponse,
    SearchCatalogResponse, Table as ProtoTable, TableFunction as ProtoTableFunction,
    TableFunctionArgument as ProtoTableFunctionArgument,
    TableFunctionResultColumn as ProtoTableFunctionResultColumn, TableSummary as ProtoTableSummary,
    catalog_item,
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::fmt::Write as _;

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
    let escaped_table = regex::escape(table);
    let search_arguments = if same_schema_tables.is_empty() {
        SuggestedCallArguments {
            pattern: Some(escaped_table),
            schema: None,
            kind: Some("table"),
            limit: None,
        }
    } else {
        SuggestedCallArguments {
            pattern: Some(escaped_table),
            schema: Some(schema),
            kind: Some("table"),
            limit: None,
        }
    };
    let mut suggested_calls = vec![SuggestedCall {
        tool: "search_catalog",
        arguments: search_arguments,
    }];
    if !same_schema_tables.is_empty() {
        suggested_calls.push(SuggestedCall {
            tool: "list_catalog",
            arguments: SuggestedCallArguments {
                pattern: None,
                schema: Some(schema),
                kind: Some("table"),
                limit: Some(10),
            },
        });
    }
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

pub(crate) fn search_catalog_value(response: &SearchCatalogResponse) -> Value {
    let pagination = response.pagination.unwrap_or_default();
    let items = response
        .items
        .iter()
        .filter_map(catalog_search_result_value)
        .collect::<Vec<_>>();
    paged_collection_value("items", items, &pagination)
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

fn catalog_item_value(item: &coral_api::v1::CatalogItem) -> Option<Value> {
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

fn catalog_search_result_value(result: &coral_api::v1::CatalogSearchResult) -> Option<Value> {
    let mut value = catalog_item_value(result.item.as_ref()?)?;
    value.as_object_mut()?.insert(
        "matched_fields".to_string(),
        serde_json::to_value(&result.matched_fields).ok()?,
    );
    Some(value)
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

pub(crate) fn catalog_items_text(value: &Value) -> Option<String> {
    let items = value.get("items")?.as_array()?;
    let mut text = collection_header("items", items.len(), value);
    text.push('\n');
    text.push_str("kind\tname\tsql_reference\tdescription\tdetails\tmatched_fields");
    for item in items {
        let item = item.as_object()?;
        text.push('\n');
        write!(
            text,
            "{}\t{}\t{}\t{}\t{}\t{}",
            field_text(item, "kind"),
            field_text(item, "name"),
            field_text(item, "sql_reference"),
            field_text(item, "description"),
            catalog_item_details(item),
            field_text(item, "matched_fields")
        )
        .expect("writing to String cannot fail");
    }
    Some(text)
}

pub(crate) fn columns_text(value: &Value) -> Option<String> {
    let columns = value.get("columns")?.as_array()?;
    let schema_name = value.get("schema_name").and_then(Value::as_str)?;
    let table_name = value.get("table_name").and_then(Value::as_str)?;
    let mut text = format!(
        "table={}.{} {}",
        sanitize_text(schema_name),
        sanitize_text(table_name),
        collection_header("columns", columns.len(), value)
    );
    text.push('\n');
    text.push_str(
        "ordinal\tcolumn_name\tdata_type\tis_nullable\tis_virtual\tis_required_filter\tdescription\tmatched_fields",
    );
    for column in columns {
        let column = column.as_object()?;
        text.push('\n');
        write!(
            text,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            field_text(column, "ordinal_position"),
            field_text(column, "column_name"),
            field_text(column, "data_type"),
            field_text(column, "is_nullable"),
            field_text(column, "is_virtual"),
            field_text(column, "is_required_filter"),
            field_text(column, "description"),
            field_text(column, "matched_fields")
        )
        .expect("writing to String cannot fail");
    }
    Some(text)
}

fn collection_header(collection_name: &str, visible_count: usize, value: &Value) -> String {
    let mut text = format!("{collection_name}={visible_count}");
    for key in ["total", "limit", "offset", "has_more", "next_offset"] {
        if let Some(value) = value.get(key) {
            write!(text, " {key}={}", compact_value(value)).expect("writing to String cannot fail");
        }
    }
    text
}

fn catalog_item_details(item: &Map<String, Value>) -> String {
    if let Some(table) = item.get("table").and_then(Value::as_object) {
        let mut details = format!("table_name={}", field_text(table, "table_name"));
        let required_filters = field_text(table, "required_filters");
        if !required_filters.is_empty() {
            write!(details, ";required_filters=[{required_filters}]")
                .expect("writing to String cannot fail");
        }
        let guide = field_text(table, "guide");
        if !guide.is_empty() {
            write!(details, ";guide={guide}").expect("writing to String cannot fail");
        }
        return details;
    }

    if let Some(function) = item.get("table_function").and_then(Value::as_object) {
        let mut details = format!("function_name={}", field_text(function, "function_name"));
        let call = field_text(item, "sql_call_example");
        if !call.is_empty() {
            write!(details, ";call={call}").expect("writing to String cannot fail");
        }
        let arguments = compact_table_function_arguments(function.get("arguments"));
        if !arguments.is_empty() {
            write!(details, ";args={arguments}").expect("writing to String cannot fail");
        }
        let result_columns = compact_table_function_result_columns(function.get("result_columns"));
        if !result_columns.is_empty() {
            write!(details, ";result_columns={result_columns}")
                .expect("writing to String cannot fail");
        }
        return details;
    }

    String::new()
}

fn compact_table_function_arguments(arguments: Option<&Value>) -> String {
    let Some(arguments) = arguments.and_then(Value::as_array) else {
        return String::new();
    };
    arguments
        .iter()
        .filter_map(Value::as_object)
        .map(|argument| {
            let name = field_text(argument, "name");
            let required = if argument
                .get("required")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "!"
            } else {
                ""
            };
            let values = field_text(argument, "values");
            if values.is_empty() {
                format!("{name}{required}")
            } else {
                format!("{name}{required}=[{values}]")
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn compact_table_function_result_columns(columns: Option<&Value>) -> String {
    let Some(columns) = columns.and_then(Value::as_array) else {
        return String::new();
    };
    columns
        .iter()
        .filter_map(Value::as_object)
        .map(|column| {
            let nullable = if column
                .get("is_nullable")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "?"
            } else {
                ""
            };
            let mut text = format!(
                "{}:{}{}",
                field_text(column, "column_name"),
                field_text(column, "data_type"),
                nullable
            );
            let description = field_text(column, "description");
            if !description.is_empty() {
                write!(text, " {description}").expect("writing to String cannot fail");
            }
            text
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn field_text(object: &Map<String, Value>, key: &str) -> String {
    object.get(key).map(compact_value).unwrap_or_default()
}

fn compact_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => sanitize_text(value),
        Value::Array(values) => values
            .iter()
            .map(compact_value)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => sanitize_text(&value.to_string()),
    }
}

fn sanitize_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
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
    pattern: Option<String>,
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{catalog_items_text, columns_text};

    #[test]
    fn catalog_items_text_renders_table_and_function_rows() {
        let value = json!({
            "items": [
                {
                    "kind": "table",
                    "name": "github.pulls",
                    "sql_reference": "github.pulls",
                    "description": "Pull requests",
                    "matched_fields": ["description", "table.name"],
                    "table": {
                        "table_name": "pulls",
                        "guide": "Use owner\nand repo filters.",
                        "required_filters": ["owner", "repo"]
                    }
                },
                {
                    "kind": "table_function",
                    "name": "datadog.logs",
                    "sql_reference": "datadog.logs",
                    "sql_call_example": "datadog.logs(start => '<value>')",
                    "description": "Datadog logs",
                    "table_function": {
                        "function_name": "logs",
                        "arguments": [
                            {"name": "start", "required": true, "values": []},
                            {"name": "env", "required": false, "values": ["prod", "dev"]}
                        ],
                        "result_columns": [
                            {
                                "column_name": "message",
                                "data_type": "Utf8",
                                "is_nullable": false,
                                "description": "Log message"
                            },
                            {
                                "column_name": "host",
                                "data_type": "Utf8",
                                "is_nullable": true,
                                "description": ""
                            }
                        ]
                    }
                }
            ],
            "total": 2,
            "limit": 20,
            "offset": 0,
            "has_more": false
        });

        let text = catalog_items_text(&value).expect("catalog text");

        assert_eq!(
            text,
            concat!(
                "items=2 total=2 limit=20 offset=0 has_more=false\n",
                "kind\tname\tsql_reference\tdescription\tdetails\tmatched_fields\n",
                "table\tgithub.pulls\tgithub.pulls\tPull requests\t",
                "table_name=pulls;required_filters=[owner,repo];guide=Use owner and repo filters.\t",
                "description,table.name\n",
                "table_function\tdatadog.logs\tdatadog.logs\tDatadog logs\t",
                "function_name=logs;call=datadog.logs(start => '<value>');",
                "args=start!,env=[prod,dev];result_columns=message:Utf8 Log message;host:Utf8?\t"
            )
        );
    }

    #[test]
    fn columns_text_renders_column_flags() {
        let value = json!({
            "schema_name": "github",
            "table_name": "pulls",
            "columns": [
                {
                    "column_name": "number",
                    "data_type": "Int64",
                    "is_nullable": false,
                    "is_virtual": false,
                    "is_required_filter": false,
                    "description": "Pull request number",
                    "ordinal_position": 1
                },
                {
                    "column_name": "owner",
                    "data_type": "Utf8",
                    "is_nullable": false,
                    "is_virtual": true,
                    "is_required_filter": true,
                    "description": "Repository owner",
                    "ordinal_position": 2,
                    "matched_fields": ["name", "description"]
                }
            ],
            "total": 2,
            "limit": 50,
            "offset": 0,
            "has_more": false
        });

        let text = columns_text(&value).expect("columns text");

        assert_eq!(
            text,
            concat!(
                "table=github.pulls columns=2 total=2 limit=50 offset=0 has_more=false\n",
                "ordinal\tcolumn_name\tdata_type\tis_nullable\tis_virtual\t",
                "is_required_filter\tdescription\tmatched_fields\n",
                "1\tnumber\tInt64\tfalse\tfalse\tfalse\tPull request number\t\n",
                "2\towner\tUtf8\tfalse\ttrue\ttrue\tRepository owner\tname,description"
            )
        );
    }
}
