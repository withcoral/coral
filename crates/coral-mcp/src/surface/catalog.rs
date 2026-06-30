use coral_api::v1::{
    ColumnSearchResult, DescribeTableResponse, ListCatalogResponse, ListColumnsResponse,
    PaginationResponse, SearchCatalogResponse, Table as ProtoTable,
    TableFunction as ProtoTableFunction, TableFunctionArgument as ProtoTableFunctionArgument,
    TableFunctionResultColumn as ProtoTableFunctionResultColumn, TableSummary as ProtoTableSummary,
    catalog_item,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;

use super::schema::tool_output_schema;
use super::values::{
    MissingTableSummaryValue, format_schema_table_equivalent, format_sql_identifier,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CatalogToolKind {
    Table,
    TableFunction,
}

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
        .map(MissingTableSummaryValue::from)
        .collect::<Vec<_>>();
    let suggestions = suggestions
        .iter()
        .map(MissingTableSummaryValue::from)
        .collect::<Vec<_>>();
    let escaped_table = regex::escape(table);
    let search_arguments = if same_schema_tables.is_empty() {
        SuggestedCallArguments {
            pattern: Some(escaped_table),
            schema: None,
            kind: Some(CatalogToolKind::Table),
            limit: None,
        }
    } else {
        SuggestedCallArguments {
            pattern: Some(escaped_table),
            schema: Some(schema),
            kind: Some(CatalogToolKind::Table),
            limit: None,
        }
    };
    let mut suggested_calls = vec![SuggestedCall {
        tool: SuggestedCallTool::SearchCatalog,
        arguments: search_arguments,
    }];
    if !same_schema_tables.is_empty() {
        suggested_calls.push(SuggestedCall {
            tool: SuggestedCallTool::ListCatalog,
            arguments: SuggestedCallArguments {
                pattern: None,
                schema: Some(schema),
                kind: Some(CatalogToolKind::Table),
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

pub(crate) fn describe_table_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<DescribeTableOutputValue<'static>>()
}

pub(crate) fn search_catalog_value(response: &SearchCatalogResponse) -> Value {
    let pagination = response.pagination.unwrap_or_default();
    let items = response
        .items
        .iter()
        .filter_map(catalog_search_result_value)
        .collect::<Vec<_>>();
    serde_json::to_value(CatalogSearchPageValue::new(items, &pagination))
        .expect("catalog search page value serializes")
}

pub(crate) fn search_catalog_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<CatalogSearchPageValue<'static>>()
}

pub(crate) fn list_catalog_value(response: &ListCatalogResponse) -> Value {
    let pagination = response.pagination.unwrap_or_default();
    let items = response
        .items
        .iter()
        .filter_map(catalog_item_value)
        .collect::<Vec<_>>();
    serde_json::to_value(CatalogPageValue::new(items, &pagination))
        .expect("catalog page value serializes")
}

pub(crate) fn list_catalog_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<CatalogPageValue<'static>>()
}

fn catalog_item_value(item: &coral_api::v1::CatalogItem) -> Option<CatalogItemValue<'_>> {
    match item.item.as_ref()? {
        catalog_item::Item::Table(table) => Some(CatalogItemValue::Table(table.into())),
        catalog_item::Item::TableFunction(function) => {
            Some(CatalogItemValue::TableFunction(function.into()))
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

fn catalog_search_result_value(
    result: &coral_api::v1::CatalogSearchResult,
) -> Option<CatalogSearchItemValue<'_>> {
    match result.item.as_ref()?.item.as_ref()? {
        catalog_item::Item::Table(table) => {
            Some(CatalogSearchItemValue::Table(CatalogTableSearchItemValue {
                item: table.into(),
                matched_fields: &result.matched_fields,
            }))
        }
        catalog_item::Item::TableFunction(function) => Some(CatalogSearchItemValue::TableFunction(
            CatalogTableFunctionSearchItemValue {
                item: function.into(),
                matched_fields: &result.matched_fields,
            },
        )),
    }
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
    serde_json::to_value(ListColumnsPageValue::new(
        schema,
        table,
        columns,
        &pagination,
    ))
    .expect("list columns page value serializes")
}

pub(crate) fn list_columns_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<ListColumnsOutputValue<'static>>()
}

fn column_search_result_value(result: &ColumnSearchResult) -> Option<ColumnSearchValue<'_>> {
    let column = result.column.as_ref()?;
    Some(ColumnSearchValue {
        column_name: &column.name,
        data_type: &column.data_type,
        is_nullable: column.nullable,
        is_virtual: column.is_virtual,
        is_required_filter: column.is_required_filter,
        description: &column.description,
        ordinal_position: column.ordinal_position,
        matched_fields: (!result.matched_fields.is_empty())
            .then_some(result.matched_fields.as_slice()),
    })
}

#[derive(JsonSchema)]
#[serde(untagged)]
#[schemars(extend("type" = "object"))]
#[expect(
    dead_code,
    reason = "schema-only enum for the describe_table output contract"
)]
enum DescribeTableOutputValue<'a> {
    Found(FoundTableValue<'a>),
    Missing(MissingTableValue<'a>),
}

#[derive(JsonSchema)]
#[serde(untagged)]
#[schemars(extend("type" = "object"))]
#[expect(
    dead_code,
    reason = "schema-only enum for the list_columns output contract"
)]
enum ListColumnsOutputValue<'a> {
    Page(ListColumnsPageValue<'a>),
    Missing(MissingTableValue<'a>),
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogPageValue<'a> {
    items: Vec<CatalogItemValue<'a>>,
    #[schemars(range(min = 0))]
    total: u32,
    #[schemars(range(min = 1))]
    limit: u32,
    #[schemars(range(min = 0))]
    offset: u32,
    has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<u32>,
}

impl<'a> CatalogPageValue<'a> {
    fn new(items: Vec<CatalogItemValue<'a>>, pagination: &PaginationResponse) -> Self {
        Self {
            items,
            total: pagination.total_count,
            limit: pagination.limit,
            offset: pagination.offset,
            has_more: pagination.has_more,
            next_offset: pagination.has_more.then_some(pagination.next_offset),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogSearchPageValue<'a> {
    items: Vec<CatalogSearchItemValue<'a>>,
    #[schemars(range(min = 0))]
    total: u32,
    #[schemars(range(min = 1))]
    limit: u32,
    #[schemars(range(min = 0))]
    offset: u32,
    has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<u32>,
}

impl<'a> CatalogSearchPageValue<'a> {
    fn new(items: Vec<CatalogSearchItemValue<'a>>, pagination: &PaginationResponse) -> Self {
        Self {
            items,
            total: pagination.total_count,
            limit: pagination.limit,
            offset: pagination.offset,
            has_more: pagination.has_more,
            next_offset: pagination.has_more.then_some(pagination.next_offset),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct ListColumnsPageValue<'a> {
    schema_name: &'a str,
    table_name: &'a str,
    columns: Vec<ColumnSearchValue<'a>>,
    #[schemars(range(min = 0))]
    total: u32,
    #[schemars(range(min = 1))]
    limit: u32,
    #[schemars(range(min = 0))]
    offset: u32,
    has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<u32>,
}

impl<'a> ListColumnsPageValue<'a> {
    fn new(
        schema_name: &'a str,
        table_name: &'a str,
        columns: Vec<ColumnSearchValue<'a>>,
        pagination: &PaginationResponse,
    ) -> Self {
        Self {
            schema_name,
            table_name,
            columns,
            total: pagination.total_count,
            limit: pagination.limit,
            offset: pagination.offset,
            has_more: pagination.has_more,
            next_offset: pagination.has_more.then_some(pagination.next_offset),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[serde(untagged)]
enum CatalogItemValue<'a> {
    Table(CatalogTableItemValue<'a>),
    TableFunction(CatalogTableFunctionItemValue<'a>),
}

#[derive(Serialize, JsonSchema)]
#[serde(untagged)]
enum CatalogSearchItemValue<'a> {
    Table(CatalogTableSearchItemValue<'a>),
    TableFunction(CatalogTableFunctionSearchItemValue<'a>),
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogTableSearchItemValue<'a> {
    #[serde(flatten)]
    item: CatalogTableItemValue<'a>,
    matched_fields: &'a [String],
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogTableFunctionSearchItemValue<'a> {
    #[serde(flatten)]
    item: CatalogTableFunctionItemValue<'a>,
    matched_fields: &'a [String],
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
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

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct MissingTableValue<'a> {
    found: bool,
    requested: RequestedTable<'a>,
    available_schemas: &'a [String],
    same_schema_tables: Vec<MissingTableSummaryValue<'a>>,
    suggestions: Vec<MissingTableSummaryValue<'a>>,
    suggested_calls: Vec<SuggestedCall<'a>>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct RequestedTable<'a> {
    schema: &'a str,
    table: &'a str,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct SuggestedCall<'a> {
    tool: SuggestedCallTool,
    arguments: SuggestedCallArguments<'a>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SuggestedCallTool {
    SearchCatalog,
    ListCatalog,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct SuggestedCallArguments<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<CatalogToolKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogTableItemValue<'a> {
    kind: CatalogTableKind,
    schema_name: &'a str,
    name: String,
    sql_reference: String,
    description: &'a str,
    table: CatalogTableValue<'a>,
}

impl<'a> From<&'a ProtoTableSummary> for CatalogTableItemValue<'a> {
    fn from(table: &'a ProtoTableSummary) -> Self {
        Self {
            kind: CatalogTableKind::Table,
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

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum CatalogTableKind {
    Table,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogTableValue<'a> {
    table_name: &'a str,
    guide: &'a str,
    required_filters: &'a [String],
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogTableFunctionItemValue<'a> {
    kind: CatalogTableFunctionKind,
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
            kind: CatalogTableFunctionKind::TableFunction,
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

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum CatalogTableFunctionKind {
    TableFunction,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct CatalogTableFunctionValue<'a> {
    function_name: &'a str,
    arguments: Vec<TableFunctionArgumentValue<'a>>,
    result_columns: Vec<TableFunctionResultColumnValue<'a>>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
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

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
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

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct ColumnSearchValue<'a> {
    column_name: &'a str,
    data_type: &'a str,
    is_nullable: bool,
    is_virtual: bool,
    is_required_filter: bool,
    description: &'a str,
    #[schemars(range(min = 0))]
    ordinal_position: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_fields: Option<&'a [String]>,
}
