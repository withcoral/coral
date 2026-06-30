use coral_api::v1::{
    ColumnSearchResult, DescribeTableResponse, ListCatalogResponse, ListColumnsResponse,
    PaginationResponse, SearchCatalogResponse, Table as ProtoTable,
    TableFunction as ProtoTableFunction, TableFunctionArgument as ProtoTableFunctionArgument,
    TableFunctionResultColumn as ProtoTableFunctionResultColumn, TableSummary as ProtoTableSummary,
    catalog_item,
};
use rmcp::{
    ErrorData,
    model::{Tool, ToolAnnotations},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;

use super::arguments::{
    optional_bool_argument, optional_non_empty_string_argument, optional_string_argument,
    required_string_argument,
};
use super::context::ToolDescriptionContext;
use super::discovery::{
    DefaultPaginationInput, Pagination, SearchPaginationInput, parse_pagination,
    parse_search_pagination,
};
use super::schema::{tool_input_schema, tool_output_schema};
use super::tool_names::ToolName;
use super::values::{
    MissingTableSummaryValue, format_schema_table_equivalent, format_sql_identifier,
};

const DEFAULT_IGNORE_CASE: bool = true;
const DEFAULT_REQUIRED_ONLY: bool = false;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CatalogToolKind {
    Table,
    TableFunction,
}

#[derive(JsonSchema)]
pub(crate) struct ListCatalogArguments {
    #[schemars(description = "Optional exact SQL schema name to list.")]
    pub(crate) schema: Option<String>,
    #[schemars(
        description = "Optional item kind to list. Omit or pass null to list all catalog items."
    )]
    pub(crate) kind: Option<CatalogToolKind>,
    #[schemars(flatten, with = "DefaultPaginationInput")]
    pub(crate) pagination: Pagination,
}

#[derive(JsonSchema)]
pub(crate) struct SearchCatalogArguments {
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Rust regex pattern to match database catalog metadata."
    )]
    pub(crate) pattern: String,
    #[schemars(description = "Optional exact SQL schema name to search.")]
    pub(crate) schema: Option<String>,
    #[schemars(
        description = "Optional item kind to search. Omit or pass null to search all catalog items."
    )]
    pub(crate) kind: Option<CatalogToolKind>,
    #[serde(default = "default_ignore_case")]
    #[schemars(description = "Whether regex matching is case-insensitive. Defaults to true.")]
    pub(crate) ignore_case: bool,
    #[schemars(flatten, with = "SearchPaginationInput")]
    pub(crate) pagination: Pagination,
}

#[derive(JsonSchema)]
pub(crate) struct DescribeTableArguments {
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Exact SQL schema name."
    )]
    pub(crate) schema: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Exact table name within the SQL schema."
    )]
    pub(crate) table: String,
}

#[derive(JsonSchema)]
pub(crate) struct ListColumnsArguments {
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Exact SQL schema name."
    )]
    pub(crate) schema: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Exact table name within the SQL schema."
    )]
    pub(crate) table: String,
    #[schemars(
        length(min = 1),
        pattern(r"\S"),
        description = "Optional Rust regex matched against column names, descriptions, and data types."
    )]
    pub(crate) pattern: Option<String>,
    #[serde(default = "default_ignore_case")]
    #[schemars(description = "Whether regex matching is case-insensitive. Defaults to true.")]
    pub(crate) ignore_case: bool,
    #[serde(default = "default_required_only")]
    #[schemars(description = "Only return columns that are required filters. Defaults to false.")]
    pub(crate) required_only: bool,
    #[schemars(flatten, with = "DefaultPaginationInput")]
    pub(crate) pagination: Pagination,
}

pub(crate) fn list_catalog_tool(context: &ToolDescriptionContext) -> Tool {
    Tool::new(
        ToolName::ListCatalog.as_str(),
        format!(
            "List database catalog items for Coral sources. {} {} table(s) and {} table function(s) are currently visible.",
            context.connected_sources_sentence(),
            context.visible_table_count,
            context.visible_function_count
        ),
        tool_input_schema::<ListCatalogArguments>(),
    )
    .with_raw_output_schema(list_catalog_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("List Catalog")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn search_catalog_tool(context: &ToolDescriptionContext) -> Tool {
    Tool::new(
        ToolName::SearchCatalog.as_str(),
        search_catalog_description(context),
        tool_input_schema::<SearchCatalogArguments>(),
    )
    .with_raw_output_schema(search_catalog_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Search Catalog")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn describe_table_tool() -> Tool {
    Tool::new(
        ToolName::DescribeTable.as_str(),
        "Describe one database table without returning full column definitions.",
        tool_input_schema::<DescribeTableArguments>(),
    )
    .with_raw_output_schema(describe_table_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Describe Table")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn list_columns_tool() -> Tool {
    Tool::new(
        ToolName::ListColumns.as_str(),
        "List columns for one database table with optional regex and required-filter narrowing.",
        tool_input_schema::<ListColumnsArguments>(),
    )
    .with_raw_output_schema(list_columns_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("List Columns")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn list_catalog_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListCatalogArguments, ErrorData> {
    Ok(ListCatalogArguments {
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        pagination: parse_pagination(arguments)?,
    })
}

pub(crate) fn search_catalog_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SearchCatalogArguments, ErrorData> {
    Ok(SearchCatalogArguments {
        pattern: required_string_argument(arguments, "pattern")?,
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        ignore_case: optional_bool_argument(arguments, "ignore_case", DEFAULT_IGNORE_CASE)?,
        pagination: parse_search_pagination(arguments)?,
    })
}

pub(crate) fn describe_table_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<DescribeTableArguments, ErrorData> {
    Ok(DescribeTableArguments {
        schema: required_string_argument(arguments, "schema")?,
        table: required_string_argument(arguments, "table")?,
    })
}

pub(crate) fn list_columns_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListColumnsArguments, ErrorData> {
    Ok(ListColumnsArguments {
        schema: required_string_argument(arguments, "schema")?,
        table: required_string_argument(arguments, "table")?,
        pattern: optional_non_empty_string_argument(arguments, "pattern")?,
        ignore_case: optional_bool_argument(arguments, "ignore_case", DEFAULT_IGNORE_CASE)?,
        required_only: optional_bool_argument(arguments, "required_only", DEFAULT_REQUIRED_ONLY)?,
        pagination: parse_pagination(arguments)?,
    })
}

fn optional_catalog_kind_argument(
    arguments: Option<&Map<String, Value>>,
) -> Result<Option<CatalogToolKind>, ErrorData> {
    let Some(kind) = optional_string_argument(arguments, "kind")? else {
        return Ok(None);
    };
    serde_json::from_value(Value::String(kind))
        .map(Some)
        .map_err(|error| {
            ErrorData::invalid_params(format!("invalid argument 'kind': {error}"), None)
        })
}

fn search_catalog_description(context: &ToolDescriptionContext) -> String {
    format!(
        "Search database catalog metadata with a Rust regex across connected Coral sources/schemas. {} {} table(s) and {} table function(s) are currently visible.",
        context.connected_sources_sentence(),
        context.visible_table_count,
        context.visible_function_count
    )
}

fn default_ignore_case() -> bool {
    DEFAULT_IGNORE_CASE
}

fn default_required_only() -> bool {
    DEFAULT_REQUIRED_ONLY
}

pub(crate) fn describe_table_value(
    schema: &str,
    table: &str,
    response: &DescribeTableResponse,
) -> Value {
    serde_json::to_value(describe_table_output(schema, table, response))
        .expect("describe table output value serializes")
}

fn describe_table_output<'a>(
    schema: &'a str,
    table: &'a str,
    response: &'a DescribeTableResponse,
) -> DescribeTableOutput<'a> {
    if let Some(table) = &response.table {
        return DescribeTableOutput::Found(FoundTableValue::from(table));
    }
    DescribeTableOutput::Missing(missing_table_value(
        schema,
        table,
        &response.available_schemas,
        &response.same_schema_tables,
        &response.suggestions,
    ))
}

pub(crate) fn list_columns_table_fallback_value(
    schema: &str,
    table: &str,
    response: &DescribeTableResponse,
) -> Value {
    serde_json::to_value(list_columns_table_fallback_output(schema, table, response))
        .expect("list columns table fallback output value serializes")
}

fn list_columns_table_fallback_output<'a>(
    schema: &'a str,
    table: &'a str,
    response: &'a DescribeTableResponse,
) -> ListColumnsOutput<'a> {
    if let Some(table) = &response.table {
        return ListColumnsOutput::Found(FoundTableValue::from(table));
    }
    ListColumnsOutput::Missing(missing_table_value(
        schema,
        table,
        &response.available_schemas,
        &response.same_schema_tables,
        &response.suggestions,
    ))
}

fn missing_table_value<'a>(
    schema: &'a str,
    table: &'a str,
    available_schemas: &'a [String],
    same_schema_tables: &'a [ProtoTableSummary],
    suggestions: &'a [ProtoTableSummary],
) -> MissingTableValue<'a> {
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
        tool: CatalogSuggestedTool::SearchCatalog,
        arguments: search_arguments,
    }];
    if !same_schema_tables.is_empty() {
        suggested_calls.push(SuggestedCall {
            tool: CatalogSuggestedTool::ListCatalog,
            arguments: SuggestedCallArguments {
                pattern: None,
                schema: Some(schema),
                kind: Some(CatalogToolKind::Table),
                limit: Some(10),
            },
        });
    }
    MissingTableValue {
        found: false,
        requested: RequestedTable { schema, table },
        available_schemas,
        same_schema_tables,
        suggestions,
        suggested_calls,
    }
}

pub(crate) fn describe_table_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<DescribeTableOutput<'static>>()
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
    serde_json::to_value(ListColumnsOutput::Page(ListColumnsPageValue::new(
        schema,
        table,
        columns,
        &pagination,
    )))
    .expect("list columns page value serializes")
}

pub(crate) fn list_columns_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<ListColumnsOutput<'static>>()
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

#[derive(Serialize, JsonSchema)]
#[serde(untagged)]
#[schemars(extend("type" = "object"))]
enum DescribeTableOutput<'a> {
    Found(FoundTableValue<'a>),
    Missing(MissingTableValue<'a>),
}

#[derive(Serialize, JsonSchema)]
#[serde(untagged)]
#[schemars(extend("type" = "object"))]
enum ListColumnsOutput<'a> {
    Page(ListColumnsPageValue<'a>),
    Found(FoundTableValue<'a>),
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
    tool: CatalogSuggestedTool,
    arguments: SuggestedCallArguments<'a>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum CatalogSuggestedTool {
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

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::{
        DEFAULT_IGNORE_CASE, DEFAULT_REQUIRED_ONLY, list_catalog_arguments, list_columns_arguments,
        search_catalog_arguments,
    };
    use crate::surface::discovery::{
        DEFAULT_PAGINATION_LIMIT, DEFAULT_PAGINATION_OFFSET, DEFAULT_SEARCH_PAGINATION_LIMIT,
    };

    #[test]
    fn catalog_kind_argument_accepts_null_as_all_kinds() {
        let mut arguments = Map::new();
        arguments.insert("kind".to_string(), Value::Null);
        let list = list_catalog_arguments(Some(&arguments)).expect("list arguments");
        assert_eq!(list.kind, None);

        arguments.insert("pattern".to_string(), Value::String("issue".to_string()));
        let search = search_catalog_arguments(Some(&arguments)).expect("search arguments");
        assert_eq!(search.kind, None);
    }

    #[test]
    fn list_columns_pattern_accepts_null_as_omitted() {
        let arguments = Map::from_iter([
            ("schema".to_string(), Value::String("github".to_string())),
            ("table".to_string(), Value::String("issues".to_string())),
            ("pattern".to_string(), Value::Null),
        ]);

        let parsed = list_columns_arguments(Some(&arguments)).expect("list columns args");

        assert_eq!(parsed.pattern, None);
    }

    #[test]
    fn catalog_argument_defaults_use_shared_constants() {
        let search_arguments =
            Map::from_iter([("pattern".to_string(), Value::String("issue".to_string()))]);

        let search = search_catalog_arguments(Some(&search_arguments)).expect("search args");

        assert_eq!(search.ignore_case, DEFAULT_IGNORE_CASE);
        assert_eq!(search.pagination.limit, DEFAULT_SEARCH_PAGINATION_LIMIT);
        assert_eq!(search.pagination.offset, DEFAULT_PAGINATION_OFFSET);

        let list_columns_arguments = Map::from_iter([
            ("schema".to_string(), Value::String("github".to_string())),
            ("table".to_string(), Value::String("issues".to_string())),
        ]);

        let list_columns = super::list_columns_arguments(Some(&list_columns_arguments))
            .expect("list columns args");

        assert_eq!(list_columns.ignore_case, DEFAULT_IGNORE_CASE);
        assert_eq!(list_columns.required_only, DEFAULT_REQUIRED_ONLY);
        assert_eq!(list_columns.pagination.limit, DEFAULT_PAGINATION_LIMIT);
        assert_eq!(list_columns.pagination.offset, DEFAULT_PAGINATION_OFFSET);
    }
}
