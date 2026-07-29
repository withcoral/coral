use coral_api::v1::{
    ColumnSearchResult, DescribeTableResponse, ListCatalogResponse, ListColumnsResponse,
    PaginationResponse, Table as ProtoTable, TableFunction as ProtoTableFunction,
    TableFunctionArgument as ProtoTableFunctionArgument,
    TableFunctionResultColumn as ProtoTableFunctionResultColumn, TableSummary as ProtoTableSummary,
    catalog_item,
};
use coral_client::minimal_table_function_call_example;
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
use super::discovery::{DefaultPaginationInput, Pagination, parse_pagination};
use super::schema::{tool_input_schema, tool_output_schema};
use super::tool_names::ToolName;
use super::values::{MissingTableSummaryValue, format_schema_table_equivalent, format_table_name};

const DEFAULT_IGNORE_CASE: bool = true;
const DEFAULT_REQUIRED_ONLY: bool = false;
const LIST_COLUMNS_FIELDS: [&str; 8] = [
    "column_name",
    "data_type",
    "is_nullable",
    "is_virtual",
    "is_required_filter",
    "description",
    "ordinal_position",
    "matched_fields",
];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CatalogToolKind {
    Table,
    TableFunction,
}

#[derive(JsonSchema)]
pub(crate) struct ListCatalogArguments {
    #[schemars(description = "Optional exact SQL catalog name to list.")]
    pub(crate) catalog: Option<String>,
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
pub(crate) struct DescribeTableArguments {
    #[schemars(description = "Optional SQL catalog name. Omit for two-part tables.")]
    pub(crate) catalog: Option<String>,
    #[schemars(length(min = 1), description = "Exact SQL schema name.")]
    pub(crate) schema: String,
    #[schemars(
        length(min = 1),
        description = "Exact table name within the SQL schema."
    )]
    pub(crate) table: String,
}

#[derive(JsonSchema)]
pub(crate) struct ListColumnsArguments {
    #[schemars(description = "Optional SQL catalog name. Omit for two-part tables.")]
    pub(crate) catalog: Option<String>,
    #[schemars(length(min = 1), description = "Exact SQL schema name.")]
    pub(crate) schema: String,
    #[schemars(
        length(min = 1),
        description = "Exact table name within the SQL schema."
    )]
    pub(crate) table: String,
    #[schemars(
        length(min = 1),
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
        "List columns for one database table as positional rows with field names returned once. Supports optional regex and required-filter narrowing.",
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
        catalog: optional_string_argument(arguments, "catalog")?,
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        pagination: parse_pagination(arguments)?,
    })
}

pub(crate) fn describe_table_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<DescribeTableArguments, ErrorData> {
    Ok(DescribeTableArguments {
        catalog: optional_string_argument(arguments, "catalog")?,
        schema: required_string_argument(arguments, "schema")?,
        table: required_string_argument(arguments, "table")?,
    })
}

pub(crate) fn list_columns_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListColumnsArguments, ErrorData> {
    Ok(ListColumnsArguments {
        catalog: optional_string_argument(arguments, "catalog")?,
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

fn default_ignore_case() -> bool {
    DEFAULT_IGNORE_CASE
}

fn default_required_only() -> bool {
    DEFAULT_REQUIRED_ONLY
}

pub(crate) fn describe_table_value(
    catalog: Option<&str>,
    schema: &str,
    table: &str,
    response: &DescribeTableResponse,
) -> Value {
    serde_json::to_value(describe_table_output(catalog, schema, table, response))
        .expect("describe table output value serializes")
}

fn describe_table_output<'a>(
    catalog: Option<&'a str>,
    schema: &'a str,
    table: &'a str,
    response: &'a DescribeTableResponse,
) -> DescribeTableOutput<'a> {
    if let Some(table) = &response.table {
        return DescribeTableOutput::Found(FoundTableValue::from(table));
    }
    DescribeTableOutput::Missing(missing_table_value(
        catalog,
        schema,
        table,
        &response.available_schemas,
        &response.same_schema_tables,
        &response.suggestions,
    ))
}

pub(crate) fn list_columns_table_fallback_value(
    catalog: Option<&str>,
    schema: &str,
    table: &str,
    response: &DescribeTableResponse,
) -> Value {
    serde_json::to_value(list_columns_table_fallback_output(
        catalog, schema, table, response,
    ))
    .expect("list columns table fallback output value serializes")
}

fn list_columns_table_fallback_output<'a>(
    catalog: Option<&'a str>,
    schema: &'a str,
    table: &'a str,
    response: &'a DescribeTableResponse,
) -> ListColumnsOutput<'a> {
    if let Some(table) = &response.table {
        return ListColumnsOutput::Found(FoundTableValue::from(table));
    }
    ListColumnsOutput::Missing(missing_table_value(
        catalog,
        schema,
        table,
        &response.available_schemas,
        &response.same_schema_tables,
        &response.suggestions,
    ))
}

fn missing_table_value<'a>(
    catalog: Option<&'a str>,
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
    let mut suggested_calls = vec![SuggestedCall {
        tool: CatalogSuggestedTool::ListCatalog,
        arguments: SuggestedCallArguments {
            catalog,
            schema: (!same_schema_tables.is_empty()).then_some(schema),
            kind: Some(CatalogToolKind::Table),
            limit: Some(10),
        },
    }];
    if catalog.is_some() && same_schema_tables.is_empty() {
        suggested_calls.push(SuggestedCall {
            tool: CatalogSuggestedTool::ListCatalog,
            arguments: SuggestedCallArguments {
                catalog: None,
                schema: None,
                kind: Some(CatalogToolKind::Table),
                limit: Some(10),
            },
        });
    }
    MissingTableValue {
        found: false,
        requested: RequestedTable {
            catalog,
            schema,
            table,
        },
        available_schemas,
        same_schema_tables,
        suggestions,
        suggested_calls,
    }
}

pub(crate) fn describe_table_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<DescribeTableOutput<'static>>()
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

pub(crate) fn list_columns_value(
    catalog: Option<&str>,
    schema: &str,
    table: &str,
    response: &ListColumnsResponse,
) -> Value {
    let pagination = response.pagination.unwrap_or_default();
    let rows = response
        .columns
        .iter()
        .filter_map(column_search_result_row)
        .collect::<Vec<_>>();
    serde_json::to_value(ListColumnsOutput::Page(ListColumnsPageValue::new(
        catalog,
        schema,
        table,
        rows,
        &pagination,
    )))
    .expect("list columns page value serializes")
}

pub(crate) fn list_columns_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<ListColumnsOutput<'static>>()
}

fn column_search_result_row(result: &ColumnSearchResult) -> Option<ColumnSearchRowValue<'_>> {
    let column = result.column.as_ref()?;
    Some(ColumnSearchRowValue(
        &column.name,
        &column.data_type,
        column.nullable,
        column.is_virtual,
        column.is_required_filter,
        &column.description,
        column.ordinal_position,
        (!result.matched_fields.is_empty()).then_some(result.matched_fields.as_slice()),
    ))
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
struct ListColumnsPageValue<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_name: Option<&'a str>,
    schema_name: &'a str,
    table_name: &'a str,
    fields: &'static [&'static str; LIST_COLUMNS_FIELDS.len()],
    rows: Vec<ColumnSearchRowValue<'a>>,
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
        catalog_name: Option<&'a str>,
        schema_name: &'a str,
        table_name: &'a str,
        rows: Vec<ColumnSearchRowValue<'a>>,
        pagination: &PaginationResponse,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            table_name,
            fields: &LIST_COLUMNS_FIELDS,
            rows,
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
#[schemars(deny_unknown_fields)]
struct FoundTableValue<'a> {
    found: bool,
    catalog_name: &'a str,
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
            catalog_name: &table.catalog_name,
            schema_name: &table.schema_name,
            table_name: &table.name,
            name: format_table_name(&table.catalog_name, &table.schema_name, &table.name),
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
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<&'a str>,
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
    ListCatalog,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct SuggestedCallArguments<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<&'a str>,
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
    catalog_name: &'a str,
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
            catalog_name: &table.catalog_name,
            schema_name: &table.schema_name,
            name: format_table_name(&table.catalog_name, &table.schema_name, &table.name),
            sql_reference: format_schema_table_equivalent(
                &table.catalog_name,
                &table.schema_name,
                &table.name,
            ),
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
    catalog_name: &'a str,
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
            catalog_name: &function.catalog_name,
            schema_name: &function.schema_name,
            name: format_table_name(
                &function.catalog_name,
                &function.schema_name,
                &function.name,
            ),
            sql_reference: format_schema_table_equivalent(
                &function.catalog_name,
                &function.schema_name,
                &function.name,
            ),
            sql_call_example: minimal_table_function_call_example(function),
            description: &function.description,
            table_function: CatalogTableFunctionValue {
                function_name: &function.name,
                guide: &function.guide,
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
    guide: &'a str,
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
struct ColumnSearchRowValue<'a>(
    &'a str,
    &'a str,
    bool,
    bool,
    bool,
    &'a str,
    #[schemars(range(min = 0))] u32,
    Option<&'a [String]>,
);

#[cfg(test)]
mod tests {
    use coral_api::v1::{
        CatalogItem, Column, ColumnSearchResult, ListCatalogResponse, ListColumnsResponse,
        PaginationResponse, TableFunction, catalog_item,
    };
    use serde_json::{Map, Value, json};

    use super::{
        DEFAULT_IGNORE_CASE, DEFAULT_REQUIRED_ONLY, list_catalog_arguments, list_catalog_value,
        list_columns_arguments, list_columns_value,
    };
    use crate::surface::discovery::{DEFAULT_PAGINATION_LIMIT, DEFAULT_PAGINATION_OFFSET};

    #[test]
    fn catalog_kind_argument_accepts_null_as_all_kinds() {
        let mut arguments = Map::new();
        arguments.insert("kind".to_string(), Value::Null);

        let list = list_catalog_arguments(Some(&arguments)).expect("list arguments");

        assert_eq!(list.kind, None);
    }

    #[test]
    fn catalog_qualified_table_function_list_catalog_uses_complete_identity() {
        let response = ListCatalogResponse {
            items: vec![CatalogItem {
                item: Some(catalog_item::Item::TableFunction(TableFunction {
                    catalog_name: "github_v4".to_string(),
                    schema_name: "issues".to_string(),
                    name: "list".to_string(),
                    ..Default::default()
                })),
            }],
            pagination: Some(PaginationResponse {
                total_count: 1,
                limit: 50,
                offset: 0,
                has_more: false,
                next_offset: 0,
            }),
            ..Default::default()
        };

        let value = list_catalog_value(&response);
        let item = value.pointer("/items/0").expect("catalog item");
        assert_eq!(item["catalog_name"], "github_v4");
        assert_eq!(item["name"], "github_v4.issues.list");
        assert_eq!(item["sql_reference"], "github_v4.issues.list");
        assert_eq!(item["sql_call_example"], "github_v4.issues.list()");
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
    fn list_columns_argument_defaults_use_shared_constants() {
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

    #[test]
    fn list_columns_fields_match_row_positions() {
        let response = ListColumnsResponse {
            columns: vec![ColumnSearchResult {
                column: Some(Column {
                    name: "issue_id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: true,
                    is_virtual: true,
                    is_required_filter: true,
                    description: "Stable issue identifier.".to_string(),
                    ordinal_position: 7,
                }),
                matched_fields: vec!["column_name".to_string()],
            }],
            pagination: Some(PaginationResponse {
                total_count: 1,
                limit: 50,
                offset: 0,
                has_more: false,
                next_offset: 0,
            }),
        };

        let value = list_columns_value(None, "github", "issues", &response);
        let fields = value.get("fields").expect("fields");
        let first_row = value
            .get("rows")
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .expect("first row");

        assert_eq!(
            fields,
            &json!([
                "column_name",
                "data_type",
                "is_nullable",
                "is_virtual",
                "is_required_filter",
                "description",
                "ordinal_position",
                "matched_fields"
            ])
        );
        assert_eq!(
            first_row,
            &json!([
                "issue_id",
                "Int64",
                true,
                true,
                true,
                "Stable issue identifier.",
                7,
                ["column_name"]
            ])
        );
    }
}
