use coral_api::v1::{
    ColumnSearchResult, DescribeCatalogSurfaceResponse, ListCatalogResponse, ListColumnsResponse,
    PaginationResponse, Table as ProtoTable, TableFunction as ProtoTableFunction,
    TableFunctionArgument as ProtoTableFunctionArgument,
    TableFunctionResultColumn as ProtoTableFunctionResultColumn, TableSummary as ProtoTableSummary,
    catalog_item, describe_catalog_surface_response,
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
    reject_unknown_arguments, required_string_argument,
};
use super::context::ToolDescriptionContext;
use super::discovery::{DefaultPaginationInput, Pagination, parse_pagination};
use super::schema::{tool_input_schema, tool_output_schema};
use super::tool_names::ToolName;
use super::values::{format_schema_table_equivalent, format_table_name, optional_catalog_name};

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

#[derive(Debug, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct CatalogSurfaceRef {
    #[schemars(
        length(min = 1),
        regex(pattern = r"\S"),
        description = "Optional SQL catalog name for a three-part table. Omit for two-part tables and table functions."
    )]
    pub(crate) catalog: Option<String>,
    #[schemars(
        length(min = 1),
        regex(pattern = r"\S"),
        description = "Exact SQL schema name."
    )]
    pub(crate) schema: String,
    #[schemars(
        length(min = 1),
        regex(pattern = r"\S"),
        description = "Exact bare table or table-function name within the SQL schema."
    )]
    pub(crate) surface: String,
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

pub(crate) fn describe_tool() -> Tool {
    Tool::new(
        ToolName::Describe.as_str(),
        "Describe one database table or table function from its SQL catalog, schema, and bare surface name. Coral resolves whether the surface is a table or table function.",
        tool_input_schema::<CatalogSurfaceRef>(),
    )
    .with_raw_output_schema(describe_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Describe")
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
        catalog: optional_non_empty_string_argument(arguments, "catalog")?,
        schema: optional_string_argument(arguments, "schema")?,
        kind: optional_catalog_kind_argument(arguments)?,
        pagination: parse_pagination(arguments)?,
    })
}

pub(crate) fn describe_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<CatalogSurfaceRef, ErrorData> {
    reject_unknown_arguments(
        arguments,
        &["catalog", "schema", "surface", "task_id", "intent"],
    )?;
    Ok(CatalogSurfaceRef {
        catalog: optional_non_empty_string_argument(arguments, "catalog")?,
        schema: required_string_argument(arguments, "schema")?,
        surface: required_string_argument(arguments, "surface")?,
    })
}

pub(crate) fn list_columns_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ListColumnsArguments, ErrorData> {
    Ok(ListColumnsArguments {
        catalog: optional_non_empty_string_argument(arguments, "catalog")?,
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

pub(crate) fn describe_value(
    arguments: &CatalogSurfaceRef,
    response: &DescribeCatalogSurfaceResponse,
) -> Result<Value, tonic::Status> {
    serde_json::to_value(describe_output(arguments, response)?).map_err(|error| {
        tonic::Status::internal(format!("failed to serialize describe response: {error}"))
    })
}

fn describe_output<'a>(
    arguments: &'a CatalogSurfaceRef,
    response: &'a DescribeCatalogSurfaceResponse,
) -> Result<DescribeOutput<'a>, tonic::Status> {
    use describe_catalog_surface_response::Result;

    match response.result.as_ref() {
        Some(Result::Table(table)) => Ok(DescribeOutput::Table(DescribeTableValue::from(table))),
        Some(Result::TableFunction(table_function)) => Ok(DescribeOutput::TableFunction(
            DescribeTableFunctionValue::from(table_function),
        )),
        Some(Result::Missing(missing)) => Ok(DescribeOutput::Missing(MissingSurfaceValue {
            reason: MissingSurfaceReason::Missing,
            available_schemas: &missing.available_schemas,
            same_schema_surfaces: missing
                .same_schema_items
                .iter()
                .filter_map(MissingSurfaceCandidateValue::from_catalog_item)
                .collect(),
            suggestions: missing
                .suggestions
                .iter()
                .filter_map(MissingSurfaceCandidateValue::from_catalog_item)
                .collect(),
            suggested_calls: vec![
                suggested_catalog_call(
                    arguments,
                    arguments.catalog.as_ref().map(|_| CatalogToolKind::Table),
                ),
                SuggestedCall {
                    tool: CatalogSuggestedTool::ListCatalog,
                    arguments: SuggestedCallArguments {
                        catalog: arguments.catalog.as_deref(),
                        schema: None,
                        kind: arguments.catalog.as_ref().map(|_| CatalogToolKind::Table),
                        limit: Some(10),
                    },
                },
            ],
        })),
        None => Err(tonic::Status::internal(
            "describe catalog surface response missing result",
        )),
    }
}

fn suggested_catalog_call(
    arguments: &CatalogSurfaceRef,
    kind: Option<CatalogToolKind>,
) -> SuggestedCall<'_> {
    SuggestedCall {
        tool: CatalogSuggestedTool::ListCatalog,
        arguments: SuggestedCallArguments {
            catalog: arguments.catalog.as_deref(),
            schema: Some(&arguments.schema),
            kind,
            limit: Some(10),
        },
    }
}

pub(crate) fn describe_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<DescribeOutput<'static>>()
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
    serde_json::to_value(ListColumnsPageValue::new(
        catalog,
        schema,
        table,
        rows,
        &pagination,
    ))
    .expect("list columns page value serializes")
}

pub(crate) fn list_columns_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<ListColumnsPageValue<'static>>()
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
enum DescribeOutput<'a> {
    Table(DescribeTableValue<'a>),
    TableFunction(DescribeTableFunctionValue<'a>),
    Missing(MissingSurfaceValue<'a>),
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
struct DescribeTableValue<'a> {
    kind: CatalogTableKind,
    description: &'a str,
    guide: &'a str,
    required_filters: &'a [String],
    column_count: usize,
    columns_hint: &'static str,
}

impl<'a> From<&'a ProtoTable> for DescribeTableValue<'a> {
    fn from(table: &'a ProtoTable) -> Self {
        Self {
            kind: CatalogTableKind::Table,
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
struct MissingSurfaceValue<'a> {
    reason: MissingSurfaceReason,
    available_schemas: &'a [String],
    same_schema_surfaces: Vec<MissingSurfaceCandidateValue<'a>>,
    suggestions: Vec<MissingSurfaceCandidateValue<'a>>,
    suggested_calls: Vec<SuggestedCall<'a>>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct MissingSurfaceCandidateValue<'a> {
    kind: CatalogToolKind,
    target: SurfaceTargetValue<'a>,
    description: &'a str,
}

impl<'a> MissingSurfaceCandidateValue<'a> {
    fn from_catalog_item(item: &'a coral_api::v1::CatalogItem) -> Option<Self> {
        match item.item.as_ref()? {
            catalog_item::Item::Table(table) => Some(Self {
                kind: CatalogToolKind::Table,
                target: SurfaceTargetValue {
                    catalog: optional_catalog_name(&table.catalog_name),
                    schema: &table.schema_name,
                    surface: &table.name,
                },
                description: &table.description,
            }),
            catalog_item::Item::TableFunction(function) => Some(Self {
                kind: CatalogToolKind::TableFunction,
                target: SurfaceTargetValue {
                    catalog: None,
                    schema: &function.schema_name,
                    surface: &function.name,
                },
                description: &function.description,
            }),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct SurfaceTargetValue<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<&'a str>,
    schema: &'a str,
    surface: &'a str,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum MissingSurfaceReason {
    Missing,
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
            name: format_table_name(
                optional_catalog_name(&table.catalog_name),
                &table.schema_name,
                &table.name,
            ),
            sql_reference: format_schema_table_equivalent(
                optional_catalog_name(&table.catalog_name),
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
    schema_name: &'a str,
    name: String,
    sql_reference: String,
    sql_call_example: String,
    description: &'a str,
    table_function: CatalogTableFunctionValue<'a>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
struct DescribeTableFunctionValue<'a> {
    kind: CatalogTableFunctionKind,
    description: &'a str,
    guide: &'a str,
    arguments: Vec<TableFunctionArgumentValue<'a>>,
    result_columns: Vec<TableFunctionResultColumnValue<'a>>,
}

impl<'a> From<&'a ProtoTableFunction> for DescribeTableFunctionValue<'a> {
    fn from(function: &'a ProtoTableFunction) -> Self {
        Self {
            kind: CatalogTableFunctionKind::TableFunction,
            description: &function.description,
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
        }
    }
}

impl<'a> From<&'a ProtoTableFunction> for CatalogTableFunctionItemValue<'a> {
    fn from(function: &'a ProtoTableFunction) -> Self {
        Self {
            kind: CatalogTableFunctionKind::TableFunction,
            schema_name: &function.schema_name,
            name: format!("{}.{}", function.schema_name, function.name),
            sql_reference: format_schema_table_equivalent(
                None,
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
        CatalogItem, Column, ColumnSearchResult, DescribeCatalogSurfaceResponse,
        ListColumnsResponse, MissingCatalogSurface, PaginationResponse, Table, TableFunction,
        TableSummary, catalog_item, describe_catalog_surface_response,
    };
    use serde_json::{Map, Value, json};

    use super::{
        CatalogSurfaceRef, DEFAULT_IGNORE_CASE, DEFAULT_REQUIRED_ONLY, describe_arguments,
        describe_tool, describe_value, list_catalog_arguments, list_columns_arguments,
        list_columns_value,
    };
    use crate::surface::discovery::{DEFAULT_PAGINATION_LIMIT, DEFAULT_PAGINATION_OFFSET};

    fn json_object(value: &Value) -> Map<String, Value> {
        value.as_object().cloned().expect("JSON object")
    }

    fn value_at<'a>(value: &'a Value, pointer: &str) -> &'a Value {
        value
            .pointer(pointer)
            .unwrap_or_else(|| panic!("missing JSON pointer '{pointer}' in {value}"))
    }

    fn assert_absent(value: &Value, keys: &[&str]) {
        for key in keys {
            assert!(
                value.get(key).is_none(),
                "unexpected key '{key}' in {value}"
            );
        }
    }

    #[test]
    fn catalog_kind_argument_accepts_null_as_all_kinds() {
        let mut arguments = Map::new();
        arguments.insert("kind".to_string(), Value::Null);

        let list = list_catalog_arguments(Some(&arguments)).expect("list arguments");

        assert_eq!(list.kind, None);
    }

    #[test]
    fn describe_parses_one_flat_surface_reference() {
        let input = json_object(&json!({
            "catalog": " warehouse ", "schema": " public ", "surface": " events ",
            "task_id": "parsed earlier", "intent": "test the parser"
        }));
        let arguments = describe_arguments(Some(&input)).expect("describe arguments");

        assert_eq!(arguments.catalog.as_deref(), Some(" warehouse "));
        assert_eq!(arguments.schema, " public ");
        assert_eq!(arguments.surface, " events ");
    }

    #[test]
    fn describe_rejects_unknown_and_nested_target_arguments() {
        for value in [
            json!({"schema": "github", "surface": "issues", "surafce": "typo"}),
            json!({"table": {"schema": "github", "table": "issues"}}),
        ] {
            let input = json_object(&value);
            assert!(
                describe_arguments(Some(&input)).is_err(),
                "invalid input accepted: {input:?}"
            );
        }
    }

    #[test]
    fn describe_input_schema_matches_the_flat_parser_contract() {
        let schema = Value::Object((*describe_tool().input_schema).clone());
        let validator = jsonschema::validator_for(&schema).expect("describe input schema compiles");

        for (input, expected) in [
            (json!({"schema": "github", "surface": "issues"}), true),
            (json!({"schema": " github ", "surface": " issues "}), true),
            (
                json!({"catalog": "warehouse", "schema": "public", "surface": "events"}),
                true,
            ),
            (json!({}), false),
            (json!({"schema": "github"}), false),
            (json!({"schema": "github", "surface": ""}), false),
            (json!({"schema": "github", "surface": "   "}), false),
            (
                json!({"catalog": "   ", "schema": "github", "surface": "issues"}),
                false,
            ),
            (
                json!({"schema": "github", "surface": "issues", "surafce": "typo"}),
                false,
            ),
            (
                json!({"table": {"schema": "github", "table": "issues"}}),
                false,
            ),
        ] {
            assert_eq!(
                validator.is_valid(&input),
                expected,
                "unexpected schema result for {input}"
            );
        }
    }

    #[test]
    fn describe_renders_table_function_and_missing_surfaces() {
        use describe_catalog_surface_response::Result;

        let arguments = CatalogSurfaceRef {
            catalog: None,
            schema: "searchy".to_string(),
            surface: "lookup".to_string(),
        };
        let table = Table {
            schema_name: "searchy".to_string(),
            name: "lookup".to_string(),
            ..Table::default()
        };
        let function = TableFunction {
            schema_name: "searchy".to_string(),
            name: "lookup".to_string(),
            description: "Lookup function".to_string(),
            guide: "Call it with an issue key.".to_string(),
            ..TableFunction::default()
        };
        let render = |result| {
            describe_value(&arguments, &DescribeCatalogSurfaceResponse { result })
                .expect("describe response")
        };
        let schema = Value::Object((*super::describe_output_schema()).clone());
        let validator =
            jsonschema::validator_for(&schema).expect("describe output schema compiles");
        let table_only = render(Some(Result::Table(table.clone())));
        assert_eq!(value_at(&table_only, "/kind"), "table");
        assert_absent(&table_only, &["name", "sql_reference", "schema_name"]);

        let function_only = render(Some(Result::TableFunction(function)));
        assert_eq!(value_at(&function_only, "/kind"), "table_function");
        assert_eq!(value_at(&function_only, "/description"), "Lookup function");
        assert_absent(&function_only, &["name", "sql_reference", "schema_name"]);

        let missing = render(Some(Result::Missing(MissingCatalogSurface {
            suggestions: vec![
                CatalogItem {
                    item: Some(catalog_item::Item::Table(TableSummary {
                        catalog_name: "warehouse".to_string(),
                        schema_name: "searchy".to_string(),
                        name: "lookups".to_string(),
                        description: "Lookup table".to_string(),
                        ..TableSummary::default()
                    })),
                },
                CatalogItem {
                    item: Some(catalog_item::Item::TableFunction(TableFunction {
                        schema_name: "searchy".to_string(),
                        name: "lookup_issue".to_string(),
                        description: "Lookup function".to_string(),
                        ..TableFunction::default()
                    })),
                },
            ],
            available_schemas: vec!["searchy".to_string()],
            same_schema_items: Vec::new(),
        })));
        assert_eq!(value_at(&missing, "/reason"), "missing");
        assert_eq!(value_at(&missing, "/suggestions/0/kind"), "table");
        assert_eq!(
            value_at(&missing, "/suggestions/0/target/catalog"),
            "warehouse"
        );
        assert_eq!(
            value_at(&missing, "/suggestions/0/target/surface"),
            "lookups"
        );
        assert_eq!(value_at(&missing, "/suggestions/1/kind"), "table_function");
        assert_eq!(
            value_at(&missing, "/suggestions/1/target/surface"),
            "lookup_issue"
        );
        assert!(missing.pointer("/suggestions/1/target/catalog").is_none());
        assert!(
            missing
                .pointer("/suggested_calls/1/arguments/schema")
                .is_none()
        );

        for (case, output) in [
            ("table", table_only),
            ("table function", function_only),
            ("missing", missing),
        ] {
            assert!(
                validator.is_valid(&output),
                "describe output schema rejected {case}: {output}"
            );
        }

        let mut wrong_kind = render(Some(Result::Table(table)));
        wrong_kind
            .as_object_mut()
            .expect("table response object")
            .insert("kind".to_string(), json!("table_function"));
        assert!(!validator.is_valid(&wrong_kind));
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
