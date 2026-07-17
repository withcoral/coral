//! Registers the `coral` system schema for discoverable source metadata.

use std::any::Any;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::ManifestInputKind;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::future::join_all;
use serde::Serialize;

use crate::backends::common::{
    RegisteredTableFunctionArgument, RegisteredTableFunctionResultColumn,
};
use crate::backends::shared::filter_expr::literal_to_string;
use crate::backends::{
    CatalogColumnFetcher, ColumnInventoryFilter, DatabaseColumnRow, RegisteredSource,
    RegisteredTableFunction,
};
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{
    ColumnInfo, TableFunctionArgumentInfo, TableFunctionInfo, TableFunctionResultColumnInfo,
    TableInfo,
};
use datafusion::datasource::TableProvider;

/// Schema name for source metadata tables such as `coral.tables`.
pub(crate) const SYSTEM_SCHEMA: &str = "coral";

/// Register `coral.tables` and `coral.columns` for the active source set.
///
/// # Errors
///
/// Returns a `DataFusionError` if the catalog is missing or the metadata
/// tables cannot be materialized.
pub(crate) fn register(
    ctx: &SessionContext,
    active_sources: &[RegisteredSource],
    column_fetchers: &[CatalogColumnFetcher],
) -> Result<()> {
    let tables_table = build_tables_table(active_sources)?;
    let columns_table = build_columns_table(active_sources, column_fetchers)?;
    let filters_table = build_filters_table(active_sources)?;
    let inputs_table = build_inputs_table(active_sources)?;
    let table_functions_table = build_table_functions_table(active_sources)?;

    let mut meta_tables: HashMap<String, Arc<dyn datafusion::datasource::TableProvider>> =
        HashMap::new();
    meta_tables.insert("tables".to_string(), Arc::new(tables_table));
    meta_tables.insert("columns".to_string(), Arc::new(columns_table));
    meta_tables.insert("filters".to_string(), Arc::new(filters_table));
    meta_tables.insert("inputs".to_string(), Arc::new(inputs_table));
    meta_tables.insert(
        "table_functions".to_string(),
        Arc::new(table_functions_table),
    );

    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("catalog 'datafusion' not found".to_string()))?;
    catalog.register_schema(
        SYSTEM_SCHEMA,
        Arc::new(StaticSchemaProvider::new(meta_tables)),
    )?;

    Ok(())
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn build_table_functions_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("function_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("arguments_json", DataType::Utf8, false),
        Field::new("result_columns_json", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("search_limits_json", DataType::Utf8, true),
    ]));

    let mut rows = active_sources
        .iter()
        .flat_map(|source| source.table_functions.iter())
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        (&left.schema_name, &left.function_name).cmp(&(&right.schema_name, &right.function_name))
    });

    let arguments_json = rows
        .iter()
        .map(|row| table_function_arguments_json(row))
        .collect::<Result<Vec<_>>>()?;
    let result_columns_json = rows
        .iter()
        .map(|row| table_function_result_columns_json(row))
        .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            utf8_column(rows.iter().map(|row| Some(row.schema_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.function_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.description.as_str()))),
            utf8_column(arguments_json.iter().map(|value| Some(value.as_str()))),
            utf8_column(result_columns_json.iter().map(|value| Some(value.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.kind.as_str()))),
            utf8_column(rows.iter().map(|row| row.search_limits_json.as_deref())),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}

fn table_function_arguments_json(row: &RegisteredTableFunction) -> Result<String> {
    let arguments = row
        .arguments
        .iter()
        .map(TableFunctionArgumentJson::from)
        .collect::<Vec<_>>();
    serde_json::to_string(&arguments).map_err(|error| DataFusionError::External(Box::new(error)))
}

fn table_function_result_columns_json(row: &RegisteredTableFunction) -> Result<String> {
    let columns = row
        .result_columns
        .iter()
        .map(TableFunctionResultColumnJson::from)
        .collect::<Vec<_>>();
    serde_json::to_string(&columns).map_err(|error| DataFusionError::External(Box::new(error)))
}

#[derive(Serialize)]
struct TableFunctionArgumentJson<'a> {
    name: &'a str,
    required: bool,
    values: &'a [String],
}

impl<'a> From<&'a RegisteredTableFunctionArgument> for TableFunctionArgumentJson<'a> {
    fn from(argument: &'a RegisteredTableFunctionArgument) -> Self {
        Self {
            name: &argument.name,
            required: argument.required,
            values: &argument.values,
        }
    }
}

#[derive(Serialize)]
struct TableFunctionResultColumnJson<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    data_type: &'a str,
    nullable: bool,
    description: &'a str,
}

impl<'a> From<&'a RegisteredTableFunctionResultColumn> for TableFunctionResultColumnJson<'a> {
    fn from(column: &'a RegisteredTableFunctionResultColumn) -> Self {
        Self {
            name: &column.name,
            data_type: &column.data_type,
            nullable: column.nullable,
            description: &column.description,
        }
    }
}

fn utf8_column<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(values.into_iter().collect::<StringArray>())
}

struct SystemTableDefinition {
    table_name: &'static str,
    description: &'static str,
    guide: &'static str,
    columns: &'static [SystemColumnDefinition],
}

struct SystemColumnDefinition {
    name: &'static str,
    data_type: &'static str,
    nullable: bool,
    description: &'static str,
}

const TABLES_COLUMNS: &[SystemColumnDefinition] = &[
    SystemColumnDefinition {
        name: "schema_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL schema containing the table.",
    },
    SystemColumnDefinition {
        name: "table_name",
        data_type: "Utf8",
        nullable: false,
        description: "Table name within the schema.",
    },
    SystemColumnDefinition {
        name: "description",
        data_type: "Utf8",
        nullable: false,
        description: "Human-readable table description.",
    },
    SystemColumnDefinition {
        name: "guide",
        data_type: "Utf8",
        nullable: false,
        description: "Query guidance for the table.",
    },
    SystemColumnDefinition {
        name: "required_filters",
        data_type: "Utf8",
        nullable: false,
        description: "Comma-separated required filter names.",
    },
    SystemColumnDefinition {
        name: "search_limits_json",
        data_type: "Utf8",
        nullable: true,
        description: "JSON search-limit metadata when the table declares provider search limits.",
    },
    SystemColumnDefinition {
        name: "catalog_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL catalog containing the table. Empty for tables queried as schema_name.table_name.",
    },
];

const COLUMNS_COLUMNS: &[SystemColumnDefinition] = &[
    SystemColumnDefinition {
        name: "schema_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL schema containing the table.",
    },
    SystemColumnDefinition {
        name: "table_name",
        data_type: "Utf8",
        nullable: false,
        description: "Table name within the schema.",
    },
    SystemColumnDefinition {
        name: "ordinal_position",
        data_type: "Int32",
        nullable: false,
        description: "Zero-based position of the column within the table.",
    },
    SystemColumnDefinition {
        name: "column_name",
        data_type: "Utf8",
        nullable: false,
        description: "Column name.",
    },
    SystemColumnDefinition {
        name: "data_type",
        data_type: "Utf8",
        nullable: false,
        description: "Column data type: Arrow/DataFusion string form for static sources; the provider-native type name for database catalogs.",
    },
    SystemColumnDefinition {
        name: "is_nullable",
        data_type: "Boolean",
        nullable: false,
        description: "Whether the column can contain SQL NULL values.",
    },
    SystemColumnDefinition {
        name: "is_virtual",
        data_type: "Boolean",
        nullable: false,
        description: "Whether the column is provider-derived metadata or a filter projection.",
    },
    SystemColumnDefinition {
        name: "is_required_filter",
        data_type: "Boolean",
        nullable: false,
        description: "Whether the column must be constrained before querying the table.",
    },
    SystemColumnDefinition {
        name: "description",
        data_type: "Utf8",
        nullable: false,
        description: "Human-readable column description.",
    },
    SystemColumnDefinition {
        name: "filter_mode",
        data_type: "Utf8",
        nullable: true,
        description: "Filter matching mode for virtual filter columns.",
    },
    SystemColumnDefinition {
        name: "catalog_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL catalog containing the table. Empty for tables queried as schema_name.table_name.",
    },
];

const FILTERS_COLUMNS: &[SystemColumnDefinition] = &[
    SystemColumnDefinition {
        name: "schema_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL schema containing the filtered table.",
    },
    SystemColumnDefinition {
        name: "table_name",
        data_type: "Utf8",
        nullable: false,
        description: "Filtered table name within the schema.",
    },
    SystemColumnDefinition {
        name: "filter_name",
        data_type: "Utf8",
        nullable: false,
        description: "Filter name.",
    },
    SystemColumnDefinition {
        name: "filter_mode",
        data_type: "Utf8",
        nullable: false,
        description: "Filter matching mode.",
    },
    SystemColumnDefinition {
        name: "is_required",
        data_type: "Boolean",
        nullable: false,
        description: "Whether the filter must be provided before querying the table.",
    },
    SystemColumnDefinition {
        name: "data_type",
        data_type: "Utf8",
        nullable: false,
        description: "Filter value data type.",
    },
    SystemColumnDefinition {
        name: "description",
        data_type: "Utf8",
        nullable: false,
        description: "Human-readable filter description.",
    },
];

const INPUTS_COLUMNS: &[SystemColumnDefinition] = &[
    SystemColumnDefinition {
        name: "schema_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL schema for the source that declares the input.",
    },
    SystemColumnDefinition {
        name: "key",
        data_type: "Utf8",
        nullable: false,
        description: "Source input key.",
    },
    SystemColumnDefinition {
        name: "kind",
        data_type: "Utf8",
        nullable: false,
        description: "Input kind: variable or secret.",
    },
    SystemColumnDefinition {
        name: "value",
        data_type: "Utf8",
        nullable: true,
        description: "Resolved variable value. Secret values are never exposed.",
    },
    SystemColumnDefinition {
        name: "default_value",
        data_type: "Utf8",
        nullable: true,
        description: "Default value declared by the source, when present.",
    },
    SystemColumnDefinition {
        name: "hint",
        data_type: "Utf8",
        nullable: true,
        description: "Input setup hint declared by the source.",
    },
    SystemColumnDefinition {
        name: "required",
        data_type: "Boolean",
        nullable: false,
        description: "Whether the input is required.",
    },
    SystemColumnDefinition {
        name: "is_set",
        data_type: "Boolean",
        nullable: false,
        description: "Whether Coral resolved a value for the input.",
    },
];

const TABLE_FUNCTIONS_COLUMNS: &[SystemColumnDefinition] = &[
    SystemColumnDefinition {
        name: "schema_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL schema containing the table function.",
    },
    SystemColumnDefinition {
        name: "function_name",
        data_type: "Utf8",
        nullable: false,
        description: "Table function name within the schema.",
    },
    SystemColumnDefinition {
        name: "description",
        data_type: "Utf8",
        nullable: false,
        description: "Human-readable table function description.",
    },
    SystemColumnDefinition {
        name: "arguments_json",
        data_type: "Utf8",
        nullable: false,
        description: "JSON array describing accepted function arguments.",
    },
    SystemColumnDefinition {
        name: "result_columns_json",
        data_type: "Utf8",
        nullable: false,
        description: "JSON array describing columns returned by the function.",
    },
    SystemColumnDefinition {
        name: "kind",
        data_type: "Utf8",
        nullable: false,
        description: "Function kind, such as search.",
    },
    SystemColumnDefinition {
        name: "search_limits_json",
        data_type: "Utf8",
        nullable: true,
        description: "JSON search-limit metadata when the function declares provider search limits.",
    },
];

const SYSTEM_TABLE_DEFINITIONS: &[SystemTableDefinition] = &[
    SystemTableDefinition {
        table_name: "columns",
        description: "Column metadata for queryable Coral tables.",
        guide: "Filter by schema_name and table_name, then order by ordinal_position to inspect a table's shape.",
        columns: COLUMNS_COLUMNS,
    },
    SystemTableDefinition {
        table_name: "filters",
        description: "Filter metadata for source-backed Coral tables.",
        guide: "Use this table to inspect required filters and filter matching modes before querying source-backed tables.",
        columns: FILTERS_COLUMNS,
    },
    SystemTableDefinition {
        table_name: "inputs",
        description: "Resolved source input metadata.",
        guide: "Use this table to inspect configured source variables and whether required secrets are set. Secret values are not exposed.",
        columns: INPUTS_COLUMNS,
    },
    SystemTableDefinition {
        table_name: "table_functions",
        description: "Metadata for source-scoped Coral table functions.",
        guide: "Use this table to discover function arguments and result columns before calling a table function in SQL.",
        columns: TABLE_FUNCTIONS_COLUMNS,
    },
    SystemTableDefinition {
        table_name: "tables",
        description: "Queryable table metadata for installed sources and Coral system catalog tables.",
        guide: "Use this table to list schemas, tables, descriptions, required filters, and search limits before querying data tables.",
        columns: TABLES_COLUMNS,
    },
];

fn system_table_infos() -> Vec<TableInfo> {
    SYSTEM_TABLE_DEFINITIONS
        .iter()
        .map(|table| TableInfo {
            catalog_name: String::new(),
            schema_name: SYSTEM_SCHEMA.to_string(),
            table_name: table.table_name.to_string(),
            description: table.description.to_string(),
            guide: table.guide.to_string(),
            columns: table
                .columns
                .iter()
                .enumerate()
                .map(|(position, column)| ColumnInfo {
                    name: column.name.to_string(),
                    data_type: column.data_type.to_string(),
                    nullable: column.nullable,
                    is_virtual: false,
                    is_required_filter: false,
                    description: column.description.to_string(),
                    ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
                })
                .collect(),
            required_filters: Vec::new(),
        })
        .collect()
}

/// Collect static query-visible table metadata for the active source set.
///
/// This intentionally excludes catalog-backed database tables. Query execution
/// uses this lightweight snapshot for static source hints and does not need to
/// enumerate database catalogs before running ordinary SQL.
#[must_use]
pub(crate) fn collect_static_tables(active_sources: &[RegisteredSource]) -> Vec<TableInfo> {
    let mut tables = system_table_infos();
    tables.extend(active_sources.iter().flat_map(|source| {
        source.tables.iter().map(move |table| TableInfo {
            catalog_name: source.catalog_name.clone().unwrap_or_default(),
            schema_name: table
                .schema_name
                .clone()
                .unwrap_or_else(|| source.schema_name.clone()),
            table_name: table.table_name.clone(),
            description: table.description.clone(),
            guide: table.guide.clone(),
            columns: table
                .columns
                .iter()
                .enumerate()
                .map(|(position, column)| ColumnInfo {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    is_virtual: column.is_virtual,
                    is_required_filter: column.is_required_filter,
                    description: column.description.clone(),
                    ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
                })
                .collect(),
            required_filters: table.required_filters.clone(),
        })
    }));
    tables.sort_by(|left, right| {
        (&left.catalog_name, &left.schema_name, &left.table_name).cmp(&(
            &right.catalog_name,
            &right.schema_name,
            &right.table_name,
        ))
    });
    tables
}

/// Collect typed table metadata by querying the public `coral` catalog views.
pub(crate) async fn collect_tables(
    ctx: &SessionContext,
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Result<Vec<TableInfo>> {
    collect_tables_with_filters(ctx, catalog_filter, schema_filter, &[], table_filter).await
}

/// Collect typed table metadata for any of the supplied source/schema filters.
pub(crate) async fn collect_tables_for_schema_filters(
    ctx: &SessionContext,
    schema_filters: &[&str],
) -> Result<Vec<TableInfo>> {
    collect_tables_with_filters(ctx, None, None, schema_filters, None).await
}

async fn collect_tables_with_filters(
    ctx: &SessionContext,
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    qualifier_filters: &[&str],
    table_filter: Option<&str>,
) -> Result<Vec<TableInfo>> {
    let mut tables = collect_table_info_by_key(
        ctx,
        catalog_filter,
        schema_filter,
        qualifier_filters,
        table_filter,
    )
    .await?;

    let columns_sql = catalog_columns_query(
        catalog_filter,
        schema_filter,
        qualifier_filters,
        table_filter,
    );
    let column_batches = ctx.sql(&columns_sql).await?.collect().await?;
    apply_column_infos_from_batches(&mut tables, &column_batches)?;

    let mut tables = tables.into_values().collect::<Vec<_>>();
    sort_tables(&mut tables);
    for table in &mut tables {
        table.columns.sort_by_key(|column| column.ordinal_position);
    }
    Ok(tables)
}

/// Collect table metadata without column expansion.
pub(crate) async fn collect_table_metadata(
    ctx: &SessionContext,
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Result<Vec<TableInfo>> {
    let mut tables =
        collect_table_info_by_key(ctx, catalog_filter, schema_filter, &[], table_filter)
            .await?
            .into_values()
            .collect::<Vec<_>>();
    sort_tables(&mut tables);
    Ok(tables)
}

pub(crate) async fn collect_table_metadata_for_qualifier(
    ctx: &SessionContext,
    qualifier_filter: &str,
) -> Result<Vec<TableInfo>> {
    let mut tables = collect_table_info_by_key(ctx, None, None, &[qualifier_filter], None)
        .await?
        .into_values()
        .collect::<Vec<_>>();
    sort_tables(&mut tables);
    Ok(tables)
}

async fn collect_table_info_by_key(
    ctx: &SessionContext,
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    qualifier_filters: &[&str],
    table_filter: Option<&str>,
) -> Result<TableInfoByKey> {
    let tables_sql = catalog_tables_query(
        catalog_filter,
        schema_filter,
        qualifier_filters,
        table_filter,
    );
    let table_batches = ctx.sql(&tables_sql).await?.collect().await?;
    collect_table_infos_from_batches(
        &table_batches,
        catalog_filter,
        schema_filter,
        qualifier_filters,
        table_filter,
    )
}

fn catalog_tables_query(
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    qualifier_filters: &[&str],
    table_filter: Option<&str>,
) -> String {
    let mut sql =
        "SELECT catalog_name, schema_name, table_name, description, guide, required_filters \
         FROM coral.tables"
            .to_string();
    append_catalog_filter(
        &mut sql,
        catalog_filter,
        schema_filter,
        qualifier_filters,
        table_filter,
    );
    sql
}

fn catalog_columns_query(
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    qualifier_filters: &[&str],
    table_filter: Option<&str>,
) -> String {
    let mut sql =
        "SELECT catalog_name, schema_name, table_name, ordinal_position, column_name, data_type, \
         is_nullable, is_virtual, is_required_filter, description FROM coral.columns"
            .to_string();
    append_catalog_filter(
        &mut sql,
        catalog_filter,
        schema_filter,
        qualifier_filters,
        table_filter,
    );
    sql
}

fn append_catalog_filter(
    sql: &mut String,
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    qualifier_filters: &[&str],
    table_filter: Option<&str>,
) {
    let mut predicates = Vec::new();
    if let Some(catalog_filter) = catalog_filter {
        predicates.push(format!(
            "catalog_name = {}",
            sql_string_literal(catalog_filter)
        ));
    }
    if let Some(schema_filter) = schema_filter {
        predicates.push(format!(
            "schema_name = {}",
            sql_string_literal(schema_filter)
        ));
    }
    if !qualifier_filters.is_empty() {
        let qualifier_predicates = qualifier_filters
            .iter()
            .map(|filter| {
                let literal = sql_string_literal(filter);
                format!("(catalog_name = {literal} OR schema_name = {literal})")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        predicates.push(format!("({qualifier_predicates})"));
    }
    if let Some(table_filter) = table_filter {
        predicates.push(format!("table_name = {}", sql_string_literal(table_filter)));
    }
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
}

fn sort_tables(tables: &mut [TableInfo]) {
    tables.sort_by(|left, right| {
        (&left.catalog_name, &left.schema_name, &left.table_name).cmp(&(
            &right.catalog_name,
            &right.schema_name,
            &right.table_name,
        ))
    });
}

type TableInfoByKey = HashMap<(String, String, String), TableInfo>;

fn collect_table_infos_from_batches(
    batches: &[RecordBatch],
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    qualifier_filters: &[&str],
    table_filter: Option<&str>,
) -> Result<TableInfoByKey> {
    let mut tables = HashMap::new();
    for batch in batches {
        let catalog_names = string_array(batch, "catalog_name")?;
        let schema_names = string_array(batch, "schema_name")?;
        let table_names = string_array(batch, "table_name")?;
        let descriptions = string_array(batch, "description")?;
        let guides = string_array(batch, "guide")?;
        let required_filters = string_array(batch, "required_filters")?;

        for row in 0..batch.num_rows() {
            let catalog_name = catalog_names.value(row).to_string();
            let schema_name = schema_names.value(row).to_string();
            let table_name = table_names.value(row).to_string();
            if !table_matches_query_filter(
                &catalog_name,
                &schema_name,
                &table_name,
                catalog_filter,
                schema_filter,
                qualifier_filters,
                table_filter,
            ) {
                continue;
            }
            tables.insert(
                (
                    catalog_name.clone(),
                    schema_name.clone(),
                    table_name.clone(),
                ),
                TableInfo {
                    catalog_name,
                    schema_name,
                    table_name,
                    description: descriptions.value(row).to_string(),
                    guide: guides.value(row).to_string(),
                    columns: Vec::new(),
                    required_filters: split_required_filters(required_filters.value(row)),
                },
            );
        }
    }
    Ok(tables)
}

fn apply_column_infos_from_batches(
    tables: &mut TableInfoByKey,
    batches: &[RecordBatch],
) -> Result<()> {
    for batch in batches {
        let catalog_names = string_array(batch, "catalog_name")?;
        let schema_names = string_array(batch, "schema_name")?;
        let table_names = string_array(batch, "table_name")?;
        let positions = int32_array(batch, "ordinal_position")?;
        let column_names = string_array(batch, "column_name")?;
        let data_types = string_array(batch, "data_type")?;
        let is_nullable = bool_array(batch, "is_nullable")?;
        let is_virtual = bool_array(batch, "is_virtual")?;
        let is_required_filter = bool_array(batch, "is_required_filter")?;
        let descriptions = string_array(batch, "description")?;

        for row in 0..batch.num_rows() {
            let key = (
                catalog_names.value(row).to_string(),
                schema_names.value(row).to_string(),
                table_names.value(row).to_string(),
            );
            let Some(table) = tables.get_mut(&key) else {
                continue;
            };
            table.columns.push(ColumnInfo {
                name: column_names.value(row).to_string(),
                data_type: data_types.value(row).to_string(),
                nullable: is_nullable.value(row),
                is_virtual: is_virtual.value(row),
                is_required_filter: is_required_filter.value(row),
                description: descriptions.value(row).to_string(),
                ordinal_position: u32::try_from(positions.value(row)).unwrap_or_default(),
            });
        }
    }
    Ok(())
}

fn string_array<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            DataFusionError::Execution(format!("coral catalog column '{name}' is not Utf8"))
        })
}

fn int32_array<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int32Array> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
        .ok_or_else(|| {
            DataFusionError::Execution(format!("coral catalog column '{name}' is not Int32"))
        })
}

fn bool_array<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BooleanArray> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
        .ok_or_else(|| {
            DataFusionError::Execution(format!("coral catalog column '{name}' is not Boolean"))
        })
}

fn split_required_filters(value: &str) -> Vec<String> {
    if value.is_empty() {
        Vec::new()
    } else {
        value.split(',').map(ToString::to_string).collect()
    }
}

fn table_matches_query_filter(
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    qualifier_filters: &[&str],
    table_filter: Option<&str>,
) -> bool {
    catalog_filter.is_none_or(|value| catalog_name == value)
        && schema_filter.is_none_or(|value| schema_name == value)
        && (qualifier_filters.is_empty()
            || qualifier_filters
                .iter()
                .any(|value| catalog_name == *value || schema_name == *value))
        && table_filter.is_none_or(|value| table_name == value)
}

/// Collect typed source-scoped table function metadata for the active source set.
#[must_use]
pub(crate) fn collect_table_functions(
    active_sources: &[RegisteredSource],
) -> Vec<TableFunctionInfo> {
    let mut functions = active_sources
        .iter()
        .flat_map(|source| {
            source
                .table_functions
                .iter()
                .map(move |function| TableFunctionInfo {
                    schema_name: function.schema_name.clone(),
                    function_name: function.function_name.clone(),
                    description: function.description.clone(),
                    arguments: function
                        .arguments
                        .iter()
                        .map(|argument| TableFunctionArgumentInfo {
                            name: argument.name.clone(),
                            required: argument.required,
                            values: argument.values.clone(),
                        })
                        .collect(),
                    result_columns: function
                        .result_columns
                        .iter()
                        .map(|column| TableFunctionResultColumnInfo {
                            name: column.name.clone(),
                            data_type: column.data_type.clone(),
                            nullable: column.nullable,
                            description: column.description.clone(),
                        })
                        .collect(),
                })
        })
        .collect::<Vec<_>>();
    functions.sort_by(|left, right| {
        (&left.schema_name, &left.function_name).cmp(&(&right.schema_name, &right.function_name))
    });
    functions
}

struct CatalogTable {
    catalog_name: String,
    schema_name: String,
    table_name: String,
    description: String,
    guide: String,
    required_filters: String,
    search_limits_json: Option<String>,
}

fn build_tables_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("guide", DataType::Utf8, false),
        Field::new("required_filters", DataType::Utf8, false),
        Field::new("search_limits_json", DataType::Utf8, true),
        Field::new("catalog_name", DataType::Utf8, false),
    ]));

    let mut rows = SYSTEM_TABLE_DEFINITIONS
        .iter()
        .map(|table| CatalogTable {
            catalog_name: String::new(),
            schema_name: SYSTEM_SCHEMA.to_string(),
            table_name: table.table_name.to_string(),
            description: table.description.to_string(),
            guide: table.guide.to_string(),
            required_filters: String::new(),
            search_limits_json: None,
        })
        .chain(active_sources.iter().flat_map(|source| {
            source.tables.iter().map(move |table| CatalogTable {
                catalog_name: source.catalog_name.clone().unwrap_or_default(),
                schema_name: table
                    .schema_name
                    .clone()
                    .unwrap_or_else(|| source.schema_name.clone()),
                table_name: table.table_name.clone(),
                description: table.description.clone(),
                guide: table.guide.clone(),
                required_filters: table.required_filters.join(","),
                search_limits_json: table.search_limits_json.clone(),
            })
        }))
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        (&left.catalog_name, &left.schema_name, &left.table_name).cmp(&(
            &right.catalog_name,
            &right.schema_name,
            &right.table_name,
        ))
    });

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            utf8_column(rows.iter().map(|row| Some(row.schema_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.table_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.description.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.guide.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.required_filters.as_str()))),
            utf8_column(rows.iter().map(|row| row.search_limits_json.as_deref())),
            utf8_column(rows.iter().map(|row| Some(row.catalog_name.as_str()))),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}

struct CatalogFilter {
    schema_name: String,
    table_name: String,
    filter_name: String,
    filter_mode: String,
    is_required: bool,
    data_type: String,
    description: String,
}

fn build_filters_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("filter_name", DataType::Utf8, false),
        Field::new("filter_mode", DataType::Utf8, false),
        Field::new("is_required", DataType::Boolean, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]));

    let mut rows = active_sources
        .iter()
        .flat_map(|source| {
            source.tables.iter().flat_map(move |table| {
                table.filters.iter().map(move |filter| CatalogFilter {
                    schema_name: source.schema_name.clone(),
                    table_name: table.table_name.clone(),
                    filter_name: filter.name.clone(),
                    filter_mode: filter.mode.clone(),
                    is_required: filter.required,
                    data_type: filter.data_type.clone(),
                    description: filter.description.clone(),
                })
            })
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        (&left.schema_name, &left.table_name, &left.filter_name).cmp(&(
            &right.schema_name,
            &right.table_name,
            &right.filter_name,
        ))
    });

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            utf8_column(rows.iter().map(|row| Some(row.schema_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.table_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.filter_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.filter_mode.as_str()))),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.is_required))
                    .collect::<BooleanArray>(),
            ),
            utf8_column(rows.iter().map(|row| Some(row.data_type.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.description.as_str()))),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}

struct CatalogInput {
    schema_name: String,
    key: String,
    kind: &'static str,
    value: Option<String>,
    /// Empty string (= "no default declared" in the spec) renders as SQL NULL.
    default_value: String,
    hint: Option<String>,
    required: bool,
    is_set: bool,
}

fn build_inputs_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("default_value", DataType::Utf8, true),
        Field::new("hint", DataType::Utf8, true),
        Field::new("required", DataType::Boolean, false),
        Field::new("is_set", DataType::Boolean, false),
    ]));

    let mut rows: Vec<CatalogInput> = active_sources
        .iter()
        .flat_map(|source| {
            source.inputs.iter().map(move |input| CatalogInput {
                schema_name: source.schema_name.clone(),
                key: input.key.clone(),
                kind: match input.kind {
                    ManifestInputKind::Variable => "variable",
                    ManifestInputKind::Secret => "secret",
                },
                value: input.resolved_value.clone(),
                default_value: input.default_value.clone(),
                hint: input.hint.clone(),
                required: input.required,
                is_set: input.is_set,
            })
        })
        .collect();

    rows.sort_by(|left, right| {
        (&left.schema_name, &left.key).cmp(&(&right.schema_name, &right.key))
    });

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.schema_name.as_str()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.key.as_str()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.kind))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| row.value.as_deref())
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| {
                        if row.default_value.is_empty() {
                            None
                        } else {
                            Some(row.default_value.as_str())
                        }
                    })
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| row.hint.as_deref())
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.required))
                    .collect::<BooleanArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.is_set))
                    .collect::<BooleanArray>(),
            ),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}

struct CatalogColumn {
    catalog_name: String,
    schema_name: String,
    table_name: String,
    column_name: String,
    data_type: String,
    is_nullable: bool,
    is_virtual: bool,
    is_required_filter: bool,
    filter_mode: Option<String>,
    description: String,
    ordinal_position: usize,
}

fn build_columns_table(
    active_sources: &[RegisteredSource],
    column_fetchers: &[CatalogColumnFetcher],
) -> Result<CoralColumnsTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("is_nullable", DataType::Boolean, false),
        Field::new("is_virtual", DataType::Boolean, false),
        Field::new("is_required_filter", DataType::Boolean, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("filter_mode", DataType::Utf8, true),
        Field::new("catalog_name", DataType::Utf8, false),
    ]));

    let rows = catalog_column_rows(active_sources);
    let static_batch = catalog_columns_batch(schema.clone(), &rows)?;

    Ok(CoralColumnsTable {
        schema,
        static_batch,
        fetchers: column_fetchers.to_vec(),
    })
}

/// `coral.columns`: static source metadata unioned, at scan time, with column
/// metadata fetched lazily from registered database catalogs.
///
/// Recognized pushed-down pins prune which databases are contacted and narrow
/// each remote fetch to the pinned schemas/tables. Pushdown is Inexact, so
/// `DataFusion` re-applies every predicate above the scan: an unrecognized
/// filter shape only costs extra fetching, never correctness.
#[derive(Debug)]
struct CoralColumnsTable {
    schema: Arc<Schema>,
    static_batch: RecordBatch,
    fetchers: Vec<CatalogColumnFetcher>,
}

#[async_trait]
impl TableProvider for CoralColumnsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if column_pin(filter).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pins = ColumnPins::from_filters(filters);
        let inventory_filter = pins.inventory_filter();
        let relevant = self
            .fetchers
            .iter()
            .filter(|fetcher| pins.includes_catalog(&fetcher.catalog_name))
            .collect::<Vec<_>>();
        let outcomes = join_all(
            relevant
                .iter()
                .map(|fetcher| fetcher.fetcher.fetch_columns(&inventory_filter)),
        )
        .await;

        let mut batches = vec![self.static_batch.clone()];
        for (fetcher, outcome) in relevant.iter().zip(outcomes) {
            match outcome {
                Ok(rows) => {
                    let rows = database_catalog_columns(&fetcher.catalog_name, rows);
                    batches.push(catalog_columns_batch(Arc::clone(&self.schema), &rows)?);
                }
                Err(error) => {
                    // Keep metadata browsing available when one of several
                    // databases is unreachable; its rows are simply absent.
                    tracing::warn!(
                        catalog = fetcher.catalog_name.as_str(),
                        detail = %error,
                        "failed to fetch database column metadata; omitting catalog from coral.columns"
                    );
                }
            }
        }

        let table = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
        table.scan(state, projection, &[], limit).await
    }
}

fn database_catalog_columns(
    catalog_name: &str,
    rows: Vec<DatabaseColumnRow>,
) -> Vec<CatalogColumn> {
    rows.into_iter()
        .map(|row| CatalogColumn {
            catalog_name: catalog_name.to_string(),
            schema_name: row.schema_name,
            table_name: row.table_name,
            column_name: row.column_name,
            data_type: row.data_type,
            is_nullable: row.is_nullable,
            is_virtual: false,
            is_required_filter: false,
            filter_mode: None,
            description: String::new(),
            ordinal_position: usize::try_from(row.ordinal_position).unwrap_or_default(),
        })
        .collect()
}

/// Per-column value restrictions recognized in pushed-down predicates.
/// `None` means the column is unrestricted.
#[derive(Debug, Default, PartialEq, Eq)]
struct ColumnPins {
    catalogs: Option<BTreeSet<String>>,
    schemas: Option<BTreeSet<String>>,
    tables: Option<BTreeSet<String>>,
}

impl ColumnPins {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut pins = Self::default();
        for filter in filters {
            if let Some((column, values)) = column_pin(filter) {
                pins.restrict(column, values);
            }
        }
        pins
    }

    fn restrict(&mut self, column: PinColumn, values: BTreeSet<String>) {
        let slot = match column {
            PinColumn::Catalog => &mut self.catalogs,
            PinColumn::Schema => &mut self.schemas,
            PinColumn::Table => &mut self.tables,
        };
        *slot = Some(match slot.take() {
            None => values,
            Some(existing) => existing.intersection(&values).cloned().collect(),
        });
    }

    fn includes_catalog(&self, catalog_name: &str) -> bool {
        self.catalogs
            .as_ref()
            .is_none_or(|catalogs| catalogs.contains(catalog_name))
    }

    fn inventory_filter(&self) -> ColumnInventoryFilter {
        ColumnInventoryFilter {
            schemas: self
                .schemas
                .as_ref()
                .map(|values| values.iter().cloned().collect()),
            tables: self
                .tables
                .as_ref()
                .map(|values| values.iter().cloned().collect()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PinColumn {
    Catalog,
    Schema,
    Table,
}

fn pin_column(name: &str) -> Option<PinColumn> {
    match name {
        "catalog_name" => Some(PinColumn::Catalog),
        "schema_name" => Some(PinColumn::Schema),
        "table_name" => Some(PinColumn::Table),
        _ => None,
    }
}

/// Recognizes one pushed-down conjunct as a single-column string restriction:
/// `col = 'x'`, `col IN (...)`, or an OR chain of same-column equalities.
fn column_pin(expr: &Expr) -> Option<(PinColumn, BTreeSet<String>)> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let (column, value) = column_equality(binary.left.as_ref(), binary.right.as_ref())
                .or_else(|| column_equality(binary.right.as_ref(), binary.left.as_ref()))?;
            Some((column, BTreeSet::from([value])))
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let (column, mut values) = column_pin(binary.left.as_ref())?;
            let (right_column, right_values) = column_pin(binary.right.as_ref())?;
            if column != right_column {
                return None;
            }
            values.extend(right_values);
            Some((column, values))
        }
        Expr::InList(in_list) if !in_list.negated => {
            let Expr::Column(column) = in_list.expr.as_ref() else {
                return None;
            };
            let column = pin_column(column.name())?;
            let values = in_list
                .list
                .iter()
                .map(literal_to_string)
                .collect::<Option<BTreeSet<_>>>()?;
            Some((column, values))
        }
        _ => None,
    }
}

fn column_equality(column_side: &Expr, literal_side: &Expr) -> Option<(PinColumn, String)> {
    let Expr::Column(column) = column_side else {
        return None;
    };
    let column = pin_column(column.name())?;
    Some((column, literal_to_string(literal_side)?))
}

fn catalog_column_rows(active_sources: &[RegisteredSource]) -> Vec<CatalogColumn> {
    let mut rows = system_catalog_column_rows();
    rows.extend(source_catalog_column_rows(active_sources));
    rows.sort_by(|left, right| {
        (
            &left.catalog_name,
            &left.schema_name,
            &left.table_name,
            left.ordinal_position,
        )
            .cmp(&(
                &right.catalog_name,
                &right.schema_name,
                &right.table_name,
                right.ordinal_position,
            ))
    });
    rows
}

fn system_catalog_column_rows() -> Vec<CatalogColumn> {
    SYSTEM_TABLE_DEFINITIONS
        .iter()
        .flat_map(|table| {
            table
                .columns
                .iter()
                .enumerate()
                .map(move |(position, column)| CatalogColumn {
                    catalog_name: String::new(),
                    schema_name: SYSTEM_SCHEMA.to_string(),
                    table_name: table.table_name.to_string(),
                    column_name: column.name.to_string(),
                    data_type: column.data_type.to_string(),
                    is_nullable: column.nullable,
                    is_virtual: false,
                    is_required_filter: false,
                    filter_mode: None,
                    description: column.description.to_string(),
                    ordinal_position: position,
                })
        })
        .collect()
}

fn source_catalog_column_rows(active_sources: &[RegisteredSource]) -> Vec<CatalogColumn> {
    active_sources
        .iter()
        .flat_map(|source| {
            source.tables.iter().flat_map(move |table| {
                let catalog_name = source.catalog_name.clone().unwrap_or_default();
                let schema_name = table
                    .schema_name
                    .clone()
                    .unwrap_or_else(|| source.schema_name.clone());
                table
                    .columns
                    .iter()
                    .enumerate()
                    .map(move |(position, column)| CatalogColumn {
                        catalog_name: catalog_name.clone(),
                        schema_name: schema_name.clone(),
                        table_name: table.table_name.clone(),
                        column_name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        is_nullable: column.nullable,
                        is_virtual: column.is_virtual,
                        is_required_filter: column.is_required_filter,
                        filter_mode: column.filter_mode.clone(),
                        description: column.description.clone(),
                        ordinal_position: position,
                    })
            })
        })
        .collect()
}

fn catalog_columns_batch(schema: Arc<Schema>, rows: &[CatalogColumn]) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.schema_name.as_str()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.table_name.as_str()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(i32::try_from(row.ordinal_position).unwrap_or_default()))
                    .collect::<Int32Array>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.column_name.as_str()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.data_type.as_str()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.is_nullable))
                    .collect::<BooleanArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.is_virtual))
                    .collect::<BooleanArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.is_required_filter))
                    .collect::<BooleanArray>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.description.as_str()))
                    .collect::<StringArray>(),
            ),
            utf8_column(rows.iter().map(|row| row.filter_mode.as_deref())),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.catalog_name.as_str()))
                    .collect::<StringArray>(),
            ),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use datafusion::prelude::{col, lit};

    use crate::backends::common::test_support::StubSourceFunctionFactory;
    use crate::backends::{RegisteredSource, RegisteredTableFunction};

    use super::collect_table_functions;
    use super::*;

    fn pins(filters: &[Expr]) -> ColumnPins {
        ColumnPins::from_filters(filters)
    }

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn column_pin_recognizes_equality_on_either_side() {
        let left = pins(&[col("catalog_name").eq(lit("db"))]);
        let right = pins(&[lit("db").eq(col("catalog_name"))]);
        assert_eq!(left.catalogs, Some(set(&["db"])));
        assert_eq!(left, right);
        assert_eq!(left.schemas, None);
        assert_eq!(left.tables, None);
    }

    #[test]
    fn column_pin_recognizes_in_list_and_or_chains() {
        let in_list = pins(&[col("table_name").in_list(vec![lit("users"), lit("orders")], false)]);
        assert_eq!(in_list.tables, Some(set(&["orders", "users"])));

        let or_chain = pins(&[col("schema_name")
            .eq(lit("main"))
            .or(col("schema_name").eq(lit("sales")))]);
        assert_eq!(or_chain.schemas, Some(set(&["main", "sales"])));
    }

    #[test]
    fn column_pin_conjuncts_intersect_per_column() {
        let intersected = pins(&[
            col("table_name").in_list(vec![lit("users"), lit("orders")], false),
            col("table_name").eq(lit("users")),
        ]);
        assert_eq!(intersected.tables, Some(set(&["users"])));
    }

    #[test]
    fn column_pin_rejects_unprunable_shapes() {
        let unpinned = pins(&[
            // Disjunction across different columns restricts neither column.
            col("catalog_name")
                .eq(lit("db"))
                .or(col("schema_name").eq(lit("main"))),
            // Negated membership is not a value restriction.
            col("table_name").in_list(vec![lit("users")], true),
            // Unknown column.
            col("column_name").eq(lit("id")),
            // Column-to-column comparison has no literal to pin.
            col("table_name").eq(col("schema_name")),
        ]);
        assert_eq!(unpinned, ColumnPins::default());
    }

    #[derive(Debug, Default)]
    struct StubColumnFetcher {
        rows: Vec<DatabaseColumnRow>,
        calls: Mutex<Vec<ColumnInventoryFilter>>,
    }

    #[async_trait]
    impl crate::backends::DatabaseColumnFetcher for StubColumnFetcher {
        async fn fetch_columns(
            &self,
            filter: &ColumnInventoryFilter,
        ) -> Result<Vec<DatabaseColumnRow>> {
            self.calls.lock().expect("stub lock").push(filter.clone());
            Ok(self.rows.clone())
        }
    }

    fn stub_row(table_name: &str, column_name: &str) -> DatabaseColumnRow {
        DatabaseColumnRow {
            schema_name: "main".to_string(),
            table_name: table_name.to_string(),
            ordinal_position: 1,
            column_name: column_name.to_string(),
            data_type: "integer".to_string(),
            is_nullable: false,
        }
    }

    fn columns_ctx(fetchers: &[CatalogColumnFetcher]) -> SessionContext {
        let ctx = SessionContext::new();
        let table = build_columns_table(&[], fetchers).expect("columns table");
        ctx.register_table("columns", Arc::new(table))
            .expect("register columns table");
        ctx
    }

    #[tokio::test]
    async fn columns_scan_prunes_fetchers_and_narrows_remote_fetch() {
        let pinned = Arc::new(StubColumnFetcher {
            rows: vec![stub_row("users", "id")],
            calls: Mutex::default(),
        });
        let pruned = Arc::new(StubColumnFetcher::default());
        let ctx = columns_ctx(&[
            CatalogColumnFetcher {
                catalog_name: "db1".to_string(),
                fetcher: Arc::clone(&pinned) as Arc<dyn crate::backends::DatabaseColumnFetcher>,
            },
            CatalogColumnFetcher {
                catalog_name: "db2".to_string(),
                fetcher: Arc::clone(&pruned) as Arc<dyn crate::backends::DatabaseColumnFetcher>,
            },
        ]);

        let batches = ctx
            .sql(
                "SELECT column_name FROM columns \
                 WHERE catalog_name = 'db1' AND schema_name = 'main' AND table_name = 'users'",
            )
            .await
            .expect("plan pinned query")
            .collect()
            .await
            .expect("run pinned query");
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        assert_eq!(rows, 1, "pinned scan returns the stubbed users.id row");

        let calls = pinned.calls.lock().expect("stub lock");
        assert_eq!(calls.len(), 1, "pinned catalog fetches exactly once");
        let filter = calls.first().expect("recorded filter");
        assert_eq!(
            filter.schemas.as_deref(),
            Some(["main".to_string()].as_slice())
        );
        assert_eq!(
            filter.tables.as_deref(),
            Some(["users".to_string()].as_slice())
        );
        assert!(
            pruned.calls.lock().expect("stub lock").is_empty(),
            "catalog pin must prevent contacting other databases"
        );
    }

    #[tokio::test]
    async fn columns_scan_without_pins_fetches_all_catalogs_unfiltered() {
        let first = Arc::new(StubColumnFetcher {
            rows: vec![stub_row("users", "id")],
            calls: Mutex::default(),
        });
        let second = Arc::new(StubColumnFetcher {
            rows: vec![stub_row("orders", "order_id")],
            calls: Mutex::default(),
        });
        let ctx = columns_ctx(&[
            CatalogColumnFetcher {
                catalog_name: "db1".to_string(),
                fetcher: Arc::clone(&first) as Arc<dyn crate::backends::DatabaseColumnFetcher>,
            },
            CatalogColumnFetcher {
                catalog_name: "db2".to_string(),
                fetcher: Arc::clone(&second) as Arc<dyn crate::backends::DatabaseColumnFetcher>,
            },
        ]);

        // `<>` is deliberately not a recognizable pin: the scan stays
        // unpruned while the predicate hides the static system-table rows.
        let batches = ctx
            .sql("SELECT column_name FROM columns WHERE catalog_name <> '' ORDER BY column_name")
            .await
            .expect("plan unpinned query")
            .collect()
            .await
            .expect("run unpinned query");
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        assert_eq!(rows, 2, "both catalogs contribute rows");

        for fetcher in [&first, &second] {
            let calls = fetcher.calls.lock().expect("stub lock");
            assert_eq!(calls.len(), 1, "each catalog fetches exactly once");
            let filter = calls.first().expect("recorded filter");
            assert!(filter.schemas.is_none() && filter.tables.is_none());
        }
    }

    #[test]
    fn collect_table_functions_preserves_registered_function_schema() {
        let functions = collect_table_functions(&[RegisteredSource {
            catalog_name: None,
            schema_name: "source_schema".to_string(),
            tables: Vec::new(),
            table_functions: vec![RegisteredTableFunction {
                schema_name: "function_schema".to_string(),
                function_name: "search".to_string(),
                factory: Arc::new(StubSourceFunctionFactory::default()),
                kind: "search".to_string(),
                description: String::new(),
                arguments: Vec::new(),
                result_columns: Vec::new(),
                search_limits_json: None,
            }],
            inputs: Vec::new(),
        }]);

        assert_eq!(functions.len(), 1);
        assert_eq!(
            functions
                .first()
                .map(|function| function.schema_name.as_str()),
            Some("function_schema")
        );
    }
}
