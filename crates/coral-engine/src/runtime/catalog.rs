//! Registers the `coral` system schema for discoverable source metadata.

use std::collections::HashMap;
use std::sync::Arc;

use coral_spec::ManifestInputKind;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::{MemTable, ViewTable};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use serde::Serialize;

use crate::backends::common::{
    RegisteredTableFunctionArgument, RegisteredTableFunctionResultColumn,
};
use crate::backends::{RegisteredSource, RegisteredTableFunction};
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{
    ColumnInfo, TableFunctionArgumentInfo, TableFunctionInfo, TableFunctionResultColumnInfo,
    TableInfo,
};

/// Schema name for source metadata tables such as `coral.tables`.
pub(crate) const SYSTEM_SCHEMA: &str = "coral";
const STATIC_TABLES_TABLE: &str = "_tables_static";
const STATIC_COLUMNS_TABLE: &str = "_columns_static";

/// Register `coral.tables` and `coral.columns` for the active source set.
///
/// # Errors
///
/// Returns a `DataFusionError` if the catalog is missing or the metadata
/// tables cannot be materialized.
pub(crate) async fn register(
    ctx: &SessionContext,
    active_sources: &[RegisteredSource],
) -> Result<()> {
    let tables_table = build_tables_table(active_sources)?;
    let columns_table = build_columns_table(active_sources)?;
    let filters_table = build_filters_table(active_sources)?;
    let inputs_table = build_inputs_table(active_sources)?;
    let table_functions_table = build_table_functions_table(active_sources)?;
    let catalog_names = active_sources
        .iter()
        .filter_map(|source| source.catalog_name.as_deref())
        .collect::<Vec<_>>();

    let mut meta_tables: HashMap<String, Arc<dyn datafusion::datasource::TableProvider>> =
        HashMap::new();
    meta_tables.insert(STATIC_TABLES_TABLE.to_string(), Arc::new(tables_table));
    meta_tables.insert(STATIC_COLUMNS_TABLE.to_string(), Arc::new(columns_table));
    meta_tables.insert("filters".to_string(), Arc::new(filters_table));
    meta_tables.insert("inputs".to_string(), Arc::new(inputs_table));
    meta_tables.insert(
        "table_functions".to_string(),
        Arc::new(table_functions_table),
    );

    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("catalog 'datafusion' not found".to_string()))?;
    let planning_schema = Arc::new(MemorySchemaProvider::new());
    for (name, table) in &meta_tables {
        planning_schema.register_table(name.clone(), table.clone())?;
    }
    catalog.register_schema(SYSTEM_SCHEMA, planning_schema)?;

    let tables_sql = tables_view_sql();
    let tables_view = view_table_for_sql(ctx, &tables_sql).await?;
    meta_tables.insert("tables".to_string(), Arc::new(tables_view));

    let columns_sql = columns_view_sql(&catalog_names);
    let columns_view = view_table_for_sql(ctx, &columns_sql).await?;
    meta_tables.insert("columns".to_string(), Arc::new(columns_view));

    catalog.register_schema(
        SYSTEM_SCHEMA,
        Arc::new(StaticSchemaProvider::new(meta_tables)),
    )?;

    Ok(())
}

async fn view_table_for_sql(ctx: &SessionContext, sql: &str) -> Result<ViewTable> {
    let df = ctx.sql(sql).await?;
    let (_state, plan) = df.into_parts();
    Ok(ViewTable::new(plan, Some(sql.to_string())))
}

fn tables_view_sql() -> String {
    format!(
        "SELECT schema_name, table_name, description, guide, required_filters, \
         search_limits_json, catalog_name FROM {SYSTEM_SCHEMA}.{STATIC_TABLES_TABLE}"
    )
}

fn columns_view_sql(catalog_names: &[&str]) -> String {
    let static_sql = format!(
        "SELECT schema_name, table_name, ordinal_position, column_name, data_type, \
         is_nullable, is_virtual, is_required_filter, description, filter_mode, catalog_name \
         FROM {SYSTEM_SCHEMA}.{STATIC_COLUMNS_TABLE}"
    );
    if catalog_names.is_empty() {
        return static_sql;
    }
    format!(
        "{static_sql} UNION ALL \
         SELECT table_schema AS schema_name, table_name, \
         CAST(ordinal_position AS INT) AS ordinal_position, column_name, data_type, \
         is_nullable = 'YES' AS is_nullable, false AS is_virtual, \
         false AS is_required_filter, '' AS description, '' AS filter_mode, \
         table_catalog AS catalog_name \
         FROM information_schema.columns \
         WHERE table_catalog IN ({}) AND table_schema <> 'information_schema'",
        sql_string_list(catalog_names)
    )
}

fn sql_string_list(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| sql_string_literal(value))
        .collect::<Vec<_>>()
        .join(", ")
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
        description: "Column data type rendered in Arrow/DataFusion string form.",
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

fn build_columns_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
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
    let batch = catalog_columns_batch(schema.clone(), &rows)?;

    MemTable::try_new(schema, vec![vec![batch]])
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
    use std::sync::Arc;

    use crate::backends::common::test_support::StubSourceFunctionFactory;
    use crate::backends::{RegisteredSource, RegisteredTableFunction};

    use super::collect_table_functions;

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
