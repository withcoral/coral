//! Registers the `coral` system schema for discoverable source metadata.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coral_spec::{ManifestInputKind, SearchLimitsSpec, SourceTableFunctionKind};
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::future::join_all;
use serde::Serialize;

use crate::backends::shared::filter_expr::literal_to_string;
use crate::backends::{
    CatalogColumnFetcher, ColumnInventoryFilter, DatabaseColumnRow, RegisteredSource,
    RegisteredTable, SourceQualifiedName,
};
use crate::runtime::normalize_catalog_name;
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{
    ColumnInfo, TableFunctionArgumentInfo, TableFunctionInfo, TableFunctionResultColumnInfo,
    TableInfo,
};

/// Schema name for source metadata tables such as `coral.tables`.
pub(crate) const SYSTEM_SCHEMA: &str = "coral";

/// Per-source budget for query-time database column inventory. Fetches run
/// concurrently, so total scan latency is bounded by this duration rather than
/// multiplying by the number of database sources.
const DATABASE_COLUMN_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Catalog-only metadata for one SQL table-function surface.
#[derive(Debug, Clone)]
pub(crate) struct CatalogTableFunction {
    pub(crate) schema_name: String,
    pub(crate) function_name: String,
    pub(crate) description: String,
    pub(crate) guide: String,
    pub(crate) require_guide_read: bool,
    pub(crate) arguments: Vec<CatalogTableFunctionArgument>,
    pub(crate) result_columns: Vec<CatalogTableFunctionResultColumn>,
    pub(crate) kind: SourceTableFunctionKind,
    pub(crate) search_limits: Option<SearchLimitsSpec>,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogTableFunctionArgument {
    pub(crate) name: String,
    pub(crate) required: bool,
    pub(crate) values: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogTableFunctionResultColumn {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) description: String,
}

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
    catalog_only_table_functions: &[CatalogTableFunction],
) -> Result<()> {
    let tables_table = build_tables_table(active_sources)?;
    let columns_table = build_columns_table(active_sources, column_fetchers)?;
    let filters_table = build_filters_table(active_sources)?;
    let inputs_table = build_inputs_table(active_sources)?;
    let table_functions_table =
        build_table_functions_table(active_sources, catalog_only_table_functions)?;

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

fn build_table_functions_table(
    active_sources: &[RegisteredSource],
    catalog_only_table_functions: &[CatalogTableFunction],
) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("function_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("arguments_json", DataType::Utf8, false),
        Field::new("result_columns_json", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("search_limits_json", DataType::Utf8, true),
        Field::new("guide", DataType::Utf8, false),
    ]));

    let rows = catalog_table_functions(active_sources, catalog_only_table_functions);

    let arguments_json = rows
        .iter()
        .map(table_function_arguments_json)
        .collect::<Result<Vec<_>>>()?;
    let result_columns_json = rows
        .iter()
        .map(table_function_result_columns_json)
        .collect::<Result<Vec<_>>>()?;
    let search_limits_json = rows
        .iter()
        .map(|row| search_limits_json(row.search_limits.as_ref()))
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
            utf8_column(search_limits_json.iter().map(|value| value.as_deref())),
            utf8_column(rows.iter().map(|row| Some(row.guide.as_str()))),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}

fn table_function_arguments_json(row: &CatalogTableFunction) -> Result<String> {
    let arguments = row
        .arguments
        .iter()
        .map(TableFunctionArgumentJson::from)
        .collect::<Vec<_>>();
    serde_json::to_string(&arguments).map_err(|error| DataFusionError::External(Box::new(error)))
}

fn table_function_result_columns_json(row: &CatalogTableFunction) -> Result<String> {
    let columns = row
        .result_columns
        .iter()
        .map(TableFunctionResultColumnJson::from)
        .collect::<Vec<_>>();
    serde_json::to_string(&columns).map_err(|error| DataFusionError::External(Box::new(error)))
}

fn search_limits_json(limits: Option<&SearchLimitsSpec>) -> Result<Option<String>> {
    limits
        .map(|limits| {
            serde_json::to_string(limits)
                .map_err(|error| DataFusionError::External(Box::new(error)))
        })
        .transpose()
}

#[derive(Serialize)]
struct TableFunctionArgumentJson<'a> {
    name: &'a str,
    required: bool,
    values: &'a [String],
}

impl<'a> From<&'a CatalogTableFunctionArgument> for TableFunctionArgumentJson<'a> {
    fn from(argument: &'a CatalogTableFunctionArgument) -> Self {
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

impl<'a> From<&'a CatalogTableFunctionResultColumn> for TableFunctionResultColumnJson<'a> {
    fn from(column: &'a CatalogTableFunctionResultColumn) -> Self {
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
        name: "require_guide_read",
        data_type: "Boolean",
        nullable: false,
        description: "Whether the table guide must be read before querying the table.",
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
    SystemColumnDefinition {
        name: "catalog_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL catalog containing the filtered table. Empty for tables queried as schema_name.table_name.",
    },
];

const INPUTS_COLUMNS: &[SystemColumnDefinition] = &[
    SystemColumnDefinition {
        name: "schema_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL schema of the source that declares the input. Empty for database sources, which are addressed by catalog_name.",
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
    SystemColumnDefinition {
        name: "catalog_name",
        data_type: "Utf8",
        nullable: false,
        description: "SQL catalog of the database source that declares the input. Empty for sources addressed by schema_name.",
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
    SystemColumnDefinition {
        name: "guide",
        data_type: "Utf8",
        nullable: false,
        description: "User-facing query guidance for the table function.",
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
        description: "Metadata for Coral table functions.",
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
            catalog_name: None,
            schema_name: SYSTEM_SCHEMA.to_string(),
            table_name: table.table_name.to_string(),
            description: table.description.to_string(),
            guide: table.guide.to_string(),
            require_guide_read: false,
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
#[must_use]
pub(crate) fn collect_static_tables(active_sources: &[RegisteredSource]) -> Vec<TableInfo> {
    let mut tables = system_table_infos();
    tables.extend(active_sources.iter().flat_map(|source| {
        source.tables.iter().map(move |table| TableInfo {
            catalog_name: source
                .qualified_name
                .catalog_name()
                .map(ToString::to_string),
            schema_name: registered_table_schema_name(source, table),
            table_name: table.table_name.clone(),
            description: table.description.clone(),
            guide: table.guide.clone(),
            require_guide_read: table.require_guide_read,
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
    let mut tables = collect_table_metadata(ctx, catalog_filter, schema_filter, table_filter)
        .await?
        .into_iter()
        .map(|table| {
            (
                (
                    table.catalog_name.clone(),
                    table.schema_name.clone(),
                    table.table_name.clone(),
                ),
                table,
            )
        })
        .collect::<HashMap<_, _>>();

    let sql = catalog_columns_query(catalog_filter, schema_filter, table_filter);
    let batches = ctx.sql(&sql).await?.collect().await?;
    apply_column_infos_from_batches(&mut tables, &batches)?;
    let mut tables = tables.into_values().collect::<Vec<_>>();
    sort_tables(&mut tables);
    for table in &mut tables {
        table.columns.sort_by_key(|column| column.ordinal_position);
    }
    Ok(tables)
}

/// Collect table metadata without forcing lazy column discovery.
pub(crate) async fn collect_table_metadata(
    ctx: &SessionContext,
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Result<Vec<TableInfo>> {
    let sql = catalog_tables_query(catalog_filter, schema_filter, table_filter);
    let batches = ctx.sql(&sql).await?.collect().await?;
    let mut tables = collect_table_infos_from_batches(&batches)?;
    sort_tables(&mut tables);
    Ok(tables)
}

fn catalog_tables_query(
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
) -> String {
    let mut sql = "SELECT catalog_name, schema_name, table_name, description, guide, \
                   require_guide_read, required_filters FROM coral.tables"
        .to_string();
    append_catalog_filter(&mut sql, catalog_filter, schema_filter, table_filter);
    sql
}

fn catalog_columns_query(
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
) -> String {
    let mut sql = "SELECT catalog_name, schema_name, table_name, ordinal_position, column_name, \
                   data_type, is_nullable, is_virtual, is_required_filter, description \
                   FROM coral.columns"
        .to_string();
    append_catalog_filter(&mut sql, catalog_filter, schema_filter, table_filter);
    sql
}

fn append_catalog_filter(
    sql: &mut String,
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
) {
    let mut predicates = Vec::new();
    if let Some(value) = catalog_filter {
        let value = normalize_catalog_name(Some(value)).unwrap_or_default();
        predicates.push(format!("catalog_name = {}", sql_string_literal(value)));
    }
    if let Some(value) = schema_filter {
        predicates.push(format!("schema_name = {}", sql_string_literal(value)));
    }
    if let Some(value) = table_filter {
        predicates.push(format!("table_name = {}", sql_string_literal(value)));
    }
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
}

fn collect_table_infos_from_batches(batches: &[RecordBatch]) -> Result<Vec<TableInfo>> {
    let mut tables = Vec::new();
    for batch in batches {
        let catalog_names = string_array(batch, "catalog_name")?;
        let schema_names = string_array(batch, "schema_name")?;
        let table_names = string_array(batch, "table_name")?;
        let descriptions = string_array(batch, "description")?;
        let guides = string_array(batch, "guide")?;
        let require_guide_reads = bool_array(batch, "require_guide_read")?;
        let required_filters = string_array(batch, "required_filters")?;
        for row in 0..batch.num_rows() {
            tables.push(TableInfo {
                catalog_name: optional_catalog_name(catalog_names.value(row)),
                schema_name: schema_names.value(row).to_string(),
                table_name: table_names.value(row).to_string(),
                description: descriptions.value(row).to_string(),
                guide: guides.value(row).to_string(),
                require_guide_read: require_guide_reads.value(row),
                columns: Vec::new(),
                required_filters: split_required_filters(required_filters.value(row)),
            });
        }
    }
    Ok(tables)
}

fn optional_catalog_name(catalog_name: &str) -> Option<String> {
    (!catalog_name.is_empty()).then(|| catalog_name.to_string())
}

type TableInfoByKey = HashMap<(Option<String>, String, String), TableInfo>;

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
                optional_catalog_name(catalog_names.value(row)),
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

fn sort_tables(tables: &mut [TableInfo]) {
    tables.sort_by(|left, right| {
        (&left.catalog_name, &left.schema_name, &left.table_name).cmp(&(
            &right.catalog_name,
            &right.schema_name,
            &right.table_name,
        ))
    });
}

fn registered_table_schema_name(source: &RegisteredSource, table: &RegisteredTable) -> String {
    match (&source.qualified_name, &table.schema_name) {
        (_, Some(schema_name)) | (SourceQualifiedName::Schema(schema_name), None) => {
            schema_name.clone()
        }
        (SourceQualifiedName::Catalog(catalog_name), None) => {
            debug_assert!(
                false,
                "catalog-backed table '{}.{}' must record its SQL schema",
                catalog_name, table.table_name
            );
            catalog_name.clone()
        }
    }
}

/// Collect typed table function metadata for the active runtime.
#[must_use]
pub(crate) fn collect_table_functions(
    active_sources: &[RegisteredSource],
    catalog_only_table_functions: &[CatalogTableFunction],
) -> Vec<TableFunctionInfo> {
    catalog_table_functions(active_sources, catalog_only_table_functions)
        .into_iter()
        .map(|function| TableFunctionInfo {
            schema_name: function.schema_name,
            function_name: function.function_name,
            description: function.description,
            guide: function.guide,
            require_guide_read: function.require_guide_read,
            arguments: function
                .arguments
                .into_iter()
                .map(|argument| TableFunctionArgumentInfo {
                    name: argument.name,
                    required: argument.required,
                    values: argument.values,
                })
                .collect(),
            result_columns: function
                .result_columns
                .into_iter()
                .map(|column| TableFunctionResultColumnInfo {
                    name: column.name,
                    data_type: column.data_type,
                    nullable: column.nullable,
                    description: column.description,
                })
                .collect(),
            kind: function.kind,
            search_limits: function.search_limits,
        })
        .collect()
}

fn catalog_table_functions(
    active_sources: &[RegisteredSource],
    catalog_only_table_functions: &[CatalogTableFunction],
) -> Vec<CatalogTableFunction> {
    let mut functions = active_sources
        .iter()
        .flat_map(|source| {
            source
                .table_functions
                .iter()
                .map(|function| CatalogTableFunction {
                    schema_name: function.schema_name.clone(),
                    function_name: function.function_name.clone(),
                    description: function.description.clone(),
                    guide: function.guide.clone(),
                    require_guide_read: function.require_guide_read,
                    arguments: function
                        .arguments
                        .iter()
                        .map(|argument| CatalogTableFunctionArgument {
                            name: argument.name.clone(),
                            required: argument.required,
                            values: argument.values.clone(),
                        })
                        .collect(),
                    result_columns: function
                        .result_columns
                        .iter()
                        .map(|column| CatalogTableFunctionResultColumn {
                            name: column.name.clone(),
                            data_type: column.data_type.clone(),
                            nullable: column.nullable,
                            description: column.description.clone(),
                        })
                        .collect(),
                    kind: function.kind,
                    search_limits: function.search_limits.clone(),
                })
        })
        .chain(catalog_only_table_functions.iter().cloned())
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
    require_guide_read: bool,
    required_filters: String,
    search_limits: Option<SearchLimitsSpec>,
}

fn build_tables_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("guide", DataType::Utf8, false),
        Field::new("require_guide_read", DataType::Boolean, false),
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
            require_guide_read: false,
            required_filters: String::new(),
            search_limits: None,
        })
        .chain(active_sources.iter().flat_map(|source| {
            source.tables.iter().map(move |table| CatalogTable {
                catalog_name: source
                    .qualified_name
                    .catalog_name()
                    .unwrap_or_default()
                    .to_string(),
                schema_name: registered_table_schema_name(source, table),
                table_name: table.table_name.clone(),
                description: table.description.clone(),
                guide: table.guide.clone(),
                require_guide_read: table.require_guide_read,
                required_filters: table.required_filters.join(","),
                search_limits: table.search_limits.clone(),
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

    let search_limits_json = rows
        .iter()
        .map(|row| search_limits_json(row.search_limits.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            utf8_column(rows.iter().map(|row| Some(row.schema_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.table_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.description.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.guide.as_str()))),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.require_guide_read))
                    .collect::<BooleanArray>(),
            ),
            utf8_column(rows.iter().map(|row| Some(row.required_filters.as_str()))),
            utf8_column(search_limits_json.iter().map(|value| value.as_deref())),
            utf8_column(rows.iter().map(|row| Some(row.catalog_name.as_str()))),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}

struct CatalogFilter {
    catalog_name: String,
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
        Field::new("catalog_name", DataType::Utf8, false),
    ]));

    let rows = catalog_filter_rows(active_sources);

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
            utf8_column(rows.iter().map(|row| Some(row.catalog_name.as_str()))),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}

fn catalog_filter_rows(active_sources: &[RegisteredSource]) -> Vec<CatalogFilter> {
    let mut rows = active_sources
        .iter()
        .flat_map(|source| {
            source.tables.iter().flat_map(move |table| {
                table.filters.iter().map(move |filter| CatalogFilter {
                    catalog_name: source
                        .qualified_name
                        .catalog_name()
                        .unwrap_or_default()
                        .to_string(),
                    schema_name: registered_table_schema_name(source, table),
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
        (
            &left.catalog_name,
            &left.schema_name,
            &left.table_name,
            &left.filter_name,
        )
            .cmp(&(
                &right.catalog_name,
                &right.schema_name,
                &right.table_name,
                &right.filter_name,
            ))
    });
    rows
}

struct CatalogInput {
    schema_name: String,
    catalog_name: String,
    key: String,
    kind: &'static str,
    value: Option<String>,
    /// Empty string (= "no default declared" in the spec) renders as SQL NULL.
    default_value: String,
    hint: Option<String>,
    required: bool,
    is_set: bool,
}

fn catalog_input_rows(active_sources: &[RegisteredSource]) -> Vec<CatalogInput> {
    let mut rows: Vec<CatalogInput> = active_sources
        .iter()
        .flat_map(|source| {
            source.inputs.iter().map(move |input| CatalogInput {
                schema_name: match &source.qualified_name {
                    SourceQualifiedName::Schema(name) => name.clone(),
                    SourceQualifiedName::Catalog(_) => String::new(),
                },
                catalog_name: source
                    .qualified_name
                    .catalog_name()
                    .unwrap_or_default()
                    .to_string(),
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
        (&left.catalog_name, &left.schema_name, &left.key).cmp(&(
            &right.catalog_name,
            &right.schema_name,
            &right.key,
        ))
    });
    rows
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
        Field::new("catalog_name", DataType::Utf8, false),
    ]));

    let rows = catalog_input_rows(active_sources);

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
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.catalog_name.as_str()))
                    .collect::<StringArray>(),
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
        fetch_timeout: DATABASE_COLUMN_FETCH_TIMEOUT,
    })
}

#[derive(Debug)]
struct CoralColumnsTable {
    schema: Arc<Schema>,
    static_batch: RecordBatch,
    fetchers: Vec<CatalogColumnFetcher>,
    fetch_timeout: Duration,
}

#[async_trait]
impl TableProvider for CoralColumnsTable {
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
            .filter(|fetcher| {
                pins.includes_catalog(&fetcher.catalog_name)
                    && ColumnPins::pin_intersects(pins.schemas.as_ref(), &fetcher.schema_names)
                    && ColumnPins::pin_intersects(pins.tables.as_ref(), &fetcher.table_names)
            })
            .collect::<Vec<_>>();
        let outcomes = join_all(relevant.iter().map(|fetcher| async {
            tokio::time::timeout(
                self.fetch_timeout,
                fetcher.fetcher.fetch_columns(&inventory_filter),
            )
            .await
            .map_err(|_elapsed| {
                DataFusionError::Execution(format!(
                    "database column metadata fetch timed out after {:?}",
                    self.fetch_timeout
                ))
            })?
        }))
        .await;

        let mut batches = vec![self.static_batch.clone()];
        for (fetcher, outcome) in relevant.iter().zip(outcomes) {
            match outcome {
                Ok(rows) => {
                    let rows = database_catalog_columns(&fetcher.catalog_name, rows);
                    batches.push(catalog_columns_batch(Arc::clone(&self.schema), &rows)?);
                }
                Err(error) => {
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

    fn pin_intersects(pinned: Option<&BTreeSet<String>>, known: &BTreeSet<String>) -> bool {
        pinned.is_none_or(|values| values.iter().any(|value| known.contains(value)))
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
    Some((pin_column(column.name())?, literal_to_string(literal_side)?))
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
                let catalog_name = source
                    .qualified_name
                    .catalog_name()
                    .unwrap_or_default()
                    .to_string();
                let schema_name = registered_table_schema_name(source, table);
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
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use coral_spec::ManifestInputKind;
    use datafusion::datasource::TableProvider as _;
    use datafusion::error::DataFusionError;
    use datafusion::prelude::{SessionContext, col, lit};

    use crate::backends::common::{
        RegisteredColumn, RegisteredFilter, test_support::StubSourceFunctionFactory,
    };
    use crate::backends::{
        CatalogColumnFetcher, ColumnInventoryFilter, DatabaseColumnFetcher, DatabaseColumnRow,
        RegisteredInput, RegisteredSource, RegisteredTable, RegisteredTableFunction,
        SourceQualifiedName,
    };

    use super::{
        ColumnPins, PinColumn, build_columns_table, catalog_filter_rows, catalog_input_rows,
        collect_static_tables, collect_table_functions, column_pin, source_catalog_column_rows,
    };

    #[derive(Clone, Debug)]
    enum FetchOutcome {
        Rows(Vec<DatabaseColumnRow>),
        Error,
        Pending,
    }

    #[derive(Debug)]
    struct RecordingFetcher {
        calls: Mutex<Vec<ColumnInventoryFilter>>,
        outcome: FetchOutcome,
    }

    impl RecordingFetcher {
        fn new(outcome: FetchOutcome) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                outcome,
            })
        }

        fn calls(&self) -> Vec<ColumnInventoryFilter> {
            self.calls.lock().expect("fetch calls lock").clone()
        }
    }

    #[async_trait]
    impl DatabaseColumnFetcher for RecordingFetcher {
        async fn fetch_columns(
            &self,
            filter: &ColumnInventoryFilter,
        ) -> datafusion::error::Result<Vec<DatabaseColumnRow>> {
            self.calls
                .lock()
                .expect("fetch calls lock")
                .push(filter.clone());
            match &self.outcome {
                FetchOutcome::Rows(rows) => Ok(rows.clone()),
                FetchOutcome::Error => Err(DataFusionError::Execution(
                    "column inventory failed".to_string(),
                )),
                FetchOutcome::Pending => std::future::pending().await,
            }
        }
    }

    fn column_fetcher(
        catalog_name: &str,
        schema_names: &[&str],
        table_names: &[&str],
        fetcher: Arc<RecordingFetcher>,
    ) -> CatalogColumnFetcher {
        CatalogColumnFetcher {
            catalog_name: catalog_name.to_string(),
            schema_names: schema_names
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            table_names: table_names
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            fetcher,
        }
    }

    fn strings(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn column_pin_recognizes_only_safe_pruning_shapes() {
        assert_eq!(
            column_pin(&col("catalog_name").eq(lit("warehouse"))),
            Some((PinColumn::Catalog, strings(&["warehouse"])))
        );
        assert_eq!(
            column_pin(&lit("warehouse").eq(col("catalog_name"))),
            Some((PinColumn::Catalog, strings(&["warehouse"])))
        );
        assert_eq!(
            column_pin(&col("schema_name").in_list(vec![lit("public"), lit("analytics")], false,)),
            Some((PinColumn::Schema, strings(&["analytics", "public"])))
        );
        assert_eq!(
            column_pin(
                &col("table_name")
                    .eq(lit("orders"))
                    .or(col("table_name").eq(lit("users")))
            ),
            Some((PinColumn::Table, strings(&["orders", "users"])))
        );
        assert!(
            column_pin(
                &col("schema_name")
                    .eq(lit("public"))
                    .or(col("table_name").eq(lit("orders")))
            )
            .is_none(),
            "a mixed-column OR cannot safely prune either dimension"
        );
        assert!(
            column_pin(&col("schema_name").in_list(vec![lit("public")], true)).is_none(),
            "NOT IN cannot be used as an inventory inclusion filter"
        );
    }

    #[test]
    fn column_pins_intersect_conjuncts() {
        let filters = vec![
            col("schema_name").in_list(vec![lit("public"), lit("analytics")], false),
            col("schema_name").eq(lit("public")),
            col("table_name").eq(lit("orders")),
        ];
        let pins = ColumnPins::from_filters(&filters);
        assert_eq!(pins.schemas, Some(strings(&["public"])));
        assert_eq!(pins.tables, Some(strings(&["orders"])));

        let impossible = ColumnPins::from_filters(&[
            col("schema_name").eq(lit("public")),
            col("schema_name").eq(lit("analytics")),
        ]);
        assert_eq!(impossible.schemas, Some(BTreeSet::new()));
    }

    #[tokio::test]
    async fn columns_scan_invokes_only_relevant_fetchers_with_pruned_filters() {
        let warehouse = RecordingFetcher::new(FetchOutcome::Rows(Vec::new()));
        let analytics = RecordingFetcher::new(FetchOutcome::Rows(Vec::new()));
        let fetchers = [
            column_fetcher(
                "warehouse",
                &["public"],
                &["orders"],
                Arc::clone(&warehouse),
            ),
            column_fetcher(
                "analytics",
                &["metrics"],
                &["events"],
                Arc::clone(&analytics),
            ),
        ];
        let table = build_columns_table(&[], &fetchers).expect("columns table");
        let filters = vec![
            col("catalog_name").eq(lit("warehouse")),
            col("schema_name").eq(lit("public")),
            col("table_name").eq(lit("orders")),
        ];
        let state = SessionContext::new().state();
        table
            .scan(&state, None, &filters, None)
            .await
            .expect("metadata scan");

        let calls = warehouse.calls();
        assert_eq!(calls.len(), 1);
        let filter = calls.first().expect("warehouse filter");
        assert_eq!(
            filter.schemas.as_deref(),
            Some(["public".to_string()].as_slice())
        );
        assert_eq!(
            filter.tables.as_deref(),
            Some(["orders".to_string()].as_slice())
        );
        assert!(analytics.calls().is_empty());
    }

    #[tokio::test]
    async fn columns_scan_omits_failed_and_timed_out_fetchers() {
        let failed = RecordingFetcher::new(FetchOutcome::Error);
        let stalled = RecordingFetcher::new(FetchOutcome::Pending);
        let fetchers = [
            column_fetcher("failed", &["public"], &["orders"], Arc::clone(&failed)),
            column_fetcher("stalled", &["public"], &["users"], Arc::clone(&stalled)),
        ];
        let mut table = build_columns_table(&[], &fetchers).expect("columns table");
        table.fetch_timeout = Duration::from_millis(20);
        let state = SessionContext::new().state();
        table
            .scan(&state, None, &[], None)
            .await
            .expect("failed sources are omitted from metadata");

        assert_eq!(failed.calls().len(), 1);
        assert_eq!(stalled.calls().len(), 1);
    }

    fn catalog_source() -> RegisteredSource {
        RegisteredSource {
            qualified_name: SourceQualifiedName::Catalog("warehouse".to_string()),
            tables: vec![RegisteredTable {
                schema_name: Some("public".to_string()),
                table_name: "orders".to_string(),
                description: String::new(),
                guide: String::new(),
                require_guide_read: false,
                columns: vec![RegisteredColumn {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                    is_virtual: false,
                    is_required_filter: true,
                    filter_mode: Some("exact".to_string()),
                    description: String::new(),
                }],
                filters: vec![RegisteredFilter {
                    name: "id".to_string(),
                    mode: "exact".to_string(),
                    required: true,
                    data_type: "Int64".to_string(),
                    description: String::new(),
                }],
                required_filters: vec!["id".to_string()],
                search_limits: None,
            }],
            table_functions: Vec::new(),
            inputs: vec![RegisteredInput {
                key: "REGION".to_string(),
                kind: ManifestInputKind::Variable,
                required: false,
                default_value: "eu".to_string(),
                hint: None,
                resolved_value: Some("us".to_string()),
                is_set: true,
            }],
        }
    }

    #[test]
    fn catalog_source_metadata_keeps_catalog_and_schema_separate() {
        let sources = [catalog_source()];

        let table = collect_static_tables(&sources)
            .into_iter()
            .find(|table| table.table_name == "orders")
            .expect("catalog table metadata");
        assert_eq!(table.catalog_name.as_deref(), Some("warehouse"));
        assert_eq!(table.schema_name, "public");

        let columns = source_catalog_column_rows(&sources);
        assert_eq!(columns.len(), 1);
        let column = columns.first().expect("catalog column metadata");
        assert_eq!(column.catalog_name, "warehouse");
        assert_eq!(column.schema_name, "public");

        let filters = catalog_filter_rows(&sources);
        assert_eq!(filters.len(), 1);
        let filter = filters.first().expect("catalog filter metadata");
        assert_eq!(filter.catalog_name, "warehouse");
        assert_eq!(filter.schema_name, "public");

        let inputs = catalog_input_rows(&sources);
        assert_eq!(inputs.len(), 1);
        let input = inputs.first().expect("catalog input metadata");
        assert_eq!(input.catalog_name, "warehouse");
        assert_eq!(input.schema_name, "");
    }

    #[test]
    fn collect_table_functions_preserves_registered_function_schema() {
        let functions = collect_table_functions(
            &[RegisteredSource {
                qualified_name: SourceQualifiedName::Schema("source_schema".to_string()),
                tables: Vec::new(),
                table_functions: vec![RegisteredTableFunction {
                    schema_name: "function_schema".to_string(),
                    function_name: "search".to_string(),
                    factory: Arc::new(StubSourceFunctionFactory::default()),
                    kind: coral_spec::SourceTableFunctionKind::Search,
                    description: String::new(),
                    guide: "Prefer this function for lookup.".to_string(),
                    require_guide_read: true,
                    arguments: Vec::new(),
                    result_columns: Vec::new(),
                    search_limits: None,
                }],
                inputs: Vec::new(),
            }],
            &[],
        );

        assert_eq!(functions.len(), 1);
        assert_eq!(
            functions
                .first()
                .map(|function| function.schema_name.as_str()),
            Some("function_schema")
        );
        assert_eq!(
            functions.first().map(|function| function.kind.as_str()),
            Some("search")
        );
    }
}
