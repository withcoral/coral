//! Registers the `coral` system schema for discoverable source metadata.

use std::collections::HashMap;
use std::sync::Arc;

use coral_spec::ManifestInputKind;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

use crate::backends::RegisteredSource;
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{ColumnInfo, TableInfo};

/// Schema name for source metadata tables such as `coral.tables`.
pub(crate) const SYSTEM_SCHEMA: &str = "coral";

/// Register `coral.tables` and `coral.columns` for the active source set.
///
/// # Errors
///
/// Returns a `DataFusionError` if the catalog is missing or the metadata
/// tables cannot be materialized.
pub(crate) fn register(ctx: &SessionContext, active_sources: &[RegisteredSource]) -> Result<()> {
    let tables_table = build_tables_table(active_sources)?;
    let columns_table = build_columns_table(active_sources)?;
    let inputs_table = build_inputs_table(active_sources)?;
    let table_functions_table = build_table_functions_table(active_sources)?;

    let mut meta_tables: HashMap<String, Arc<dyn datafusion::datasource::TableProvider>> =
        HashMap::new();
    meta_tables.insert("tables".to_string(), Arc::new(tables_table));
    meta_tables.insert("columns".to_string(), Arc::new(columns_table));
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

fn build_table_functions_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("function_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("arguments_json", DataType::Utf8, false),
        Field::new("result_columns_json", DataType::Utf8, false),
    ]));

    let mut rows = active_sources
        .iter()
        .flat_map(|source| source.table_functions.iter())
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        (&left.schema_name, &left.function_name).cmp(&(&right.schema_name, &right.function_name))
    });

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            utf8_column(rows.iter().map(|row| Some(row.schema_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.function_name.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.description.as_str()))),
            utf8_column(rows.iter().map(|row| Some(row.arguments_json.as_str()))),
            utf8_column(
                rows.iter()
                    .map(|row| Some(row.result_columns_json.as_str())),
            ),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}

fn utf8_column<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(values.into_iter().collect::<StringArray>())
}

/// Collect typed query-visible table metadata for the active source set.
#[must_use]
pub(crate) fn collect_tables(active_sources: &[RegisteredSource]) -> Vec<TableInfo> {
    let mut tables = active_sources
        .iter()
        .flat_map(|source| {
            source.tables.iter().map(move |table| TableInfo {
                schema_name: source.schema_name.clone(),
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
        })
        .collect::<Vec<_>>();
    tables.sort_by(|left, right| {
        (&left.schema_name, &left.table_name).cmp(&(&right.schema_name, &right.table_name))
    });
    tables
}

fn build_tables_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("guide", DataType::Utf8, false),
        Field::new("required_filters", DataType::Utf8, false),
    ]));

    let mut rows = active_sources
        .iter()
        .flat_map(|source| {
            source.tables.iter().map(move |table| {
                (
                    source.schema_name.as_str(),
                    table.table_name.as_str(),
                    table.description.as_str(),
                    table.guide.as_str(),
                    table.required_filters.join(","),
                )
            })
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(rows.iter().map(|row| Some(row.0)).collect::<StringArray>()),
            Arc::new(rows.iter().map(|row| Some(row.1)).collect::<StringArray>()),
            Arc::new(rows.iter().map(|row| Some(row.2)).collect::<StringArray>()),
            Arc::new(rows.iter().map(|row| Some(row.3)).collect::<StringArray>()),
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.4.as_str()))
                    .collect::<StringArray>(),
            ),
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
    schema_name: String,
    table_name: String,
    column_name: String,
    data_type: String,
    is_nullable: bool,
    is_virtual: bool,
    is_required_filter: bool,
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
    ]));

    let mut rows = active_sources
        .iter()
        .flat_map(|source| {
            source.tables.iter().flat_map(move |table| {
                table
                    .columns
                    .iter()
                    .enumerate()
                    .map(move |(position, column)| CatalogColumn {
                        schema_name: source.schema_name.clone(),
                        table_name: table.table_name.clone(),
                        column_name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        is_nullable: column.nullable,
                        is_virtual: column.is_virtual,
                        is_required_filter: column.is_required_filter,
                        description: column.description.clone(),
                        ordinal_position: position,
                    })
            })
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        (&left.schema_name, &left.table_name, left.ordinal_position).cmp(&(
            &right.schema_name,
            &right.table_name,
            right.ordinal_position,
        ))
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
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}
