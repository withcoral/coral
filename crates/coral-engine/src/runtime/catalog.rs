//! Registers the `coral` system schema for discoverable source metadata.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, Int32Array, RecordBatch, StringArray};
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

    let mut meta_tables: HashMap<String, Arc<dyn datafusion::datasource::TableProvider>> =
        HashMap::new();
    meta_tables.insert("tables".to_string(), Arc::new(tables_table));
    meta_tables.insert("columns".to_string(), Arc::new(columns_table));

    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("catalog 'datafusion' not found".to_string()))?;
    catalog.register_schema(
        SYSTEM_SCHEMA,
        Arc::new(StaticSchemaProvider::new(meta_tables)),
    )?;

    Ok(())
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
                columns: table
                    .columns
                    .iter()
                    .map(|column| ColumnInfo {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
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

struct CatalogColumn {
    schema_name: String,
    table_name: String,
    column_name: String,
    data_type: String,
    is_virtual: bool,
    is_required_filter: bool,
    description: String,
    ordinal_position: usize,
}

#[allow(
    clippy::too_many_lines,
    reason = "The metadata batch builder keeps the fixed coral.columns layout in one place."
)]
fn build_columns_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
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
