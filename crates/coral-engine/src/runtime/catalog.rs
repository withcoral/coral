//! Registers the `coral` system schema for discoverable source metadata.

use std::collections::HashMap;
use std::sync::Arc;

use coral_spec::ManifestInputKind;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
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

/// Register `coral.tables` and `coral.columns` for the active source set.
///
/// # Errors
///
/// Returns a `DataFusionError` if the catalog is missing or the metadata
/// tables cannot be materialized.
pub(crate) fn register(ctx: &SessionContext, active_sources: &[RegisteredSource]) -> Result<()> {
    let tables_table = build_tables_table(active_sources)?;
    let columns_table = build_columns_table(active_sources)?;
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

fn build_tables_table(active_sources: &[RegisteredSource]) -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("guide", DataType::Utf8, false),
        Field::new("required_filters", DataType::Utf8, false),
        Field::new("search_limits_json", DataType::Utf8, true),
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
                    table.search_limits_json.as_deref(),
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
            utf8_column(rows.iter().map(|row| row.5)),
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
                        filter_mode: column.filter_mode.clone(),
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
            utf8_column(rows.iter().map(|row| row.filter_mode.as_deref())),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    MemTable::try_new(schema, vec![vec![batch]])
}

#[cfg(test)]
mod tests {
    use crate::backends::{RegisteredSource, RegisteredTableFunction};

    use super::collect_table_functions;

    #[test]
    fn collect_table_functions_preserves_registered_function_schema() {
        let functions = collect_table_functions(&[RegisteredSource {
            schema_name: "source_schema".to_string(),
            tables: Vec::new(),
            table_functions: vec![RegisteredTableFunction {
                schema_name: "function_schema".to_string(),
                function_name: "search".to_string(),
                internal_name: "internal_search".to_string(),
                kind: "search".to_string(),
                description: String::new(),
                arguments: Vec::new(),
                result_columns: Vec::new(),
                arg_names: Vec::new(),
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
