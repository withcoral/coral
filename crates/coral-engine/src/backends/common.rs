//! Shared internal backend contracts and registry-visible metadata.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::QueryRuntimeContext;
use async_trait::async_trait;
use coral_spec::backends::file::PartitionColumnSpec;
use coral_spec::{ColumnSpec, FilterSpec, ManifestDataType, TableCommon};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

#[derive(Debug, Clone)]
pub(crate) struct RegisteredColumn {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) is_virtual: bool,
    pub(crate) is_required_filter: bool,
    pub(crate) description: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RegisteredTable {
    pub(crate) table_name: String,
    pub(crate) description: String,
    pub(crate) guide: String,
    pub(crate) columns: Vec<RegisteredColumn>,
    pub(crate) required_filters: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RegisteredSource {
    pub(crate) schema_name: String,
    pub(crate) tables: Vec<RegisteredTable>,
}

pub(crate) struct BackendRegistration {
    pub(crate) tables: HashMap<String, Arc<dyn TableProvider>>,
    pub(crate) source: RegisteredSource,
}

pub(crate) struct BackendCompileRequest<'a> {
    pub(crate) runtime_context: &'a QueryRuntimeContext,
    pub(crate) source_secrets: BTreeMap<String, String>,
    pub(crate) source_variables: BTreeMap<String, String>,
}

#[async_trait]
pub(crate) trait CompiledBackendSource: Send + Sync {
    fn schema_name(&self) -> &str;

    fn source_name(&self) -> &str;

    async fn register(
        &self,
        ctx: &SessionContext,
    ) -> datafusion::error::Result<BackendRegistration>;
}

pub(crate) fn required_filter_names(filters: &[FilterSpec]) -> Vec<String> {
    filters
        .iter()
        .filter(|filter| filter.required)
        .map(|filter| filter.name.clone())
        .collect()
}

pub(crate) fn registered_columns_from_specs(
    columns: &[ColumnSpec],
    required_filters: &[String],
) -> Vec<RegisteredColumn> {
    columns
        .iter()
        .map(|column| RegisteredColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            is_virtual: column.r#virtual,
            is_required_filter: required_filters.iter().any(|filter| filter == &column.name),
            description: column.description.clone(),
        })
        .collect()
}

pub(crate) fn registered_columns_from_schema(
    schema: &SchemaRef,
    required_filters: &[String],
) -> Vec<RegisteredColumn> {
    schema
        .fields()
        .iter()
        .map(|field| RegisteredColumn {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
            is_virtual: false,
            is_required_filter: required_filters.iter().any(|filter| filter == field.name()),
            description: String::new(),
        })
        .collect()
}

pub(crate) fn build_registered_table(
    common: &TableCommon,
    columns: Vec<RegisteredColumn>,
    required_filters: Vec<String>,
) -> RegisteredTable {
    RegisteredTable {
        table_name: common.name.clone(),
        description: common.description.clone(),
        guide: common.guide.clone(),
        columns,
        required_filters,
    }
}

pub(crate) fn manifest_data_type_to_arrow(data_type: ManifestDataType) -> DataType {
    match data_type {
        ManifestDataType::Utf8 => DataType::Utf8,
        ManifestDataType::Int64 => DataType::Int64,
        ManifestDataType::Boolean => DataType::Boolean,
        ManifestDataType::Float64 => DataType::Float64,
        ManifestDataType::Timestamp => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        }
    }
}

pub(crate) fn arrow_type_for_column(column: &ColumnSpec) -> datafusion::error::Result<DataType> {
    column
        .manifest_data_type()
        .map(manifest_data_type_to_arrow)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

pub(crate) fn schema_from_columns(
    columns: &[ColumnSpec],
    source_schema: &str,
    table_name: &str,
) -> datafusion::error::Result<SchemaRef> {
    if columns.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{source_schema}.{table_name} has no columns defined in the manifest"
        )));
    }

    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        fields.push(Field::new(
            &column.name,
            arrow_type_for_column(column)?,
            column.nullable,
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn partition_columns_to_arrow(
    partitions: &[PartitionColumnSpec],
) -> datafusion::error::Result<Vec<(String, DataType)>> {
    partitions
        .iter()
        .map(|partition: &PartitionColumnSpec| {
            partition
                .manifest_data_type()
                .map(|data_type| {
                    (
                        partition.name.clone(),
                        manifest_data_type_to_arrow(data_type),
                    )
                })
                .map_err(|error: coral_spec::ManifestError| {
                    DataFusionError::Execution(error.to_string())
                })
        })
        .collect()
}
