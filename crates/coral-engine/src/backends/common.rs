//! Shared internal backend contracts and registry-visible metadata.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::{QueryRuntimeContext, RequestAuthenticator};
use async_trait::async_trait;
use coral_spec::backends::file::PartitionColumnSpec;
use coral_spec::{
    ColumnSpec, FilterSpec, ManifestDataType, ManifestInputKind, ManifestInputSpec,
    SourceTableFunctionSpec, TableCommon,
};
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
pub(crate) struct RegisteredTableFunction {
    pub(crate) schema_name: String,
    pub(crate) function_name: String,
    pub(crate) description: String,
    pub(crate) arguments_json: String,
    pub(crate) result_columns_json: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RegisteredInput {
    pub(crate) key: String,
    pub(crate) kind: ManifestInputKind,
    pub(crate) required: bool,
    /// Mirrors [`ManifestInputSpec::default_value`]: empty string means
    /// "no default declared". The catalog layer maps empty to SQL `NULL`.
    pub(crate) default_value: String,
    pub(crate) hint: Option<String>,
    /// Resolved value for variables. Unconditionally `None` for secrets.
    pub(crate) resolved_value: Option<String>,
    pub(crate) is_set: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RegisteredSource {
    pub(crate) schema_name: String,
    pub(crate) tables: Vec<RegisteredTable>,
    pub(crate) table_functions: Vec<RegisteredTableFunction>,
    pub(crate) inputs: Vec<RegisteredInput>,
}

pub(crate) struct BackendRegistration {
    pub(crate) tables: HashMap<String, Arc<dyn TableProvider>>,
    pub(crate) source: RegisteredSource,
}

pub(crate) struct BackendCompileRequest<'a> {
    pub(crate) runtime_context: &'a QueryRuntimeContext,
    pub(crate) source_secrets: BTreeMap<String, String>,
    pub(crate) source_variables: BTreeMap<String, String>,
    pub(crate) request_authenticators: &'a HashMap<String, Arc<dyn RequestAuthenticator>>,
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

/// Build registry-visible input metadata.
///
/// Takes the manifest-declared inputs, the resolved non-secret variables map,
/// and the set of configured secret keys. Secret *values* are never consumed
/// here — only their keys — so the catalog layer has no path to leak secret
/// values.
pub(crate) fn build_registered_inputs(
    declared: &[ManifestInputSpec],
    variables: &BTreeMap<String, String>,
    secret_keys: &BTreeSet<String>,
) -> Vec<RegisteredInput> {
    declared
        .iter()
        .map(|input| {
            let (resolved_value, is_set) = match input.kind {
                ManifestInputKind::Variable => {
                    let explicit = variables.get(&input.key).cloned();
                    let has_default = !input.default_value.is_empty();
                    let resolved = explicit
                        .clone()
                        .or_else(|| has_default.then(|| input.default_value.clone()));
                    // Variable is "set" if the user explicitly configured the
                    // key (even with an empty string — HTTP input resolution
                    // and required-variable validation both treat the key's
                    // presence as authoritative) or the manifest provides a
                    // non-empty default.
                    let is_set = explicit.is_some() || has_default;
                    (resolved, is_set)
                }
                ManifestInputKind::Secret => (None, secret_keys.contains(&input.key)),
            };
            debug_assert!(
                !(matches!(input.kind, ManifestInputKind::Secret) && resolved_value.is_some()),
                "secret inputs must never carry a resolved value"
            );
            RegisteredInput {
                key: input.key.clone(),
                kind: input.kind,
                required: input.required,
                default_value: input.default_value.clone(),
                hint: input.hint.clone(),
                resolved_value,
                is_set,
            }
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

pub(crate) fn build_registered_table_function(
    schema_name: &str,
    function: &SourceTableFunctionSpec,
) -> RegisteredTableFunction {
    let arguments = function
        .args
        .iter()
        .map(|arg| {
            serde_json::json!({
                "name": arg.name,
                "required": arg.required,
                "values": arg.values,
            })
        })
        .collect::<Vec<_>>();
    let result_columns = registered_columns_from_specs(&function.columns, &[])
        .into_iter()
        .map(|column| {
            serde_json::json!({
                "name": column.name,
                "type": column.data_type,
                "nullable": column.nullable,
                "description": column.description,
            })
        })
        .collect::<Vec<_>>();

    RegisteredTableFunction {
        schema_name: schema_name.to_string(),
        function_name: function.name.clone(),
        description: function.description.clone(),
        arguments_json: serde_json::to_string(&arguments).expect("arguments json"),
        result_columns_json: serde_json::to_string(&result_columns).expect("result columns json"),
    }
}

pub(crate) fn manifest_data_type_to_arrow(data_type: ManifestDataType) -> DataType {
    match data_type {
        ManifestDataType::Utf8 | ManifestDataType::Json => DataType::Utf8,
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
