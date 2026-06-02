//! Shared internal backend contracts and registry-visible metadata.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write;
use std::sync::{Arc, OnceLock};

use crate::{QueryRuntimeContext, QuerySource, RequestAuthenticator, SourceInputResolver};
use async_trait::async_trait;
use coral_spec::{
    ColumnSpec, FilterSpec, ManifestDataType, ManifestInputKind, ManifestInputSpec,
    SearchLimitsSpec, SourceTableFunctionSpec, TableCommon,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

pub(crate) type SourceTableFunctions = HashMap<String, Arc<dyn TableFunctionImpl>>;

#[derive(Debug, Clone)]
pub(crate) struct RegisteredColumn {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) is_virtual: bool,
    pub(crate) is_required_filter: bool,
    pub(crate) filter_mode: Option<String>,
    pub(crate) description: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RegisteredTable {
    pub(crate) table_name: String,
    pub(crate) description: String,
    pub(crate) guide: String,
    pub(crate) columns: Vec<RegisteredColumn>,
    pub(crate) filters: Vec<RegisteredFilter>,
    pub(crate) required_filters: Vec<String>,
    pub(crate) search_limits_json: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RegisteredTableFunction {
    pub(crate) schema_name: String,
    pub(crate) function_name: String,
    pub(crate) internal_name: String,
    pub(crate) kind: String,
    pub(crate) description: String,
    pub(crate) arguments: Vec<RegisteredTableFunctionArgument>,
    pub(crate) result_columns: Vec<RegisteredTableFunctionResultColumn>,
    pub(crate) arg_names: Vec<String>,
    pub(crate) search_limits_json: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RegisteredFilter {
    pub(crate) name: String,
    pub(crate) mode: String,
    pub(crate) required: bool,
    pub(crate) data_type: String,
    pub(crate) description: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RegisteredTableFunctionArgument {
    pub(crate) name: String,
    pub(crate) required: bool,
    pub(crate) values: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RegisteredTableFunctionResultColumn {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) description: String,
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
    pub(crate) table_functions: SourceTableFunctions,
    pub(crate) source: RegisteredSource,
}

/// Build a collision-free `DataFusion` UDTF name for a source-scoped function.
///
/// `DataFusion`'s UDTF registry is flat, so both source schema and public
/// function name are hex-encoded to preserve arbitrary valid identifiers
/// without delimiter collisions.
pub(crate) fn internal_table_function_name(schema: &str, function: &str) -> String {
    format!(
        "__coral_udtf_{}_{}",
        hex_encode(schema),
        hex_encode(function)
    )
}

fn hex_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String never fails");
    }
    encoded
}

pub(crate) struct BackendCompileRequest<'a> {
    pub(crate) source: &'a QuerySource,
    pub(crate) runtime_context: &'a QueryRuntimeContext,
    pub(crate) source_secrets: BTreeMap<String, String>,
    pub(crate) source_variables: BTreeMap<String, String>,
    pub(crate) request_authenticators: &'a HashMap<String, Arc<dyn RequestAuthenticator>>,
    pub(crate) source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
}

/// Shared resources available while registering one batch of compiled sources.
///
/// Every backend receives this context. Most backends ignore it today; HTTP uses
/// it to share default transport setup across sources registered in the same
/// runtime build.
#[derive(Default)]
pub(crate) struct BackendRegistrationContext {
    default_http_client: OnceLock<Result<reqwest::Client, String>>,
}

impl BackendRegistrationContext {
    pub(crate) fn default_http_client(
        &self,
        build_client: impl FnOnce() -> Result<reqwest::Client, String>,
    ) -> Result<reqwest::Client, String> {
        self.default_http_client
            .get_or_init(build_client)
            .as_ref()
            .cloned()
            .map_err(Clone::clone)
    }
}

#[async_trait]
pub(crate) trait CompiledBackendSource: Send + Sync {
    fn schema_name(&self) -> &str;

    fn source_name(&self) -> &str;

    /// Register this compiled source into a `DataFusion` session.
    ///
    /// The registration context is batch-scoped and backend-agnostic. Backends
    /// should use it only for resources that are safe to share across sources in
    /// the same registration pass.
    async fn register(
        &self,
        ctx: &SessionContext,
        registration: &BackendRegistrationContext,
    ) -> datafusion::error::Result<BackendRegistration>;
}

pub(crate) fn required_filter_names(filters: &[FilterSpec]) -> Vec<String> {
    filters
        .iter()
        .filter(|filter| filter.required)
        .map(|filter| filter.name.clone())
        .collect()
}

pub(crate) fn registered_filters_from_specs(filters: &[FilterSpec]) -> Vec<RegisteredFilter> {
    filters
        .iter()
        .map(|filter| RegisteredFilter {
            name: filter.name.clone(),
            mode: filter.mode.as_str().to_string(),
            required: filter.required,
            data_type: filter.data_type.clone(),
            description: filter.description.clone(),
        })
        .collect()
}

pub(crate) fn registered_columns_from_specs(
    columns: &[ColumnSpec],
    filters: &[FilterSpec],
) -> Vec<RegisteredColumn> {
    columns
        .iter()
        .map(|column| {
            let filter = filters
                .iter()
                .find(|filter| filter.name == column.name.as_str());
            RegisteredColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                is_virtual: column.r#virtual,
                is_required_filter: filter.is_some_and(|filter| filter.required),
                filter_mode: filter.map(|filter| filter.mode.as_str().to_string()),
                description: column.description.clone(),
            }
        })
        .collect()
}

pub(crate) fn registered_columns_from_schema(
    schema: &SchemaRef,
    filters: &[FilterSpec],
) -> Vec<RegisteredColumn> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let filter = filters
                .iter()
                .find(|filter| filter.name == field.name().as_str());
            RegisteredColumn {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
                is_virtual: false,
                is_required_filter: filter.is_some_and(|filter| filter.required),
                filter_mode: filter.map(|filter| filter.mode.as_str().to_string()),
                description: String::new(),
            }
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
        filters: registered_filters_from_specs(&common.filters),
        required_filters,
        search_limits_json: common.search_limits.as_ref().map(serialize_search_limits),
    }
}

pub(crate) fn build_registered_table_function(
    schema_name: &str,
    function: &SourceTableFunctionSpec,
    internal_name: String,
) -> RegisteredTableFunction {
    let arguments = function
        .args
        .iter()
        .map(|arg| RegisteredTableFunctionArgument {
            name: arg.name.clone(),
            required: arg.required,
            values: arg.values.clone(),
        })
        .collect::<Vec<_>>();
    let result_columns = registered_columns_from_specs(&function.columns, &[])
        .into_iter()
        .map(|column| RegisteredTableFunctionResultColumn {
            name: column.name,
            data_type: column.data_type,
            nullable: column.nullable,
            description: column.description,
        })
        .collect::<Vec<_>>();

    RegisteredTableFunction {
        schema_name: schema_name.to_string(),
        function_name: function.name.clone(),
        internal_name,
        kind: function.kind.as_str().to_string(),
        description: function.description.clone(),
        arguments,
        result_columns,
        arg_names: function.args.iter().map(|arg| arg.name.clone()).collect(),
        search_limits_json: function.search_limits.as_ref().map(serialize_search_limits),
    }
}

fn serialize_search_limits(limits: &SearchLimitsSpec) -> String {
    serde_json::to_string(limits).expect("search limits json")
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
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn default_http_client_is_shared_only_within_registration_context() {
        let build_count = AtomicUsize::new(0);
        let first_context = BackendRegistrationContext::default();

        first_context
            .default_http_client(|| build_counted_client(&build_count))
            .expect("first context should build a client");
        first_context
            .default_http_client(|| build_counted_client(&build_count))
            .expect("first context should reuse its client");

        assert_eq!(
            build_count.load(Ordering::SeqCst),
            1,
            "one registration context should build one default HTTP client"
        );

        let second_context = BackendRegistrationContext::default();
        second_context
            .default_http_client(|| build_counted_client(&build_count))
            .expect("new context should build its own client");

        assert_eq!(
            build_count.load(Ordering::SeqCst),
            2,
            "default HTTP clients should not be process-global"
        );
    }

    fn build_counted_client(build_count: &AtomicUsize) -> Result<reqwest::Client, String> {
        build_count.fetch_add(1, Ordering::SeqCst);
        reqwest::Client::builder()
            .build()
            .map_err(|error| error.to_string())
    }
}
