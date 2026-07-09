//! Native file table provider backed by local files or object-store URLs.

mod error;
mod file_groups;
mod json;
mod listing;
mod metadata;
mod parquet_schema;
mod partitions;
mod provider;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::backends::{
    BackendCompileRequest, BackendRegistration, BackendRegistrationContext,
    BackendSchemaRegistration, CompiledBackendSource, RegisteredSource, RegisteredTable,
    build_registered_inputs, build_registered_table, registered_columns_from_schema,
    registered_columns_from_specs, required_filter_names,
    validate_lookup_key_filter_backend_support,
};
use crate::contracts::{StatisticsObservationScope, TableSchemaSignature};
use crate::runtime::statistics::{BatchStatisticsPlan, observe_execution_plan};
use coral_spec::SourceBackend;
use coral_spec::backends::file::{FileFormat, FileSourceManifest, FileTableSpec};

use self::json::JsonFileTableProvider;
use self::provider::FileTableProvider;

#[derive(Debug, Clone)]
struct FileCompiledSource {
    manifest: FileSourceManifest,
    home_dir: Option<PathBuf>,
    source_secrets: BTreeMap<String, String>,
    source_variables: BTreeMap<String, String>,
}

pub(crate) fn compile_source(
    manifest: FileSourceManifest,
    home_dir: Option<PathBuf>,
    source_secrets: BTreeMap<String, String>,
    source_variables: BTreeMap<String, String>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(FileCompiledSource {
        manifest,
        home_dir,
        source_secrets,
        source_variables,
    })
}

pub(crate) fn compile_manifest(
    manifest: &FileSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendSource> {
    compile_source(
        manifest.clone(),
        request.runtime_context.home_dir.clone(),
        request.source_secrets.clone(),
        request.source_variables.clone(),
    )
}

#[derive(Debug, Clone)]
pub(super) struct FileStatisticsRegistration {
    source_version: String,
}

impl FileStatisticsRegistration {
    fn new(source_version: impl Into<String>) -> Self {
        Self {
            source_version: source_version.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct FileTableStatistics {
    source_schema: String,
    table_name: String,
    source_version: Option<String>,
    schema_signature: TableSchemaSignature,
    field_count: usize,
}

impl FileTableStatistics {
    fn new(
        source_schema: &str,
        table_name: &str,
        schema_signature: TableSchemaSignature,
        field_count: usize,
        registration: FileStatisticsRegistration,
    ) -> Self {
        Self {
            source_schema: source_schema.to_string(),
            table_name: table_name.to_string(),
            source_version: Some(registration.source_version),
            schema_signature,
            field_count,
        }
    }

    fn observe_scan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        projection: Option<&Vec<usize>>,
        pushed_filter_columns: Vec<String>,
        limit: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        observe_execution_plan(
            input,
            self.plan_with_scope(self.scope(projection, pushed_filter_columns, limit)),
        )
    }

    fn observe_scan_with_scope(
        &self,
        input: Arc<dyn ExecutionPlan>,
        scope: StatisticsObservationScope,
    ) -> Arc<dyn ExecutionPlan> {
        observe_execution_plan(input, self.plan_with_scope(scope))
    }

    fn scope(
        &self,
        projection: Option<&Vec<usize>>,
        pushed_filter_columns: Vec<String>,
        limit: Option<usize>,
    ) -> StatisticsObservationScope {
        statistics_scope(projection, self.field_count, pushed_filter_columns, limit)
    }

    fn plan_with_scope(&self, scope: StatisticsObservationScope) -> BatchStatisticsPlan {
        BatchStatisticsPlan::table_global(
            self.source_schema.clone(),
            self.table_name.clone(),
            self.source_version.clone(),
            self.schema_signature.clone(),
        )
        .with_scope(scope)
    }
}

fn statistics_scope(
    projection: Option<&Vec<usize>>,
    field_count: usize,
    pushed_filter_columns: Vec<String>,
    limit: Option<usize>,
) -> StatisticsObservationScope {
    if limit.is_some() {
        return StatisticsObservationScope::Limited;
    }
    if projection_is_partial(projection, field_count) {
        return StatisticsObservationScope::Unknown;
    }
    if pushed_filter_columns.is_empty() {
        StatisticsObservationScope::TableGlobal
    } else {
        StatisticsObservationScope::Filtered {
            filter_columns: pushed_filter_columns,
        }
    }
}

fn projection_is_partial(projection: Option<&Vec<usize>>, field_count: usize) -> bool {
    let Some(projection) = projection else {
        return false;
    };
    projection.iter().copied().collect::<BTreeSet<_>>().len() != field_count
}

#[async_trait]
impl CompiledBackendSource for FileCompiledSource {
    fn schema_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn source_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn validate_runtime_capabilities(&self) -> Result<()> {
        validate_lookup_key_filter_backend_support(
            self.source_name(),
            SourceBackend::File,
            self.manifest
                .tables
                .iter()
                .flat_map(FileTableSpec::filters)
                .any(|filter| filter.lookup_key),
        )
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        _registration: &BackendRegistrationContext,
    ) -> Result<BackendRegistration> {
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut table_infos = Vec::with_capacity(self.manifest.tables.len());
        let resolved_inputs = coral_spec::resolve_inputs(
            &self.manifest.declared_inputs,
            &self.source_secrets,
            &self.source_variables,
        );

        for table in &self.manifest.tables {
            let table_statistics = if matches!(
                table.format,
                FileFormat::Json | FileFormat::Jsonl | FileFormat::Parquet
            ) {
                Some(FileStatisticsRegistration::new(
                    self.manifest.common.version.clone(),
                ))
            } else {
                None
            };
            let provider: Arc<dyn TableProvider> = match table.format {
                FileFormat::Jsonl | FileFormat::Json if json::requires_custom_provider(table)? => {
                    Arc::new(
                        JsonFileTableProvider::try_new_async(
                            ctx,
                            &self.manifest.common.name,
                            table.clone(),
                            self.home_dir.as_deref(),
                            &resolved_inputs,
                            table_statistics,
                        )
                        .await?,
                    )
                }
                FileFormat::Parquet | FileFormat::Csv | FileFormat::Jsonl | FileFormat::Json => {
                    Arc::new(
                        FileTableProvider::try_new_async(
                            ctx,
                            &self.manifest.common.name,
                            table.clone(),
                            self.home_dir.as_deref(),
                            &resolved_inputs,
                            table_statistics,
                        )
                        .await?,
                    )
                }
            };
            let schema = provider.schema();
            let table_name = table.name().to_string();
            let metadata = registered_table(table, &schema);
            tables.insert(table_name, provider);
            table_infos.push(metadata);
        }

        let secret_keys = self.source_secrets.keys().cloned().collect();
        let inputs = build_registered_inputs(
            &self.manifest.declared_inputs,
            &self.source_variables,
            &secret_keys,
        );

        let schema_name = self.manifest.common.name.clone();
        Ok(BackendRegistration {
            schemas: vec![BackendSchemaRegistration {
                tables,
                source: RegisteredSource {
                    schema_name,
                    source_version: self.manifest.common.version.clone(),
                    tables: table_infos,
                    table_functions: vec![],
                    inputs,
                },
            }],
        })
    }
}

fn registered_table(table: &FileTableSpec, inferred_schema: &SchemaRef) -> RegisteredTable {
    let filters = table.filters();
    let required_filters = required_filter_names(filters);
    let columns = if table.columns().is_empty() {
        registered_columns_from_schema(inferred_schema, filters)
    } else {
        let mut columns = registered_columns_from_specs(table.columns(), filters);
        let declared_names = table
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<HashSet<_>>();
        columns.extend(
            registered_columns_from_schema(inferred_schema, filters)
                .into_iter()
                .filter(|column| !declared_names.contains(column.name.as_str())),
        );
        columns
    };

    build_registered_table(&table.common, columns, required_filters)
}
