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

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

use crate::backends::{
    BackendCompileRequest, BackendRegistration, BackendRegistrationContext, CatalogPublication,
    CompiledBackendSource, RegisteredTableMetadata, build_registered_inputs,
    build_registered_table_metadata, registered_columns_from_schema, registered_columns_from_specs,
    required_filter_names, validate_lookup_key_filter_backend_support,
};
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

#[async_trait]
impl CompiledBackendSource for FileCompiledSource {
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
        let resolved_inputs = coral_spec::resolve_inputs(
            &self.manifest.declared_inputs,
            &self.source_secrets,
            &self.source_variables,
        );
        let secret_keys = self.source_secrets.keys().cloned().collect();
        let inputs = build_registered_inputs(
            &self.manifest.declared_inputs,
            &self.source_variables,
            &secret_keys,
        );
        let mut publications = BTreeMap::<String, CatalogPublication>::new();

        for table in &self.manifest.tables {
            let provider: Arc<dyn TableProvider> = match table.format {
                FileFormat::Jsonl | FileFormat::Json if json::requires_custom_provider(table)? => {
                    Arc::new(
                        JsonFileTableProvider::try_new_async(
                            ctx,
                            &self.manifest.common.name,
                            table.clone(),
                            self.home_dir.as_deref(),
                            &resolved_inputs,
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
                        )
                        .await?,
                    )
                }
            };
            let schema = provider.schema();
            let metadata = registered_table(table, &schema);
            publications
                .entry(table.common.catalog_name.clone())
                .or_insert_with(|| {
                    CatalogPublication::new(table.common.catalog_name.clone(), inputs.clone())
                })
                .publish_table(
                    table.common.schema_name.clone(),
                    table.table_name(),
                    provider,
                    metadata,
                )?;
        }

        Ok(BackendRegistration::from_publications(
            publications.into_values().collect(),
        ))
    }
}

fn registered_table(table: &FileTableSpec, inferred_schema: &SchemaRef) -> RegisteredTableMetadata {
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

    build_registered_table_metadata(&table.common, columns, required_filters)
}
