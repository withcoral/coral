//! Local catalog snapshot loading without query-runtime provider I/O.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "local catalog snapshot loader is consumed by the follow-up catalog provider PR"
    )
)]

use coral_engine::{
    CatalogInfo, ColumnInfo, RuntimeSourceComponent, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo,
};
use coral_spec::v4::SurfaceType;
use coral_spec::{
    ColumnSpec, FilterSpec, SourceTableFunctionSpec, TableCommon, ValidatedSourceManifest,
};

use crate::bootstrap::AppError;
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::materialization::{
    SourceDiagnosticReporter, SourceLoadDiagnosticStage, incompatible_materialization_error,
    load_v4_materialization_with_reporter,
};
use crate::sources::model::InstalledSource;
use crate::sources::runtime_package::{
    runtime_component_for_v4_database_source, runtime_component_for_v4_source,
};
use crate::state::{AppConfig, AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

/// Builds catalog metadata from persisted app state and local source artifacts.
///
/// This deliberately avoids runtime provider registration: it does not refresh
/// credentials, infer remote/local file schemas, start MCP processes, or issue
/// provider HTTP calls. DSL v4 sources still require their published
/// materialized projection artifacts because those artifacts are the local
/// catalog source of truth for generated runtime components.
#[derive(Clone)]
pub(crate) struct CatalogSnapshotLoader {
    config_store: ConfigStore,
    layout: AppStateLayout,
    diagnostic_reporter: SourceDiagnosticReporter,
}

impl CatalogSnapshotLoader {
    pub(crate) fn new(config_store: ConfigStore, layout: AppStateLayout) -> Self {
        Self::with_diagnostic_reporter(config_store, layout, SourceDiagnosticReporter::default())
    }

    pub(crate) fn with_diagnostic_reporter(
        config_store: ConfigStore,
        layout: AppStateLayout,
        diagnostic_reporter: SourceDiagnosticReporter,
    ) -> Self {
        Self {
            config_store,
            layout,
            diagnostic_reporter,
        }
    }

    pub(crate) fn load_catalog(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<CatalogInfo, AppError> {
        let _state_lock = self.config_store.state_lock_shared()?;
        let config = self.config_store.load_config_unlocked()?;
        self.load_catalog_from_config(workspace_name, &config)
    }

    fn load_catalog_from_config(
        &self,
        workspace_name: &WorkspaceName,
        config: &AppConfig,
    ) -> Result<CatalogInfo, AppError> {
        let mut catalog = CatalogInfo {
            tables: Vec::new(),
            table_functions: Vec::new(),
        };

        for source in config.workspace_sources(workspace_name) {
            match self.load_source_catalog(workspace_name, &source) {
                Ok(source_catalog) => {
                    self.diagnostic_reporter.clear_source_load_failure(
                        SourceLoadDiagnosticStage::Catalog,
                        workspace_name,
                        &source.name,
                    );
                    catalog.tables.extend(source_catalog.tables);
                    catalog
                        .table_functions
                        .extend(source_catalog.table_functions);
                }
                Err(
                    error @ (AppError::MissingOrIncompatibleV4Materialization { .. }
                    | AppError::IncompatibleInstalledV4Manifest { .. }
                    | AppError::InvalidV4ProjectionOverride { .. }
                    | AppError::InvalidV4ParameterMetadataOverride { .. }),
                ) => {
                    self.diagnostic_reporter.report_source_load_failure(
                        SourceLoadDiagnosticStage::Catalog,
                        workspace_name,
                        &source.name,
                        "SOURCE_LOAD_FAILED",
                        &error.to_string(),
                    );
                }
                Err(error) => return Err(error),
            }
        }

        sort_catalog(&mut catalog);
        Ok(catalog)
    }

    fn load_source_catalog(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<CatalogInfo, AppError> {
        let components = self.runtime_components_for_installed_source(workspace_name, source)?;
        Ok(catalog_info_from_components(&components))
    }

    fn runtime_components_for_installed_source(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<Vec<RuntimeSourceComponent>, AppError> {
        let installed = resolve_installed_manifest(workspace_name, source, &self.layout)?;
        let source_spec = installed.source_spec;
        if let Some(v4) = source_spec.as_v4() {
            if v4.surface.surface_type == SurfaceType::Database {
                return runtime_component_for_v4_database_source(v4)
                    .map(|component| vec![component]);
            }
            let materialized = load_v4_materialization_with_reporter(
                &self.layout,
                workspace_name,
                &source.name,
                &installed.manifest_yaml,
                v4,
                &self.diagnostic_reporter,
            )?;
            runtime_component_for_v4_source(v4, &materialized)
                .map(|component| component.into_iter().collect())
                .map_err(|error| {
                    incompatible_materialization_error(
                        &source.name,
                        format!("failed to assemble runtime package: {error}"),
                    )
                })
        } else {
            Ok(runtime_components_from_manifest(&source_spec))
        }
    }
}

fn runtime_components_from_manifest(
    source_spec: &ValidatedSourceManifest,
) -> Vec<RuntimeSourceComponent> {
    if let Some(http) = source_spec.as_http() {
        return vec![RuntimeSourceComponent::Http(http.clone())];
    }
    if let Some(file) = source_spec.as_file() {
        return vec![RuntimeSourceComponent::File(file.clone())];
    }
    if let Some(mcp) = source_spec.as_mcp() {
        return vec![RuntimeSourceComponent::Mcp(mcp.clone())];
    }
    Vec::new()
}

fn catalog_info_from_components(components: &[RuntimeSourceComponent]) -> CatalogInfo {
    let mut catalog = CatalogInfo {
        tables: Vec::new(),
        table_functions: Vec::new(),
    };

    for component in components {
        match component {
            RuntimeSourceComponent::Database(_) => {
                // Database catalogs are live-discovered; snapshot construction must not connect.
            }
            RuntimeSourceComponent::Http(manifest) => {
                let schema_name = manifest.common.name.as_str();
                catalog.tables.extend(
                    manifest
                        .tables
                        .iter()
                        .map(|table| table_info(schema_name, &table.common)),
                );
                catalog.table_functions.extend(
                    manifest
                        .functions
                        .iter()
                        .map(|function| table_function_info(schema_name, function)),
                );
            }
            RuntimeSourceComponent::File(manifest) => {
                let schema_name = manifest.common.name.as_str();
                catalog.tables.extend(
                    manifest
                        .tables
                        .iter()
                        .map(|table| table_info(schema_name, &table.common)),
                );
            }
            RuntimeSourceComponent::Mcp(manifest) => {
                let schema_name = manifest.common.name.as_str();
                catalog.tables.extend(
                    manifest
                        .tables
                        .iter()
                        .map(|table| table_info(schema_name, &table.common)),
                );
                catalog.table_functions.extend(
                    manifest
                        .functions
                        .iter()
                        .map(|function| table_function_info(schema_name, &function.common)),
                );
            }
        }
    }

    catalog
}

fn table_info(schema_name: &str, common: &TableCommon) -> TableInfo {
    TableInfo {
        catalog_name: String::new(),
        schema_name: schema_name.to_string(),
        table_name: common.name.clone(),
        description: common.description.clone(),
        guide: common.guide.clone(),
        columns: column_infos_from_specs(&common.columns, &common.filters),
        required_filters: required_filter_names(&common.filters),
    }
}

// Keep this mapping aligned with coral-engine's catalog registration helpers:
// `registered_columns_from_specs` and `required_filter_names`. The local
// snapshot must mirror runtime-visible manifest metadata without compiling
// providers or inferring file schemas.
fn column_infos_from_specs(columns: &[ColumnSpec], filters: &[FilterSpec]) -> Vec<ColumnInfo> {
    columns
        .iter()
        .enumerate()
        .map(|(position, column)| {
            let filter = filters
                .iter()
                .find(|filter| filter.name == column.name.as_str());
            ColumnInfo {
                name: column.name.clone(),
                data_type: column.data_type.as_manifest_str().to_string(),
                nullable: column.nullable,
                is_virtual: column.r#virtual,
                is_required_filter: filter.is_some_and(|filter| filter.required),
                description: column.description.clone(),
                ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
            }
        })
        .collect()
}

fn required_filter_names(filters: &[FilterSpec]) -> Vec<String> {
    filters
        .iter()
        .filter(|filter| filter.required)
        .map(|filter| filter.name.clone())
        .collect()
}

fn table_function_info(schema_name: &str, function: &SourceTableFunctionSpec) -> TableFunctionInfo {
    TableFunctionInfo {
        schema_name: schema_name.to_string(),
        function_name: function.name.clone(),
        description: function.description.clone(),
        arguments: function
            .args
            .iter()
            .map(|argument| TableFunctionArgumentInfo {
                name: argument.name.clone(),
                required: argument.required,
                values: argument.values.clone(),
            })
            .collect(),
        result_columns: function
            .columns
            .iter()
            .map(|column| TableFunctionResultColumnInfo {
                name: column.name.clone(),
                data_type: column.data_type.as_manifest_str().to_string(),
                nullable: column.nullable,
                description: column.description.clone(),
            })
            .collect(),
        kind: function.kind,
        search_limits: function.search_limits.clone(),
    }
}

fn sort_catalog(catalog: &mut CatalogInfo) {
    catalog.tables.sort_by(|left, right| {
        (&left.schema_name, &left.table_name).cmp(&(&right.schema_name, &right.table_name))
    });
    catalog.table_functions.sort_by(|left, right| {
        (&left.schema_name, &left.function_name).cmp(&(&right.schema_name, &right.function_name))
    });
}

#[cfg(test)]
mod tests;
