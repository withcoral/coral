//! Query-time loading, validation, and execution over installed sources.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use coral_engine::{
    CatalogInfo, CoralQuery, CoreError, DescribeTableInfo, PreparedQueryRuntime, QueryExecution,
    QueryExecutionProvenance, QueryPlan, QueryRuntimeConfig, QueryRuntimeContext, QuerySource,
    SourceDecorator, SourceDecoratorError, SourceFailurePolicy, SourceInputResolver,
    SourceValidationReport, StatusCode, TableInfo, UdfRuntimeDefinition,
};
use coral_spec::{ManifestInputKind, ManifestInputSpec};
use opentelemetry::trace::Status as OtelStatus;
use serde_json::json;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::AppError;
use crate::catalog::model::{CatalogResolution, RuntimeRelationOwners};
use crate::credentials::{CredentialManager, CredentialSetId};
use crate::functions::manager::{FunctionListing, FunctionManager, ValidatedFunctionInstall};
use crate::query::QueryAttribution;
use crate::query::extensions::{EngineExtensionsProvider, engine_extensions_for_providers};
use crate::query::input_resolver::{
    CredentialRefreshingInputResolver, SourceCredentialSnapshot, StoredCredentialInputResolver,
};
use crate::search::observed::{SearchObservationHandle, SearchObservationSource};
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::materialization::{SourceDiagnosticReporter, SourceLoadDiagnosticStage};
use crate::sources::model::InstalledSource;
use crate::sources::runtime_package::{
    RuntimeContractFingerprint, query_source_from_installed_manifest,
};
use crate::sources::{SourceName, ensure_database_source_feature_enabled};
use crate::state::{AppConfig, AppStateLayout, ConfigStore};
use crate::task::id::TaskId;
use crate::telemetry::WORKSPACE_SPAN_ATTRIBUTE;
use crate::workspaces::{
    WorkspaceLifecycleLock, WorkspaceLifecycleRevision, WorkspaceManager, WorkspaceName,
    WorkspacePoolRegistry,
};

#[derive(Debug)]
pub(crate) enum QueryManagerError {
    App(AppError),
    Core(CoreError),
}

pub(crate) struct ValidatedSource {
    pub(crate) source: InstalledSource,
    pub(crate) report: SourceValidationReport,
}

#[derive(Clone, Copy)]
enum CredentialResolutionMode {
    Refreshing,
    StoredOnly,
}

#[derive(Debug, Clone)]
struct LoadedQuerySource {
    source: InstalledSource,
    query_source: QuerySource,
    runtime_contract_fingerprint: RuntimeContractFingerprint,
    credential_material: BTreeMap<String, String>,
}

struct QuerySourceLoad {
    loaded: Vec<LoadedQuerySource>,
    failed_source_names: BTreeSet<String>,
}

/// Maps each loaded runtime catalog/schema identity to its installed source.
fn runtime_relation_owners(
    loaded_sources: &[LoadedQuerySource],
    catalog: &CatalogInfo,
) -> Result<RuntimeRelationOwners, AppError> {
    let mut owners = BTreeMap::new();
    for loaded in loaded_sources {
        let schema_names = loaded.query_source.schema_names();
        let catalog_names = loaded.query_source.catalog_names();
        claim_runtime_relation_owners(
            &mut owners,
            loaded.source.name.as_str(),
            &schema_names,
            &catalog_names,
            catalog,
        )?;
    }
    Ok(owners)
}

fn claim_runtime_relation_owners(
    owners: &mut RuntimeRelationOwners,
    source_name: &str,
    schema_names: &[&str],
    catalog_names: &[&str],
    catalog: &CatalogInfo,
) -> Result<(), AppError> {
    let relation_namespaces = catalog
        .tables
        .iter()
        .map(|table| (table.catalog_name.as_deref(), table.schema_name.as_str()))
        .chain(catalog.table_functions.iter().map(|function| {
            (
                function.catalog_name.as_deref(),
                function.schema_name.as_str(),
            )
        }))
        .filter(|(catalog_name, schema_name)| {
            catalog_name.map_or_else(
                || schema_names.contains(schema_name),
                |catalog_name| catalog_names.contains(&catalog_name),
            )
        });
    for (catalog_name, schema_name) in relation_namespaces {
        let identity = (
            catalog_name.map(ToString::to_string),
            schema_name.to_string(),
        );
        match owners.entry(identity) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(source_name.to_string());
            }
            std::collections::btree_map::Entry::Occupied(entry)
                if entry.get().as_str() != source_name =>
            {
                let (catalog_name, schema_name) = entry.key();
                let namespace = catalog_name.as_ref().map_or_else(
                    || schema_name.clone(),
                    |catalog_name| format!("{catalog_name}.{schema_name}"),
                );
                return Err(AppError::InvalidInput(format!(
                    "catalog runtime namespace '{namespace}' is owned by both '{}' and '{source_name}'",
                    entry.get(),
                )));
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    Ok(())
}

#[derive(Clone, Default)]
struct CatalogFailureRecorder {
    failed_source_names: Arc<Mutex<BTreeSet<String>>>,
}

impl CatalogFailureRecorder {
    fn failed_source_names(&self) -> BTreeSet<String> {
        self.failed_source_names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl SourceDecorator for CatalogFailureRecorder {
    fn name(&self) -> &'static str {
        "catalog_failure_recorder"
    }

    fn supports_lazy_schemas(&self) -> bool {
        true
    }

    fn source_failed(
        &mut self,
        source: &QuerySource,
        _error: &CoreError,
    ) -> Result<SourceFailurePolicy, SourceDecoratorError> {
        self.failed_source_names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(source.source_name().to_string());
        Ok(SourceFailurePolicy::Ignore)
    }
}

#[derive(Clone)]
pub(crate) struct QueryManager {
    config_store: ConfigStore,
    workspace_manager: Arc<WorkspaceManager>,
    credential_manager: CredentialManager,
    function_manager: FunctionManager,
    lifecycle_lock: WorkspaceLifecycleLock,
    runtime_context: QueryRuntimeContext,
    layout: AppStateLayout,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    diagnostic_reporter: SourceDiagnosticReporter,
    search_observations: Option<SearchObservationHandle>,
    pool_registry: Arc<WorkspacePoolRegistry>,
    database_sources_enabled: bool,
}

impl QueryManager {
    #[cfg(test)]
    pub(crate) fn new_for_tests(
        config_store: ConfigStore,
        workspace_manager: WorkspaceManager,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> Self {
        Self::new(
            config_store,
            workspace_manager,
            credential_manager,
            runtime_context,
            layout,
            WorkspaceLifecycleLock::default(),
            engine_extensions_providers,
        )
    }

    #[cfg(test)]
    pub(crate) fn new(
        config_store: ConfigStore,
        workspace_manager: WorkspaceManager,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        lifecycle_lock: WorkspaceLifecycleLock,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> Self {
        Self::with_diagnostic_reporter(
            config_store,
            workspace_manager,
            credential_manager,
            runtime_context,
            layout,
            lifecycle_lock,
            engine_extensions_providers,
            SourceDiagnosticReporter::default(),
            Arc::new(WorkspacePoolRegistry::default()),
        )
        .with_database_sources_enabled(true)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the composition root passes shared lifecycle and diagnostic state explicitly"
    )]
    pub(crate) fn with_diagnostic_reporter(
        config_store: ConfigStore,
        workspace_manager: WorkspaceManager,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        lifecycle_lock: WorkspaceLifecycleLock,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        diagnostic_reporter: SourceDiagnosticReporter,
        pool_registry: Arc<WorkspacePoolRegistry>,
    ) -> Self {
        let function_manager =
            FunctionManager::new(config_store.clone(), &layout, lifecycle_lock.clone());
        Self {
            config_store,
            workspace_manager: Arc::new(workspace_manager),
            credential_manager,
            function_manager,
            lifecycle_lock,
            runtime_context,
            layout,
            engine_extensions_providers,
            diagnostic_reporter,
            search_observations: None,
            pool_registry,
            database_sources_enabled: false,
        }
    }

    pub(crate) fn with_database_sources_enabled(mut self, enabled: bool) -> Self {
        self.database_sources_enabled = enabled;
        self
    }

    pub(crate) fn with_search_observation_handle(
        mut self,
        search_observations: SearchObservationHandle,
    ) -> Self {
        self.search_observations = Some(search_observations);
        self
    }

    pub(crate) async fn list_tables(
        &self,
        workspace_name: &WorkspaceName,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
        attribution: &QueryAttribution,
    ) -> Result<Vec<TableInfo>, QueryManagerError> {
        let trace_sql = list_tables_trace_sql(catalog_filter, schema_filter, table_filter);
        run_query_operation(
            QueryOperation::ListTables,
            workspace_name,
            &trace_sql,
            attribution.task_id.as_ref(),
            async {
                let (source_load, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let runtime = self
                    .prepared_runtime_with_udfs(
                        workspace_name,
                        &source_load.loaded,
                        &config,
                        CredentialResolutionMode::StoredOnly,
                        SourceObservationMode::Disabled,
                    )
                    .await?;
                runtime
                    .list_tables(catalog_filter, schema_filter, table_filter)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |tables| Some(u64::try_from(tables.len()).unwrap_or(u64::MAX)),
            |_, _| {},
        )
        .await
    }

    pub(crate) async fn list_catalog(
        &self,
        workspace_name: &WorkspaceName,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
        attribution: &QueryAttribution,
    ) -> Result<CatalogInfo, QueryManagerError> {
        Ok(self
            .resolve_catalog(workspace_name, catalog_filter, schema_filter, attribution)
            .await?
            .catalog)
    }

    pub(crate) async fn resolve_catalog(
        &self,
        workspace_name: &WorkspaceName,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
        attribution: &QueryAttribution,
    ) -> Result<CatalogResolution, QueryManagerError> {
        let trace_sql = list_catalog_trace_sql(catalog_filter, schema_filter);
        run_query_operation(
            QueryOperation::ListCatalog,
            workspace_name,
            &trace_sql,
            attribution.task_id.as_ref(),
            async {
                let (source_load, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let failure_recorder = CatalogFailureRecorder::default();
                let runtime = self
                    .prepared_catalog_runtime_with_udfs(
                        workspace_name,
                        &source_load.loaded,
                        &config,
                        failure_recorder.clone(),
                    )
                    .await?;
                let mut failed_source_names = source_load.failed_source_names;
                failed_source_names.extend(failure_recorder.failed_source_names());
                let catalog = runtime
                    .list_catalog(catalog_filter, schema_filter)
                    .await
                    .map_err(QueryManagerError::Core)?;
                let runtime_relation_owners =
                    runtime_relation_owners(&source_load.loaded, &catalog)
                        .map_err(QueryManagerError::App)?;
                Ok(CatalogResolution {
                    catalog,
                    failed_source_names,
                    runtime_relation_owners,
                })
            },
            |resolution| {
                Some(
                    u64::try_from(
                        resolution
                            .catalog
                            .tables
                            .len()
                            .saturating_add(resolution.catalog.table_functions.len()),
                    )
                    .unwrap_or(u64::MAX),
                )
            },
            |_, _| {},
        )
        .await
    }

    pub(crate) async fn describe_table(
        &self,
        workspace_name: &WorkspaceName,
        catalog_name: Option<&str>,
        schema_name: &str,
        table_name: &str,
        attribution: &QueryAttribution,
    ) -> Result<DescribeTableInfo, QueryManagerError> {
        let trace_sql = describe_table_trace_sql(catalog_name, schema_name, table_name);
        run_query_operation(
            QueryOperation::DescribeTable,
            workspace_name,
            &trace_sql,
            attribution.task_id.as_ref(),
            async {
                let (source_load, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let runtime = self
                    .prepared_runtime_with_udfs(
                        workspace_name,
                        &source_load.loaded,
                        &config,
                        CredentialResolutionMode::Refreshing,
                        SourceObservationMode::Disabled,
                    )
                    .await?;
                runtime
                    .describe_table(catalog_name, schema_name, table_name)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |_| None,
            |_, _| {},
        )
        .await
    }

    pub(crate) async fn execute_sql(
        &self,
        workspace_name: &WorkspaceName,
        sql: &str,
        attribution: &QueryAttribution,
    ) -> Result<QueryExecution, QueryManagerError> {
        run_query_operation(
            QueryOperation::ExecuteSql,
            workspace_name,
            sql,
            attribution.task_id.as_ref(),
            async {
                let (source_load, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let runtime = self
                    .prepared_runtime_with_udfs(
                        workspace_name,
                        &source_load.loaded,
                        &config,
                        CredentialResolutionMode::Refreshing,
                        SourceObservationMode::Enabled,
                    )
                    .await?;
                runtime
                    .execute_sql(sql)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |execution| Some(u64::try_from(execution.row_count()).unwrap_or(u64::MAX)),
            |span, execution| record_query_provenance(span, execution.provenance()),
        )
        .await
    }

    pub(crate) async fn explain_sql(
        &self,
        workspace_name: &WorkspaceName,
        sql: &str,
        attribution: &QueryAttribution,
    ) -> Result<QueryPlan, QueryManagerError> {
        run_query_operation(
            QueryOperation::ExplainSql,
            workspace_name,
            sql,
            attribution.task_id.as_ref(),
            async {
                let (source_load, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let runtime = self
                    .prepared_runtime_with_udfs(
                        workspace_name,
                        &source_load.loaded,
                        &config,
                        CredentialResolutionMode::Refreshing,
                        SourceObservationMode::Disabled,
                    )
                    .await?;
                runtime
                    .explain_sql(sql)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |_| None,
            |_, _| {},
        )
        .await
    }

    pub(crate) async fn validate_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<ValidatedSource, QueryManagerError> {
        let (source, loaded_source, version, config) = {
            self.require_workspace(workspace_name)
                .await
                .map_err(QueryManagerError::App)?;
            let _state_lock = self
                .config_store
                .state_lock_shared()
                .map_err(QueryManagerError::App)?;
            let config = self
                .config_store
                .load_config_unlocked()
                .map_err(QueryManagerError::App)?;
            let source = config
                .get_source(workspace_name, source_name)
                .ok_or_else(|| AppError::SourceNotFound(format!("{workspace_name}:{source_name}")))
                .map_err(QueryManagerError::App)?;
            let (loaded_source, version) = self
                .load_query_source(workspace_name, &source)
                .map_err(QueryManagerError::App)?;
            (source, loaded_source, version, config)
        };
        let runtime = self
            .runtime_config_without_source_observations(
                workspace_name,
                std::slice::from_ref(&loaded_source),
                &config,
            )
            .map_err(QueryManagerError::App)?;
        let report = CoralQuery::validate_source(
            &loaded_source.query_source,
            runtime,
            loaded_source.query_source.test_queries(),
        )
        .await
        .map_err(QueryManagerError::Core)?;
        let mut source = source;
        source.version = version;

        Ok(ValidatedSource { source, report })
    }

    async fn load_query_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<(QuerySourceLoad, AppConfig), AppError> {
        self.require_workspace(workspace_name).await?;
        let _state_lock = self.config_store.state_lock_shared()?;
        let config = self.config_store.load_config_unlocked()?;
        let sources = self.load_query_sources_from_config(workspace_name, &config);
        Ok((sources, config))
    }

    async fn require_workspace(&self, workspace_name: &WorkspaceName) -> Result<(), AppError> {
        self.workspace_manager
            .require_workspace(workspace_name)
            .await
    }

    fn load_query_sources_from_config(
        &self,
        workspace_name: &WorkspaceName,
        config: &AppConfig,
    ) -> QuerySourceLoad {
        let span = tracing::info_span!(
            "coral.app.query_sources.load",
            workspace = tracing::field::Empty,
            source.count = tracing::field::Empty,
        );
        span.record(WORKSPACE_SPAN_ATTRIBUTE, workspace_name.as_str());
        let _guard = span.enter();
        let mut loaded_sources = Vec::new();
        let mut failed_source_names = BTreeSet::new();
        for source in config.workspace_sources(workspace_name) {
            match self.load_query_source(workspace_name, &source) {
                Ok((loaded_source, _version)) => {
                    self.diagnostic_reporter.clear_source_load_failure(
                        SourceLoadDiagnosticStage::Query,
                        workspace_name,
                        &source.name,
                    );
                    loaded_sources.push(loaded_source);
                }
                Err(error) => {
                    failed_source_names.insert(source.name.to_string());
                    self.diagnostic_reporter.report_source_load_failure(
                        SourceLoadDiagnosticStage::Query,
                        workspace_name,
                        &source.name,
                        &error.to_string(),
                    );
                }
            }
        }
        span.record("source.count", loaded_sources.len());
        QuerySourceLoad {
            loaded: loaded_sources,
            failed_source_names,
        }
    }

    fn load_query_source(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<(LoadedQuerySource, Option<String>), AppError> {
        let installed = resolve_installed_manifest(workspace_name, source, &self.layout)?;
        let source_spec = &installed.source_spec;
        ensure_database_source_feature_enabled(source_spec, self.database_sources_enabled)?;
        validate_required_variables(source, source_spec.declared_inputs())?;
        let stored_secrets =
            if let Some(credential_storage) = source.credential_storage_for_material() {
                let credential_set_id = CredentialSetId::for_source(&source.name);
                self.credential_manager.read_material(
                    workspace_name,
                    &credential_set_id,
                    credential_storage,
                )?
            } else {
                BTreeMap::new()
            };
        let mut resolved_secrets = BTreeMap::new();
        let missing_secrets: Vec<String> = source_spec
            .required_secret_names()
            .into_iter()
            .filter(|name| !stored_secrets.contains_key(name))
            .collect();
        if let Some((first, rest)) = missing_secrets.split_first() {
            let detail = if rest.is_empty() {
                format!("secret '{first}'")
            } else {
                format!("secret '{first}' and {} other(s)", rest.len())
            };
            return Err(AppError::MissingSourceInputs {
                source_name: source.name.to_string(),
                detail,
            });
        }
        for secret_name in source_spec.declared_secret_names() {
            if let Some(value) = stored_secrets.get(&secret_name) {
                resolved_secrets.insert(secret_name, value.clone());
            }
        }
        let loaded_runtime = query_source_from_installed_manifest(
            &self.layout,
            workspace_name,
            source,
            &installed,
            &self.diagnostic_reporter,
            resolved_secrets,
        )?;
        Ok((
            LoadedQuerySource {
                source: source.clone(),
                query_source: loaded_runtime.query_source,
                runtime_contract_fingerprint: loaded_runtime.runtime_contract_fingerprint,
                credential_material: stored_secrets,
            },
            installed.candidate.version,
        ))
    }

    #[cfg(test)]
    fn runtime_config(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[LoadedQuerySource],
        config: &AppConfig,
    ) -> Result<QueryRuntimeConfig, AppError> {
        self.runtime_config_with_credential_mode(
            workspace_name,
            selected_sources,
            config,
            CredentialResolutionMode::Refreshing,
            SourceObservationMode::Enabled,
        )
    }

    fn runtime_config_without_source_observations(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[LoadedQuerySource],
        config: &AppConfig,
    ) -> Result<QueryRuntimeConfig, AppError> {
        self.runtime_config_with_credential_mode(
            workspace_name,
            selected_sources,
            config,
            CredentialResolutionMode::Refreshing,
            SourceObservationMode::Disabled,
        )
    }

    fn runtime_config_with_credential_mode(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[LoadedQuerySource],
        config: &AppConfig,
        credential_resolution_mode: CredentialResolutionMode,
        source_observation_mode: SourceObservationMode,
    ) -> Result<QueryRuntimeConfig, AppError> {
        let query_sources = query_sources_from_loaded(selected_sources);
        let mut extensions =
            engine_extensions_for_providers(&self.engine_extensions_providers, &query_sources);
        if matches!(source_observation_mode, SourceObservationMode::Enabled)
            && let Some(search_observations) = &self.search_observations
        {
            let observation_sources = selected_sources
                .iter()
                .map(|source| {
                    SearchObservationSource::new(
                        &source.query_source,
                        source.runtime_contract_fingerprint.as_str(),
                        source.source.credential_revision,
                    )
                })
                .collect::<Vec<_>>();
            let observed_extensions =
                search_observations.extensions_for(workspace_name, &observation_sources);
            extensions
                .source_observation_publishers
                .extend(observed_extensions.source_observation_publishers);
        }
        let provider_input_resolver = extensions.source_input_resolver.take();
        let source_credentials = selected_sources
            .iter()
            .map(|source| {
                (
                    source.query_source.source_name().to_string(),
                    SourceCredentialSnapshot {
                        source: source.source.clone(),
                        material: source.credential_material.clone(),
                    },
                )
            })
            .collect();
        let input_resolver: Arc<dyn SourceInputResolver> = match credential_resolution_mode {
            CredentialResolutionMode::Refreshing => {
                Arc::new(CredentialRefreshingInputResolver::new(
                    workspace_name.clone(),
                    self.config_store.clone(),
                    self.credential_manager.clone(),
                    source_credentials,
                    provider_input_resolver,
                ))
            }
            CredentialResolutionMode::StoredOnly => Arc::new(StoredCredentialInputResolver::new(
                source_credentials,
                provider_input_resolver,
            )),
        };
        extensions.source_input_resolver = Some(input_resolver);
        let mut runtime_context = self.runtime_context.clone();
        runtime_context.trace_context = Some(tracing::Span::current().context());
        let mut runtime = QueryRuntimeConfig::new(runtime_context, extensions);
        runtime.database_pool_registry = self.pool_registry.for_workspace(workspace_name);
        let selected_source_names = selected_sources
            .iter()
            .map(|source| source.query_source.source_name().to_string())
            .collect::<Vec<_>>();
        runtime.memory = config.memory_config()?;
        runtime.dependent_join = config.dependent_join_config(&selected_source_names)?;
        Ok(runtime)
    }

    pub(crate) async fn list_functions(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<FunctionListing>, QueryManagerError> {
        let (source_load, config) = self
            .load_query_sources(workspace_name)
            .await
            .map_err(QueryManagerError::App)?;
        let sources = query_sources_from_loaded(&source_load.loaded);
        self.function_manager
            .list_functions(workspace_name, &sources, || {
                self.runtime_config_without_source_observations(
                    workspace_name,
                    &source_load.loaded,
                    &config,
                )
            })
            .await
            .map_err(QueryManagerError::App)
    }

    pub(crate) fn function_manager(&self) -> FunctionManager {
        self.function_manager.clone()
    }

    #[cfg(test)]
    pub(crate) async fn validate_udf_sql(
        &self,
        workspace_name: &WorkspaceName,
        raw_sql: &str,
    ) -> Result<UdfRuntimeDefinition, QueryManagerError> {
        self.require_workspace(workspace_name)
            .await
            .map_err(QueryManagerError::App)?;
        let _lifecycle_snapshot = self.lifecycle_lock.snapshot();
        let (loaded_sources, config) = self.load_function_validation_sources(workspace_name)?;
        self.validate_udf_sql_against_snapshot(workspace_name, raw_sql, &loaded_sources, &config)
            .await
    }

    pub(crate) async fn add_user_function(
        &self,
        workspace_name: &WorkspaceName,
        raw_sql: &str,
    ) -> Result<UdfRuntimeDefinition, QueryManagerError> {
        for _ in 0..2 {
            let revision = self.lifecycle_lock.snapshot().revision();
            self.require_workspace(workspace_name)
                .await
                .map_err(QueryManagerError::App)?;
            let Some((loaded_sources, config)) =
                self.function_validation_snapshot_if_unchanged(workspace_name, revision)?
            else {
                continue;
            };
            let runtime_function = self
                .validate_udf_sql_against_snapshot(
                    workspace_name,
                    raw_sql,
                    &loaded_sources,
                    &config,
                )
                .await?;
            match self
                .function_manager
                .install_validated_user_function_if_unchanged(
                    workspace_name,
                    raw_sql,
                    &runtime_function,
                    revision,
                )
                .map_err(QueryManagerError::App)?
            {
                ValidatedFunctionInstall::Installed => return Ok(runtime_function),
                ValidatedFunctionInstall::WorkspaceChanged => {}
            }
        }
        Err(QueryManagerError::App(AppError::FailedPrecondition(
            "workspace changed repeatedly while the function was being validated; retry the add"
                .to_string(),
        )))
    }

    fn function_validation_snapshot_if_unchanged(
        &self,
        workspace_name: &WorkspaceName,
        revision: WorkspaceLifecycleRevision,
    ) -> Result<Option<(Vec<LoadedQuerySource>, AppConfig)>, QueryManagerError> {
        let lifecycle_snapshot = self.lifecycle_lock.snapshot();
        if lifecycle_snapshot.revision() != revision {
            return Ok(None);
        }
        let (loaded_sources, config) = self.load_function_validation_sources(workspace_name)?;
        Ok(Some((loaded_sources, config)))
    }

    fn load_function_validation_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<(Vec<LoadedQuerySource>, AppConfig), QueryManagerError> {
        let (loaded_sources, config) = {
            let _state_lock = self
                .config_store
                .state_lock_shared()
                .map_err(QueryManagerError::App)?;
            let config = self
                .config_store
                .load_config_unlocked()
                .map_err(QueryManagerError::App)?;
            let source_load = self.load_query_sources_from_config(workspace_name, &config);
            (source_load.loaded, config)
        };
        Ok((loaded_sources, config))
    }

    async fn validate_udf_sql_against_snapshot(
        &self,
        workspace_name: &WorkspaceName,
        raw_sql: &str,
        loaded_sources: &[LoadedQuerySource],
        config: &AppConfig,
    ) -> Result<UdfRuntimeDefinition, QueryManagerError> {
        let sources = query_sources_from_loaded(loaded_sources);
        self.function_manager
            .validate_user_function_sql(
                workspace_name,
                &sources,
                || {
                    self.runtime_config_without_source_observations(
                        workspace_name,
                        loaded_sources,
                        config,
                    )
                },
                raw_sql,
            )
            .await
            .map_err(QueryManagerError::App)
    }

    async fn prepared_runtime_with_udfs(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[LoadedQuerySource],
        config: &AppConfig,
        credential_resolution_mode: CredentialResolutionMode,
        source_observation_mode: SourceObservationMode,
    ) -> Result<PreparedQueryRuntime, QueryManagerError> {
        let runtime_config = self
            .runtime_config_with_credential_mode(
                workspace_name,
                selected_sources,
                config,
                credential_resolution_mode,
                source_observation_mode,
            )
            .map_err(QueryManagerError::App)?;
        self.prepare_runtime_with_udfs(workspace_name, selected_sources, runtime_config)
            .await
    }

    async fn prepared_catalog_runtime_with_udfs(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[LoadedQuerySource],
        config: &AppConfig,
        failure_recorder: CatalogFailureRecorder,
    ) -> Result<PreparedQueryRuntime, QueryManagerError> {
        let mut runtime_config = self
            .runtime_config_with_credential_mode(
                workspace_name,
                selected_sources,
                config,
                CredentialResolutionMode::StoredOnly,
                SourceObservationMode::Disabled,
            )
            .map_err(QueryManagerError::App)?;
        runtime_config
            .extensions
            .source_decorators
            .insert(0, Box::new(failure_recorder));
        self.prepare_runtime_with_udfs(workspace_name, selected_sources, runtime_config)
            .await
    }

    async fn prepare_runtime_with_udfs(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[LoadedQuerySource],
        runtime_config: QueryRuntimeConfig,
    ) -> Result<PreparedQueryRuntime, QueryManagerError> {
        let query_sources = query_sources_from_loaded(selected_sources);
        let runtime = CoralQuery::prepare(&query_sources, runtime_config)
            .await
            .map_err(QueryManagerError::Core)?;
        let functions = self
            .function_manager
            .load_runtime_udfs(workspace_name, &query_sources, &runtime)
            .await
            .map_err(QueryManagerError::App)?;
        runtime
            .with_udfs(functions)
            .await
            .map_err(QueryManagerError::Core)
    }
}

fn query_sources_from_loaded(loaded_sources: &[LoadedQuerySource]) -> Vec<QuerySource> {
    loaded_sources
        .iter()
        .map(|source| source.query_source.clone())
        .collect()
}

#[derive(Clone, Copy)]
enum QueryOperation {
    ExecuteSql,
    ExplainSql,
    ListTables,
    ListCatalog,
    DescribeTable,
}

#[derive(Clone, Copy)]
enum SourceObservationMode {
    Enabled,
    Disabled,
}

impl QueryOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExecuteSql => "execute_sql",
            Self::ExplainSql => "explain_sql",
            Self::ListTables => "list_tables",
            Self::ListCatalog => "list_catalog",
            Self::DescribeTable => "describe_table",
        }
    }
}

fn list_tables_trace_sql(
    catalog_filter: Option<&str>,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
) -> String {
    match (catalog_filter, schema_filter, table_filter) {
        (Some(catalog), Some(schema), Some(table)) => {
            format!("LIST TABLES {catalog}.{schema}.{table}")
        }
        (Some(catalog), Some(schema), None) => format!("LIST TABLES {catalog}.{schema}.*"),
        (None, Some(schema), Some(table)) => format!("LIST TABLES {schema}.{table}"),
        (None, Some(schema), None) => format!("LIST TABLES {schema}.*"),
        (Some(catalog), None, Some(table)) => format!("LIST TABLES {catalog}.*.{table}"),
        (Some(catalog), None, None) => format!("LIST TABLES {catalog}.*.*"),
        (None, None, Some(table)) => format!("LIST TABLES *.{table}"),
        (None, None, None) => "LIST TABLES *.*".to_string(),
    }
}

fn list_catalog_trace_sql(catalog_filter: Option<&str>, schema_filter: Option<&str>) -> String {
    match (catalog_filter, schema_filter) {
        (Some(catalog), Some(schema)) => format!("LIST CATALOG {catalog}.{schema}"),
        (Some(catalog), None) => format!("LIST CATALOG {catalog}.*"),
        (None, Some(schema)) => format!("LIST CATALOG {schema}"),
        (None, None) => "LIST CATALOG".to_string(),
    }
}

fn describe_table_trace_sql(
    catalog_name: Option<&str>,
    schema_name: &str,
    table_name: &str,
) -> String {
    catalog_name.map_or_else(
        || format!("DESCRIBE TABLE {schema_name}.{table_name}"),
        |catalog| format!("DESCRIBE TABLE {catalog}.{schema_name}.{table_name}"),
    )
}

async fn run_query_operation<T, Fut, RowCount>(
    operation: QueryOperation,
    workspace_name: &WorkspaceName,
    sql: &str,
    task_id: Option<&TaskId>,
    query: Fut,
    row_count: RowCount,
    record_success_fields: impl FnOnce(&tracing::Span, &T),
) -> Result<T, QueryManagerError>
where
    Fut: Future<Output = Result<T, QueryManagerError>>,
    RowCount: FnOnce(&T) -> Option<u64>,
{
    let started_at = Instant::now();
    let query_span = create_query_span(operation, workspace_name, sql, task_id);
    let result = query.instrument(query_span.clone()).await;

    let row_count = result.as_ref().ok().and_then(row_count);
    crate::telemetry::metrics::metrics().record_query(
        operation.as_str(),
        started_at.elapsed(),
        row_count,
        result.is_ok(),
    );

    if result.is_ok() {
        query_span.record("status", "ok");
        query_span.set_status(OtelStatus::Ok);
        if let Some(row_count) = row_count {
            query_span.record("row_count", row_count);
        }
        if let Ok(value) = &result {
            record_success_fields(&query_span, value);
        }
    } else if let Err(error) = &result {
        let error_kind = query_error_kind(error);
        let error_type = query_error_type(error);
        let error_message = query_error_message(error);
        query_span.record("status", "error");
        query_span.record("error.kind", error_kind);
        query_span.record("error.type", error_type.as_str());
        query_span.record("exception.message", error_message.as_str());
        query_span.set_status(OtelStatus::error(error_message));
    }

    result
}

fn create_query_span(
    operation: QueryOperation,
    workspace_name: &WorkspaceName,
    sql: &str,
    task_id: Option<&TaskId>,
) -> tracing::Span {
    let operation = operation.as_str();
    let span = tracing::info_span!(
        "coral.query",
        otel.name = "coral.query",
        operation = operation,
        workspace = tracing::field::Empty,
        sql = %sql,
        task.id = tracing::field::Empty,
        row_count = tracing::field::Empty,
        coral.query.sources = tracing::field::Empty,
        coral.query.tables = tracing::field::Empty,
        coral.query.table_functions = tracing::field::Empty,
        status = tracing::field::Empty,
        error.kind = tracing::field::Empty,
        error.type = tracing::field::Empty,
        exception.message = tracing::field::Empty,
    );
    if let Some(task_id) = task_id {
        span.record("task.id", tracing::field::display(task_id));
    }
    span.record(WORKSPACE_SPAN_ATTRIBUTE, workspace_name.as_str());
    span
}

fn record_query_provenance(span: &tracing::Span, provenance: &QueryExecutionProvenance) {
    record_json_field(
        span,
        crate::telemetry::QUERY_TRACE_SOURCES_ATTR,
        provenance.sources(),
    );
    record_json_field(
        span,
        crate::telemetry::QUERY_TRACE_TABLES_ATTR,
        &provenance
            .tables()
            .iter()
            .map(|table| {
                json!({
                    "source_name": table.source_name(),
                    "schema_name": table.schema_name(),
                    "table_name": table.table_name(),
                })
            })
            .collect::<Vec<_>>(),
    );
    record_json_field(
        span,
        crate::telemetry::QUERY_TRACE_TABLE_FUNCTIONS_ATTR,
        &provenance
            .table_functions()
            .iter()
            .map(|function| {
                json!({
                    "source_name": function.source_name(),
                    "schema_name": function.schema_name(),
                    "function_name": function.function_name(),
                })
            })
            .collect::<Vec<_>>(),
    );
}

fn record_json_field<T: serde::Serialize + ?Sized>(
    span: &tracing::Span,
    field: &'static str,
    value: &T,
) {
    if let Ok(encoded) = serde_json::to_string(value) {
        span.record(field, encoded.as_str());
    }
}

fn query_error_kind(error: &QueryManagerError) -> &'static str {
    match error {
        QueryManagerError::App(_) => "app",
        QueryManagerError::Core(_) => "core",
    }
}

fn query_error_type(error: &QueryManagerError) -> String {
    match error {
        QueryManagerError::App(error) => app_error_type(error).to_string(),
        QueryManagerError::Core(error) => core_error_type(error),
    }
}

fn query_error_message(error: &QueryManagerError) -> String {
    match error {
        QueryManagerError::App(error) => error.to_string(),
        QueryManagerError::Core(CoreError::QueryFailure(error)) => error.summary().to_string(),
        QueryManagerError::Core(error) => error.to_string(),
    }
}

fn app_error_type(error: &AppError) -> &'static str {
    match error {
        AppError::Unauthenticated(_) => "UNAUTHENTICATED",
        AppError::SourceNotFound(_) => "SOURCE_NOT_FOUND",
        AppError::FunctionNotFound(_) => "FUNCTION_NOT_FOUND",
        AppError::WorkspaceNotFound(_) => "WORKSPACE_NOT_FOUND",
        AppError::WorkspaceAlreadyExists(_) => "WORKSPACE_ALREADY_EXISTS",
        AppError::InvalidInput(_) => "INVALID_INPUT",
        AppError::FailedPrecondition(_) => "FAILED_PRECONDITION",
        AppError::MissingSourceInputs { .. } => "MISSING_SOURCE_INPUTS",
        AppError::UnsupportedV4IdentityRequirements { .. } => {
            "UNSUPPORTED_V4_IDENTITY_REQUIREMENTS"
        }
        AppError::MissingOrIncompatibleV4Materialization { .. } => {
            "MISSING_OR_INCOMPATIBLE_V4_MATERIALIZATION"
        }
        AppError::IncompatibleInstalledV4Manifest { .. } => "INCOMPATIBLE_INSTALLED_V4_MANIFEST",
        AppError::InvalidV4ProjectionOverride { .. } => "INVALID_V4_PROJECTION_OVERRIDE",
        AppError::InvalidV4OperationMetadataOverride { .. } => {
            "INVALID_V4_OPERATION_METADATA_OVERRIDE"
        }
        AppError::CredentialRefresh(_) => "CREDENTIAL_REFRESH",
        AppError::Unavailable(_) => "UNAVAILABLE",
        AppError::ResourceExhausted(_) => "RESOURCE_EXHAUSTED",
        AppError::Internal(_) => "INTERNAL",
        AppError::Io(_) => "IO",
        AppError::Yaml(_) => "YAML",
        AppError::TomlDecode(_) | AppError::TomlEditDecode(_) => "TOML_DECODE",
        AppError::TomlEncode(_) => "TOML_ENCODE",
        AppError::Json(_) => "JSON",
        AppError::Transport(_) => "TRANSPORT",
        AppError::TaskJoin(_) => "TASK_JOIN",
        AppError::Credentials(_) => "CREDENTIALS",
        AppError::Database(_) => "DATABASE",
        AppError::MissingConfigDir => "MISSING_CONFIG_DIR",
    }
}

fn core_error_type(error: &CoreError) -> String {
    match error {
        CoreError::QueryFailure(error) => error.reason().to_string(),
        error => status_code_error_type(error.status_code()).to_string(),
    }
}

fn status_code_error_type(status: StatusCode) -> &'static str {
    match status {
        StatusCode::InvalidArgument => "INVALID_ARGUMENT",
        StatusCode::NotFound => "NOT_FOUND",
        StatusCode::FailedPrecondition => "FAILED_PRECONDITION",
        StatusCode::Unavailable => "UNAVAILABLE",
        StatusCode::Unimplemented => "UNIMPLEMENTED",
        StatusCode::Internal => "INTERNAL",
    }
}

fn validate_required_variables(
    source: &InstalledSource,
    inputs: &[ManifestInputSpec],
) -> Result<(), AppError> {
    let missing: Vec<_> = inputs
        .iter()
        .filter(|input| {
            input.kind == ManifestInputKind::Variable
                && input.required
                && !source.variables.contains_key(&input.key)
        })
        .collect();
    if let Some((first, rest)) = missing.split_first() {
        let detail = if rest.is_empty() {
            format!("variable '{}'", first.key)
        } else {
            format!("variable '{}' and {} other(s)", first.key, rest.len())
        };
        return Err(AppError::MissingSourceInputs {
            source_name: source.name.to_string(),
            detail,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use coral_engine::{
        EngineExtensions, QueryExecution, QueryExecutionProvenance, QueryTableFunctionUsage,
        QueryTableUsage, SourceDecorator, SourceDecoratorError, SourceInputResolutionContext,
        SourceInputResolver, SourceInputResolverError,
    };
    use coral_spec::parse_source_manifest_yaml;
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::credentials::{CredentialStorageKind, CredentialStoragePreference, CredentialStore};
    use crate::identity::Principal;
    use crate::request_context::RequestContext;
    use crate::sources::manager::{ImportSourceCommand, SourceBindings, SourceManager};
    use crate::sources::model::SourceOrigin;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, run_state_migrations};
    use crate::task::manager::TaskManager;
    use crate::task::store::TaskStore;

    struct QueryManagerFixture {
        _temp: TempDir,
        manager: QueryManager,
        db: Arc<CoralDb>,
    }

    #[test]
    fn runtime_relation_owners_distinguish_catalogs_with_the_same_schema() {
        let catalog = CatalogInfo {
            tables: vec![
                TableInfo {
                    catalog_name: Some("warehouse".to_string()),
                    schema_name: "public".to_string(),
                    table_name: "orders".to_string(),
                    description: String::new(),
                    guide: String::new(),
                    columns: Vec::new(),
                    required_filters: Vec::new(),
                },
                TableInfo {
                    catalog_name: Some("analytics".to_string()),
                    schema_name: "public".to_string(),
                    table_name: "events".to_string(),
                    description: String::new(),
                    guide: String::new(),
                    columns: Vec::new(),
                    required_filters: Vec::new(),
                },
            ],
            table_functions: Vec::new(),
        };
        let mut owners = RuntimeRelationOwners::new();
        claim_runtime_relation_owners(
            &mut owners,
            "warehouse_source",
            &[],
            &["warehouse"],
            &catalog,
        )
        .expect("claim warehouse");
        claim_runtime_relation_owners(
            &mut owners,
            "analytics_source",
            &[],
            &["analytics"],
            &catalog,
        )
        .expect("claim analytics");

        assert_eq!(
            owners.get(&(Some("warehouse".to_string()), "public".to_string())),
            Some(&"warehouse_source".to_string())
        );
        assert_eq!(
            owners.get(&(Some("analytics".to_string()), "public".to_string())),
            Some(&"analytics_source".to_string())
        );
    }

    async fn query_manager_with(
        runtime_context: QueryRuntimeContext,
        providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> QueryManagerFixture {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = test_db(&layout, &config_store).await;
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let manager = QueryManager::new_for_tests(
            config_store,
            workspace_manager,
            credential_manager,
            runtime_context,
            layout,
            providers,
        );
        QueryManagerFixture {
            _temp: temp,
            manager,
            db,
        }
    }

    async fn query_manager_with_unavailable_keychain() -> QueryManagerFixture {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = test_db(&layout, &config_store).await;
        let credential_store = CredentialStore::with_unavailable_keychain_for_test(
            layout.clone(),
            CredentialStoragePreference::Keychain,
        );
        let credential_manager = CredentialManager::new(credential_store);
        let workspace_manager = WorkspaceManager::new_for_tests(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
            None,
            Arc::clone(&db),
        );
        let manager = QueryManager::new_for_tests(
            config_store,
            workspace_manager,
            credential_manager,
            QueryRuntimeContext::default(),
            layout,
            Vec::new(),
        );
        QueryManagerFixture {
            _temp: temp,
            manager,
            db,
        }
    }

    fn install_keychain_github_source(config_store: &ConfigStore, workspace_name: &WorkspaceName) {
        config_store
            .upsert_source(
                workspace_name,
                InstalledSource {
                    name: SourceName::parse("github").expect("source name"),
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: vec!["GITHUB_TOKEN".to_string()],
                    credential_storage: Some(CredentialStorageKind::Keychain),
                    credential_revision: uuid::Uuid::default(),
                    origin: SourceOrigin::Bundled,
                },
            )
            .expect("persist source");
    }

    async fn test_db(layout: &AppStateLayout, config_store: &ConfigStore) -> Arc<CoralDb> {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        run_state_migrations(&db, config_store, layout)
            .await
            .expect("run state migrations");
        Arc::new(db)
    }

    async fn active_task_context(db: &Arc<CoralDb>) -> (TaskManager, RequestContext, String) {
        let task = TaskManager::new(TaskStore::new(Arc::clone(db)));
        let principal = Principal::local();
        let started = task
            .start_task(
                WorkspaceName::default(),
                principal.clone(),
                "Exercise query attribution".to_string(),
            )
            .await
            .expect("start attributed task");
        let task_id = started.id.to_string();
        let context = RequestContext::new(principal).with_task_id(Some(started.id));
        (task, context, task_id)
    }

    fn assert_workspace_not_found(error: AppError, workspace_name: &WorkspaceName) {
        match error {
            AppError::WorkspaceNotFound(actual) => assert_eq!(actual, workspace_name.as_str()),
            error => panic!("expected WorkspaceNotFound for '{workspace_name}', got {error}"),
        }
    }

    #[tokio::test]
    async fn load_query_sources_fails_closed_for_missing_workspace() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        let missing_workspace = WorkspaceName::parse("missing").expect("workspace");

        let Err(error) = fixture.manager.load_query_sources(&missing_workspace).await else {
            panic!("missing workspace should fail closed");
        };

        assert_workspace_not_found(error, &missing_workspace);
    }

    #[tokio::test]
    async fn validate_source_fails_with_workspace_not_found_for_missing_workspace() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        let missing_workspace = WorkspaceName::parse("missing").expect("workspace");
        let source_name = SourceName::parse("github").expect("source");

        let result = fixture
            .manager
            .validate_source(&missing_workspace, &source_name)
            .await;
        let Err(error) = result else {
            panic!("missing workspace should fail before source lookup");
        };

        match error {
            QueryManagerError::App(error) => {
                assert_workspace_not_found(error, &missing_workspace);
            }
            QueryManagerError::Core(error) => {
                panic!("expected app error for missing workspace, got {error}");
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_sql_stamps_task_id_on_query_span() {
        use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
        use coral_api::v1::{ExecuteSqlRequest, Workspace};
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tonic::Request;
        use tracing_subscriber::layer::SubscriberExt as _;

        use crate::query::service::QueryService;

        // Capture finished spans into memory via a scoped subscriber so the
        // assertion exercises the real metadata -> manager -> span path end to end.
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("task-attribution-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        let (task, request_context, task_id) = active_task_context(&fixture.db).await;
        let service = QueryService::new(fixture.manager.clone(), task);

        let mut request = Request::new(ExecuteSqlRequest {
            workspace: Some(Workspace {
                name: WorkspaceName::default().as_str().to_string(),
            }),
            sql: "SELECT 1".to_string(),
        });
        request.extensions_mut().insert(request_context);

        // The query may fail (the fixture has no installed sources); the
        // `coral.query` span is created and stamped before execution regardless.
        let _result = service.execute_sql(request).await;

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let query_span = spans
            .iter()
            .find(|span| span.name == "coral.query")
            .expect("coral.query span recorded");
        let task_attr = query_span
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == "task.id")
            .expect("task.id attribute present");
        assert_eq!(task_attr.value.as_str(), task_id);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn catalog_service_stamps_task_id_on_query_spans() {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tracing_subscriber::layer::SubscriberExt as _;

        use crate::catalog::service::CatalogService;

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("catalog-task-attribution-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        let (task, request_context, task_id) = active_task_context(&fixture.db).await;
        let service = CatalogService::new(fixture.manager.clone(), task);

        call_catalog_tools_with_task(&service, &request_context).await;

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        assert_catalog_task_spans(&spans, &task_id);
    }

    #[test]
    fn query_provenance_records_trace_attributes() {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tracing_subscriber::layer::SubscriberExt as _;

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("query-provenance-trace-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        let span = create_query_span(
            QueryOperation::ExecuteSql,
            &WorkspaceName::default(),
            "SELECT title FROM github.issues",
            None,
        );
        let provenance = QueryExecutionProvenance::new(
            "SELECT title FROM github.issues",
            vec!["github".to_string()],
            vec![QueryTableUsage::new("github", "github", "issues")],
            vec![QueryTableFunctionUsage::new(
                "github",
                "datafusion",
                "github",
                "search_issues",
            )],
        );
        record_query_provenance(&span, &provenance);
        drop(span);

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let query_span = spans
            .iter()
            .find(|span| span.name == "coral.query")
            .expect("coral.query span recorded");

        assert_eq!(
            span_attr(query_span, crate::telemetry::QUERY_TRACE_SOURCES_ATTR),
            Some(r#"["github"]"#.to_string())
        );
        assert_eq!(
            span_attr(query_span, crate::telemetry::QUERY_TRACE_TABLES_ATTR),
            Some(
                r#"[{"source_name":"github","schema_name":"github","table_name":"issues"}]"#
                    .to_string()
            )
        );
        assert_eq!(
            span_attr(
                query_span,
                crate::telemetry::QUERY_TRACE_TABLE_FUNCTIONS_ATTR
            ),
            Some(
                r#"[{"source_name":"github","schema_name":"github","function_name":"search_issues"}]"#
                    .to_string()
            )
        );
    }

    async fn call_catalog_tools_with_task(
        service: &crate::catalog::service::CatalogService,
        request_context: &RequestContext,
    ) {
        use coral_api::v1::catalog_service_server::CatalogService as CatalogServiceApi;
        use coral_api::v1::{
            DescribeTableRequest, ListCatalogRequest, ListColumnsRequest, PaginationRequest,
            SearchCatalogRequest,
        };

        let _list_catalog_result = service
            .list_catalog(tagged_catalog_request(
                request_context,
                ListCatalogRequest {
                    workspace: Some(default_workspace_proto()),
                    catalog_name: String::new(),
                    schema_name: String::new(),
                    kind: 0,
                    pagination: Some(PaginationRequest {
                        limit: 10,
                        offset: 0,
                    }),
                },
            ))
            .await;
        let _search_catalog_result = service
            .search_catalog(tagged_catalog_request(
                request_context,
                SearchCatalogRequest {
                    workspace: Some(default_workspace_proto()),
                    catalog_name: String::new(),
                    pattern: "tables".to_string(),
                    ignore_case: true,
                    schema_name: String::new(),
                    kind: 0,
                    pagination: Some(PaginationRequest {
                        limit: 10,
                        offset: 0,
                    }),
                },
            ))
            .await;
        let _describe_table_result = service
            .describe_table(tagged_catalog_request(
                request_context,
                DescribeTableRequest {
                    workspace: Some(default_workspace_proto()),
                    catalog_name: String::new(),
                    schema_name: "coral".to_string(),
                    table_name: "tables".to_string(),
                },
            ))
            .await;
        let _list_columns_result = service
            .list_columns(tagged_catalog_request(
                request_context,
                ListColumnsRequest {
                    workspace: Some(default_workspace_proto()),
                    catalog_name: String::new(),
                    schema_name: "coral".to_string(),
                    table_name: "tables".to_string(),
                    pattern: None,
                    ignore_case: true,
                    required_only: false,
                    pagination: Some(PaginationRequest {
                        limit: 10,
                        offset: 0,
                    }),
                },
            ))
            .await;
    }

    fn default_workspace_proto() -> coral_api::v1::Workspace {
        coral_api::v1::Workspace {
            name: WorkspaceName::default().as_str().to_string(),
        }
    }

    fn tagged_catalog_request<T>(
        request_context: &RequestContext,
        message: T,
    ) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.extensions_mut().insert(request_context.clone());
        request
    }

    fn assert_catalog_task_spans(spans: &[opentelemetry_sdk::trace::SpanData], task_id: &str) {
        let attributed_query_spans = spans
            .iter()
            .filter(|span| {
                span.name == "coral.query"
                    && span.attributes.iter().any(|attribute| {
                        attribute.key.as_str() == "task.id" && attribute.value.as_str() == task_id
                    })
            })
            .collect::<Vec<_>>();
        assert!(
            attributed_query_spans.len() >= 4,
            "each catalog service call should stamp a backend query span: {spans:?}"
        );
        let operations = attributed_query_spans
            .iter()
            .flat_map(|span| {
                span.attributes
                    .iter()
                    .filter(|attribute| attribute.key.as_str() == "operation")
                    .map(|attribute| attribute.value.as_str().to_string())
            })
            .collect::<Vec<_>>();
        assert!(
            operations
                .iter()
                .any(|operation| operation == "list_catalog")
        );
        assert!(
            operations
                .iter()
                .any(|operation| operation == "describe_table")
        );
        assert!(
            operations
                .iter()
                .any(|operation| operation == "list_tables")
        );
    }

    fn span_attr(span: &opentelemetry_sdk::trace::SpanData, name: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == name)
            .map(|attribute| attribute.value.as_str().into_owned())
    }

    fn execution_to_rows(execution: &QueryExecution) -> Vec<Value> {
        let mut bytes = Vec::new();
        {
            let mut writer = arrow::json::ArrayWriter::new(&mut bytes);
            for batch in execution.batches() {
                writer.write(batch).expect("batch should encode to json");
            }
            writer.finish().expect("json writer should finish");
        }
        serde_json::from_slice(&bytes).expect("json rows should decode")
    }

    async fn mount_v4_openapi_catalog_server(server: &MockServer) {
        for (path_value, id, title) in [
            ("/tagged", 1, "Tagged"),
            ("/public", 2, "Public"),
            ("/search", 3, "Search"),
        ] {
            Mock::given(method("GET"))
                .and(path(path_value))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(json!([{"id": id, "title": title}])),
                )
                .mount(server)
                .await;
        }
    }

    fn import_v4_openapi_catalog_source(
        manager: &QueryManager,
        workspace_name: &WorkspaceName,
        source_name: &str,
        server_uri: &str,
    ) -> SourceName {
        let source_manager = SourceManager::new_for_tests(
            manager.config_store.clone(),
            manager.credential_manager.clone(),
            manager.layout.clone(),
        );
        let descriptor_temp = tempfile::tempdir().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("catalog-openapi.yaml");
        std::fs::write(
            &openapi_file,
            format!(
                r"
openapi: 3.0.3
info:
  title: Catalog runtime
servers:
  - url: {server_uri}
paths:
  /tagged:
    get:
      tags: [issues]
      operationId: issues/list_tagged
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/Item'
  /public:
    get:
      operationId: list_public
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/Item'
  /search:
    get:
      operationId: search_public
      parameters:
        - name: query
          in: query
          required: true
          schema: {{type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/Item'
components:
  schemas:
    Item:
      type: object
      properties:
        id: {{type: integer}}
        title: {{type: string}}
"
            ),
        )
        .expect("write OpenAPI fixture");
        let source_name = SourceName::parse(source_name).expect("source name");
        source_manager
            .import_source(
                workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: format!(
                        r"
name: {source_name}
dsl_version: 4
surface:
  type: openapi
  file: {}
",
                        openapi_file.display()
                    ),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import v4 OpenAPI source");
        source_name
    }

    fn v4_mcp_rpc_result(request: &wiremock::Request, result: &Value) -> ResponseTemplate {
        let body: Value = request.body_json().expect("JSON-RPC request body");
        let id = body.get("id").cloned().expect("JSON-RPC request id");
        ResponseTemplate::new(200)
            .append_header("Content-Type", "application/json")
            .set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result
            }))
    }

    async fn mount_v4_mcp_catalog_server(server: &MockServer) {
        Mock::given(method("POST"))
            .respond_with(|request: &wiremock::Request| {
                let body: Value = request.body_json().expect("JSON-RPC request body");
                match body.get("method").and_then(Value::as_str) {
                    Some("initialize") => v4_mcp_rpc_result(
                        request,
                        &json!({
                            "protocolVersion": "2025-03-26",
                            "capabilities": {"tools": {}},
                            "serverInfo": {"name": "catalog-runtime", "version": "1.0.0"}
                        }),
                    ),
                    Some("notifications/initialized") => ResponseTemplate::new(202),
                    Some("tools/list") => v4_mcp_rpc_result(
                        request,
                        &json!({
                            "tools": [
                                {
                                    "name": "list_items",
                                    "description": "List items",
                                    "inputSchema": {
                                        "type": "object",
                                        "properties": {}
                                    },
                                    "outputSchema": {
                                        "type": "object",
                                        "properties": {
                                            "items": {
                                                "type": "array",
                                                "items": {"type": "object"}
                                            }
                                        }
                                    },
                                    "annotations": {"readOnlyHint": true}
                                },
                                {
                                    "name": "search_items",
                                    "description": "Search items",
                                    "inputSchema": {
                                        "type": "object",
                                        "properties": {
                                            "query": {"type": "string"}
                                        },
                                        "required": ["query"]
                                    },
                                    "outputSchema": {
                                        "type": "object",
                                        "properties": {
                                            "items": {
                                                "type": "array",
                                                "items": {"type": "object"}
                                            }
                                        }
                                    },
                                    "annotations": {"readOnlyHint": true}
                                }
                            ]
                        }),
                    ),
                    Some("tools/call") => {
                        let tool_name = body
                            .pointer("/params/name")
                            .and_then(Value::as_str)
                            .expect("tool name");
                        let arguments = body
                            .pointer("/params/arguments")
                            .cloned()
                            .unwrap_or_else(|| json!({}));
                        v4_mcp_rpc_result(
                            request,
                            &json!({
                                "structuredContent": {
                                    "tool": tool_name,
                                    "arguments": arguments
                                }
                            }),
                        )
                    }
                    other => ResponseTemplate::new(404)
                        .set_body_string(format!("unexpected MCP method {other:?}")),
                }
            })
            .mount(server)
            .await;
    }

    fn import_v4_mcp_catalog_source(
        manager: &QueryManager,
        workspace_name: &WorkspaceName,
        source_name: &str,
        server_uri: &str,
    ) -> SourceName {
        let source_manager = SourceManager::new_for_tests(
            manager.config_store.clone(),
            manager.credential_manager.clone(),
            manager.layout.clone(),
        );
        let source_name = SourceName::parse(source_name).expect("source name");
        source_manager
            .import_source(
                workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: format!(
                        r#"
name: {source_name}
dsl_version: 4
surface:
  type: mcp
  server:
    transport: streamable_http
    url: "{server_uri}"
"#
                    ),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import v4 MCP source");
        source_name
    }

    #[tokio::test]
    async fn runtime_config_preserves_app_owned_body_capture_max_bytes() {
        let fixture = query_manager_with(
            QueryRuntimeContext::default().with_body_capture_max_bytes(Some(42)),
            Vec::new(),
        )
        .await;

        let runtime = fixture
            .manager
            .runtime_config(&WorkspaceName::default(), &[], &AppConfig::default())
            .expect("runtime config");

        let config = runtime
            .context
            .body_capture_max_bytes
            .expect("body capture config");
        assert_eq!(config, 42);
    }

    #[tokio::test]
    async fn metadata_runtime_config_skips_observed_values_publishers() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        let workspace_name = WorkspaceName::default();
        let manager =
            fixture
                .manager
                .clone()
                .with_search_observation_handle(SearchObservationHandle::new(
                    fixture.manager.layout.clone(),
                ));
        let loaded_source = observed_values_loaded_source();

        let runtime = manager
            .runtime_config_without_source_observations(
                &workspace_name,
                std::slice::from_ref(&loaded_source),
                &AppConfig::default(),
            )
            .expect("metadata runtime config");

        assert!(runtime.extensions.source_observation_publishers.is_empty());
        assert!(
            !manager.layout.search_sqlite_file(&workspace_name).exists(),
            "metadata runtime config should not open the observed-values SQLite store"
        );
    }

    #[tokio::test]
    async fn execution_runtime_config_attaches_observed_values_publishers() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        let workspace_name = WorkspaceName::default();
        let manager =
            fixture
                .manager
                .clone()
                .with_search_observation_handle(SearchObservationHandle::new(
                    fixture.manager.layout.clone(),
                ));
        let loaded_source = observed_values_loaded_source();

        let runtime = manager
            .runtime_config(
                &workspace_name,
                std::slice::from_ref(&loaded_source),
                &AppConfig::default(),
            )
            .expect("execution runtime config");

        assert_eq!(runtime.extensions.source_observation_publishers.len(), 1);
        assert!(
            manager.layout.search_sqlite_file(&workspace_name).exists(),
            "execution runtime config should open the observed-values SQLite store"
        );
    }

    #[tokio::test]
    async fn function_metadata_runtimes_do_not_open_observed_values_store() {
        let fake_home = tempfile::tempdir().expect("fake home");
        let fixture = query_manager_with(
            QueryRuntimeContext {
                home_dir: Some(fake_home.path().to_path_buf()),
                ..QueryRuntimeContext::default()
            },
            Vec::new(),
        )
        .await;
        let workspace_name = WorkspaceName::default();
        install_function_demo_source(&fixture.manager, &workspace_name, fake_home.path());
        let function_sql = r"/*
name: demo_items
schema: functions
description: Returns demo messages
*/

select text from function_demo.messages
";
        let validated = fixture
            .manager
            .validate_udf_sql(&workspace_name, function_sql)
            .await
            .expect("validate function without observations");
        fixture
            .manager
            .function_manager
            .install_validated_user_function(&workspace_name, function_sql, &validated)
            .expect("install function");

        let manager =
            fixture
                .manager
                .clone()
                .with_search_observation_handle(SearchObservationHandle::new(
                    fixture.manager.layout.clone(),
                ));
        let observed_values_path = manager.layout.search_sqlite_file(&workspace_name);
        manager
            .validate_udf_sql(&workspace_name, function_sql)
            .await
            .expect("validate function with observations enabled");
        assert!(
            !observed_values_path.exists(),
            "function validation should not open the observed-values SQLite store"
        );

        let functions = manager
            .list_functions(&workspace_name)
            .await
            .expect("list functions with observations enabled");
        assert_eq!(functions.len(), 1);
        assert!(
            !observed_values_path.exists(),
            "function listing should not open the observed-values SQLite store"
        );
    }

    #[tokio::test]
    async fn load_query_source_passes_present_optional_secrets_to_runtime() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("optional_auth").expect("source name");
        let manifest_path = fixture
            .manager
            .layout
            .manifest_file(&workspace_name, &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create source dir");
        std::fs::write(
            &manifest_path,
            r"
name: optional_auth
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://api.example.com
inputs:
  API_KEY:
    kind: secret
    required: false
  OAUTH_TOKEN:
    kind: secret
    required: false
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: one_of
      values:
        - from: input
          key: API_KEY
        - from: bearer
          key: OAUTH_TOKEN
tables:
  - name: items
    description: Items
    request:
      path: /items
    columns:
      - name: id
        type: Utf8
",
        )
        .expect("write manifest");
        let source = InstalledSource {
            name: source_name.clone(),
            version: Some("0.1.0".to_string()),
            variables: BTreeMap::new(),
            secrets: vec!["API_KEY".to_string(), "OAUTH_TOKEN".to_string()],
            credential_storage: Some(CredentialStorageKind::File),
            credential_revision: uuid::Uuid::default(),
            origin: SourceOrigin::Imported,
        };
        fixture
            .manager
            .config_store
            .upsert_source(&workspace_name, source.clone())
            .expect("persist source");
        fixture
            .manager
            .credential_manager
            .replace_material(
                &workspace_name,
                &CredentialSetId::for_source(&source_name),
                CredentialStorageKind::File,
                &BTreeMap::from([("OAUTH_TOKEN".to_string(), "oauth-token".to_string())]),
            )
            .expect("persist secret material");

        let (loaded_source, _) = fixture
            .manager
            .load_query_source(&workspace_name, &source)
            .expect("optional secret should load when present");

        assert_eq!(
            loaded_source.query_source.secrets(),
            &BTreeMap::from([("OAUTH_TOKEN".to_string(), "oauth-token".to_string())])
        );
    }

    #[tokio::test]
    async fn v4_openapi_catalog_runtime_queries_tagged_and_public_relations() {
        let server = MockServer::start().await;
        mount_v4_openapi_catalog_server(&server).await;

        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name = import_v4_openapi_catalog_source(
            &fixture.manager,
            &workspace_name,
            "github_v4_openapi",
            &server.uri(),
        );

        let validated = fixture
            .manager
            .validate_source(&workspace_name, &source_name)
            .await
            .expect("validate source");
        let table_identities = validated
            .report
            .tables
            .iter()
            .map(|table| {
                (
                    table.catalog_name.as_deref(),
                    table.schema_name.as_str(),
                    table.table_name.as_str(),
                )
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            table_identities,
            std::collections::BTreeSet::from([
                (Some("github_v4_openapi"), "issues", "list_tagged"),
                (Some("github_v4_openapi"), "public", "list_public"),
            ])
        );
        let function = validated
            .report
            .table_functions
            .first()
            .expect("OpenAPI table function");
        assert_eq!(function.catalog_name.as_deref(), Some("github_v4_openapi"));
        assert_eq!(function.schema_name, "public");
        assert_eq!(function.function_name, "search_public");

        for (sql, expected_id) in [
            ("SELECT id FROM github_v4_openapi.issues.list_tagged", 1),
            ("SELECT id FROM github_v4_openapi.public.list_public", 2),
            (
                "SELECT id FROM github_v4_openapi.public.search_public(query => 'needle')",
                3,
            ),
        ] {
            let execution = fixture
                .manager
                .execute_sql(&workspace_name, sql, &QueryAttribution::default())
                .await
                .expect("three-part OpenAPI query executes");
            assert_eq!(
                execution_to_rows(&execution),
                vec![json!({"id": expected_id})]
            );
        }
    }

    #[tokio::test]
    async fn v4_mcp_catalog_runtime_queries_table_and_function() {
        let server = MockServer::start().await;
        mount_v4_mcp_catalog_server(&server).await;

        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name = import_v4_mcp_catalog_source(
            &fixture.manager,
            &workspace_name,
            "github_v4_mcp",
            &server.uri(),
        );

        let validated = fixture
            .manager
            .validate_source(&workspace_name, &source_name)
            .await
            .expect("validate MCP source");
        let table = validated.report.tables.first().expect("MCP table");
        assert_eq!(table.catalog_name.as_deref(), Some("github_v4_mcp"));
        assert_eq!(table.schema_name, "public");
        assert_eq!(table.table_name, "list_items");
        let function = validated
            .report
            .table_functions
            .first()
            .expect("MCP table function");
        assert_eq!(function.catalog_name.as_deref(), Some("github_v4_mcp"));
        assert_eq!(function.schema_name, "public");
        assert_eq!(function.function_name, "search_items");

        for sql in [
            "SELECT result_json FROM github_v4_mcp.public.list_items LIMIT 1",
            "SELECT result_json FROM github_v4_mcp.public.search_items(query => 'needle') LIMIT 1",
        ] {
            let execution = fixture
                .manager
                .execute_sql(&workspace_name, sql, &QueryAttribution::default())
                .await
                .expect("three-part MCP query executes");
            assert_eq!(execution.row_count(), 1);
        }

        let called_tools = server
            .received_requests()
            .await
            .expect("request recording")
            .iter()
            .filter_map(|request| request.body_json::<Value>().ok())
            .filter(|body| body.get("method").and_then(Value::as_str) == Some("tools/call"))
            .filter_map(|body| {
                body.pointer("/params/name")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect::<Vec<_>>();
        assert_eq!(called_tools, ["list_items", "search_items"]);
    }

    #[tokio::test]
    async fn v4_two_part_alias_is_absent() {
        let server = MockServer::start().await;
        mount_v4_openapi_catalog_server(&server).await;

        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        import_v4_openapi_catalog_source(
            &fixture.manager,
            &workspace_name,
            "github_v4_no_alias",
            &server.uri(),
        );

        let error = fixture
            .manager
            .execute_sql(
                &workspace_name,
                "SELECT id FROM github_v4_no_alias.list_public",
                &QueryAttribution::default(),
            )
            .await
            .expect_err("v4 two-part aliases must not be registered");
        let message = match &error {
            QueryManagerError::App(error) => error.to_string(),
            QueryManagerError::Core(error) => error.to_string(),
        };

        assert!(
            message.contains("github_v4_no_alias.list_public"),
            "missing-table error should preserve the rejected alias: {error:?}"
        );
    }

    #[tokio::test]
    async fn load_query_source_preserves_unsupported_v4_identity_requirements_error() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let source_manager = SourceManager::new_for_tests(
            fixture.manager.config_store.clone(),
            fixture.manager.credential_manager.clone(),
            fixture.manager.layout.clone(),
        );
        let workspace_name = WorkspaceName::default();
        let descriptor_temp = tempfile::tempdir().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("identity-guard-openapi.yaml");
        std::fs::write(
            &openapi_file,
            r"
openapi: 3.0.3
info: {title: Identity Guard}
paths:
  /items:
    get:
      operationId: items/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: integer}
",
        )
        .expect("write OpenAPI fixture");
        let source = source_manager
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: format!(
                        r"
name: github_v4_identity_guard
dsl_version: 4
identity_requirements:
  accepts:
    - id: github_api
      identity_specs: [github_oauth]
surface:
  type: openapi
  file: {}
",
                        openapi_file.display()
                    ),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import identity-gated v4 source");
        std::fs::remove_file(&openapi_file).expect("remove authored descriptor after import");

        let error = fixture
            .manager
            .load_query_source(&workspace_name, &source)
            .expect_err("identity-gated source must fail closed");

        assert!(matches!(
            &error,
            AppError::UnsupportedV4IdentityRequirements { source_name }
                if source_name == "github_v4_identity_guard"
        ));
        assert!(!error.to_string().contains("Re-add"));
    }

    #[tokio::test]
    async fn installed_v4_source_uses_operation_metadata_pagination_override() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/widgets"))
            .respond_with(|request: &wiremock::Request| {
                let page = request
                    .url
                    .query_pairs()
                    .find_map(|(key, value)| (key == "page").then_some(value.into_owned()));
                match page.as_deref() {
                    Some("1") => ResponseTemplate::new(200).set_body_json(json!([
                        {"id": 1},
                        {"id": 2}
                    ])),
                    Some("2") => ResponseTemplate::new(200).set_body_json(json!([
                        {"id": 3},
                        {"id": 4}
                    ])),
                    other => ResponseTemplate::new(400)
                        .set_body_string(format!("unexpected page {other:?}")),
                }
            })
            .mount(&server)
            .await;

        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let source_manager = SourceManager::new_for_tests(
            fixture.manager.config_store.clone(),
            fixture.manager.credential_manager.clone(),
            fixture.manager.layout.clone(),
        );
        let workspace_name = WorkspaceName::default();
        let descriptor_temp = tempfile::tempdir().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("widgets-openapi.yaml");
        std::fs::write(&openapi_file, widgets_pagination_openapi(&server.uri()))
            .expect("write OpenAPI fixture");
        source_manager
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: format!(
                        r"
name: github_v4_pagination_override
dsl_version: 4
surface:
    type: openapi
    file: {}
",
                        openapi_file.display()
                    ),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import v4 source");

        let source_name = SourceName::parse("github_v4_pagination_override").expect("source name");
        write_widgets_operation_metadata_override(
            &fixture.manager.layout,
            &workspace_name,
            &source_name,
        );

        let execution = fixture
            .manager
            .execute_sql(
                &workspace_name,
                "SELECT id FROM github_v4_pagination_override.public.list LIMIT 3",
                &QueryAttribution::default(),
            )
            .await
            .expect("query executes");

        assert_eq!(
            execution_to_rows(&execution),
            vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]
        );
        let requests = server
            .received_requests()
            .await
            .expect("request recording should be enabled");
        let pages = request_query_values(&requests, "page");
        let page_sizes = request_query_values(&requests, "per_page");
        assert_eq!(pages, ["1", "2"]);
        assert_eq!(page_sizes, ["2", "2"]);
    }

    fn widgets_pagination_openapi(server_uri: &str) -> String {
        format!(
            r"
openapi: 3.0.3
info:
  title: Widgets
servers:
  - url: {server_uri}
paths:
  /widgets:
    get:
      operationId: widgets/list
      parameters:
        - name: page
          in: query
          required: true
          schema: {{type: integer}}
        - name: per_page
          in: query
          required: true
          schema: {{type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {{type: integer}}
"
        )
    }

    fn write_widgets_operation_metadata_override(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) {
        let generated_path = layout
            .v4_materialized_dir(workspace_name, source_name)
            .join(crate::sources::materialization::OPERATION_METADATA_FILENAME);
        let mut metadata: coral_spec::v4::OperationMetadataCatalog = serde_yaml::from_slice(
            &std::fs::read(&generated_path).expect("read generated operation metadata"),
        )
        .expect("parse generated operation metadata");
        let operation = metadata
            .operations
            .values_mut()
            .next()
            .expect("operation metadata");
        let coral_spec::v4::OperationMetadata::Rest { pagination, .. } = operation else {
            panic!("expected REST operation metadata");
        };
        *pagination = coral_spec::PaginationSpec {
            mode: coral_spec::PaginationMode::Page,
            page_param: Some("page".to_string()),
            page_start: 1,
            page_size: Some(coral_spec::PageSizeSpec {
                default: 2,
                max: 2,
                query_param: Some("per_page".to_string()),
                body_path: Vec::new(),
            }),
            ..coral_spec::PaginationSpec::default()
        };
        let override_path = layout
            .v4_override_dir(workspace_name, source_name)
            .join(crate::sources::materialization::OPERATION_METADATA_FILENAME);
        std::fs::create_dir_all(override_path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::write(
            &override_path,
            serde_yaml::to_string(&metadata).expect("encode operation metadata override"),
        )
        .expect("write operation metadata override");
    }

    fn request_query_values(requests: &[wiremock::Request], query_key: &str) -> Vec<String> {
        requests
            .iter()
            .map(|request| {
                request
                    .url
                    .query_pairs()
                    .find_map(|(key, value)| (key == query_key).then_some(value.into_owned()))
                    .expect("query param")
            })
            .collect()
    }

    #[tokio::test]
    async fn add_user_function_revalidates_when_source_changes_before_commit() {
        let fake_home = tempfile::tempdir().expect("fake home");
        let mut fixture = query_manager_with(
            QueryRuntimeContext {
                home_dir: Some(fake_home.path().to_path_buf()),
                ..QueryRuntimeContext::default()
            },
            Vec::new(),
        )
        .await;
        let workspace_name = WorkspaceName::default();
        install_function_demo_source(&fixture.manager, &workspace_name, fake_home.path());
        let calls = Arc::new(AtomicUsize::new(0));
        let config_store = fixture.manager.config_store.clone();
        let lifecycle_lock = fixture.manager.lifecycle_lock.clone();
        let workspace = workspace_name.clone();
        let source_name = SourceName::parse("function_demo").expect("source name");
        fixture.manager.engine_extensions_providers.push(Arc::new(
            PrepareCountingExtensionsProvider {
                calls: Arc::clone(&calls),
                on_first_prepare: Some(Arc::new(move || {
                    let _lifecycle_guard = lifecycle_lock.lock();
                    config_store
                        .remove_source(&workspace, &source_name)
                        .expect("remove source during function validation");
                })),
            },
        ));
        let function_sql = r"/*
name: demo_items
schema: functions
description: Returns demo messages
*/

select text from function_demo.messages
";

        let error = fixture
            .manager
            .add_user_function(&workspace_name, function_sql)
            .await
            .expect_err("source change should invalidate the original validation snapshot");

        let detail = match error {
            QueryManagerError::App(error) => error.to_string(),
            QueryManagerError::Core(error) => error.to_string(),
        };
        assert!(
            detail.contains("function_demo.messages"),
            "unexpected error: {detail}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            fixture
                .manager
                .list_functions(&workspace_name)
                .await
                .expect("list functions")
                .is_empty(),
            "stale validation must not install the function"
        );
    }

    #[tokio::test]
    async fn user_function_publishes_table_function_and_executes_against_installed_source() {
        let fake_home = tempfile::tempdir().expect("fake home");
        let fixture = query_manager_with(
            QueryRuntimeContext {
                home_dir: Some(fake_home.path().to_path_buf()),
                ..QueryRuntimeContext::default()
            },
            Vec::new(),
        )
        .await;
        let workspace_name = WorkspaceName::default();
        install_function_demo_source(&fixture.manager, &workspace_name, fake_home.path());
        let function_sql = r"/*
name: messages_by_type
schema: functions
description: Messages filtered by sender type
guide: |-
  Prefer this function for sender lookups.
  Required argument: kind.
*/

select text
from function_demo.messages
where type = $kind
";
        let expected_guide = "Prefer this function for sender lookups.\nRequired argument: kind.";
        let validated_function = fixture
            .manager
            .validate_udf_sql(&workspace_name, function_sql)
            .await
            .expect("validate function");
        fixture
            .manager
            .function_manager
            .install_validated_user_function(&workspace_name, function_sql, &validated_function)
            .expect("install function");

        let catalog = fixture
            .manager
            .list_catalog(
                &workspace_name,
                None,
                Some("functions"),
                &QueryAttribution::default(),
            )
            .await
            .expect("catalog");
        let function_function = catalog.table_functions.first().expect("function function");
        assert_eq!(function_function.function_name, "messages_by_type");
        assert_eq!(function_function.guide, expected_guide);
        assert_eq!(
            function_function
                .result_columns
                .first()
                .expect("text result column")
                .name,
            "text"
        );

        let functions = fixture
            .manager
            .list_functions(&workspace_name)
            .await
            .expect("functions");
        assert_eq!(functions.len(), 1);
        let function = functions.first().expect("function");
        let crate::functions::manager::FunctionRuntimeStatus::Ready(definition) = &function.runtime
        else {
            panic!("function should be runtime-ready");
        };
        assert_eq!(definition.publish.table_function.guide, expected_guide);
        let column = definition
            .result_columns
            .first()
            .expect("text result column");
        assert_eq!(column.name, "text");

        let guide_query = fixture
            .manager
            .execute_sql(
                &workspace_name,
                "select guide from coral.table_functions \
                 where schema_name = 'functions' and function_name = 'messages_by_type'",
                &QueryAttribution::default(),
            )
            .await
            .expect("function guide query");
        assert_eq!(
            execution_to_rows(&guide_query),
            vec![json!({"guide": expected_guide})]
        );

        let execution = fixture
            .manager
            .execute_sql(
                &workspace_name,
                "select text from functions.messages_by_type(kind => 'user')",
                &QueryAttribution::default(),
            )
            .await
            .expect("function query");
        assert_eq!(
            execution_to_rows(&execution),
            vec![json!({"text": "hello"})]
        );
    }

    fn install_function_demo_source(
        manager: &QueryManager,
        workspace_name: &WorkspaceName,
        fake_home: &std::path::Path,
    ) {
        let data_dir = fake_home.join("fixture-data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        std::fs::write(
            data_dir.join("messages.jsonl"),
            r#"{"type":"user","text":"hello"}
{"type":"assistant","text":"world"}
"#,
        )
        .expect("write fixture");
        let source_manager = SourceManager::new_for_tests(
            manager.config_store.clone(),
            manager.credential_manager.clone(),
            manager.layout.clone(),
        );
        source_manager
            .import_source(
                workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: r#"
name: function_demo
version: 0.1.0
dsl_version: 3
backend: file
tables:
  - name: messages
    description: Fixture messages
    format: jsonl
    source:
      location: file://~/fixture-data/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: text
        type: Utf8
"#
                    .to_string(),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import source");
    }

    fn install_missing_v4_materialization_source(
        manager: &QueryManager,
        workspace_name: &WorkspaceName,
    ) -> SourceName {
        let source_name = SourceName::parse("github_v4_missing_artifacts").expect("source name");
        let manifest_path = manager.layout.manifest_file(workspace_name, &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create source dir");
        std::fs::write(
            &manifest_path,
            r"
name: github_v4_missing_artifacts
dsl_version: 4
surface:
    type: openapi
    url: https://example.com/openapi.yaml
",
        )
        .expect("write manifest");
        manager
            .config_store
            .upsert_source(
                workspace_name,
                InstalledSource {
                    name: source_name.clone(),
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: Vec::new(),
                    credential_storage: None,
                    credential_revision: uuid::Uuid::default(),
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("persist source");
        source_name
    }

    fn install_corrupt_parquet_source(
        manager: &QueryManager,
        workspace_name: &WorkspaceName,
        fake_home: &std::path::Path,
    ) -> SourceName {
        let data_dir = fake_home.join("corrupt-parquet");
        std::fs::create_dir_all(&data_dir).expect("create corrupt parquet dir");
        std::fs::write(data_dir.join("events.parquet"), b"not a parquet file")
            .expect("write corrupt parquet");

        let source_name = SourceName::parse("corrupt_parquet").expect("source name");
        let manifest_path = manager.layout.manifest_file(workspace_name, &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create source dir");
        std::fs::write(
            &manifest_path,
            r#"
name: corrupt_parquet
version: 0.1.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Corrupt parquet fixture
    format: parquet
    source:
      location: file://~/corrupt-parquet/
      glob: "**/*.parquet"
    columns: []
"#,
        )
        .expect("write manifest");
        manager
            .config_store
            .upsert_source(
                workspace_name,
                InstalledSource {
                    name: source_name.clone(),
                    version: Some("0.1.0".to_string()),
                    variables: BTreeMap::new(),
                    secrets: Vec::new(),
                    credential_storage: None,
                    credential_revision: uuid::Uuid::default(),
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("persist source");
        source_name
    }

    #[tokio::test]
    async fn load_query_sources_skips_missing_v4_materialization() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name =
            install_missing_v4_materialization_source(&fixture.manager, &workspace_name);

        let (source_load, _) = fixture
            .manager
            .load_query_sources(&workspace_name)
            .await
            .expect("missing materialization should be isolated");

        assert!(source_load.loaded.is_empty());
        assert_eq!(
            source_load.failed_source_names,
            BTreeSet::from([source_name.to_string()])
        );
    }

    #[tokio::test]
    async fn resolve_catalog_reports_load_failure_and_keeps_healthy_metadata() {
        let fake_home = tempfile::tempdir().expect("fake home");
        let fixture = query_manager_with(
            QueryRuntimeContext {
                home_dir: Some(fake_home.path().to_path_buf()),
                ..QueryRuntimeContext::default()
            },
            Vec::new(),
        )
        .await;
        let workspace_name = WorkspaceName::default();
        install_function_demo_source(&fixture.manager, &workspace_name, fake_home.path());
        let failed_source =
            install_missing_v4_materialization_source(&fixture.manager, &workspace_name);

        let resolution = fixture
            .manager
            .resolve_catalog(&workspace_name, None, None, &QueryAttribution::default())
            .await
            .expect("healthy catalog should survive one source load failure");

        assert_eq!(
            resolution.failed_source_names,
            BTreeSet::from([failed_source.to_string()])
        );
        assert!(resolution.catalog.tables.iter().any(|table| {
            table.schema_name == "function_demo" && table.table_name == "messages"
        }));
    }

    #[tokio::test]
    async fn resolve_catalog_reports_registration_failure_and_keeps_healthy_metadata() {
        let fake_home = tempfile::tempdir().expect("fake home");
        let fixture = query_manager_with(
            QueryRuntimeContext {
                home_dir: Some(fake_home.path().to_path_buf()),
                ..QueryRuntimeContext::default()
            },
            Vec::new(),
        )
        .await;
        let workspace_name = WorkspaceName::default();
        install_function_demo_source(&fixture.manager, &workspace_name, fake_home.path());
        let failed_source =
            install_corrupt_parquet_source(&fixture.manager, &workspace_name, fake_home.path());

        let resolution = fixture
            .manager
            .resolve_catalog(&workspace_name, None, None, &QueryAttribution::default())
            .await
            .expect("healthy catalog should survive one registration failure");

        assert_eq!(
            resolution.failed_source_names,
            BTreeSet::from([failed_source.to_string()])
        );
        assert!(resolution.catalog.tables.iter().any(|table| {
            table.schema_name == "function_demo" && table.table_name == "messages"
        }));
        assert!(
            resolution
                .catalog
                .tables
                .iter()
                .all(|table| table.schema_name != failed_source.as_str())
        );
    }

    #[tokio::test]
    async fn load_query_sources_skips_unavailable_keychain_source() {
        let fixture = query_manager_with_unavailable_keychain().await;
        let workspace_name = WorkspaceName::default();
        install_keychain_github_source(&fixture.manager.config_store, &workspace_name);

        let (source_load, _) = fixture
            .manager
            .load_query_sources(&workspace_name)
            .await
            .expect("unavailable keychain source should be isolated");
        assert!(source_load.loaded.is_empty());
        assert_eq!(
            source_load.failed_source_names,
            BTreeSet::from(["github".to_string()])
        );
    }

    #[tokio::test]
    async fn list_functions_keeps_unrelated_function_ready_when_source_preparation_fails() {
        let fixture = query_manager_with_unavailable_keychain().await;
        let workspace_name = WorkspaceName::default();
        let function_sql = r"/*
name: constant_value
schema: functions
description: Returns a constant value
*/

select 1 as value
";
        let validated = fixture
            .manager
            .validate_udf_sql(&workspace_name, function_sql)
            .await
            .expect("validate constant function before source failure");
        fixture
            .manager
            .function_manager
            .install_validated_user_function(&workspace_name, function_sql, &validated)
            .expect("install constant function");
        install_keychain_github_source(&fixture.manager.config_store, &workspace_name);

        let functions = fixture
            .manager
            .list_functions(&workspace_name)
            .await
            .expect("source preparation failure should not hide function inventory");

        let function = functions
            .first()
            .expect("installed function remains visible");
        assert_eq!(function.name.as_str(), "constant_value");
        let crate::functions::manager::FunctionRuntimeStatus::Ready(_) = &function.runtime else {
            panic!("unrelated function should remain ready when source preparation fails");
        };
    }

    struct PrepareCountingDecorator {
        calls: Arc<AtomicUsize>,
        on_first_prepare: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl SourceDecorator for PrepareCountingDecorator {
        fn name(&self) -> &'static str {
            "prepare-counter"
        }

        fn prepare(
            &mut self,
            _selected_sources: &[QuerySource],
        ) -> Result<(), SourceDecoratorError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0
                && let Some(on_first_prepare) = &self.on_first_prepare
            {
                on_first_prepare();
            }
            Ok(())
        }
    }

    struct PrepareCountingExtensionsProvider {
        calls: Arc<AtomicUsize>,
        on_first_prepare: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl EngineExtensionsProvider for PrepareCountingExtensionsProvider {
        fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
            let mut extensions = EngineExtensions::default();
            extensions
                .source_decorators
                .push(Box::new(PrepareCountingDecorator {
                    calls: Arc::clone(&self.calls),
                    on_first_prepare: self.on_first_prepare.clone(),
                }));
            extensions
        }
    }

    #[tokio::test]
    async fn function_enabled_query_prepares_source_decorators_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(PrepareCountingExtensionsProvider {
            calls: Arc::clone(&calls),
            on_first_prepare: None,
        });
        let fixture = query_manager_with(QueryRuntimeContext::default(), vec![provider]).await;
        let workspace_name = WorkspaceName::default();
        let function_sql = r"/*
name: constant_value
schema: functions
description: Returns a constant value
*/

select 1 as value
";
        let validated = fixture
            .manager
            .validate_udf_sql(&workspace_name, function_sql)
            .await
            .expect("validate constant function");
        fixture
            .manager
            .function_manager
            .install_validated_user_function(&workspace_name, function_sql, &validated)
            .expect("install constant function");
        calls.store(0, Ordering::SeqCst);

        fixture
            .manager
            .execute_sql(
                &workspace_name,
                "select value from functions.constant_value()",
                &QueryAttribution::default(),
            )
            .await
            .expect("execute constant function");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[derive(Debug)]
    struct DelegatingInputResolver {
        calls: Arc<AtomicUsize>,
        observed_token: Arc<Mutex<Option<String>>>,
    }

    #[tonic::async_trait]
    impl SourceInputResolver for DelegatingInputResolver {
        async fn resolve_inputs(
            &self,
            source: &SourceInputResolutionContext,
        ) -> Result<BTreeMap<String, String>, SourceInputResolverError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.observed_token.lock().expect("observed token lock") =
                source.secrets().get("API_TOKEN").cloned();
            Ok(BTreeMap::from([
                ("API_TOKEN".to_string(), "delegated-token".to_string()),
                ("DELEGATED_ONLY".to_string(), "provider-token".to_string()),
            ]))
        }
    }

    struct DelegatingInputResolverProvider {
        calls: Arc<AtomicUsize>,
        observed_token: Arc<Mutex<Option<String>>>,
    }

    impl EngineExtensionsProvider for DelegatingInputResolverProvider {
        fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
            EngineExtensions {
                source_input_resolver: Some(Arc::new(DelegatingInputResolver {
                    calls: Arc::clone(&self.calls),
                    observed_token: Arc::clone(&self.observed_token),
                })),
                ..Default::default()
            }
        }
    }

    #[tokio::test]
    async fn runtime_input_resolver_uses_loaded_credential_snapshot() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source name");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let installed_source = InstalledSource {
            name: source_name.clone(),
            version: None,
            variables: BTreeMap::new(),
            secrets: vec!["API_TOKEN".to_string()],
            credential_storage: Some(CredentialStorageKind::File),
            credential_revision: uuid::Uuid::default(),
            origin: SourceOrigin::Bundled,
        };
        fixture
            .manager
            .config_store
            .upsert_source(&workspace_name, installed_source.clone())
            .expect("persist live source");
        fixture
            .manager
            .credential_manager
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::from([("API_TOKEN".to_string(), "live-token".to_string())]),
            )
            .expect("write live credential material");
        let source_spec = parse_source_manifest_yaml(
            r"
name: secured_messages
version: 0.1.0
dsl_version: 3
backend: http
inputs:
  API_TOKEN:
    kind: secret
base_url: https://example.com
tables:
  - name: messages
    description: Secured messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
",
        )
        .expect("parse source manifest");
        let loaded_source = LoadedQuerySource {
            source: installed_source,
            query_source: QuerySource::new(source_spec, BTreeMap::new(), BTreeMap::new()),
            runtime_contract_fingerprint: RuntimeContractFingerprint::for_test("contract"),
            credential_material: BTreeMap::from([(
                "API_TOKEN".to_string(),
                "snapshot-token".to_string(),
            )]),
        };
        let runtime = fixture
            .manager
            .runtime_config(
                &workspace_name,
                std::slice::from_ref(&loaded_source),
                &AppConfig::default(),
            )
            .expect("runtime config");
        let input_resolver = runtime
            .extensions
            .source_input_resolver
            .expect("runtime installs input resolver");
        fixture
            .manager
            .credential_manager
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::from([("API_TOKEN".to_string(), "changed-live-token".to_string())]),
            )
            .expect("replace live credential material");

        let resolved_inputs = input_resolver
            .resolve_inputs(&SourceInputResolutionContext::from_query_source(
                &loaded_source.query_source,
            ))
            .await
            .expect("resolve source inputs");

        assert_eq!(
            resolved_inputs.get("API_TOKEN").map(String::as_str),
            Some("snapshot-token")
        );
    }

    #[tokio::test]
    async fn runtime_contract_fingerprint_ignores_refreshed_credential_material() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source name");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let installed_source = InstalledSource {
            name: source_name.clone(),
            version: Some("0.1.0".to_string()),
            variables: BTreeMap::new(),
            secrets: vec!["API_TOKEN".to_string()],
            credential_storage: Some(CredentialStorageKind::File),
            credential_revision: uuid::Uuid::new_v4(),
            origin: SourceOrigin::Imported,
        };
        std::fs::create_dir_all(fixture.manager.layout.source_dir(&workspace, &source_name))
            .expect("source directory");
        std::fs::write(
            fixture
                .manager
                .layout
                .manifest_file(&workspace, &source_name),
            r"
name: secured_messages
version: 0.1.0
dsl_version: 3
backend: http
inputs:
  API_TOKEN:
    kind: secret
base_url: https://example.com
tables:
  - name: messages
    description: Secured messages
    request:
      path: /messages
    columns:
      - name: id
        type: Utf8
",
        )
        .expect("write manifest");
        fixture
            .manager
            .config_store
            .upsert_source(&workspace, installed_source.clone())
            .expect("persist source");
        fixture
            .manager
            .credential_manager
            .replace_material(
                &workspace,
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::from([("API_TOKEN".to_string(), "first-token".to_string())]),
            )
            .expect("first credential material");
        let first = fixture
            .manager
            .load_query_source(&workspace, &installed_source)
            .expect("first runtime")
            .0;

        fixture
            .manager
            .credential_manager
            .replace_material(
                &workspace,
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::from([("API_TOKEN".to_string(), "refreshed-token".to_string())]),
            )
            .expect("refreshed credential material");
        let refreshed = fixture
            .manager
            .load_query_source(&workspace, &installed_source)
            .expect("refreshed runtime")
            .0;

        assert_eq!(
            first.runtime_contract_fingerprint,
            refreshed.runtime_contract_fingerprint
        );
    }

    #[tokio::test]
    async fn runtime_config_composes_provider_input_resolver_with_refreshed_inputs() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_token = Arc::new(Mutex::new(None));
        let fixture = query_manager_with(
            QueryRuntimeContext::default(),
            vec![Arc::new(DelegatingInputResolverProvider {
                calls: Arc::clone(&calls),
                observed_token: Arc::clone(&observed_token),
            })],
        )
        .await;
        let source_spec = parse_source_manifest_yaml(
            r#"
name: secured_messages
version: 0.1.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://example.com
  API_TOKEN:
    kind: secret
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Secured messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect("parse source manifest");
        let loaded_source = LoadedQuerySource {
            source: InstalledSource {
                name: SourceName::parse("secured_messages").expect("source name"),
                version: None,
                variables: BTreeMap::new(),
                secrets: vec!["API_TOKEN".to_string()],
                credential_storage: None,
                credential_revision: uuid::Uuid::default(),
                origin: SourceOrigin::Bundled,
            },
            query_source: QuerySource::new(source_spec.clone(), BTreeMap::new(), BTreeMap::new()),
            runtime_contract_fingerprint: RuntimeContractFingerprint::for_test("contract"),
            credential_material: BTreeMap::from([(
                "API_TOKEN".to_string(),
                "stored-token".to_string(),
            )]),
        };
        let runtime = fixture
            .manager
            .runtime_config(
                &WorkspaceName::default(),
                std::slice::from_ref(&loaded_source),
                &AppConfig::default(),
            )
            .expect("runtime config");
        let input_resolver = runtime
            .extensions
            .source_input_resolver
            .expect("runtime installs input resolver");

        let resolved_inputs = input_resolver
            .resolve_inputs(&SourceInputResolutionContext::from_query_source(
                &loaded_source.query_source,
            ))
            .await
            .expect("resolve source inputs");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            resolved_inputs.get("API_TOKEN").map(String::as_str),
            Some("stored-token")
        );
        assert_eq!(
            resolved_inputs.get("API_BASE").map(String::as_str),
            Some("https://example.com")
        );
        assert_eq!(
            resolved_inputs.get("DELEGATED_ONLY").map(String::as_str),
            Some("provider-token")
        );
        assert_eq!(
            observed_token
                .lock()
                .expect("observed token lock")
                .as_deref(),
            Some("stored-token")
        );
    }

    fn observed_values_loaded_source() -> LoadedQuerySource {
        let source_spec = parse_source_manifest_yaml(
            r"
name: github
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://api.github.com
tables:
  - name: issues
    description: Issues
    request:
      path: /issues
    columns:
      - name: title
        type: Utf8
",
        )
        .expect("parse source manifest");
        LoadedQuerySource {
            source: InstalledSource {
                name: SourceName::parse("github").expect("source name"),
                version: None,
                variables: BTreeMap::new(),
                secrets: Vec::new(),
                credential_storage: None,
                credential_revision: uuid::Uuid::default(),
                origin: SourceOrigin::Bundled,
            },
            query_source: QuerySource::new(source_spec.clone(), BTreeMap::new(), BTreeMap::new()),
            runtime_contract_fingerprint: RuntimeContractFingerprint::for_test("contract"),
            credential_material: BTreeMap::new(),
        }
    }
}
