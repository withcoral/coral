//! Query-time loading, validation, and execution over installed sources.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use coral_engine::{
    CatalogInfo, CoralQuery, CoreError, DescribeTableInfo, QueryExecution,
    QueryExecutionProvenance, QueryPlan, QueryRuntimeConfig, QueryRuntimeContext, QuerySource,
    RuntimeSourcePackage, SourceInputResolver, SourceValidationReport, StatusCode, TableInfo,
    UdfRuntimeDefinition,
};
use coral_spec::{ManifestInputKind, ManifestInputSpec};
use opentelemetry::trace::Status as OtelStatus;
use serde_json::json;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId, CredentialsError};
use crate::functions::manager::{FunctionListing, FunctionManager};
use crate::query::QueryAttribution;
use crate::query::extensions::{EngineExtensionsProvider, engine_extensions_for_providers};
use crate::query::input_resolver::{
    CredentialRefreshingInputResolver, SourceCredentialSnapshot, StoredCredentialInputResolver,
};
use crate::sources::SourceName;
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::materialization::{
    incompatible_materialization_error, load_v4_materialization,
};
use crate::sources::model::InstalledSource;
use crate::sources::runtime_package::runtime_components_for_v4_source;
use crate::state::{AppConfig, AppStateLayout, ConfigStore};
use crate::task::id::TaskId;
use crate::telemetry::WORKSPACE_SPAN_ATTRIBUTE;
use crate::workspaces::{WorkspaceManager, WorkspaceName};

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
    credential_material: BTreeMap<String, String>,
}

#[derive(Clone)]
pub(crate) struct QueryManager {
    config_store: ConfigStore,
    workspace_manager: Arc<WorkspaceManager>,
    credential_manager: CredentialManager,
    function_manager: FunctionManager,
    runtime_context: QueryRuntimeContext,
    layout: AppStateLayout,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
}

impl QueryManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        workspace_manager: WorkspaceManager,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> Self {
        let function_manager = FunctionManager::new(config_store.clone(), &layout);
        Self::with_function_manager(
            config_store,
            workspace_manager,
            credential_manager,
            function_manager,
            runtime_context,
            layout,
            engine_extensions_providers,
        )
    }

    pub(crate) fn with_function_manager(
        config_store: ConfigStore,
        workspace_manager: WorkspaceManager,
        credential_manager: CredentialManager,
        function_manager: FunctionManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> Self {
        Self {
            config_store,
            workspace_manager: Arc::new(workspace_manager),
            credential_manager,
            function_manager,
            runtime_context,
            layout,
            engine_extensions_providers,
        }
    }

    pub(crate) async fn list_tables(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
        attribution: &QueryAttribution,
    ) -> Result<Vec<TableInfo>, QueryManagerError> {
        let trace_sql = list_tables_trace_sql(schema_filter, table_filter);
        run_query_operation(
            QueryOperation::ListTables,
            workspace_name,
            &trace_sql,
            attribution.task_id.as_ref(),
            async {
                let (loaded_sources, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let runtime = self
                    .runtime_config_with_udfs(
                        workspace_name,
                        &loaded_sources,
                        &config,
                        CredentialResolutionMode::StoredOnly,
                    )
                    .await?;
                let sources = query_sources_from_loaded(&loaded_sources);
                CoralQuery::list_tables(&sources, runtime, schema_filter, table_filter)
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
        schema_filter: Option<&str>,
        attribution: &QueryAttribution,
    ) -> Result<CatalogInfo, QueryManagerError> {
        let trace_sql = list_catalog_trace_sql(schema_filter);
        run_query_operation(
            QueryOperation::ListCatalog,
            workspace_name,
            &trace_sql,
            attribution.task_id.as_ref(),
            async {
                let (loaded_sources, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let runtime = self
                    .runtime_config_with_udfs(
                        workspace_name,
                        &loaded_sources,
                        &config,
                        CredentialResolutionMode::StoredOnly,
                    )
                    .await?;
                let sources = query_sources_from_loaded(&loaded_sources);
                CoralQuery::list_catalog(&sources, runtime, schema_filter)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |catalog| {
                Some(
                    u64::try_from(
                        catalog
                            .tables
                            .len()
                            .saturating_add(catalog.table_functions.len()),
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
        schema_name: &str,
        table_name: &str,
        attribution: &QueryAttribution,
    ) -> Result<DescribeTableInfo, QueryManagerError> {
        let trace_sql = describe_table_trace_sql(schema_name, table_name);
        run_query_operation(
            QueryOperation::DescribeTable,
            workspace_name,
            &trace_sql,
            attribution.task_id.as_ref(),
            async {
                let (loaded_sources, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let runtime = self
                    .runtime_config_with_udfs(
                        workspace_name,
                        &loaded_sources,
                        &config,
                        CredentialResolutionMode::Refreshing,
                    )
                    .await?;
                let sources = query_sources_from_loaded(&loaded_sources);
                CoralQuery::describe_table(&sources, runtime, schema_name, table_name)
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
                let (loaded_sources, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let runtime = self
                    .runtime_config_with_udfs(
                        workspace_name,
                        &loaded_sources,
                        &config,
                        CredentialResolutionMode::Refreshing,
                    )
                    .await?;
                let sources = query_sources_from_loaded(&loaded_sources);
                CoralQuery::execute_sql(&sources, runtime, sql)
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
                let (loaded_sources, config) = self
                    .load_query_sources(workspace_name)
                    .await
                    .map_err(QueryManagerError::App)?;
                let runtime = self
                    .runtime_config_with_udfs(
                        workspace_name,
                        &loaded_sources,
                        &config,
                        CredentialResolutionMode::Refreshing,
                    )
                    .await?;
                let sources = query_sources_from_loaded(&loaded_sources);
                CoralQuery::explain_sql(&sources, runtime, sql)
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
            .runtime_config(
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
    ) -> Result<(Vec<LoadedQuerySource>, AppConfig), AppError> {
        self.require_workspace(workspace_name).await?;
        let _state_lock = self.config_store.state_lock_shared()?;
        let config = self.config_store.load_config_unlocked()?;
        let sources = self.load_query_sources_from_config(workspace_name, &config)?;
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
    ) -> Result<Vec<LoadedQuerySource>, AppError> {
        let span = tracing::info_span!(
            "coral.app.query_sources.load",
            workspace = tracing::field::Empty,
            source.count = tracing::field::Empty,
        );
        span.record(WORKSPACE_SPAN_ATTRIBUTE, workspace_name.as_str());
        let _guard = span.enter();
        let mut loaded_sources = Vec::new();
        for source in config.workspace_sources(workspace_name) {
            match self.load_query_source(workspace_name, &source) {
                Ok((loaded_source, _version)) => loaded_sources.push(loaded_source),
                Err(
                    error @ (AppError::Credentials(CredentialsError::Unavailable(_))
                    | AppError::MissingOrIncompatibleV4Materialization { .. }
                    | AppError::InvalidV4ProjectionOverride { .. }),
                ) => {
                    return Err(error);
                }
                Err(error) => {
                    tracing::warn!(
                        source = %source.name,
                        detail = %error,
                        "skipping source during query-source load"
                    );
                }
            }
        }
        span.record("source.count", loaded_sources.len());
        Ok(loaded_sources)
    }

    fn load_query_source(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<(LoadedQuerySource, Option<String>), AppError> {
        let installed = resolve_installed_manifest(workspace_name, source, &self.layout)?;
        let source_spec = installed.source_spec;
        let v4_runtime_components = if let Some(v4) = source_spec.as_v4() {
            let materialized = load_v4_materialization(
                &self.layout,
                workspace_name,
                &source.name,
                &installed.manifest_yaml,
                v4,
            )?;
            Some(
                runtime_components_for_v4_source(v4, &materialized).map_err(|error| {
                    incompatible_materialization_error(
                        &source.name,
                        format!("failed to assemble runtime package: {error}"),
                    )
                })?,
            )
        } else {
            None
        };
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
            return Err(AppError::FailedPrecondition(format!(
                "source '{}' is missing {detail}",
                source.name
            )));
        }
        for secret_name in source_spec.declared_secret_names() {
            if let Some(value) = stored_secrets.get(&secret_name) {
                resolved_secrets.insert(secret_name, value.clone());
            }
        }
        let query_source = if let Some(components) = v4_runtime_components {
            QuerySource::from_runtime_components(
                RuntimeSourcePackage {
                    source_name: source_spec.schema_name().to_string(),
                    authored_version: source_spec.source_version().map(ToString::to_string),
                    description: source_spec.description().to_string(),
                    declared_inputs: source_spec.declared_inputs().to_vec(),
                    test_queries: source_spec.test_queries().to_vec(),
                    components,
                },
                source.variables.clone(),
                resolved_secrets,
            )
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))?
        } else {
            QuerySource::from_manifest(&source_spec, source.variables.clone(), resolved_secrets)
        };
        Ok((
            LoadedQuerySource {
                source: source.clone(),
                query_source,
                credential_material: stored_secrets,
            },
            installed.candidate.version,
        ))
    }

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
        )
    }

    fn runtime_config_with_credential_mode(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[LoadedQuerySource],
        config: &AppConfig,
        credential_resolution_mode: CredentialResolutionMode,
    ) -> Result<QueryRuntimeConfig, AppError> {
        let query_sources = query_sources_from_loaded(selected_sources);
        let mut extensions =
            engine_extensions_for_providers(&self.engine_extensions_providers, &query_sources);
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
        let selected_source_names = selected_sources
            .iter()
            .map(|source| source.query_source.source_name().to_string())
            .collect::<Vec<_>>();
        runtime.memory = config.memory_config()?;
        runtime.dependent_join = config.dependent_join_config(&selected_source_names)?;
        Ok(runtime)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "function lifecycle service callers land upstack in the split stack"
        )
    )]
    pub(crate) async fn list_functions(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<FunctionListing>, QueryManagerError> {
        let (loaded_sources, config) = self
            .load_query_sources(workspace_name)
            .await
            .map_err(QueryManagerError::App)?;
        let sources = query_sources_from_loaded(&loaded_sources);
        self.function_manager
            .list_functions(workspace_name, &sources, || {
                self.runtime_config(workspace_name, &loaded_sources, &config)
            })
            .await
            .map_err(QueryManagerError::App)
    }

    #[expect(
        dead_code,
        reason = "function lifecycle service callers land upstack in the split stack"
    )]
    pub(crate) fn function_manager(&self) -> FunctionManager {
        self.function_manager.clone()
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "function lifecycle service callers land upstack in the split stack"
        )
    )]
    pub(crate) async fn validate_udf_sql(
        &self,
        workspace_name: &WorkspaceName,
        raw_sql: &str,
    ) -> Result<UdfRuntimeDefinition, QueryManagerError> {
        self.require_workspace(workspace_name)
            .await
            .map_err(QueryManagerError::App)?;
        let (loaded_sources, config) = {
            let _state_lock = self
                .config_store
                .state_lock_shared()
                .map_err(QueryManagerError::App)?;
            let config = self
                .config_store
                .load_config_unlocked()
                .map_err(QueryManagerError::App)?;
            let loaded_sources = self
                .load_query_sources_from_config(workspace_name, &config)
                .map_err(QueryManagerError::App)?;
            (loaded_sources, config)
        };
        let sources = query_sources_from_loaded(&loaded_sources);
        self.function_manager
            .validate_user_function_sql(
                workspace_name,
                &sources,
                || self.runtime_config(workspace_name, &loaded_sources, &config),
                raw_sql,
            )
            .await
            .map_err(QueryManagerError::App)
    }

    async fn runtime_config_with_udfs(
        &self,
        workspace_name: &WorkspaceName,
        selected_sources: &[LoadedQuerySource],
        config: &AppConfig,
        credential_resolution_mode: CredentialResolutionMode,
    ) -> Result<QueryRuntimeConfig, QueryManagerError> {
        let runtime = self
            .runtime_config_with_credential_mode(
                workspace_name,
                selected_sources,
                config,
                credential_resolution_mode,
            )
            .map_err(QueryManagerError::App)?;
        let query_sources = query_sources_from_loaded(selected_sources);
        let functions = self
            .function_manager
            .load_runtime_udfs(workspace_name, &query_sources, || {
                self.runtime_config_with_credential_mode(
                    workspace_name,
                    selected_sources,
                    config,
                    credential_resolution_mode,
                )
            })
            .await
            .map_err(QueryManagerError::App)?;
        Ok(runtime.with_udfs(functions))
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

fn list_tables_trace_sql(schema_filter: Option<&str>, table_filter: Option<&str>) -> String {
    match (schema_filter, table_filter) {
        (Some(schema), Some(table)) => format!("LIST TABLES {schema}.{table}"),
        (Some(schema), None) => format!("LIST TABLES {schema}.*"),
        (None, Some(table)) => format!("LIST TABLES *.{table}"),
        (None, None) => "LIST TABLES *.*".to_string(),
    }
}

fn list_catalog_trace_sql(schema_filter: Option<&str>) -> String {
    match schema_filter {
        Some(schema) => format!("LIST CATALOG {schema}"),
        None => "LIST CATALOG".to_string(),
    }
}

fn describe_table_trace_sql(schema_name: &str, table_name: &str) -> String {
    format!("DESCRIBE TABLE {schema_name}.{table_name}")
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
        AppError::MissingOrIncompatibleV4Materialization { .. } => {
            "MISSING_OR_INCOMPATIBLE_V4_MATERIALIZATION"
        }
        AppError::InvalidV4ProjectionOverride { .. } => "INVALID_V4_PROJECTION_OVERRIDE",
        AppError::CredentialRefresh(_) => "CREDENTIAL_REFRESH",
        AppError::Unavailable(_) => "UNAVAILABLE",
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
        return Err(AppError::FailedPrecondition(format!(
            "source '{}' is missing {detail}",
            source.name
        )));
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
        QueryTableUsage, SourceInputResolutionContext, SourceInputResolver,
        SourceInputResolverError,
    };
    use coral_spec::parse_source_manifest_yaml;
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::credentials::{CredentialStorageKind, CredentialStoragePreference, CredentialStore};
    use crate::sources::manager::{ImportSourceCommand, SourceBindings, SourceManager};
    use crate::sources::model::SourceOrigin;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, run_state_migrations};

    struct QueryManagerFixture {
        _temp: TempDir,
        manager: QueryManager,
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
        let manager = QueryManager::new(
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
        }
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
        run_state_migrations(&db, config_store)
            .await
            .expect("run state migrations");
        Arc::new(db)
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

        let error = fixture
            .manager
            .load_query_sources(&missing_workspace)
            .await
            .expect_err("missing workspace should fail closed");

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
        let service = QueryService::new(fixture.manager.clone());

        let mut request = Request::new(ExecuteSqlRequest {
            workspace: Some(Workspace {
                name: WorkspaceName::default().as_str().to_string(),
            }),
            sql: "SELECT 1".to_string(),
        });
        request.extensions_mut().insert(
            crate::task::id::TaskId::parse("550e8400-e29b-41d4-a716-446655440000")
                .expect("task id"),
        );

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
        assert_eq!(
            task_attr.value.as_str(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
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
        let service = CatalogService::new(fixture.manager.clone());

        call_catalog_tools_with_task(&service).await;

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        assert_catalog_task_spans(&spans);
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

    async fn call_catalog_tools_with_task(service: &crate::catalog::service::CatalogService) {
        use coral_api::v1::catalog_service_server::CatalogService as CatalogServiceApi;
        use coral_api::v1::{
            DescribeTableRequest, ListCatalogRequest, ListColumnsRequest, PaginationRequest,
            SearchCatalogRequest,
        };

        let _list_catalog_result = service
            .list_catalog(tagged_catalog_request(ListCatalogRequest {
                workspace: Some(default_workspace_proto()),
                schema_name: String::new(),
                kind: 0,
                pagination: Some(PaginationRequest {
                    limit: 10,
                    offset: 0,
                }),
            }))
            .await;
        let _search_catalog_result = service
            .search_catalog(tagged_catalog_request(SearchCatalogRequest {
                workspace: Some(default_workspace_proto()),
                pattern: "tables".to_string(),
                ignore_case: true,
                schema_name: String::new(),
                kind: 0,
                pagination: Some(PaginationRequest {
                    limit: 10,
                    offset: 0,
                }),
            }))
            .await;
        let _describe_table_result = service
            .describe_table(tagged_catalog_request(DescribeTableRequest {
                workspace: Some(default_workspace_proto()),
                schema_name: "coral".to_string(),
                table_name: "tables".to_string(),
            }))
            .await;
        let _list_columns_result = service
            .list_columns(tagged_catalog_request(ListColumnsRequest {
                workspace: Some(default_workspace_proto()),
                schema_name: "coral".to_string(),
                table_name: "tables".to_string(),
                pattern: None,
                ignore_case: true,
                required_only: false,
                pagination: Some(PaginationRequest {
                    limit: 10,
                    offset: 0,
                }),
            }))
            .await;
    }

    fn default_workspace_proto() -> coral_api::v1::Workspace {
        coral_api::v1::Workspace {
            name: WorkspaceName::default().as_str().to_string(),
        }
    }

    fn tagged_catalog_request<T>(message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.extensions_mut().insert(
            crate::task::id::TaskId::parse("650e8400-e29b-41d4-a716-446655440000")
                .expect("task id"),
        );
        request
    }

    fn assert_catalog_task_spans(spans: &[opentelemetry_sdk::trace::SpanData]) {
        let attributed_query_spans = spans
            .iter()
            .filter(|span| {
                span.name == "coral.query"
                    && span.attributes.iter().any(|attribute| {
                        attribute.key.as_str() == "task.id"
                            && attribute.value.as_str() == "650e8400-e29b-41d4-a716-446655440000"
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
    async fn installed_v4_source_queries_through_app_assembled_runtime_component() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/issues"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"id": 1, "title": "Generated runtime package"}
            ])))
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
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        std::fs::write(
            &openapi_file,
            format!(
                r"
openapi: 3.0.3
info:
  title: GitHub
servers:
  - url: {}
paths:
  /issues:
    get:
      operationId: issues/list
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
                    title: {{type: string}}
",
                server.uri()
            ),
        )
        .expect("write OpenAPI fixture");
        source_manager
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: format!(
                        r"
name: github_v4_query
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
",
                        openapi_file.display()
                    ),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import v4 source");
        std::fs::remove_file(&openapi_file).expect("remove authored descriptor after import");

        let execution = fixture
            .manager
            .execute_sql(
                &workspace_name,
                "SELECT id, title FROM github_v4_query.issues",
                &QueryAttribution::default(),
            )
            .await
            .expect("query executes");

        assert_eq!(
            execution_to_rows(&execution),
            vec![json!({"id": 1, "title": "Generated runtime package"})]
        );
    }

    #[tokio::test]
    async fn installed_v4_source_uses_parameter_metadata_pagination_override() {
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
surfaces:
  - id: rest
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
        write_widgets_parameter_metadata_override(
            &fixture.manager.layout,
            &workspace_name,
            &source_name,
        );

        let execution = fixture
            .manager
            .execute_sql(
                &workspace_name,
                "SELECT id FROM github_v4_pagination_override.widgets LIMIT 3",
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

    fn write_widgets_parameter_metadata_override(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) {
        let override_path =
            layout.v4_parameter_metadata_override_file(workspace_name, source_name, "rest");
        std::fs::create_dir_all(override_path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::write(
            &override_path,
            r"
pagination:
  - name: widgets_page
    match:
      operation_ids: [widgets/list]
    mode: page
    page_param: page
    page_start: 1
    page_size:
      default: 2
      max: 2
      query_param: per_page
",
        )
        .expect("write parameter metadata override");
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
    #[expect(
        clippy::too_many_lines,
        reason = "covers function validation, catalog publication, listing, and execution together"
    )]
    async fn user_function_publishes_table_function_and_executes_against_installed_source() {
        let fake_home = tempfile::tempdir().expect("fake home");
        let data_dir = fake_home.path().join("fixture-data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        std::fs::write(
            data_dir.join("messages.jsonl"),
            r#"{"type":"user","text":"hello"}
{"type":"assistant","text":"world"}
"#,
        )
        .expect("write fixture");

        let fixture = query_manager_with(
            QueryRuntimeContext {
                home_dir: Some(fake_home.path().to_path_buf()),
                ..QueryRuntimeContext::default()
            },
            Vec::new(),
        )
        .await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_manager = SourceManager::new_for_tests(
            fixture.manager.config_store.clone(),
            fixture.manager.credential_manager.clone(),
            fixture.manager.layout.clone(),
        );
        source_manager
            .import_source(
                &workspace_name,
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

        let function_sql = r"/*
name: messages_by_type
schema: functions
description: Messages filtered by sender type
*/

select text
from function_demo.messages
where type = $kind
";
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
                Some("functions"),
                &QueryAttribution::default(),
            )
            .await
            .expect("catalog");
        let function_function = catalog.table_functions.first().expect("function function");
        assert_eq!(function_function.function_name, "messages_by_type");
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
        let column = function
            .definition
            .as_ref()
            .expect("runtime-ready function")
            .result_columns
            .first()
            .expect("text result column");
        assert_eq!(column.name, "text");

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

    #[tokio::test]
    async fn load_query_sources_fails_closed_for_missing_v4_materialization() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new()).await;
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("github_v4_missing_artifacts").expect("source name");
        let manifest_path = fixture
            .manager
            .layout
            .manifest_file(&workspace_name, &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create source dir");
        std::fs::write(
            &manifest_path,
            r"
name: github_v4_missing_artifacts
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
",
        )
        .expect("write manifest");
        fixture
            .manager
            .config_store
            .upsert_source(
                &workspace_name,
                InstalledSource {
                    name: source_name.clone(),
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: Vec::new(),
                    credential_storage: None,
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("persist source");

        let error = fixture
            .manager
            .load_query_sources(&workspace_name)
            .await
            .expect_err("missing materialization should fail closed");

        assert!(
            matches!(
                error,
                AppError::MissingOrIncompatibleV4Materialization { .. }
            ),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn load_query_sources_fails_closed_for_unavailable_keychain_source() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = test_db(&layout, &config_store).await;
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("github").expect("source name");
        config_store
            .upsert_source(
                &workspace_name,
                InstalledSource {
                    name: source_name,
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: vec!["GITHUB_TOKEN".to_string()],
                    credential_storage: Some(CredentialStorageKind::Keychain),
                    origin: SourceOrigin::Bundled,
                },
            )
            .expect("persist source");
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
        let manager = QueryManager::new(
            config_store,
            workspace_manager,
            credential_manager,
            QueryRuntimeContext::default(),
            layout,
            Vec::new(),
        );
        let error = manager
            .load_query_sources(&workspace_name)
            .await
            .expect_err("unavailable keychain should fail closed");

        assert!(
            matches!(
                error,
                AppError::Credentials(CredentialsError::Unavailable(_))
            ),
            "unexpected error: {error:#}"
        );
        assert!(
            error
                .to_string()
                .contains("configured for keychain storage"),
            "keychain-routed query failure should name the routed backend: {error}"
        );
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
                origin: SourceOrigin::Bundled,
            },
            query_source: QuerySource::new(source_spec, BTreeMap::new(), BTreeMap::new()),
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
}
