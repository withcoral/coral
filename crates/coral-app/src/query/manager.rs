//! Query-time loading, validation, and execution over installed sources.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use coral_engine::{
    CatalogInfo, CoralQuery, CoreError, QueryExecution, QueryPlan, QueryRuntimeConfig,
    QueryRuntimeContext, QuerySource, SourceValidationReport, StatusCode, TableInfo,
    QueryCacheInput, QueryCacheOperation, query_cache_fingerprint,
};
use coral_spec::{ManifestInputKind, ManifestInputSpec};
use opentelemetry::{KeyValue, trace::Status as OtelStatus};
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId};
use crate::query::extensions::{EngineExtensionsProvider, engine_extensions_for_providers};
use crate::sources::SourceName;
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::model::InstalledSource;
use crate::state::CacheConfig;
use crate::state::{AppStateLayout, ConfigStore};
use crate::storage::cache::{
    CacheScope, CacheValueKind, QueryCache, cache_fingerprint_digest, cache_scope,
    cache_settings_from_config,
};
use crate::workspaces::WorkspaceName;

#[derive(Debug)]
pub(crate) enum QueryManagerError {
    App(AppError),
    Core(CoreError),
}

pub(crate) struct ValidatedSource {
    pub(crate) source: InstalledSource,
    pub(crate) report: SourceValidationReport,
}

#[derive(Clone)]
pub(crate) struct QueryManager {
    config_store: ConfigStore,
    credential_manager: CredentialManager,
    runtime_context: QueryRuntimeContext,
    layout: AppStateLayout,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    cache: Arc<QueryCache>,
}

impl QueryManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        cache_config: CacheConfig,
    ) -> Self {
        let cache = Arc::new(QueryCache::new(
            layout.clone(),
            cache_settings_from_config(cache_config),
        ));
        Self {
            config_store,
            credential_manager,
            runtime_context,
            layout,
            engine_extensions_providers,
            cache,
        }
    }

    pub(crate) async fn list_tables(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, QueryManagerError> {
        let sources = self
            .load_query_sources(workspace_name)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_config(&sources);
        let cache_key = self.cache_key_for_list_tables(
            workspace_name,
            schema_filter,
            table_filter,
            &sources,
        );
        if let Some(cached) = self
            .cache
            .get_json::<Vec<TableInfo>>(&cache_key, CacheValueKind::TableInfoList)
            .map_err(QueryManagerError::App)?
        {
            record_cache_event("list_tables", true);
            return Ok(cached);
        }
        record_cache_event("list_tables", false);
        CoralQuery::list_tables(&sources, runtime, schema_filter, table_filter)
            .await
            .map_err(QueryManagerError::Core)
            .inspect(|result| {
                let _ = self.cache.put_json(
                    &cache_key,
                    cache_scope(
                        workspace_name.as_str(),
                        source_names(&sources),
                    ),
                    CacheValueKind::TableInfoList,
                    result,
                );
            })
    }

    pub(crate) async fn list_catalog(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
    ) -> Result<CatalogInfo, QueryManagerError> {
        let sources = self
            .load_query_sources(workspace_name)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_config(&sources);
        let cache_key = self.cache_key_for_list_catalog(workspace_name, schema_filter, &sources);
        if let Some(cached) = self
            .cache
            .get_json::<CatalogInfo>(&cache_key, CacheValueKind::CatalogInfo)
            .map_err(QueryManagerError::App)?
        {
            record_cache_event("list_catalog", true);
            return Ok(cached);
        }
        record_cache_event("list_catalog", false);
        CoralQuery::list_catalog(&sources, runtime, schema_filter)
            .await
            .map_err(QueryManagerError::Core)
            .inspect(|result| {
                let _ = self.cache.put_json(
                    &cache_key,
                    cache_scope(workspace_name.as_str(), source_names(&sources)),
                    CacheValueKind::CatalogInfo,
                    result,
                );
            })
    }

    pub(crate) async fn execute_sql(
        &self,
        workspace_name: &WorkspaceName,
        sql: &str,
    ) -> Result<QueryExecution, QueryManagerError> {
        let cache_key = self.cache_key_for_sql(
            workspace_name,
            QueryCacheOperation::ExecuteSql,
            sql,
            &[],
        );
        run_query_operation(
            QueryOperation::ExecuteSql,
            workspace_name,
            sql,
            async {
                let sources = self
                    .load_query_sources(workspace_name)
                    .map_err(QueryManagerError::App)?;
                let runtime = self.runtime_config(&sources);
                if let Some(cached) = self
                    .cache
                    .get_query_execution(&cache_key)
                    .map_err(QueryManagerError::App)?
                {
                    record_cache_event("execute_sql", true);
                    return Ok(cached);
                }
                record_cache_event("execute_sql", false);
                CoralQuery::execute_sql(&sources, runtime, sql)
                    .await
                    .map_err(QueryManagerError::Core)
                    .inspect(|result| {
                        let _ = self.cache.put_query_execution(
                            &cache_key,
                            cache_scope(workspace_name.as_str(), source_names(&sources)),
                            result,
                        );
                    })
            },
            |execution| Some(u64::try_from(execution.row_count()).unwrap_or(u64::MAX)),
        )
        .await
    }

    pub(crate) async fn explain_sql(
        &self,
        workspace_name: &WorkspaceName,
        sql: &str,
    ) -> Result<QueryPlan, QueryManagerError> {
        let cache_key = self.cache_key_for_sql(
            workspace_name,
            QueryCacheOperation::ExplainSql,
            sql,
            &[],
        );
        run_query_operation(
            QueryOperation::ExplainSql,
            workspace_name,
            sql,
            async {
                let sources = self
                    .load_query_sources(workspace_name)
                    .map_err(QueryManagerError::App)?;
                let runtime = self.runtime_config(&sources);
                if let Some(cached) = self
                    .cache
                    .get_json::<QueryPlan>(&cache_key, CacheValueKind::QueryPlan)
                    .map_err(QueryManagerError::App)?
                {
                    record_cache_event("explain_sql", true);
                    return Ok(cached);
                }
                record_cache_event("explain_sql", false);
                CoralQuery::explain_sql(&sources, runtime, sql)
                    .await
                    .map_err(QueryManagerError::Core)
                    .inspect(|result| {
                        let _ = self.cache.put_json(
                            &cache_key,
                            cache_scope(workspace_name.as_str(), source_names(&sources)),
                            CacheValueKind::QueryPlan,
                            result,
                        );
                    })
            },
            |_| None,
        )
        .await
    }

    pub(crate) async fn validate_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<ValidatedSource, QueryManagerError> {
        let source = self
            .config_store
            .get_source(workspace_name, source_name)
            .map_err(QueryManagerError::App)?;
        let (query_source, version) = self
            .load_query_source(workspace_name, &source)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_config(std::slice::from_ref(&query_source));
        let cache_key = self.cache_key_for_validate_source(workspace_name, &source, &query_source);
        if let Some(cached) = self
            .cache
            .get_json::<SourceValidationReport>(&cache_key, CacheValueKind::SourceValidationReport)
            .map_err(QueryManagerError::App)?
        {
            record_cache_event("validate_source", true);
            let mut source = source;
            source.version = Some(version);
            return Ok(ValidatedSource { source, report: cached });
        }
        record_cache_event("validate_source", false);
        let report = CoralQuery::validate_source(
            &query_source,
            runtime,
            query_source.source_spec().test_queries(),
        )
        .await
        .map_err(QueryManagerError::Core)?;
        let mut source = source;
        source.version = Some(version);

        let _ = self.cache.put_json(
            &cache_key,
            cache_scope(workspace_name.as_str(), vec![source.name.as_str().to_string()]),
            CacheValueKind::SourceValidationReport,
            &report,
        );

        Ok(ValidatedSource { source, report })
    }

    fn load_query_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<QuerySource>, AppError> {
        let catalog = self.config_store.load_catalog()?;
        let mut query_sources = Vec::new();
        for source in catalog.workspace_sources(workspace_name) {
            match self.load_query_source(workspace_name, &source) {
                Ok((query_source, _version)) => query_sources.push(query_source),
                Err(error) => {
                    tracing::warn!(
                        source = %source.name,
                        detail = %error,
                        "skipping source during query-source load"
                    );
                }
            }
        }
        Ok(query_sources)
    }

    fn load_query_source(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<(QuerySource, String), AppError> {
        let installed = resolve_installed_manifest(workspace_name, source, &self.layout)?;
        let source_spec = installed.source_spec;
        validate_required_variables(source, source_spec.declared_inputs())?;
        let credential_set_id = CredentialSetId::for_source(&source.name);
        let stored_secrets = self
            .credential_manager
            .read_material(workspace_name, &credential_set_id)?;
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
        for secret_name in source_spec.required_secret_names() {
            let value = stored_secrets.get(&secret_name).cloned().ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "source '{}' is missing secret '{secret_name}'",
                    source.name
                ))
            })?;
            resolved_secrets.insert(secret_name, value);
        }
        Ok((
            QuerySource::new(source_spec, source.variables.clone(), resolved_secrets),
            installed.candidate.version,
        ))
    }

    fn runtime_config(&self, selected_sources: &[QuerySource]) -> QueryRuntimeConfig {
        QueryRuntimeConfig::new(
            self.runtime_context.clone(),
            engine_extensions_for_providers(&self.engine_extensions_providers, selected_sources),
        )
    }

    fn cache_key_for_list_tables(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
        sources: &[QuerySource],
    ) -> String {
        let source_fingerprint = source_fingerprint(sources);
        query_cache_fingerprint(QueryCacheInput {
            operation: QueryCacheOperation::ListTables,
            workspace_name: workspace_name.as_str(),
            sql: None,
            source_fingerprint: &source_fingerprint,
            execution_settings: &[
                schema_filter.unwrap_or(""),
                table_filter.unwrap_or(""),
                self.runtime_context
                    .home_dir
                    .as_ref()
                    .map(|path| path.to_string_lossy())
                    .as_deref()
                    .unwrap_or(""),
            ],
        })
    }

    fn cache_key_for_list_catalog(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
        sources: &[QuerySource],
    ) -> String {
        let source_fingerprint = source_fingerprint(sources);
        query_cache_fingerprint(QueryCacheInput {
            operation: QueryCacheOperation::ListCatalog,
            workspace_name: workspace_name.as_str(),
            sql: None,
            source_fingerprint: &source_fingerprint,
            execution_settings: &[
                schema_filter.unwrap_or(""),
                self.runtime_context
                    .home_dir
                    .as_ref()
                    .map(|path| path.to_string_lossy())
                    .as_deref()
                    .unwrap_or(""),
            ],
        })
    }

    fn cache_key_for_sql(
        &self,
        workspace_name: &WorkspaceName,
        operation: QueryCacheOperation,
        sql: &str,
        sources: &[QuerySource],
    ) -> String {
        let source_fingerprint = source_fingerprint(sources);
        query_cache_fingerprint(QueryCacheInput {
            operation,
            workspace_name: workspace_name.as_str(),
            sql: Some(sql),
            source_fingerprint: &source_fingerprint,
            execution_settings: &[
                self.runtime_context
                    .home_dir
                    .as_ref()
                    .map(|path| path.to_string_lossy())
                    .as_deref()
                    .unwrap_or(""),
            ],
        })
    }

    fn cache_key_for_validate_source(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
        query_source: &QuerySource,
    ) -> String {
        let source_fingerprint = query_source_fingerprint(query_source);
        query_cache_fingerprint(QueryCacheInput {
            operation: QueryCacheOperation::ValidateSource,
            workspace_name: workspace_name.as_str(),
            sql: None,
            source_fingerprint: &source_fingerprint,
            execution_settings: &[
                source.name.as_str(),
                query_source.version(),
                self.runtime_context
                    .home_dir
                    .as_ref()
                    .map(|path| path.to_string_lossy())
                    .as_deref()
                    .unwrap_or(""),
            ],
        })
    }
}

#[derive(Clone, Copy)]
enum QueryOperation {
    ExecuteSql,
    ExplainSql,
}

impl QueryOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExecuteSql => "execute_sql",
            Self::ExplainSql => "explain_sql",
        }
    }
}

async fn run_query_operation<T, Fut, RowCount>(
    operation: QueryOperation,
    workspace_name: &WorkspaceName,
    sql: &str,
    query: Fut,
    row_count: RowCount,
) -> Result<T, QueryManagerError>
where
    Fut: Future<Output = Result<T, QueryManagerError>>,
    RowCount: FnOnce(&T) -> Option<u64>,
{
    let started_at = Instant::now();
    let query_span = create_query_span(operation, workspace_name, sql);
    let result = query.instrument(query_span.clone()).await;

    let metrics = crate::telemetry::metrics::metrics();
    let status = crate::telemetry::metrics::status_attr(result.is_ok());
    let attributes = [status, KeyValue::new("operation", operation.as_str())];
    metrics.count.add(1, &attributes);
    metrics
        .duration
        .record(started_at.elapsed().as_secs_f64(), &attributes);

    if let Ok(value) = &result {
        query_span.record("status", "ok");
        query_span.set_status(OtelStatus::Ok);
        if let Some(row_count) = row_count(value) {
            query_span.record("row_count", row_count);
            metrics.rows.record(row_count, &attributes);
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
) -> tracing::Span {
    let operation = operation.as_str();
    tracing::info_span!(
        "coral.query",
        otel.name = "coral.query",
        operation = operation,
        workspace = %workspace_name.as_str(),
        sql = %sql,
        row_count = tracing::field::Empty,
        status = tracing::field::Empty,
        error.kind = tracing::field::Empty,
        error.type = tracing::field::Empty,
        exception.message = tracing::field::Empty,
    )
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
        AppError::SourceNotFound(_) => "SOURCE_NOT_FOUND",
        AppError::InvalidInput(_) => "INVALID_INPUT",
        AppError::FailedPrecondition(_) => "FAILED_PRECONDITION",
        AppError::Io(_) => "IO",
        AppError::Yaml(_) => "YAML",
        AppError::TomlDecode(_) => "TOML_DECODE",
        AppError::TomlEncode(_) => "TOML_ENCODE",
        AppError::Json(_) => "JSON",
        AppError::Transport(_) => "TRANSPORT",
        AppError::TaskJoin(_) => "TASK_JOIN",
        AppError::Credentials(_) => "CREDENTIALS",
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

fn source_fingerprint(sources: &[QuerySource]) -> String {
    let mut parts = Vec::with_capacity(sources.len());
    for source in sources {
        parts.push(query_source_fingerprint(source));
    }
    parts.sort();
    parts.join(";")
}

fn query_source_fingerprint(source: &QuerySource) -> String {
    let mut fields = Vec::new();
    fields.push(source.source_name().to_string());
    fields.push(source.version().to_string());
    for (key, value) in source.variables() {
        fields.push(format!("var:{key}={value}"));
    }
    for (key, value) in source.secrets() {
        fields.push(format!("secret:{key}={value}"));
    }
    cache_fingerprint_digest(&fields.iter().map(String::as_str).collect::<Vec<_>>())
}

fn record_cache_event(operation: &str, hit: bool) {
    let metrics = crate::telemetry::metrics::metrics();
    let attributes = [KeyValue::new("operation", operation)];
    if hit {
        metrics.cache_hits.add(1, &attributes);
    } else {
        metrics.cache_misses.add(1, &attributes);
    }
}
