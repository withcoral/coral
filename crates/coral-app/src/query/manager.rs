//! Query-time loading, validation, and execution over installed sources.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use std::time::SystemTime;

use coral_engine::{
    CatalogInfo, CoralQuery, CoreError, QueryExecution, QueryPlan, QueryRuntimeConfig,
    QueryRuntimeContext, QuerySource, SourceValidationReport, StatusCode, TableInfo,
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
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::{AppStateLayout, ConfigStore};
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

#[derive(Debug, Clone, Eq, PartialEq)]
struct CatalogCacheKey {
    sources: Vec<CatalogSourceCacheKey>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CatalogSourceCacheKey {
    name: String,
    version: Option<String>,
    variables: BTreeMap<String, String>,
    secrets: Vec<String>,
    origin: SourceOrigin,
    manifest_file_len: Option<u64>,
    manifest_modified: Option<SystemTime>,
}

#[derive(Debug, Clone)]
struct CachedCatalog {
    workspace_name: WorkspaceName,
    key: CatalogCacheKey,
    catalog: CatalogInfo,
}

#[derive(Clone)]
pub(crate) struct QueryManager {
    config_store: ConfigStore,
    credential_manager: CredentialManager,
    runtime_context: QueryRuntimeContext,
    layout: AppStateLayout,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    catalog_cache: Arc<Mutex<Option<CachedCatalog>>>,
}

impl QueryManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> Self {
        Self {
            config_store,
            credential_manager,
            runtime_context,
            layout,
            engine_extensions_providers,
            catalog_cache: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn invalidate_catalog_cache(&self) {
        *self.catalog_cache.lock().expect("catalog cache poisoned") = None;
    }

    pub(crate) async fn list_tables(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, QueryManagerError> {
        let catalog = self.catalog_info(workspace_name).await?;
        Ok(catalog
            .tables
            .into_iter()
            .filter(|table| schema_filter.is_none_or(|schema| table.schema_name == schema))
            .filter(|table| table_filter.is_none_or(|name| table.table_name == name))
            .collect())
    }

    pub(crate) async fn list_catalog(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
    ) -> Result<CatalogInfo, QueryManagerError> {
        let catalog = self.catalog_info(workspace_name).await?;
        Ok(filter_catalog(catalog, schema_filter))
    }

    async fn catalog_info(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<CatalogInfo, QueryManagerError> {
        let sources = self
            .workspace_sources(workspace_name)
            .map_err(QueryManagerError::App)?;
        let key = self.catalog_cache_key(workspace_name, &sources);
        if let Some(cached) = self
            .catalog_cache
            .lock()
            .expect("catalog cache poisoned")
            .as_ref()
            && cached.workspace_name == *workspace_name
            && cached.key == key
        {
            return Ok(cached.catalog.clone());
        }

        let query_sources = self.load_query_sources_from_sources(workspace_name, &sources);
        let runtime = self.runtime_config(&query_sources);
        let catalog = CoralQuery::list_catalog(&query_sources, runtime, None)
            .await
            .map_err(QueryManagerError::Core)?;
        *self.catalog_cache.lock().expect("catalog cache poisoned") = Some(CachedCatalog {
            workspace_name: workspace_name.clone(),
            key,
            catalog: catalog.clone(),
        });
        Ok(catalog)
    }

    fn workspace_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, AppError> {
        let catalog = self.config_store.load_catalog()?;
        Ok(catalog.workspace_sources(workspace_name))
    }

    fn catalog_cache_key(
        &self,
        workspace_name: &WorkspaceName,
        sources: &[InstalledSource],
    ) -> CatalogCacheKey {
        CatalogCacheKey {
            sources: sources
                .iter()
                .map(|source| {
                    let manifest_metadata = match source.origin {
                        SourceOrigin::Bundled => None,
                        SourceOrigin::Imported => std::fs::metadata(
                            self.layout.manifest_file(workspace_name, &source.name),
                        )
                        .ok(),
                    };
                    CatalogSourceCacheKey {
                        name: source.name.to_string(),
                        version: source.version.clone(),
                        variables: source.variables.clone(),
                        secrets: source.secrets.clone(),
                        origin: source.origin,
                        manifest_file_len: manifest_metadata.as_ref().map(std::fs::Metadata::len),
                        manifest_modified: manifest_metadata
                            .and_then(|metadata| metadata.modified().ok()),
                    }
                })
                .collect(),
        }
    }

    pub(crate) async fn execute_sql(
        &self,
        workspace_name: &WorkspaceName,
        sql: &str,
    ) -> Result<QueryExecution, QueryManagerError> {
        run_query_operation(
            QueryOperation::ExecuteSql,
            workspace_name,
            sql,
            async {
                let sources = self
                    .load_query_sources(workspace_name)
                    .map_err(QueryManagerError::App)?;
                let runtime = self.runtime_config(&sources);
                CoralQuery::execute_sql(&sources, runtime, sql)
                    .await
                    .map_err(QueryManagerError::Core)
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
        run_query_operation(
            QueryOperation::ExplainSql,
            workspace_name,
            sql,
            async {
                let sources = self
                    .load_query_sources(workspace_name)
                    .map_err(QueryManagerError::App)?;
                let runtime = self.runtime_config(&sources);
                CoralQuery::explain_sql(&sources, runtime, sql)
                    .await
                    .map_err(QueryManagerError::Core)
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
        let report = CoralQuery::validate_source(
            &query_source,
            runtime,
            query_source.source_spec().test_queries(),
        )
        .await
        .map_err(QueryManagerError::Core)?;
        let mut source = source;
        source.version = Some(version);

        Ok(ValidatedSource { source, report })
    }

    fn load_query_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<QuerySource>, AppError> {
        let sources = self.workspace_sources(workspace_name)?;
        Ok(self.load_query_sources_from_sources(workspace_name, &sources))
    }

    fn load_query_sources_from_sources(
        &self,
        workspace_name: &WorkspaceName,
        sources: &[InstalledSource],
    ) -> Vec<QuerySource> {
        let mut query_sources = Vec::new();
        for source in sources {
            match self.load_query_source(workspace_name, source) {
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
        query_sources
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
}

fn filter_catalog(mut catalog: CatalogInfo, schema_filter: Option<&str>) -> CatalogInfo {
    if let Some(schema_name) = schema_filter {
        catalog
            .tables
            .retain(|table| table.schema_name == schema_name);
        catalog
            .table_functions
            .retain(|function| function.schema_name == schema_name);
    }
    catalog
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
