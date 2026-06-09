//! Query-time loading, validation, and execution over installed sources.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use coral_capabilities::{CapabilityId, SourceId};
use coral_exports::{Binding, CapabilityExport};
use coral_sql::{
    QueryExecution, QueryPlan, QueryTestResult, SourceValidationReport, SqlError, SqlMetadataInfo,
    SqlProviderInvocation, SqlProviderInvoker, SqlRuntimeBinding, SqlWorkspace, StatusCode,
    validate_read_only_sql,
};
use opentelemetry::{KeyValue, trace::Status as OtelStatus};
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::RuntimeExposureMode;
use crate::bootstrap::AppError;
use crate::capability::service::{SqlProviderCapabilityInvocation, invoke_sql_provider_capability};
use crate::credentials::CredentialManager;
use crate::discovery::manager::{DiscoveryManager, LoadedSourceRuntime, LoadedWorkspaceExports};
use crate::sources::SourceName;
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::model::InstalledSource;
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

#[derive(Debug)]
pub(crate) enum QueryManagerError {
    App(AppError),
    Core(SqlError),
}

pub(crate) struct ValidatedSource {
    pub(crate) source: InstalledSource,
    pub(crate) report: SourceValidationReport,
}

#[derive(Clone)]
pub(crate) struct QueryManager {
    config_store: ConfigStore,
    layout: AppStateLayout,
    credential_manager: CredentialManager,
    runtime_exposure: RuntimeExposureMode,
}

impl QueryManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        layout: AppStateLayout,
        runtime_exposure: RuntimeExposureMode,
    ) -> Self {
        Self {
            config_store,
            layout,
            credential_manager,
            runtime_exposure,
        }
    }

    fn load_sql_metadata(
        &self,
        workspace_name: &WorkspaceName,
        schema_filter: Option<&str>,
    ) -> Result<SqlMetadataInfo, QueryManagerError> {
        self.ensure_sql_exposed()?;
        let workspace = self.load_sql_workspace(workspace_name)?;
        Ok(workspace.sql_metadata(schema_filter))
    }

    pub(crate) async fn execute_sql(
        &self,
        workspace_name: &WorkspaceName,
        sql: &str,
    ) -> Result<QueryExecution, QueryManagerError> {
        self.ensure_sql_exposed()?;
        run_query_operation(
            QueryOperation::ExecuteSql,
            workspace_name,
            sql,
            async {
                let workspace = self.load_sql_workspace(workspace_name)?;
                workspace
                    .execute_sql(sql)
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
        self.ensure_sql_exposed()?;
        run_query_operation(
            QueryOperation::ExplainSql,
            workspace_name,
            sql,
            async {
                let workspace = self.load_sql_workspace(workspace_name)?;
                workspace
                    .explain_sql(sql)
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
        self.ensure_sql_exposed()?;
        let source = self
            .config_store
            .get_source(workspace_name, source_name)
            .map_err(QueryManagerError::App)?;
        let installed_manifest = resolve_installed_manifest(workspace_name, &source, &self.layout)
            .map_err(QueryManagerError::App)?;
        let loaded = DiscoveryManager::new_with_runtime_exposure(
            self.config_store.clone(),
            self.layout.clone(),
            self.runtime_exposure,
        )
        .load_source_exports(workspace_name, &source)
        .map_err(QueryManagerError::App)?;
        let source_id = coral_capabilities::SourceId(source.source_id.clone());
        let bindings = sql_runtime_bindings(&loaded)
            .into_iter()
            .filter(|binding| binding.capability.source_id == source_id)
            .collect::<Vec<_>>();
        let workspace = self.sql_workspace_from_loaded(workspace_name, &loaded, bindings);
        let sql_metadata = workspace.sql_metadata(None);
        let query_tests = run_source_query_tests(
            &workspace,
            installed_manifest.source_spec.test_queries.as_slice(),
        )
        .await;
        let report = SourceValidationReport::new(
            sql_metadata.tables,
            sql_metadata.table_functions,
            query_tests,
        );
        Ok(ValidatedSource { source, report })
    }

    fn load_sql_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<SqlWorkspace, QueryManagerError> {
        let loaded = self.load_workspace_exports(workspace_name)?;
        let bindings = sql_runtime_bindings(&loaded);
        Ok(self.sql_workspace_from_loaded(workspace_name, &loaded, bindings))
    }

    fn load_workspace_exports(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<LoadedWorkspaceExports, QueryManagerError> {
        let span = tracing::info_span!(
            "coral.app.sql_workspace.load",
            workspace = %workspace_name,
            binding.count = tracing::field::Empty,
        );
        let _guard = span.enter();
        let loaded = DiscoveryManager::new_with_runtime_exposure(
            self.config_store.clone(),
            self.layout.clone(),
            self.runtime_exposure,
        )
        .load_workspace_exports(workspace_name)
        .map_err(QueryManagerError::App)?;
        span.record("binding.count", sql_runtime_bindings(&loaded).len());
        Ok(loaded)
    }

    fn ensure_sql_exposed(&self) -> Result<(), QueryManagerError> {
        if self.runtime_exposure.exposes_sql() {
            return Ok(());
        }
        Err(QueryManagerError::App(AppError::FailedPrecondition(
            "SQL runtime exposure is disabled; start Coral with runtime exposure 'sql' or 'both' to use SQL.".to_string(),
        )))
    }

    fn sql_workspace_from_loaded(
        &self,
        workspace_name: &WorkspaceName,
        loaded: &LoadedWorkspaceExports,
        bindings: Vec<SqlRuntimeBinding>,
    ) -> SqlWorkspace {
        SqlWorkspace::new(bindings).with_provider_invoker(Arc::new(AppSqlProviderInvoker {
            workspace_name: workspace_name.clone(),
            credentials: self.credential_manager.clone(),
            entries_by_capability_id: loaded
                .exports
                .entries
                .iter()
                .map(|entry| (entry.capability_id.clone(), entry.clone()))
                .collect(),
            source_runtime_by_id: loaded.source_runtime_by_id.clone(),
        }))
    }
}

struct AppSqlProviderInvoker {
    workspace_name: WorkspaceName,
    credentials: CredentialManager,
    entries_by_capability_id: std::collections::BTreeMap<CapabilityId, CapabilityExport>,
    source_runtime_by_id: std::collections::BTreeMap<SourceId, LoadedSourceRuntime>,
}

impl std::fmt::Debug for AppSqlProviderInvoker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppSqlProviderInvoker")
            .field("workspace_name", &self.workspace_name)
            .field("entry_count", &self.entries_by_capability_id.len())
            .field("source_count", &self.source_runtime_by_id.len())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl SqlProviderInvoker for AppSqlProviderInvoker {
    async fn invoke_provider(
        &self,
        request: SqlProviderInvocation<'_>,
    ) -> Result<serde_json::Value, SqlError> {
        let entry = self
            .entries_by_capability_id
            .get(&request.capability.capability_id)
            .cloned()
            .ok_or_else(|| {
                SqlError::FailedPrecondition(format!(
                    "capability '{}' is missing from loaded SQL exports",
                    request.capability.capability_id
                ))
            })?;
        let runtime = self
            .source_runtime_by_id
            .get(&request.capability.source_id)
            .cloned()
            .ok_or_else(|| {
                SqlError::FailedPrecondition(format!(
                    "source runtime '{}' is missing from loaded SQL exports",
                    request.capability.source_id
                ))
            })?;
        Box::pin(invoke_sql_provider_capability(
            &self.workspace_name,
            &self.credentials,
            SqlProviderCapabilityInvocation {
                entry,
                capability: request.capability.clone(),
                source_materialized_dir: request.source_materialized_dir.to_path_buf(),
                source_name: runtime.name,
                credential_storage: runtime.credential_storage,
                source_variables: runtime.variables,
                args: request.args,
            },
        ))
        .await
        .map_err(SqlError::FailedPrecondition)
    }
}

async fn run_source_query_tests(
    workspace: &SqlWorkspace,
    queries: &[String],
) -> Vec<QueryTestResult> {
    let mut results = Vec::with_capacity(queries.len());
    for query in queries {
        if validate_read_only_sql(query).is_err() {
            results.push(QueryTestResult::failure(
                query.clone(),
                "test query must be read-only SQL",
            ));
            continue;
        }
        let result = match workspace.execute_sql(query).await {
            Ok(execution) => QueryTestResult::success(
                query.clone(),
                u64::try_from(execution.row_count()).unwrap_or(u64::MAX),
            ),
            Err(error) => QueryTestResult::failure(query.clone(), error.to_string()),
        };
        results.push(result);
    }
    results
}

fn sql_runtime_bindings(loaded: &LoadedWorkspaceExports) -> Vec<SqlRuntimeBinding> {
    loaded
        .exports
        .entries
        .iter()
        .flat_map(|entry| {
            let capability = loaded.capability_by_id.get(&entry.capability_id).cloned();
            let source_materialized_dir = loaded
                .source_materialized_dir_by_id
                .get(&entry.source_id)
                .cloned();
            entry.bindings.iter().filter_map(move |binding| {
                let Binding::Sql(sql_binding) = binding else {
                    return None;
                };
                Some(SqlRuntimeBinding {
                    capability: capability.clone()?,
                    binding: sql_binding.clone(),
                    source_materialized_dir: source_materialized_dir.clone()?,
                })
            })
        })
        .collect()
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
        QueryManagerError::Core(error) => error.to_string(),
    }
}

fn app_error_type(error: &AppError) -> &'static str {
    match error {
        AppError::SourceNotFound(_) => "SOURCE_NOT_FOUND",
        AppError::InvalidInput(_) => "INVALID_INPUT",
        AppError::FailedPrecondition(_) => "FAILED_PRECONDITION",
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
        AppError::MissingConfigDir => "MISSING_CONFIG_DIR",
    }
}

fn core_error_type(error: &SqlError) -> String {
    status_code_error_type(error.status_code()).to_string()
}

fn status_code_error_type(status: StatusCode) -> &'static str {
    match status {
        StatusCode::InvalidArgument => "INVALID_ARGUMENT",
        StatusCode::NotFound => "NOT_FOUND",
        StatusCode::FailedPrecondition => "FAILED_PRECONDITION",
        StatusCode::Unimplemented => "UNIMPLEMENTED",
        StatusCode::Internal => "INTERNAL",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::sources::model::SourceOrigin;

    use super::*;

    #[test]
    fn load_sql_metadata_reports_stale_source_artifacts() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let stale_source = InstalledSource {
            name: SourceName::parse("codex").expect("source name"),
            source_id: "src_codex".to_string(),
            display_name: "codex".to_string(),
            source_key: "codex".to_string(),
            version: None,
            interface_ids: Vec::new(),
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Imported,
        };
        config_store
            .upsert_source(&workspace_name, stale_source)
            .expect("stale source config");
        let manager = QueryManager::new(
            config_store,
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
            RuntimeExposureMode::Both,
        );

        let Err(error) = manager.load_sql_metadata(&workspace_name, None) else {
            panic!("stale artifacts should fail SQL metadata loading");
        };
        let QueryManagerError::App(error) = error else {
            panic!("expected app error for stale artifacts");
        };
        assert!(
            error.to_string().contains("source 'codex' is missing"),
            "unexpected error: {error}"
        );
    }
}
