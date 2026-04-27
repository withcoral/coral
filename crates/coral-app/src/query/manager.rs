//! Query-time loading, validation, and execution over installed sources.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use coral_engine::{
    CoralQuery, CoreError, EngineExtensions, QueryExecution, QueryRuntimeContext,
    QueryRuntimeProvider, QuerySource, SourceValidationReport, TableInfo,
};
use coral_spec::{ManifestInputKind, ManifestInputSpec, parse_source_manifest_yaml};

use crate::bootstrap::AppError;
use crate::query::extensions::{EngineExtensionsProvider, engine_extensions_for_providers};
use crate::sources::SourceName;
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::model::InstalledSource;
use crate::state::{AppStateLayout, ConfigStore, SecretStore};
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
    secret_store: SecretStore,
    runtime_context: QueryRuntimeContext,
    layout: AppStateLayout,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
}

impl QueryManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        secret_store: SecretStore,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> Self {
        Self {
            config_store,
            secret_store,
            runtime_context,
            layout,
            engine_extensions_providers,
        }
    }

    pub(crate) async fn list_tables(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<TableInfo>, QueryManagerError> {
        let sources = self
            .load_query_sources(workspace_name)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_provider(&sources);
        CoralQuery::list_tables(&sources, &runtime, None)
            .await
            .map_err(QueryManagerError::Core)
    }

    pub(crate) async fn execute_sql(
        &self,
        workspace_name: &WorkspaceName,
        sql: &str,
    ) -> Result<QueryExecution, QueryManagerError> {
        let sources = self
            .load_query_sources(workspace_name)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_provider(&sources);
        CoralQuery::execute_sql(&sources, &runtime, sql)
            .await
            .map_err(QueryManagerError::Core)
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
        let runtime = self.runtime_provider(std::slice::from_ref(&query_source));
        let report = CoralQuery::validate_source(
            &query_source,
            &runtime,
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
        let manifest_yaml = installed.manifest_yaml;
        let source_spec = parse_source_manifest_yaml(&manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        validate_required_variables(source, source_spec.declared_inputs())?;
        let stored_secrets = self
            .secret_store
            .read_source_secrets_for(workspace_name, &source.name)?;
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
            let value = stored_secrets[&secret_name].clone();
            resolved_secrets.insert(secret_name, value);
        }
        Ok((
            QuerySource::new(source_spec, source.variables.clone(), resolved_secrets),
            installed.candidate.version,
        ))
    }

    fn runtime_provider(&self, sources: &[QuerySource]) -> RuntimeProvider {
        RuntimeProvider {
            runtime_context: self.runtime_context.clone(),
            engine_extensions: Mutex::new(Some(engine_extensions_for_providers(
                &self.engine_extensions_providers,
                sources,
            ))),
        }
    }
}

struct RuntimeProvider {
    runtime_context: QueryRuntimeContext,
    engine_extensions: Mutex<Option<EngineExtensions>>,
}

impl QueryRuntimeProvider for RuntimeProvider {
    fn runtime_context(&self) -> QueryRuntimeContext {
        self.runtime_context.clone()
    }

    fn engine_extensions(&self) -> EngineExtensions {
        self.engine_extensions
            .lock()
            .expect("engine extensions mutex poisoned")
            .take()
            .unwrap_or_default()
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
