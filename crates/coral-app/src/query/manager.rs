//! Query-time loading, validation, and execution over installed sources.

use std::collections::BTreeMap;

use crate::bootstrap::AppError;
use crate::sources::model::ManagedSource;
use crate::state::{AppStateLayout, ConfigStore, SecretStore};
use coral_api::v1::Workspace;
use coral_engine::{
    CoralQuery, CoreError, QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource,
    TableInfo,
};
use coral_spec::{ManifestInputKind, collect_source_inputs_yaml, parse_source_manifest_yaml};

use crate::sources::catalog::resolve_installed_manifest;

#[derive(Debug)]
pub(crate) enum QueryManagerError {
    App(AppError),
    Core(CoreError),
}

pub(crate) struct ValidatedSource {
    pub(crate) source: ManagedSource,
    pub(crate) tables: Vec<TableInfo>,
}

#[derive(Clone)]
pub(crate) struct QueryManager {
    config_store: ConfigStore,
    secret_store: SecretStore,
    runtime_context: QueryRuntimeContext,
    layout: AppStateLayout,
}

impl QueryManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        secret_store: SecretStore,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
    ) -> Self {
        Self {
            config_store,
            secret_store,
            runtime_context,
            layout,
        }
    }

    pub(crate) async fn list_tables(
        &self,
        workspace: &Workspace,
    ) -> Result<Vec<TableInfo>, QueryManagerError> {
        let sources = self
            .load_query_sources(workspace)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_provider();
        CoralQuery::list_tables(&sources, &runtime, None)
            .await
            .map_err(QueryManagerError::Core)
    }

    pub(crate) async fn execute_sql(
        &self,
        workspace: &Workspace,
        sql: &str,
    ) -> Result<QueryExecution, QueryManagerError> {
        let sources = self
            .load_query_sources(workspace)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_provider();
        CoralQuery::execute_sql(&sources, &runtime, sql)
            .await
            .map_err(QueryManagerError::Core)
    }

    pub(crate) async fn validate_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<ValidatedSource, QueryManagerError> {
        let source = self
            .config_store
            .get_source(workspace, source_name)
            .map_err(QueryManagerError::App)?;
        let (query_source, version) = self
            .load_query_source(&source)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_provider();
        let tables = CoralQuery::test_source(&query_source, &runtime)
            .await
            .map_err(QueryManagerError::Core)?;
        let mut source = source;
        source.version = version;

        Ok(ValidatedSource { source, tables })
    }

    fn load_query_sources(&self, workspace: &Workspace) -> Result<Vec<QuerySource>, AppError> {
        let mut query_sources = Vec::new();
        for source in self.config_store.list_workspace_sources(workspace)? {
            match self.load_query_source(&source) {
                Ok((query_source, _version)) => query_sources.push(query_source),
                Err(error @ AppError::FailedPrecondition(_)) => return Err(error),
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

    fn load_query_source(&self, source: &ManagedSource) -> Result<(QuerySource, String), AppError> {
        let installed = resolve_installed_manifest(source, &self.layout)?;
        let manifest_yaml = installed.manifest_yaml;
        let source_spec = parse_source_manifest_yaml(&manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        validate_required_variables(source, &manifest_yaml)?;
        let stored_secrets = self
            .secret_store
            .read_source_secrets_for(&source.workspace, &source.name)?;
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
                "source '{}' is missing {detail} — re-run `coral source add {}` to update",
                source.name, source.name
            )));
        }
        for secret_name in source_spec.required_secret_names() {
            let value = stored_secrets[&secret_name].clone();
            resolved_secrets.insert(secret_name, value);
        }
        Ok((
            QuerySource::new(source_spec, source.variables.clone(), resolved_secrets),
            installed.available.version,
        ))
    }

    fn runtime_provider(&self) -> RuntimeProvider {
        RuntimeProvider {
            runtime_context: self.runtime_context.clone(),
        }
    }
}

#[derive(Clone)]
struct RuntimeProvider {
    runtime_context: QueryRuntimeContext,
}

impl QueryRuntimeProvider for RuntimeProvider {
    fn runtime_context(&self) -> QueryRuntimeContext {
        self.runtime_context.clone()
    }
}

fn validate_required_variables(
    source: &ManagedSource,
    manifest_yaml: &str,
) -> Result<(), AppError> {
    let inputs = collect_source_inputs_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    let missing: Vec<_> = inputs
        .into_iter()
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
            "source '{}' is missing {detail} — re-run `coral source add {}` to update",
            source.name, source.name
        )));
    }
    Ok(())
}
