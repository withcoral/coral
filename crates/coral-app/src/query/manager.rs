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
        let query_source = self
            .load_query_source(&source)
            .map_err(QueryManagerError::App)?;
        let runtime = self.runtime_provider();
        let tables = CoralQuery::test_source(&query_source, &runtime)
            .await
            .map_err(QueryManagerError::Core)?;

        Ok(ValidatedSource { source, tables })
    }

    fn load_query_sources(&self, workspace: &Workspace) -> Result<Vec<QuerySource>, AppError> {
        let mut query_sources = Vec::new();
        for source in self.config_store.list_workspace_sources(workspace)? {
            match self.load_query_source(&source) {
                Ok(query_source) => query_sources.push(query_source),
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

    fn load_query_source(&self, source: &ManagedSource) -> Result<QuerySource, AppError> {
        let manifest_path = self.layout.manifest_file(&source.workspace, &source.name);
        if !manifest_path.exists() {
            return Err(AppError::SourceNotFound(source.name.clone()));
        }
        let source_spec = coral_spec::load_manifest_path(&manifest_path)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        if source_spec.schema_name() != source.name {
            return Err(AppError::FailedPrecondition(format!(
                "installed source '{}' does not match manifest name '{}'",
                source.name,
                source_spec.schema_name()
            )));
        }
        if source_spec.source_version() != source.version {
            return Err(AppError::FailedPrecondition(format!(
                "installed source '{}' version '{}' does not match manifest version '{}'",
                source.name,
                source.version,
                source_spec.source_version()
            )));
        }
        let stored_secrets = self
            .secret_store
            .read_source_secrets_for(&source.workspace, &source.name)?;
        let mut resolved_secrets = BTreeMap::new();
        for secret_name in source_spec.required_secret_names() {
            let Some(value) = stored_secrets.get(&secret_name).cloned() else {
                return Err(AppError::FailedPrecondition(format!(
                    "source '{}' is missing secret '{}'",
                    source.name, secret_name
                )));
            };
            resolved_secrets.insert(secret_name, value);
        }
        Ok(QuerySource::new(
            source_spec,
            source.variables.clone(),
            resolved_secrets,
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
