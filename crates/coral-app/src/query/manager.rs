//! Query-time loading, validation, and execution over installed sources.

use std::collections::{BTreeMap, BTreeSet};

use coral_api::v1::Workspace;
use coral_engine::{
    CoralQuery, CoreError, QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource,
    TableInfo,
};
use coral_spec::parse_source_manifest_yaml;

use crate::bootstrap::AppError;
use crate::sources::model::ManagedSource;
use crate::state::{AppStateLayout, ConfigStore, SecretStore};

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
        let manifest_yaml = std::fs::read_to_string(&manifest_path)?;
        let source_spec = parse_source_manifest_yaml(&manifest_yaml)
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
        Ok(QuerySource::new(
            source.workspace.name.clone(),
            source_spec,
            source.variables.clone(),
        ))
    }

    fn runtime_provider(&self) -> RuntimeProvider {
        RuntimeProvider {
            config_store: self.config_store.clone(),
            secret_store: self.secret_store.clone(),
            runtime_context: self.runtime_context.clone(),
        }
    }
}

#[derive(Clone)]
struct RuntimeProvider {
    config_store: ConfigStore,
    secret_store: SecretStore,
    runtime_context: QueryRuntimeContext,
}

impl QueryRuntimeProvider for RuntimeProvider {
    fn resolve_source_secrets(
        &self,
        query_source: &QuerySource,
        secret_names: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, String>, CoreError> {
        let source = self
            .config_store
            .get_source(
                &Workspace {
                    name: query_source.workspace_name().to_string(),
                },
                query_source.source_name(),
            )
            .map_err(app_error_to_core)?;
        let values = self
            .secret_store
            .read_source_secrets_for(&source.workspace, &source.name)
            .map_err(app_error_to_core)?;
        let mut entries = BTreeMap::new();
        for key in secret_names {
            let Some(value) = values.get(key).cloned() else {
                return Err(CoreError::FailedPrecondition(format!(
                    "source '{}' is missing secret '{}'",
                    query_source.source_name(),
                    key
                )));
            };
            entries.insert(key.clone(), value);
        }
        Ok(entries)
    }

    fn runtime_context(&self) -> QueryRuntimeContext {
        self.runtime_context.clone()
    }
}

fn app_error_to_core(error: AppError) -> CoreError {
    match error {
        AppError::SourceNotFound(source_name) => {
            CoreError::NotFound(format!("source '{source_name}'"))
        }
        AppError::InvalidInput(detail) => CoreError::InvalidInput(detail),
        AppError::FailedPrecondition(detail) => CoreError::FailedPrecondition(detail),
        AppError::Io(inner) => CoreError::internal(inner.to_string()),
        AppError::Yaml(inner) => CoreError::internal(inner.to_string()),
        AppError::TomlDecode(inner) => CoreError::internal(inner.to_string()),
        AppError::TomlEncode(inner) => CoreError::internal(inner.to_string()),
        AppError::Json(inner) => CoreError::internal(inner.to_string()),
        AppError::Transport(inner) => CoreError::internal(inner.to_string()),
        AppError::TaskJoin(inner) => CoreError::internal(inner.to_string()),
        AppError::Credentials(crate::state::CredentialsError::Parse(detail)) => {
            CoreError::FailedPrecondition(detail)
        }
        AppError::Credentials(inner) => CoreError::internal(inner.to_string()),
        AppError::MissingConfigDir => {
            CoreError::FailedPrecondition("failed to determine Coral config directory".into())
        }
    }
}
