//! Concrete `DataFusion` runtime assembly for the data plane.

use std::sync::Arc;

use datafusion::common::SchemaError;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};

use crate::backends::compile_query_source;
use crate::backends::http::ProviderQueryError;
use crate::runtime::catalog;
use crate::runtime::registry::{SourceRegistrationFailure, register_sources};
use crate::{CoreError, QueryError, QueryExecution, QueryRuntimeProvider, QuerySource, TableInfo};

pub(crate) struct QueryRuntimeAdapter {
    ctx: Arc<SessionContext>,
    tables: Vec<TableInfo>,
}

pub(crate) async fn build_runtime(
    sources: &[QuerySource],
    runtime: &dyn QueryRuntimeProvider,
) -> Result<QueryRuntimeAdapter, CoreError> {
    let session_config = SessionConfig::new().with_information_schema(true);
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_object_list_cache_limit(0)
            .build()
            .map_err(datafusion_to_core)?,
    );
    let ctx = Arc::new(SessionContext::new_with_config_rt(
        session_config,
        runtime_env,
    ));

    let runtime_context = runtime.runtime_context();
    let mut compiled_sources = Vec::new();
    let mut failures = Vec::new();
    for source in sources {
        match compile_query_source(source, &runtime_context) {
            Ok(compiled) => compiled_sources.push(compiled),
            Err(error) => failures.push(SourceRegistrationFailure {
                schema_name: source.source_name().to_string(),
                detail: error.to_string(),
            }),
        }
    }
    let registration = register_sources(&ctx, compiled_sources)
        .await
        .map_err(datafusion_to_core)?;
    catalog::register(&ctx, &registration.active_sources).map_err(datafusion_to_core)?;
    let tables = catalog::collect_tables(&registration.active_sources);
    for failure in &failures {
        tracing::warn!(
            source = %failure.schema_name,
            detail = %failure.detail,
            "skipping source during runtime build"
        );
    }

    Ok(QueryRuntimeAdapter { ctx, tables })
}

impl QueryRuntimeAdapter {
    pub(crate) fn list_tables(&self, source_filter: Option<&str>) -> Vec<TableInfo> {
        self.tables
            .iter()
            .filter(|table| source_filter.is_none_or(|value| table.schema_name == value))
            .cloned()
            .collect()
    }

    pub(crate) async fn execute_sql(&self, sql: &str) -> Result<QueryExecution, CoreError> {
        let df = self
            .ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(datafusion_to_core)?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await.map_err(datafusion_to_core)?;
        Ok(QueryExecution::new(arrow_schema, batches))
    }
}

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn datafusion_to_core(error: DataFusionError) -> CoreError {
    match error {
        DataFusionError::SQL(detail, _) => CoreError::InvalidInput(detail.to_string()),
        DataFusionError::Plan(detail) => CoreError::InvalidInput(detail),
        DataFusionError::SchemaError(schema_error, _) => match schema_error.as_ref() {
            SchemaError::FieldNotFound { field, .. } => CoreError::NotFound(field.to_string()),
            _ => CoreError::InvalidInput(schema_error.to_string()),
        },
        DataFusionError::NotImplemented(detail) => CoreError::Unimplemented(detail),
        DataFusionError::External(inner) => {
            if let Some(provider_error) = inner.downcast_ref::<ProviderQueryError>() {
                return provider_error_to_core(provider_error);
            }
            CoreError::internal(inner.to_string())
        }
        DataFusionError::ObjectStore(error) => CoreError::Unavailable(error.to_string()),
        DataFusionError::ResourcesExhausted(detail) => CoreError::Unavailable(detail),
        other => CoreError::internal(other.to_string()),
    }
}

fn provider_error_to_core(error: &ProviderQueryError) -> CoreError {
    CoreError::Structured(Box::new(provider_error_to_query_error(error)))
}

fn provider_error_to_query_error(error: &ProviderQueryError) -> QueryError {
    match error {
        ProviderQueryError::MissingRequiredFilter {
            schema,
            table,
            field,
        } => QueryError::missing_required_filter(
            schema.clone(),
            table.clone(),
            field.clone(),
            format!("{schema}.{table} requires a constant equality filter on {field}"),
        ),
        ProviderQueryError::ApiRequest {
            source_schema,
            table,
            status,
            method,
            url,
            detail,
        } => QueryError::provider_request(
            source_schema.clone(),
            table.clone(),
            *status,
            method.clone(),
            url.clone(),
            detail.clone(),
        ),
    }
}
