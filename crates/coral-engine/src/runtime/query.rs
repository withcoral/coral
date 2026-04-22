//! Concrete `DataFusion` runtime assembly for the data plane.

use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};

use crate::backends::compile_query_source;
use crate::backends::http::ProviderQueryError;
use crate::runtime::catalog;
use crate::runtime::registry::{SourceRegistrationFailure, register_sources};
use crate::{CoreError, QueryExecution, QueryRuntimeProvider, QuerySource, TableInfo};

pub(crate) struct QueryRuntimeAdapter {
    ctx: Arc<SessionContext>,
    tables: Vec<TableInfo>,
    failures: Vec<SourceRegistrationFailure>,
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
            .map_err(|err| datafusion_to_core(&err))?,
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
        .map_err(|err| datafusion_to_core(&err))?;
    catalog::register(&ctx, &registration.active_sources)
        .map_err(|err| datafusion_to_core(&err))?;
    let tables = catalog::collect_tables(&registration.active_sources);
    failures.extend(registration.failures);
    for failure in &failures {
        tracing::warn!(
            source = %failure.schema_name,
            detail = %failure.detail,
            "skipping source during runtime build"
        );
    }

    Ok(QueryRuntimeAdapter {
        ctx,
        tables,
        failures,
    })
}

impl QueryRuntimeAdapter {
    pub(crate) fn list_tables(&self, source_filter: Option<&str>) -> Vec<TableInfo> {
        self.tables
            .iter()
            .filter(|table| source_filter.is_none_or(|value| table.schema_name == value))
            .cloned()
            .collect()
    }

    pub(crate) fn registration_failure(
        &self,
        source_name: &str,
    ) -> Option<&SourceRegistrationFailure> {
        self.failures
            .iter()
            .find(|failure| failure.schema_name == source_name)
    }

    pub(crate) async fn execute_sql(&self, sql: &str) -> Result<QueryExecution, CoreError> {
        let df = self
            .ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(|err| datafusion_to_core(&err))?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await.map_err(|err| datafusion_to_core(&err))?;
        Ok(QueryExecution::new(arrow_schema, batches))
    }
}

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn datafusion_to_core(error: &DataFusionError) -> CoreError {
    // Unwrap Context/Shared/Diagnostic wrappers so wrapped schema errors
    // get classified by their root variant instead of all landing in the
    // `Internal` bucket. Without `find_root()`, `SELECT bogus FROM wide`
    // surfaces as `CoreError::Internal` because DataFusion wraps the
    // SchemaError in `Context`/`Execution`, hiding the structured variant
    // from the match arms below.
    match error.find_root() {
        DataFusionError::SQL(detail, _) => CoreError::InvalidInput(detail.to_string()),
        DataFusionError::Plan(detail) => CoreError::InvalidInput(detail.clone()),
        DataFusionError::SchemaError(schema_error, _) => {
            CoreError::InvalidInput(schema_error.to_string())
        }
        DataFusionError::NotImplemented(detail) => CoreError::Unimplemented(detail.clone()),
        DataFusionError::External(inner) => {
            if let Some(provider_error) = inner.downcast_ref::<ProviderQueryError>() {
                return provider_error_to_core(provider_error);
            }
            CoreError::internal(inner.to_string())
        }
        DataFusionError::ObjectStore(err) => CoreError::Unavailable(err.to_string()),
        DataFusionError::ResourcesExhausted(detail) => CoreError::Unavailable(detail.clone()),
        other => CoreError::internal(other.to_string()),
    }
}

fn provider_error_to_core(error: &ProviderQueryError) -> CoreError {
    CoreError::QueryFailure(Box::new(error.to_structured()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datafusion_to_core_unwraps_context_wrapped_schema_error_to_invalid_input() {
        use datafusion::common::{Column, SchemaError};

        let schema_err = Box::new(SchemaError::FieldNotFound {
            field: Box::new(Column::new_unqualified("user_login")),
            valid_fields: vec![
                Column::new_unqualified("user__login"),
                Column::new_unqualified("title"),
            ],
        });
        let inner = DataFusionError::SchemaError(schema_err, Box::new(None));
        let wrapped = DataFusionError::Context("wrapping context".to_string(), Box::new(inner));

        let core = datafusion_to_core(&wrapped);

        match core {
            CoreError::InvalidInput(msg) => {
                assert!(msg.contains("user_login"), "expected field name in: {msg}");
            }
            other => panic!("expected CoreError::InvalidInput, got {other:?}"),
        }
    }
}
