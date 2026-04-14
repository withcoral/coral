//! Concrete `DataFusion` runtime assembly for the data plane.

use std::sync::Arc;

use datafusion::common::{Column, SchemaError};
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
        DataFusionError::Plan(detail) if detail.contains("not found") => {
            CoreError::NotFound(detail.clone())
        }
        DataFusionError::Plan(detail) => CoreError::InvalidInput(detail.clone()),
        DataFusionError::SchemaError(schema_error, _) => match schema_error.as_ref() {
            SchemaError::FieldNotFound {
                field,
                valid_fields,
            } => CoreError::InvalidInput(format_field_not_found(field, valid_fields)),
            other => CoreError::InvalidInput(other.to_string()),
        },
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

/// Format a `SchemaError::FieldNotFound` into a concise, hint-bearing
/// `Status` detail. The full `valid_fields` list is deliberately *not*
/// embedded in the message — on wide manifests (e.g. `github.search_issues`
/// with ~561 columns) that list can exceed the HTTP/2 trailer size limit
/// and trigger a `PROTOCOL_ERROR` before the CLI ever sees the status.
/// Callers who want the full set of valid columns can query
/// `coral.columns`.
fn format_field_not_found(field: &Column, valid_fields: &[Column]) -> String {
    let missing = field.name();
    // Match a `__`-flattened counterpart by normalizing away underscores
    // on both sides and requiring the candidate to actually contain `__`
    // (i.e. be a flattened nested path). Catches the common `user_login`
    // → `user__login` typo regardless of which underscore the user
    // wrote as single vs. double.
    let missing_squashed = missing.replace('_', "");
    let nested_hint = valid_fields
        .iter()
        .find(|c| c.name().contains("__") && c.name().replace('_', "") == missing_squashed)
        .map(|c| {
            format!(
                ". Did you mean `{}`? Nested JSON paths are flattened with `__`",
                c.name(),
            )
        })
        .unwrap_or_default();
    format!(
        "No field named `{}` ({} valid fields; query `coral.columns` for the list{})",
        field.quoted_flat_name(),
        valid_fields.len(),
        nested_hint,
    )
}

fn provider_error_to_core(error: &ProviderQueryError) -> CoreError {
    match error {
        ProviderQueryError::MissingRequiredFilter {
            schema,
            table,
            field,
        } => CoreError::FailedPrecondition(format!(
            "{schema}.{table} requires WHERE {field} = <constant>"
        )),
        ProviderQueryError::ApiRequest {
            status,
            detail,
            method,
            url,
            ..
        } => match status {
            Some(429 | 500..=599) => CoreError::Unavailable(format!(
                "{}{}{}",
                detail,
                method
                    .as_ref()
                    .map(|value| format!(" [{value}]"))
                    .unwrap_or_default(),
                url.as_ref()
                    .map(|value| format!(" {value}"))
                    .unwrap_or_default()
            )),
            _ => CoreError::FailedPrecondition(detail.clone()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> Column {
        Column::new_unqualified(name)
    }

    #[test]
    fn format_field_not_found_emits_nested_hint_when_typo_matches_flattened_column() {
        let msg = format_field_not_found(
            &col("user_login"),
            &[col("title"), col("user__login"), col("state")],
        );
        assert!(msg.contains("`user__login`"), "missing hint target: {msg}");
        assert!(
            msg.contains("Nested JSON paths are flattened with `__`"),
            "missing hint explanation: {msg}"
        );
        assert!(msg.contains("3 valid fields"));
    }

    #[test]
    fn format_field_not_found_omits_hint_when_no_flattened_match_exists() {
        let msg = format_field_not_found(&col("bogus"), &[col("title"), col("state")]);
        assert!(!msg.contains("Did you mean"), "unexpected hint: {msg}");
        assert!(msg.contains("2 valid fields"));
    }

    #[test]
    fn datafusion_to_core_unwraps_context_wrapped_schema_error_to_invalid_input() {
        let schema_err = Box::new(SchemaError::FieldNotFound {
            field: Box::new(col("user_login")),
            valid_fields: vec![col("user__login"), col("title")],
        });
        let inner = DataFusionError::SchemaError(schema_err, Box::new(None));
        let wrapped = DataFusionError::Context("wrapping context".to_string(), Box::new(inner));

        let core = datafusion_to_core(&wrapped);

        match core {
            CoreError::InvalidInput(msg) => {
                assert!(
                    msg.contains("`user__login`"),
                    "expected nested-field hint in: {msg}"
                );
            }
            other => panic!("expected CoreError::InvalidInput, got {other:?}"),
        }
    }
}
