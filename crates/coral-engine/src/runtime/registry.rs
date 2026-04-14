//! Registers compiled backend sources into a shared `DataFusion` session.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

use crate::backends::{BackendRegistration, CompiledBackendSource, RegisteredSource};
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{QuerySource, SourceDecorator, SourceDecoratorError, SourceFailurePolicy};

const RESERVED_SCHEMA_NAMES: &[&str] = &["coral", "coral_admin"];

pub(crate) struct SelectedCompiledSource {
    pub(crate) source: QuerySource,
    pub(crate) compiled: Box<dyn CompiledBackendSource>,
}

/// Captures one source manifest that failed to initialize during registration.
#[derive(Debug, Clone)]
pub(crate) struct SourceRegistrationFailure {
    /// Schema name whose registration failed.
    pub schema_name: String,
    /// Human-readable failure detail.
    pub detail: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SourceRegistrationResult {
    pub(crate) active_sources: Vec<RegisteredSource>,
    pub(crate) failures: Vec<SourceRegistrationFailure>,
}

fn check_reserved_schema(schema: &str) -> Result<()> {
    if RESERVED_SCHEMA_NAMES.contains(&schema) {
        return Err(DataFusionError::Execution(format!(
            "source schema '{schema}' is reserved and cannot be used by manifests"
        )));
    }
    Ok(())
}

/// Register all configured source manifests into the active `SessionContext`.
///
/// # Errors
///
/// Returns a `DataFusionError` if the catalog is missing or if the source list
/// itself cannot be processed. Individual source registration failures are
/// logged and skipped so the remaining sources can still be registered.
pub(crate) async fn register_sources(
    ctx: &SessionContext,
    sources: Vec<SelectedCompiledSource>,
    source_decorators: &mut [Box<dyn SourceDecorator>],
) -> Result<SourceRegistrationResult> {
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("catalog 'datafusion' not found".to_string()))?;

    let selected_sources = sources
        .iter()
        .map(|selected| selected.source.clone())
        .collect::<Vec<_>>();
    prepare_source_decorators(source_decorators, &selected_sources)?;

    let mut result = SourceRegistrationResult::default();
    let mut seen_schemas = std::collections::HashSet::new();

    for selected_source in sources {
        let query_source = &selected_source.source;
        let compiled_source = selected_source.compiled;
        let schema_name = compiled_source.schema_name().to_string();
        let source_name = compiled_source.source_name().to_string();

        match register_source(ctx, &mut seen_schemas, compiled_source.as_ref()).await {
            Ok(registration) => {
                let BackendRegistration {
                    tables,
                    source: registered_source,
                } = registration;
                let decorated_tables =
                    decorate_source_tables(source_decorators, query_source, tables)?;
                match catalog.register_schema(
                    compiled_source.schema_name(),
                    Arc::new(StaticSchemaProvider::new(decorated_tables)),
                ) {
                    Ok(_) => result.active_sources.push(registered_source),
                    Err(error) => {
                        tracing::warn!(source = %source_name, error = %error, "skipping source");
                        result.failures.push(SourceRegistrationFailure {
                            schema_name,
                            detail: error.to_string(),
                        });
                    }
                }
            }
            Err(error) => {
                if handle_source_registration_failure(source_decorators, query_source, &error)? {
                    return Err(error);
                }
                tracing::warn!(source = %source_name, error = %error, "skipping source");
                result.failures.push(SourceRegistrationFailure {
                    schema_name,
                    detail: error.to_string(),
                });
            }
        }
    }

    finish_source_decorators(source_decorators)?;

    Ok(result)
}

#[cfg(test)]
pub(crate) fn register_sources_blocking(
    ctx: &SessionContext,
    sources: Vec<SelectedCompiledSource>,
) -> Result<SourceRegistrationResult> {
    let mut source_decorators: Vec<Box<dyn SourceDecorator>> = Vec::new();
    futures::executor::block_on(register_sources(
        ctx,
        sources,
        source_decorators.as_mut_slice(),
    ))
}

async fn register_source(
    ctx: &SessionContext,
    seen_schemas: &mut std::collections::HashSet<String>,
    source: &dyn CompiledBackendSource,
) -> Result<BackendRegistration> {
    check_reserved_schema(source.schema_name())?;

    if !seen_schemas.insert(source.schema_name().to_string()) {
        return Err(DataFusionError::Execution(format!(
            "duplicate source schema '{}'",
            source.schema_name()
        )));
    }

    source.register(ctx).await
}

fn prepare_source_decorators(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    selected_sources: &[QuerySource],
) -> Result<()> {
    for decorator in source_decorators {
        decorator
            .prepare(selected_sources)
            .map_err(|error| source_decorator_error(decorator.name(), error))?;
    }
    Ok(())
}

fn decorate_source_tables(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    source: &QuerySource,
    mut tables: crate::SourceTables,
) -> Result<crate::SourceTables> {
    for decorator in source_decorators {
        tables = decorator
            .decorate_source(source, tables)
            .map_err(|error| source_decorator_error(decorator.name(), error))?;
    }
    Ok(tables)
}

fn handle_source_registration_failure(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    source: &QuerySource,
    error: &DataFusionError,
) -> Result<bool> {
    for decorator in source_decorators {
        let policy = decorator
            .source_failed(source, error)
            .map_err(|decorator_error| source_decorator_error(decorator.name(), decorator_error))?;
        if policy == SourceFailurePolicy::Abort {
            return Ok(true);
        }
    }
    Ok(false)
}

fn finish_source_decorators(source_decorators: &mut [Box<dyn SourceDecorator>]) -> Result<()> {
    for decorator in source_decorators {
        decorator
            .finish()
            .map_err(|error| source_decorator_error(decorator.name(), error))?;
    }
    Ok(())
}

fn source_decorator_error(name: &str, error: SourceDecoratorError) -> DataFusionError {
    match error {
        SourceDecoratorError::InvalidInput(detail) => {
            DataFusionError::Plan(format!("source decorator '{name}': {detail}"))
        }
        SourceDecoratorError::FailedPrecondition(detail) => {
            DataFusionError::External(Box::new(SourceDecoratorError::FailedPrecondition(format!(
                "source decorator '{name}': {detail}"
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::check_reserved_schema;

    #[test]
    fn reserved_schema_coral_is_rejected() {
        let result = check_reserved_schema("coral");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("coral"),
            "error message should mention the schema name"
        );
    }

    #[test]
    fn non_reserved_schema_is_accepted() {
        assert!(check_reserved_schema("github").is_ok());
        assert!(check_reserved_schema("pagerduty").is_ok());
        assert!(check_reserved_schema("slack").is_ok());
    }
}
