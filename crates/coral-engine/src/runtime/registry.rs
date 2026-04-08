//! Registers compiled backend sources into a shared `DataFusion` session.

use std::path::Path;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

use crate::backends::{BackendRegistration, CompiledBackendSource, RegisteredSource};
use crate::needles::NeedleState;
use crate::runtime::schema_provider::StaticSchemaProvider;

const RESERVED_SCHEMA_NAMES: &[&str] = &["coral", "coral_admin"];

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
    sources: Vec<Box<dyn CompiledBackendSource>>,
    needles_file: Option<&Path>,
) -> Result<SourceRegistrationResult> {
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("catalog 'datafusion' not found".to_string()))?;

    let mut needles = NeedleState::from_path(needles_file)?;

    let mut result = SourceRegistrationResult::default();
    let mut seen_schemas = std::collections::HashSet::new();

    for source in sources {
        let schema_name = source.schema_name().to_string();
        let source_name = source.source_name().to_string();

        match register_source(ctx, &mut seen_schemas, source.as_ref()).await {
            Ok(registration) => {
                let BackendRegistration {
                    tables,
                    source: registered_source,
                } = needles.decorate(source.schema_name(), registration)?;
                match catalog.register_schema(
                    source.schema_name(),
                    Arc::new(StaticSchemaProvider::new(tables)),
                ) {
                    Ok(_) => result.active_sources.push(registered_source),
                    Err(error) => {
                        if let Some(error) =
                            needles.source_registration_error(source.schema_name(), &error)
                        {
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
            Err(error) => {
                if let Some(error) = needles.source_registration_error(source.schema_name(), &error)
                {
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

    needles.finish()?;

    Ok(result)
}

#[cfg(test)]
pub(crate) fn register_sources_blocking(
    ctx: &SessionContext,
    sources: Vec<Box<dyn CompiledBackendSource>>,
) -> Result<SourceRegistrationResult> {
    futures::executor::block_on(register_sources(ctx, sources, None))
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

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
