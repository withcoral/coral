//! Registers compiled backend sources into a shared `DataFusion` session.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

use crate::backends::{CompiledBackendSource, RegisteredSource};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceRegistrationMode {
    BestEffort,
    Strict,
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
    mode: SourceRegistrationMode,
) -> Result<SourceRegistrationResult> {
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("catalog 'datafusion' not found".to_string()))?;

    let mut result = SourceRegistrationResult::default();
    let mut seen_schemas = std::collections::HashSet::new();

    for source in sources {
        let schema_name = source.schema_name().to_string();
        let source_name = source.source_name().to_string();

        match register_source(ctx, &catalog, &mut seen_schemas, source.as_ref()).await {
            Ok(active_source) => result.active_sources.push(active_source),
            Err(error) => {
                if mode == SourceRegistrationMode::Strict {
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

    Ok(result)
}

#[cfg(test)]
pub(crate) fn register_sources_blocking(
    ctx: &SessionContext,
    sources: Vec<Box<dyn CompiledBackendSource>>,
) -> Result<SourceRegistrationResult> {
    futures::executor::block_on(register_sources(
        ctx,
        sources,
        SourceRegistrationMode::BestEffort,
    ))
}

async fn register_source(
    ctx: &SessionContext,
    catalog: &Arc<dyn datafusion::catalog::CatalogProvider>,
    seen_schemas: &mut std::collections::HashSet<String>,
    source: &dyn CompiledBackendSource,
) -> Result<RegisteredSource> {
    check_reserved_schema(source.schema_name())?;

    if !seen_schemas.insert(source.schema_name().to_string()) {
        return Err(DataFusionError::Execution(format!(
            "duplicate source schema '{}'",
            source.schema_name()
        )));
    }

    let registration = source.register(ctx).await?;
    catalog.register_schema(
        source.schema_name(),
        Arc::new(StaticSchemaProvider::new(registration.tables)),
    )?;

    Ok(registration.source)
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
