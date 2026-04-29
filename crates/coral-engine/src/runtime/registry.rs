//! Registers compiled backend sources into a shared `DataFusion` session.

use std::collections::HashMap;
use std::sync::Arc;

use coral_spec::DEFAULT_NAMESPACE;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::SessionContext;

use crate::backends::{BackendRegistration, CompiledBackendSource, RegisteredSource};
use crate::runtime::catalog_provider::StaticCatalogProvider;
use crate::runtime::error::{datafusion_to_core, source_decorator_error_to_core};
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{CoreError, QuerySource, SourceDecorator, SourceFailurePolicy};

const RESERVED_SOURCE_NAMES: &[&str] = &["coral", "coral_admin", "datafusion"];

/// One selected query source together with its compiled backend artifact.
///
/// The registry needs both values at once: the compiled backend source drives
/// registration, while the original `QuerySource` is what source decorators
/// reason about during prepare, decoration, and failure handling.
pub(crate) struct CompiledQuerySource {
    pub(crate) source: QuerySource,
    pub(crate) compiled: Box<dyn CompiledBackendSource>,
}

/// One selected source's readiness for runtime registration.
pub(crate) enum SourceRegistrationCandidate {
    Compiled(CompiledQuerySource),
    CompileFailed {
        source: QuerySource,
        error: CoreError,
    },
}

impl SourceRegistrationCandidate {
    fn source(&self) -> &QuerySource {
        match self {
            Self::Compiled(compiled) => &compiled.source,
            Self::CompileFailed { source, .. } => source,
        }
    }
}

/// Captures one source manifest that failed to initialize during registration.
#[derive(Debug, Clone)]
pub(crate) struct SourceRegistrationFailure {
    /// Source/schema name whose registration failed.
    pub schema_name: String,
    /// Human-readable failure detail.
    pub detail: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SourceRegistrationResult {
    pub(crate) active_sources: Vec<RegisteredSource>,
    pub(crate) failures: Vec<SourceRegistrationFailure>,
}

fn check_reserved_source_name(source_name: &str) -> DataFusionResult<()> {
    if RESERVED_SOURCE_NAMES.contains(&source_name) {
        return Err(DataFusionError::Execution(format!(
            "source name '{source_name}' is reserved and cannot be used by manifests"
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
    sources: Vec<SourceRegistrationCandidate>,
    source_decorators: &mut [Box<dyn SourceDecorator>],
) -> std::result::Result<SourceRegistrationResult, CoreError> {
    let catalog = ctx.catalog("datafusion").ok_or_else(|| {
        let plan_err = DataFusionError::Plan("catalog 'datafusion' not found".to_string());
        datafusion_to_core(&plan_err, &[])
    })?;

    let selected_sources = sources
        .iter()
        .map(|selected| selected.source().clone())
        .collect::<Vec<_>>();
    prepare_source_decorators(source_decorators, &selected_sources)?;

    let mut result = SourceRegistrationResult::default();
    let mut seen_schemas = std::collections::HashSet::new();

    for source in sources {
        match source {
            SourceRegistrationCandidate::Compiled(selected_source) => {
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
                        match register_runtime_tables(
                            ctx,
                            catalog.as_ref(),
                            compiled_source.schema_name(),
                            &registered_source,
                            &decorated_tables,
                        ) {
                            Ok(()) => result.active_sources.push(registered_source),
                            Err(error) => {
                                let core_error = datafusion_to_core(&error, &[]);
                                if record_source_failure(
                                    &mut result,
                                    source_decorators,
                                    query_source,
                                    &schema_name,
                                    &source_name,
                                    &core_error,
                                )? {
                                    return Err(core_error);
                                }
                            }
                        }
                    }
                    Err(error) => {
                        let core_error = datafusion_to_core(&error, &[]);
                        if record_source_failure(
                            &mut result,
                            source_decorators,
                            query_source,
                            &schema_name,
                            &source_name,
                            &core_error,
                        )? {
                            return Err(core_error);
                        }
                    }
                }
            }
            SourceRegistrationCandidate::CompileFailed { source, error } => {
                let source_name = source.source_name().to_string();
                if record_source_failure(
                    &mut result,
                    source_decorators,
                    &source,
                    &source_name,
                    &source_name,
                    &error,
                )? {
                    return Err(error);
                }
            }
        }
    }

    finish_source_decorators(source_decorators)?;

    Ok(result)
}

#[cfg(test)]
pub(crate) fn register_sources_blocking(
    ctx: &SessionContext,
    sources: Vec<CompiledQuerySource>,
) -> std::result::Result<SourceRegistrationResult, CoreError> {
    let mut source_decorators: Vec<Box<dyn SourceDecorator>> = Vec::new();
    futures::executor::block_on(register_sources(
        ctx,
        sources
            .into_iter()
            .map(SourceRegistrationCandidate::Compiled)
            .collect(),
        source_decorators.as_mut_slice(),
    ))
}

async fn register_source(
    ctx: &SessionContext,
    seen_schemas: &mut std::collections::HashSet<String>,
    source: &dyn CompiledBackendSource,
) -> DataFusionResult<BackendRegistration> {
    check_reserved_source_name(source.schema_name())?;

    if !seen_schemas.insert(source.schema_name().to_string()) {
        return Err(DataFusionError::Execution(format!(
            "duplicate source schema '{}'",
            source.schema_name()
        )));
    }

    source.register(ctx).await
}

fn register_runtime_tables(
    ctx: &SessionContext,
    default_catalog: &dyn CatalogProvider,
    schema_name: &str,
    registered_source: &RegisteredSource,
    tables: &HashMap<String, Arc<dyn TableProvider>>,
) -> DataFusionResult<()> {
    ensure_runtime_path_available(ctx, default_catalog, schema_name)?;

    let providers = align_table_providers(schema_name, registered_source, tables)?;
    let source_schemas = source_catalog_schemas(&providers);
    let core_aliases = core_alias_tables(&providers);

    register_core_aliases(default_catalog, schema_name, core_aliases)?;

    if ctx
        .register_catalog(
            schema_name.to_string(),
            Arc::new(StaticCatalogProvider::new(source_schemas)),
        )
        .is_some()
    {
        rollback_core_aliases(default_catalog, schema_name);
        return Err(DataFusionError::Execution(format!(
            "duplicate source catalog '{schema_name}'"
        )));
    }

    Ok(())
}

fn ensure_runtime_path_available(
    ctx: &SessionContext,
    default_catalog: &dyn CatalogProvider,
    schema_name: &str,
) -> DataFusionResult<()> {
    if ctx.catalog(schema_name).is_some() {
        return Err(DataFusionError::Execution(format!(
            "duplicate source catalog '{schema_name}'"
        )));
    }
    if default_catalog.schema(schema_name).is_some() {
        return Err(DataFusionError::Execution(format!(
            "duplicate source schema '{schema_name}'"
        )));
    }
    Ok(())
}

struct RuntimeTableProvider {
    namespace: String,
    table_name: String,
    provider: Arc<dyn TableProvider>,
}

fn align_table_providers(
    schema_name: &str,
    registered_source: &RegisteredSource,
    tables: &HashMap<String, Arc<dyn TableProvider>>,
) -> DataFusionResult<Vec<RuntimeTableProvider>> {
    // Source decorators still receive the flat provider map because v1 source
    // specs require table names to be unique per source. This is the boundary
    // where runtime registration restores the logical namespace for DataFusion.
    registered_source
        .tables
        .iter()
        .map(|table| {
            let provider = tables.get(&table.table_name).cloned().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{schema_name}.{} has metadata but no registered table provider",
                    table.table_name
                ))
            })?;
            Ok(RuntimeTableProvider {
                namespace: table.namespace.clone(),
                table_name: table.table_name.clone(),
                provider,
            })
        })
        .collect()
}

fn source_catalog_schemas(
    providers: &[RuntimeTableProvider],
) -> HashMap<String, Arc<dyn SchemaProvider>> {
    let mut by_namespace: HashMap<String, HashMap<String, Arc<dyn TableProvider>>> = HashMap::new();
    for table in providers {
        by_namespace
            .entry(table.namespace.clone())
            .or_default()
            .insert(table.table_name.clone(), table.provider.clone());
    }
    by_namespace
        .into_iter()
        .map(|(namespace, namespace_tables)| {
            (
                namespace,
                Arc::new(StaticSchemaProvider::new(namespace_tables)) as Arc<dyn SchemaProvider>,
            )
        })
        .collect()
}

fn core_alias_tables(
    providers: &[RuntimeTableProvider],
) -> HashMap<String, Arc<dyn TableProvider>> {
    providers
        .iter()
        .filter(|table| table.namespace == DEFAULT_NAMESPACE)
        .map(|table| (table.table_name.clone(), table.provider.clone()))
        .collect()
}

fn register_core_aliases(
    default_catalog: &dyn CatalogProvider,
    schema_name: &str,
    core_aliases: HashMap<String, Arc<dyn TableProvider>>,
) -> DataFusionResult<()> {
    if core_aliases.is_empty() {
        return Ok(());
    }
    default_catalog.register_schema(
        schema_name,
        Arc::new(StaticSchemaProvider::new(core_aliases)),
    )?;
    Ok(())
}

fn rollback_core_aliases(default_catalog: &dyn CatalogProvider, schema_name: &str) {
    if let Err(error) = default_catalog.deregister_schema(schema_name, true) {
        tracing::warn!(
            schema_name,
            detail = %error,
            "failed to roll back core table aliases after source catalog registration failure"
        );
    }
}

fn prepare_source_decorators(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    selected_sources: &[QuerySource],
) -> std::result::Result<(), CoreError> {
    for decorator in source_decorators {
        decorator
            .prepare(selected_sources)
            .map_err(|error| source_decorator_error(decorator.name(), &error))?;
    }
    Ok(())
}

fn decorate_source_tables(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    source: &QuerySource,
    mut tables: crate::SourceTables,
) -> std::result::Result<crate::SourceTables, CoreError> {
    for decorator in source_decorators {
        tables = decorator
            .decorate_source(source, tables)
            .map_err(|error| source_decorator_error(decorator.name(), &error))?;
    }
    Ok(tables)
}

fn handle_source_registration_failure(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    source: &QuerySource,
    error: &CoreError,
) -> std::result::Result<bool, CoreError> {
    for decorator in source_decorators {
        let policy = decorator
            .source_failed(source, error)
            .map_err(|decorator_error| {
                source_decorator_error(decorator.name(), &decorator_error)
            })?;
        if policy == SourceFailurePolicy::Abort {
            return Ok(true);
        }
    }
    Ok(false)
}

fn record_source_failure(
    result: &mut SourceRegistrationResult,
    source_decorators: &mut [Box<dyn SourceDecorator>],
    source: &QuerySource,
    schema_name: &str,
    source_name: &str,
    error: &CoreError,
) -> std::result::Result<bool, CoreError> {
    if handle_source_registration_failure(source_decorators, source, error)? {
        return Ok(true);
    }
    let failure = SourceRegistrationFailure {
        schema_name: schema_name.to_string(),
        detail: error.to_string(),
    };
    tracing::warn!(
        source = source_name,
        schema_name = %failure.schema_name,
        detail = %failure.detail,
        "skipping source"
    );
    result.failures.push(failure);
    Ok(false)
}

fn finish_source_decorators(
    source_decorators: &mut [Box<dyn SourceDecorator>],
) -> std::result::Result<(), CoreError> {
    for decorator in source_decorators {
        decorator
            .finish()
            .map_err(|error| source_decorator_error(decorator.name(), &error))?;
    }
    Ok(())
}

fn source_decorator_error(name: &str, error: &crate::SourceDecoratorError) -> CoreError {
    let core = source_decorator_error_to_core(error);
    match core {
        CoreError::InvalidInput(detail) => {
            CoreError::InvalidInput(format!("source decorator '{name}': {detail}"))
        }
        CoreError::FailedPrecondition(detail) => {
            CoreError::FailedPrecondition(format!("source decorator '{name}': {detail}"))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::check_reserved_source_name;

    #[test]
    fn reserved_source_name_coral_is_rejected() {
        let result = check_reserved_source_name("coral");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("coral"),
            "error message should mention the source name"
        );
    }

    #[test]
    fn non_reserved_source_name_is_accepted() {
        assert!(check_reserved_source_name("github").is_ok());
        assert!(check_reserved_source_name("pagerduty").is_ok());
        assert!(check_reserved_source_name("slack").is_ok());
    }
}
