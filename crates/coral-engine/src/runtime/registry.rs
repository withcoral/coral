//! Registers compiled backend sources into a shared `DataFusion` session.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::SessionContext;
use tracing::{Instrument as _, info_span};

use crate::backends::{
    BackendCatalogRegistration, BackendRegistration, BackendRegistrationContext,
    BackendSchemaRegistration, CompiledBackendSource, RegisteredSource,
};
use crate::runtime::error::{datafusion_to_core, source_decorator_error_to_core};
use crate::runtime::registration_cache::{CacheLookup, RegistrationCache};
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{CoreError, QuerySource, SourceDecorator, SourceFailurePolicy};

const RESERVED_SCHEMA_NAMES: &[&str] = &["coral", "coral_admin", "datafusion", "public"];

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

fn check_reserved_schema(schema: &str) -> DataFusionResult<()> {
    if is_reserved_schema(schema) {
        return Err(DataFusionError::Execution(reserved_schema_detail(schema)));
    }
    Ok(())
}

fn is_reserved_schema(schema: &str) -> bool {
    RESERVED_SCHEMA_NAMES.contains(&schema)
}

fn reserved_schema_detail(schema: &str) -> String {
    format!("source schema '{schema}' is reserved and cannot be used by manifests")
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
    registration_cache: Option<&RegistrationCache>,
) -> std::result::Result<SourceRegistrationResult, CoreError> {
    let span = info_span!(
        "coral.engine.sources.register",
        source.count = sources.len(),
    );
    register_sources_inner(ctx, sources, source_decorators, registration_cache)
        .instrument(span)
        .await
}

async fn register_sources_inner(
    ctx: &SessionContext,
    sources: Vec<SourceRegistrationCandidate>,
    source_decorators: &mut [Box<dyn SourceDecorator>],
    registration_cache: Option<&RegistrationCache>,
) -> std::result::Result<SourceRegistrationResult, CoreError> {
    let catalog = ctx.catalog("datafusion").ok_or_else(|| {
        let plan_err = DataFusionError::Plan("catalog 'datafusion' not found".to_string());
        datafusion_to_core(&plan_err, &[])
    })?;

    let selected_sources = sources
        .iter()
        .map(|selected| selected.source().clone())
        .collect::<Vec<_>>();
    validate_selected_source_schema_names(&selected_sources)?;
    prepare_source_decorators(source_decorators, &selected_sources)?;

    let mut result = SourceRegistrationResult::default();
    let mut seen_schemas = catalog.schema_names().into_iter().collect();
    let mut seen_catalogs = ctx.catalog_names().into_iter().collect();
    let registration_context = BackendRegistrationContext::default();

    for source in sources {
        match source {
            SourceRegistrationCandidate::Compiled(selected_source) => {
                let query_source = &selected_source.source;
                let compiled_source = selected_source.compiled;
                let source_name = compiled_source.source_name().to_string();

                match register_source(
                    ctx,
                    &registration_context,
                    &mut seen_schemas,
                    &mut seen_catalogs,
                    compiled_source.as_ref(),
                    registration_cache,
                )
                .await
                {
                    Ok(registration) => {
                        register_backend_registration(
                            ctx,
                            catalog.as_ref(),
                            source_decorators,
                            query_source,
                            &source_name,
                            registration,
                            &mut result,
                        )?;
                    }
                    Err(error) => {
                        let core_error = datafusion_to_core(&error, &[]);
                        if handle_source_registration_failure(
                            source_decorators,
                            query_source,
                            &core_error,
                        )? {
                            return Err(core_error);
                        }
                        push_source_failure(
                            &mut result,
                            &source_name,
                            compiled_source.schema_name(),
                            core_error.to_string(),
                        );
                    }
                }
            }
            SourceRegistrationCandidate::CompileFailed { source, error } => {
                if handle_source_registration_failure(source_decorators, &source, &error)? {
                    return Err(error);
                }
                push_source_failure(
                    &mut result,
                    source.source_name(),
                    source.source_name(),
                    error.to_string(),
                );
            }
        }
    }

    finish_source_decorators(source_decorators)?;

    Ok(result)
}

fn validate_selected_source_schema_names(
    sources: &[QuerySource],
) -> std::result::Result<(), CoreError> {
    let mut owner_by_schema = HashMap::new();
    for source in sources {
        for schema_name in source.schema_names() {
            if is_reserved_schema(schema_name) {
                return Err(CoreError::InvalidInput(reserved_schema_detail(schema_name)));
            }
            if let Some(existing_source) =
                owner_by_schema.insert(schema_name.to_string(), source.source_name().to_string())
            {
                return Err(CoreError::InvalidInput(format!(
                    "source '{}' runtime schema name '{schema_name}' conflicts with selected source '{existing_source}'",
                    source.source_name()
                )));
            }
        }
    }
    Ok(())
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
        None,
    ))
}

async fn register_source(
    ctx: &SessionContext,
    registration_context: &BackendRegistrationContext,
    seen_schemas: &mut std::collections::HashSet<String>,
    seen_catalogs: &mut std::collections::HashSet<String>,
    source: &dyn CompiledBackendSource,
    registration_cache: Option<&RegistrationCache>,
) -> DataFusionResult<BackendRegistration> {
    source.validate_runtime_capabilities()?;

    let fingerprint = source.registration_fingerprint();
    let cached = match (registration_cache, fingerprint.as_deref()) {
        (Some(cache), Some(fingerprint)) => cache.lookup(source.source_name(), fingerprint),
        _ => None,
    };
    let stale = match cached {
        Some(CacheLookup::Fresh(registration)) => {
            tracing::debug!(
                source = %source.source_name(),
                "reusing cached source registration"
            );
            claim_registration_schemas(&registration, seen_schemas)?;
            claim_registration_catalogs(&registration, seen_catalogs)?;
            return Ok(registration);
        }
        Some(CacheLookup::Refreshing(registration)) => {
            tracing::debug!(
                source = %source.source_name(),
                "reusing stale cached source registration while refresh is in progress"
            );
            claim_registration_schemas(&registration, seen_schemas)?;
            claim_registration_catalogs(&registration, seen_catalogs)?;
            return Ok(registration);
        }
        Some(CacheLookup::Stale {
            registration,
            claim,
        }) => Some((registration, claim)),
        None => None,
    };

    let registration = match source.register(ctx, registration_context).await {
        Ok(registration) => registration,
        Err(error) => {
            // Availability over freshness: a source that registered before
            // keeps serving its last known catalog when a claimed refresh
            // fails. The cache claim defers the next refresh attempt by one
            // time-to-live so an unreachable source costs one attempt per
            // window, not one per query.
            let Some((registration, claim)) = stale else {
                return Err(error);
            };
            tracing::warn!(
                source = %source.source_name(),
                detail = %error,
                "source registration refresh failed; keeping stale cached registration"
            );
            if let Some(cache) = registration_cache {
                cache.refresh_failed(&claim);
            }
            claim_registration_schemas(&registration, seen_schemas)?;
            claim_registration_catalogs(&registration, seen_catalogs)?;
            return Ok(registration);
        }
    };
    claim_registration_schemas(&registration, seen_schemas)?;
    claim_registration_catalogs(&registration, seen_catalogs)?;
    if let (Some(cache), Some(fingerprint)) = (registration_cache, fingerprint.as_deref()) {
        cache.store(source.source_name(), fingerprint, &registration);
    }

    Ok(registration)
}

fn claim_registration_schemas(
    registration: &BackendRegistration,
    seen_schemas: &mut std::collections::HashSet<String>,
) -> DataFusionResult<()> {
    let mut registration_schemas = std::collections::HashSet::new();
    for schema in &registration.schemas {
        let schema_name = &schema.source.schema_name;
        check_reserved_schema(schema_name)?;

        if !registration_schemas.insert(schema_name.clone()) || seen_schemas.contains(schema_name) {
            return Err(DataFusionError::Execution(format!(
                "duplicate source schema '{schema_name}'"
            )));
        }
    }
    seen_schemas.extend(registration_schemas);
    Ok(())
}

fn claim_registration_catalogs(
    registration: &BackendRegistration,
    seen_catalogs: &mut std::collections::HashSet<String>,
) -> DataFusionResult<()> {
    let mut registration_catalogs = std::collections::HashSet::new();
    for catalog in &registration.catalogs {
        let catalog_name = &catalog.catalog_name;
        check_reserved_schema(catalog_name)?;

        if !registration_catalogs.insert(catalog_name.clone())
            || seen_catalogs.contains(catalog_name)
        {
            return Err(DataFusionError::Execution(format!(
                "duplicate source catalog '{catalog_name}'"
            )));
        }
    }
    seen_catalogs.extend(registration_catalogs);
    Ok(())
}

fn register_backend_registration(
    ctx: &SessionContext,
    catalog: &dyn datafusion::catalog::CatalogProvider,
    source_decorators: &mut [Box<dyn SourceDecorator>],
    query_source: &QuerySource,
    source_name: &str,
    registration: BackendRegistration,
    result: &mut SourceRegistrationResult,
) -> std::result::Result<(), CoreError> {
    // Source decorators wrap table providers at registration time, but catalog
    // registrations expose providers lazily through the catalog itself, so
    // decorators cannot be applied to them. Fail the source instead of
    // silently bypassing an embedder's policy/observability hook.
    if !registration.catalogs.is_empty() && !source_decorators.is_empty() {
        let core_error = CoreError::FailedPrecondition(format!(
            "source '{source_name}' registers database catalogs, which do not support source decorators"
        ));
        if handle_source_registration_failure(source_decorators, query_source, &core_error)? {
            return Err(core_error);
        }
        push_source_failure(result, source_name, source_name, core_error.to_string());
        return Ok(());
    }

    let mut staged = Vec::with_capacity(registration.schemas.len());
    let mut catalog_staged = Vec::with_capacity(registration.catalogs.len());
    for catalog_registration in registration.catalogs {
        let BackendCatalogRegistration {
            catalog_name,
            catalog,
            source,
        } = catalog_registration;
        catalog_staged.push((catalog_name, catalog, source));
    }

    for schema_registration in registration.schemas {
        let BackendSchemaRegistration {
            tables,
            source: registered_source,
        } = schema_registration;
        let schema_name = registered_source.schema_name.clone();
        let decorated_tables = decorate_source_tables(source_decorators, query_source, tables)?;
        staged.push((schema_name, decorated_tables, registered_source));
    }

    let mut registered_schema_names = Vec::with_capacity(staged.len());
    for (schema_name, decorated_tables, _registered_source) in &mut staged {
        match catalog.register_schema(
            schema_name,
            Arc::new(StaticSchemaProvider::new(std::mem::take(decorated_tables))),
        ) {
            Ok(_) => {
                registered_schema_names.push(schema_name.clone());
            }
            Err(error) => {
                rollback_registered_schemas(catalog, &registered_schema_names);
                let core_error = datafusion_to_core(&error, &[]);
                if handle_source_registration_failure(source_decorators, query_source, &core_error)?
                {
                    return Err(core_error);
                }
                push_source_failure(result, source_name, schema_name, core_error.to_string());
                return Ok(());
            }
        }
    }

    for (catalog_name, registered_catalog, _registered_source) in &catalog_staged {
        ctx.register_catalog(catalog_name, Arc::clone(registered_catalog));
    }

    for (_schema_name, _decorated_tables, registered_source) in staged {
        result.active_sources.push(registered_source);
    }
    for (_catalog_name, _registered_catalog, registered_source) in catalog_staged {
        result.active_sources.push(registered_source);
    }
    Ok(())
}

fn rollback_registered_schemas(
    catalog: &dyn datafusion::catalog::CatalogProvider,
    schema_names: &[String],
) {
    for schema_name in schema_names.iter().rev() {
        if let Err(error) = catalog.deregister_schema(schema_name, true) {
            tracing::warn!(
                schema_name,
                detail = %error,
                "failed to roll back source schema registration"
            );
        }
    }
}

fn push_source_failure(
    result: &mut SourceRegistrationResult,
    source_name: &str,
    schema_name: &str,
    detail: String,
) {
    let failure = SourceRegistrationFailure {
        schema_name: schema_name.to_string(),
        detail,
    };
    tracing::warn!(
        source = %source_name,
        schema_name = %failure.schema_name,
        detail = %failure.detail,
        "skipping source"
    );
    result.failures.push(failure);
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
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use datafusion::error::Result as DataFusionResult;
    use datafusion::prelude::SessionContext;

    use crate::backends::{
        BackendRegistration, BackendRegistrationContext, BackendSchemaRegistration,
        CompiledBackendSource, RegisteredSource,
    };
    use crate::runtime::registration_cache::RegistrationCache;
    use crate::{CoreError, QuerySource, RuntimeSourcePackage, SourceDecorator};

    use super::{
        CompiledQuerySource, SourceRegistrationCandidate, check_reserved_schema, register_sources,
        validate_selected_source_schema_names,
    };

    struct CountingSource {
        name: String,
        fingerprint: Option<String>,
        registrations: Arc<AtomicUsize>,
        fail: Arc<AtomicBool>,
    }

    #[async_trait]
    impl CompiledBackendSource for CountingSource {
        fn schema_name(&self) -> &str {
            &self.name
        }

        fn source_name(&self) -> &str {
            &self.name
        }

        fn validate_runtime_capabilities(&self) -> DataFusionResult<()> {
            Ok(())
        }

        fn registration_fingerprint(&self) -> Option<String> {
            self.fingerprint.clone()
        }

        async fn register(
            &self,
            _ctx: &SessionContext,
            _registration: &BackendRegistrationContext,
        ) -> DataFusionResult<BackendRegistration> {
            self.registrations.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(datafusion::error::DataFusionError::Execution(
                    "simulated registration failure".to_string(),
                ));
            }
            Ok(BackendRegistration {
                schemas: vec![BackendSchemaRegistration {
                    tables: HashMap::new(),
                    source: RegisteredSource {
                        schema_name: self.name.clone(),
                        tables: Vec::new(),
                        table_functions: Vec::new(),
                        inputs: Vec::new(),
                    },
                }],
                catalogs: Vec::new(),
            })
        }
    }

    fn empty_query_source(name: &str) -> QuerySource {
        QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: name.to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                components: Vec::new(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("runtime package")
    }

    fn counting_candidate(
        name: &str,
        fingerprint: Option<&str>,
        registrations: &Arc<AtomicUsize>,
    ) -> SourceRegistrationCandidate {
        failable_candidate(
            name,
            fingerprint,
            registrations,
            &Arc::new(AtomicBool::new(false)),
        )
    }

    fn failable_candidate(
        name: &str,
        fingerprint: Option<&str>,
        registrations: &Arc<AtomicUsize>,
        fail: &Arc<AtomicBool>,
    ) -> SourceRegistrationCandidate {
        SourceRegistrationCandidate::Compiled(CompiledQuerySource {
            source: empty_query_source(name),
            compiled: Box::new(CountingSource {
                name: name.to_string(),
                fingerprint: fingerprint.map(ToString::to_string),
                registrations: Arc::clone(registrations),
                fail: Arc::clone(fail),
            }),
        })
    }

    async fn register_once(
        candidates: Vec<SourceRegistrationCandidate>,
        cache: Option<&RegistrationCache>,
    ) -> super::SourceRegistrationResult {
        let mut source_decorators: Vec<Box<dyn SourceDecorator>> = Vec::new();
        register_sources(
            &SessionContext::new(),
            candidates,
            source_decorators.as_mut_slice(),
            cache,
        )
        .await
        .expect("register sources")
    }

    #[tokio::test]
    async fn cached_registration_skips_backend_register_when_fingerprint_matches() {
        let registrations = Arc::new(AtomicUsize::new(0));
        let cache = RegistrationCache::new();

        for _ in 0..2 {
            let result = register_once(
                vec![counting_candidate("fake", Some("v1"), &registrations)],
                Some(&cache),
            )
            .await;
            assert_eq!(result.active_sources.len(), 1);
            assert!(result.failures.is_empty());
        }

        assert_eq!(registrations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fingerprint_change_invalidates_cached_registration() {
        let registrations = Arc::new(AtomicUsize::new(0));
        let cache = RegistrationCache::new();

        register_once(
            vec![counting_candidate("fake", Some("v1"), &registrations)],
            Some(&cache),
        )
        .await;
        register_once(
            vec![counting_candidate("fake", Some("v2"), &registrations)],
            Some(&cache),
        )
        .await;

        assert_eq!(registrations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn sources_without_fingerprint_register_every_time() {
        let registrations = Arc::new(AtomicUsize::new(0));
        let cache = RegistrationCache::new();

        for _ in 0..2 {
            register_once(
                vec![counting_candidate("fake", None, &registrations)],
                Some(&cache),
            )
            .await;
        }

        assert_eq!(registrations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn expired_cached_registration_refreshes_via_backend() {
        let registrations = Arc::new(AtomicUsize::new(0));
        let cache = RegistrationCache::with_ttl(std::time::Duration::ZERO);

        for _ in 0..2 {
            let result = register_once(
                vec![counting_candidate("fake", Some("v1"), &registrations)],
                Some(&cache),
            )
            .await;
            assert_eq!(result.active_sources.len(), 1);
            assert!(result.failures.is_empty());
        }

        assert_eq!(registrations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_refresh_serves_stale_cached_registration() {
        let registrations = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let cache = RegistrationCache::with_ttl(std::time::Duration::ZERO);

        register_once(
            vec![failable_candidate(
                "fake",
                Some("v1"),
                &registrations,
                &fail,
            )],
            Some(&cache),
        )
        .await;

        fail.store(true, Ordering::SeqCst);
        let result = register_once(
            vec![failable_candidate(
                "fake",
                Some("v1"),
                &registrations,
                &fail,
            )],
            Some(&cache),
        )
        .await;

        assert_eq!(result.active_sources.len(), 1);
        assert!(result.failures.is_empty());
        assert_eq!(registrations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_refresh_defers_next_attempt_by_one_ttl() {
        let registrations = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let cache = RegistrationCache::with_ttl(std::time::Duration::from_hours(1));

        register_once(
            vec![failable_candidate(
                "fake",
                Some("v1"),
                &registrations,
                &fail,
            )],
            Some(&cache),
        )
        .await;

        cache.force_stale("fake");
        fail.store(true, Ordering::SeqCst);
        register_once(
            vec![failable_candidate(
                "fake",
                Some("v1"),
                &registrations,
                &fail,
            )],
            Some(&cache),
        )
        .await;
        assert_eq!(registrations.load(Ordering::SeqCst), 2);

        // The failed refresh restarted the entry's time-to-live, so the next
        // build serves the cached registration without another attempt.
        let result = register_once(
            vec![failable_candidate(
                "fake",
                Some("v1"),
                &registrations,
                &fail,
            )],
            Some(&cache),
        )
        .await;
        assert_eq!(result.active_sources.len(), 1);
        assert!(result.failures.is_empty());
        assert_eq!(registrations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cache_survives_builds_that_select_other_sources() {
        let registrations = Arc::new(AtomicUsize::new(0));
        let other_registrations = Arc::new(AtomicUsize::new(0));
        let cache = RegistrationCache::new();

        register_once(
            vec![counting_candidate("fake", Some("v1"), &registrations)],
            Some(&cache),
        )
        .await;
        register_once(
            vec![counting_candidate(
                "other",
                Some("v1"),
                &other_registrations,
            )],
            Some(&cache),
        )
        .await;
        register_once(
            vec![counting_candidate("fake", Some("v1"), &registrations)],
            Some(&cache),
        )
        .await;

        assert_eq!(registrations.load(Ordering::SeqCst), 1);
        assert_eq!(other_registrations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retain_sources_evicts_uninstalled_entries() {
        let registrations = Arc::new(AtomicUsize::new(0));
        let cache = RegistrationCache::new();

        register_once(
            vec![counting_candidate("fake", Some("v1"), &registrations)],
            Some(&cache),
        )
        .await;

        cache.retain_sources(&std::collections::HashSet::from(["other"]));
        register_once(
            vec![counting_candidate("fake", Some("v1"), &registrations)],
            Some(&cache),
        )
        .await;

        assert_eq!(registrations.load(Ordering::SeqCst), 2);
    }

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
    fn reserved_schema_public_is_rejected() {
        let result = check_reserved_schema("public");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("public"),
            "error message should mention the schema name"
        );
    }

    #[test]
    fn non_reserved_schema_is_accepted() {
        check_reserved_schema("github").expect("github is not reserved");
        check_reserved_schema("pagerduty").expect("pagerduty is not reserved");
        check_reserved_schema("slack").expect("slack is not reserved");
    }

    #[test]
    fn selected_sources_reject_reserved_schema_before_backend_registration() {
        let source = QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: "public".to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                components: Vec::new(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("runtime package");

        let error = validate_selected_source_schema_names(&[source])
            .expect_err("reserved source schema should fail selected-source preflight");

        let CoreError::InvalidInput(detail) = error else {
            panic!("expected invalid input, got {error:?}");
        };
        assert!(
            detail.contains("source schema 'public' is reserved"),
            "unexpected error: {detail}"
        );
    }
}
