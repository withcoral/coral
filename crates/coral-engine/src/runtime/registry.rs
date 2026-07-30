//! Registers compiled backend sources into a shared `DataFusion` session.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, SchemaProvider};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::SessionContext;
use tracing::{Instrument as _, info_span};

use crate::backends::{
    BackendRegistration, BackendRegistrationContext, CatalogColumnFetcher, CatalogPublication,
    CompiledBackendSource, PublishedTables, SchemaPublication,
};
use crate::runtime::error::{datafusion_to_core, source_decorator_error_to_core};
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{
    CoreError, QuerySource, SourceDecorator, SourceFailurePolicy, SourceSchemaDecorationContext,
    SourceTableDecoratorTarget,
};

const RESERVED_SCHEMA_NAMES: &[&str] = &["coral", "coral_admin", "datafusion", "public"];
const DATAFUSION_DEFAULT_CATALOG: &str = "datafusion";

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

#[derive(Default)]
pub(crate) struct SourceRegistrationResult {
    pub(crate) active_publications: Vec<CatalogPublication>,
    pub(crate) column_fetchers: Vec<CatalogColumnFetcher>,
    pub(crate) failures: Vec<SourceRegistrationFailure>,
}

impl std::fmt::Debug for SourceRegistrationResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceRegistrationResult")
            .field("active_publications", &self.active_publications.len())
            .field("column_fetchers", &self.column_fetchers.len())
            .field("failures", &self.failures)
            .finish()
    }
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
    format!("source SQL name '{schema}' is reserved and cannot be used by manifests")
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
    let span = info_span!(
        "coral.engine.sources.register",
        source.count = sources.len(),
    );
    register_sources_inner(ctx, sources, source_decorators)
        .instrument(span)
        .await
}

async fn register_sources_inner(
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
    validate_selected_source_names(&selected_sources)?;
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
                )
                .await
                {
                    Ok(registration) => {
                        if let Err(core_error) = register_backend_registration(
                            ctx,
                            source_decorators,
                            query_source,
                            &source_name,
                            registration,
                            &mut result,
                        ) {
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
                                &source_name,
                                core_error.to_string(),
                            );
                        }
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
                            &source_name,
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

fn validate_selected_source_names(sources: &[QuerySource]) -> std::result::Result<(), CoreError> {
    let mut owner_by_name = HashMap::new();
    for source in sources {
        let names = source
            .schema_names()
            .into_iter()
            .map(|name| (name, "schema"))
            .chain(
                source
                    .catalog_names()
                    .into_iter()
                    .map(|name| (name, "catalog")),
            );
        for (name, kind) in names {
            if is_reserved_schema(name) {
                return Err(CoreError::InvalidInput(reserved_schema_detail(name)));
            }
            if let Some(existing_source) =
                owner_by_name.insert(name.to_string(), source.source_name().to_string())
            {
                return Err(CoreError::InvalidInput(format!(
                    "source '{}' runtime {kind} name '{name}' conflicts with selected source '{existing_source}'",
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
    ))
}

async fn register_source(
    ctx: &SessionContext,
    registration_context: &BackendRegistrationContext,
    seen_schemas: &mut std::collections::HashSet<String>,
    seen_catalogs: &mut std::collections::HashSet<String>,
    source: &dyn CompiledBackendSource,
) -> DataFusionResult<BackendRegistration> {
    source.validate_runtime_capabilities()?;

    let registration = source.register(ctx, registration_context).await?;
    claim_catalog_publications(&registration, seen_schemas, seen_catalogs)?;

    Ok(registration)
}

fn claim_catalog_publications(
    registration: &BackendRegistration,
    seen_schemas: &mut HashSet<String>,
    seen_catalogs: &mut HashSet<String>,
) -> DataFusionResult<()> {
    let mut claimed_schemas = HashSet::new();
    let mut claimed_catalogs = HashSet::new();

    for publication in &registration.catalog_publications {
        validate_catalog_name(&publication.catalog_name)?;
        if publication.catalog_name == DATAFUSION_DEFAULT_CATALOG {
            for schema_name in publication.schema_names() {
                check_reserved_schema(schema_name)?;
                if seen_schemas.contains(schema_name)
                    || !claimed_schemas.insert(schema_name.to_string())
                {
                    return Err(DataFusionError::Execution(format!(
                        "duplicate source schema '{schema_name}'"
                    )));
                }
            }
        } else if seen_catalogs.contains(&publication.catalog_name)
            || !claimed_catalogs.insert(publication.catalog_name.clone())
        {
            return Err(DataFusionError::Execution(format!(
                "duplicate source catalog '{}'",
                publication.catalog_name
            )));
        }
    }

    seen_schemas.extend(claimed_schemas);
    seen_catalogs.extend(claimed_catalogs);
    Ok(())
}

fn validate_catalog_name(catalog_name: &str) -> DataFusionResult<()> {
    if catalog_name.trim().is_empty() {
        return Err(DataFusionError::Execution(
            "source catalog name cannot be empty".to_string(),
        ));
    }
    if catalog_name != DATAFUSION_DEFAULT_CATALOG && is_reserved_schema(catalog_name) {
        return Err(DataFusionError::Execution(reserved_schema_detail(
            catalog_name,
        )));
    }
    Ok(())
}

fn register_backend_registration(
    ctx: &SessionContext,
    source_decorators: &mut [Box<dyn SourceDecorator>],
    query_source: &QuerySource,
    source_name: &str,
    registration: BackendRegistration,
    result: &mut SourceRegistrationResult,
) -> std::result::Result<(), CoreError> {
    register_catalog_publications(
        ctx,
        source_decorators,
        query_source,
        source_name,
        registration.catalog_publications,
        result,
    )
}

fn register_catalog_publications(
    ctx: &SessionContext,
    source_decorators: &mut [Box<dyn SourceDecorator>],
    query_source: &QuerySource,
    source_name: &str,
    mut publications: Vec<CatalogPublication>,
    result: &mut SourceRegistrationResult,
) -> std::result::Result<(), CoreError> {
    preflight_catalog_publication_targets(ctx, &publications)
        .map_err(|error| datafusion_to_core(&error, &[]))?;

    if publications.iter().any(catalog_publication_has_lazy_schema)
        && let Some(decorator) = source_decorators
            .iter()
            .find(|decorator| !decorator.supports_lazy_schemas())
    {
        let core_error = CoreError::FailedPrecondition(format!(
            "source '{source_name}' publishes lazy schemas, which source decorator '{}' does not support",
            decorator.name()
        ));
        if handle_source_registration_failure(source_decorators, query_source, &core_error)? {
            return Err(core_error);
        }
        push_source_failure(result, source_name, source_name, core_error.to_string());
        return Ok(());
    }

    decorate_catalog_publications(source_decorators, query_source, &mut publications)?;

    let default_catalog = ctx.catalog(DATAFUSION_DEFAULT_CATALOG).ok_or_else(|| {
        CoreError::FailedPrecondition(format!("catalog '{DATAFUSION_DEFAULT_CATALOG}' not found"))
    })?;
    let mut default_schemas = Vec::new();
    let mut new_catalogs = Vec::new();

    for publication in &publications {
        if publication.catalog_name == DATAFUSION_DEFAULT_CATALOG {
            for schema in publication.schema_publications() {
                default_schemas.push((
                    schema.schema_name.clone(),
                    schema_provider_for_publication(schema),
                ));
            }
        } else {
            let catalog = Arc::new(MemoryCatalogProvider::new());
            for schema in publication.schema_publications() {
                catalog
                    .register_schema(&schema.schema_name, schema_provider_for_publication(schema))
                    .map_err(|error| datafusion_to_core(&error, &[]))?;
            }
            new_catalogs.push((
                publication.catalog_name.clone(),
                catalog as Arc<dyn CatalogProvider>,
            ));
        }
    }

    let mut registered_default_schemas = Vec::new();
    for (schema_name, provider) in default_schemas {
        match default_catalog.register_schema(&schema_name, provider) {
            Ok(None) => registered_default_schemas.push(schema_name),
            Ok(Some(previous)) => {
                let _restore = default_catalog.register_schema(&schema_name, previous);
                rollback_registered_schemas(default_catalog.as_ref(), &registered_default_schemas);
                return Err(CoreError::FailedPrecondition(format!(
                    "catalog '{DATAFUSION_DEFAULT_CATALOG}' schema '{schema_name}' appeared after publication preflight"
                )));
            }
            Err(error) => {
                rollback_registered_schemas(default_catalog.as_ref(), &registered_default_schemas);
                return Err(datafusion_to_core(&error, &[]));
            }
        }
    }

    // Non-default catalogs are fully assembled in memory above. DataFusion's
    // catalog-list insertion is infallible, so no fallible work remains after
    // the first source-owned catalog becomes visible.
    for (catalog_name, catalog) in new_catalogs {
        debug_assert!(
            ctx.catalog(&catalog_name).is_none(),
            "publication preflight must reject existing source-owned catalogs"
        );
        ctx.register_catalog(catalog_name, catalog);
    }

    for publication in &publications {
        if let Some(fetcher) = &publication.column_fetcher {
            let relation_names = publication
                .schema_publications()
                .flat_map(|schema| {
                    let table_names = match &schema.tables {
                        PublishedTables::Static(tables) => tables.keys().collect::<Vec<_>>(),
                        PublishedTables::Lazy { tables, .. } => tables.keys().collect::<Vec<_>>(),
                    };
                    table_names
                        .into_iter()
                        .map(|table_name| (schema.schema_name.clone(), table_name.clone()))
                        .collect::<Vec<_>>()
                })
                .collect();
            result.column_fetchers.push(CatalogColumnFetcher {
                catalog_name: publication.catalog_name.clone(),
                relation_names,
                fetcher: Arc::clone(fetcher),
            });
        }
    }
    result.active_publications.extend(publications);

    Ok(())
}

fn preflight_catalog_publication_targets(
    ctx: &SessionContext,
    publications: &[CatalogPublication],
) -> DataFusionResult<()> {
    let default_catalog = ctx.catalog(DATAFUSION_DEFAULT_CATALOG).ok_or_else(|| {
        DataFusionError::Plan(format!("catalog '{DATAFUSION_DEFAULT_CATALOG}' not found"))
    })?;
    let existing_default_schemas = default_catalog
        .schema_names()
        .into_iter()
        .collect::<HashSet<_>>();
    let mut staged_default_schemas = HashSet::new();
    let mut staged_catalogs = HashSet::new();

    for publication in publications {
        validate_catalog_name(&publication.catalog_name)?;
        let mut input_keys = HashSet::new();
        for input in &publication.inputs {
            if !input_keys.insert(input.key.as_str()) {
                return Err(DataFusionError::Execution(format!(
                    "catalog '{}' publishes duplicate input '{}'",
                    publication.catalog_name, input.key
                )));
            }
        }
        let _column_fetcher = publication.column_fetcher.as_ref();

        if publication.catalog_name == DATAFUSION_DEFAULT_CATALOG {
            for (schema_key, schema) in publication.schema_entries() {
                validate_schema_publication_key(&publication.catalog_name, schema_key, schema)?;
                check_reserved_schema(schema_key)?;
                if existing_default_schemas.contains(schema_key)
                    || !staged_default_schemas.insert(schema_key.to_string())
                {
                    return Err(DataFusionError::Execution(format!(
                        "duplicate source schema '{schema_key}'"
                    )));
                }
                validate_schema_publication_contents(&publication.catalog_name, schema)?;
            }
        } else {
            if ctx.catalog(&publication.catalog_name).is_some()
                || !staged_catalogs.insert(publication.catalog_name.clone())
            {
                return Err(DataFusionError::Execution(format!(
                    "duplicate source catalog '{}'",
                    publication.catalog_name
                )));
            }
            for (schema_key, schema) in publication.schema_entries() {
                validate_schema_publication_key(&publication.catalog_name, schema_key, schema)?;
                validate_schema_publication_contents(&publication.catalog_name, schema)?;
            }
        }
    }
    Ok(())
}

fn validate_schema_publication_key(
    catalog_name: &str,
    schema_key: &str,
    schema: &SchemaPublication,
) -> DataFusionResult<()> {
    if schema_key.trim().is_empty() {
        return Err(DataFusionError::Execution(format!(
            "catalog '{catalog_name}' schema name cannot be empty"
        )));
    }
    if schema.schema_name != schema_key {
        return Err(DataFusionError::Internal(format!(
            "catalog '{catalog_name}' schema map key '{schema_key}' does not match publication name '{}'",
            schema.schema_name
        )));
    }
    Ok(())
}

fn validate_schema_publication_contents(
    catalog_name: &str,
    schema: &SchemaPublication,
) -> DataFusionResult<()> {
    match &schema.tables {
        PublishedTables::Static(tables) => {
            for (table_name, table) in tables {
                validate_relation_leaf(catalog_name, &schema.schema_name, table_name, "table")?;
                validate_table_metadata(
                    catalog_name,
                    &schema.schema_name,
                    table_name,
                    &table.metadata,
                )?;
            }
        }
        PublishedTables::Lazy { provider, tables } => {
            let provider_names = provider.table_names().into_iter().collect::<BTreeSet<_>>();
            let metadata_names = tables.keys().cloned().collect::<BTreeSet<_>>();
            if provider_names != metadata_names {
                return Err(DataFusionError::Execution(format!(
                    "catalog '{catalog_name}' schema '{}' lazy provider table names do not match published metadata",
                    schema.schema_name
                )));
            }
            for (table_name, metadata) in tables {
                validate_relation_leaf(catalog_name, &schema.schema_name, table_name, "table")?;
                validate_table_metadata(catalog_name, &schema.schema_name, table_name, metadata)?;
            }
        }
    }
    for (function_name, function) in &schema.table_functions {
        validate_relation_leaf(
            catalog_name,
            &schema.schema_name,
            function_name,
            "table function",
        )?;
        validate_table_function_metadata(
            catalog_name,
            &schema.schema_name,
            function_name,
            function,
        )?;
    }
    Ok(())
}

fn validate_table_metadata(
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    metadata: &crate::backends::common::RegisteredTableMetadata,
) -> DataFusionResult<()> {
    validate_metadata_text(
        catalog_name,
        schema_name,
        table_name,
        "table",
        "description",
        &metadata.description,
    )?;
    validate_metadata_text(
        catalog_name,
        schema_name,
        table_name,
        "table",
        "guide",
        &metadata.guide,
    )?;
    validate_search_limits_metadata(
        catalog_name,
        schema_name,
        table_name,
        "table",
        metadata.search_limits.as_ref(),
    )?;
    let mut column_names = HashSet::new();
    for column in &metadata.columns {
        if column.name.trim().is_empty() || !column_names.insert(column.name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "catalog '{catalog_name}' schema '{schema_name}' table '{table_name}' publishes invalid or duplicate column metadata '{}'",
                column.name
            )));
        }
    }
    let mut filter_names = HashSet::new();
    for filter in &metadata.filters {
        if filter.name.trim().is_empty() || !filter_names.insert(filter.name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "catalog '{catalog_name}' schema '{schema_name}' table '{table_name}' publishes invalid or duplicate filter metadata '{}'",
                filter.name
            )));
        }
    }
    for required_filter in &metadata.required_filters {
        if !filter_names.contains(required_filter.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "catalog '{catalog_name}' schema '{schema_name}' table '{table_name}' requires unpublished filter '{required_filter}'"
            )));
        }
    }
    Ok(())
}

fn validate_table_function_metadata(
    catalog_name: &str,
    schema_name: &str,
    function_name: &str,
    function: &crate::backends::common::TableFunctionPublication,
) -> DataFusionResult<()> {
    validate_metadata_text(
        catalog_name,
        schema_name,
        function_name,
        "table function",
        "description",
        &function.metadata.description,
    )?;
    validate_metadata_text(
        catalog_name,
        schema_name,
        function_name,
        "table function",
        "guide",
        &function.metadata.guide,
    )?;
    validate_search_limits_metadata(
        catalog_name,
        schema_name,
        function_name,
        "table function",
        function.metadata.search_limits.as_ref(),
    )?;
    if function.metadata.kind == coral_spec::SourceTableFunctionKind::Search
        && function.metadata.search_limits.is_none()
    {
        return Err(DataFusionError::Execution(format!(
            "catalog '{catalog_name}' schema '{schema_name}' table function '{function_name}' search metadata is missing limits"
        )));
    }
    let mut argument_names = HashSet::new();
    for argument in &function.metadata.arguments {
        if argument.name.trim().is_empty() || !argument_names.insert(argument.name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "catalog '{catalog_name}' schema '{schema_name}' table function '{function_name}' publishes invalid or duplicate argument metadata '{}'",
                argument.name
            )));
        }
    }
    let mut result_column_names = HashSet::new();
    for column in &function.metadata.result_columns {
        if column.name.trim().is_empty() || !result_column_names.insert(column.name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "catalog '{catalog_name}' schema '{schema_name}' table function '{function_name}' publishes invalid or duplicate result column metadata '{}'",
                column.name
            )));
        }
    }
    let factory_schema = function.factory.schema();
    let factory_column_names = factory_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<HashSet<_>>();
    if !result_column_names.is_empty() && factory_column_names != result_column_names {
        return Err(DataFusionError::Execution(format!(
            "catalog '{catalog_name}' schema '{schema_name}' table function '{function_name}' factory schema does not match published result metadata"
        )));
    }
    Ok(())
}

fn validate_metadata_text(
    catalog_name: &str,
    schema_name: &str,
    relation_name: &str,
    relation_kind: &str,
    field_name: &str,
    value: &str,
) -> DataFusionResult<()> {
    if value.contains('\0') {
        return Err(DataFusionError::Execution(format!(
            "catalog '{catalog_name}' schema '{schema_name}' {relation_kind} '{relation_name}' {field_name} contains a NUL character"
        )));
    }
    Ok(())
}

fn validate_search_limits_metadata(
    catalog_name: &str,
    schema_name: &str,
    relation_name: &str,
    relation_kind: &str,
    limits: Option<&coral_spec::SearchLimitsSpec>,
) -> DataFusionResult<()> {
    let Some(limits) = limits else {
        return Ok(());
    };
    if limits.default_top_k == 0
        || limits.max_top_k == 0
        || limits.max_calls_per_query == 0
        || limits.default_top_k > limits.max_top_k
        || limits
            .max_top_k
            .checked_mul(limits.max_calls_per_query)
            .is_none()
    {
        return Err(DataFusionError::Execution(format!(
            "catalog '{catalog_name}' schema '{schema_name}' {relation_kind} '{relation_name}' publishes invalid search limits"
        )));
    }
    Ok(())
}

fn validate_relation_leaf(
    catalog_name: &str,
    schema_name: &str,
    relation_name: &str,
    relation_kind: &str,
) -> DataFusionResult<()> {
    if relation_name.trim().is_empty() {
        return Err(DataFusionError::Execution(format!(
            "catalog '{catalog_name}' schema '{schema_name}' {relation_kind} name cannot be empty"
        )));
    }
    Ok(())
}

fn catalog_publication_has_lazy_schema(publication: &CatalogPublication) -> bool {
    publication
        .schema_publications()
        .any(|schema| matches!(schema.tables, PublishedTables::Lazy { .. }))
}

fn decorate_catalog_publications(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    source: &QuerySource,
    publications: &mut [CatalogPublication],
) -> std::result::Result<(), CoreError> {
    for publication in publications {
        let catalog_name = publication.catalog_name.clone();
        for schema in publication.schema_publications_mut() {
            let PublishedTables::Static(tables) = &mut schema.tables else {
                continue;
            };
            for decorator in &mut *source_decorators {
                let mut targets = tables
                    .iter_mut()
                    .map(|(table_name, table)| {
                        SourceTableDecoratorTarget::new(table_name.as_str(), &mut table.provider)
                    })
                    .collect::<Vec<_>>();
                decorator
                    .decorate_schema(
                        SourceSchemaDecorationContext::new(
                            source,
                            &catalog_name,
                            &schema.schema_name,
                        ),
                        &mut targets,
                    )
                    .map_err(|error| source_decorator_error(decorator.name(), &error))?;
            }
        }
    }
    Ok(())
}

fn schema_provider_for_publication(schema: &SchemaPublication) -> Arc<dyn SchemaProvider> {
    match &schema.tables {
        PublishedTables::Static(tables) => Arc::new(StaticSchemaProvider::new(
            tables
                .iter()
                .map(|(name, table)| (name.clone(), Arc::clone(&table.provider)))
                .collect(),
        )),
        PublishedTables::Lazy { provider, .. } => Arc::clone(provider),
    }
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
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::SchemaProvider;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::prelude::SessionContext;

    use crate::backends::common::test_support::StubSourceFunctionFactory;
    use crate::backends::common::{
        BackendRegistration, CatalogPublication, RegisteredTableFunctionMetadata,
        RegisteredTableMetadata,
    };
    use crate::runtime::schema_provider::StaticSchemaProvider;
    use crate::{
        CoreError, QuerySource, RuntimeSourcePackage, SourceDecorator, SourceDecoratorError,
        SourceSchemaDecorationContext, SourceTableDecoratorTarget,
    };

    use super::{
        DATAFUSION_DEFAULT_CATALOG, SourceRegistrationResult, check_reserved_schema,
        register_backend_registration, validate_selected_source_names,
    };

    fn query_source(source_name: &str) -> QuerySource {
        QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: source_name.to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                identity_requirements: None,
                components: Vec::new(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("mock query source")
    }

    fn table_provider(value: i64) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .expect("mock record batch");
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mock table"))
    }

    fn lazy_schema(
        tables: impl IntoIterator<Item = (&'static str, i64)>,
    ) -> Arc<dyn SchemaProvider> {
        Arc::new(StaticSchemaProvider::new(
            tables
                .into_iter()
                .map(|(name, value)| (name.to_string(), table_provider(value)))
                .collect(),
        ))
    }

    fn install_publication(
        ctx: &SessionContext,
        source: &QuerySource,
        decorators: &mut [Box<dyn SourceDecorator>],
        publication: CatalogPublication,
    ) -> Result<SourceRegistrationResult, CoreError> {
        let mut result = SourceRegistrationResult::default();
        register_backend_registration(
            ctx,
            decorators,
            source,
            source.source_name(),
            BackendRegistration::single(publication),
            &mut result,
        )?;
        Ok(result)
    }

    async fn query_values(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        ctx.sql(sql)
            .await
            .expect("mock SQL should plan")
            .collect()
            .await
            .expect("mock SQL should execute")
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 result");
                (0..values.len())
                    .map(|row| values.value(row))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    struct ReplacingDecorator {
        decorator_name: &'static str,
        replacement: i64,
        fail_schema: Option<&'static str>,
        supports_lazy: bool,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl SourceDecorator for ReplacingDecorator {
        fn name(&self) -> &'static str {
            self.decorator_name
        }

        fn supports_lazy_schemas(&self) -> bool {
            self.supports_lazy
        }

        fn decorate_schema(
            &mut self,
            context: SourceSchemaDecorationContext<'_>,
            tables: &mut [SourceTableDecoratorTarget<'_>],
        ) -> Result<(), SourceDecoratorError> {
            self.calls
                .lock()
                .map_err(|_poisoned| {
                    SourceDecoratorError::failed_precondition("decorator call log is poisoned")
                })?
                .push(format!(
                    "{}:{}.{}",
                    self.decorator_name,
                    context.catalog_name(),
                    context.schema_name()
                ));
            if context.source().source_name() != "github_v4" {
                return Err(SourceDecoratorError::failed_precondition(
                    "unexpected source decoration context",
                ));
            }
            if self.fail_schema == Some(context.schema_name()) {
                return Err(SourceDecoratorError::failed_precondition(
                    "mock decoration failure",
                ));
            }
            for table in tables {
                if table.table_name() != "items" {
                    return Err(SourceDecoratorError::failed_precondition(
                        "unexpected table decoration target",
                    ));
                }
                if table.provider().schema().fields().len() != 1 {
                    return Err(SourceDecoratorError::failed_precondition(
                        "unexpected provider schema",
                    ));
                }
                table.replace_provider(table_provider(self.replacement));
            }
            Ok(())
        }
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
                identity_requirements: None,
                components: Vec::new(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("runtime package");

        let error = validate_selected_source_names(&[source])
            .expect_err("reserved source schema should fail selected-source preflight");

        let CoreError::InvalidInput(detail) = error else {
            panic!("expected invalid input, got {error:?}");
        };
        assert!(
            detail.contains("source SQL name 'public' is reserved"),
            "unexpected error: {detail}"
        );
    }

    #[test]
    fn catalog_publication_builder_rejects_only_exact_duplicates() {
        let mut publication = CatalogPublication::new("github_v4", Vec::new());
        publication
            .publish_table(
                "issues",
                "items",
                table_provider(1),
                RegisteredTableMetadata::default(),
            )
            .expect("first table publication");
        publication
            .publish_table(
                "pulls",
                "items",
                table_provider(2),
                RegisteredTableMetadata::default(),
            )
            .expect("repeated leaf in another schema");
        let error = publication
            .publish_table(
                "issues",
                "items",
                table_provider(3),
                RegisteredTableMetadata::default(),
            )
            .expect_err("exact duplicate table identity");
        assert!(
            error
                .to_string()
                .contains("duplicate table publication 'github_v4.issues.items'")
        );

        let factory = Arc::new(StubSourceFunctionFactory::default());
        publication
            .publish_table_function(
                "issues",
                "search",
                factory.clone(),
                RegisteredTableFunctionMetadata::default(),
            )
            .expect("first function publication");
        let error = publication
            .publish_table_function(
                "issues",
                "search",
                factory,
                RegisteredTableFunctionMetadata::default(),
            )
            .expect_err("exact duplicate function identity");
        assert!(
            error
                .to_string()
                .contains("duplicate table-function publication 'github_v4.issues.search'")
        );
    }

    #[tokio::test]
    async fn catalog_publication_registers_static_and_nested_public_lazy_schemas() {
        let ctx = SessionContext::new();
        let source = query_source("github_v4");
        let mut publication = CatalogPublication::new("github_v4", Vec::new());
        publication
            .publish_table(
                "issues",
                "items",
                table_provider(1),
                RegisteredTableMetadata::default(),
            )
            .expect("static table");
        publication
            .publish_lazy_schema(
                "public",
                lazy_schema([("items", 2)]),
                BTreeMap::from([("items".to_string(), RegisteredTableMetadata::default())]),
            )
            .expect("nested public lazy schema");

        let mut decorators: Vec<Box<dyn SourceDecorator>> = Vec::new();
        install_publication(&ctx, &source, &mut decorators, publication)
            .expect("catalog publication");

        assert_eq!(
            query_values(
                &ctx,
                "SELECT value FROM github_v4.issues.items \
                 UNION ALL SELECT value FROM github_v4.public.items ORDER BY value",
            )
            .await,
            vec![1, 2]
        );
    }

    #[test]
    fn catalog_publication_rejects_lazy_provider_metadata_drift() {
        let ctx = SessionContext::new();
        let source = query_source("github_v4");
        let mut publication = CatalogPublication::new("github_v4", Vec::new());
        publication
            .publish_lazy_schema(
                "public",
                lazy_schema([("actual", 1)]),
                BTreeMap::from([("declared".to_string(), RegisteredTableMetadata::default())]),
            )
            .expect("builder captures lazy provider and metadata");

        let mut decorators: Vec<Box<dyn SourceDecorator>> = Vec::new();
        let error = install_publication(&ctx, &source, &mut decorators, publication)
            .expect_err("provider and metadata names must match");

        assert!(
            error
                .to_string()
                .contains("lazy provider table names do not match published metadata")
        );
        assert!(ctx.catalog("github_v4").is_none());
    }

    #[tokio::test]
    async fn catalog_publication_extends_datafusion_without_replacing_it() {
        let ctx = SessionContext::new();
        let original = ctx
            .catalog(DATAFUSION_DEFAULT_CATALOG)
            .expect("default catalog");
        let source = query_source("github");
        let mut publication = CatalogPublication::new(DATAFUSION_DEFAULT_CATALOG, Vec::new());
        publication
            .publish_table(
                "github",
                "items",
                table_provider(7),
                RegisteredTableMetadata::default(),
            )
            .expect("legacy schema table");

        let mut decorators: Vec<Box<dyn SourceDecorator>> = Vec::new();
        install_publication(&ctx, &source, &mut decorators, publication)
            .expect("datafusion extension");

        let current = ctx
            .catalog(DATAFUSION_DEFAULT_CATALOG)
            .expect("default catalog remains");
        assert!(Arc::ptr_eq(&original, &current));
        assert_eq!(
            query_values(&ctx, "SELECT value FROM github.items").await,
            vec![7]
        );
    }

    #[test]
    fn catalog_publication_rejects_top_level_public_schema() {
        let ctx = SessionContext::new();
        let source = query_source("legacy");
        let mut publication = CatalogPublication::new(DATAFUSION_DEFAULT_CATALOG, Vec::new());
        publication
            .publish_table(
                "public",
                "items",
                table_provider(1),
                RegisteredTableMetadata::default(),
            )
            .expect("builder accepts identity before registry policy");

        let mut decorators: Vec<Box<dyn SourceDecorator>> = Vec::new();
        let error = install_publication(&ctx, &source, &mut decorators, publication)
            .expect_err("top-level public schema remains reserved");
        assert!(error.to_string().contains("public"));
    }

    #[tokio::test]
    async fn source_schema_decorator_replaces_providers_in_order() {
        let ctx = SessionContext::new();
        let source = query_source("github_v4");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut publication = CatalogPublication::new("github_v4", Vec::new());
        publication
            .publish_table(
                "issues",
                "items",
                table_provider(1),
                RegisteredTableMetadata::default(),
            )
            .expect("mock table");
        let mut decorators: Vec<Box<dyn SourceDecorator>> = vec![
            Box::new(ReplacingDecorator {
                decorator_name: "first",
                replacement: 2,
                fail_schema: None,
                supports_lazy: false,
                calls: Arc::clone(&calls),
            }),
            Box::new(ReplacingDecorator {
                decorator_name: "second",
                replacement: 3,
                fail_schema: None,
                supports_lazy: false,
                calls: Arc::clone(&calls),
            }),
        ];

        install_publication(&ctx, &source, &mut decorators, publication)
            .expect("decorated publication");

        assert_eq!(
            calls.lock().expect("decorator calls").as_slice(),
            ["first:github_v4.issues", "second:github_v4.issues"]
        );
        assert_eq!(
            query_values(&ctx, "SELECT value FROM github_v4.issues.items").await,
            vec![3]
        );
    }

    #[test]
    fn source_schema_decorator_failure_leaves_no_partial_catalog() {
        let ctx = SessionContext::new();
        let source = query_source("github_v4");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut publication = CatalogPublication::new("github_v4", Vec::new());
        for schema_name in ["issues", "pulls"] {
            publication
                .publish_table(
                    schema_name,
                    "items",
                    table_provider(1),
                    RegisteredTableMetadata::default(),
                )
                .expect("mock table");
        }
        let mut decorators: Vec<Box<dyn SourceDecorator>> = vec![Box::new(ReplacingDecorator {
            decorator_name: "failing",
            replacement: 2,
            fail_schema: Some("pulls"),
            supports_lazy: false,
            calls,
        })];

        let error = install_publication(&ctx, &source, &mut decorators, publication)
            .expect_err("decoration should fail");

        assert!(error.to_string().contains("mock decoration failure"));
        assert!(ctx.catalog("github_v4").is_none());
    }

    #[test]
    fn source_schema_decorator_lazy_preflight_fails_closed_before_static_decoration() {
        let ctx = SessionContext::new();
        let source = query_source("github_v4");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut publication = CatalogPublication::new("github_v4", Vec::new());
        publication
            .publish_table(
                "issues",
                "items",
                table_provider(1),
                RegisteredTableMetadata::default(),
            )
            .expect("static table");
        publication
            .publish_lazy_schema(
                "public",
                lazy_schema([("items", 2)]),
                BTreeMap::from([("items".to_string(), RegisteredTableMetadata::default())]),
            )
            .expect("lazy schema");
        let mut decorators: Vec<Box<dyn SourceDecorator>> = vec![Box::new(ReplacingDecorator {
            decorator_name: "static-only",
            replacement: 3,
            fail_schema: None,
            supports_lazy: false,
            calls: Arc::clone(&calls),
        })];

        let result = install_publication(&ctx, &source, &mut decorators, publication)
            .expect("unsupported lazy schema is isolated");

        assert!(calls.lock().expect("decorator calls").is_empty());
        assert_eq!(result.failures.len(), 1);
        assert!(ctx.catalog("github_v4").is_none());
    }
}
