//! Registers compiled backend sources into a shared `DataFusion` session.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::SessionContext;
use tracing::{Instrument as _, info_span};

use crate::backends::{
    BackendRegistration, BackendRegistrationContext, CompiledBackendSource, RegisteredSource,
    RegisteredSourceTable, RegisteredTableImplementation, SourceTableFunctions,
};
use crate::runtime::error::{datafusion_to_core, source_decorator_error_to_core};
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::runtime::source_views::{SourceSqlView, build_source_views};
use crate::{CoreError, QuerySource, SourceDecorator, SourceFailurePolicy, SourceTables};

const RESERVED_SCHEMA_NAMES: &[&str] = &["coral", "coral_admin"];

/// One selected query source together with its compiled backend artifact.
///
/// The registry needs both values at once: the compiled backend source drives
/// registration, while the original `QuerySource` is what source decorators
/// reason about during prepare, decoration, and failure handling.
pub(crate) struct CompiledQuerySource {
    pub(crate) source: QuerySource,
    pub(crate) compiled: Box<dyn CompiledBackendSource>,
}

struct SourceTableImplementations {
    provider_tables: SourceTables,
    sql_views: Vec<SourceSqlView>,
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
    prepare_source_decorators(source_decorators, &selected_sources)?;

    let mut result = SourceRegistrationResult::default();
    let mut seen_schemas = HashSet::new();
    let registration_context = BackendRegistrationContext::default();

    for source in sources {
        match source {
            SourceRegistrationCandidate::Compiled(selected_source) => {
                register_compiled_source(
                    ctx,
                    &catalog,
                    &registration_context,
                    &mut seen_schemas,
                    source_decorators,
                    &mut result,
                    selected_source,
                )
                .await?;
            }
            SourceRegistrationCandidate::CompileFailed { source, error } => {
                record_source_failure(
                    source_decorators,
                    &mut result,
                    &source,
                    source.source_name(),
                    source.source_name(),
                    error,
                )?;
            }
        }
    }

    finish_source_decorators(source_decorators)?;

    Ok(result)
}

async fn register_compiled_source(
    ctx: &SessionContext,
    catalog: &Arc<dyn CatalogProvider>,
    registration_context: &BackendRegistrationContext,
    seen_schemas: &mut HashSet<String>,
    source_decorators: &mut [Box<dyn SourceDecorator>],
    result: &mut SourceRegistrationResult,
    selected_source: CompiledQuerySource,
) -> std::result::Result<(), CoreError> {
    let query_source = selected_source.source;
    let compiled_source = selected_source.compiled;
    let schema_name = compiled_source.schema_name().to_string();
    let source_name = compiled_source.source_name().to_string();

    let registration = match register_source(
        ctx,
        registration_context,
        seen_schemas,
        compiled_source.as_ref(),
    )
    .await
    {
        Ok(registration) => registration,
        Err(error) => {
            return record_source_failure(
                source_decorators,
                result,
                &query_source,
                &source_name,
                &schema_name,
                datafusion_to_core(&error, &[]),
            );
        }
    };

    let (source_tables, table_functions, registered_source) = registration.into_parts();
    let table_implementations = split_source_table_implementations(source_tables);
    // Decorators wrap concrete backend providers first; SQL views are planned
    // afterward so they resolve through the decorated source tables.
    let decorated_tables = decorate_source_tables(
        source_decorators,
        &query_source,
        table_implementations.provider_tables,
    )?;
    let source_tables = match build_source_schema_tables(
        ctx,
        catalog,
        compiled_source.schema_name(),
        decorated_tables,
        table_implementations.sql_views,
    )
    .await
    {
        Ok(source_tables) => source_tables,
        Err(error) => {
            return record_source_failure(
                source_decorators,
                result,
                &query_source,
                &source_name,
                &schema_name,
                datafusion_to_core(&error, &[]),
            );
        }
    };

    if let Err(error) = publish_source_schema(catalog, compiled_source.schema_name(), source_tables)
    {
        return record_source_failure(
            source_decorators,
            result,
            &query_source,
            &source_name,
            &schema_name,
            datafusion_to_core(&error, &[]),
        );
    }

    register_table_functions(ctx, table_functions);
    result.active_sources.push(registered_source);
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
    source: &dyn CompiledBackendSource,
) -> DataFusionResult<BackendRegistration> {
    check_reserved_schema(source.schema_name())?;

    if !seen_schemas.insert(source.schema_name().to_string()) {
        return Err(DataFusionError::Execution(format!(
            "duplicate source schema '{}'",
            source.schema_name()
        )));
    }

    source.register(ctx, registration_context).await
}

fn split_source_table_implementations(
    tables: Vec<RegisteredSourceTable>,
) -> SourceTableImplementations {
    let mut provider_tables = SourceTables::new();
    let mut sql_views = Vec::new();

    for table in tables {
        match table.implementation {
            RegisteredTableImplementation::Provider(provider) => {
                provider_tables.insert(table.metadata.table_name, provider);
            }
            RegisteredTableImplementation::SqlView { sql } => {
                sql_views.push(SourceSqlView::new(table.metadata, sql));
            }
        }
    }

    SourceTableImplementations {
        provider_tables,
        sql_views,
    }
}

async fn build_source_schema_tables(
    ctx: &SessionContext,
    catalog: &Arc<dyn CatalogProvider>,
    schema_name: &str,
    mut provider_tables: SourceTables,
    sql_views: Vec<SourceSqlView>,
) -> DataFusionResult<SourceTables> {
    if sql_views.is_empty() {
        return Ok(provider_tables);
    }

    // Source registration uses a mutable DataFusion schema only while building
    // SQL-backed views. The published source schema remains static, preserving
    // the runtime invariant that source tables cannot be added after
    // registration.
    let planning_schema = SourcePlanningSchema::register(catalog, schema_name, &provider_tables)?;
    let view_tables = build_source_views(ctx, schema_name, sql_views).await;
    let restore_result = planning_schema.restore();
    let view_tables = match (view_tables, restore_result) {
        (Err(error), _) | (Ok(_), Err(error)) => return Err(error),
        (Ok(view_tables), Ok(())) => view_tables,
    };
    merge_source_view_tables(schema_name, &mut provider_tables, view_tables)?;
    Ok(provider_tables)
}

struct SourcePlanningSchema {
    catalog: Arc<dyn CatalogProvider>,
    schema_name: String,
    previous_schema: Option<Arc<dyn SchemaProvider>>,
}

impl SourcePlanningSchema {
    fn register(
        catalog: &Arc<dyn CatalogProvider>,
        schema_name: &str,
        provider_tables: &SourceTables,
    ) -> DataFusionResult<Self> {
        let planning_schema = MemorySchemaProvider::new();
        for (table_name, provider) in provider_tables {
            planning_schema.register_table(table_name.clone(), Arc::clone(provider))?;
        }
        let previous_schema = catalog.register_schema(schema_name, Arc::new(planning_schema))?;
        Ok(Self {
            catalog: Arc::clone(catalog),
            schema_name: schema_name.to_string(),
            previous_schema,
        })
    }

    fn restore(self) -> DataFusionResult<()> {
        match self.previous_schema {
            Some(previous_schema) => {
                self.catalog
                    .register_schema(&self.schema_name, previous_schema)?;
            }
            None => {
                drop(self.catalog.deregister_schema(&self.schema_name, true)?);
            }
        }
        Ok(())
    }
}

fn merge_source_view_tables(
    schema_name: &str,
    provider_tables: &mut SourceTables,
    view_tables: SourceTables,
) -> DataFusionResult<()> {
    for (table_name, provider) in view_tables {
        if provider_tables
            .insert(table_name.clone(), provider)
            .is_some()
        {
            return Err(DataFusionError::Plan(format!(
                "source view {schema_name}.{table_name} conflicts with an existing source table"
            )));
        }
    }
    Ok(())
}

fn publish_source_schema(
    catalog: &Arc<dyn CatalogProvider>,
    schema_name: &str,
    source_tables: SourceTables,
) -> DataFusionResult<()> {
    catalog.register_schema(
        schema_name,
        Arc::new(StaticSchemaProvider::new(source_tables)),
    )?;
    Ok(())
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

fn record_source_failure(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    result: &mut SourceRegistrationResult,
    source: &QuerySource,
    source_name: &str,
    schema_name: &str,
    error: CoreError,
) -> std::result::Result<(), CoreError> {
    if handle_source_registration_failure(source_decorators, source, &error)? {
        return Err(error);
    }
    push_source_failure(result, source_name, schema_name, error.to_string());
    Ok(())
}

fn register_table_functions(ctx: &SessionContext, table_functions: SourceTableFunctions) {
    for (internal_name, function) in table_functions {
        ctx.register_udtf(&internal_name, function);
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

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemorySchemaProvider;
    use datafusion::common::TableReference;
    use datafusion::datasource::MemTable;
    use datafusion::error::Result as DataFusionResult;
    use datafusion::prelude::SessionContext;
    use serde_json::json;

    use super::{
        CompiledQuerySource, SourceRegistrationCandidate, SourceRegistrationResult,
        check_reserved_schema, register_sources,
    };
    use crate::QuerySource;
    use crate::backends::common::{
        BackendRegistration, BackendRegistrationContext, CompiledBackendSource, RegisteredColumn,
        RegisteredSourceTable, RegisteredTable,
    };

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
        check_reserved_schema("github").expect("github is not reserved");
        check_reserved_schema("pagerduty").expect("pagerduty is not reserved");
        check_reserved_schema("slack").expect("slack is not reserved");
    }

    #[tokio::test]
    async fn source_registration_publishes_static_schema_with_views() {
        let ctx = SessionContext::new();
        let mut decorators = Vec::new();

        let registration = register_sources(
            &ctx,
            vec![SourceRegistrationCandidate::Compiled(
                compiled_query_source(TestCompiledSource::with_view(
                    "SELECT text FROM test_source.events",
                )),
            )],
            decorators.as_mut_slice(),
        )
        .await
        .expect("source registration should succeed");

        let active_source = registration
            .active_sources
            .first()
            .expect("source registration should report one active source");
        let table_names = active_source
            .tables
            .iter()
            .map(|table| table.table_name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(table_names, ["events", "messages"]);

        let rows = ctx
            .sql("SELECT text FROM test_source.messages")
            .await
            .expect("view query should plan")
            .collect()
            .await
            .expect("view query should execute");
        assert_single_text_value(&rows, "hello");

        let source_schema = ctx
            .catalog("datafusion")
            .expect("catalog should exist")
            .schema("test_source")
            .expect("source schema should exist");
        let error = source_schema
            .register_table("late_table".to_string(), empty_mem_table())
            .expect_err("published source schema should be static");

        assert!(
            error
                .to_string()
                .contains("static schema provider does not support register_table"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn invalid_source_view_does_not_publish_source_schema() {
        let ctx = SessionContext::new();
        let mut decorators = Vec::new();

        let registration = register_sources(
            &ctx,
            vec![SourceRegistrationCandidate::Compiled(
                compiled_query_source(TestCompiledSource::with_view(
                    "SELECT text FROM test_source.missing",
                )),
            )],
            decorators.as_mut_slice(),
        )
        .await
        .expect("invalid source should be skipped, not abort registration");

        assert_source_skipped_without_schema(
            &ctx,
            &registration,
            "failed to plan view test_source.messages",
        );
    }

    #[tokio::test]
    async fn invalid_source_view_restores_existing_schema() {
        let ctx = SessionContext::new();
        let catalog = ctx.catalog("datafusion").expect("catalog should exist");
        catalog
            .register_schema("test_source", Arc::new(MemorySchemaProvider::new()))
            .expect("existing schema should register");
        ctx.register_table(
            TableReference::partial("test_source", "existing"),
            event_mem_table(),
        )
        .expect("existing table should register");
        let mut decorators = Vec::new();

        let registration = register_sources(
            &ctx,
            vec![SourceRegistrationCandidate::Compiled(
                compiled_query_source(TestCompiledSource::with_view(
                    "SELECT text FROM test_source.missing",
                )),
            )],
            decorators.as_mut_slice(),
        )
        .await
        .expect("invalid source should be skipped, not abort registration");

        assert_source_skipped(&registration, "failed to plan view test_source.messages");
        let rows = ctx
            .sql("SELECT text FROM test_source.existing")
            .await
            .expect("restored schema query should plan")
            .collect()
            .await
            .expect("restored schema query should execute");
        assert_single_text_value(&rows, "hello");
    }

    #[tokio::test]
    async fn source_view_cannot_conflict_with_provider_table_name() {
        let ctx = SessionContext::new();
        let mut decorators = Vec::new();

        let registration = register_sources(
            &ctx,
            vec![SourceRegistrationCandidate::Compiled(
                compiled_query_source(TestCompiledSource::with_named_view(
                    "events",
                    "SELECT text FROM test_source.events",
                )),
            )],
            decorators.as_mut_slice(),
        )
        .await
        .expect("conflicting source view should be skipped, not abort registration");

        assert_source_skipped_without_schema(
            &ctx,
            &registration,
            "source view test_source.events conflicts with an existing source table",
        );
    }

    #[tokio::test]
    async fn duplicate_source_view_names_do_not_publish_source_schema() {
        let ctx = SessionContext::new();
        let mut decorators = Vec::new();

        let registration = register_sources(
            &ctx,
            vec![SourceRegistrationCandidate::Compiled(
                compiled_query_source(TestCompiledSource::with_views(vec![
                    TestSourceView::new("messages", "SELECT text FROM test_source.events"),
                    TestSourceView::new("messages", "SELECT text FROM test_source.events"),
                ])),
            )],
            decorators.as_mut_slice(),
        )
        .await
        .expect("duplicate source views should be skipped, not abort registration");

        assert_source_skipped_without_schema(
            &ctx,
            &registration,
            "invalid view test_source.messages: duplicate source view name",
        );
    }

    struct TestCompiledSource {
        views: Vec<TestSourceView>,
    }

    struct TestSourceView {
        name: &'static str,
        sql: &'static str,
    }

    impl TestSourceView {
        fn new(name: &'static str, sql: &'static str) -> Self {
            Self { name, sql }
        }
    }

    impl TestCompiledSource {
        fn with_view(view_sql: &'static str) -> Self {
            Self::with_named_view("messages", view_sql)
        }

        fn with_named_view(name: &'static str, sql: &'static str) -> Self {
            Self::with_views(vec![TestSourceView::new(name, sql)])
        }

        fn with_views(views: Vec<TestSourceView>) -> Self {
            Self { views }
        }
    }

    #[async_trait]
    impl CompiledBackendSource for TestCompiledSource {
        fn schema_name(&self) -> &'static str {
            "test_source"
        }

        fn source_name(&self) -> &'static str {
            "test_source"
        }

        async fn register(
            &self,
            _ctx: &SessionContext,
            _registration: &BackendRegistrationContext,
        ) -> DataFusionResult<BackendRegistration> {
            let mut tables = vec![RegisteredSourceTable::provider(
                registered_text_table("events"),
                event_mem_table(),
            )];
            tables.extend(self.views.iter().map(|view| {
                RegisteredSourceTable::sql_view(
                    registered_text_table(view.name),
                    view.sql.to_string(),
                )
            }));

            Ok(BackendRegistration::new(
                "test_source".to_string(),
                tables,
                HashMap::default(),
                vec![],
                vec![],
            ))
        }
    }

    fn compiled_query_source(compiled: TestCompiledSource) -> CompiledQuerySource {
        let manifest = coral_spec::parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "test_source",
            "version": "0.1.0",
            "backend": "file",
            "tables": [{
                "name": "events",
                "description": "Events",
                "format": "jsonl",
                "source": { "location": "file:///tmp/coral-test/events.jsonl" },
                "columns": [{ "name": "text", "type": "Utf8" }]
            }]
        }))
        .expect("test manifest should parse");

        CompiledQuerySource {
            source: QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new()),
            compiled: Box::new(compiled),
        }
    }

    fn registered_text_table(table_name: &str) -> RegisteredTable {
        RegisteredTable {
            table_name: table_name.to_string(),
            description: String::new(),
            guide: String::new(),
            columns: vec![RegisteredColumn {
                name: "text".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
                is_virtual: false,
                is_required_filter: false,
                filter_mode: None,
                description: String::new(),
            }],
            filters: vec![],
            required_filters: vec![],
            search_limits_json: None,
        }
    }

    fn event_mem_table() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["hello"])) as ArrayRef],
        )
        .expect("record batch should build");
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should build"))
    }

    fn empty_mem_table() -> Arc<MemTable> {
        Arc::new(
            MemTable::try_new(Arc::new(Schema::empty()), vec![vec![]])
                .expect("mem table should build"),
        )
    }

    fn assert_source_skipped_without_schema(
        ctx: &SessionContext,
        registration: &SourceRegistrationResult,
        expected_failure_detail: &str,
    ) {
        assert_source_skipped(registration, expected_failure_detail);

        let source_schema = ctx
            .catalog("datafusion")
            .expect("catalog should exist")
            .schema("test_source");
        assert!(
            source_schema.is_none(),
            "failed view registration must remove the planning schema"
        );
    }

    fn assert_source_skipped(
        registration: &SourceRegistrationResult,
        expected_failure_detail: &str,
    ) {
        assert!(registration.active_sources.is_empty());
        let failure = registration
            .failures
            .first()
            .expect("invalid source view should report one failure");
        assert!(
            failure.detail.contains(expected_failure_detail),
            "unexpected failure: {:?}",
            registration.failures
        );
    }

    fn assert_single_text_value(batches: &[RecordBatch], expected: &str) {
        let [batch] = batches else {
            panic!("expected one batch, got {}", batches.len());
        };
        assert_eq!(batch.num_rows(), 1, "expected one row");
        let text = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("first column should be a string array");
        assert_eq!(text.value(0), expected);
    }
}
