//! Concrete `DataFusion` runtime assembly for the data plane.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use datafusion_tracing::{InstrumentationOptions, RuleInstrumentationOptions};
use tokio::sync::OnceCell;
use tracing::{Instrument as _, info_span};

use crate::backends::compile_query_source;
use crate::backends::http::ProviderQueryError;
use crate::runtime::catalog;
use crate::runtime::dependent_join::error::resolver_rows_exceeded;
use crate::runtime::dependent_join::optimizer;
use crate::runtime::dependent_join::planner::DependentJoinExtensionPlanner;
use crate::runtime::error::{
    datafusion_to_core, datafusion_to_core_with_sql_and_table_functions,
    query_result_observer_error_to_core,
};
use crate::runtime::json::register_json_support;
use crate::runtime::pattern_validator::register_pattern_validator;
use crate::runtime::query_planner::CoralQueryPlanner;
use crate::runtime::registry::{
    CompiledQuerySource, SourceRegistrationCandidate, SourceRegistrationFailure, register_sources,
};
use crate::runtime::source_functions::SourceFunctionRegistry;
use crate::{
    CatalogInfo, CoreError, DependentJoinConfig, DescribeTableInfo, QueryExecution, QueryPlan,
    QueryResultObserver, QueryResultObserverError, QueryRuntimeConfig, QueryRuntimeContext,
    QuerySource, RequestAuthenticator, SourceDecorator, SourceInputResolver, TableFunctionInfo,
    TableInfo,
};

pub(crate) struct QueryRuntimeAdapter {
    ctx: Arc<SessionContext>,
    fallback_runtime: Option<FallbackRuntime>,
    tables: Vec<TableInfo>,
    table_functions: Vec<TableFunctionInfo>,
    failures: Vec<SourceRegistrationFailure>,
    query_result_observers: Vec<Arc<dyn QueryResultObserver>>,
}

struct FallbackRuntime {
    config: FallbackRuntimeConfig,
    runtime: OnceCell<RegisteredRuntime>,
}

#[derive(Clone)]
struct FallbackRuntimeConfig {
    sources: Vec<QuerySource>,
    runtime_context: QueryRuntimeContext,
    dependent_join: DependentJoinConfig,
    request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
}

struct RegisteredRuntime {
    ctx: Arc<SessionContext>,
    tables: Vec<TableInfo>,
    table_functions: Vec<TableFunctionInfo>,
    failures: Vec<SourceRegistrationFailure>,
}

enum SqlExecutionFailure {
    Planning(DataFusionError),
    Collection(DataFusionError),
    Observer(CoreError),
}

pub(crate) async fn build_runtime(
    sources: &[QuerySource],
    runtime: QueryRuntimeConfig,
) -> Result<QueryRuntimeAdapter, CoreError> {
    let span = info_span!("coral.engine.runtime.build", source.count = sources.len());
    build_runtime_inner(sources, runtime).instrument(span).await
}

async fn build_runtime_inner(
    sources: &[QuerySource],
    runtime: QueryRuntimeConfig,
) -> Result<QueryRuntimeAdapter, CoreError> {
    let QueryRuntimeConfig {
        context: runtime_context,
        dependent_join,
        mut extensions,
    } = runtime;
    let request_authenticators = extensions.request_authenticators.clone();
    let source_input_resolver = extensions.source_input_resolver.clone();
    // Resolver-row overflow can retry without the dependent-join optimizer only
    // when runtime registration is replayable. Source decorators are mutable
    // one-shot registration hooks today, so decorated runtimes keep resolver-row
    // overflow as a hard error instead of applying decorators a second time with
    // potentially different side effects.
    let fallback_without_dependent_join =
        dependent_join.optimizer_enabled() && extensions.source_decorators.is_empty();
    let fallback_runtime = fallback_without_dependent_join.then(|| {
        FallbackRuntime::new(FallbackRuntimeConfig {
            sources: sources.to_vec(),
            runtime_context: runtime_context.clone(),
            dependent_join: dependent_join.clone(),
            request_authenticators: request_authenticators.clone(),
            source_input_resolver: source_input_resolver.clone(),
        })
    });

    let primary = build_registered_runtime(
        sources,
        &runtime_context,
        &request_authenticators,
        source_input_resolver,
        extensions.source_decorators.as_mut_slice(),
        &dependent_join,
    )
    .await?;

    Ok(QueryRuntimeAdapter {
        ctx: primary.ctx,
        fallback_runtime,
        tables: primary.tables,
        table_functions: primary.table_functions,
        failures: primary.failures,
        query_result_observers: extensions.query_result_observers,
    })
}

async fn build_registered_runtime(
    sources: &[QuerySource],
    runtime_context: &QueryRuntimeContext,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    source_decorators: &mut [Box<dyn SourceDecorator>],
    dependent_join: &DependentJoinConfig,
) -> Result<RegisteredRuntime, CoreError> {
    let ctx = build_session_context(dependent_join)?;
    let registration = register_runtime_sources(
        &ctx,
        sources,
        runtime_context,
        request_authenticators,
        source_input_resolver,
        source_decorators,
    )
    .await?;
    catalog::register(&ctx, &registration.active_sources)
        .map_err(|err| datafusion_to_core(&err, &[]))?;
    let tables = catalog::collect_tables(&registration.active_sources);
    let table_functions = catalog::collect_table_functions(&registration.active_sources);
    let source_functions = SourceFunctionRegistry::new(
        registration
            .active_sources
            .iter()
            .flat_map(|source| source.table_functions.iter()),
    );
    if !source_functions.is_empty() {
        ctx.register_relation_planner(Arc::new(source_functions))
            .map_err(|err| datafusion_to_core(&err, &tables))?;
    }
    for failure in &registration.failures {
        tracing::warn!(
            source = %failure.schema_name,
            detail = %failure.detail,
            "skipping source during runtime build"
        );
    }

    Ok(RegisteredRuntime {
        ctx,
        tables,
        table_functions,
        failures: registration.failures,
    })
}

fn build_session_context(
    dependent_join: &DependentJoinConfig,
) -> Result<Arc<SessionContext>, CoreError> {
    let session_config = SessionConfig::new().with_information_schema(true).set_bool(
        "datafusion.execution.listing_table_ignore_subdirectory",
        false,
    );
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_object_list_cache_limit(0)
            .build()
            .map_err(|err| datafusion_to_core(&err, &[]))?,
    );
    let exec_options = InstrumentationOptions::builder()
        .record_metrics(true)
        .build();
    let instrument_rule = datafusion_tracing::instrument_with_trace_spans!(
        target: "coral_engine::datafusion",
        options: exec_options
    );
    let mut builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_default_features();
    if dependent_join.optimizer_enabled() {
        builder = builder.with_optimizer_rule(Arc::new(optimizer::rule(dependent_join.clone())));
    }
    let session_state = builder
        .with_query_planner(Arc::new(CoralQueryPlanner::new(vec![Arc::new(
            DependentJoinExtensionPlanner,
        )])))
        .with_physical_optimizer_rule(instrument_rule)
        .build();
    let session_state = datafusion_tracing::instrument_rules_with_trace_spans!(
        target: "coral_engine::datafusion",
        options: RuleInstrumentationOptions::full(),
        state: session_state
    );
    let mut ctx = SessionContext::new_with_state(session_state);
    register_json_support(&mut ctx).map_err(|err| datafusion_to_core(&err, &[]))?;
    register_pattern_validator(&mut ctx).map_err(|err| datafusion_to_core(&err, &[]))?;
    Ok(Arc::new(ctx))
}

async fn register_runtime_sources(
    ctx: &SessionContext,
    sources: &[QuerySource],
    runtime_context: &QueryRuntimeContext,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    source_decorators: &mut [Box<dyn SourceDecorator>],
) -> Result<crate::runtime::registry::SourceRegistrationResult, CoreError> {
    let mut source_candidates = Vec::new();
    for source in sources {
        match compile_query_source(
            source,
            runtime_context,
            request_authenticators,
            source_input_resolver.clone(),
        ) {
            Ok(compiled) => {
                source_candidates.push(SourceRegistrationCandidate::Compiled(
                    CompiledQuerySource {
                        source: source.clone(),
                        compiled,
                    },
                ));
            }
            Err(error) => source_candidates.push(SourceRegistrationCandidate::CompileFailed {
                source: source.clone(),
                error,
            }),
        }
    }
    register_sources(ctx, source_candidates, source_decorators).await
}

impl QueryRuntimeAdapter {
    pub(crate) fn list_tables(
        &self,
        source_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Vec<TableInfo> {
        self.tables
            .iter()
            .filter(|table| source_filter.is_none_or(|value| table.schema_name == value))
            .filter(|table| table_filter.is_none_or(|value| table.table_name == value))
            .cloned()
            .collect()
    }

    fn list_table_functions(
        &self,
        source_filter: Option<&str>,
        function_filter: Option<&str>,
    ) -> Vec<TableFunctionInfo> {
        self.table_functions
            .iter()
            .filter(|function| source_filter.is_none_or(|value| function.schema_name == value))
            .filter(|function| function_filter.is_none_or(|value| function.function_name == value))
            .cloned()
            .collect()
    }

    pub(crate) fn catalog_info(&self, source_filter: Option<&str>) -> CatalogInfo {
        CatalogInfo {
            tables: self.list_tables(source_filter, None),
            table_functions: self.list_table_functions(source_filter, None),
        }
    }

    pub(crate) fn describe_table(&self, schema_name: &str, table_name: &str) -> DescribeTableInfo {
        if let Some(table) = self
            .tables
            .iter()
            .find(|table| table.schema_name == schema_name && table.table_name == table_name)
            .cloned()
        {
            return DescribeTableInfo {
                table: Some(table),
                missing_context_tables: Vec::new(),
            };
        }

        let missing_context_tables = self
            .tables
            .iter()
            .map(table_metadata_without_columns)
            .collect();
        DescribeTableInfo {
            table: None,
            missing_context_tables,
        }
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
        match self.execute_sql_once(&self.ctx, sql).await {
            Ok(execution) => Ok(execution),
            Err(SqlExecutionFailure::Collection(error)) => {
                // Resolver-row overflow is a dependent-join buffering limit, not
                // a SQL correctness boundary. Retry the original query with only
                // the dependent-join rewrite disabled; binding fanout and
                // per-binding fetch caps remain hard execution errors.
                let Some(cap_error) = resolver_rows_exceeded(&error) else {
                    return Err(datafusion_to_core(&error, &self.tables));
                };
                let cap_core_error = datafusion_to_core(&error, &self.tables);
                let Some(fallback_runtime) = &self.fallback_runtime else {
                    return Err(cap_core_error);
                };

                tracing::warn!(
                    target = "coral_engine::dependent_join",
                    source = %cap_error.source_schema,
                    table = %cap_error.table,
                    observed = cap_error.observed,
                    cap = cap_error.cap,
                    disposition = "fallback",
                    "dependent join resolver row cap exceeded",
                );

                let fallback = fallback_runtime
                    .get_or_build_without_dependent_join()
                    .await?;

                match self.execute_sql_once(&fallback.ctx, sql).await {
                    Ok(execution) => Ok(execution),
                    Err(error) => {
                        if is_missing_required_filter_failure(&error) {
                            return Err(cap_core_error);
                        }
                        let fallback_error = self.sql_execution_failure_to_core(error, sql);
                        Err(fallback_error)
                    }
                }
            }
            Err(error) => Err(self.sql_execution_failure_to_core(error, sql)),
        }
    }

    async fn execute_sql_once(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<QueryExecution, SqlExecutionFailure> {
        let df = ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(SqlExecutionFailure::Planning)?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df
            .collect()
            .await
            .map_err(SqlExecutionFailure::Collection)?;
        self.observe_query_result(sql, arrow_schema.as_ref(), &batches)
            .map_err(SqlExecutionFailure::Observer)?;
        Ok(QueryExecution::new(arrow_schema, batches))
    }

    fn sql_execution_failure_to_core(&self, error: SqlExecutionFailure, sql: &str) -> CoreError {
        match error {
            SqlExecutionFailure::Planning(error) => {
                datafusion_to_core_with_sql_and_table_functions(
                    &error,
                    &self.tables,
                    &self.table_functions,
                    Some(sql),
                )
            }
            SqlExecutionFailure::Collection(error) => datafusion_to_core(&error, &self.tables),
            SqlExecutionFailure::Observer(error) => error,
        }
    }

    fn observe_query_result(
        &self,
        sql: &str,
        schema: &arrow::datatypes::Schema,
        batches: &[arrow::record_batch::RecordBatch],
    ) -> Result<(), CoreError> {
        for observer in &self.query_result_observers {
            observer
                .observe_result(sql, schema, batches)
                .map_err(|error| query_result_observer_error(observer.name(), &error))?;
        }
        Ok(())
    }

    pub(crate) async fn explain_sql(&self, sql: &str) -> Result<QueryPlan, CoreError> {
        let df = self.sql_dataframe(sql).await?;
        let unoptimized_logical_plan = df.logical_plan().display_indent_schema().to_string();
        let (session_state, logical_plan) = df.into_parts();
        let optimized_logical_plan = session_state
            .optimize(&logical_plan)
            .map_err(|err| datafusion_to_core(&err, &self.tables))?;
        let optimized_logical_plan_display =
            optimized_logical_plan.display_indent_schema().to_string();
        let physical_plan = session_state
            .query_planner()
            .create_physical_plan(&optimized_logical_plan, &session_state)
            .await
            .map_err(|err| datafusion_to_core(&err, &self.tables))?;
        let physical_plan = displayable(physical_plan.as_ref())
            .set_show_schema(true)
            .indent(true)
            .to_string();

        Ok(QueryPlan::new(
            unoptimized_logical_plan,
            optimized_logical_plan_display,
            physical_plan,
        ))
    }

    async fn sql_dataframe(&self, sql: &str) -> Result<DataFrame, CoreError> {
        self.ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(|err| {
                datafusion_to_core_with_sql_and_table_functions(
                    &err,
                    &self.tables,
                    &self.table_functions,
                    Some(sql),
                )
            })
    }
}

fn is_missing_required_filter_failure(error: &SqlExecutionFailure) -> bool {
    let SqlExecutionFailure::Collection(error) = error else {
        return false;
    };
    let DataFusionError::External(inner) = error.find_root() else {
        return false;
    };
    matches!(
        inner.downcast_ref::<ProviderQueryError>(),
        Some(ProviderQueryError::MissingRequiredFilter { .. })
    )
}

impl FallbackRuntimeConfig {
    async fn build_without_dependent_join(&self) -> Result<RegisteredRuntime, CoreError> {
        let mut source_decorators = Vec::new();
        build_registered_runtime(
            &self.sources,
            &self.runtime_context,
            &self.request_authenticators,
            self.source_input_resolver.clone(),
            source_decorators.as_mut_slice(),
            &self.dependent_join.without_rewrites(),
        )
        .await
    }
}

impl FallbackRuntime {
    fn new(config: FallbackRuntimeConfig) -> Self {
        Self {
            config,
            runtime: OnceCell::new(),
        }
    }

    async fn get_or_build_without_dependent_join(&self) -> Result<&RegisteredRuntime, CoreError> {
        self.runtime
            .get_or_try_init(|| async { self.config.build_without_dependent_join().await })
            .await
    }
}

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn table_metadata_without_columns(table: &TableInfo) -> TableInfo {
    TableInfo {
        schema_name: table.schema_name.clone(),
        table_name: table.table_name.clone(),
        description: table.description.clone(),
        guide: table.guide.clone(),
        columns: Vec::new(),
        required_filters: table.required_filters.clone(),
    }
}

fn query_result_observer_error(name: &str, error: &QueryResultObserverError) -> CoreError {
    let core = query_result_observer_error_to_core(error);
    match core {
        CoreError::InvalidInput(detail) => {
            CoreError::InvalidInput(format!("query result observer '{name}': {detail}"))
        }
        CoreError::FailedPrecondition(detail) => {
            CoreError::FailedPrecondition(format!("query result observer '{name}': {detail}"))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnInfo;

    fn adapter_with_table() -> QueryRuntimeAdapter {
        QueryRuntimeAdapter {
            ctx: Arc::new(SessionContext::new()),
            fallback_runtime: None,
            tables: vec![TableInfo {
                schema_name: "demo".to_string(),
                table_name: "events".to_string(),
                description: "Event rows".to_string(),
                guide: "Query event rows.".to_string(),
                columns: vec![ColumnInfo {
                    name: "event_id".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    is_virtual: false,
                    is_required_filter: false,
                    description: "Event ID".to_string(),
                    ordinal_position: 0,
                }],
                required_filters: vec!["owner".to_string()],
            }],
            table_functions: Vec::new(),
            failures: Vec::new(),
            query_result_observers: Vec::new(),
        }
    }

    #[test]
    fn describe_table_hit_returns_full_table_without_missing_context() {
        let result = adapter_with_table().describe_table("demo", "events");

        let table = result.table.expect("exact table");
        assert_eq!(table.columns.len(), 1);
        assert!(result.missing_context_tables.is_empty());
    }

    #[test]
    fn describe_table_miss_returns_columnless_context_tables() {
        let result = adapter_with_table().describe_table("demo", "missing");

        assert!(result.table.is_none());
        assert_eq!(result.missing_context_tables.len(), 1);
        let context_table = result
            .missing_context_tables
            .first()
            .expect("missing context table");
        assert!(context_table.columns.is_empty());
        assert_eq!(context_table.required_filters, ["owner".to_string()]);
    }
}
