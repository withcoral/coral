//! Concrete `DataFusion` runtime assembly for the data plane.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{ScalarValue, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan};
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
use crate::runtime::registration_cache::RegistrationCache;
use crate::runtime::registry::{
    CompiledQuerySource, SourceRegistrationCandidate, SourceRegistrationFailure, register_sources,
};
use crate::runtime::source_functions::{
    SOURCE_FUNCTION_NODE_NAME, SourceFunctionNode, SourceFunctionRegistry,
};
use crate::{
    CatalogInfo, CoreError, DependentJoinConfig, DescribeTableInfo, MemorySize, QueryExecution,
    QueryExecutionProvenance, QueryMemoryConfig, QueryParameterValue, QueryParameters, QueryPlan,
    QueryResultObserver, QueryResultObserverError, QueryRuntimeConfig, QueryRuntimeContext,
    QuerySource, QueryTableFunctionUsage, QueryTableUsage, RequestAuthenticator, SourceDecorator,
    SourceInputResolver, TableFunctionInfo, TableInfo,
};

pub(crate) struct QueryRuntimeAdapter {
    ctx: Arc<SessionContext>,
    fallback_runtime: Option<FallbackRuntime>,
    memory: QueryMemoryConfig,
    tables: Vec<TableInfo>,
    table_functions: Vec<TableFunctionInfo>,
    failures: Vec<SourceRegistrationFailure>,
    schema_to_source: HashMap<String, String>,
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
    memory: QueryMemoryConfig,
    request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    registration_cache: Option<Arc<RegistrationCache>>,
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
        memory,
        dependent_join,
        mut extensions,
    } = runtime;
    let request_authenticators = extensions.request_authenticators.clone();
    let source_input_resolver = extensions.source_input_resolver.clone();
    let registration_cache = extensions.registration_cache.clone();
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
            memory: memory.clone(),
            request_authenticators: request_authenticators.clone(),
            source_input_resolver: source_input_resolver.clone(),
            registration_cache: registration_cache.clone(),
        })
    });

    let primary = build_registered_runtime(
        sources,
        &runtime_context,
        &request_authenticators,
        source_input_resolver,
        extensions.source_decorators.as_mut_slice(),
        &dependent_join,
        &memory,
        registration_cache.as_deref(),
    )
    .await?;

    Ok(QueryRuntimeAdapter {
        ctx: primary.ctx,
        fallback_runtime,
        memory,
        tables: primary.tables,
        table_functions: primary.table_functions,
        failures: primary.failures,
        schema_to_source: schema_to_source_names(sources),
        query_result_observers: extensions.query_result_observers,
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "runtime build inputs are assembled once per query from already-destructured config"
)]
async fn build_registered_runtime(
    sources: &[QuerySource],
    runtime_context: &QueryRuntimeContext,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    source_decorators: &mut [Box<dyn SourceDecorator>],
    dependent_join: &DependentJoinConfig,
    memory: &QueryMemoryConfig,
    registration_cache: Option<&RegistrationCache>,
) -> Result<RegisteredRuntime, CoreError> {
    let ctx = build_session_context(dependent_join, memory)?;
    let registration = register_runtime_sources(
        &ctx,
        sources,
        runtime_context,
        request_authenticators,
        source_input_resolver,
        source_decorators,
        registration_cache,
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
        source_functions
            .install(&ctx)
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
    memory: &QueryMemoryConfig,
) -> Result<Arc<SessionContext>, CoreError> {
    let session_config = SessionConfig::new().with_information_schema(true).set_bool(
        "datafusion.execution.listing_table_ignore_subdirectory",
        false,
    );
    let mut runtime_env_builder = RuntimeEnvBuilder::new().with_object_list_cache_limit(0);
    if let Some(limit) = memory.limit {
        runtime_env_builder = runtime_env_builder.with_memory_limit(limit.as_bytes(), 1.0);
    }
    let runtime_env = Arc::new(
        runtime_env_builder
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
    registration_cache: Option<&RegistrationCache>,
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
    register_sources(
        ctx,
        source_candidates,
        source_decorators,
        registration_cache,
    )
    .await
}

/// Matches a user-supplied source/schema filter against one table.
///
/// Database sources expose three-part names, so a table is addressable by its
/// source schema (`coral_db`) or the dotted schema-namespace combination
/// (`coral_db.main`). Tables without a namespace reduce to the plain
/// schema-name match.
fn table_schema_matches(schema_name: &str, namespace: &str, value: &str) -> bool {
    if schema_name == value {
        return true;
    }
    !namespace.is_empty()
        && value
            .strip_prefix(schema_name)
            .and_then(|rest| rest.strip_prefix('.'))
            .is_some_and(|rest| rest == namespace)
}

impl QueryRuntimeAdapter {
    pub(crate) fn list_tables(
        &self,
        source_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Vec<TableInfo> {
        self.tables
            .iter()
            .filter(|table| {
                source_filter.is_none_or(|value| {
                    table_schema_matches(&table.schema_name, &table.namespace, value)
                })
            })
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

    pub(crate) fn catalog_info_for_schemas(&self, schema_filters: &[&str]) -> CatalogInfo {
        CatalogInfo {
            tables: self
                .tables
                .iter()
                .filter(|table| {
                    schema_filters.iter().any(|schema| {
                        table_schema_matches(&table.schema_name, &table.namespace, schema)
                    })
                })
                .cloned()
                .collect(),
            table_functions: self
                .table_functions
                .iter()
                .filter(|function| {
                    schema_filters
                        .iter()
                        .any(|schema| function.schema_name == *schema)
                })
                .cloned()
                .collect(),
        }
    }

    pub(crate) fn describe_table(&self, schema_name: &str, table_name: &str) -> DescribeTableInfo {
        if let Some(table) = self
            .tables
            .iter()
            .find(|table| {
                table_schema_matches(&table.schema_name, &table.namespace, schema_name)
                    && table.table_name == table_name
            })
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

    pub(crate) async fn execute_sql(
        &self,
        sql: &str,
        params: &QueryParameters,
    ) -> Result<QueryExecution, CoreError> {
        match self.execute_sql_once(&self.ctx, sql, params).await {
            Ok(execution) => Ok(execution),
            Err(SqlExecutionFailure::Collection(error)) => {
                // Resolver-row overflow is a dependent-join buffering limit, not
                // a SQL correctness boundary. Retry the original query with only
                // the dependent-join rewrite disabled; binding fanout and
                // per-binding fetch caps remain hard execution errors.
                let Some(cap_error) = resolver_rows_exceeded(&error) else {
                    return Err(self.collection_error_to_core(&error));
                };
                let cap_core_error = self.collection_error_to_core(&error);
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

                match self.execute_sql_once(&fallback.ctx, sql, params).await {
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
        params: &QueryParameters,
    ) -> Result<QueryExecution, SqlExecutionFailure> {
        let df = ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(SqlExecutionFailure::Planning)?;
        let df = apply_query_parameters(df, params).map_err(SqlExecutionFailure::Planning)?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let provenance = self
            .query_provenance(sql, df.logical_plan())
            .map_err(SqlExecutionFailure::Planning)?;
        let batches = df
            .collect()
            .await
            .map_err(SqlExecutionFailure::Collection)?;
        let execution = QueryExecution::new(arrow_schema, batches, provenance);
        self.observe_query_result(
            sql,
            execution.arrow_schema().as_ref(),
            execution.batches(),
            execution.provenance(),
        )
        .map_err(SqlExecutionFailure::Observer)?;
        Ok(execution)
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
            SqlExecutionFailure::Collection(error) => self.collection_error_to_core(&error),
            SqlExecutionFailure::Observer(error) => error,
        }
    }

    fn collection_error_to_core(&self, error: &DataFusionError) -> CoreError {
        if let Some(limit) = self.memory.limit
            && let Some(error) = memory_budget_error(error, limit)
        {
            return error;
        }
        datafusion_to_core(error, &self.tables)
    }

    fn observe_query_result(
        &self,
        sql: &str,
        schema: &arrow::datatypes::Schema,
        batches: &[arrow::record_batch::RecordBatch],
        provenance: &QueryExecutionProvenance,
    ) -> Result<(), CoreError> {
        for observer in &self.query_result_observers {
            observer
                .observe_result(sql, schema, batches, provenance)
                .map_err(|error| query_result_observer_error(observer.name(), &error))?;
        }
        Ok(())
    }

    fn query_provenance(
        &self,
        sql: &str,
        plan: &LogicalPlan,
    ) -> Result<QueryExecutionProvenance, DataFusionError> {
        let mut tables = BTreeSet::new();
        let mut table_functions = BTreeSet::new();
        plan.apply_with_subqueries(|node| {
            match node {
                LogicalPlan::TableScan(scan) => {
                    self.collect_table_scan_usage(&scan.table_name, &mut tables);
                }
                LogicalPlan::Extension(extension) => {
                    if let Some(function) =
                        extension.node.as_any().downcast_ref::<SourceFunctionNode>()
                    {
                        self.collect_table_function_usage(
                            function.table_reference(),
                            &mut table_functions,
                        );
                    }
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut sources = BTreeSet::new();
        sources.extend(tables.iter().map(|usage| usage.source_name().to_string()));
        sources.extend(
            table_functions
                .iter()
                .map(|usage| usage.source_name().to_string()),
        );

        Ok(QueryExecutionProvenance::new(
            sql,
            sources.into_iter().collect(),
            tables.into_iter().collect(),
            table_functions.into_iter().collect(),
        ))
    }

    fn collect_table_scan_usage(
        &self,
        table_reference: &TableReference,
        tables: &mut BTreeSet<QueryTableUsage>,
    ) {
        let Some((schema_name, table_name)) = relation_parts(table_reference) else {
            return;
        };
        // Three-part references put the source schema in the catalog slot and
        // the inner namespace in the schema slot.
        let catalog_name = table_reference.catalog();
        if self.tables.iter().any(|table| match catalog_name {
            Some(catalog) => {
                table.schema_name == catalog
                    && table.namespace == schema_name
                    && table.table_name == table_name
            }
            None => {
                table.schema_name == schema_name
                    && table.namespace.is_empty()
                    && table.table_name == table_name
            }
        }) {
            tables.insert(QueryTableUsage::new(
                self.source_name_for_schema(catalog_name.unwrap_or(schema_name)),
                schema_name,
                table_name,
            ));
        }
    }

    fn collect_table_function_usage(
        &self,
        table_reference: &TableReference,
        table_functions: &mut BTreeSet<QueryTableFunctionUsage>,
    ) -> bool {
        let Some((schema_name, function_name)) = relation_parts(table_reference) else {
            return false;
        };
        if self.table_functions.iter().any(|function| {
            function.schema_name == schema_name && function.function_name == function_name
        }) {
            table_functions.insert(QueryTableFunctionUsage::new(
                self.source_name_for_schema(schema_name),
                schema_name,
                function_name,
            ));
            return true;
        }
        false
    }

    fn source_name_for_schema(&self, schema_name: &str) -> String {
        self.schema_to_source
            .get(schema_name)
            .cloned()
            .unwrap_or_else(|| schema_name.to_string())
    }

    pub(crate) async fn explain_sql(
        &self,
        sql: &str,
        params: &QueryParameters,
    ) -> Result<QueryPlan, CoreError> {
        let df = self.sql_dataframe(sql, params).await?;
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

    async fn sql_dataframe(
        &self,
        sql: &str,
        params: &QueryParameters,
    ) -> Result<DataFrame, CoreError> {
        let df = self
            .ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(|err| {
                datafusion_to_core_with_sql_and_table_functions(
                    &err,
                    &self.tables,
                    &self.table_functions,
                    Some(sql),
                )
            })?;
        apply_query_parameters(df, params).map_err(|err| {
            datafusion_to_core_with_sql_and_table_functions(
                &err,
                &self.tables,
                &self.table_functions,
                Some(sql),
            )
        })
    }
}

/// Binds named query parameter values into a planned statement.
///
/// Rejects parameters the statement never references, then substitutes the
/// values into the logical plan via `DataFusion` parameter binding — values
/// stay data and are never rendered into SQL text.
fn apply_query_parameters(
    df: DataFrame,
    params: &QueryParameters,
) -> Result<DataFrame, DataFusionError> {
    if params.is_empty() {
        reject_unbound_sql_parameters(df.logical_plan())?;
        return Ok(df);
    }
    reject_unknown_parameters(df.logical_plan(), params)?;
    let values: Vec<(String, ScalarValue)> = params
        .iter()
        .map(|(name, value)| (name.clone(), query_parameter_scalar_value(value)))
        .collect();
    df.with_param_values(values)
}

fn reject_unbound_sql_parameters(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    let mut placeholders = ordinary_sql_placeholders(plan)?;
    if placeholders.is_empty() {
        return Ok(());
    }
    let placeholder = placeholders
        .pop_first()
        .expect("empty placeholder set returned above");
    Err(DataFusionError::Plan(format!(
        "SQL parameter {placeholder} has no value; pass query parameters or remove the placeholder"
    )))
}

fn ordinary_sql_placeholders(plan: &LogicalPlan) -> Result<BTreeSet<String>, DataFusionError> {
    let mut placeholders = BTreeSet::new();
    plan.apply_with_subqueries(|node| {
        if is_parameterized_source_function_call(node) {
            return Ok(TreeNodeRecursion::Jump);
        }
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Expr::Placeholder(placeholder) = expr {
                    placeholders.insert(placeholder.id.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(placeholders)
}

fn is_parameterized_source_function_call(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Extension(extension) = plan else {
        return false;
    };
    extension.node.name() == SOURCE_FUNCTION_NODE_NAME
}

fn query_parameter_scalar_value(value: &QueryParameterValue) -> ScalarValue {
    match value {
        QueryParameterValue::String(value) => ScalarValue::Utf8(value.clone()),
        QueryParameterValue::Integer(value) => ScalarValue::Int64(*value),
        QueryParameterValue::Float(value) => ScalarValue::Float64(*value),
        QueryParameterValue::Boolean(value) => ScalarValue::Boolean(*value),
    }
}

fn reject_unknown_parameters(
    plan: &LogicalPlan,
    params: &QueryParameters,
) -> Result<(), DataFusionError> {
    let referenced = plan.get_parameter_names()?;
    let mut unknown: Vec<&str> = params
        .keys()
        .map(String::as_str)
        .filter(|name| !referenced.contains(&format!("${name}")))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort_unstable();

    let mut placeholders: Vec<&str> = referenced.iter().map(String::as_str).collect();
    placeholders.sort_unstable();
    let placeholder_hint = if placeholders.is_empty() {
        "the statement has no parameter placeholders".to_string()
    } else {
        format!("the statement references: {}", placeholders.join(", "))
    };

    Err(DataFusionError::Plan(format!(
        "unknown query parameter(s): {}; {placeholder_hint}",
        unknown.join(", ")
    )))
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

fn memory_budget_error(error: &DataFusionError, limit: MemorySize) -> Option<CoreError> {
    let DataFusionError::ResourcesExhausted(detail) = error.find_root() else {
        return None;
    };

    Some(CoreError::Unavailable(format!(
        "query engine memory budget exceeded ([engine.memory].limit = {}). The query was aborted. \
         Increase [engine.memory].limit in config.toml, narrow the query, or reduce source rows. \
         Underlying engine error: {detail}",
        format_memory_limit(limit),
    )))
}

fn format_memory_limit(limit: MemorySize) -> String {
    let bytes = limit.as_bytes();
    for (suffix, multiplier) in [
        ("Ti", 1024_usize.pow(4)),
        ("Gi", 1024_usize.pow(3)),
        ("Mi", 1024_usize.pow(2)),
        ("Ki", 1024),
    ] {
        if bytes.is_multiple_of(multiplier) {
            return format!("{}{} ({} bytes)", bytes / multiplier, suffix, bytes);
        }
    }
    format!("{bytes} bytes")
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
            &self.memory,
            self.registration_cache.as_deref(),
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

fn schema_to_source_names(sources: &[QuerySource]) -> HashMap<String, String> {
    sources
        .iter()
        .flat_map(|source| {
            let source_name = source.source_name().to_string();
            source
                .schema_names()
                .into_iter()
                .map(move |schema_name| (schema_name.to_string(), source_name.clone()))
        })
        .collect()
}

fn relation_parts(table_reference: &TableReference) -> Option<(&str, &str)> {
    let schema_name = table_reference.schema()?;
    Some((schema_name, table_reference.table()))
}

fn table_metadata_without_columns(table: &TableInfo) -> TableInfo {
    TableInfo {
        schema_name: table.schema_name.clone(),
        namespace: table.namespace.clone(),
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
    use std::collections::HashMap;
    use std::str::FromStr as _;

    use datafusion::execution::memory_pool::MemoryConsumer;

    use super::*;
    use crate::{
        ColumnInfo, DependentJoinConfig, MemorySize, QueryMemoryConfig, QueryRuntimeContext,
    };

    fn adapter_with_table() -> QueryRuntimeAdapter {
        QueryRuntimeAdapter {
            ctx: Arc::new(SessionContext::new()),
            fallback_runtime: None,
            memory: QueryMemoryConfig::default(),
            tables: vec![TableInfo {
                schema_name: "demo".to_string(),
                namespace: String::new(),
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
            schema_to_source: HashMap::from([("demo".to_string(), "demo".to_string())]),
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

    #[test]
    fn build_session_context_applies_memory_limit() {
        let ctx = build_session_context(
            &DependentJoinConfig::default(),
            &QueryMemoryConfig {
                limit: Some(MemorySize::from_str("1Ki").unwrap()),
            },
        )
        .expect("session context should build");
        let pool = ctx.runtime_env().memory_pool.clone();
        let reservation = MemoryConsumer::new("test").register(&pool);

        reservation
            .try_grow(512)
            .expect("reservation below limit should succeed");
        let error = reservation
            .try_grow(1024)
            .expect_err("reservation above limit should fail");

        assert!(
            error.to_string().contains("Resources exhausted"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn fallback_runtime_preserves_memory_limit() {
        let fallback = FallbackRuntimeConfig {
            sources: Vec::new(),
            runtime_context: QueryRuntimeContext::default(),
            dependent_join: DependentJoinConfig::default(),
            memory: QueryMemoryConfig {
                limit: Some(MemorySize::from_str("1Ki").unwrap()),
            },
            request_authenticators: HashMap::new(),
            source_input_resolver: None,
            registration_cache: None,
        };

        let runtime = fallback
            .build_without_dependent_join()
            .await
            .expect("fallback runtime should build");
        let pool = runtime.ctx.runtime_env().memory_pool.clone();
        let reservation = MemoryConsumer::new("fallback-test").register(&pool);

        reservation
            .try_grow(512)
            .expect("reservation below fallback limit should succeed");
        let error = reservation
            .try_grow(1024)
            .expect_err("reservation above fallback limit should fail");

        assert!(
            error.to_string().contains("Resources exhausted"),
            "unexpected error: {error}"
        );
    }
}
