//! Concrete `DataFusion` runtime assembly for the data plane.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use coral_spec::ManifestDataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{ScalarValue, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::Analyzer as DataFusionAnalyzer;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use datafusion_tracing::{InstrumentationOptions, RuleInstrumentationOptions};
use tokio::sync::OnceCell;
use tracing::{Instrument as _, info_span};

use crate::backends::http::ProviderQueryError;
use crate::backends::{CatalogColumnFetcher, RegisteredSource, compile_query_source};
use crate::runtime::catalog;
use crate::runtime::dependent_join::error::resolver_rows_exceeded;
use crate::runtime::dependent_join::optimizer;
use crate::runtime::dependent_join::planner::DependentJoinExtensionPlanner;
use crate::runtime::error::{
    datafusion_to_core, datafusion_to_core_with_sql_and_table_functions, missing_table_reference,
    query_result_observer_error_to_core,
};
use crate::runtime::json::register_json_support;
use crate::runtime::non_default_catalog_name;
use crate::runtime::pattern_validator::register_pattern_validator;
use crate::runtime::query_planner::CoralQueryPlanner;
use crate::runtime::registry::{
    CompiledQuerySource, SourceRegistrationCandidate, SourceRegistrationFailure, register_sources,
};
use crate::runtime::scoped_table_functions::ScopedTableFunctionName;
use crate::runtime::source_functions::{
    SOURCE_FUNCTION_NODE_NAME, SourceFunctionNode, SourceFunctionRegistry,
};
use crate::runtime::udf_calls::{
    UDF_CALL_NODE_NAME, UdfCallAnalyzerRule, UdfCallNode, UdfCallRegistry,
};
use crate::runtime::udfs::published_table_functions;
use crate::{
    CatalogInfo, CoreError, DependentJoinConfig, DescribeTableInfo, MemorySize, QueryExecution,
    QueryExecutionProvenance, QueryMemoryConfig, QueryParameterValue, QueryParameters, QueryPlan,
    QueryResultObserver, QueryResultObserverError, QueryRuntimeConfig, QueryRuntimeContext,
    QuerySource, QueryTableFunctionUsage, QueryTableUsage, RequestAuthenticator, SourceDecorator,
    SourceInputResolver, SourceObservationPublisher, TableFunctionInfo, TableInfo,
    UdfRuntimeDefinition,
};

pub(crate) struct QueryRuntimeAdapter {
    ctx: Arc<SessionContext>,
    fallback_runtime: Option<FallbackRuntime>,
    memory: QueryMemoryConfig,
    active_sources: Vec<RegisteredSource>,
    column_fetchers: Vec<CatalogColumnFetcher>,
    source_function_names: HashSet<ScopedTableFunctionName>,
    udfs_installed: bool,
    tables: Vec<TableInfo>,
    table_functions: Vec<TableFunctionInfo>,
    failures: Vec<SourceRegistrationFailure>,
    /// Source name keyed by top-level SQL name (schema for two-part sources,
    /// catalog for database sources).
    name_to_source: HashMap<String, String>,
    query_result_observers: Vec<Arc<dyn QueryResultObserver>>,
}

pub(crate) struct InferredSqlSignature {
    pub(crate) parameter_fields: HashMap<String, Option<FieldRef>>,
    pub(crate) declared_parameter_types: HashMap<String, ManifestDataType>,
    pub(crate) planned_schema: Arc<arrow::datatypes::Schema>,
}

struct FallbackRuntime {
    config: FallbackRuntimeConfig,
    runtime: OnceCell<RegisteredRuntime>,
}

#[derive(Clone)]
struct RuntimeExtensionHooks {
    request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    source_observation_publishers: Vec<Arc<dyn SourceObservationPublisher>>,
}

#[derive(Clone)]
struct FallbackRuntimeConfig {
    sources: Vec<QuerySource>,
    runtime_context: QueryRuntimeContext,
    dependent_join: DependentJoinConfig,
    memory: QueryMemoryConfig,
    udfs: Vec<UdfRuntimeDefinition>,
    extension_hooks: RuntimeExtensionHooks,
}

struct RegisteredRuntime {
    ctx: Arc<SessionContext>,
    active_sources: Vec<RegisteredSource>,
    column_fetchers: Vec<CatalogColumnFetcher>,
    source_function_names: HashSet<ScopedTableFunctionName>,
    tables: Vec<TableInfo>,
    table_functions: Vec<TableFunctionInfo>,
    failures: Vec<SourceRegistrationFailure>,
}

struct RuntimeBuildInputs<'a> {
    sources: &'a [QuerySource],
    runtime_context: &'a QueryRuntimeContext,
    extension_hooks: &'a RuntimeExtensionHooks,
    source_decorators: &'a mut [Box<dyn SourceDecorator>],
    dependent_join: &'a DependentJoinConfig,
    memory: &'a QueryMemoryConfig,
    udfs: &'a [UdfRuntimeDefinition],
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
    Box::pin(build_runtime_inner(sources, runtime).instrument(span)).await
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
        udfs,
    } = runtime;
    let extension_hooks = RuntimeExtensionHooks {
        request_authenticators: extensions.request_authenticators.clone(),
        source_input_resolver: extensions.source_input_resolver.clone(),
        source_observation_publishers: extensions.source_observation_publishers.clone(),
    };
    let udfs_installed = !udfs.is_empty();
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
            udfs: udfs.clone(),
            extension_hooks: extension_hooks.clone(),
        })
    });

    let primary = build_registered_runtime(RuntimeBuildInputs {
        sources,
        runtime_context: &runtime_context,
        extension_hooks: &extension_hooks,
        source_decorators: extensions.source_decorators.as_mut_slice(),
        dependent_join: &dependent_join,
        memory: &memory,
        udfs: &udfs,
    })
    .await?;

    Ok(QueryRuntimeAdapter {
        ctx: primary.ctx,
        fallback_runtime,
        memory,
        active_sources: primary.active_sources,
        column_fetchers: primary.column_fetchers,
        source_function_names: primary.source_function_names,
        udfs_installed,
        tables: primary.tables,
        table_functions: primary.table_functions,
        failures: primary.failures,
        name_to_source: name_to_source_names(sources),
        query_result_observers: extensions.query_result_observers,
    })
}

async fn build_registered_runtime(
    config: RuntimeBuildInputs<'_>,
) -> Result<RegisteredRuntime, CoreError> {
    let ctx = build_session_context(config.dependent_join, config.memory)?;
    let registration = register_runtime_sources(
        &ctx,
        config.sources,
        config.runtime_context,
        config.extension_hooks,
        config.source_decorators,
    )
    .await?;
    let source_functions = SourceFunctionRegistry::new(
        registration
            .active_sources
            .iter()
            .flat_map(|source| source.table_functions.iter()),
    );
    let source_function_names = source_functions.names();
    let udf_table_functions = published_table_functions(config.udfs, &source_function_names)
        .map_err(|err| datafusion_to_core(&err, &[]))?;
    catalog::register(
        &ctx,
        &registration.active_sources,
        &registration.column_fetchers,
        &udf_table_functions,
    )
    .map_err(|err| datafusion_to_core(&err, &[]))?;
    let tables = catalog::collect_static_tables(&registration.active_sources);
    let table_functions =
        catalog::collect_table_functions(&registration.active_sources, &udf_table_functions);
    install_table_function_call_planners(
        &ctx,
        source_functions,
        source_function_names.clone(),
        config.udfs,
        &tables,
    )
    .await?;
    for failure in &registration.failures {
        tracing::warn!(
            source = %failure.schema_name,
            detail = %failure.detail,
            "skipping source during runtime build"
        );
    }

    Ok(RegisteredRuntime {
        ctx,
        active_sources: registration.active_sources,
        column_fetchers: registration.column_fetchers,
        source_function_names,
        tables,
        table_functions,
        failures: registration.failures,
    })
}

async fn install_table_function_call_planners(
    ctx: &SessionContext,
    source_functions: SourceFunctionRegistry,
    source_table_function_names: HashSet<ScopedTableFunctionName>,
    udfs: &[UdfRuntimeDefinition],
    tables: &[TableInfo],
) -> Result<(), CoreError> {
    match (!source_functions.is_empty(), !udfs.is_empty()) {
        (false, false) => Ok(()),
        (true, false) => source_functions
            .install(ctx)
            .map_err(|err| datafusion_to_core(&err, tables)),
        (false, true) => {
            install_udf_call_planner(ctx, udfs, source_table_function_names, tables).await
        }
        (true, true) => {
            // UDF expansion can reveal source-function calls inside the UDF body.
            // Install source planning first, then run the source analyzer after UDF expansion.
            source_functions
                .install_relation_planner(ctx)
                .map_err(|err| datafusion_to_core(&err, tables))?;
            install_udf_call_planner(ctx, udfs, source_table_function_names, tables).await?;
            SourceFunctionRegistry::install_analyzer(ctx);
            Ok(())
        }
    }
}

async fn install_udf_call_planner(
    ctx: &SessionContext,
    udfs: &[UdfRuntimeDefinition],
    source_table_function_names: HashSet<ScopedTableFunctionName>,
    tables: &[TableInfo],
) -> Result<(), CoreError> {
    let udf_calls = Box::pin(UdfCallRegistry::new(ctx, udfs, source_table_function_names))
        .await
        .map_err(|err| datafusion_to_core(&err, tables))?;
    udf_calls
        .install(ctx)
        .map_err(|err| datafusion_to_core(&err, tables))
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
    let mut analyzer_rules = DataFusionAnalyzer::new().rules;
    analyzer_rules.insert(0, Arc::new(UdfCallAnalyzerRule));
    let mut builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_analyzer_rules(analyzer_rules)
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
    extension_hooks: &RuntimeExtensionHooks,
    source_decorators: &mut [Box<dyn SourceDecorator>],
) -> Result<crate::runtime::registry::SourceRegistrationResult, CoreError> {
    let mut source_candidates = Vec::new();
    for source in sources {
        match compile_query_source(
            source,
            runtime_context,
            &extension_hooks.request_authenticators,
            extension_hooks.source_input_resolver.clone(),
            &extension_hooks.source_observation_publishers,
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
    pub(crate) async fn install_udfs(
        &mut self,
        udfs: Vec<UdfRuntimeDefinition>,
    ) -> Result<(), CoreError> {
        if udfs.is_empty() {
            return Ok(());
        }
        if self.udfs_installed {
            return Err(CoreError::FailedPrecondition(
                "query runtime already has installed UDFs".to_string(),
            ));
        }
        if self
            .fallback_runtime
            .as_ref()
            .is_some_and(FallbackRuntime::is_built)
        {
            return Err(CoreError::FailedPrecondition(
                "cannot install UDFs after query execution has initialized the fallback runtime"
                    .to_string(),
            ));
        }

        let udf_table_functions = published_table_functions(&udfs, &self.source_function_names)
            .map_err(|err| datafusion_to_core(&err, &self.tables))?;
        let udf_calls = Box::pin(UdfCallRegistry::new(
            &self.ctx,
            &udfs,
            self.source_function_names.clone(),
        ))
        .await
        .map_err(|err| datafusion_to_core(&err, &self.tables))?;
        catalog::register(
            &self.ctx,
            &self.active_sources,
            &self.column_fetchers,
            &udf_table_functions,
        )
        .map_err(|err| datafusion_to_core(&err, &self.tables))?;
        udf_calls
            .install(&self.ctx)
            .map_err(|err| datafusion_to_core(&err, &self.tables))?;

        self.table_functions =
            catalog::collect_table_functions(&self.active_sources, &udf_table_functions);
        if let Some(fallback_runtime) = &mut self.fallback_runtime {
            fallback_runtime.config.udfs = udfs;
        }
        self.udfs_installed = true;
        Ok(())
    }

    pub(crate) async fn list_tables(
        &self,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, CoreError> {
        catalog::collect_tables(&self.ctx, catalog_filter, schema_filter, table_filter)
            .await
            .map_err(|err| datafusion_to_core(&err, &self.tables))
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

    pub(crate) async fn catalog_info(
        &self,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
    ) -> Result<CatalogInfo, CoreError> {
        // Catalog summaries never surface columns, so skip the coral.columns
        // expansion (and the remote database inventory fetches behind it).
        let tables =
            catalog::collect_table_metadata(&self.ctx, catalog_filter, schema_filter, None)
                .await
                .map_err(|err| datafusion_to_core(&err, &self.tables))?;
        Ok(CatalogInfo {
            tables,
            table_functions: if catalog_filter.is_none() {
                self.list_table_functions(schema_filter, None)
            } else {
                Vec::new()
            },
        })
    }

    /// Catalog metadata restricted to the sources publishing the given
    /// schema and catalog names. Schema names never match database-internal
    /// schemas, which are addressed through their catalog.
    pub(crate) async fn catalog_info_for_sources(
        &self,
        schema_names: &[&str],
        catalog_names: &[&str],
    ) -> Result<CatalogInfo, CoreError> {
        let mut tables = Vec::new();
        for schema_name in schema_names {
            tables.extend(self.list_tables(None, Some(schema_name), None).await?);
        }
        for catalog_name in catalog_names {
            tables.extend(self.list_tables(Some(catalog_name), None, None).await?);
        }
        tables.sort_by(|left, right| {
            (&left.catalog_name, &left.schema_name, &left.table_name).cmp(&(
                &right.catalog_name,
                &right.schema_name,
                &right.table_name,
            ))
        });
        tables.dedup_by(|left, right| {
            left.catalog_name == right.catalog_name
                && left.schema_name == right.schema_name
                && left.table_name == right.table_name
        });
        Ok(CatalogInfo {
            tables,
            table_functions: self
                .table_functions
                .iter()
                .filter(|function| schema_names.contains(&function.schema_name.as_str()))
                .cloned()
                .collect(),
        })
    }

    pub(crate) async fn describe_table(
        &self,
        catalog_name: Option<&str>,
        schema_name: &str,
        table_name: &str,
    ) -> Result<DescribeTableInfo, CoreError> {
        let matches = self
            .list_tables(catalog_name, Some(schema_name), Some(table_name))
            .await?;
        if matches.len() > 1 {
            let candidates = matches
                .iter()
                .map(Self::qualified_table_reference)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(CoreError::InvalidInput(format!(
                "table reference `{schema_name}.{table_name}` is ambiguous; \
                 qualify the catalog: {candidates}"
            )));
        }
        if let Some(table) = matches.into_iter().next() {
            return Ok(DescribeTableInfo {
                table: Some(table),
                missing_context_tables: Vec::new(),
            });
        }

        let missing_context_tables = catalog::collect_table_metadata(&self.ctx, None, None, None)
            .await
            .map_err(|err| datafusion_to_core(&err, &self.tables))?
            .iter()
            .map(table_metadata_without_columns)
            .collect();
        Ok(DescribeTableInfo {
            table: None,
            missing_context_tables,
        })
    }

    fn qualified_table_reference(table: &TableInfo) -> String {
        if table.catalog_name.is_empty() {
            format!("`{}.{}`", table.schema_name, table.table_name)
        } else {
            format!(
                "`{}.{}.{}`",
                table.catalog_name, table.schema_name, table.table_name
            )
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
                        let fallback_error = self.sql_execution_failure_to_core(error, sql).await;
                        Err(fallback_error)
                    }
                }
            }
            Err(error) => Err(self.sql_execution_failure_to_core(error, sql).await),
        }
    }

    pub(crate) async fn infer_sql_signature(
        &self,
        sql: &str,
    ) -> Result<InferredSqlSignature, CoreError> {
        let plan_error = |err| {
            datafusion_to_core_with_sql_and_table_functions(
                &err,
                &self.tables,
                &self.table_functions,
                Some(sql),
            )
        };
        let df = self
            .ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(plan_error)?;
        let mut parameter_fields = df
            .logical_plan()
            .get_parameter_fields()
            .map_err(plan_error)?;
        infer_cast_parameter_fields(df.logical_plan(), &mut parameter_fields)
            .map_err(plan_error)?;
        let mut declared_parameter_types = HashMap::new();
        infer_source_function_parameter_fields(
            df.logical_plan(),
            &mut parameter_fields,
            &mut declared_parameter_types,
        )
        .map_err(plan_error)?;

        Ok(InferredSqlSignature {
            parameter_fields,
            declared_parameter_types,
            planned_schema: Arc::new(df.logical_plan().schema().as_arrow().clone()),
        })
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
        let provenance = self
            .query_provenance(sql, df.logical_plan())
            .map_err(SqlExecutionFailure::Planning)?;
        let task_ctx = Arc::new(df.task_ctx());
        let physical_plan = df
            .create_physical_plan()
            .await
            .map_err(SqlExecutionFailure::Collection)?;
        let arrow_schema = physical_plan.schema();
        let batches = collect(physical_plan, task_ctx)
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

    async fn sql_execution_failure_to_core(
        &self,
        error: SqlExecutionFailure,
        sql: &str,
    ) -> CoreError {
        match error {
            SqlExecutionFailure::Planning(error) => {
                let tables = self.table_context_for_planning_error(&error, sql).await;
                datafusion_to_core_with_sql_and_table_functions(
                    &error,
                    &tables,
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

    async fn table_context_for_planning_error(
        &self,
        error: &DataFusionError,
        sql: &str,
    ) -> Vec<TableInfo> {
        let filters = missing_table_reference(error, Some(sql))
            .map(|reference| table_context_filters(reference.parts.as_slice()))
            .unwrap_or_default();
        if filters.is_empty() {
            return self.tables.clone();
        }
        match self.table_metadata_for_error_context(&filters).await {
            Ok(tables) => tables,
            Err(error) => {
                tracing::debug!(
                    detail = %error,
                    "failed to collect dynamic table metadata for error context"
                );
                self.tables.clone()
            }
        }
    }

    async fn table_metadata_for_error_context(
        &self,
        filters: &TableContextFilters,
    ) -> Result<Vec<TableInfo>, CoreError> {
        let mut tables = Vec::new();
        for (catalog_name, schema_name) in &filters.0 {
            tables.extend(
                catalog::collect_table_metadata(
                    &self.ctx,
                    catalog_name.as_deref(),
                    Some(schema_name),
                    None,
                )
                .await
                .map_err(|error| datafusion_to_core(&error, &self.tables))?,
            );
        }
        let mut seen = HashSet::new();
        tables.retain(|table| {
            seen.insert((
                table.catalog_name.clone(),
                table.schema_name.clone(),
                table.table_name.clone(),
            ))
        });
        Ok(tables)
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
        self.collect_plan_provenance(plan, &mut tables, &mut table_functions)?;

        let mut sources = BTreeSet::new();
        sources.extend(tables.iter().map(|usage| usage.source_name().to_string()));
        sources.extend(
            table_functions
                .iter()
                .filter(|usage| self.name_to_source.contains_key(usage.schema_name()))
                .map(|usage| usage.source_name().to_string()),
        );

        Ok(QueryExecutionProvenance::new(
            sql,
            sources.into_iter().collect(),
            tables.into_iter().collect(),
            table_functions.into_iter().collect(),
        ))
    }

    fn collect_plan_provenance(
        &self,
        plan: &LogicalPlan,
        tables: &mut BTreeSet<QueryTableUsage>,
        table_functions: &mut BTreeSet<QueryTableFunctionUsage>,
    ) -> Result<(), DataFusionError> {
        plan.apply_with_subqueries(|node| {
            match node {
                LogicalPlan::TableScan(scan) => {
                    self.collect_table_scan_usage(&scan.table_name, tables);
                }
                LogicalPlan::Extension(extension) => {
                    if let Some(function) =
                        extension.node.as_any().downcast_ref::<SourceFunctionNode>()
                    {
                        self.collect_table_function_usage(
                            function.table_reference(),
                            table_functions,
                        );
                    } else if let Some(function) =
                        extension.node.as_any().downcast_ref::<UdfCallNode>()
                    {
                        self.record_table_function_usage(
                            function.table_reference(),
                            table_functions,
                        );
                        self.collect_plan_provenance(
                            function.body_plan(),
                            tables,
                            table_functions,
                        )?;
                    }
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    fn collect_table_scan_usage(
        &self,
        table_reference: &TableReference,
        tables: &mut BTreeSet<QueryTableUsage>,
    ) {
        let Some((schema_name, table_name)) = relation_parts(table_reference) else {
            return;
        };
        let catalog_name = non_default_catalog_name(table_reference.catalog());
        if self.tables.iter().any(|table| {
            table.catalog_name == catalog_name.unwrap_or_default()
                && table.schema_name == schema_name
                && table.table_name == table_name
        }) {
            tables.insert(QueryTableUsage::new(
                self.source_name_for(catalog_name.unwrap_or(schema_name)),
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
            return self.record_table_function_usage(table_reference, table_functions);
        }
        false
    }

    fn record_table_function_usage(
        &self,
        table_reference: &TableReference,
        table_functions: &mut BTreeSet<QueryTableFunctionUsage>,
    ) -> bool {
        let Some((schema_name, function_name)) = relation_parts(table_reference) else {
            return false;
        };
        table_functions.insert(QueryTableFunctionUsage::new(
            self.source_name_for(schema_name),
            schema_name,
            function_name,
        ));
        true
    }

    /// Resolves the source owning a top-level SQL name (schema or catalog).
    fn source_name_for(&self, name: &str) -> String {
        self.name_to_source
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
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

fn infer_cast_parameter_fields(
    plan: &LogicalPlan,
    parameter_fields: &mut HashMap<String, Option<FieldRef>>,
) -> Result<(), DataFusionError> {
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                let (cast_expr, data_type) = match expr {
                    Expr::Cast(cast) => (cast.expr.as_ref(), cast.field.data_type().clone()),
                    Expr::TryCast(cast) => (cast.expr.as_ref(), cast.field.data_type().clone()),
                    _ => return Ok(TreeNodeRecursion::Continue),
                };
                let Expr::Placeholder(placeholder) = cast_expr else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                set_parameter_field(
                    parameter_fields,
                    &placeholder.id,
                    Arc::new(Field::new("", data_type, true)),
                )?;
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })?;
    Ok(())
}

fn infer_source_function_parameter_fields(
    plan: &LogicalPlan,
    parameter_fields: &mut HashMap<String, Option<FieldRef>>,
    declared_parameter_types: &mut HashMap<String, ManifestDataType>,
) -> Result<(), DataFusionError> {
    plan.apply_with_subqueries(|node| {
        let LogicalPlan::Extension(extension) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Some(function) = extension.node.as_any().downcast_ref::<SourceFunctionNode>() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        for (argument, expr) in function.declared_args_with_call_exprs() {
            let Expr::Placeholder(placeholder) = expr else {
                continue;
            };
            set_parameter_field(
                parameter_fields,
                &placeholder.id,
                Arc::new(Field::new(
                    "",
                    crate::types::arrow_data_type(argument.data_type),
                    true,
                )),
            )?;
            set_declared_parameter_type(
                declared_parameter_types,
                &placeholder.id,
                argument.data_type,
            )?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn set_declared_parameter_type(
    declared_parameter_types: &mut HashMap<String, ManifestDataType>,
    parameter: &str,
    data_type: ManifestDataType,
) -> Result<(), DataFusionError> {
    match declared_parameter_types.get(parameter) {
        Some(existing) if *existing != data_type => Err(DataFusionError::Plan(format!(
            "conflicting types for parameter {parameter}: {} and {}; use explicit casts so every use has one type",
            existing.as_manifest_str(),
            data_type.as_manifest_str()
        ))),
        Some(_) => Ok(()),
        None => {
            declared_parameter_types.insert(parameter.to_string(), data_type);
            Ok(())
        }
    }
}

fn set_parameter_field(
    parameter_fields: &mut HashMap<String, Option<FieldRef>>,
    parameter: &str,
    field: FieldRef,
) -> Result<(), DataFusionError> {
    match parameter_fields.get(parameter) {
        Some(Some(existing))
            if !parameter_types_are_compatible(existing.data_type(), field.data_type()) =>
        {
            Err(DataFusionError::Plan(format!(
                "conflicting types for parameter {parameter}: {} and {}; use explicit casts so every use has one type",
                existing.data_type(),
                field.data_type()
            )))
        }
        Some(Some(_)) => Ok(()),
        _ => {
            parameter_fields.insert(parameter.to_string(), Some(field));
            Ok(())
        }
    }
}

fn parameter_types_are_compatible(left: &DataType, right: &DataType) -> bool {
    if left == right {
        return true;
    }

    match (
        crate::types::manifest_data_type_for_arrow(left),
        crate::types::manifest_data_type_for_arrow(right),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
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
        if is_parameterized_table_function_call(node) {
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

fn is_parameterized_table_function_call(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Extension(extension) = plan else {
        return false;
    };
    matches!(
        extension.node.name(),
        SOURCE_FUNCTION_NODE_NAME | UDF_CALL_NODE_NAME
    )
}

pub(crate) fn query_parameter_scalar_value(value: &QueryParameterValue) -> ScalarValue {
    match value {
        QueryParameterValue::String(value) => ScalarValue::Utf8(value.clone()),
        QueryParameterValue::Integer(value) => ScalarValue::Int64(*value),
        QueryParameterValue::Float(value) => ScalarValue::Float64(*value),
        QueryParameterValue::Boolean(value) => ScalarValue::Boolean(*value),
        QueryParameterValue::Timestamp(value) => {
            ScalarValue::TimestampMicrosecond(*value, Some("+00:00".into()))
        }
    }
}

pub(crate) fn reject_unknown_parameters(
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
        let dependent_join = self.dependent_join.without_rewrites();
        build_registered_runtime(RuntimeBuildInputs {
            sources: &self.sources,
            runtime_context: &self.runtime_context,
            extension_hooks: &self.extension_hooks,
            source_decorators: source_decorators.as_mut_slice(),
            dependent_join: &dependent_join,
            memory: &self.memory,
            udfs: &self.udfs,
        })
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

    fn is_built(&self) -> bool {
        self.runtime.get().is_some()
    }
}

pub(crate) fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn name_to_source_names(sources: &[QuerySource]) -> HashMap<String, String> {
    sources
        .iter()
        .flat_map(|source| {
            let source_name = source.source_name().to_string();
            source
                .schema_names()
                .into_iter()
                .chain(source.catalog_names())
                .map(move |name| (name.to_string(), source_name.clone()))
        })
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq)]
struct TableContextFilters(Vec<(Option<String>, String)>);

impl TableContextFilters {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn table_context_filters(parts: &[String]) -> TableContextFilters {
    let body = match parts {
        [catalog, rest @ ..] if catalog == "datafusion" => rest,
        _ => parts,
    };
    match body {
        [] | [_] => TableContextFilters::default(),
        [schema_name, _table_name] => TableContextFilters(vec![(None, schema_name.clone())]),
        [catalog_name, schema @ .., _table_name] => {
            let schema_name = schema.join(".");
            TableContextFilters(vec![
                (None, format!("{catalog_name}.{schema_name}")),
                (Some(catalog_name.clone()), schema_name),
            ])
        }
    }
}

fn relation_parts(table_reference: &TableReference) -> Option<(&str, &str)> {
    let schema_name = table_reference.schema()?;
    Some((schema_name, table_reference.table()))
}

fn table_metadata_without_columns(table: &TableInfo) -> TableInfo {
    TableInfo {
        catalog_name: table.catalog_name.clone(),
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
    use std::collections::HashMap;
    use std::str::FromStr as _;

    use datafusion::execution::memory_pool::MemoryConsumer;

    use super::*;
    use crate::backends::common::RegisteredColumn;
    use crate::backends::{RegisteredTable, SourceQualifiedName};
    use crate::{
        DependentJoinConfig, MemorySize, QueryMemoryConfig, QueryRuntimeContext,
        UdfRuntimeImplementation, UdfRuntimePublish, UdfRuntimeResultColumn,
        UdfRuntimeTableFunctionPublish,
    };

    async fn adapter_with_table() -> QueryRuntimeAdapter {
        let active_sources = vec![RegisteredSource {
            qualified_name: SourceQualifiedName::Schema("demo".to_string()),
            tables: vec![RegisteredTable {
                schema_name: None,
                table_name: "events".to_string(),
                description: "Event rows".to_string(),
                guide: "Query event rows.".to_string(),
                columns: vec![RegisteredColumn {
                    name: "event_id".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    is_virtual: false,
                    is_required_filter: false,
                    filter_mode: None,
                    description: "Event ID".to_string(),
                }],
                filters: Vec::new(),
                required_filters: vec!["owner".to_string()],
                search_limits: None,
            }],
            table_functions: Vec::new(),
            inputs: Vec::new(),
        }];
        let ctx = Arc::new(SessionContext::new());
        catalog::register(&ctx, &active_sources, &[], &[]).expect("catalog should register");
        let tables = catalog::collect_static_tables(&active_sources);
        QueryRuntimeAdapter {
            ctx,
            fallback_runtime: None,
            memory: QueryMemoryConfig::default(),
            active_sources,
            column_fetchers: Vec::new(),
            source_function_names: HashSet::new(),
            udfs_installed: false,
            tables,
            table_functions: Vec::new(),
            failures: Vec::new(),
            name_to_source: HashMap::from([("demo".to_string(), "demo".to_string())]),
            query_result_observers: Vec::new(),
        }
    }

    #[tokio::test]
    async fn describe_table_hit_returns_full_table_without_missing_context() {
        let result = adapter_with_table()
            .await
            .describe_table(None, "demo", "events")
            .await
            .expect("describe table");

        let table = result.table.expect("exact table");
        assert_eq!(table.columns.len(), 1);
        assert!(result.missing_context_tables.is_empty());
    }

    #[tokio::test]
    async fn describe_table_miss_returns_columnless_context_tables() {
        let result = adapter_with_table()
            .await
            .describe_table(None, "demo", "missing")
            .await
            .expect("describe table miss");

        assert!(result.table.is_none());
        let context_table = result
            .missing_context_tables
            .iter()
            .find(|table| table.schema_name == "demo" && table.table_name == "events")
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
            udfs: Vec::new(),
            extension_hooks: RuntimeExtensionHooks {
                request_authenticators: HashMap::new(),
                source_input_resolver: None,
                source_observation_publishers: Vec::new(),
            },
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

    #[tokio::test]
    async fn late_udf_install_is_retained_by_fallback_runtime() {
        let mut runtime = build_runtime(&[], QueryRuntimeConfig::default())
            .await
            .expect("primary runtime");
        runtime
            .install_udfs(vec![UdfRuntimeDefinition {
                name: "constant_value".to_string(),
                description: "Returns one value".to_string(),
                arguments: Vec::new(),
                implementation: UdfRuntimeImplementation::CoralSql {
                    query: "select 1 as value".to_string(),
                },
                publish: UdfRuntimePublish {
                    table_function: UdfRuntimeTableFunctionPublish {
                        schema: "functions".to_string(),
                        name: "constant_value".to_string(),
                        description: "Returns one value".to_string(),
                    },
                },
                result_columns: vec![UdfRuntimeResultColumn {
                    name: "value".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                }],
            }])
            .await
            .expect("late UDF install");

        let fallback = runtime
            .fallback_runtime
            .as_ref()
            .expect("dependent join fallback")
            .get_or_build_without_dependent_join()
            .await
            .expect("fallback runtime");
        let batches = fallback
            .ctx
            .sql("select value from functions.constant_value()")
            .await
            .expect("plan fallback UDF query")
            .collect()
            .await
            .expect("execute fallback UDF query");

        assert_eq!(
            batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
    }
}
