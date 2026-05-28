//! Concrete `DataFusion` runtime assembly for the data plane.

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{LogicalPlan, Statement};
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use datafusion_tracing::{InstrumentationOptions, RuleInstrumentationOptions};

use crate::backends::compile_query_source;
use crate::runtime::catalog;
use crate::runtime::error::{
    datafusion_to_core, datafusion_to_core_with_sql, query_result_observer_error_to_core,
};
use crate::runtime::json::register_json_support;
use crate::runtime::pattern_validator::register_pattern_validator;
use crate::runtime::registry::{
    CompiledQuerySource, SourceRegistrationCandidate, SourceRegistrationFailure, register_sources,
};
use crate::runtime::source_functions::SourceFunctionRegistry;
use crate::{
    CatalogInfo, CoreError, PreparedStatementInfo, QueryExecution, QueryPlan, QueryResultObserver,
    QueryResultObserverError, QueryRuntimeConfig, QuerySource, TableFunctionInfo, TableInfo,
};

pub(crate) struct QueryRuntimeAdapter {
    ctx: Arc<SessionContext>,
    tables: Vec<TableInfo>,
    table_functions: Vec<TableFunctionInfo>,
    prepared_statements: Vec<PreparedStatementInfo>,
    failures: Vec<SourceRegistrationFailure>,
    query_result_observers: Vec<Arc<dyn QueryResultObserver>>,
}

pub(crate) async fn build_runtime(
    sources: &[QuerySource],
    runtime: QueryRuntimeConfig,
) -> Result<QueryRuntimeAdapter, CoreError> {
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
    let session_state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_default_features()
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
    let ctx = Arc::new(ctx);

    let QueryRuntimeConfig {
        context: runtime_context,
        mut extensions,
    } = runtime;
    let mut source_candidates = Vec::new();
    for source in sources {
        match compile_query_source(source, &runtime_context, &extensions.request_authenticators) {
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
    let registration = register_sources(
        &ctx,
        source_candidates,
        extensions.source_decorators.as_mut_slice(),
    )
    .await?;
    catalog::register(&ctx, &registration.active_sources)
        .map_err(|err| datafusion_to_core(&err, &[]))?;
    let tables = catalog::collect_tables(&registration.active_sources);
    let table_functions = catalog::collect_table_functions(&registration.active_sources);
    let prepared_statements = catalog::collect_prepared_statements(&registration.active_sources);
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
    register_prepared_statements(&ctx, &registration.active_sources, &tables).await?;

    Ok(QueryRuntimeAdapter {
        ctx,
        tables,
        table_functions,
        prepared_statements,
        failures: registration.failures,
        query_result_observers: extensions.query_result_observers,
    })
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
            prepared_statements: self.list_prepared_statements(source_filter),
        }
    }

    fn list_prepared_statements(&self, source_filter: Option<&str>) -> Vec<PreparedStatementInfo> {
        self.prepared_statements
            .iter()
            .filter(|statement| source_filter.is_none_or(|value| statement.schema_name == value))
            .cloned()
            .collect()
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
        let df = self.sql_dataframe(sql).await?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df
            .collect()
            .await
            .map_err(|err| datafusion_to_core(&err, &self.tables))?;
        self.observe_query_result(sql, arrow_schema.as_ref(), &batches)?;
        Ok(QueryExecution::new(arrow_schema, batches))
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
        let plan = self
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|err| datafusion_to_core_with_sql(&err, &self.tables, Some(sql)))?;
        sql_options_for_plan(&plan)
            .verify_plan(&plan)
            .map_err(|err| datafusion_to_core_with_sql(&err, &self.tables, Some(sql)))?;
        self.ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|err| datafusion_to_core_with_sql(&err, &self.tables, Some(sql)))
    }
}

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn execute_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(true)
}

fn sql_options_for_plan(plan: &LogicalPlan) -> SQLOptions {
    match plan {
        LogicalPlan::Statement(Statement::Execute(_)) => execute_sql_options(),
        _ => read_only_sql_options(),
    }
}

async fn register_prepared_statements(
    ctx: &SessionContext,
    active_sources: &[crate::backends::RegisteredSource],
    tables: &[TableInfo],
) -> Result<(), CoreError> {
    for statement in active_sources
        .iter()
        .flat_map(|source| source.prepared_statements.iter())
    {
        let sql = prepare_statement_sql(statement);
        ctx.sql_with_options(&sql, execute_sql_options())
            .await
            .map_err(|err| datafusion_to_core(&err, tables))?
            .collect()
            .await
            .map_err(|err| datafusion_to_core(&err, tables))?;
    }
    Ok(())
}

fn prepare_statement_sql(
    statement: &crate::backends::common::RegisteredPreparedStatement,
) -> String {
    let data_types = statement
        .arguments
        .iter()
        .map(|argument| prepared_statement_data_type(&argument.data_type))
        .collect::<Vec<_>>()
        .join(", ");
    if data_types.is_empty() {
        format!("PREPARE {} AS {}", statement.execute_name, statement.sql)
    } else {
        format!(
            "PREPARE {}({}) AS {}",
            statement.execute_name, data_types, statement.sql
        )
    }
}

fn prepared_statement_data_type(data_type: &str) -> &'static str {
    match data_type {
        "Int64" => "BIGINT",
        "Boolean" => "BOOLEAN",
        "Float64" => "DOUBLE",
        "Timestamp" => "TIMESTAMP",
        _ => "STRING",
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
