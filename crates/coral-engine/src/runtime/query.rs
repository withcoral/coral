//! Concrete `DataFusion` runtime assembly for the data plane.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use datafusion::sql::sqlparser::ast::{CreateTable, CreateTableOptions, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
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
    CatalogInfo, CoreError, QueryBatchExecution, QueryBatchResult, QueryExecution, QueryPlan,
    QueryResultObserver, QueryResultObserverError, QueryRuntimeConfig, QuerySource,
    TableFunctionInfo, TableInfo,
};

pub(crate) struct QueryRuntimeAdapter {
    ctx: Arc<SessionContext>,
    tables: Vec<TableInfo>,
    table_functions: Vec<TableFunctionInfo>,
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
        match compile_query_source(
            source,
            &runtime_context,
            &extensions.request_authenticators,
            extensions.source_input_resolver.clone(),
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

    Ok(QueryRuntimeAdapter {
        ctx,
        tables,
        table_functions,
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
        let df = self.sql_dataframe(sql).await?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df
            .collect()
            .await
            .map_err(|err| datafusion_to_core(&err, &self.tables))?;
        self.observe_query_result(sql, arrow_schema.as_ref(), &batches)?;
        Ok(QueryExecution::new(arrow_schema, batches))
    }

    pub(crate) async fn execute_sql_batch(
        &self,
        sql: &[String],
    ) -> Result<QueryBatchExecution, CoreError> {
        let mut results = Vec::new();
        for (position, statement_sql) in sql.iter().enumerate() {
            let statement = parse_batch_statement(statement_sql)?;
            match statement {
                BatchStatement::Query => {
                    let execution = self.execute_sql(statement_sql).await?;
                    results.push(QueryBatchResult::new(
                        position + 1,
                        statement_sql.clone(),
                        execution,
                    ));
                }
                BatchStatement::TempTableCtas(rewritten_sql) => {
                    self.execute_batch_ddl(&rewritten_sql).await?;
                }
            }
        }

        Ok(QueryBatchExecution::new(results))
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
            .map_err(|err| datafusion_to_core_with_sql(&err, &self.tables, Some(sql)))
    }

    async fn execute_batch_ddl(&self, sql: &str) -> Result<(), CoreError> {
        let batches = self
            .ctx
            .sql_with_options(sql, batch_ddl_sql_options())
            .await
            .map_err(|err| datafusion_to_core_with_sql(&err, &self.tables, Some(sql)))?
            .collect()
            .await
            .map_err(|err| datafusion_to_core_with_sql(&err, &self.tables, Some(sql)))?;
        if batches.iter().map(RecordBatch::num_rows).sum::<usize>() == 0 {
            Ok(())
        } else {
            Err(CoreError::InvalidInput(
                "batch DDL statements must not return rows".to_string(),
            ))
        }
    }
}

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn batch_ddl_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(true)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

enum BatchStatement {
    Query,
    TempTableCtas(String),
}

fn parse_batch_statement(sql: &str) -> Result<BatchStatement, CoreError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|error| {
        CoreError::InvalidInput(format!("invalid batch SQL statement: {error}"))
    })?;
    let [statement] = statements.as_slice() else {
        return Err(CoreError::InvalidInput(
            "batch SQL entries must each contain exactly one statement".to_string(),
        ));
    };

    match statement {
        Statement::Query(_) => Ok(BatchStatement::Query),
        Statement::CreateTable(create) => {
            let rewritten_sql = rewrite_temp_table_ctas(create.clone())?;
            Ok(BatchStatement::TempTableCtas(rewritten_sql))
        }
        _ => Err(CoreError::InvalidInput(
            "batch SQL only supports read-only queries and CREATE TEMP TABLE ... AS SELECT"
                .to_string(),
        )),
    }
}

fn rewrite_temp_table_ctas(mut create: CreateTable) -> Result<String, CoreError> {
    let supported = create.temporary
        && !create.external
        && !create.dynamic
        && create.global.is_none()
        && !create.transient
        && !create.volatile
        && !create.iceberg
        && create.query.is_some()
        && create.hive_formats.is_none()
        && matches!(&create.table_options, CreateTableOptions::None)
        && create.file_format.is_none()
        && create.location.is_none()
        && create.like.is_none()
        && create.clone.is_none()
        && create.version.is_none()
        && create.on_cluster.is_none()
        && create.primary_key.is_none()
        && create.order_by.is_none()
        && create.partition_by.is_none()
        && create.cluster_by.is_none()
        && create.clustered_by.is_none()
        && create.inherits.is_none();

    if !supported {
        return Err(CoreError::InvalidInput(
            "batch SQL only supports CREATE TEMP TABLE ... AS SELECT for temporary tables"
                .to_string(),
        ));
    }

    create.temporary = false;
    Ok(Statement::CreateTable(create).to_string())
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
    use super::{CoreError, QueryRuntimeConfig, build_runtime};

    #[tokio::test]
    async fn batch_temp_table_ctas_is_visible_to_later_statement() {
        let runtime = build_runtime(&[], QueryRuntimeConfig::default())
            .await
            .expect("runtime");
        let sql = vec![
            "create temp table t as select 1 as value".to_string(),
            "select * from t".to_string(),
        ];

        let execution = runtime
            .execute_sql_batch(&sql)
            .await
            .expect("batch should execute");

        assert_eq!(execution.results().len(), 1);
        let result = execution.results().first().expect("first result");
        assert_eq!(result.index(), 2);
        assert_eq!(result.execution().row_count(), 1);
    }

    #[tokio::test]
    async fn unary_execute_sql_still_rejects_temp_table_ddl() {
        let runtime = build_runtime(&[], QueryRuntimeConfig::default())
            .await
            .expect("runtime");

        let error = runtime
            .execute_sql("create temp table t as select 1 as value")
            .await
            .expect_err("unary temp table DDL should fail");

        assert!(
            error.to_string().contains("DDL not supported")
                || error.to_string().contains("Temporary tables not supported"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn batch_rejects_persistent_create_table() {
        let runtime = build_runtime(&[], QueryRuntimeConfig::default())
            .await
            .expect("runtime");
        let sql = vec!["create table t as select 1 as value".to_string()];

        let error = runtime
            .execute_sql_batch(&sql)
            .await
            .expect_err("persistent create table should fail");

        assert!(matches!(error, CoreError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn batch_rejects_dml() {
        let runtime = build_runtime(&[], QueryRuntimeConfig::default())
            .await
            .expect("runtime");
        let sql = vec!["delete from t".to_string()];

        let error = runtime
            .execute_sql_batch(&sql)
            .await
            .expect_err("DML should fail");

        assert!(matches!(error, CoreError::InvalidInput(_)));
    }
}
