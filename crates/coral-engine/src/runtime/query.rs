//! Concrete `DataFusion` runtime assembly for the data plane.

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use datafusion_tracing::{InstrumentationOptions, RuleInstrumentationOptions};
use tracing::{Instrument as _, info_span};

use crate::backends::compile_query_source;
use crate::runtime::catalog;
use crate::runtime::error::{
    datafusion_to_core, datafusion_to_core_with_sql_and_table_functions,
    query_result_observer_error_to_core,
};
use crate::runtime::json::register_json_support;
use crate::runtime::pattern_validator::register_pattern_validator;
use crate::runtime::registry::{
    CompiledQuerySource, SourceRegistrationCandidate, SourceRegistrationFailure, register_sources,
};
use crate::runtime::source_functions::SourceFunctionRegistry;
use crate::{
    CatalogInfo, CoreError, DescribeTableInfo, QueryExecution, QueryPlan, QueryResultObserver,
    QueryResultObserverError, QueryRuntimeConfig, QuerySource, TableFunctionInfo, TableInfo,
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
    let span = info_span!("coral.engine.runtime.build", source.count = sources.len());
    build_runtime_inner(sources, runtime).instrument(span).await
}

async fn build_runtime_inner(
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
