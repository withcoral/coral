//! Concrete `DataFusion` runtime assembly for the data plane.

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::ExecutionPlan as PhysicalExecutionPlan;
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
    CatalogInfo, CoreError, QueryExecution, QueryPlan, QueryResultObserver,
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
    let session_config = SessionConfig::new().with_information_schema(true);
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
        let execution_plan = build_execution_plan(physical_plan.as_ref());

        Ok(QueryPlan::new(
            unoptimized_logical_plan,
            optimized_logical_plan_display,
            physical_plan,
            Some(execution_plan),
        ))
    }

    async fn sql_dataframe(&self, sql: &str) -> Result<DataFrame, CoreError> {
        self.ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(|err| datafusion_to_core_with_sql(&err, &self.tables, Some(sql)))
    }
}

fn build_execution_plan(plan: &dyn PhysicalExecutionPlan) -> crate::ExecutionPlan {
    let mut pushdowns = Vec::new();
    let mut cache = Vec::new();
    let steps = build_execution_steps(plan, Vec::new(), &mut pushdowns, &mut cache);
    crate::ExecutionPlan::new(
        vec![steps],
        pushdowns,
        cache,
        estimated_rows(plan),
        None,
    )
}

fn build_execution_steps(
    plan: &dyn PhysicalExecutionPlan,
    step_path: Vec<u32>,
    pushdowns: &mut Vec<crate::PushdownDecision>,
    cache: &mut Vec<crate::CacheDecision>,
) -> crate::ExecutionPlanStep {
    let kind = classify_step_kind(plan.name());
    let detail = step_detail(plan);
    if let Some(scan) = scan_metadata(plan) {
        if !scan.pushdowns.is_empty() {
            pushdowns.extend(scan.pushdowns.iter().cloned().map(|predicate| {
                crate::PushdownDecision::new(
                    step_path.clone(),
                    scan.target.clone(),
                    predicate,
                    true,
                    scan.pushdown_detail.clone(),
                )
            }));
        }
        if !scan.cache.is_empty() {
            cache.extend(scan.cache.iter().cloned().map(|entry| {
                crate::CacheDecision::new(
                    step_path.clone(),
                    scan.target.clone(),
                    entry.strategy,
                    entry.status,
                    entry.detail,
                )
            }));
        }
        return crate::ExecutionPlanStep::new(
            kind,
            plan.name(),
            detail.unwrap_or_else(|| scan.detail),
            estimated_rows(plan),
            None,
            Vec::new(),
        );
    }

    let children = plan
        .children()
        .into_iter()
        .enumerate()
        .map(|(idx, child)| {
            let mut child_path = step_path.clone();
            child_path.push(u32::try_from(idx).unwrap_or(u32::MAX));
            build_execution_steps(child.as_ref(), child_path, pushdowns, cache)
        })
        .collect::<Vec<_>>();

    crate::ExecutionPlanStep::new(
        kind,
        plan.name(),
        detail.unwrap_or_default(),
        estimated_rows(plan),
        None,
        children,
    )
}

fn classify_step_kind(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if lower.contains("join") {
        "join".to_string()
    } else if lower.contains("filter") {
        "filter".to_string()
    } else if lower.contains("projection") || lower.contains("project") {
        "projection".to_string()
    } else if lower.contains("sort") || lower.contains("order") {
        "sort".to_string()
    } else if lower.contains("limit") {
        "limit".to_string()
    } else if lower.contains("aggregate") {
        "aggregate".to_string()
    } else if lower.contains("scan") || lower.contains("exec") {
        "scan".to_string()
    } else {
        "step".to_string()
    }
}

fn step_detail(plan: &dyn PhysicalExecutionPlan) -> Option<String> {
    if let Some(json_exec) = plan
        .as_any()
        .downcast_ref::<crate::backends::shared::json_exec::JsonExec>()
    {
        return Some(json_exec.plan_summary());
    }
    None
}

fn scan_metadata(
    plan: &dyn PhysicalExecutionPlan,
) -> Option<crate::backends::shared::json_exec::JsonExecExplain> {
    if let Some(json_exec) = plan
        .as_any()
        .downcast_ref::<crate::backends::shared::json_exec::JsonExec>()
    {
        return Some(json_exec.explain().clone());
    }
    None
}

fn estimated_rows(plan: &dyn PhysicalExecutionPlan) -> Option<u64> {
    let stats = plan.partition_statistics(None).ok()?;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    {
        stats.num_rows.map(|rows| rows as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::classify_step_kind;

    #[test]
    fn classifies_specific_exec_plan_names_before_generic_scan() {
        assert_eq!(classify_step_kind("SortExec"), "sort");
        assert_eq!(classify_step_kind("GlobalLimitExec"), "limit");
        assert_eq!(classify_step_kind("AggregateExec"), "aggregate");
        assert_eq!(classify_step_kind("ParquetExec"), "scan");
    }
}

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
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
