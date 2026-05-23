//! Typed query inputs and results.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_spec::ValidatedSourceManifest;

use super::ColumnInfo;
use crate::EngineExtensions;

/// One managed source selected into the current query runtime.
#[derive(Debug, Clone)]
pub struct QuerySource {
    source_spec: ValidatedSourceManifest,
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

impl QuerySource {
    #[must_use]
    /// Builds one app-to-query source selection from installed metadata and a
    /// validated declarative source spec.
    pub fn new(
        source_spec: ValidatedSourceManifest,
        variables: BTreeMap<String, String>,
        secrets: BTreeMap<String, String>,
    ) -> Self {
        Self {
            source_spec,
            variables,
            secrets,
        }
    }

    #[must_use]
    /// Returns the canonical source name. This is also the visible SQL schema name.
    pub fn source_name(&self) -> &str {
        self.source_spec.schema_name()
    }

    #[must_use]
    /// Returns the installed manifest version for this source.
    pub fn version(&self) -> &str {
        self.source_spec.source_version()
    }

    #[must_use]
    /// Returns the validated declarative source spec for this source.
    pub fn source_spec(&self) -> &ValidatedSourceManifest {
        &self.source_spec
    }

    #[must_use]
    /// Returns configured non-secret source variables.
    pub fn variables(&self) -> &BTreeMap<String, String> {
        &self.variables
    }

    #[must_use]
    /// Returns resolved source secrets required by the manifest.
    pub fn secrets(&self) -> &BTreeMap<String, String> {
        &self.secrets
    }
}

/// One source-spec validation query executed during source validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTestResult {
    sql: String,
    result: Result<QueryTestSuccess, QueryTestFailure>,
}

/// Success metadata for one validation query execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTestSuccess {
    row_count: u64,
}

impl QueryTestSuccess {
    #[must_use]
    /// Returns the row count captured for the successful query.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }
}

/// Failure details for one validation query execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTestFailure {
    error_message: String,
}

impl QueryTestFailure {
    #[must_use]
    /// Returns the error message captured for the failed query.
    pub fn error_message(&self) -> &str {
        &self.error_message
    }
}

impl QueryTestResult {
    #[must_use]
    /// Builds one successful query-test result entry.
    pub fn success(sql: impl Into<String>, row_count: u64) -> Self {
        Self {
            sql: sql.into(),
            result: Ok(QueryTestSuccess { row_count }),
        }
    }

    #[must_use]
    /// Builds one failed query-test result entry.
    pub fn failure(sql: impl Into<String>, error_message: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            result: Err(QueryTestFailure {
                error_message: error_message.into(),
            }),
        }
    }

    #[must_use]
    /// Returns the SQL text that was executed.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    #[must_use]
    /// Returns whether the query executed successfully.
    pub fn passed(&self) -> bool {
        self.result.is_ok()
    }

    #[must_use]
    /// Returns the captured row count for successful queries.
    pub fn row_count(&self) -> Option<u64> {
        self.result.as_ref().ok().map(QueryTestSuccess::row_count)
    }

    #[must_use]
    /// Returns the error message for failed queries, when present.
    pub fn error_message(&self) -> Option<&str> {
        self.result
            .as_ref()
            .err()
            .map(QueryTestFailure::error_message)
    }

    /// Returns the execution result metadata for this query test.
    pub fn result(&self) -> &Result<QueryTestSuccess, QueryTestFailure> {
        &self.result
    }
}

/// Structured report for validating one source and its optional test queries.
#[derive(Debug, Clone)]
pub struct SourceValidationReport {
    /// Tables exposed by the validated source.
    pub tables: Vec<super::TableInfo>,
    /// Table functions exposed by the validated source.
    pub table_functions: Vec<super::TableFunctionInfo>,
    /// One result per declared validation query, in manifest order.
    pub query_tests: Vec<QueryTestResult>,
}

impl SourceValidationReport {
    #[must_use]
    /// Builds one structured source-validation report.
    pub fn new(
        tables: Vec<super::TableInfo>,
        table_functions: Vec<super::TableFunctionInfo>,
        query_tests: Vec<QueryTestResult>,
    ) -> Self {
        Self {
            tables,
            table_functions,
            query_tests,
        }
    }
}

/// App-owned non-secret runtime inputs needed while compiling sources.
#[derive(Debug, Clone, Default)]
pub struct QueryRuntimeContext {
    /// Current user's home directory for local path resolution.
    pub home_dir: Option<PathBuf>,
}

/// Owned runtime-build inputs needed while compiling and registering sources.
#[derive(Default)]
pub struct QueryRuntimeConfig {
    /// Non-secret runtime inputs owned by the application layer.
    pub context: QueryRuntimeContext,
    /// Optional engine extensions for this runtime build.
    pub extensions: EngineExtensions,
}

impl QueryRuntimeConfig {
    /// Builds one runtime config from app-owned context and extension state.
    #[must_use]
    pub fn new(context: QueryRuntimeContext, extensions: EngineExtensions) -> Self {
        Self {
            context,
            extensions,
        }
    }
}

/// Query-engine plan renderings for one `SQL` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    unoptimized_logical: String,
    optimized_logical: String,
    physical: String,
    execution_plan: Option<ExecutionPlan>,
}

/// Structured execution-plan metadata for one SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPlan {
    steps: Vec<ExecutionPlanStep>,
    pushdowns: Vec<PushdownDecision>,
    cache: Vec<CacheDecision>,
    estimated_rows: Option<u64>,
    actual_rows: Option<u64>,
}

/// One execution-plan step in tree form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPlanStep {
    kind: String,
    name: String,
    detail: String,
    estimated_rows: Option<u64>,
    actual_rows: Option<u64>,
    children: Vec<ExecutionPlanStep>,
}

/// One predicate pushdown decision for a query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushdownDecision {
    step_path: Vec<u32>,
    target: String,
    predicate: String,
    applied: bool,
    detail: String,
}

/// One cache decision for a query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheDecision {
    step_path: Vec<u32>,
    target: String,
    strategy: String,
    status: String,
    detail: String,
}

impl QueryPlan {
    #[must_use]
    /// Builds one query-plan snapshot from engine plan renderings.
    pub fn new(
        unoptimized_logical_plan: String,
        optimized_logical_plan: String,
        physical_plan: String,
        execution_plan: Option<ExecutionPlan>,
    ) -> Self {
        Self {
            unoptimized_logical: unoptimized_logical_plan,
            optimized_logical: optimized_logical_plan,
            physical: physical_plan,
            execution_plan,
        }
    }

    #[must_use]
    /// Returns the parsed logical plan before logical optimizer rules run.
    pub fn unoptimized_logical_plan(&self) -> &str {
        &self.unoptimized_logical
    }

    #[must_use]
    /// Returns the logical plan after logical optimizer rules run.
    pub fn optimized_logical_plan(&self) -> &str {
        &self.optimized_logical
    }

    #[must_use]
    /// Returns the physical execution plan after physical optimizer rules run.
    pub fn physical_plan(&self) -> &str {
        &self.physical
    }

    #[must_use]
    /// Returns the structured execution-plan metadata when available.
    pub fn execution_plan(&self) -> Option<&ExecutionPlan> {
        self.execution_plan.as_ref()
    }
}

impl ExecutionPlan {
    #[must_use]
    /// Builds one structured execution plan.
    pub fn new(
        steps: Vec<ExecutionPlanStep>,
        pushdowns: Vec<PushdownDecision>,
        cache: Vec<CacheDecision>,
        estimated_rows: Option<u64>,
        actual_rows: Option<u64>,
    ) -> Self {
        Self {
            steps,
            pushdowns,
            cache,
            estimated_rows,
            actual_rows,
        }
    }

    #[must_use]
    /// Returns the root execution-plan steps.
    pub fn steps(&self) -> &[ExecutionPlanStep] {
        &self.steps
    }

    #[must_use]
    /// Returns the pushdown decisions collected for the plan.
    pub fn pushdowns(&self) -> &[PushdownDecision] {
        &self.pushdowns
    }

    #[must_use]
    /// Returns the cache decisions collected for the plan.
    pub fn cache(&self) -> &[CacheDecision] {
        &self.cache
    }

    #[must_use]
    /// Returns the estimated row count when available.
    pub fn estimated_rows(&self) -> Option<u64> {
        self.estimated_rows
    }

    #[must_use]
    /// Returns the actual row count when available.
    pub fn actual_rows(&self) -> Option<u64> {
        self.actual_rows
    }
}

impl ExecutionPlanStep {
    #[must_use]
    /// Builds one execution-plan step.
    pub fn new(
        kind: impl Into<String>,
        name: impl Into<String>,
        detail: impl Into<String>,
        estimated_rows: Option<u64>,
        actual_rows: Option<u64>,
        children: Vec<ExecutionPlanStep>,
    ) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
            detail: detail.into(),
            estimated_rows,
            actual_rows,
            children,
        }
    }

    #[must_use]
    /// Returns the step kind.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    /// Returns the step name.
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    /// Returns the step detail text.
    pub fn detail(&self) -> &str {
        &self.detail
    }

    #[must_use]
    /// Returns the estimated rows for the step when available.
    pub fn estimated_rows(&self) -> Option<u64> {
        self.estimated_rows
    }

    #[must_use]
    /// Returns the actual rows for the step when available.
    pub fn actual_rows(&self) -> Option<u64> {
        self.actual_rows
    }

    #[must_use]
    /// Returns the child steps.
    pub fn children(&self) -> &[ExecutionPlanStep] {
        &self.children
    }
}

impl PushdownDecision {
    #[must_use]
    /// Builds one pushdown decision.
    pub fn new(
        step_path: Vec<u32>,
        target: impl Into<String>,
        predicate: impl Into<String>,
        applied: bool,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            step_path,
            target: target.into(),
            predicate: predicate.into(),
            applied,
            detail: detail.into(),
        }
    }

    #[must_use]
    /// Returns the step path for the pushdown decision.
    pub fn step_path(&self) -> &[u32] {
        &self.step_path
    }

    #[must_use]
    /// Returns the target relation or function.
    pub fn target(&self) -> &str {
        &self.target
    }

    #[must_use]
    /// Returns the predicate text.
    pub fn predicate(&self) -> &str {
        &self.predicate
    }

    #[must_use]
    /// Returns whether the pushdown was applied.
    pub fn applied(&self) -> bool {
        self.applied
    }

    #[must_use]
    /// Returns additional detail text.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl CacheDecision {
    #[must_use]
    /// Builds one cache decision.
    pub fn new(
        step_path: Vec<u32>,
        target: impl Into<String>,
        strategy: impl Into<String>,
        status: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            step_path,
            target: target.into(),
            strategy: strategy.into(),
            status: status.into(),
            detail: detail.into(),
        }
    }

    #[must_use]
    /// Returns the step path for the cache decision.
    pub fn step_path(&self) -> &[u32] {
        &self.step_path
    }

    #[must_use]
    /// Returns the target relation or function.
    pub fn target(&self) -> &str {
        &self.target
    }

    #[must_use]
    /// Returns the cache strategy.
    pub fn strategy(&self) -> &str {
        &self.strategy
    }

    #[must_use]
    /// Returns the cache status.
    pub fn status(&self) -> &str {
        &self.status
    }

    #[must_use]
    /// Returns additional detail text.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// The fully materialized result of executing one `SQL` statement.
#[derive(Debug, Clone)]
pub struct QueryExecution {
    schema: Vec<ColumnInfo>,
    arrow_schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

impl QueryExecution {
    #[must_use]
    /// Builds a validated fully materialized query result.
    pub fn new(arrow_schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        let schema = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(position, field)| ColumnInfo {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
                is_virtual: false,
                is_required_filter: false,
                description: String::new(),
                ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
            })
            .collect();
        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        Self {
            schema,
            arrow_schema,
            batches,
            row_count,
        }
    }

    #[must_use]
    /// Returns the logical result-set schema.
    pub fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    #[must_use]
    /// Returns the Arrow schema preserved even for empty result sets.
    pub fn arrow_schema(&self) -> &Arc<Schema> {
        &self.arrow_schema
    }

    #[must_use]
    /// Returns the materialized Arrow record batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    #[must_use]
    /// Returns the total number of rows across all batches.
    pub fn row_count(&self) -> usize {
        self.row_count
    }
}
