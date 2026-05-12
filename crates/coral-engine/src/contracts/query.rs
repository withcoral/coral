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
    /// One result per declared validation query, in manifest order.
    pub query_tests: Vec<QueryTestResult>,
}

impl SourceValidationReport {
    #[must_use]
    /// Builds one structured source-validation report.
    pub fn new(tables: Vec<super::TableInfo>, query_tests: Vec<QueryTestResult>) -> Self {
        Self {
            tables,
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
