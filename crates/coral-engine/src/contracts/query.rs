//! Typed query inputs and results.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_spec::ValidatedSourceManifest;

use super::ColumnInfo;

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
    passed: bool,
    row_count: Option<u64>,
    error_message: Option<String>,
}

impl QueryTestResult {
    #[must_use]
    /// Builds one query-test result entry.
    pub fn new(
        sql: impl Into<String>,
        passed: bool,
        row_count: Option<u64>,
        error_message: Option<String>,
    ) -> Self {
        Self {
            sql: sql.into(),
            passed,
            row_count,
            error_message,
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
        self.passed
    }

    #[must_use]
    /// Returns the optional row count captured for successful queries.
    pub fn row_count(&self) -> Option<u64> {
        self.row_count
    }

    #[must_use]
    /// Returns the error message for failed queries, when present.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// Structured outcome for validating one source and its optional test queries.
#[derive(Debug, Clone)]
pub struct SourceValidationOutcome {
    pub tables: Vec<super::TableInfo>,
    pub query_tests: Vec<QueryTestResult>,
    pub declared_query_count: u32,
    pub passed_query_count: u32,
    pub failed_query_count: u32,
    pub all_query_tests_passed: bool,
}

impl SourceValidationOutcome {
    #[must_use]
    /// Builds one structured source-validation outcome.
    pub fn new(tables: Vec<super::TableInfo>, query_tests: Vec<QueryTestResult>) -> Self {
        let declared_query_count = query_tests.len() as u32;
        let passed_query_count = query_tests.iter().filter(|test| test.passed).count() as u32;
        let failed_query_count = declared_query_count.saturating_sub(passed_query_count);
        Self {
            tables,
            query_tests,
            declared_query_count,
            passed_query_count,
            failed_query_count,
            all_query_tests_passed: failed_query_count == 0,
        }
    }
}

/// App-owned non-secret runtime inputs needed while compiling sources.
#[derive(Debug, Clone, Default)]
pub struct QueryRuntimeContext {
    /// Current user's home directory for local path resolution.
    pub home_dir: Option<PathBuf>,
}

/// Resolves app-owned runtime inputs at query time.
pub trait QueryRuntimeProvider: Send + Sync {
    /// Returns non-secret runtime inputs owned by the application layer.
    fn runtime_context(&self) -> QueryRuntimeContext;
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
            .map(|field| ColumnInfo {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
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
