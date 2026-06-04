//! Typed query inputs and results.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_spec::backends::file::FileSourceManifest;
use coral_spec::backends::http::HttpSourceManifest;
use coral_spec::backends::mcp::McpSourceManifest;
use coral_spec::{ManifestInputSpec, ValidatedSourceManifest};

use super::ColumnInfo;
use crate::EngineExtensions;

/// One managed source selected into the current query runtime.
#[derive(Debug, Clone)]
pub struct QuerySource {
    source_name: String,
    authored_version: Option<String>,
    description: String,
    declared_inputs: Vec<ManifestInputSpec>,
    test_queries: Vec<String>,
    components: Vec<RuntimeSourceComponent>,
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

/// Backend-ready runtime package for one logical query source.
#[derive(Debug, Clone)]
pub struct RuntimeSourcePackage {
    /// Canonical source name, also used as the visible SQL schema.
    pub source_name: String,
    /// Authored manifest version, when the authoring DSL has one.
    pub authored_version: Option<String>,
    /// Source description shown in catalog and source metadata surfaces.
    pub description: String,
    /// Declared source inputs in authored order.
    pub declared_inputs: Vec<ManifestInputSpec>,
    /// Source-level validation queries in authored order.
    pub test_queries: Vec<String>,
    /// Backend-ready runtime components that make up the logical source.
    pub components: Vec<RuntimeSourceComponent>,
}

/// One backend-ready component inside an app-assembled query source package.
#[derive(Debug, Clone)]
pub enum RuntimeSourceComponent {
    /// HTTP-backed runtime component.
    Http(HttpSourceManifest),
    /// File-backed runtime component.
    File(FileSourceManifest),
    /// MCP-backed runtime component.
    Mcp(McpSourceManifest),
}

impl QuerySource {
    #[must_use]
    /// Builds one app-to-query source selection from installed metadata and a
    /// validated declarative source spec.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Preserves the existing constructor API that takes ownership of parsed manifests."
    )]
    pub fn new(
        source_spec: ValidatedSourceManifest,
        variables: BTreeMap<String, String>,
        secrets: BTreeMap<String, String>,
    ) -> Self {
        Self::from_manifest(&source_spec, variables, secrets)
    }

    #[must_use]
    /// Builds one source selection from a validated v3 source manifest.
    pub fn from_manifest(
        source_spec: &ValidatedSourceManifest,
        variables: BTreeMap<String, String>,
        secrets: BTreeMap<String, String>,
    ) -> Self {
        let components = components_from_manifest(source_spec);
        Self {
            source_name: source_spec.schema_name().to_string(),
            authored_version: source_spec.source_version().map(ToString::to_string),
            description: source_spec.description().to_string(),
            declared_inputs: source_spec.declared_inputs().to_vec(),
            test_queries: source_spec.test_queries().to_vec(),
            components,
            variables,
            secrets,
        }
    }

    /// Builds one source selection from app-assembled runtime components.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`](crate::CoreError) when any component belongs to a
    /// different logical source/schema.
    pub fn from_runtime_components(
        package: RuntimeSourcePackage,
        variables: BTreeMap<String, String>,
        secrets: BTreeMap<String, String>,
    ) -> Result<Self, crate::CoreError> {
        for component in &package.components {
            let component_source = component.source_name();
            if component_source != package.source_name {
                return Err(crate::CoreError::InvalidInput(format!(
                    "runtime component for source '{}' belongs to source '{component_source}'",
                    package.source_name
                )));
            }
        }
        Ok(Self {
            source_name: package.source_name,
            authored_version: package.authored_version,
            description: package.description,
            declared_inputs: package.declared_inputs,
            test_queries: package.test_queries,
            components: package.components,
            variables,
            secrets,
        })
    }

    #[must_use]
    /// Returns the canonical source name. This is also the visible SQL schema name.
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    #[must_use]
    /// Returns the authored manifest version for this source, when present.
    pub fn version(&self) -> Option<&str> {
        self.authored_version.as_deref()
    }

    #[must_use]
    /// Returns the source description.
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    /// Returns the declared source inputs in authored order.
    pub fn declared_inputs(&self) -> &[ManifestInputSpec] {
        &self.declared_inputs
    }

    #[must_use]
    /// Returns the source-level validation queries in authored order.
    pub fn test_queries(&self) -> &[String] {
        &self.test_queries
    }

    #[must_use]
    /// Returns backend-ready runtime components supplied by the app.
    pub fn components(&self) -> &[RuntimeSourceComponent] {
        &self.components
    }

    #[must_use]
    /// Returns configured non-secret source variables.
    pub fn variables(&self) -> &BTreeMap<String, String> {
        &self.variables
    }

    #[must_use]
    /// Returns resolved declared source secrets that are available at runtime.
    pub fn secrets(&self) -> &BTreeMap<String, String> {
        &self.secrets
    }
}

impl RuntimeSourceComponent {
    #[must_use]
    /// Returns the logical source/schema name declared by this component.
    pub fn source_name(&self) -> &str {
        match self {
            Self::Http(manifest) => &manifest.common.name,
            Self::File(manifest) => &manifest.common.name,
            Self::Mcp(manifest) => &manifest.common.name,
        }
    }
}

fn components_from_manifest(source_spec: &ValidatedSourceManifest) -> Vec<RuntimeSourceComponent> {
    if let Some(http) = source_spec.as_http() {
        return vec![RuntimeSourceComponent::Http(http.clone())];
    }
    if let Some(file) = source_spec.as_file() {
        return vec![RuntimeSourceComponent::File(file.clone())];
    }
    if let Some(mcp) = source_spec.as_mcp() {
        return vec![RuntimeSourceComponent::Mcp(mcp.clone())];
    }
    Vec::new()
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
    /// Optional positive byte cap for pre-export trace body preview capture.
    /// Shared across backends — HTTP request/response bodies, MCP tool
    /// arguments, and MCP tool result payloads are all truncated to this
    /// limit before being recorded as child trace spans.
    pub body_capture_max_bytes: Option<usize>,
}

impl QueryRuntimeContext {
    /// Adds app-owned local trace body capture byte cap to this runtime context.
    #[must_use]
    pub fn with_body_capture_max_bytes(mut self, max_bytes: Option<usize>) -> Self {
        self.body_capture_max_bytes = max_bytes.filter(|bytes| *bytes > 0);
        self
    }
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
}

impl QueryPlan {
    #[must_use]
    /// Builds one query-plan snapshot from engine plan renderings.
    pub fn new(
        unoptimized_logical_plan: String,
        optimized_logical_plan: String,
        physical_plan: String,
    ) -> Self {
        Self {
            unoptimized_logical: unoptimized_logical_plan,
            optimized_logical: optimized_logical_plan,
            physical: physical_plan,
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
