//! Typed query inputs and results.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_spec::backends::database::{DatabaseConnectionSpec, DatabaseSourceManifest};
use coral_spec::backends::file::FileSourceManifest;
use coral_spec::backends::http::HttpSourceManifest;
use coral_spec::backends::mcp::McpSourceManifest;
use coral_spec::v4::IdentityRequirements;
use coral_spec::{ManifestInputSpec, ValidatedSourceManifest};
use opentelemetry::Context as OtelContext;

use super::ColumnInfo;
use crate::{
    EngineExtensions, RequestIdentityHttpAuthenticatorFactory, RequestIdentitySelectionContext,
    RequestIdentitySelector,
};

/// One managed source selected into the current query runtime.
#[derive(Clone)]
pub struct QuerySource {
    source_name: String,
    authored_version: Option<String>,
    description: String,
    declared_inputs: Vec<ManifestInputSpec>,
    test_queries: Vec<String>,
    identity_requirements: Option<IdentityRequirements>,
    components: Vec<RuntimeSourceComponent>,
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

/// Backend-ready runtime package for one logical query source.
#[derive(Debug, Clone)]
pub struct RuntimeSourcePackage {
    /// Canonical installed source name.
    pub source_name: String,
    /// Authored manifest version, when the authoring DSL has one.
    pub authored_version: Option<String>,
    /// Source description shown in catalog and source metadata surfaces.
    pub description: String,
    /// Declared source inputs in authored order.
    pub declared_inputs: Vec<ManifestInputSpec>,
    /// Source-level validation queries in authored order.
    pub test_queries: Vec<String>,
    /// Source-level request identity requirements, when declared.
    pub identity_requirements: Option<IdentityRequirements>,
    /// Backend-ready runtime components that make up the logical source.
    pub components: Vec<RuntimeSourceComponent>,
}

/// One backend-ready component inside an app-assembled query source package.
#[derive(Clone)]
pub enum RuntimeSourceComponent {
    /// Relational database-backed runtime component.
    Database(DatabaseSourceManifest),
    /// HTTP-backed runtime component.
    Http(HttpSourceManifest),
    /// File-backed runtime component.
    File(FileSourceManifest),
    /// MCP-backed runtime component.
    Mcp(McpSourceManifest),
}

impl fmt::Debug for QuerySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuerySource")
            .field("source_name", &self.source_name)
            .field("authored_version", &self.authored_version)
            .field("description", &self.description)
            .field("declared_inputs", &self.declared_inputs)
            .field("test_queries", &self.test_queries)
            .field("components", &self.components)
            .field("variables", &self.variables)
            .field("secret_count", &self.secrets.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for RuntimeSourceComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(manifest) => {
                let provider = match &manifest.connection {
                    DatabaseConnectionSpec::Postgres(_) => "postgres",
                    DatabaseConnectionSpec::MySql(_) => "mysql",
                    DatabaseConnectionSpec::Sqlite(_) => "sqlite",
                };
                formatter
                    .debug_struct("Database")
                    .field("source_name", &manifest.common.name)
                    .field("provider", &provider)
                    .finish_non_exhaustive()
            }
            Self::Http(manifest) => formatter
                .debug_struct("Http")
                .field("source_name", &manifest.common.name)
                .finish_non_exhaustive(),
            Self::File(manifest) => formatter
                .debug_struct("File")
                .field("source_name", &manifest.common.name)
                .finish_non_exhaustive(),
            Self::Mcp(manifest) => formatter
                .debug_struct("Mcp")
                .field("source_name", &manifest.common.name)
                .finish_non_exhaustive(),
        }
    }
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
            identity_requirements: None,
            components,
            variables,
            secrets,
        }
    }

    /// Builds one source selection from app-assembled runtime components.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`](crate::CoreError) when the package is invalid.
    pub fn from_runtime_components(
        package: RuntimeSourcePackage,
        variables: BTreeMap<String, String>,
        secrets: BTreeMap<String, String>,
    ) -> Result<Self, crate::CoreError> {
        if package.source_name.trim().is_empty() {
            return Err(crate::CoreError::InvalidInput(
                "runtime source package source_name must not be empty".to_string(),
            ));
        }
        for component in &package.components {
            let schema_name = component.source_name();
            if schema_name.trim().is_empty() {
                return Err(crate::CoreError::InvalidInput(format!(
                    "runtime source package '{}' has a component with an empty schema name",
                    package.source_name
                )));
            }
        }
        validate_runtime_source_identity_requirements(&package)?;
        Ok(Self {
            source_name: package.source_name,
            authored_version: package.authored_version,
            description: package.description,
            declared_inputs: package.declared_inputs,
            test_queries: package.test_queries,
            identity_requirements: package.identity_requirements,
            components: package.components,
            variables,
            secrets,
        })
    }

    #[must_use]
    /// Returns the canonical installed source name.
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
    /// Returns the source-level request identity requirements, when declared.
    pub fn identity_requirements(&self) -> Option<&IdentityRequirements> {
        self.identity_requirements.as_ref()
    }

    #[must_use]
    /// Builds the identity-selection context for this source, when gated.
    pub fn identity_selection_context(&self) -> Option<RequestIdentitySelectionContext> {
        self.identity_requirements.as_ref().map(|requirements| {
            RequestIdentitySelectionContext::new(self.source_name.clone(), requirements.clone())
        })
    }

    #[must_use]
    /// Returns backend-ready runtime components supplied by the app.
    pub fn components(&self) -> &[RuntimeSourceComponent] {
        &self.components
    }

    #[must_use]
    /// Returns the SQL schema names published by this source's two-part
    /// components. Database components publish catalogs instead; see
    /// [`Self::catalog_names`]. A source with no components claims its source
    /// name as a schema. Schema and catalog names of all selected sources
    /// share one flat namespace.
    pub fn schema_names(&self) -> Vec<&str> {
        let mut names = Vec::new();
        for component in &self.components {
            if matches!(component, RuntimeSourceComponent::Database(_)) {
                continue;
            }
            let name = component.source_name();
            if !names.contains(&name) {
                names.push(name);
            }
        }
        if self.components.is_empty() {
            names.push(self.source_name());
        }
        names
    }

    #[must_use]
    /// Returns the SQL catalog names published by this source's database
    /// components. Tables of these components resolve as
    /// `catalog.schema.table`, with schemas discovered at registration time.
    pub fn catalog_names(&self) -> Vec<&str> {
        let mut names = Vec::new();
        for component in &self.components {
            let RuntimeSourceComponent::Database(manifest) = component else {
                continue;
            };
            let name = manifest.common.name.as_str();
            if !names.contains(&name) {
                names.push(name);
            }
        }
        names
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
    /// Returns the name this component publishes at runtime: a schema name,
    /// or a catalog name for database components.
    pub fn source_name(&self) -> &str {
        match self {
            Self::Database(manifest) => &manifest.common.name,
            Self::Http(manifest) => &manifest.common.name,
            Self::File(manifest) => &manifest.common.name,
            Self::Mcp(manifest) => &manifest.common.name,
        }
    }
}

fn validate_runtime_source_identity_requirements(
    package: &RuntimeSourcePackage,
) -> Result<(), crate::CoreError> {
    if package.identity_requirements.is_none() {
        return Ok(());
    }

    for component in &package.components {
        let RuntimeSourceComponent::Http(manifest) = component else {
            return Err(crate::CoreError::InvalidInput(format!(
                "runtime source package '{}' declares identity_requirements, but identity_requirements require every runtime component to be a DSL v4 HTTP component",
                package.source_name
            )));
        };
        if manifest.common.dsl_version != 4 {
            return Err(crate::CoreError::InvalidInput(format!(
                "runtime source package '{}' declares identity_requirements, but component '{}' uses DSL v{} HTTP instead of DSL v4 HTTP",
                package.source_name, manifest.common.name, manifest.common.dsl_version
            )));
        }
    }
    Ok(())
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
#[derive(Clone, Default)]
pub struct QueryRuntimeContext {
    /// Current user's home directory for local path resolution.
    pub home_dir: Option<PathBuf>,
    /// Active query trace context, when the app layer is executing under one.
    pub trace_context: Option<OtelContext>,
    /// Optional positive byte cap for pre-export trace body preview capture.
    /// Shared across backends — HTTP request/response bodies, MCP tool
    /// arguments, and MCP tool result payloads are all truncated to this
    /// limit before being recorded as child trace spans.
    pub body_capture_max_bytes: Option<usize>,
}

impl fmt::Debug for QueryRuntimeContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryRuntimeContext")
            .field("home_dir", &self.home_dir)
            .field("trace_context", &self.trace_context.is_some())
            .field("body_capture_max_bytes", &self.body_capture_max_bytes)
            .finish()
    }
}

impl QueryRuntimeContext {
    /// Adds app-owned local trace body capture byte cap to this runtime context.
    #[must_use]
    pub fn with_body_capture_max_bytes(mut self, max_bytes: Option<usize>) -> Self {
        self.body_capture_max_bytes = max_bytes.filter(|bytes| *bytes > 0);
        self
    }
}

/// Named SQL query parameters, keyed by parameter name without the `$`
/// prefix: binding `owner` supplies `$owner` in the statement.
///
/// Values are typed Coral scalar values. Callers should treat values as data,
/// never SQL text.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryParameters {
    values: BTreeMap<String, QueryParameterValue>,
}

impl QueryParameters {
    /// Builds an empty parameter set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true when no parameters are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Inserts one named parameter value and returns the previous value.
    pub fn insert(
        &mut self,
        name: impl Into<String>,
        value: QueryParameterValue,
    ) -> Option<QueryParameterValue> {
        self.values.insert(name.into(), value)
    }

    /// Returns one parameter value by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&QueryParameterValue> {
        self.values.get(name)
    }

    /// Iterates over parameter names and values in deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &QueryParameterValue)> {
        self.values.iter()
    }

    /// Iterates over parameter names in deterministic order.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.values.keys()
    }
}

impl From<BTreeMap<String, QueryParameterValue>> for QueryParameters {
    fn from(values: BTreeMap<String, QueryParameterValue>) -> Self {
        Self { values }
    }
}

impl<const N: usize> From<[(String, QueryParameterValue); N]> for QueryParameters {
    fn from(values: [(String, QueryParameterValue); N]) -> Self {
        Self {
            values: BTreeMap::from(values),
        }
    }
}

impl FromIterator<(String, QueryParameterValue)> for QueryParameters {
    fn from_iter<T: IntoIterator<Item = (String, QueryParameterValue)>>(iter: T) -> Self {
        Self {
            values: iter.into_iter().collect(),
        }
    }
}

/// One typed SQL query parameter value.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum QueryParameterValue {
    /// UTF-8 string value, or a typed string NULL.
    String(Option<String>),
    /// 64-bit signed integer value, or a typed integer NULL.
    Integer(Option<i64>),
    /// 64-bit floating point value, or a typed float NULL.
    Float(Option<f64>),
    /// Boolean value, or a typed boolean NULL.
    Boolean(Option<bool>),
    /// UTC timestamp as microseconds since the Unix epoch, or a typed timestamp NULL.
    Timestamp(Option<i64>),
}

impl QueryParameterValue {
    /// Builds a non-null string parameter.
    #[must_use]
    pub fn string(value: impl Into<String>) -> Self {
        Self::String(Some(value.into()))
    }

    /// Builds a typed string NULL parameter.
    #[must_use]
    pub fn null_string() -> Self {
        Self::String(None)
    }

    /// Builds a non-null integer parameter.
    #[must_use]
    pub fn integer(value: i64) -> Self {
        Self::Integer(Some(value))
    }

    /// Builds a typed integer NULL parameter.
    #[must_use]
    pub fn null_integer() -> Self {
        Self::Integer(None)
    }

    /// Builds a non-null float parameter.
    #[must_use]
    pub fn float(value: f64) -> Self {
        Self::Float(Some(value))
    }

    /// Builds a typed float NULL parameter.
    #[must_use]
    pub fn null_float() -> Self {
        Self::Float(None)
    }

    /// Builds a non-null boolean parameter.
    #[must_use]
    pub fn boolean(value: bool) -> Self {
        Self::Boolean(Some(value))
    }

    /// Builds a typed boolean NULL parameter.
    #[must_use]
    pub fn null_boolean() -> Self {
        Self::Boolean(None)
    }

    /// Builds a non-null UTC timestamp from microseconds since the Unix epoch.
    #[must_use]
    pub fn timestamp_micros(value: i64) -> Self {
        Self::Timestamp(Some(value))
    }

    /// Builds a typed timestamp NULL.
    #[must_use]
    pub fn null_timestamp() -> Self {
        Self::Timestamp(None)
    }
}

/// Owned runtime-build inputs needed while compiling and registering sources.
#[derive(Default)]
pub struct QueryRuntimeConfig {
    /// Non-secret runtime inputs owned by the application layer.
    pub context: QueryRuntimeContext,
    /// Optional engine extensions for this runtime build.
    pub extensions: EngineExtensions,
    /// Engine-wide query memory policy.
    pub memory: QueryMemoryConfig,
    /// Runtime-build selector for app-owned request identities.
    pub request_identity_selector: Option<Arc<dyn RequestIdentitySelector>>,
    /// Factory that binds selected identities to request-time HTTP authenticators.
    pub request_identity_http_authenticator_factory:
        Option<RequestIdentityHttpAuthenticatorFactory>,
    /// Runtime policy for dependent predicate pushdown.
    pub dependent_join: DependentJoinConfig,
    /// Validated UDFs available in this runtime build.
    pub udfs: Vec<super::UdfRuntimeDefinition>,
}

impl QueryRuntimeConfig {
    /// Builds one runtime config from app-owned context and extension state.
    #[must_use]
    pub fn new(context: QueryRuntimeContext, extensions: EngineExtensions) -> Self {
        Self {
            context,
            extensions,
            memory: QueryMemoryConfig::default(),
            request_identity_selector: None,
            request_identity_http_authenticator_factory: None,
            dependent_join: DependentJoinConfig::default(),
            udfs: Vec::new(),
        }
    }

    /// Attaches validated UDFs to this runtime config.
    #[must_use]
    pub fn with_udfs(mut self, udfs: Vec<super::UdfRuntimeDefinition>) -> Self {
        self.udfs = udfs;
        self
    }

    /// Installs the request identity selector for this runtime build.
    #[must_use]
    pub fn with_request_identity_selector(
        mut self,
        selector: Option<Arc<dyn RequestIdentitySelector>>,
    ) -> Self {
        self.request_identity_selector = selector;
        self
    }

    /// Installs the request identity HTTP-authenticator factory.
    #[must_use]
    pub fn with_request_identity_http_authenticator_factory(
        mut self,
        factory: Option<RequestIdentityHttpAuthenticatorFactory>,
    ) -> Self {
        self.request_identity_http_authenticator_factory = factory;
        self
    }
}

/// Engine-wide query memory policy.
///
/// This type is non-exhaustive so additional global memory policy can be added
/// later without changing the meaning of [`Self::limit`], including source- or
/// table-scoped retained-memory budgets and memory-pool strategy selection.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryMemoryConfig {
    /// Optional total query-engine memory limit.
    pub limit: Option<MemorySize>,
}

impl QueryMemoryConfig {
    /// Builds a memory policy with an optional whole-runtime memory limit.
    #[must_use]
    pub fn with_limit(limit: Option<MemorySize>) -> Self {
        Self { limit }
    }
}

/// Human-readable memory size stored internally as bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MemorySize {
    bytes: usize,
}

impl MemorySize {
    /// Builds a memory size from a positive byte count.
    ///
    /// # Errors
    ///
    /// Returns an error when `bytes` is zero.
    pub fn from_bytes(bytes: usize) -> Result<Self, MemorySizeParseError> {
        if bytes == 0 {
            return Err(MemorySizeParseError::new(
                "memory limit must be greater than 0",
            ));
        }
        Ok(Self { bytes })
    }

    /// Returns this size in bytes.
    #[must_use]
    pub fn as_bytes(self) -> usize {
        self.bytes
    }
}

/// Error returned when parsing a human-readable memory size fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySizeParseError {
    detail: String,
}

impl MemorySizeParseError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for MemorySizeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for MemorySizeParseError {}

impl FromStr for MemorySize {
    type Err = MemorySizeParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let value = raw.trim();
        if value.is_empty() {
            return Err(MemorySizeParseError::new("memory limit must not be empty"));
        }

        let (number, multiplier) = parse_memory_unit(value)?;
        if number.is_empty() || !number.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(MemorySizeParseError::new(
                "memory limit must be an integer followed by Ki, Mi, Gi, or Ti",
            ));
        }

        let amount = number
            .parse::<u128>()
            .map_err(|_error| MemorySizeParseError::new("memory limit is too large"))?;
        if amount == 0 {
            return Err(MemorySizeParseError::new(
                "memory limit must be greater than 0",
            ));
        }
        let bytes = amount
            .checked_mul(multiplier)
            .ok_or_else(|| MemorySizeParseError::new("memory limit is too large"))?;
        let bytes = usize::try_from(bytes)
            .map_err(|_error| MemorySizeParseError::new("memory limit is too large"))?;
        Self::from_bytes(bytes)
    }
}

fn parse_memory_unit(value: &str) -> Result<(&str, u128), MemorySizeParseError> {
    for (suffix, multiplier) in [
        ("Ki", 1024_u128),
        ("Mi", 1024_u128.pow(2)),
        ("Gi", 1024_u128.pow(3)),
        ("Ti", 1024_u128.pow(4)),
    ] {
        if let Some(number) = value.strip_suffix(suffix) {
            return Ok((number, multiplier));
        }
    }
    Err(MemorySizeParseError::new(
        "memory limit must use binary unit Ki, Mi, Gi, or Ti",
    ))
}

/// Runtime policy for dependent predicate pushdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependentJoinConfig {
    /// Default enablement for dependent join rewrites.
    pub enabled: bool,
    /// Maximum distinct join-key combinations to push into upstream APIs.
    pub max_bindings: usize,
    /// Maximum rows read from the key-supplying side before falling back.
    pub max_resolver_rows: usize,
    /// Maximum rows accepted for one join-key combination across the full upstream fetch.
    pub max_rows_per_binding: usize,
    /// Maximum key-supplying rows allowed for one join-key combination.
    pub max_resolver_rows_per_binding: usize,
    /// Maximum concurrent upstream requests issued by one dependent join.
    pub max_concurrency: usize,
    /// Source-specific overrides keyed by source name.
    pub per_source: BTreeMap<String, DependentJoinSourceConfig>,
}

/// Source-specific dependent predicate pushdown policy overrides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DependentJoinSourceConfig {
    /// Overrides dependent join rewrite enablement for this source.
    pub enabled: Option<bool>,
    /// Overrides maximum distinct join-key combinations for this source.
    pub max_bindings: Option<usize>,
    /// Overrides maximum resolver-side rows for this source.
    pub max_resolver_rows: Option<usize>,
    /// Overrides maximum rows accepted from one upstream request.
    pub max_rows_per_binding: Option<usize>,
    /// Overrides maximum resolver rows allowed for one join-key combination.
    pub max_resolver_rows_per_binding: Option<usize>,
    /// Overrides concurrent upstream requests issued by one dependent join.
    pub max_concurrency: Option<usize>,
}

/// Fully resolved dependent predicate pushdown policy for one source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveDependentJoinConfig {
    /// Enables dependent join rewrites for this source.
    pub enabled: bool,
    /// Maximum distinct join-key combinations to push into upstream APIs.
    pub max_bindings: usize,
    /// Maximum rows read from the key-supplying side before falling back.
    pub max_resolver_rows: usize,
    /// Maximum rows accepted from one upstream request.
    pub max_rows_per_binding: usize,
    /// Maximum key-supplying rows allowed for one join-key combination.
    pub max_resolver_rows_per_binding: usize,
    /// Maximum concurrent upstream requests issued by one dependent join.
    pub max_concurrency: usize,
}

impl Default for DependentJoinConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_bindings: 500,
            max_resolver_rows: 10_000,
            max_rows_per_binding: 1_000,
            max_resolver_rows_per_binding: 1_000,
            max_concurrency: 8,
            per_source: BTreeMap::new(),
        }
    }
}

impl DependentJoinConfig {
    /// Returns a copy with all dependent join rewrites disabled.
    #[must_use]
    pub fn without_rewrites(&self) -> Self {
        Self {
            enabled: false,
            per_source: BTreeMap::new(),
            ..self.clone()
        }
    }

    /// Returns whether the optimizer rule should be registered.
    #[must_use]
    pub fn optimizer_enabled(&self) -> bool {
        self.enabled
            || self
                .per_source
                .values()
                .any(|source| source.enabled == Some(true))
    }

    /// Resolves the effective dependent join policy for one source.
    #[must_use]
    pub fn for_source(&self, source_name: &str) -> EffectiveDependentJoinConfig {
        let source = self.per_source.get(source_name);
        let max_concurrency = source
            .and_then(|override_config| override_config.max_concurrency)
            .unwrap_or(self.max_concurrency)
            .max(1);
        EffectiveDependentJoinConfig {
            enabled: source
                .and_then(|override_config| override_config.enabled)
                .unwrap_or(self.enabled),
            max_bindings: source
                .and_then(|override_config| override_config.max_bindings)
                .unwrap_or(self.max_bindings),
            max_resolver_rows: source
                .and_then(|override_config| override_config.max_resolver_rows)
                .unwrap_or(self.max_resolver_rows),
            max_rows_per_binding: source
                .and_then(|override_config| override_config.max_rows_per_binding)
                .unwrap_or(self.max_rows_per_binding),
            max_resolver_rows_per_binding: source
                .and_then(|override_config| override_config.max_resolver_rows_per_binding)
                .unwrap_or(self.max_resolver_rows_per_binding),
            max_concurrency,
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
    provenance: QueryExecutionProvenance,
}

impl QueryExecution {
    #[must_use]
    /// Builds a validated fully materialized query result with successful-execution provenance.
    pub fn new(
        arrow_schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        sql: impl Into<String>,
        resources: ResolvedQueryResources,
    ) -> Self {
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
        let provenance = QueryExecutionProvenance::new(sql, resources, row_count);
        Self {
            schema,
            arrow_schema,
            batches,
            row_count,
            provenance,
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

    #[must_use]
    /// Returns successful-execution provenance for this query result.
    pub fn provenance(&self) -> &QueryExecutionProvenance {
        &self.provenance
    }
}

/// Source resources referenced by a resolved logical query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedQueryResources {
    sources: Vec<String>,
    tables: Vec<QueryTableUsage>,
    table_functions: Vec<QueryTableFunctionUsage>,
}

impl ResolvedQueryResources {
    #[must_use]
    /// Builds the resource set resolved from one logical query plan.
    pub fn new(
        sources: Vec<String>,
        tables: Vec<QueryTableUsage>,
        table_functions: Vec<QueryTableFunctionUsage>,
    ) -> Self {
        Self {
            sources,
            tables,
            table_functions,
        }
    }

    #[must_use]
    /// Returns the installed source names referenced by the query.
    pub fn sources(&self) -> &[String] {
        &self.sources
    }

    #[must_use]
    /// Returns source tables referenced by the query.
    pub fn tables(&self) -> &[QueryTableUsage] {
        &self.tables
    }

    #[must_use]
    /// Returns source-scoped table functions referenced by the query.
    pub fn table_functions(&self) -> &[QueryTableFunctionUsage] {
        &self.table_functions
    }
}

/// Successful-execution provenance for one query result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryExecutionProvenance {
    sql: String,
    resources: ResolvedQueryResources,
    row_count: usize,
}

impl QueryExecutionProvenance {
    #[must_use]
    /// Builds provenance for one successfully materialized query.
    pub fn new(
        sql: impl Into<String>,
        resources: ResolvedQueryResources,
        row_count: usize,
    ) -> Self {
        Self {
            sql: sql.into(),
            resources,
            row_count,
        }
    }

    #[must_use]
    /// Returns the SQL text that was executed.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    #[must_use]
    /// Returns the installed source names used by the query.
    pub fn sources(&self) -> &[String] {
        self.resources.sources()
    }

    #[must_use]
    /// Returns source tables used by the query.
    pub fn tables(&self) -> &[QueryTableUsage] {
        self.resources.tables()
    }

    #[must_use]
    /// Returns source-scoped table functions used by the query.
    pub fn table_functions(&self) -> &[QueryTableFunctionUsage] {
        self.resources.table_functions()
    }

    #[must_use]
    /// Returns the total number of rows across all result batches.
    pub fn row_count(&self) -> usize {
        self.row_count
    }
}

/// One source table referenced by a query.
///
/// `(schema, table)` alone does not identify a table once catalog-backed sources
/// are registered: two databases can each expose `public.users`. The catalog is
/// part of the identity, so consumers keying on this entry must include it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryTableUsage {
    source: String,
    catalog: Option<String>,
    schema: String,
    table: String,
}

impl QueryTableUsage {
    #[must_use]
    /// Builds one source table usage entry.
    ///
    /// `catalog_name` is `None` for a table addressed as `schema.table`, and the
    /// SQL catalog for one addressed as `catalog.schema.table`.
    pub fn new(
        source_name: impl Into<String>,
        catalog_name: Option<&str>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            source: source_name.into(),
            catalog: catalog_name.map(ToString::to_string),
            schema: schema_name.into(),
            table: table_name.into(),
        }
    }

    #[must_use]
    /// Returns the installed source name that owns this table.
    pub fn source_name(&self) -> &str {
        &self.source
    }

    #[must_use]
    /// Returns the SQL catalog for this table, or `None` when it is addressed as
    /// `schema.table`.
    pub fn catalog_name(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    #[must_use]
    /// Returns the SQL schema name for this table.
    pub fn schema_name(&self) -> &str {
        &self.schema
    }

    #[must_use]
    /// Returns the table name within the SQL schema.
    pub fn table_name(&self) -> &str {
        &self.table
    }
}

/// One source-scoped table function referenced by a query.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryTableFunctionUsage {
    source: String,
    schema: String,
    function: String,
}

impl QueryTableFunctionUsage {
    #[must_use]
    /// Builds one source-scoped table function usage entry.
    pub fn new(
        source_name: impl Into<String>,
        schema_name: impl Into<String>,
        function_name: impl Into<String>,
    ) -> Self {
        Self {
            source: source_name.into(),
            schema: schema_name.into(),
            function: function_name.into(),
        }
    }

    #[must_use]
    /// Returns the installed source name that owns this table function.
    pub fn source_name(&self) -> &str {
        &self.source
    }

    #[must_use]
    /// Returns the SQL schema name for this table function.
    pub fn schema_name(&self) -> &str {
        &self.schema
    }

    #[must_use]
    /// Returns the function name within the SQL schema.
    pub fn function_name(&self) -> &str {
        &self.function
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr as _;

    use coral_spec::parse_source_manifest_value;
    use coral_spec::v4::{AcceptedIdentityRequirement, IdentityRequirements};
    use serde_json::json;

    use super::{MemorySize, QuerySource, RuntimeSourceComponent, RuntimeSourcePackage};

    #[test]
    fn memory_size_parses_binary_units() {
        assert_eq!(MemorySize::from_str("1Ki").unwrap().as_bytes(), 1024);
        assert_eq!(
            MemorySize::from_str("2Mi").unwrap().as_bytes(),
            2 * 1024 * 1024
        );
        assert_eq!(
            MemorySize::from_str("3Gi").unwrap().as_bytes(),
            3 * 1024 * 1024 * 1024
        );
        assert_eq!(
            MemorySize::from_str("1Ti").unwrap().as_bytes(),
            1024_usize.pow(4)
        );
    }

    #[test]
    fn memory_size_rejects_invalid_values() {
        for raw in ["", "0Mi", "2GiB", "2.5Gi", "2gi", "2G", "Gi"] {
            assert!(
                MemorySize::from_str(raw).is_err(),
                "{raw:?} should be rejected"
            );
        }
    }

    #[test]
    fn runtime_source_package_rejects_identity_requirements_on_non_v4_http_component() {
        let error = QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: "github".to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                identity_requirements: Some(identity_requirements()),
                components: vec![RuntimeSourceComponent::Http(http_manifest())],
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect_err("v3 HTTP component should not accept identity requirements");

        assert!(
            error.to_string().contains(
                "declares identity_requirements, but component 'github' uses DSL v3 HTTP instead of DSL v4 HTTP"
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn runtime_source_package_preserves_source_identity_requirements() {
        let mut manifest = http_manifest();
        manifest.common.dsl_version = 4;
        let requirements = identity_requirements();

        let source = QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: "github_v4".to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                identity_requirements: Some(requirements.clone()),
                components: vec![RuntimeSourceComponent::Http(manifest)],
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("v4 source identity requirements");

        assert_eq!(source.identity_requirements(), Some(&requirements));
        let context = source
            .identity_selection_context()
            .expect("identity selection context");
        assert_eq!(context.source_name(), "github_v4");
        assert_eq!(context.identity_requirements(), &requirements);
    }

    #[test]
    fn v3_manifest_query_source_is_ungated() {
        let manifest = source_manifest();
        let source = QuerySource::from_manifest(&manifest, BTreeMap::new(), BTreeMap::new());

        assert!(source.identity_requirements().is_none());
        assert!(source.identity_selection_context().is_none());
        assert!(matches!(
            source.components(),
            [RuntimeSourceComponent::Http(http)] if http.common.dsl_version == 3
        ));
    }

    fn http_manifest() -> coral_spec::backends::http::HttpSourceManifest {
        source_manifest().as_http().expect("http manifest").clone()
    }

    fn source_manifest() -> coral_spec::ValidatedSourceManifest {
        parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "github",
            "version": "1.0.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "tables": [{
                "name": "issues",
                "description": "Issues",
                "request": {
                    "method": "GET",
                    "path": "/issues"
                },
                "response": {},
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }))
        .expect("manifest")
    }

    fn identity_requirements() -> IdentityRequirements {
        IdentityRequirements {
            accepts: vec![AcceptedIdentityRequirement {
                id: "github_rest_read".to_string(),
                identity_specs: vec!["github_oauth".to_string()],
                audience: BTreeMap::new(),
            }],
        }
    }
}
