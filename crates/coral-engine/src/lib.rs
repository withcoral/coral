//! Data-plane query engine for Coral.
//!
//! `coral-engine` is the federated `DataFusion` engine for Coral. It owns
//! backend-specific source adapters, backend compilation into executable
//! providers, runtime assembly, and `SQL` execution over app-provided managed
//! sources.
//!
//! # Primary Entry Points
//!
//! - [`CoralQuery`] performs high-level query operations.
//! - [`contracts`] contains the reviewable app-to-query seam types and
//!   transport-neutral error contract.
//! - `backends::mod` defines the internal plugin seam that keeps common runtime
//!   orchestration backend-blind.
//!
//! # Crate Relationships
//!
//! - `coral-app` is the management plane and supplies selected [`QuerySource`]
//!   values plus credential providers.
//! - `coral-spec` owns source-spec parsing, validation, and normalized
//!   declarative source models consumed by this engine.
//!
//! # Example
//!
//! ```no_run
//! use std::collections::BTreeMap;
//!
//! use coral_engine::{CoralQuery, QueryRuntimeConfig, QuerySource};
//! use coral_spec::parse_source_manifest_yaml;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//! # let source_spec = parse_source_manifest_yaml(
//! #     "name: demo\nversion: 0.1.0\ndsl_version: 3\nbackend: file\ntables: []",
//! # )?;
//! # let sources = vec![QuerySource::new(
//! #     source_spec,
//! #     BTreeMap::new(),
//! #     BTreeMap::new(),
//! # )];
//! # async fn demo(
//! #     sources: &[QuerySource],
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! let _ = CoralQuery::list_tables(
//!     sources,
//!     QueryRuntimeConfig::default(),
//!     None,
//!     None,
//!     None,
//! )
//! .await?;
//! # Ok(())
//! # }
//! # Ok(())
//! # }
//! ```
#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "wiremock is only used by the integration test target in this crate's dev-dependencies."
    )
)]

mod backends;
mod composition;
pub mod contracts;
mod runtime;
mod types;

pub use backends::database::DatabasePoolRegistry;
pub use backends::mcp::discover_tool_catalog as discover_mcp_tool_catalog;
pub use composition::{
    BoundRequestIdentityHttpAuthenticator, EngineExtensions, QueryResultObserver,
    QueryResultObserverError, RequestAuthenticator, RequestAuthenticatorError,
    RequestIdentityHttpAuthenticatorError, RequestIdentityHttpAuthenticatorFactory,
    RequestIdentitySelectionContext, RequestIdentitySelectionError, RequestIdentitySelector,
    SelectedRequestIdentity, SourceDecorator, SourceDecoratorError, SourceFailurePolicy,
    SourceInputResolutionContext, SourceInputResolver, SourceInputResolverError,
    SourceObservationPublisher, SourceObservationSurfaceKind, SourceScanObservation, SourceTables,
};
pub use contracts::{
    CatalogInfo, ColumnInfo, CoreError, DatabaseProviderDiscoveredRuntimeCatalog,
    DatabaseRuntimeBackend, DeclaredRuntimeCatalog, DependentJoinConfig, DependentJoinSourceConfig,
    DescribeTableInfo, EffectiveDependentJoinConfig, FileDeclaredRuntimeCatalog,
    FileRuntimeBackend, FileRuntimeRelation, FileRuntimeTableRelation, HttpDeclaredRuntimeCatalog,
    HttpRuntimeBackend, HttpRuntimeRelation, HttpRuntimeTableFunctionRelation,
    HttpRuntimeTableRelation, McpDeclaredRuntimeCatalog, McpRuntimeBackend, McpRuntimeRelation,
    McpRuntimeTableFunctionRelation, McpRuntimeTableRelation, MemorySize,
    ProviderDiscoveredRuntimeCatalog, QueryExecution, QueryExecutionProvenance, QueryMemoryConfig,
    QueryParameterValue, QueryParameters, QueryPlan, QueryRuntimeConfig, QueryRuntimeContext,
    QuerySource, QueryTableFunctionUsage, QueryTableUsage, QueryTestFailure, QueryTestResult,
    QueryTestSuccess, ResolvedQueryResources, RuntimeCatalog, RuntimeRelationKind,
    RuntimeRelationRef, RuntimeSourcePackage, SourceValidationReport, StatusCode,
    StructuredQueryError, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo, UdfRuntimeArgument, UdfRuntimeDefinition,
    UdfRuntimeImplementation, UdfRuntimePublish, UdfRuntimeResultColumn, UdfRuntimeSignature,
    UdfRuntimeSqlDefinition, UdfRuntimeTableFunctionPublish,
};
pub use runtime::normalize_catalog_name;

/// High-level query operations for the local query engine.
pub struct CoralQuery;

/// One request-scoped query runtime with its sources already registered.
///
/// The runtime can infer and install UDFs without rebuilding its underlying
/// `DataFusion` session. Its internals remain engine-owned.
pub struct PreparedQueryRuntime {
    inner: runtime::query::QueryRuntimeAdapter,
}

/// One logically planned query bound to its originating runtime.
///
/// Inspect [`Self::resources`] before consuming the query with [`Self::execute`].
pub struct PreparedQuery<'runtime> {
    runtime: &'runtime PreparedQueryRuntime,
    inner: runtime::query::PreparedSql,
}

impl PreparedQuery<'_> {
    /// Returns source resources referenced by the logical query plan.
    #[must_use]
    pub fn resources(&self) -> &ResolvedQueryResources {
        self.inner.resources()
    }

    /// Physically plans and executes this query.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if physical planning or execution fails.
    pub async fn execute(self) -> Result<QueryExecution, CoreError> {
        self.runtime.inner.execute_prepared(self.inner).await
    }
}

impl PreparedQueryRuntime {
    /// Lists queryable tables from this prepared runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if live catalog metadata cannot be collected.
    pub async fn list_tables(
        &self,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, CoreError> {
        Box::pin(
            self.inner
                .list_tables(catalog_filter, schema_filter, table_filter),
        )
        .await
    }

    /// Lists queryable catalog metadata from this prepared runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if live catalog metadata cannot be collected.
    pub async fn list_catalog(
        &self,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
    ) -> Result<CatalogInfo, CoreError> {
        Box::pin(self.inner.catalog_info(catalog_filter, schema_filter)).await
    }

    /// Describes one table from this prepared runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if live catalog metadata cannot be collected.
    pub async fn describe_table(
        &self,
        catalog_name: Option<&str>,
        schema_name: &str,
        table_name: &str,
    ) -> Result<DescribeTableInfo, CoreError> {
        Box::pin(
            self.inner
                .describe_table(catalog_name, schema_name, table_name),
        )
        .await
    }

    /// Infers typed signatures for multiple UDFs against this runtime.
    ///
    /// The outer result reports runtime failures. Each inner result reports
    /// validation for the UDF at the same input position.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if a UDF cannot be planned against the prepared
    /// source runtime.
    pub async fn infer_udf_signatures(
        &self,
        udfs: Vec<UdfRuntimeSqlDefinition>,
    ) -> Result<Vec<Result<UdfRuntimeSignature, CoreError>>, CoreError> {
        let mut results = Vec::with_capacity(udfs.len());
        for udf in udfs {
            if udf.sql().trim().is_empty() {
                results.push(Err(CoreError::InvalidInput(format!(
                    "udf '{}' SQL body cannot be empty",
                    udf.name
                ))));
                continue;
            }
            results.push(runtime::udfs::infer_udf_signature(&self.inner, &udf).await);
        }
        Ok(results)
    }

    /// Installs validated UDFs into this prepared runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if UDF publication conflicts with the prepared
    /// catalog or a UDF body cannot be planned.
    pub async fn with_udfs(mut self, udfs: Vec<UdfRuntimeDefinition>) -> Result<Self, CoreError> {
        self.inner.install_udfs(udfs).await?;
        Ok(self)
    }

    /// Executes one `SQL` statement over this prepared runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty or cannot be executed.
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryExecution, CoreError> {
        self.execute_sql_with_params(sql, QueryParameters::new())
            .await
    }

    /// Executes one parameterized `SQL` statement over this prepared runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty, parameter binding fails, or
    /// the statement cannot be executed.
    pub async fn execute_sql_with_params(
        &self,
        sql: &str,
        params: QueryParameters,
    ) -> Result<QueryExecution, CoreError> {
        self.prepare_sql_with_params(sql, params)
            .await?
            .execute()
            .await
    }

    /// Logically plans one `SQL` statement without physical execution.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty or cannot be logically planned.
    pub async fn prepare_sql(&self, sql: &str) -> Result<PreparedQuery<'_>, CoreError> {
        self.prepare_sql_with_params(sql, QueryParameters::new())
            .await
    }

    async fn prepare_sql_with_params(
        &self,
        sql: &str,
        params: QueryParameters,
    ) -> Result<PreparedQuery<'_>, CoreError> {
        if sql.trim().is_empty() {
            return Err(CoreError::InvalidInput("SQL must not be empty".to_string()));
        }
        Ok(PreparedQuery {
            runtime: self,
            inner: self.inner.prepare_sql(sql, params).await?,
        })
    }

    /// Explains one `SQL` statement against this prepared runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty or cannot be planned.
    pub async fn explain_sql(&self, sql: &str) -> Result<QueryPlan, CoreError> {
        self.explain_sql_with_params(sql, QueryParameters::new())
            .await
    }

    /// Explains one parameterized `SQL` statement against this runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty, parameter binding fails, or
    /// the statement cannot be planned.
    pub async fn explain_sql_with_params(
        &self,
        sql: &str,
        params: QueryParameters,
    ) -> Result<QueryPlan, CoreError> {
        if sql.trim().is_empty() {
            return Err(CoreError::InvalidInput("SQL must not be empty".to_string()));
        }
        self.inner.explain_sql(sql, &params).await
    }
}

impl CoralQuery {
    /// Builds one request-scoped runtime from the selected sources.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if source compilation, registration, or runtime
    /// construction fails.
    pub async fn prepare(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
    ) -> Result<PreparedQueryRuntime, CoreError> {
        Ok(PreparedQueryRuntime {
            inner: runtime::query::build_runtime(sources, runtime).await?,
        })
    }

    /// Lists queryable tables from the provided source set.
    ///
    /// When `schema_filter` is present, only tables for that visible `SQL`
    /// schema are returned.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if credential resolution fails, if any validated
    /// source spec cannot be compiled, or if the underlying query runtime
    /// cannot be built.
    pub async fn list_tables(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, CoreError> {
        Self::prepare(sources, runtime)
            .await?
            .list_tables(catalog_filter, schema_filter, table_filter)
            .await
    }

    /// Lists queryable catalog metadata from the provided source set.
    ///
    /// When `schema_filter` is present, only catalog items for that visible
    /// `SQL` schema are returned. Tables and source-scoped table functions are
    /// collected from one runtime build so callers see one consistent catalog
    /// snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if credential resolution fails, if any validated
    /// source spec cannot be compiled, or if the underlying query runtime
    /// cannot be built.
    pub async fn list_catalog(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        catalog_filter: Option<&str>,
        schema_filter: Option<&str>,
    ) -> Result<CatalogInfo, CoreError> {
        Self::prepare(sources, runtime)
            .await?
            .list_catalog(catalog_filter, schema_filter)
            .await
    }

    /// Describes one table or returns lightweight table metadata for missing-table help.
    ///
    /// This builds the runtime once, clones only the matched table on exact
    /// hits, and clones lightweight table metadata when the table is missing.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if credential resolution fails, if any validated
    /// source spec cannot be compiled, or if the underlying query runtime
    /// cannot be built.
    pub async fn describe_table(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        catalog_name: Option<&str>,
        schema_name: &str,
        table_name: &str,
    ) -> Result<DescribeTableInfo, CoreError> {
        Self::prepare(sources, runtime)
            .await?
            .describe_table(catalog_name, schema_name, table_name)
            .await
    }

    /// Executes one `SQL` statement over the provided source set.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty, if source compilation fails,
    /// or if the runtime cannot execute the statement.
    pub async fn execute_sql(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        sql: &str,
    ) -> Result<QueryExecution, CoreError> {
        Self::execute_sql_with_params(sources, runtime, sql, QueryParameters::new()).await
    }

    /// Executes one `SQL` statement with named query parameter values bound
    /// into its `$name` placeholders.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty, if source compilation fails,
    /// if a supplied parameter is not referenced by the statement, or if the
    /// runtime cannot execute the statement.
    pub async fn execute_sql_with_params(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        sql: &str,
        params: QueryParameters,
    ) -> Result<QueryExecution, CoreError> {
        if sql.trim().is_empty() {
            return Err(CoreError::InvalidInput("SQL must not be empty".to_string()));
        }

        Self::prepare(sources, runtime)
            .await?
            .execute_sql_with_params(sql, params)
            .await
    }

    /// Infers the typed signature for one UDF by planning its SQL against selected sources.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if source compilation fails, if the UDF SQL cannot
    /// plan against the selected sources, or if any SQL parameter has no
    /// inferred type.
    pub async fn infer_udf_signature(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        udf: UdfRuntimeSqlDefinition,
    ) -> Result<UdfRuntimeSignature, CoreError> {
        let mut results = Self::infer_udf_signatures(sources, runtime, vec![udf]).await?;
        results.pop().ok_or_else(|| {
            CoreError::InvalidInput("UDF signature inference returned no result".to_string())
        })?
    }

    /// Infers typed signatures for multiple UDFs through one source runtime.
    ///
    /// The outer result reports source runtime construction failures. Each
    /// inner result reports validation for the UDF at the same input position.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the shared source runtime cannot be built.
    pub async fn infer_udf_signatures(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        udfs: Vec<UdfRuntimeSqlDefinition>,
    ) -> Result<Vec<Result<UdfRuntimeSignature, CoreError>>, CoreError> {
        if udfs.is_empty() {
            return Ok(Vec::new());
        }

        Self::prepare(sources, runtime)
            .await?
            .infer_udf_signatures(udfs)
            .await
    }

    /// Explains one `SQL` statement with logical and physical plan renderings.
    ///
    /// The explanation is built against the provided source set and current
    /// runtime state. It does not execute the SQL statement.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty, if source compilation fails,
    /// or if the query engine cannot explain the statement.
    pub async fn explain_sql(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        sql: &str,
    ) -> Result<QueryPlan, CoreError> {
        Self::explain_sql_with_params(sources, runtime, sql, QueryParameters::new()).await
    }

    /// Explains one `SQL` statement with named query parameter values bound
    /// into its `$name` placeholders before planning output is rendered.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty, if source compilation fails,
    /// if a supplied parameter is not referenced by the statement, or if the
    /// query engine cannot explain the statement.
    pub async fn explain_sql_with_params(
        sources: &[QuerySource],
        runtime: QueryRuntimeConfig,
        sql: &str,
        params: QueryParameters,
    ) -> Result<QueryPlan, CoreError> {
        if sql.trim().is_empty() {
            return Err(CoreError::InvalidInput("SQL must not be empty".to_string()));
        }

        Self::prepare(sources, runtime)
            .await?
            .explain_sql_with_params(sql, params)
            .await
    }

    /// Validates that a single source can be initialized and queried.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if runtime construction fails or if the source
    /// cannot be registered or enumerated successfully.
    pub async fn test_source(
        source: &QuerySource,
        runtime: QueryRuntimeConfig,
    ) -> Result<Vec<TableInfo>, CoreError> {
        Ok(Self::validate_source(source, runtime, &[]).await?.tables)
    }

    /// Validates one source and then executes any declared validation queries.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the source cannot be initialized or enumerated.
    /// Query-test failures are reported in the returned outcome instead of
    /// failing the whole call.
    pub async fn validate_source(
        source: &QuerySource,
        runtime: QueryRuntimeConfig,
        test_queries: &[String],
    ) -> Result<SourceValidationReport, CoreError> {
        let query_runtime =
            runtime::query::build_runtime(std::slice::from_ref(source), runtime).await?;
        let source_name = source.source_name();
        let schema_names = source.schema_names();
        let catalog_names = source.catalog_names();
        let catalog = query_runtime
            .catalog_info_for_sources(&schema_names, &catalog_names)
            .await?;
        if catalog.tables.is_empty() && catalog.table_functions.is_empty() {
            if let Some(failure) = query_runtime.registration_failure(source_name) {
                return Err(CoreError::FailedPrecondition(failure.detail.clone()));
            }
            for name in schema_names.into_iter().chain(catalog_names) {
                if let Some(failure) = query_runtime.registration_failure(name) {
                    return Err(CoreError::FailedPrecondition(failure.detail.clone()));
                }
            }
            return Err(CoreError::FailedPrecondition(format!(
                "source '{source_name}' did not become queryable during validation"
            )));
        }

        let mut query_tests = Vec::with_capacity(test_queries.len());
        for sql in test_queries {
            match query_runtime
                .execute_sql(sql, &QueryParameters::new())
                .await
            {
                Ok(execution) => query_tests.push(QueryTestResult::success(
                    sql.clone(),
                    execution.row_count() as u64,
                )),
                Err(error) => {
                    let error_message = match &error {
                        CoreError::InvalidInput(detail) if is_non_read_only_sql_error(detail) => {
                            "test query must be read-only SQL".to_string()
                        }
                        _ => error.to_string(),
                    };
                    query_tests.push(QueryTestResult::failure(sql.clone(), error_message));
                }
            }
        }

        Ok(SourceValidationReport::new(
            catalog.tables,
            catalog.table_functions,
            query_tests,
        ))
    }
}

fn is_non_read_only_sql_error(detail: &str) -> bool {
    detail.starts_with("DDL not supported")
        || detail.starts_with("DML not supported")
        || detail.starts_with("Statement not supported")
}
