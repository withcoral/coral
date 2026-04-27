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
//! use coral_engine::{CoralQuery, QueryRuntimeContext, QueryRuntimeProvider, QuerySource};
//! use coral_spec::parse_source_manifest_yaml;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//! struct EmptyRuntime;
//!
//! impl QueryRuntimeProvider for EmptyRuntime {
//!     fn runtime_context(&self) -> QueryRuntimeContext {
//!         QueryRuntimeContext::default()
//!     }
//! }
//!
//! # let source_spec = parse_source_manifest_yaml(
//! #     "name: demo\nversion: 0.1.0\ndsl_version: 3\nbackend: jsonl\ntables: []",
//! # )?;
//! # let sources = vec![QuerySource::new(
//! #     source_spec,
//! #     BTreeMap::new(),
//! #     BTreeMap::new(),
//! # )];
//! # let provider = EmptyRuntime;
//! # async fn demo(
//! #     sources: &[QuerySource],
//! #     provider: &dyn QueryRuntimeProvider,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! let _ = CoralQuery::list_tables(sources, provider, None).await?;
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

pub use composition::{
    EngineExtensions, RequestAuthenticator, RequestAuthenticatorError, SourceDecorator,
    SourceDecoratorError, SourceFailurePolicy, SourceTables,
};
pub use contracts::{
    ColumnInfo, CoreError, QueryExecution, QueryRuntimeContext, QueryRuntimeProvider, QuerySource,
    QueryTestFailure, QueryTestResult, QueryTestSuccess, SourceValidationReport, StatusCode,
    StructuredQueryError, TableInfo,
};

/// High-level query operations for the local query engine.
pub struct CoralQuery;

impl CoralQuery {
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
        runtime: &dyn QueryRuntimeProvider,
        schema_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, CoreError> {
        Ok(runtime::query::build_runtime(sources, runtime)
            .await?
            .list_tables(schema_filter))
    }

    /// Executes one `SQL` statement over the provided source set.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] if the SQL is empty, if source compilation fails,
    /// or if the runtime cannot execute the statement.
    pub async fn execute_sql(
        sources: &[QuerySource],
        runtime: &dyn QueryRuntimeProvider,
        sql: &str,
    ) -> Result<QueryExecution, CoreError> {
        if sql.trim().is_empty() {
            return Err(CoreError::InvalidInput("SQL must not be empty".to_string()));
        }

        runtime::query::build_runtime(sources, runtime)
            .await?
            .execute_sql(sql)
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
        runtime: &dyn QueryRuntimeProvider,
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
        runtime: &dyn QueryRuntimeProvider,
        test_queries: &[String],
    ) -> Result<SourceValidationReport, CoreError> {
        let query_runtime =
            runtime::query::build_runtime(std::slice::from_ref(source), runtime).await?;
        let tables = query_runtime.list_tables(Some(source.source_name()));
        if tables.is_empty() {
            if let Some(failure) = query_runtime.registration_failure(source.source_name()) {
                return Err(CoreError::FailedPrecondition(failure.detail.clone()));
            }
            return Err(CoreError::FailedPrecondition(format!(
                "source '{}' did not become queryable during validation",
                source.source_name()
            )));
        }

        let mut query_tests = Vec::with_capacity(test_queries.len());
        for sql in test_queries {
            match query_runtime.execute_sql(sql).await {
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

        Ok(SourceValidationReport::new(tables, query_tests))
    }
}

fn is_non_read_only_sql_error(detail: &str) -> bool {
    detail.starts_with("DDL not supported")
        || detail.starts_with("DML not supported")
        || detail.starts_with("Statement not supported")
}
