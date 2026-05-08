//! Advanced composition seams for engine extension points.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use reqwest::header::{HeaderName, HeaderValue};

use crate::{CoreError, contracts::QuerySource};

/// One source's table providers keyed by manifest table name.
pub type SourceTables = HashMap<String, Arc<dyn TableProvider>>;

/// Neutral bundle of optional engine extensions for one runtime build.
#[derive(Default)]
pub struct EngineExtensions {
    /// Registration-time table decorators for the selected source set.
    pub source_decorators: Vec<Box<dyn SourceDecorator>>,
    /// Post-query observers invoked after successful SQL result collection.
    pub query_result_observers: Vec<Arc<dyn QueryResultObserver>>,
    /// Request-time custom authenticators keyed by `auth.authenticator`.
    pub request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
}

/// Neutral policy decision for one source registration failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFailurePolicy {
    /// The failure does not require aborting the runtime build.
    Ignore,
    /// The original source failure should abort the runtime build.
    Abort,
}

/// Neutral error type for source-decoration failures.
#[derive(Debug, thiserror::Error)]
pub enum SourceDecoratorError {
    /// The decorator was configured with invalid input.
    #[error("{0}")]
    InvalidInput(String),
    /// The decorator could not proceed because a precondition was unmet.
    #[error("{0}")]
    FailedPrecondition(String),
}

impl SourceDecoratorError {
    #[must_use]
    /// Builds an invalid-input error.
    pub fn invalid_input(detail: impl Into<String>) -> Self {
        Self::InvalidInput(detail.into())
    }

    #[must_use]
    /// Builds a failed-precondition error.
    pub fn failed_precondition(detail: impl Into<String>) -> Self {
        Self::FailedPrecondition(detail.into())
    }
}

/// Neutral error type for query-result observer failures.
#[derive(Debug, thiserror::Error)]
pub enum QueryResultObserverError {
    /// The observer was configured with invalid input.
    #[error("{0}")]
    InvalidInput(String),
    /// The observer could not proceed because a precondition was unmet.
    #[error("{0}")]
    FailedPrecondition(String),
}

impl QueryResultObserverError {
    #[must_use]
    /// Builds an invalid-input error.
    pub fn invalid_input(detail: impl Into<String>) -> Self {
        Self::InvalidInput(detail.into())
    }

    #[must_use]
    /// Builds a failed-precondition error.
    pub fn failed_precondition(detail: impl Into<String>) -> Self {
        Self::FailedPrecondition(detail.into())
    }
}

/// Neutral error type for request-authenticator failures.
#[derive(Debug, thiserror::Error)]
pub enum RequestAuthenticatorError {
    /// The authenticator was configured with invalid input.
    #[error("{0}")]
    InvalidInput(String),
    /// The authenticator could not proceed because a precondition was unmet.
    #[error("{0}")]
    FailedPrecondition(String),
}

impl RequestAuthenticatorError {
    #[must_use]
    /// Builds an invalid-input error.
    pub fn invalid_input(detail: impl Into<String>) -> Self {
        Self::InvalidInput(detail.into())
    }

    #[must_use]
    /// Builds a failed-precondition error.
    pub fn failed_precondition(detail: impl Into<String>) -> Self {
        Self::FailedPrecondition(detail.into())
    }
}

/// Request-time HTTP authenticator registered through engine extensions.
pub trait RequestAuthenticator: Send + Sync + std::fmt::Debug {
    /// Stable authenticator name used in diagnostics and manifest dispatch.
    fn name(&self) -> &str;

    /// Returns the headers to apply to the fully built outbound request.
    ///
    /// # Errors
    ///
    /// Returns [`RequestAuthenticatorError`] if the auth config is malformed
    /// or the authenticator cannot mint request headers.
    fn authenticate(
        &self,
        auth: &coral_spec::CustomAuthSpec,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestAuthenticatorError>;

    /// Performs source-registration-time validation against resolved inputs.
    ///
    /// # Errors
    ///
    /// Returns [`RequestAuthenticatorError`] if the config or resolved inputs
    /// are insufficient for the authenticator to run.
    fn validate(
        &self,
        _auth: &coral_spec::CustomAuthSpec,
        _resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<(), RequestAuthenticatorError> {
        Ok(())
    }
}

/// Post-query hook for observing fully materialized successful query results.
///
/// Observers run synchronously on the query execution path after `DataFusion`
/// successfully collects result batches and before [`crate::QueryExecution`] is
/// returned. Observer work therefore contributes directly to `execute_sql`
/// latency, and observer failures fail the query after SQL execution has
/// succeeded. Implementations should keep in-band work lightweight; expensive
/// persistence, network calls, or telemetry fanout should be handed off to
/// background workers when they should not delay the query response.
///
/// Observers receive read-only references to the final SQL text, Arrow schema,
/// and result batches; implementations must not rely on mutating the returned
/// query result.
pub trait QueryResultObserver: Send + Sync {
    /// Stable observer name used in diagnostics.
    fn name(&self) -> &'static str;

    /// Observes one successful query result.
    ///
    /// # Errors
    ///
    /// Returns [`QueryResultObserverError`] if the observer cannot process the
    /// final result. Observer failures fail the query after SQL execution has
    /// succeeded.
    fn observe_result(
        &self,
        sql: &str,
        schema: &Schema,
        batches: &[RecordBatch],
    ) -> Result<(), QueryResultObserverError>;
}

/// Registration-time hook for wrapping or replacing a source's table providers.
///
/// Decorators can wrap successfully registered source tables and may also
/// observe selected-source failures to decide whether runtime construction
/// should abort.
pub trait SourceDecorator: Send + Sync {
    /// Stable decorator name used in diagnostics.
    fn name(&self) -> &'static str;

    /// Performs one-time setup before any sources are registered.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if the decorator cannot initialize.
    fn prepare(&mut self, _selected_sources: &[QuerySource]) -> Result<(), SourceDecoratorError> {
        Ok(())
    }

    /// Decorates the registered tables for one source before catalog insertion.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if the tables cannot be decorated.
    fn decorate_source(
        &mut self,
        source: &QuerySource,
        tables: SourceTables,
    ) -> Result<SourceTables, SourceDecoratorError>;

    /// Reports a selected source that failed during registration.
    ///
    /// Returning [`SourceFailurePolicy::Abort`] causes the original source
    /// registration error to abort runtime construction.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if the decorator cannot process the
    /// failure event.
    fn source_failed(
        &mut self,
        _source: &QuerySource,
        _error: &CoreError,
    ) -> Result<SourceFailurePolicy, SourceDecoratorError> {
        Ok(SourceFailurePolicy::Ignore)
    }

    /// Performs final validation after all source registration attempts finish.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if final invariants are not satisfied.
    fn finish(&mut self) -> Result<(), SourceDecoratorError> {
        Ok(())
    }
}
