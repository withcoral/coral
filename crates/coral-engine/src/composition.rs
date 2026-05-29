//! Advanced composition seams for engine extension points.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use reqwest::header::{HeaderName, HeaderValue};

use crate::CoreError;
use crate::contracts::QuerySource;
use coral_spec::{ManifestInputKind, ManifestInputSpec};

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
    /// Request-time resolver for app-managed source inputs.
    pub source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
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

/// Neutral error type for request-time source input resolution failures.
#[derive(Debug, thiserror::Error)]
pub enum SourceInputResolverError {
    /// The resolver was configured with invalid input.
    #[error("{0}")]
    InvalidInput(String),
    /// The resolver could not proceed because a precondition was unmet.
    #[error("{0}")]
    FailedPrecondition(String),
}

impl SourceInputResolverError {
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

/// Request-time source input-resolution context exposed to source input resolvers.
///
/// This carries only the source identity and declared input state needed to
/// refresh app-managed inputs before an outbound source request. It deliberately
/// avoids carrying the full validated source manifest, because backends clone
/// request state for each registered table and table function.
#[derive(Debug, Clone)]
pub struct SourceInputResolutionContext {
    source_name: Arc<str>,
    declared_inputs: Arc<[ManifestInputSpec]>,
    variables: Arc<BTreeMap<String, String>>,
    secrets: Arc<BTreeMap<String, String>>,
}

impl SourceInputResolutionContext {
    #[must_use]
    /// Builds request input-resolution context from one selected query source.
    pub fn from_query_source(source: &QuerySource) -> Self {
        Self {
            source_name: Arc::from(source.source_name()),
            declared_inputs: Arc::from(source.source_spec().declared_inputs().to_vec()),
            variables: Arc::new(source.variables().clone()),
            secrets: Arc::new(source.secrets().clone()),
        }
    }

    #[must_use]
    /// Returns the canonical source name. This is also the SQL schema name.
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    #[must_use]
    /// Returns the declared source inputs in authored order.
    pub fn declared_inputs(&self) -> &[ManifestInputSpec] {
        &self.declared_inputs
    }

    #[must_use]
    /// Returns configured non-secret source variables.
    pub fn variables(&self) -> &BTreeMap<String, String> {
        &self.variables
    }

    #[must_use]
    /// Returns resolved declared source secrets available to request-time resolvers.
    pub fn secrets(&self) -> &BTreeMap<String, String> {
        &self.secrets
    }

    #[must_use]
    /// Returns required declared secret names.
    pub fn required_secret_names(&self) -> Vec<String> {
        self.declared_inputs
            .iter()
            .filter(|input| input.kind == ManifestInputKind::Secret && input.required)
            .map(|input| input.key.clone())
            .collect()
    }

    #[must_use]
    /// Returns a new context with refreshed secret values.
    pub fn with_secrets(&self, secrets: BTreeMap<String, String>) -> Self {
        Self {
            source_name: Arc::clone(&self.source_name),
            declared_inputs: Arc::clone(&self.declared_inputs),
            variables: Arc::clone(&self.variables),
            secrets: Arc::new(secrets),
        }
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

/// Request-time resolver for source inputs owned by the app layer.
///
/// The engine calls this only when a selected source is about to issue an
/// outbound request, allowing app-managed credentials to refresh lazily.
#[async_trait]
pub trait SourceInputResolver: Send + Sync + std::fmt::Debug {
    /// Returns current resolved inputs for the selected source.
    ///
    /// # Errors
    ///
    /// Returns [`SourceInputResolverError`] when app-managed inputs cannot be
    /// resolved for active source use.
    async fn resolve_inputs(
        &self,
        source: &SourceInputResolutionContext,
    ) -> Result<BTreeMap<String, String>, SourceInputResolverError>;
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_spec::parse_source_manifest_value;
    use serde_json::json;

    use crate::{QuerySource, SourceInputResolutionContext};

    #[test]
    fn source_input_resolution_context_keeps_only_request_input_contract() {
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "secured_messages",
            "version": "0.1.0",
            "backend": "http",
            "inputs": {
                "API_BASE": {
                    "kind": "variable",
                    "default": "https://api.example.com"
                },
                "API_TOKEN": {
                    "kind": "secret"
                },
                "OPTIONAL_TOKEN": {
                    "kind": "secret",
                    "required": false
                }
            },
            "base_url": "{{input.API_BASE}}",
            "tables": [{
                "name": "messages",
                "description": "Messages",
                "request": {
                    "method": "GET",
                    "path": "/messages"
                },
                "response": {},
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }))
        .expect("parse source manifest");
        let source = QuerySource::new(
            manifest,
            BTreeMap::from([(
                "API_BASE".to_string(),
                "https://configured.example.com".to_string(),
            )]),
            BTreeMap::from([
                ("API_TOKEN".to_string(), "stale-token".to_string()),
                ("OPTIONAL_TOKEN".to_string(), "optional-token".to_string()),
            ]),
        );

        let context = SourceInputResolutionContext::from_query_source(&source);

        assert_eq!(context.source_name(), "secured_messages");
        assert_eq!(
            context.variables().get("API_BASE").map(String::as_str),
            Some("https://configured.example.com")
        );
        assert_eq!(
            context.secrets().get("API_TOKEN").map(String::as_str),
            Some("stale-token")
        );
        assert_eq!(
            context
                .declared_inputs()
                .iter()
                .map(|input| input.key.as_str())
                .collect::<Vec<_>>(),
            vec!["API_BASE", "API_TOKEN", "OPTIONAL_TOKEN"]
        );
        assert_eq!(
            context.required_secret_names(),
            vec!["API_TOKEN".to_string()]
        );

        let refreshed = context.with_secrets(BTreeMap::from([(
            "API_TOKEN".to_string(),
            "fresh-token".to_string(),
        )]));

        assert_eq!(refreshed.source_name(), context.source_name());
        assert_eq!(refreshed.declared_inputs(), context.declared_inputs());
        assert_eq!(refreshed.variables(), context.variables());
        assert_eq!(
            refreshed.secrets().get("API_TOKEN").map(String::as_str),
            Some("fresh-token")
        );
        assert!(!refreshed.secrets().contains_key("OPTIONAL_TOKEN"));
    }
}
