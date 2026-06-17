//! Advanced composition seams for engine extension points.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::Value;

use crate::CoreError;
use crate::contracts::QuerySource;
use coral_spec::v4::IdentityRequirements;
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

/// Defines one neutral two-variant error type for an engine extension point.
///
/// Every extension seam reports failures the same way, so the error shape is
/// kept uniform on purpose: callers map `InvalidInput` to invalid-argument
/// handling and `FailedPrecondition` to precondition handling.
macro_rules! extension_error {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, thiserror::Error)]
        pub enum $name {
            /// The extension was configured with invalid input.
            #[error("{0}")]
            InvalidInput(String),
            /// The extension could not proceed because a precondition was unmet.
            #[error("{0}")]
            FailedPrecondition(String),
        }

        impl $name {
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
    };
}

extension_error!(
    /// Neutral error type for source-decoration failures.
    SourceDecoratorError
);

extension_error!(
    /// Neutral error type for query-result observer failures.
    QueryResultObserverError
);

extension_error!(
    /// Neutral error type for request-authenticator failures.
    RequestAuthenticatorError
);

extension_error!(
    /// Neutral error type for request-time source input resolution failures.
    SourceInputResolverError
);

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
        Self::from_query_source_with_declared_inputs(source, source.declared_inputs())
    }

    #[must_use]
    /// Builds request input-resolution context with a narrowed input contract.
    ///
    /// DSL v4 compiles each surface into its own executable HTTP source, so each
    /// synthesized surface should expose only the inputs declared by that
    /// surface rather than the union of every surface in the manifest.
    pub fn from_query_source_with_declared_inputs(
        source: &QuerySource,
        declared_inputs: &[ManifestInputSpec],
    ) -> Self {
        Self {
            source_name: Arc::from(source.source_name()),
            declared_inputs: Arc::from(declared_inputs.to_vec()),
            variables: Arc::new(source.variables().clone()),
            secrets: Arc::new(source.secrets().clone()),
        }
    }

    #[must_use]
    /// Returns the canonical installed source name.
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

/// Request-time identity-resolution context exposed to request identity resolvers.
#[derive(Debug, Clone)]
pub struct RequestIdentityResolutionContext {
    source_name: Arc<str>,
    surface_id: Arc<str>,
    identity_requirements: Arc<IdentityRequirements>,
}

impl RequestIdentityResolutionContext {
    #[must_use]
    /// Builds identity-resolution context for one executable source surface.
    pub fn new(
        source_name: impl Into<Arc<str>>,
        surface_id: impl Into<Arc<str>>,
        identity_requirements: IdentityRequirements,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            surface_id: surface_id.into(),
            identity_requirements: Arc::new(identity_requirements),
        }
    }

    #[must_use]
    /// Returns the canonical source name. This is also the SQL schema name.
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    #[must_use]
    /// Returns the DSL v4 surface id whose request is being executed.
    pub fn surface_id(&self) -> &str {
        &self.surface_id
    }

    #[must_use]
    /// Returns the identity requirements declared by the selected surface.
    pub fn identity_requirements(&self) -> &IdentityRequirements {
        &self.identity_requirements
    }

    #[must_use]
    /// Returns whether a candidate identity satisfies the selected surface's
    /// declared requirements.
    ///
    /// Accepted identity entries have OR semantics: the candidate is accepted if
    /// it satisfies any single accepted entry. Within one accepted entry, the
    /// candidate identity's spec id must be listed in `identity_specs`, and the
    /// declared `audience` must be a *subset* of the candidate `audience`: every
    /// required key/value must appear in the candidate, but the candidate may
    /// carry additional audience entries. An accepted entry with an empty
    /// `audience` therefore imposes no audience constraint.
    ///
    /// Audience values are compared by exact JSON equality, type included: a
    /// required `443` (integer) does not match a candidate `443.0` (float) or
    /// `"443"` (string). Resolvers must construct candidate audience values
    /// with the same JSON types the manifest declares.
    pub fn accepts_identity(
        &self,
        identity_spec_id: &str,
        audience: &BTreeMap<String, Value>,
    ) -> bool {
        self.identity_requirements.accepts.iter().any(|accepted| {
            accepted
                .identity_specs
                .iter()
                .any(|accepted_spec_id| accepted_spec_id == identity_spec_id)
                && accepted
                    .audience
                    .iter()
                    .all(|(key, required_value)| audience.get(key) == Some(required_value))
        })
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

extension_error!(
    /// Neutral error type for request identity-resolution failures.
    RequestIdentityResolverError
);

/// Request-time resolver for app-managed identities.
///
/// The engine calls this only when a DSL v4 surface declares identity
/// requirements and an outbound request is about to be sent.
#[async_trait]
pub trait RequestIdentityResolver: Send + Sync + std::fmt::Debug {
    /// Returns identity headers to append to the outbound request.
    ///
    /// # Errors
    ///
    /// Returns [`RequestIdentityResolverError`] when no suitable identity is
    /// bound, the identity is unhealthy, or identity material cannot be minted.
    async fn resolve_identity_headers(
        &self,
        identity: &RequestIdentityResolutionContext,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError>;
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
    use coral_spec::v4::{AcceptedIdentityRequirement, IdentityRequirements};
    use serde_json::{Value, json};

    use crate::{QuerySource, RequestIdentityResolutionContext, SourceInputResolutionContext};

    /// One-requirement identity context accepting `identity_specs` for the
    /// `github-rest-read` requirement with the given audience constraints.
    fn identity_context(
        identity_specs: &[&str],
        audience: BTreeMap<String, Value>,
    ) -> RequestIdentityResolutionContext {
        RequestIdentityResolutionContext::new(
            "github",
            "github_rest",
            IdentityRequirements {
                accepts: vec![AcceptedIdentityRequirement {
                    id: "github-rest-read".to_string(),
                    identity_specs: identity_specs.iter().map(ToString::to_string).collect(),
                    audience,
                }],
            },
        )
    }

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

    #[test]
    fn request_identity_context_matches_candidate_by_spec_id_and_audience() {
        let audience = BTreeMap::from([("host".to_string(), json!("github.com"))]);
        let context = identity_context(&["github_oauth", "github_pat"], audience.clone());

        assert!(context.accepts_identity("github_oauth", &audience));
        assert!(context.accepts_identity("github_pat", &audience));
        assert!(!context.accepts_identity("gitlab_oauth", &audience));
    }

    #[test]
    fn request_identity_context_audience_is_subset_and_json_type_exact() {
        let context = identity_context(
            &["github_oauth"],
            BTreeMap::from([("port".to_string(), json!(443))]),
        );
        let accepts =
            |audience: &BTreeMap<String, Value>| context.accepts_identity("github_oauth", audience);

        // Required entry present plus an extra candidate entry still matches
        // (declared audience is a subset of the candidate audience).
        assert!(accepts(&BTreeMap::from([
            ("port".to_string(), json!(443)),
            ("tenant".to_string(), json!("acme")),
        ])));
        // Exact JSON-type equality: float and string do not match the integer.
        assert!(!accepts(&BTreeMap::from([(
            "port".to_string(),
            json!(443.0)
        )])));
        assert!(!accepts(&BTreeMap::from([(
            "port".to_string(),
            json!("443")
        )])));
        // Missing required key does not match.
        assert!(!accepts(&BTreeMap::new()));
    }

    #[test]
    fn request_identity_context_empty_audience_imposes_no_constraint() {
        let context = identity_context(&["github_oauth"], BTreeMap::new());

        // An accepted shape with no declared audience matches any candidate
        // audience, but identity spec id still must match exactly.
        assert!(context.accepts_identity(
            "github_oauth",
            &BTreeMap::from([("host".to_string(), json!("github.com"))])
        ));
        assert!(!context.accepts_identity("gitlab_oauth", &BTreeMap::new()));
    }
}
