//! HTTP client orchestration for manifest-driven HTTP sources.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use opentelemetry::Context as OtelContext;
use serde_json::Value;

use crate::backends::BackendRegistrationContext;
use crate::backends::http::auth::is_credential_safe_auth_transport;
use crate::backends::http::fetch::{FetchCompleteness, fetch_rows};
use crate::backends::http::filter_usage::{HttpRequestFilterUsage, http_request_filter_names};
use crate::backends::http::registration_checks::validate_source_scoped_http_config;
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::http::trace::HttpBodyCapture;
use crate::{
    BoundRequestIdentityHttpAuthenticator, RequestAuthenticator, SourceInputResolutionContext,
    SourceInputResolver, SourceInputResolverError,
};
use coral_spec::backends::http::{HttpSourceManifest, RateLimitSpec};
use coral_spec::{AuthSpec, HeaderSpec, ParsedTemplate, RequestSpec as ManifestRequestSpec};

const DEFAULT_HTTP_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_HTTP_USER_AGENT: &str = concat!("coral/", env!("CARGO_PKG_VERSION"));

#[derive(Clone)]
pub(crate) struct HttpSourceClient {
    pub(super) http: reqwest::Client,
    pub(super) request_timeout: Duration,
    pub(super) source_schema: String,
    pub(super) base_url: ParsedTemplate,
    pub(super) auth: AuthSpec,
    pub(super) request_headers: Vec<HeaderSpec>,
    pub(super) request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    source_input_resolution_context: Option<SourceInputResolutionContext>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    pub(super) request_identity_http_authenticator: Option<BoundRequestIdentityHttpAuthenticator>,
    pub(super) rate_limit: RateLimitSpec,
    pub(super) resolved_inputs: Arc<BTreeMap<String, String>>,
    pub(super) body_capture: HttpBodyCapture,
    pub(super) trace_context: Option<OtelContext>,
}

pub(crate) struct HttpSourceClientRuntime {
    source_input_resolution_context: Option<SourceInputResolutionContext>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    request_identity_http_authenticator: Option<BoundRequestIdentityHttpAuthenticator>,
    body_capture_max_bytes: Option<usize>,
    trace_context: Option<OtelContext>,
    http: reqwest::Client,
}

impl HttpSourceClientRuntime {
    pub(crate) fn new(
        source_input_resolution_context: SourceInputResolutionContext,
        source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
        request_identity_http_authenticator: Option<BoundRequestIdentityHttpAuthenticator>,
        body_capture_max_bytes: Option<usize>,
        trace_context: Option<OtelContext>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            source_input_resolution_context: Some(source_input_resolution_context),
            source_input_resolver,
            request_identity_http_authenticator,
            body_capture_max_bytes,
            trace_context,
            http,
        }
    }

    #[cfg(test)]
    fn static_inputs(body_capture_max_bytes: Option<usize>, http: reqwest::Client) -> Self {
        Self {
            source_input_resolution_context: None,
            source_input_resolver: None,
            request_identity_http_authenticator: None,
            body_capture_max_bytes,
            trace_context: None,
            http,
        }
    }
}

impl std::fmt::Debug for HttpSourceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSourceClient")
            .field("source_schema", &self.source_schema)
            .field("base_url", &self.base_url)
            .field("auth", &self.auth)
            .field("request_headers", &self.request_headers)
            .field("rate_limit", &self.rate_limit)
            .field("body_capture", &self.body_capture)
            .finish_non_exhaustive()
    }
}

pub(super) fn default_http_client(
    registration: &BackendRegistrationContext,
    source_name: &str,
    credential_safe: bool,
) -> Result<reqwest::Client> {
    registration
        .default_http_client(credential_safe, || {
            let redirect_policy = if credential_safe {
                credential_safe_redirect_policy()
            } else {
                source_redirect_policy()
            };
            reqwest::Client::builder()
                .timeout(Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS))
                .redirect(redirect_policy)
                .user_agent(DEFAULT_HTTP_USER_AGENT)
                .build()
                .map_err(|error| error.to_string())
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to build HTTP client for source '{source_name}': {error}"
            ))
        })
}

fn credential_safe_redirect_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        let allowed = attempt
            .previous()
            .last()
            .is_some_and(|previous| credential_safe_redirect_target(previous, attempt.url()));
        if allowed {
            reqwest::redirect::Policy::default().redirect(attempt)
        } else {
            attempt.error("credential-bearing HTTP source redirect rejected")
        }
    })
}

fn credential_safe_redirect_target(previous: &reqwest::Url, next: &reqwest::Url) -> bool {
    previous.origin() == next.origin() && is_credential_safe_auth_transport(next)
}

fn source_redirect_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() > 10 {
            return attempt.error(std::io::Error::other("too many redirects"));
        }
        let Some(previous) = attempt.previous().last() else {
            return attempt.follow();
        };
        if same_origin(previous, attempt.url()) {
            attempt.follow()
        } else {
            attempt.error(std::io::Error::other(
                "cross-origin redirects are disabled for source HTTP requests",
            ))
        }
    })
}

fn same_origin(left: &reqwest::Url, right: &reqwest::Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

impl HttpSourceClient {
    pub(crate) fn request_filter_names(&self, request: &ManifestRequestSpec) -> HashSet<String> {
        http_request_filter_names(&self.base_url, &self.request_headers, request)
    }

    pub(crate) fn filter_usage(&self) -> HttpRequestFilterUsage {
        HttpRequestFilterUsage::new(self.base_url.clone(), self.request_headers.clone())
    }

    /// Build a backend client from a validated source spec.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if required credentials are missing or if an
    /// authentication header template cannot be resolved.
    #[cfg(test)]
    pub(crate) fn from_manifest(
        manifest: &HttpSourceManifest,
        source_secrets: &BTreeMap<String, String>,
        source_variables: &BTreeMap<String, String>,
        request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
        body_capture_max_bytes: Option<usize>,
        http: reqwest::Client,
    ) -> Result<Self> {
        Self::build(
            manifest,
            source_secrets,
            source_variables,
            request_authenticators,
            HttpSourceClientRuntime::static_inputs(body_capture_max_bytes, http),
        )
    }

    pub(crate) fn from_manifest_with_source_input_resolver(
        manifest: &HttpSourceManifest,
        source_secrets: &BTreeMap<String, String>,
        source_variables: &BTreeMap<String, String>,
        request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
        runtime: HttpSourceClientRuntime,
    ) -> Result<Self> {
        Self::build(
            manifest,
            source_secrets,
            source_variables,
            request_authenticators,
            runtime,
        )
    }

    fn build(
        manifest: &HttpSourceManifest,
        source_secrets: &BTreeMap<String, String>,
        source_variables: &BTreeMap<String, String>,
        request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
        runtime: HttpSourceClientRuntime,
    ) -> Result<Self> {
        let resolved_inputs =
            coral_spec::resolve_inputs(&manifest.declared_inputs, source_secrets, source_variables);
        validate_source_scoped_http_config(manifest, request_authenticators, &resolved_inputs)?;

        let request_timeout = Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS);

        Ok(Self {
            http: runtime.http,
            request_timeout,
            source_schema: manifest.common.name.clone(),
            base_url: manifest.base_url.clone(),
            auth: manifest.auth.clone(),
            request_headers: manifest.request_headers.clone(),
            request_authenticators: request_authenticators.clone(),
            source_input_resolution_context: runtime.source_input_resolution_context,
            source_input_resolver: runtime.source_input_resolver,
            request_identity_http_authenticator: runtime.request_identity_http_authenticator,
            rate_limit: manifest.rate_limit.clone(),
            resolved_inputs: Arc::new(resolved_inputs),
            body_capture: HttpBodyCapture::new(runtime.body_capture_max_bytes),
            trace_context: runtime.trace_context,
        })
    }

    /// Fetch rows for a single table from the backend API.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if request templates cannot be resolved, the
    /// `HTTP` request fails, the response payload cannot be interpreted, or the
    /// fetched rows cannot be extracted for the table strategy.
    pub(crate) async fn fetch(
        &self,
        target: &HttpFetchTarget,
        filter_values: &HashMap<String, String>,
        arg_values: &HashMap<String, String>,
        sql_limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        fetch_rows(
            self,
            target,
            filter_values,
            arg_values,
            sql_limit.or(target.fetch_limit_default()),
            sql_limit,
            FetchCompleteness::Default,
        )
        .await
    }

    pub(crate) async fn fetch_complete(
        &self,
        target: &HttpFetchTarget,
        filter_values: &HashMap<String, String>,
        arg_values: &HashMap<String, String>,
        row_limit: Option<usize>,
        page_hint: Option<usize>,
    ) -> Result<Vec<Value>> {
        fetch_rows(
            self,
            target,
            filter_values,
            arg_values,
            row_limit,
            page_hint,
            FetchCompleteness::Complete,
        )
        .await
    }

    pub(super) async fn resolved_inputs_for_request(
        &self,
    ) -> Result<Arc<BTreeMap<String, String>>> {
        let (Some(resolver), Some(source)) = (
            &self.source_input_resolver,
            &self.source_input_resolution_context,
        ) else {
            return Ok(Arc::clone(&self.resolved_inputs));
        };
        resolver
            .resolve_inputs(source)
            .await
            .map(Arc::new)
            .map_err(source_input_error)
    }
}

fn source_input_error(error: SourceInputResolverError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::credential_safe_redirect_target;

    #[test]
    fn credential_safe_redirects_stay_on_a_safe_origin() {
        for (previous, next, allowed) in [
            ("https://api.test/a", "https://api.test/b", true),
            ("http://127.0.0.1:8080/a", "http://127.0.0.1:8080/b", true),
            ("https://api.test/a", "https://other.test/b", false),
            ("https://api.test/a", "http://api.test/b", false),
            ("http://127.0.0.1:8080/a", "http://127.0.0.1:8081/b", false),
            ("http://api.test/a", "http://api.test/b", false),
        ] {
            let previous = reqwest::Url::parse(previous).expect("previous URL");
            let next = reqwest::Url::parse(next).expect("next URL");
            assert_eq!(credential_safe_redirect_target(&previous, &next), allowed);
        }
    }
}
