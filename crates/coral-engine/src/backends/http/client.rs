//! HTTP client orchestration for manifest-driven HTTP sources.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use serde_json::Value;

use crate::backends::BackendRegistrationContext;
use crate::backends::http::fetch::fetch_rows;
use crate::backends::http::registration_checks::validate_source_scoped_http_config;
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::http::trace::HttpBodyCapture;
use crate::{
    RequestAuthenticator, SourceInputResolutionContext, SourceInputResolver,
    SourceInputResolverError,
};
use coral_spec::backends::http::{HttpSourceManifest, RateLimitSpec};
use coral_spec::{AuthSpec, HeaderSpec, ParsedTemplate};

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
    pub(super) rate_limit: RateLimitSpec,
    pub(super) resolved_inputs: Arc<BTreeMap<String, String>>,
    pub(super) body_capture: HttpBodyCapture,
}

pub(crate) struct HttpSourceClientRuntime {
    source_input_resolution_context: Option<SourceInputResolutionContext>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    body_capture_max_bytes: Option<usize>,
    http: reqwest::Client,
}

impl HttpSourceClientRuntime {
    pub(crate) fn new(
        source_input_resolution_context: SourceInputResolutionContext,
        source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
        body_capture_max_bytes: Option<usize>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            source_input_resolution_context: Some(source_input_resolution_context),
            source_input_resolver,
            body_capture_max_bytes,
            http,
        }
    }

    #[cfg(test)]
    fn static_inputs(body_capture_max_bytes: Option<usize>, http: reqwest::Client) -> Self {
        Self {
            source_input_resolution_context: None,
            source_input_resolver: None,
            body_capture_max_bytes,
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
) -> Result<reqwest::Client> {
    registration
        .default_http_client(|| {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS))
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

impl HttpSourceClient {
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
            rate_limit: manifest.rate_limit.clone(),
            resolved_inputs: Arc::new(resolved_inputs),
            body_capture: HttpBodyCapture::new(runtime.body_capture_max_bytes),
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
        fetch_rows(self, target, filter_values, arg_values, sql_limit).await
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
