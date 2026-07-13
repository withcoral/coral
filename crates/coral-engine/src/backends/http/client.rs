//! HTTP client orchestration for manifest-driven HTTP sources.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use opentelemetry::Context as OtelContext;
use serde_json::Value;

use crate::backends::BackendRegistrationContext;
use crate::backends::http::auth::ensure_auth_uses_credential_safe_transport;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpClientRoute {
    ProxyAware,
    DirectLoopback,
}

#[derive(Clone)]
pub(super) struct HttpClients {
    proxy_aware: reqwest::Client,
    direct_loopback: Option<reqwest::Client>,
}

impl HttpClients {
    pub(super) fn legacy(http: reqwest::Client) -> Self {
        Self {
            proxy_aware: http,
            direct_loopback: None,
        }
    }

    pub(super) fn credential_safe(
        proxy_aware: reqwest::Client,
        direct_loopback: reqwest::Client,
    ) -> Self {
        Self {
            proxy_aware,
            direct_loopback: Some(direct_loopback),
        }
    }

    pub(super) fn for_request(
        &self,
        url: &reqwest::Url,
        credential_bearing: bool,
    ) -> Result<&reqwest::Client> {
        match self.route_for_request(url, credential_bearing)? {
            HttpClientRoute::ProxyAware => Ok(&self.proxy_aware),
            HttpClientRoute::DirectLoopback => self.direct_loopback.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "direct HTTP route selected without a direct client".to_string(),
                )
            }),
        }
    }

    pub(super) fn proxy_aware(&self) -> &reqwest::Client {
        &self.proxy_aware
    }

    fn route_for_request(
        &self,
        url: &reqwest::Url,
        credential_bearing: bool,
    ) -> Result<HttpClientRoute> {
        if self.direct_loopback.is_none() || !credential_bearing {
            return Ok(HttpClientRoute::ProxyAware);
        }
        ensure_auth_uses_credential_safe_transport(url)?;
        Ok(if url.scheme() == "http" {
            HttpClientRoute::DirectLoopback
        } else {
            HttpClientRoute::ProxyAware
        })
    }
}

#[derive(Clone)]
pub(crate) struct HttpSourceClient {
    pub(super) http: HttpClients,
    pub(super) request_timeout: Duration,
    pub(super) source_schema: String,
    pub(super) base_url: ParsedTemplate,
    pub(super) auth: AuthSpec,
    pub(super) request_headers: Vec<HeaderSpec>,
    pub(super) request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    pub(super) require_credential_safe_auth_transport: bool,
    source_input_resolution_context: Option<SourceInputResolutionContext>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    pub(super) request_identity_http_authenticator: Option<BoundRequestIdentityHttpAuthenticator>,
    pub(super) rate_limit: RateLimitSpec,
    pub(super) secret_input_names: Arc<BTreeSet<String>>,
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
    http: HttpClients,
}

impl HttpSourceClientRuntime {
    pub(super) fn new(
        source_input_resolution_context: SourceInputResolutionContext,
        source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
        request_identity_http_authenticator: Option<BoundRequestIdentityHttpAuthenticator>,
        body_capture_max_bytes: Option<usize>,
        trace_context: Option<OtelContext>,
        http: HttpClients,
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
    fn static_inputs(
        body_capture_max_bytes: Option<usize>,
        http: reqwest::Client,
        credential_safe: bool,
    ) -> Self {
        let http = if credential_safe {
            HttpClients::credential_safe(http.clone(), http)
        } else {
            HttpClients::legacy(http)
        };
        Self::test_with_http_clients(body_capture_max_bytes, http, None)
    }

    #[cfg(test)]
    pub(super) fn test_with_http_clients(
        body_capture_max_bytes: Option<usize>,
        http: HttpClients,
        request_identity_http_authenticator: Option<BoundRequestIdentityHttpAuthenticator>,
    ) -> Self {
        Self {
            source_input_resolution_context: None,
            source_input_resolver: None,
            request_identity_http_authenticator,
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

pub(super) fn default_http_clients(
    registration: &BackendRegistrationContext,
    source_name: &str,
    credential_safe: bool,
) -> Result<HttpClients> {
    let proxy_aware = registration
        .default_http_client(credential_safe, || {
            let mut builder = default_http_client_builder();
            if credential_safe {
                builder = builder.redirect(reqwest::redirect::Policy::none());
            }
            builder.build().map_err(|error| error.to_string())
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to build HTTP client for source '{source_name}': {error}"
            ))
        })?;
    if !credential_safe {
        return Ok(HttpClients::legacy(proxy_aware));
    }

    let direct_loopback = registration
        .direct_credential_safe_http_client(|| {
            default_http_client_builder()
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .build()
                .map_err(|error| error.to_string())
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to build direct HTTP client for source '{source_name}': {error}"
            ))
        })?;
    Ok(HttpClients::credential_safe(proxy_aware, direct_loopback))
}

fn default_http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS))
        .user_agent(DEFAULT_HTTP_USER_AGENT)
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
            HttpSourceClientRuntime::static_inputs(
                body_capture_max_bytes,
                http,
                manifest.common.dsl_version == 4,
            ),
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
            require_credential_safe_auth_transport: manifest.common.dsl_version == 4,
            source_input_resolution_context: runtime.source_input_resolution_context,
            source_input_resolver: runtime.source_input_resolver,
            request_identity_http_authenticator: runtime.request_identity_http_authenticator,
            rate_limit: manifest.rate_limit.clone(),
            secret_input_names: Arc::new(manifest.declared_secret_names()),
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
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{HttpClientRoute, HttpClients, default_http_client_builder, default_http_clients};
    use crate::backends::BackendRegistrationContext;

    #[test]
    fn credential_safe_client_uses_direct_transport_only_for_loopback_http() {
        let clients = HttpClients::credential_safe(reqwest::Client::new(), reqwest::Client::new());

        for url in [
            "http://localhost:8080/items",
            "http://LOCALHOST/items",
            "http://127.0.0.1/items",
            "http://127.255.255.254/items",
            "http://[::1]/items",
        ] {
            let url = reqwest::Url::parse(url).expect("test URL");
            assert_eq!(
                clients
                    .route_for_request(&url, true)
                    .expect("safe credential URL"),
                HttpClientRoute::DirectLoopback,
                "{url}"
            );
        }
        for url in [
            "https://localhost/items",
            "https://127.0.0.1/items",
            "https://api.example.test/items",
        ] {
            let url = reqwest::Url::parse(url).expect("test URL");
            assert_eq!(
                clients
                    .route_for_request(&url, true)
                    .expect("safe credential URL"),
                HttpClientRoute::ProxyAware,
                "{url}"
            );
        }
        for url in [
            "http://localhost.example/items",
            "http://api.localhost/items",
            "http://0.0.0.0/items",
            "http://192.0.2.1/items",
            "http://[::2]/items",
            "http://[2001:db8::1]/items",
        ] {
            let url = reqwest::Url::parse(url).expect("test URL");
            clients
                .route_for_request(&url, true)
                .expect_err("unsafe credential URL");
        }

        let loopback = reqwest::Url::parse("http://127.0.0.1/items").expect("test URL");
        assert_eq!(
            clients
                .route_for_request(&loopback, false)
                .expect("anonymous v4 request"),
            HttpClientRoute::ProxyAware
        );
        let legacy = HttpClients::legacy(reqwest::Client::new());
        assert_eq!(
            legacy
                .route_for_request(&loopback, true)
                .expect("legacy credential request"),
            HttpClientRoute::ProxyAware
        );
    }

    #[tokio::test]
    async fn credential_safe_clients_do_not_follow_redirects() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/start"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", format!("{}/target", server.uri())),
            )
            .mount(&server)
            .await;

        let registration = BackendRegistrationContext::default();
        let clients =
            default_http_clients(&registration, "test", true).expect("credential-safe clients");
        for client in [
            &clients.proxy_aware,
            clients.direct_loopback.as_ref().expect("direct client"),
        ] {
            let response = client
                .get(format!("{}/start", server.uri()))
                .send()
                .await
                .expect("redirect response");

            assert_eq!(response.status(), reqwest::StatusCode::FOUND);
            assert_eq!(response.url().path(), "/start");
        }

        let legacy = default_http_clients(&registration, "test", false).expect("legacy client");
        let start_url =
            reqwest::Url::parse(&format!("{}/start", server.uri())).expect("redirect URL");
        let response = legacy
            .for_request(&start_url, false)
            .expect("legacy transport")
            .get(format!("{}/start", server.uri()))
            .send()
            .await
            .expect("followed redirect response");
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(response.url().path(), "/target");
    }

    #[tokio::test]
    async fn loopback_credential_client_bypasses_a_hostile_proxy() {
        let target = MockServer::start().await;
        let proxy = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/items"))
            .and(header("authorization", "Bearer runtime-secret"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&target)
            .await;

        let proxy_aware = default_http_client_builder()
            .redirect(reqwest::redirect::Policy::none())
            .proxy(reqwest::Proxy::all(proxy.uri()).expect("proxy URL"))
            .build()
            .expect("proxy-aware client");
        let direct_loopback = default_http_client_builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .expect("direct client");
        let clients = HttpClients::credential_safe(proxy_aware, direct_loopback);
        let url = reqwest::Url::parse(&format!("{}/items", target.uri())).expect("target URL");

        let response = clients
            .for_request(&url, true)
            .expect("loopback credential transport")
            .get(url.clone())
            .bearer_auth("runtime-secret")
            .send()
            .await
            .expect("loopback request");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert!(
            proxy
                .received_requests()
                .await
                .expect("proxy request recording")
                .is_empty(),
            "the proxy must receive neither the request nor its bearer token"
        );

        let response = clients
            .for_request(&url, false)
            .expect("anonymous v4 transport")
            .get(url)
            .send()
            .await
            .expect("proxied anonymous request");
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(
            proxy
                .received_requests()
                .await
                .expect("proxy request recording")
                .len(),
            1,
            "anonymous loopback traffic should retain proxy-aware behavior"
        );
    }
}
