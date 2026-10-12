//! Client ID Metadata Document fetching and validation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::Deserialize;
use thiserror::Error;
use zeroize::Zeroizing;

use crate::oauth_resource::CanonicalOauthUrl;
use coral_outbound::{
    ClientMetadata, Configured, EndpointUrl, OutboundUrlPolicyError, PUBLIC_METADATA_TIMEOUT,
    read_bounded_body,
};

const CLIENT_METADATA_MAX_BYTES: usize = 5 * 1024;
pub(super) use coral_auth::oauth_clients::{
    ClientMetadataError, ClientMetadataResolver, OAuthClientRegistration,
};
use coral_auth::oauth_clients::{valid_client_name, validate_oauth_redirect_uri};

/// Production resolver backed by the hardened client-metadata HTTP client.
#[derive(Clone)]
pub(super) struct HttpClientMetadataResolver {
    issuer: EndpointUrl<Configured>,
    allowed_loopback_client_ids: BTreeSet<String>,
    configured_reef_metadata: BTreeMap<String, EndpointUrl<Configured>>,
    fetcher: Arc<dyn MetadataFetcher>,
}

impl HttpClientMetadataResolver {
    pub(super) fn new(
        issuer: &str,
        authorization_resources: &BTreeSet<String>,
        policy: coral_outbound::DestinationPolicy,
    ) -> Result<Self, ClientMetadataError> {
        Self::with_fetcher(
            issuer,
            authorization_resources,
            Arc::new(ReqwestMetadataFetcher::new(policy)?),
        )
    }

    fn with_fetcher(
        issuer: &str,
        authorization_resources: &BTreeSet<String>,
        fetcher: Arc<dyn MetadataFetcher>,
    ) -> Result<Self, ClientMetadataError> {
        let issuer = EndpointUrl::<Configured>::parse(issuer)
            .map_err(|_error| ClientMetadataError::InvalidConfiguration)?;
        let allowed_loopback_client_ids =
            derive_allowed_loopback_client_ids(authorization_resources)?;
        Ok(Self {
            issuer,
            allowed_loopback_client_ids,
            configured_reef_metadata: derive_configured_reef_metadata(authorization_resources)?,
            fetcher,
        })
    }

    /// Rejects a trusted client ID no request through this resolver could name.
    ///
    /// Config validation holds `trusted_clients` entries to canonical URL
    /// spellings, but whether an entry can ever equal an accepted client ID
    /// also depends on the served topology — the issuer's scheme and the
    /// loopback IDs derived from registered resources — which only exists once
    /// this resolver does. An entry that fails here would otherwise pass
    /// startup and then never match, and its runtime symptom — the approval
    /// page still appearing — points nowhere near the config that caused it.
    ///
    /// The rejection is rendered because an operator reading a startup failure
    /// is the one caller entitled to know which rule the entry broke.
    pub(super) fn check_trusted_client_id(&self, client_id: &str) -> Result<(), String> {
        self.accepted_metadata_url(client_id)
            .map(|_metadata_url| ())
            .map_err(|rejection| rejection.to_string())
    }

    /// Resolves `client_id` to the URL this server would fetch its metadata
    /// from, rejecting an ID it does not accept.
    ///
    /// This is the single definition of an acceptable client ID. The request
    /// path and the `trusted_clients` startup check both go through it, so the
    /// URL policy and the canonical-identity rule cannot come to disagree
    /// about which IDs this server honors — the divergence that otherwise lets
    /// a configured trusted client pass startup and then silently never match.
    fn accepted_metadata_url(&self, client_id: &str) -> Result<MetadataUrl, ClientIdRejection> {
        // Look up the exact bytes before parsing caller input. The fetched URL
        // is constructed from operator configuration, never from a request.
        // This grants neither an origin-wide exception nor consent bypass.
        if let Some(metadata_url) = self.configured_reef_metadata.get(client_id) {
            return Ok(MetadataUrl::ConfiguredReef(metadata_url.clone()));
        }
        let metadata_url = EndpointUrl::<ClientMetadata>::parse(
            client_id,
            &self.issuer,
            &self.allowed_loopback_client_ids,
        )
        .map_err(ClientIdRejection::Policy)?;
        // A client ID must already be the URL Coral fetches, character for
        // character. Parsing normalizes — it strips tab, CR, and LF from
        // anywhere in the input, trims surrounding spaces, lowercases the host,
        // and drops a default port — so without this check one document is
        // reachable under unboundedly many client IDs that all fetch it. The ID
        // is the client's identity: it is echoed back by the document, recorded
        // on an approval, and compared at the token endpoint, so it has to be a
        // single canonical string rather than a family of equivalent ones.
        if metadata_url.as_url().as_str() != client_id {
            return Err(ClientIdRejection::NotCanonical);
        }
        Ok(MetadataUrl::PublicClient(metadata_url))
    }
}

/// Keep operator-selected Reef requests separate from untrusted CIMD fetches.
/// Only the former may use private HTTPS destinations; both validate TLS and
/// apply the same response, timeout, and no-redirect constraints.
enum MetadataUrl {
    ConfiguredReef(EndpointUrl<Configured>),
    PublicClient(EndpointUrl<ClientMetadata>),
}

fn derive_configured_reef_metadata(
    authorization_resources: &BTreeSet<String>,
) -> Result<BTreeMap<String, EndpointUrl<Configured>>, ClientMetadataError> {
    let mut metadata = BTreeMap::new();
    for value in authorization_resources {
        let resource = CanonicalOauthUrl::parse(value)
            .map_err(|_error| ClientMetadataError::InvalidConfiguration)?;
        if resource.identifier() != value {
            return Err(ClientMetadataError::InvalidConfiguration);
        }
        // Reef's configured resource is an origin. A Workspace MCP resource
        // or any other path-scoped resource cannot grant private fetch access.
        if resource.url().scheme() == "https" && resource.url().path() == "/" {
            let client_id = format!("{value}/.well-known/oauth-client");
            let endpoint = EndpointUrl::<Configured>::parse(&client_id)
                .map_err(|_error| ClientMetadataError::InvalidConfiguration)?;
            metadata.insert(client_id, endpoint);
        }
    }
    Ok(metadata)
}

/// Why a client ID is not one this server accepts.
///
/// # Trust
///
/// Only the startup check for `trusted_clients` renders this. [`resolve`]
/// maps every variant onto the opaque [`ClientMetadataError::InvalidClientId`]
/// deliberately: the party that supplied the ID learns that it was rejected
/// and nothing about which rule rejected it. Letting this detail out of
/// `resolve` to improve its errors would hand a caller a description of the
/// policy its input just failed, so these variants exist for an operator
/// reading a startup error and for no one else.
///
/// [`resolve`]: HttpClientMetadataResolver::resolve
#[derive(Debug, Error)]
enum ClientIdRejection {
    #[error("{0}")]
    Policy(OutboundUrlPolicyError),
    #[error("client ID must be the canonical form of the URL it names")]
    NotCanonical,
}

fn derive_allowed_loopback_client_ids(
    authorization_resources: &BTreeSet<String>,
) -> Result<BTreeSet<String>, ClientMetadataError> {
    authorization_resources
        .iter()
        .try_fold(BTreeSet::new(), |mut client_ids, value| {
            let resource = CanonicalOauthUrl::parse(value)
                .map_err(|_error| ClientMetadataError::InvalidConfiguration)?;
            if resource.identifier() != value {
                return Err(ClientMetadataError::InvalidConfiguration);
            }
            let url = resource.url();
            if url.scheme() == "http" && url.path() == "/" {
                let client_id = url
                    .join("/.well-known/oauth-client")
                    .map_err(|_error| ClientMetadataError::InvalidConfiguration)?;
                client_ids.insert(client_id.to_string());
            }
            Ok(client_ids)
        })
}

#[async_trait::async_trait]
impl ClientMetadataResolver for HttpClientMetadataResolver {
    async fn resolve(
        &self,
        client_id: &str,
    ) -> Result<OAuthClientRegistration, ClientMetadataError> {
        // Every rejection collapses into one opaque error, by design: see
        // `ClientIdRejection`.
        let metadata_url = self
            .accepted_metadata_url(client_id)
            .map_err(|_rejection| ClientMetadataError::InvalidClientId)?;
        let response = self.fetcher.fetch(&metadata_url).await?;
        registration_from_response(client_id, response).await
    }
}

#[async_trait::async_trait]
trait MetadataFetcher: Send + Sync {
    async fn fetch(
        &self,
        metadata_url: &MetadataUrl,
    ) -> Result<reqwest::Response, ClientMetadataError>;
}

struct ReqwestMetadataFetcher {
    public_http: coral_outbound::Client,
    configured_http: reqwest::Client,
}

impl ReqwestMetadataFetcher {
    fn new(policy: coral_outbound::DestinationPolicy) -> Result<Self, ClientMetadataError> {
        let public_http = coral_outbound::Client::new(policy, PUBLIC_METADATA_TIMEOUT)
            .map_err(|_error| ClientMetadataError::ClientBuild)?;
        let configured_http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(PUBLIC_METADATA_TIMEOUT)
            .timeout(PUBLIC_METADATA_TIMEOUT)
            .build()
            .map_err(|_error| ClientMetadataError::ClientBuild)?;
        Ok(Self {
            public_http,
            configured_http,
        })
    }
}

#[async_trait::async_trait]
impl MetadataFetcher for ReqwestMetadataFetcher {
    async fn fetch(
        &self,
        metadata_url: &MetadataUrl,
    ) -> Result<reqwest::Response, ClientMetadataError> {
        let request = match metadata_url {
            MetadataUrl::ConfiguredReef(url) => url.get(&self.configured_http),
            MetadataUrl::PublicClient(url) => self
                .public_http
                .get(url.as_url())
                .map_err(|_error| ClientMetadataError::Fetch)?,
        };
        request
            .send()
            .await
            .map_err(|_error| ClientMetadataError::Fetch)
    }
}

#[derive(Deserialize)]
struct ClientMetadataDocument {
    client_id: String,
    redirect_uris: Vec<String>,
    client_name: String,
    token_endpoint_auth_method: Option<String>,
    grant_types: Option<Vec<String>>,
    response_types: Option<Vec<String>>,
}

async fn registration_from_response(
    client_id: &str,
    response: reqwest::Response,
) -> Result<OAuthClientRegistration, ClientMetadataError> {
    let status = response.status();
    if status.is_redirection() {
        return Err(ClientMetadataError::Redirect);
    }
    if status != reqwest::StatusCode::OK {
        return Err(ClientMetadataError::HttpStatus);
    }
    let body = read_bounded_body(response, CLIENT_METADATA_MAX_BYTES)
        .await
        .map_err(|error| map_body_error(&error))?;
    let body = Zeroizing::new(body);
    let document =
        serde_json::from_slice(&body).map_err(|_error| ClientMetadataError::InvalidJson)?;
    validate_document(client_id, document)
}

fn validate_document(
    client_id: &str,
    document: ClientMetadataDocument,
) -> Result<OAuthClientRegistration, ClientMetadataError> {
    if document.client_id != client_id {
        return Err(ClientMetadataError::InvalidMetadata);
    }
    if !valid_client_name(&document.client_name) {
        return Err(ClientMetadataError::InvalidMetadata);
    }
    // A client publishes one document for every authorization server, so it
    // may list redirects this server cannot use. Such an entry can never match
    // a request; drop it rather than refuse the client's usable redirects.
    let redirect_uris: Vec<String> = document
        .redirect_uris
        .into_iter()
        .filter(|uri| validate_oauth_redirect_uri(uri).is_ok())
        .collect();
    if redirect_uris.is_empty() {
        return Err(ClientMetadataError::InvalidMetadata);
    }
    if document
        .token_endpoint_auth_method
        .as_deref()
        .unwrap_or("none")
        != "none"
    {
        return Err(ClientMetadataError::InvalidMetadata);
    }
    if document
        .grant_types
        .as_ref()
        .is_some_and(|values| !values.iter().any(|value| value == "authorization_code"))
    {
        return Err(ClientMetadataError::InvalidMetadata);
    }
    if document
        .response_types
        .as_ref()
        .is_some_and(|values| !values.iter().any(|value| value == "code"))
    {
        return Err(ClientMetadataError::InvalidMetadata);
    }
    Ok(OAuthClientRegistration {
        redirect_uris,
        client_name: document.client_name,
    })
}

fn map_body_error(error: &OutboundUrlPolicyError) -> ClientMetadataError {
    match error {
        OutboundUrlPolicyError::BodyTooLarge { .. } => ClientMetadataError::BodyTooLarge,
        _ => ClientMetadataError::Fetch,
    }
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "metadata JSON fixture mutation")]
mod tests {
    use crate::auth::test_config::loopback_policy;
    use std::convert::Infallible;
    use std::sync::Arc;

    use axum::Router;
    use axum::body::{Body, Bytes};
    use axum::routing::get;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    const MAX_CLIENT_NAME_BYTES: usize = 255;
    const CLIENT_ID: &str =
        "https://client.example.test/oauth/client.json?token=client-query-secret";
    const PUBLIC_ISSUER: &str = "https://auth.example.test";
    const LOOPBACK_ISSUER: &str = "http://localhost:9080";

    struct LocalFetcher {
        endpoint: String,
        http: reqwest::Client,
    }

    #[async_trait::async_trait]
    impl MetadataFetcher for LocalFetcher {
        async fn fetch(
            &self,
            _metadata_url: &MetadataUrl,
        ) -> Result<reqwest::Response, ClientMetadataError> {
            self.http
                .get(&self.endpoint)
                .send()
                .await
                .map_err(|_error| ClientMetadataError::Fetch)
        }
    }

    struct PanicFetcher;

    #[async_trait::async_trait]
    impl MetadataFetcher for PanicFetcher {
        async fn fetch(
            &self,
            _metadata_url: &MetadataUrl,
        ) -> Result<reqwest::Response, ClientMetadataError> {
            panic!("invalid client IDs must be rejected before fetching")
        }
    }

    fn resolver(endpoint: String) -> HttpClientMetadataResolver {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("test client");
        HttpClientMetadataResolver::with_fetcher(
            PUBLIC_ISSUER,
            &BTreeSet::new(),
            Arc::new(LocalFetcher { endpoint, http }),
        )
        .expect("test resolver")
    }

    fn document() -> Value {
        json!({
            "client_id": CLIENT_ID,
            "redirect_uris": [
                "https://client.example.test/callback?tenant=one",
                "http://127.0.0.1:14554/oauth/callback"
            ],
            "client_name": "client-body-secret",
            "token_endpoint_auth_method": "none",
            "grant_types": ["refresh_token", "authorization_code"],
            "response_types": ["code"],
            "logo_uri": "https://client.example.test/ignored-extension.png"
        })
    }

    fn parsed(value: Value) -> ClientMetadataDocument {
        serde_json::from_value(value).expect("metadata document")
    }

    async fn resolve_template(
        template: ResponseTemplate,
    ) -> Result<OAuthClientRegistration, ClientMetadataError> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(template)
            .mount(&server)
            .await;
        resolver(server.uri()).resolve(CLIENT_ID).await
    }

    #[tokio::test]
    async fn delegates_client_id_url_shape_to_public_metadata_policy() {
        let resolver = HttpClientMetadataResolver::with_fetcher(
            PUBLIC_ISSUER,
            &BTreeSet::new(),
            Arc::new(PanicFetcher),
        )
        .expect("resolver");
        for client_id in [
            "http://client.example.test/client.json",
            "https://localhost/client.json",
            "https://client.example.test",
            "https://user:secret@client.example.test/client.json",
            "https://client.example.test/a/../client.json",
            "https://client.example.test/client.json#fragment",
        ] {
            assert_eq!(
                resolver.resolve(client_id).await.expect_err(client_id),
                ClientMetadataError::InvalidClientId
            );
        }
    }

    #[tokio::test]
    async fn rejects_client_ids_that_are_not_their_own_fetched_url() {
        let resolver = HttpClientMetadataResolver::with_fetcher(
            PUBLIC_ISSUER,
            &BTreeSet::new(),
            Arc::new(PanicFetcher),
        )
        .expect("resolver");
        for client_id in [
            // Each of these parses, and parsing rewrites it into a URL that
            // names a different — or differently spelled — document than the ID
            // Coral was handed. The tab case is the sharpest: it fetches
            // `client.example.test.evil.test`.
            "https://client.example.test\t.evil.test/client.json",
            "https://client.exa\tmple.test/client.json",
            "https://client.example.test/cli\rent.json",
            "https://client.example.test/client.json\n",
            " https://client.example.test/client.json",
            "https://client.example.test/client.json ",
            "https://CLIENT.example.test/client.json",
            "https://client.example.test:443/client.json",
        ] {
            assert_eq!(
                resolver.resolve(client_id).await.expect_err(client_id),
                ClientMetadataError::InvalidClientId
            );
        }
    }

    #[test]
    fn derives_local_client_ids_from_every_root_loopback_resource() {
        let resources = BTreeSet::from([
            "http://localhost:3000".to_string(),
            "http://127.42.0.1:4000".to_string(),
            "http://localhost:5000/mcp".to_string(),
            "https://coral-web.example.test".to_string(),
        ]);

        assert_eq!(
            derive_allowed_loopback_client_ids(&resources).expect("validated resources"),
            BTreeSet::from([
                "http://localhost:3000/.well-known/oauth-client".to_string(),
                "http://127.42.0.1:4000/.well-known/oauth-client".to_string(),
            ])
        );
    }

    #[tokio::test]
    async fn resolves_exact_client_id_derived_from_a_local_resource() {
        let server = MockServer::start().await;
        let resource = server.uri();
        let client_id = format!("{resource}/.well-known/oauth-client");
        let mut metadata = document();
        metadata["client_id"] = json!(client_id.clone());
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata))
            .mount(&server)
            .await;

        let resolver = HttpClientMetadataResolver::new(
            LOOPBACK_ISSUER,
            &BTreeSet::from([resource]),
            loopback_policy(),
        )
        .expect("local resolver");
        let registration = resolver.resolve(&client_id).await.expect("local CIMD");
        assert_eq!(registration.redirect_uris.len(), 2);
    }

    #[tokio::test]
    async fn resolves_exact_https_client_id_derived_from_a_private_reef_origin() {
        let server = MockServer::start().await;
        let resource = "https://100.90.118.86";
        let client_id = format!("{resource}/.well-known/oauth-client");
        let mut metadata = document();
        metadata["client_id"] = json!(client_id);
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata))
            .expect(1)
            .mount(&server)
            .await;
        let resolver = HttpClientMetadataResolver::with_fetcher(
            PUBLIC_ISSUER,
            &BTreeSet::from([resource.to_string()]),
            Arc::new(LocalFetcher {
                endpoint: server.uri(),
                http: reqwest::Client::new(),
            }),
        )
        .expect("private Reef resolver");

        resolver
            .check_trusted_client_id(&client_id)
            .expect("configured private Reef client is eligible for trust");
        let registration = resolver
            .resolve(&client_id)
            .await
            .expect("private Reef CIMD");
        assert_eq!(registration.redirect_uris.len(), 2);
    }

    #[test]
    fn private_https_permission_is_exact_not_host_wide_or_resource_path_derived() {
        let resource = "https://reef.internal.test";
        let client_id = format!("{resource}/.well-known/oauth-client");
        let resolver = HttpClientMetadataResolver::new(
            PUBLIC_ISSUER,
            &BTreeSet::from([
                resource.to_string(),
                "https://mcp.internal.test/workspaces/one/mcp".to_string(),
            ]),
            coral_outbound::DestinationPolicy::default(),
        )
        .expect("resolver");
        assert!(matches!(
            resolver.accepted_metadata_url(&client_id),
            Ok(MetadataUrl::ConfiguredReef(_))
        ));
        for candidate in [
            "https://reef.internal.test/other",
            "https://reef.internal.test/.well-known/oauth-client?extra=1",
            "https://reef.internal.test:8443/.well-known/oauth-client",
            "https://reef.internal.test.attacker.test/.well-known/oauth-client",
            "https://mcp.internal.test/.well-known/oauth-client",
            "https://unregistered.internal.test/.well-known/oauth-client",
        ] {
            assert!(
                matches!(
                    resolver.accepted_metadata_url(candidate),
                    Ok(MetadataUrl::PublicClient(_))
                ),
                "{candidate} must keep public DNS restrictions"
            );
        }
        for candidate in [
            "http://reef.internal.test/.well-known/oauth-client",
            "https://REEF.internal.test/.well-known/oauth-client",
            "https://reef.internal.test:443/.well-known/oauth-client",
            "https://reef.internal.test/a/../.well-known/oauth-client",
            "https://reef.internal.test/.well-known/oauth-client#fragment",
            "https://user:secret@reef.internal.test/.well-known/oauth-client",
            "https://reef.internal.test/.well-known/oauth-client\n",
            "https://100.90.118.86/.well-known/oauth-client",
        ] {
            assert!(
                resolver.accepted_metadata_url(candidate).is_err(),
                "{candidate}"
            );
        }
    }

    #[tokio::test]
    async fn configured_fetcher_keeps_redirect_and_response_validation() {
        let destination = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(document()))
            .expect(0)
            .mount(&destination)
            .await;
        let server = MockServer::start().await;
        let endpoint = EndpointUrl::<Configured>::parse(&server.uri()).expect("test endpoint");
        let fetcher = ReqwestMetadataFetcher::new(loopback_policy()).expect("production fetcher");
        let mut wrong_id = document();
        wrong_id["client_id"] = json!("https://other.test/client");
        for (template, expected) in [
            (
                ResponseTemplate::new(302).insert_header("Location", destination.uri()),
                ClientMetadataError::Redirect,
            ),
            (ResponseTemplate::new(503), ClientMetadataError::HttpStatus),
            (
                ResponseTemplate::new(200)
                    .set_body_bytes(vec![b'x'; CLIENT_METADATA_MAX_BYTES + 1]),
                ClientMetadataError::BodyTooLarge,
            ),
            (
                ResponseTemplate::new(200).set_body_json(wrong_id),
                ClientMetadataError::InvalidMetadata,
            ),
        ] {
            server.reset().await;
            Mock::given(method("GET"))
                .respond_with(template)
                .mount(&server)
                .await;
            // The transport Adapter also supports explicit loopback HTTP,
            // allowing this test to exercise the real configured client without
            // weakening TLS verification for HTTPS in production.
            let response = fetcher
                .fetch(&MetadataUrl::ConfiguredReef(endpoint.clone()))
                .await
                .expect("bounded response");
            assert_eq!(
                registration_from_response(CLIENT_ID, response)
                    .await
                    .expect_err("invalid metadata"),
                expected
            );
        }
    }

    #[tokio::test]
    async fn rejects_unlisted_local_ids_and_mixed_topology_before_fetching() {
        let resource = "http://localhost:3000".to_string();
        let resources = BTreeSet::from([resource]);
        let resolver = HttpClientMetadataResolver::with_fetcher(
            LOOPBACK_ISSUER,
            &resources,
            Arc::new(PanicFetcher),
        )
        .expect("local resolver");

        for client_id in [
            "http://localhost:3001/.well-known/oauth-client",
            "http://localhost:3000/other-client",
            "http://127.0.0.1:3000/.well-known/oauth-client",
        ] {
            assert_eq!(
                resolver.resolve(client_id).await.expect_err(client_id),
                ClientMetadataError::InvalidClientId
            );
        }

        let mixed = HttpClientMetadataResolver::with_fetcher(
            PUBLIC_ISSUER,
            &resources,
            Arc::new(PanicFetcher),
        )
        .expect("mixed resolver");
        assert_eq!(
            mixed
                .resolve("http://localhost:3000/.well-known/oauth-client")
                .await
                .expect_err("public issuer must reject local client ID"),
            ClientMetadataError::InvalidClientId
        );
    }

    #[tokio::test]
    async fn requires_exact_ok_status_without_leaking_responses() {
        for (template, expected) in [
            (
                ResponseTemplate::new(302)
                    .insert_header(
                        "Location",
                        "https://redirect.test/path?secret=redirect-secret",
                    )
                    .set_body_string("redirect-body-secret"),
                ClientMetadataError::Redirect,
            ),
            (
                ResponseTemplate::new(503).set_body_string("status-body-secret"),
                ClientMetadataError::HttpStatus,
            ),
            (
                ResponseTemplate::new(201).set_body_json(document()),
                ClientMetadataError::HttpStatus,
            ),
            (
                ResponseTemplate::new(206).set_body_json(document()),
                ClientMetadataError::HttpStatus,
            ),
        ] {
            let error = resolve_template(template).await.expect_err("failure");
            assert_eq!(error, expected);
            let rendered = format!("{error:?} {error}");
            assert!(!rendered.contains("secret"));
        }
    }

    #[tokio::test]
    async fn rejects_declared_and_streamed_oversize_documents() {
        let declared =
            ResponseTemplate::new(200).set_body_bytes(vec![b'x'; CLIENT_METADATA_MAX_BYTES + 1]);
        assert_eq!(
            resolve_template(declared)
                .await
                .expect_err("declared bound"),
            ClientMetadataError::BodyTooLarge
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let app = Router::new().route(
            "/",
            get(|| async {
                Body::from_stream(tokio_stream::iter([
                    Ok::<_, Infallible>(Bytes::from(vec![b'x'; CLIENT_METADATA_MAX_BYTES])),
                    Ok(Bytes::from_static(b"x")),
                ]))
            }),
        );
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        let error = resolver(format!("http://{address}/"))
            .resolve(CLIENT_ID)
            .await
            .expect_err("streamed bound");
        task.abort();
        assert!(task.await.expect_err("aborted server").is_cancelled());
        assert_eq!(error, ClientMetadataError::BodyTooLarge);
    }

    #[tokio::test]
    async fn rejects_invalid_json_and_client_id_mismatch_without_echoing_input() {
        let invalid = ResponseTemplate::new(200).set_body_string("json-body-secret");
        let error = resolve_template(invalid).await.expect_err("invalid JSON");
        assert_eq!(error, ClientMetadataError::InvalidJson);
        assert!(!format!("{error:?} {error}").contains("secret"));

        let mut mismatched = document();
        mismatched["client_id"] = json!("https://other.example.test/client.json?secret=value");
        let error = resolve_template(ResponseTemplate::new(200).set_body_json(mismatched))
            .await
            .expect_err("mismatched client ID");
        assert_eq!(error, ClientMetadataError::InvalidMetadata);
        assert!(!format!("{error:?} {error}").contains("other.example"));
    }

    #[tokio::test]
    async fn requires_a_nonempty_client_name() {
        for value in [
            Value::Null,
            json!(""),
            json!("   "),
            json!(" leading"),
            json!("trailing "),
            json!("line\nbreak"),
            Value::String("x".repeat(MAX_CLIENT_NAME_BYTES + 1)),
        ] {
            let mut metadata = document();
            metadata["client_name"] = value;
            let error = resolve_template(ResponseTemplate::new(200).set_body_json(metadata))
                .await
                .expect_err("invalid client name");
            assert!(matches!(
                error,
                ClientMetadataError::InvalidJson | ClientMetadataError::InvalidMetadata
            ));
        }

        let mut missing = document();
        missing
            .as_object_mut()
            .expect("metadata object")
            .remove("client_name");
        assert_eq!(
            resolve_template(ResponseTemplate::new(200).set_body_json(missing))
                .await
                .expect_err("missing client name"),
            ClientMetadataError::InvalidJson
        );

        for value in ["Renée Coral".to_string(), "x".repeat(MAX_CLIENT_NAME_BYTES)] {
            let mut metadata = document();
            metadata["client_name"] = Value::String(value.clone());
            let registration =
                validate_document(CLIENT_ID, parsed(metadata)).expect("valid client name");
            assert_eq!(registration.client_name, value);
        }
    }

    #[test]
    fn redirect_uris_use_first_party_transport_and_collision_rules() {
        for redirect_uris in [
            json!([]),
            json!(["http://client.example.test/callback"]),
            json!(["https://user:secret@client.example.test/callback"]),
            json!(["https://client.example.test/callback#fragment"]),
            json!(["https://client.example.test/callback?STATE=x"]),
            json!(["https://client.example.test/callback?%65rror=x"]),
            json!([" https://client.example.test/callback"]),
        ] {
            let mut value = document();
            value["redirect_uris"] = redirect_uris;
            assert_eq!(
                validate_document(CLIENT_ID, parsed(value)).expect_err("invalid redirect"),
                ClientMetadataError::InvalidMetadata
            );
        }
    }

    /// Raycast's published document lists `raycast://`, which this server
    /// cannot use, beside callbacks it can. The client still resolves.
    #[test]
    fn unusable_redirect_uris_are_dropped_rather_than_rejecting_the_client() {
        let mut value = document();
        value["redirect_uris"] = json!([
            "com.raycast:/oauth",
            "raycast://mcp-oauth-callback",
            "http://client.example.test/callback",
            "https://client.example.test/callback?state=x",
            "http://127.0.0.1/callback",
        ]);
        let registration = validate_document(CLIENT_ID, parsed(value)).expect("usable redirects");
        assert_eq!(
            registration.redirect_uris,
            ["com.raycast:/oauth", "http://127.0.0.1/callback"]
        );
    }

    #[test]
    fn optional_metadata_fields_match_cloud_compatible_public_code_semantics() {
        for (field, value, expected) in [
            ("token_endpoint_auth_method", Value::Null, None),
            ("token_endpoint_auth_method", json!("none"), None),
            (
                "token_endpoint_auth_method",
                json!("client_secret_basic"),
                Some(ClientMetadataError::InvalidMetadata),
            ),
            ("grant_types", Value::Null, None),
            ("grant_types", json!(["authorization_code"]), None),
            (
                "grant_types",
                json!(["refresh_token"]),
                Some(ClientMetadataError::InvalidMetadata),
            ),
            ("response_types", Value::Null, None),
            ("response_types", json!(["code", "token"]), None),
            (
                "response_types",
                json!(["token"]),
                Some(ClientMetadataError::InvalidMetadata),
            ),
        ] {
            let mut document = document();
            document[field] = value;
            let result = validate_document(CLIENT_ID, parsed(document));
            assert_eq!(result.err(), expected, "field {field}");
        }
    }

    #[tokio::test]
    async fn resolves_valid_document_and_ignores_extension_metadata() {
        let registration = resolve_template(ResponseTemplate::new(200).set_body_json(document()))
            .await
            .expect("valid metadata");
        assert_eq!(registration.redirect_uris.len(), 2);
        assert_eq!(registration.client_name, "client-body-secret");
        let rendered = format!("{registration:?}");
        assert!(!rendered.contains("callback"));
        assert!(!rendered.contains("body-secret"));
        HttpClientMetadataResolver::new(
            PUBLIC_ISSUER,
            &BTreeSet::new(),
            coral_outbound::DestinationPolicy::default(),
        )
        .expect("hardened production client");
    }
}
