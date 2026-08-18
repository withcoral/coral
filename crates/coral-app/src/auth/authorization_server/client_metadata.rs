//! Client ID Metadata Document fetching and validation.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
use thiserror::Error;
use zeroize::Zeroizing;

use crate::oauth_resource::CanonicalOauthUrl;
use crate::outbound_url_policy::{
    BrowserRedirect, ClientMetadata, Configured, EndpointUrl, OutboundUrlPolicyError,
    client_metadata_http_client, read_bounded_body,
};

const CLIENT_METADATA_MAX_BYTES: usize = 5 * 1024;
const MAX_CLIENT_NAME_BYTES: usize = 255;

/// The client properties needed after a metadata document has been validated.
#[derive(Clone)]
pub(super) struct OAuthClientRegistration {
    pub(super) redirect_uris: Vec<String>,
    pub(super) client_name: String,
}

impl fmt::Debug for OAuthClientRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthClientRegistration")
            .field("redirect_uri_count", &self.redirect_uris.len())
            .finish_non_exhaustive()
    }
}

/// A replaceable async boundary for resolving a metadata-URL client ID.
#[async_trait::async_trait]
pub(super) trait ClientMetadataResolver: Send + Sync {
    async fn resolve(
        &self,
        client_id: &str,
    ) -> Result<OAuthClientRegistration, ClientMetadataError>;
}

/// Production resolver backed by the hardened client-metadata HTTP client.
#[derive(Clone)]
pub(super) struct HttpClientMetadataResolver {
    issuer: EndpointUrl<Configured>,
    allowed_loopback_client_ids: BTreeSet<String>,
    fetcher: Arc<dyn MetadataFetcher>,
}

impl HttpClientMetadataResolver {
    pub(super) fn new(
        issuer: &str,
        authorization_resources: &BTreeSet<String>,
    ) -> Result<Self, ClientMetadataError> {
        let http =
            client_metadata_http_client().map_err(|_error| ClientMetadataError::ClientBuild)?;
        Self::with_fetcher(
            issuer,
            authorization_resources,
            Arc::new(ReqwestMetadataFetcher { http }),
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
    fn accepted_metadata_url(
        &self,
        client_id: &str,
    ) -> Result<EndpointUrl<ClientMetadata>, ClientIdRejection> {
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
        Ok(metadata_url)
    }
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
        metadata_url: &EndpointUrl<ClientMetadata>,
    ) -> Result<reqwest::Response, ClientMetadataError>;
}

struct ReqwestMetadataFetcher {
    http: reqwest::Client,
}

#[async_trait::async_trait]
impl MetadataFetcher for ReqwestMetadataFetcher {
    async fn fetch(
        &self,
        metadata_url: &EndpointUrl<ClientMetadata>,
    ) -> Result<reqwest::Response, ClientMetadataError> {
        metadata_url
            .get(&self.http)
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
    if document.client_name.trim().is_empty()
        || document.client_name.trim() != document.client_name
        || document.client_name.len() > MAX_CLIENT_NAME_BYTES
        || document.client_name.chars().any(char::is_control)
    {
        return Err(ClientMetadataError::InvalidMetadata);
    }
    if document.redirect_uris.is_empty()
        || document
            .redirect_uris
            .iter()
            .any(|uri| validate_oauth_redirect_uri(uri).is_err())
    {
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
        redirect_uris: document.redirect_uris,
        client_name: document.client_name,
    })
}

fn validate_oauth_redirect_uri(uri: &str) -> Result<(), String> {
    if uri.trim() != uri {
        return Err("redirect URI has surrounding whitespace".to_string());
    }
    let url = EndpointUrl::<BrowserRedirect>::parse(uri)
        .map_err(|error| format!("OAuth client redirect URI is invalid: {error}"))?;
    if url.as_url().query_pairs().any(|(key, _value)| {
        matches!(
            key.to_ascii_lowercase().as_str(),
            "code" | "state" | "error" | "error_description"
        )
    }) {
        return Err(
            "OAuth client redirect URI must not contain OAuth response parameters".to_string(),
        );
    }
    Ok(())
}

fn map_body_error(error: &OutboundUrlPolicyError) -> ClientMetadataError {
    match error {
        OutboundUrlPolicyError::BodyTooLarge { .. } => ClientMetadataError::BodyTooLarge,
        _ => ClientMetadataError::Fetch,
    }
}

/// Sanitized client metadata resolution failures.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum ClientMetadataError {
    #[error("OAuth client_id is not a valid metadata document URL")]
    InvalidClientId,
    #[error("OAuth client metadata resolver configuration is invalid")]
    InvalidConfiguration,
    #[error("failed to build the OAuth client metadata HTTP client")]
    ClientBuild,
    #[error("OAuth client metadata fetch failed")]
    Fetch,
    #[error("OAuth client metadata endpoint returned a redirect")]
    Redirect,
    #[error("OAuth client metadata endpoint returned an unsuccessful status")]
    HttpStatus,
    #[error("OAuth client metadata document exceeded 5120 bytes")]
    BodyTooLarge,
    #[error("OAuth client metadata document was not valid JSON")]
    InvalidJson,
    #[error("OAuth client metadata document is not supported")]
    InvalidMetadata,
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "metadata JSON fixture mutation")]
mod tests {
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
            _metadata_url: &EndpointUrl<ClientMetadata>,
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
            _metadata_url: &EndpointUrl<ClientMetadata>,
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
            "https://coral-ui.example.test".to_string(),
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

        let resolver =
            HttpClientMetadataResolver::new(LOOPBACK_ISSUER, &BTreeSet::from([resource]))
                .expect("local resolver");
        let registration = resolver.resolve(&client_id).await.expect("local CIMD");
        assert_eq!(registration.redirect_uris.len(), 2);
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
        HttpClientMetadataResolver::new(PUBLIC_ISSUER, &BTreeSet::new())
            .expect("hardened production client");
    }
}
