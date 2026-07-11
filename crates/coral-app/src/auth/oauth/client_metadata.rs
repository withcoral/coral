//! Client ID Metadata Document fetching and validation.

use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
use thiserror::Error;
use zeroize::Zeroizing;

use super::validate_oauth_redirect_uri;
use crate::outbound_url_policy::{
    OutboundUrlPolicyError, PublicMetadataUrl, public_metadata_http_client, read_bounded_body,
};

const CLIENT_METADATA_MAX_BYTES: usize = 5 * 1024;

/// The client properties needed after a metadata document has been validated.
#[derive(Clone)]
pub(super) struct OAuthClientRegistration {
    pub(super) redirect_uris: Vec<String>,
    pub(super) client_name: Option<String>,
}

impl fmt::Debug for OAuthClientRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthClientRegistration")
            .field("redirect_uri_count", &self.redirect_uris.len())
            .field("has_client_name", &self.client_name.is_some())
            .finish()
    }
}

/// A replaceable async boundary for resolving a metadata-URL client ID.
#[async_trait::async_trait]
pub(super) trait ClientMetadataResolver: Send + Sync {
    async fn resolve(
        &self,
        client_id: &str,
        supported_scope: &str,
    ) -> Result<OAuthClientRegistration, ClientMetadataError>;
}

/// Production resolver backed by the hardened public-metadata HTTP client.
#[derive(Clone)]
pub(super) struct HttpClientMetadataResolver {
    fetcher: Arc<dyn MetadataFetcher>,
}

impl HttpClientMetadataResolver {
    pub(super) fn new() -> Result<Self, ClientMetadataError> {
        let http =
            public_metadata_http_client().map_err(|_error| ClientMetadataError::ClientBuild)?;
        Ok(Self {
            fetcher: Arc::new(ReqwestMetadataFetcher { http }),
        })
    }

    #[cfg(test)]
    fn with_fetcher(fetcher: Arc<dyn MetadataFetcher>) -> Self {
        Self { fetcher }
    }
}

#[async_trait::async_trait]
impl ClientMetadataResolver for HttpClientMetadataResolver {
    async fn resolve(
        &self,
        client_id: &str,
        supported_scope: &str,
    ) -> Result<OAuthClientRegistration, ClientMetadataError> {
        let metadata_url = PublicMetadataUrl::parse(client_id)
            .map_err(|_error| ClientMetadataError::InvalidClientId)?;
        let response = self.fetcher.fetch(&metadata_url).await?;
        registration_from_response(client_id, supported_scope, response).await
    }
}

#[async_trait::async_trait]
trait MetadataFetcher: Send + Sync {
    async fn fetch(
        &self,
        metadata_url: &PublicMetadataUrl,
    ) -> Result<reqwest::Response, ClientMetadataError>;
}

struct ReqwestMetadataFetcher {
    http: reqwest::Client,
}

#[async_trait::async_trait]
impl MetadataFetcher for ReqwestMetadataFetcher {
    async fn fetch(
        &self,
        metadata_url: &PublicMetadataUrl,
    ) -> Result<reqwest::Response, ClientMetadataError> {
        self.http
            .get(metadata_url.as_url().clone())
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
    scope: Option<String>,
}

async fn registration_from_response(
    client_id: &str,
    supported_scope: &str,
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
    validate_document(client_id, supported_scope, document)
}

fn validate_document(
    client_id: &str,
    supported_scope: &str,
    document: ClientMetadataDocument,
) -> Result<OAuthClientRegistration, ClientMetadataError> {
    if document.client_id != client_id {
        return Err(ClientMetadataError::InvalidMetadata);
    }
    if document.client_name.trim().is_empty() {
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
    if document
        .scope
        .as_deref()
        .is_some_and(|scope| !scope_is_exactly_supported(scope, supported_scope))
    {
        return Err(ClientMetadataError::InvalidMetadata);
    }
    Ok(OAuthClientRegistration {
        redirect_uris: document.redirect_uris,
        client_name: Some(document.client_name),
    })
}

pub(super) fn scope_is_exactly_supported(scope: &str, supported: &str) -> bool {
    let mut scopes = scope.split_ascii_whitespace();
    scopes.next() == Some(supported) && scopes.next().is_none()
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
    #[error("OAuth client_id is not a valid public metadata URL")]
    InvalidClientId,
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
    const SCOPE: &str = "coral:mcp";

    struct LocalFetcher {
        endpoint: String,
        http: reqwest::Client,
    }

    #[async_trait::async_trait]
    impl MetadataFetcher for LocalFetcher {
        async fn fetch(
            &self,
            _metadata_url: &PublicMetadataUrl,
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
            _metadata_url: &PublicMetadataUrl,
        ) -> Result<reqwest::Response, ClientMetadataError> {
            panic!("invalid client IDs must be rejected before fetching")
        }
    }

    fn resolver(endpoint: String) -> HttpClientMetadataResolver {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("test client");
        HttpClientMetadataResolver::with_fetcher(Arc::new(LocalFetcher { endpoint, http }))
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
            "scope": SCOPE,
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
        resolver(server.uri()).resolve(CLIENT_ID, SCOPE).await
    }

    #[tokio::test]
    async fn delegates_client_id_url_shape_to_public_metadata_policy() {
        let resolver = HttpClientMetadataResolver::with_fetcher(Arc::new(PanicFetcher));
        for client_id in [
            "http://client.example.test/client.json",
            "https://localhost/client.json",
            "https://client.example.test",
            "https://user:secret@client.example.test/client.json",
            "https://client.example.test/a/../client.json",
            "https://client.example.test/client.json#fragment",
        ] {
            assert_eq!(
                resolver
                    .resolve(client_id, SCOPE)
                    .await
                    .expect_err(client_id),
                ClientMetadataError::InvalidClientId
            );
        }
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
            .resolve(CLIENT_ID, SCOPE)
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
        for value in [Value::Null, json!(""), json!("   ")] {
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
                validate_document(CLIENT_ID, SCOPE, parsed(value)).expect_err("invalid redirect"),
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
            ("scope", Value::Null, None),
            ("scope", json!(SCOPE), None),
            (
                "scope",
                json!("coral:mcp offline_access"),
                Some(ClientMetadataError::InvalidMetadata),
            ),
            ("scope", json!(" coral:mcp "), None),
        ] {
            let mut document = document();
            document[field] = value;
            let result = validate_document(CLIENT_ID, SCOPE, parsed(document));
            assert_eq!(result.err(), expected, "field {field}");
        }
    }

    #[tokio::test]
    async fn resolves_valid_document_and_ignores_extension_metadata() {
        let registration = resolve_template(ResponseTemplate::new(200).set_body_json(document()))
            .await
            .expect("valid metadata");
        assert_eq!(registration.redirect_uris.len(), 2);
        assert_eq!(
            registration.client_name.as_deref(),
            Some("client-body-secret")
        );
        let rendered = format!("{registration:?}");
        assert!(!rendered.contains("callback"));
        assert!(!rendered.contains("body-secret"));
        HttpClientMetadataResolver::new().expect("hardened production client");
    }
}
