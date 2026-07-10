use std::collections::{BTreeMap, BTreeSet};

use axum::extract::{RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use url::{Url, form_urlencoded};

use super::super::PROVIDER_ID;
use super::super::provider_client::OidcAuthorizationRequest;
use super::super::state_store::OAuthAuthorizationSessionRecord;
use super::{AuthorizationServerHttpState, canonical_authorization_resource};

const MAX_QUERY_BYTES: usize = 8 * 1024;
const MAX_PARAMETERS: usize = 32;
const MAX_PARAMETER_NAME_BYTES: usize = 64;
const MAX_PARAMETER_VALUE_BYTES: usize = 2 * 1024;

pub(super) async fn oauth_authorize(
    State(state): State<AuthorizationServerHttpState>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let Ok(query) = parse_query(raw_query.as_deref().unwrap_or_default()) else {
        return direct_error("invalid_request", "authorization request is malformed");
    };
    let (Some(client_id), Some(redirect_uri)) =
        (query.client_id.as_deref(), query.redirect_uri.as_deref())
    else {
        return direct_error("invalid_request", "client_id and redirect_uri are required");
    };
    let Some(callback) = registered_redirect(&state.registered_clients, client_id, redirect_uri)
    else {
        return direct_error("invalid_request", "client_id or redirect_uri is invalid");
    };
    let trusted = TrustedRedirect {
        url: callback,
        client_state: query.state.clone(),
    };

    match query.response_type.as_deref() {
        Some("code") => {}
        None => return trusted.error("invalid_request", "response_type is required"),
        Some(_) => {
            return trusted.error(
                "unsupported_response_type",
                "only response_type=code is supported",
            );
        }
    }
    let code_challenge = match query.code_challenge.as_deref() {
        Some(challenge)
            if query.code_challenge_method.as_deref() == Some("S256")
                && valid_s256_challenge(challenge) =>
        {
            challenge.to_string()
        }
        _ => {
            return trusted.error("invalid_request", "PKCE S256 code_challenge is required");
        }
    };
    if query.scope.is_some() {
        return trusted.error("invalid_scope", "scope is not supported");
    }
    let Some(resource) = query.resource.as_deref() else {
        return trusted.error("invalid_target", "resource is required");
    };
    let Ok(resource) = canonical_authorization_resource(resource) else {
        return trusted.error("invalid_target", "resource is invalid");
    };
    if !state.authorization_resources.contains(&resource) {
        return trusted.error("invalid_target", "resource does not match this server");
    }
    let request = match state
        .provider_client
        .authorization_request(state.settings.provider())
        .await
    {
        Ok(request) => request,
        Err(_error) => return trusted.error("server_error", "authorization failed"),
    };
    let OidcAuthorizationRequest {
        url,
        state: oidc_state,
        nonce: oidc_nonce,
        code_verifier: oidc_code_verifier,
    } = request;
    let session = OAuthAuthorizationSessionRecord {
        provider_id: PROVIDER_ID.to_string(),
        client_id: client_id.to_string(),
        redirect_uri: redirect_uri.to_string(),
        client_state: query.state,
        code_challenge,
        resource,
        oidc_code_verifier,
        oidc_nonce,
    };
    if state
        .state_store
        .store_authorization_session(&oidc_state, session)
        .await
        .is_err()
    {
        return trusted.error("server_error", "authorization failed");
    }
    redirect(url.as_str())
}

#[derive(Default)]
struct AuthorizeQuery {
    response_type: Option<String>,
    client_id: Option<String>,
    redirect_uri: Option<String>,
    scope: Option<String>,
    state: Option<String>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
    resource: Option<String>,
}

fn parse_query(raw: &str) -> Result<AuthorizeQuery, ()> {
    if raw.len() > MAX_QUERY_BYTES {
        return Err(());
    }
    let mut query = AuthorizeQuery::default();
    let mut seen = BTreeSet::new();
    for (index, (name, value)) in form_urlencoded::parse(raw.as_bytes()).enumerate() {
        if index >= MAX_PARAMETERS
            || name.len() > MAX_PARAMETER_NAME_BYTES
            || value.len() > MAX_PARAMETER_VALUE_BYTES
        {
            return Err(());
        }
        let name = name.into_owned();
        if !seen.insert(name.clone()) {
            return Err(());
        }
        let target = match name.as_str() {
            "response_type" => &mut query.response_type,
            "client_id" => &mut query.client_id,
            "redirect_uri" => &mut query.redirect_uri,
            "scope" => &mut query.scope,
            "state" => &mut query.state,
            "code_challenge" => &mut query.code_challenge,
            "code_challenge_method" => &mut query.code_challenge_method,
            "resource" => &mut query.resource,
            _ => continue,
        };
        *target = Some(value.into_owned());
    }
    Ok(query)
}

fn registered_redirect(
    registered_clients: &BTreeMap<String, Vec<String>>,
    client_id: &str,
    redirect_uri: &str,
) -> Option<Url> {
    registered_clients
        .get(client_id)?
        .iter()
        .find(|uri| uri.as_str() == redirect_uri)
        .and_then(|uri| Url::parse(uri).ok())
}

fn valid_s256_challenge(value: &str) -> bool {
    value.len() == 43
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

struct TrustedRedirect {
    url: Url,
    client_state: Option<String>,
}

impl TrustedRedirect {
    fn error(&self, error: &'static str, description: &'static str) -> Response {
        let mut url = self.url.clone();
        let mut query = url.query_pairs_mut();
        query
            .append_pair("error", error)
            .append_pair("error_description", description);
        if let Some(state) = &self.client_state {
            query.append_pair("state", state);
        }
        drop(query);
        redirect(url.as_str())
    }
}

fn direct_error(error: &'static str, description: &'static str) -> Response {
    let body = serde_json::json!({
        "error": error,
        "error_description": description,
    })
    .to_string();
    (
        StatusCode::BAD_REQUEST,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
        ],
        body,
    )
        .into_response()
}

fn redirect(location: &str) -> Response {
    (
        StatusCode::FOUND,
        [
            (header::LOCATION, location),
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        "",
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::to_bytes;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::{AuthorizationServerHttpState, CoralAuthorizationServer};
    use super::*;
    use crate::auth::config::AuthSettings;
    use crate::auth::config::ResolvedAuthSettings;
    use crate::auth::provider_client::OidcProviderClient;
    use crate::auth::session::SessionTokenIssuer;
    use crate::auth::state_store::{InMemoryStateStore, StateStore};

    const AUTH_ISSUER: &str = "https://auth.example.test";
    const RESOURCE: &str = "https://api.example.test/mcp";
    const CLIENT_ID: &str = "https://client.example.test/oauth/client.json";
    const REDIRECT_URI: &str = "https://client.example.test/callback?tenant=one";
    const CLIENT_STATE: &str = "client-state&error=injected";
    const CHALLENGE: &str = "0123456789012345678901234567890123456789012";
    const PROVIDER_SECRET: &str = "provider-secret-must-not-leak";

    fn settings(issuer: &str) -> ResolvedAuthSettings {
        let settings = AuthSettings::from_toml(&format!(
            "[auth]
             [auth.session]
             [auth.authorization_server]
             issuer = '{AUTH_ISSUER}'
             [auth.provider]
             issuer = '{issuer}'
             client_id = 'provider-client'
             client_secret = '{PROVIDER_SECRET}'
             redirect_uri = '{AUTH_ISSUER}/auth/oidc/callback'"
        ))
        .expect("valid auth config")
        .expect("auth settings");
        let signing_key = STANDARD.encode(
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key"),
        );
        let (settings, _issuer) = settings
            .resolve_runtime_dependencies(Path::new("config.toml"), &|name| {
                Ok((name == "CORAL_SESSION_SIGNING_KEY").then(|| signing_key.clone()))
            })
            .expect("resolved runtime dependencies");
        settings
    }

    fn state(issuer: &str, store: Arc<dyn StateStore>) -> AuthorizationServerHttpState {
        let signing_key =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key");
        let session_tokens = SessionTokenIssuer::new(
            Some(AUTH_ISSUER),
            signing_key.as_ref(),
            Duration::from_mins(5),
        )
        .expect("session");
        AuthorizationServerHttpState {
            settings: Arc::new(settings(issuer)),
            session_tokens,
            state_store: store,
            provider_client: OidcProviderClient::new().expect("provider client"),
            registered_clients: Arc::new(BTreeMap::from([(
                CLIENT_ID.into(),
                vec![REDIRECT_URI.into()],
            )])),
            authorization_resources: Arc::new(BTreeSet::from([RESOURCE.into()])),
        }
    }

    fn pairs() -> Vec<(String, String)> {
        [
            ("response_type", "code"),
            ("client_id", CLIENT_ID),
            ("redirect_uri", REDIRECT_URI),
            ("state", CLIENT_STATE),
            ("code_challenge", CHALLENGE),
            ("code_challenge_method", "S256"),
            ("resource", RESOURCE),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect()
    }

    fn encode(pairs: &[(String, String)]) -> String {
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        for (key, value) in pairs {
            serializer.append_pair(key, value);
        }
        serializer.finish()
    }

    fn replace(pairs: &mut Vec<(String, String)>, key: &str, value: Option<&str>) {
        pairs.retain(|(name, _value)| name != key);
        if let Some(value) = value {
            pairs.push((key.into(), value.into()));
        }
    }

    async fn request(state: AuthorizationServerHttpState, pairs: &[(String, String)]) -> Response {
        oauth_authorize(State(state), RawQuery(Some(encode(pairs)))).await
    }

    fn location(response: &Response) -> Option<&str> {
        response
            .headers()
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
    }

    async fn mount_discovery(server: &MockServer, status: u16) {
        let document = json!({
            "issuer": server.uri(),
            "authorization_endpoint": format!("{}/authorize?upstream=one", server.uri()),
            "token_endpoint": format!("{}/token", server.uri()),
            "jwks_uri": format!("{}/jwks", server.uri()),
            "id_token_signing_alg_values_supported": ["RS256"],
        });
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(status).set_body_json(document))
            .mount(server)
            .await;
    }

    #[test]
    fn query_parser_bounds_and_rejects_decoded_duplicates() {
        let encoded = encode(&pairs());
        let cases = [
            format!("{encoded}&client%5Fid=duplicate"),
            "x".repeat(MAX_QUERY_BYTES + 1),
            format!("{}=value", "n".repeat(MAX_PARAMETER_NAME_BYTES + 1)),
            format!("value={}", "v".repeat(MAX_PARAMETER_VALUE_BYTES + 1)),
            (0..=MAX_PARAMETERS)
                .map(|index| format!("x{index}=1"))
                .collect::<Vec<_>>()
                .join("&"),
        ];
        for raw in cases {
            assert!(parse_query(&raw).is_err(), "accepted {raw}");
        }
    }

    #[tokio::test]
    async fn client_and_redirect_must_match_exactly_before_redirecting() {
        let store = Arc::new(InMemoryStateStore::new());
        for (field, value) in [
            (
                "client_id",
                "https://client.example.test/oauth/%63lient.json",
            ),
            ("redirect_uri", "https://client.example.test/other"),
        ] {
            let mut request_pairs = pairs();
            replace(&mut request_pairs, field, Some(value));
            let response = request(
                state("https://provider.invalid", store.clone()),
                &request_pairs,
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(location(&response).is_none());
        }

        let raw = format!("{}&client%5Fid=raw-secret", encode(&pairs()));
        let response = oauth_authorize(
            State(state("https://provider.invalid", store)),
            RawQuery(Some(raw)),
        )
        .await;
        assert!(location(&response).is_none());
        let body = to_bytes(response.into_body(), 4096).await.expect("body");
        assert!(!String::from_utf8_lossy(&body).contains("raw-secret"));
    }

    #[tokio::test]
    async fn trusted_protocol_errors_redirect_with_one_encoded_client_state() {
        let store = Arc::new(InMemoryStateStore::new());
        for (field, value, expected) in [
            ("response_type", None, "invalid_request"),
            ("response_type", Some("token"), "unsupported_response_type"),
            ("code_challenge", None, "invalid_request"),
            ("code_challenge", Some("short"), "invalid_request"),
            ("code_challenge_method", Some("plain"), "invalid_request"),
            ("scope", Some("coral:access"), "invalid_scope"),
            ("resource", None, "invalid_target"),
            ("resource", Some("https://other.test"), "invalid_target"),
        ] {
            let mut request_pairs = pairs();
            replace(&mut request_pairs, field, value);
            let response = request(
                state("https://provider.invalid", store.clone()),
                &request_pairs,
            )
            .await;
            assert_eq!(response.status(), StatusCode::FOUND);
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
            assert_eq!(response.headers()[header::REFERRER_POLICY], "no-referrer");
            let url = Url::parse(location(&response).expect("error redirect")).expect("URL");
            let query = url.query_pairs().into_owned().collect::<Vec<_>>();
            assert!(query.contains(&("tenant".into(), "one".into())));
            assert!(query.contains(&("error".into(), expected.into())));
            assert_eq!(
                query.iter().filter(|(key, _value)| key == "state").count(),
                1
            );
            assert!(query.contains(&("state".into(), CLIENT_STATE.into())));
        }
    }

    #[tokio::test]
    async fn empty_resource_allowlist_fails_closed() {
        let mut auth_state = state(
            "https://provider.invalid",
            Arc::new(InMemoryStateStore::new()),
        );
        auth_state.authorization_resources = Arc::new(BTreeSet::new());
        let response = request(auth_state, &pairs()).await;
        let location = location(&response).expect("trusted error redirect");
        assert!(location.contains("error=invalid_target"));
    }

    #[tokio::test]
    async fn success_stores_single_use_session_before_provider_redirect() {
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let store = Arc::new(InMemoryStateStore::new());
        let response = request(state(&provider.uri(), store.clone()), &pairs()).await;
        let provider_url = location(&response).expect("provider redirect");
        let query = Url::parse(provider_url)
            .expect("URL")
            .query_pairs()
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        let oidc_state = query.get("state").expect("OIDC state");
        let session = store
            .take_authorization_session(oidc_state)
            .await
            .expect("store")
            .expect("stored before redirect");
        assert_eq!(session.provider_id, PROVIDER_ID);
        assert_eq!(session.client_id, CLIENT_ID);
        assert_eq!(session.redirect_uri, REDIRECT_URI);
        assert_eq!(session.client_state.as_deref(), Some(CLIENT_STATE));
        assert_eq!(session.code_challenge, CHALLENGE);
        assert_eq!(session.resource, RESOURCE);
        assert_eq!(session.oidc_code_verifier.len(), 43);
        assert_eq!(session.oidc_nonce.len(), 43);
        assert!(!query.values().any(|value| value == CLIENT_STATE));
        for secret in [&session.oidc_code_verifier, PROVIDER_SECRET] {
            assert!(!provider_url.contains(secret));
        }
        assert!(
            store
                .take_authorization_session(oidc_state)
                .await
                .expect("store")
                .is_none()
        );
    }

    #[tokio::test]
    async fn provider_and_store_failures_are_generic_trusted_errors() {
        let provider = MockServer::start().await;
        mount_discovery(&provider, 500).await;
        let store = Arc::new(InMemoryStateStore::new());
        let response = request(state(&provider.uri(), store.clone()), &pairs()).await;
        let error_location = location(&response).expect("error redirect");
        assert!(error_location.contains("error=server_error"));
        assert!(!error_location.contains(PROVIDER_SECRET));

        provider.reset().await;
        mount_discovery(&provider, 200).await;
        let filler = OAuthAuthorizationSessionRecord {
            provider_id: PROVIDER_ID.into(),
            client_id: CLIENT_ID.into(),
            redirect_uri: REDIRECT_URI.into(),
            client_state: None,
            code_challenge: CHALLENGE.into(),
            resource: RESOURCE.into(),
            oidc_code_verifier: "verifier".to_string().into(),
            oidc_nonce: "nonce".into(),
        };
        for index in 0..4096 {
            store
                .store_authorization_session(&format!("filler-{index}"), filler.clone())
                .await
                .expect("fill store");
        }
        let response = request(state(&provider.uri(), store), &pairs()).await;
        assert!(
            location(&response)
                .expect("store error redirect")
                .contains("error=server_error")
        );
    }

    #[tokio::test]
    async fn listener_exposes_authorize_route() {
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let state = state(&provider.uri(), Arc::new(InMemoryStateStore::new()));
        let server = CoralAuthorizationServer {
            settings: state.settings,
            session_tokens: state.session_tokens,
            state_store: state.state_store,
            registered_clients: state.registered_clients,
            authorization_resources: state.authorization_resources.as_ref().clone(),
        }
        .start()
        .await
        .expect("start");
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("client");
        let response = client
            .get(format!(
                "{}/oauth/authorize?{}",
                server.endpoint_uri(),
                encode(&pairs())
            ))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::FOUND);
        server.shutdown().await.expect("shutdown");
    }
}
