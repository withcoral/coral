use std::collections::BTreeSet;

use axum::extract::{RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::{SecureRandom as _, SystemRandom};
use url::{Url, form_urlencoded};
use zeroize::Zeroizing;

use super::super::state_store::{OAuthAuthorizationCodeRecord, OAuthAuthorizationSessionRecord};
use super::AuthorizationServerHttpState;

const MAX_QUERY_BYTES: usize = 8 * 1024;
const MAX_PARAMETERS: usize = 32;
const MAX_PARAMETER_NAME_BYTES: usize = 64;
const MAX_PARAMETER_VALUE_BYTES: usize = 2 * 1024;

pub(super) async fn oidc_callback(
    State(state): State<AuthorizationServerHttpState>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let Ok(query) = parse_query(raw_query.as_deref().unwrap_or_default()) else {
        return direct_error("invalid_request", "OIDC callback request is malformed");
    };
    let Some(oidc_state) = query.state.as_deref().filter(|state| !state.is_empty()) else {
        return direct_error("invalid_request", "OIDC callback state is required");
    };
    let Ok(Some(session)) = state
        .state_store
        .take_authorization_session(oidc_state)
        .await
    else {
        return direct_error(
            "invalid_request",
            "OIDC callback state is invalid or expired",
        );
    };
    let Some(trusted) = TrustedRedirect::new(&session) else {
        return direct_error("server_error", "authorization failed");
    };
    let provider_code = match classify_result(&query) {
        Ok(CallbackResult::Code(code)) => code,
        Ok(CallbackResult::AccessDenied) => {
            return trusted.error("access_denied", "authorization was denied");
        }
        Ok(CallbackResult::ProviderError) => {
            return trusted.error("server_error", "authorization failed");
        }
        Err(()) => {
            return trusted.error("invalid_request", "OIDC callback response is invalid");
        }
    };
    let provider = state.settings.provider();
    let Ok(exchange) = state
        .provider_client
        .exchange_code(provider, &provider_code, &session.oidc_code_verifier)
        .await
    else {
        return trusted.error("server_error", "authorization failed");
    };
    let Ok(identity) = state
        .provider_client
        .validate_code_exchange(provider, exchange, &session.oidc_nonce)
        .await
    else {
        return trusted.error("server_error", "authorization failed");
    };
    let Ok(authorization_code) = random_code() else {
        return trusted.error("server_error", "authorization failed");
    };
    let authorization = OAuthAuthorizationCodeRecord {
        provider_id: session.provider_id,
        user_id: identity.principal,
        client_id: session.client_id,
        redirect_uri: session.redirect_uri,
        code_challenge: session.code_challenge,
        resource: session.resource,
    };
    if state
        .state_store
        .store_authorization_code(&authorization_code, authorization)
        .await
        .is_err()
    {
        return trusted.error("server_error", "authorization failed");
    }
    trusted.success(&authorization_code)
}

#[derive(Default)]
struct CallbackQuery {
    state: Option<String>,
    code: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

fn parse_query(raw: &str) -> Result<CallbackQuery, ()> {
    if raw.len() > MAX_QUERY_BYTES {
        return Err(());
    }
    let mut query = CallbackQuery::default();
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
            "state" => &mut query.state,
            "code" => &mut query.code,
            "error" => &mut query.error,
            "error_description" => &mut query.error_description,
            _ => continue,
        };
        *target = Some(value.into_owned());
    }
    Ok(query)
}

enum CallbackResult {
    Code(String),
    AccessDenied,
    ProviderError,
}

fn classify_result(query: &CallbackQuery) -> Result<CallbackResult, ()> {
    match (
        query.code.as_deref(),
        query.error.as_deref(),
        query.error_description.is_some(),
    ) {
        (Some(code), None, false) if !code.is_empty() => Ok(CallbackResult::Code(code.into())),
        (None, Some("access_denied"), _) => Ok(CallbackResult::AccessDenied),
        (None, Some(error), _) if !error.is_empty() => Ok(CallbackResult::ProviderError),
        _ => Err(()),
    }
}

fn random_code() -> Result<String, ()> {
    let mut bytes = Zeroizing::new([0_u8; 32]);
    SystemRandom::new().fill(&mut *bytes).map_err(|_error| ())?;
    Ok(URL_SAFE_NO_PAD.encode(bytes.as_slice()))
}

struct TrustedRedirect {
    url: Url,
    client_state: Option<String>,
}

impl TrustedRedirect {
    fn new(session: &OAuthAuthorizationSessionRecord) -> Option<Self> {
        Some(Self {
            url: Url::parse(&session.redirect_uri).ok()?,
            client_state: session.client_state.clone(),
        })
    }

    fn success(&self, code: &str) -> Response {
        self.redirect("code", code, None)
    }

    fn error(&self, error: &'static str, description: &'static str) -> Response {
        self.redirect("error", error, Some(description))
    }

    fn redirect(&self, key: &str, value: &str, description: Option<&str>) -> Response {
        let mut url = self.url.clone();
        let mut query = url.query_pairs_mut();
        query.append_pair(key, value);
        if let Some(description) = description {
            query.append_pair("error_description", description);
        }
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
        security_headers(),
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

fn redirect(location: &str) -> Response {
    (
        StatusCode::FOUND,
        security_headers(),
        [(header::LOCATION, location)],
        "",
    )
        .into_response()
}

fn security_headers() -> [(header::HeaderName, &'static str); 3] {
    [
        (header::CACHE_CONTROL, "no-store"),
        (header::PRAGMA, "no-cache"),
        (header::REFERRER_POLICY, "no-referrer"),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use base64::engine::general_purpose::STANDARD;
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::AuthorizationServerHttpState;
    use super::*;
    use crate::auth::PROVIDER_ID;
    use crate::auth::config::{AuthSettings, ResolvedAuthSettings};
    use crate::auth::id_token::tests::{
        claims as id_token_claims, rsa_key, set_claim, token as id_token,
    };
    use crate::auth::provider_client::OidcProviderClient;
    use crate::auth::session::SessionTokenIssuer;
    use crate::auth::state_store::{InMemoryStateStore, StateStore};

    const OIDC_STATE: &str = "oidc-state";
    const CLIENT_STATE: &str = "client&error=injected";
    const REDIRECT_URI: &str = "https://client.example.test/callback?tenant=one";
    const VERIFIER: &str = "stored-upstream-verifier";
    const NONCE: &str = "stored-upstream-nonce";
    const PROVIDER_CODE: &str = "provider-code-secret";
    const PROVIDER_SECRET: &str = "provider-secret-must-not-leak";
    const AUTH_ISSUER: &str = "https://auth.example.test";
    const RESOURCE: &str = "https://api.example.test/mcp";

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

    fn state(issuer: &str, state_store: Arc<dyn StateStore>) -> AuthorizationServerHttpState {
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
            state_store,
            provider_client: OidcProviderClient::new().expect("client"),
            registered_clients: Arc::new(BTreeMap::new()),
            authorization_resources: Arc::new(BTreeSet::from([RESOURCE.into()])),
        }
    }

    fn session() -> OAuthAuthorizationSessionRecord {
        OAuthAuthorizationSessionRecord {
            provider_id: PROVIDER_ID.into(),
            client_id: "https://client.example.test/oauth/client.json".into(),
            redirect_uri: REDIRECT_URI.into(),
            client_state: Some(CLIENT_STATE.into()),
            code_challenge: "client-code-challenge".into(),
            resource: RESOURCE.into(),
            oidc_code_verifier: VERIFIER.to_string().into(),
            oidc_nonce: NONCE.into(),
        }
    }

    fn query(pairs: &[(&str, &str)]) -> String {
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        for (key, value) in pairs {
            serializer.append_pair(key, value);
        }
        serializer.finish()
    }

    async fn callback_raw(state: AuthorizationServerHttpState, raw: String) -> Response {
        oidc_callback(State(state), RawQuery(Some(raw))).await
    }

    async fn callback(state: AuthorizationServerHttpState, pairs: &[(&str, &str)]) -> Response {
        callback_raw(state, query(pairs)).await
    }

    async fn seed(store: &dyn StateStore, key: &str) {
        store
            .store_authorization_session(key, session())
            .await
            .expect("store");
    }

    async fn take(store: &dyn StateStore, key: &str) -> Option<OAuthAuthorizationSessionRecord> {
        store.take_authorization_session(key).await.expect("store")
    }

    fn assert_security(response: &Response) {
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(response.headers()[header::PRAGMA], "no-cache");
        assert_eq!(response.headers()[header::REFERRER_POLICY], "no-referrer");
    }

    fn redirect_query(response: &Response) -> Vec<(String, String)> {
        Url::parse(
            response.headers()[header::LOCATION]
                .to_str()
                .expect("location"),
        )
        .expect("redirect URL")
        .query_pairs()
        .into_owned()
        .collect()
    }

    async fn mount_provider(server: &MockServer, token_status: u16, jwks_status: u16, nonce: &str) {
        let mut claims = id_token_claims();
        set_claim(&mut claims, "iss", Value::String(server.uri()));
        set_claim(&mut claims, "nonce", Value::String(nonce.into()));
        set_claim(&mut claims, "sub", Value::String("raw-principal".into()));
        let discovery = json!({
            "issuer": server.uri(),
            "authorization_endpoint": format!("{}/authorize", server.uri()),
            "token_endpoint": format!("{}/token", server.uri()),
            "jwks_uri": format!("{}/jwks", server.uri()),
            "id_token_signing_alg_values_supported": ["RS256"],
        });
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(discovery))
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(
                ResponseTemplate::new(token_status)
                    .set_body_json(json!({"id_token": id_token(&claims)})),
            )
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(
                ResponseTemplate::new(jwks_status).set_body_json(json!({"keys": [rsa_key()]})),
            )
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn malformed_queries_are_bounded_and_do_not_consume_state() {
        for raw in [
            "x".repeat(MAX_QUERY_BYTES + 1),
            format!("{}=x", "n".repeat(MAX_PARAMETER_NAME_BYTES + 1)),
            format!("x={}", "v".repeat(MAX_PARAMETER_VALUE_BYTES + 1)),
            (0..=MAX_PARAMETERS)
                .map(|index| format!("x{index}=1"))
                .collect::<Vec<_>>()
                .join("&"),
        ] {
            let Err(()) = parse_query(&raw) else {
                panic!("malformed callback query must be rejected");
            };
        }
        let store = Arc::new(InMemoryStateStore::new());
        seed(store.as_ref(), OIDC_STATE).await;
        let response = callback_raw(
            state("https://provider.invalid", store.clone()),
            format!("state={OIDC_STATE}&%73tate=duplicate&code=x"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!response.headers().contains_key(header::LOCATION));
        assert_security(&response);
        assert!(take(store.as_ref(), OIDC_STATE).await.is_some());
        let response = callback(
            state("https://provider.invalid", store),
            &[("state", "unknown"), ("code", "x")],
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!response.headers().contains_key(header::LOCATION));
    }

    #[tokio::test]
    async fn provider_errors_are_sanitized_and_malformed_results_consume_state() {
        let store = Arc::new(InMemoryStateStore::new());
        for (index, suffix, expected) in [
            (
                0,
                "error=access_denied&error_description=secret",
                "access_denied",
            ),
            (
                1,
                "error=provider_failure&error_description=secret",
                "server_error",
            ),
        ] {
            let state_key = format!("state-{index}");
            seed(store.as_ref(), &state_key).await;
            let response = callback_raw(
                state("https://provider.invalid", store.clone()),
                format!("state={state_key}&{suffix}"),
            )
            .await;
            let values = redirect_query(&response);
            assert!(values.contains(&("error".into(), expected.into())));
            assert!(values.contains(&("state".into(), CLIENT_STATE.into())));
            assert!(!values.iter().any(|(_key, value)| value == "secret"));
            assert_security(&response);
        }
        for (index, suffix) in [
            (2, "code=x&error=access_denied"),
            (3, "error="),
            (4, "error_description=orphan"),
            (5, ""),
        ] {
            let state_key = format!("state-{index}");
            seed(store.as_ref(), &state_key).await;
            let response = callback_raw(
                state("https://provider.invalid", store.clone()),
                format!("state={state_key}&{suffix}"),
            )
            .await;
            assert!(
                redirect_query(&response).contains(&("error".into(), "invalid_request".into()))
            );
            assert!(take(store.as_ref(), &state_key).await.is_none());
        }
    }

    #[tokio::test]
    async fn success_is_single_use_and_stores_raw_identity_with_bindings() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, NONCE).await;
        let store = Arc::new(InMemoryStateStore::new());
        seed(store.as_ref(), OIDC_STATE).await;
        let app = state(&server.uri(), store.clone());
        let raw = query(&[("state", OIDC_STATE), ("code", PROVIDER_CODE)]);
        let (first, second) = tokio::join!(
            callback_raw(app.clone(), raw.clone()),
            callback_raw(app, raw)
        );
        let (success, replay) = if first.status() == StatusCode::FOUND {
            (first, second)
        } else {
            (second, first)
        };
        assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
        assert_security(&success);
        let values = redirect_query(&success);
        assert!(values.contains(&("tenant".into(), "one".into())));
        assert_eq!(values.iter().filter(|(key, _)| key == "state").count(), 1);
        assert!(values.contains(&("state".into(), CLIENT_STATE.into())));
        let code = values
            .iter()
            .find(|(key, _value)| key == "code")
            .expect("authorization code")
            .1
            .clone();
        assert_eq!(code.len(), 43);
        let authorization = store
            .take_authorization_code_for_request(
                &code,
                &session().client_id,
                REDIRECT_URI,
                &session().code_challenge,
                RESOURCE,
            )
            .await
            .expect("store")
            .expect("authorization");
        assert_eq!(authorization.provider_id, PROVIDER_ID);
        assert_eq!(authorization.user_id, "raw-principal");
        assert_eq!(authorization.resource, RESOURCE);
        let requests = server.received_requests().await.expect("requests");
        let token_requests = requests
            .iter()
            .filter(|request| request.url.path() == "/token")
            .collect::<Vec<_>>();
        assert_eq!(token_requests.len(), 1);
        let token_request = token_requests.first().expect("token request");
        let form = form_urlencoded::parse(&token_request.body)
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            form.get("code_verifier").map(String::as_str),
            Some(VERIFIER)
        );
        let location = success.headers()[header::LOCATION]
            .to_str()
            .expect("location");
        for secret in [VERIFIER, PROVIDER_SECRET, PROVIDER_CODE] {
            assert!(!location.contains(secret));
        }
    }
}
