use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

use super::AuthState;

const MAX_BODY_BYTES: usize = 8 * 1024;
const MAX_PARAMETERS: usize = 32;
const MAX_PARAMETER_NAME_BYTES: usize = 64;
const MAX_PARAMETER_VALUE_BYTES: usize = 2 * 1024;

pub(super) async fn oauth_token(State(state): State<AuthState>, request: Request) -> Response {
    if let Some(response) = client_auth_rejection(request.headers()) {
        return response;
    }
    if !valid_content_type(request.headers()) {
        return token_error(TokenError::InvalidRequest);
    }
    let body = match to_bytes(request.into_body(), MAX_BODY_BYTES).await {
        Ok(body) => body,
        Err(_error) => return token_error(TokenError::InvalidRequest),
    };
    let body = Zeroizing::new(body.to_vec());
    let form = match parse_form(&body) {
        Ok(form) => form,
        Err(FormError::InvalidClient) => return token_error(TokenError::InvalidClient),
        Err(FormError::Malformed) => return token_error(TokenError::InvalidRequest),
    };
    match form.grant_type.as_ref().map(|value| value.as_str()) {
        Some("authorization_code") => {}
        None => return token_error(TokenError::InvalidRequest),
        Some(_) => return token_error(TokenError::UnsupportedGrantType),
    }
    let (Some(code), Some(client_id), Some(redirect_uri), Some(verifier)) = (
        form.code.as_deref(),
        form.client_id.as_deref(),
        form.redirect_uri.as_deref(),
        form.code_verifier.as_deref(),
    ) else {
        return token_error(TokenError::InvalidRequest);
    };
    if code.is_empty()
        || client_id.is_empty()
        || redirect_uri.is_empty()
        || !valid_code_verifier(verifier)
    {
        return token_error(TokenError::InvalidRequest);
    }
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let authorization = match state
        .store
        .take_authorization_code_for_request(code, client_id, redirect_uri, &challenge)
        .await
    {
        Ok(Some(authorization)) => authorization,
        Ok(None) => return token_error(TokenError::InvalidGrant),
        Err(_error) => return token_error(TokenError::ServerError),
    };
    let issued = match state
        .config
        .session
        .issue_access_token(&authorization.provider_id, &authorization.user_id)
    {
        Ok(issued) => issued,
        Err(_error) => return token_error(TokenError::ServerError),
    };
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_error) => return token_error(TokenError::ServerError),
    };
    let Some(expires_in) = issued.expires_at.checked_sub(now).filter(|ttl| *ttl > 0) else {
        return token_error(TokenError::ServerError);
    };
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "access_token": issued.access_token,
            "token_type": "Bearer",
            "expires_in": expires_in,
            "scope": authorization.scope,
        }),
    )
}

fn client_auth_rejection(headers: &axum::http::HeaderMap) -> Option<Response> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next()?;
    let challenge = value
        .to_str()
        .ok()
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _credentials)| {
            values.next().is_none() && axum::http::HeaderName::from_bytes(scheme.as_bytes()).is_ok()
        })
        .and_then(|(scheme, _credentials)| {
            axum::http::HeaderValue::try_from(format!("{scheme} realm=\"Coral OAuth\"")).ok()
        });
    let mut response = token_error(TokenError::InvalidClient);
    if let Some(challenge) = challenge {
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        let headers = response.headers_mut();
        headers.insert(header::WWW_AUTHENTICATE, challenge);
    }
    Some(response)
}

fn valid_content_type(headers: &axum::http::HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false;
    };
    value.to_str().is_ok_and(|value| {
        value.split(';').next().is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case("application/x-www-form-urlencoded")
        })
    })
}

#[derive(Default)]
struct TokenForm {
    grant_type: Option<Zeroizing<String>>,
    code: Option<Zeroizing<String>>,
    redirect_uri: Option<Zeroizing<String>>,
    client_id: Option<Zeroizing<String>>,
    code_verifier: Option<Zeroizing<String>>,
}

enum FormError {
    Malformed,
    InvalidClient,
}

fn parse_form(raw: &[u8]) -> Result<TokenForm, FormError> {
    let mut form = TokenForm::default();
    let mut seen = BTreeSet::new();
    if raw.is_empty() {
        return Ok(form);
    }
    for (index, field) in raw.split(|byte| *byte == b'&').enumerate() {
        if index >= MAX_PARAMETERS {
            return Err(FormError::Malformed);
        }
        let mut parts = field.splitn(2, |byte| *byte == b'=');
        let name = decode_component(parts.next().unwrap_or_default())?;
        let value = Zeroizing::new(decode_component(parts.next().unwrap_or_default())?);
        if name.len() > MAX_PARAMETER_NAME_BYTES || value.len() > MAX_PARAMETER_VALUE_BYTES {
            return Err(FormError::Malformed);
        }
        if name == "client_secret" {
            return Err(FormError::InvalidClient);
        }
        if !seen.insert(name.clone()) {
            return Err(FormError::Malformed);
        }
        let target = match name.as_str() {
            "grant_type" => &mut form.grant_type,
            "code" => &mut form.code,
            "redirect_uri" => &mut form.redirect_uri,
            "client_id" => &mut form.client_id,
            "code_verifier" => &mut form.code_verifier,
            _ => continue,
        };
        *target = Some(value);
    }
    Ok(form)
}

fn decode_component(raw: &[u8]) -> Result<String, FormError> {
    let mut decoded = Vec::with_capacity(raw.len());
    let mut bytes = raw.iter().copied();
    while let Some(byte) = bytes.next() {
        match byte {
            b'+' => decoded.push(b' '),
            b'%' => {
                let high = bytes
                    .next()
                    .and_then(hex_value)
                    .ok_or(FormError::Malformed)?;
                let low = bytes
                    .next()
                    .and_then(hex_value)
                    .ok_or(FormError::Malformed)?;
                decoded.push((high << 4) | low);
            }
            byte => decoded.push(byte),
        }
    }
    String::from_utf8(decoded).map_err(|_error| FormError::Malformed)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn valid_code_verifier(value: &str) -> bool {
    (43..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~'))
}

#[derive(Clone, Copy)]
enum TokenError {
    InvalidRequest,
    InvalidClient,
    InvalidGrant,
    UnsupportedGrantType,
    ServerError,
}

fn token_error(error: TokenError) -> Response {
    let status = match error {
        TokenError::ServerError => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::BAD_REQUEST,
    };
    let (error, description) = match error {
        TokenError::InvalidRequest => ("invalid_request", "token request is malformed"),
        TokenError::InvalidClient => ("invalid_client", "client authentication is not supported"),
        TokenError::InvalidGrant => ("invalid_grant", "authorization code is invalid or expired"),
        TokenError::UnsupportedGrantType => ("unsupported_grant_type", "grant type is unsupported"),
        TokenError::ServerError => ("server_error", "token issuance failed"),
    };
    json_response(
        status,
        &serde_json::json!({"error": error, "error_description": description}),
    )
}

fn json_response(status: StatusCode, body: &serde_json::Value) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        body.to_string(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{HeaderMap, HeaderValue};
    use serde_json::Value;
    use url::form_urlencoded;

    use super::super::{OAuthServerConfig, OidcAuthConfig};
    use super::*;
    use crate::auth::session::SessionTokenConfig;
    use crate::auth::state_store::{InMemoryStateStore, OAuthAuthorizationCodeRecord, StateStore};

    const CLIENT: &str = "https://auth.example.test/oauth/clients/web";
    const REDIRECT: &str = "http://127.0.0.1:14554/oauth/callback";
    const VERIFIER: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";
    const TTL: u64 = 300;

    fn state() -> (AuthState, Arc<InMemoryStateStore>, SessionTokenConfig) {
        let session = SessionTokenConfig::new(
            Some("https://auth.example.test"),
            Some("https://api.example.test/mcp"),
            [b'k'; 32],
            Duration::from_secs(TTL),
        )
        .expect("session");
        let oauth = OAuthServerConfig {
            issuer: session.issuer.clone(),
            resource: session.audience.clone(),
            scope: "coral:access".into(),
            clients: BTreeMap::new(),
        };
        let store = Arc::new(InMemoryStateStore::new());
        let state = AuthState::new(
            OidcAuthConfig {
                bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                session: session.clone(),
                providers: BTreeMap::new(),
                oauth,
            },
            store.clone(),
        )
        .expect("auth state");
        (state, store, session)
    }

    async fn seed(store: &dyn StateStore, code: &str, verifier: &str) {
        store
            .store_authorization_code(
                code,
                OAuthAuthorizationCodeRecord {
                    provider_id: "alpha".into(),
                    user_id: "raw/provider/subject".into(),
                    client_id: CLIENT.into(),
                    redirect_uri: REDIRECT.into(),
                    code_challenge: URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
                    scope: "stored:scope".into(),
                },
            )
            .await
            .expect("seed code");
    }

    fn form(code: &str, client: &str, redirect: &str, verifier: &str) -> String {
        form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("grant_type", "authorization_code"),
                ("code", code),
                ("client_id", client),
                ("redirect_uri", redirect),
                ("code_verifier", verifier),
            ])
            .finish()
    }

    fn set_field(raw: &str, name: &str, value: Option<&str>) -> String {
        let mut output = form_urlencoded::Serializer::new(String::new());
        for (key, old) in form_urlencoded::parse(raw.as_bytes()) {
            if key != name {
                output.append_pair(&key, &old);
            }
        }
        if let Some(value) = value {
            output.append_pair(name, value);
        }
        output.finish()
    }

    fn request(body: impl Into<Body>) -> Request {
        Request::builder()
            .method("POST")
            .header(
                header::CONTENT_TYPE,
                "Application/X-WWW-Form-Urlencoded; charset=UTF-8",
            )
            .body(body.into())
            .expect("request")
    }

    async fn send(state: AuthState, request: Request) -> (StatusCode, HeaderMap, String) {
        let response = oauth_token(State(state), request).await;
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("response body");
        let body = String::from_utf8(body.to_vec()).expect("UTF-8");
        (status, headers, body)
    }

    fn assert_security(headers: &HeaderMap) {
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        assert_eq!(headers[header::PRAGMA], "no-cache");
        assert_eq!(headers[header::REFERRER_POLICY], "no-referrer");
    }

    async fn expect_error(state: AuthState, request: Request, expected: &str) -> String {
        let (status, headers, body) = send(state, request).await;
        assert_security(&headers);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!headers.contains_key(header::WWW_AUTHENTICATE));
        let value: Value = serde_json::from_str(&body).expect("error JSON");
        assert_eq!(value.get("error").and_then(Value::as_str), Some(expected));
        body
    }

    async fn redeem(state: AuthState, code: &str, verifier: &str) -> (StatusCode, String) {
        let (status, headers, body) =
            send(state, request(form(code, CLIENT, REDIRECT, verifier))).await;
        assert_security(&headers);
        (status, body)
    }

    async fn assert_redeems(state: AuthState, code: &str, verifier: &str) {
        assert_eq!(redeem(state, code, verifier).await.0, StatusCode::OK);
    }

    async fn invalid_grant(state: AuthState, code: &str) -> String {
        let request = request(form(code, CLIENT, REDIRECT, VERIFIER));
        expect_error(state, request, "invalid_grant").await
    }

    #[tokio::test]
    async fn rejects_content_type_body_and_malformed_or_unbounded_forms() {
        let (state, _store, _session) = state();
        let mut duplicate_type = request(Body::empty());
        let content_type = HeaderValue::from_static("application/x-www-form-urlencoded");
        duplicate_type
            .headers_mut()
            .append(header::CONTENT_TYPE, content_type);
        let too_many = (0..33)
            .map(|index| format!("unknown{index}=x"))
            .collect::<Vec<_>>()
            .join("&");
        let requests = [
            Request::builder()
                .method("POST")
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method("POST")
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .unwrap(),
            duplicate_type,
            request(vec![b'x'; MAX_BODY_BYTES + 1]),
            request("code=%"),
            request("code=secret&%63ode=second"),
            request(too_many),
            request(format!("{}=x", "n".repeat(65))),
            request(format!("unknown={}", "x".repeat(2049))),
        ];
        for invalid in requests {
            let body = expect_error(state.clone(), invalid, "invalid_request").await;
            assert!(!body.contains("secret"));
        }
    }

    #[tokio::test]
    async fn client_auth_grant_and_verifier_failures_do_not_consume_codes() {
        let (state, store, _session) = state();
        for client_secret in [None, Some(""), Some("client-secret-must-not-leak")] {
            let code = format!("client-auth-{}", client_secret.is_some());
            seed(store.as_ref(), &code, VERIFIER).await;
            let mut body = form(&code, CLIENT, REDIRECT, VERIFIER);
            let mut invalid = request(body.clone());
            if let Some(secret) = client_secret {
                body = set_field(&body, "client_secret", Some(secret));
                invalid = request(body);
            } else {
                let headers = invalid.headers_mut();
                headers.append(header::AUTHORIZATION, HeaderValue::from_static("malformed"));
                headers.append(header::AUTHORIZATION, HeaderValue::from_static("duplicate"));
            }
            let body = expect_error(state.clone(), invalid, "invalid_client").await;
            assert!(!body.contains("secret"));
            assert_redeems(state.clone(), &code, VERIFIER).await;
        }
        seed(store.as_ref(), "basic-auth", VERIFIER).await;
        let mut basic = request(form("basic-auth", CLIENT, REDIRECT, VERIFIER));
        let authorization = HeaderValue::from_static("Basic ");
        let headers = basic.headers_mut();
        headers.insert(header::AUTHORIZATION, authorization);
        let (status, headers, body) = send(state.clone(), basic).await;
        assert_security(&headers);
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let challenge = headers.get(header::WWW_AUTHENTICATE).expect("challenge");
        assert_eq!(challenge, "Basic realm=\"Coral OAuth\"");
        assert!(body.contains("invalid_client"));
        assert_redeems(state.clone(), "basic-auth", VERIFIER).await;

        let edits = [
            ("grant_type", None, "invalid_request"),
            (
                "grant_type",
                Some("client_credentials"),
                "unsupported_grant_type",
            ),
            ("code", None, "invalid_request"),
            ("client_id", None, "invalid_request"),
            ("redirect_uri", None, "invalid_request"),
            ("code_verifier", None, "invalid_request"),
            ("code", Some(""), "invalid_request"),
            ("client_id", Some(""), "invalid_request"),
            ("redirect_uri", Some(""), "invalid_request"),
        ];
        for (index, (field, value, error)) in edits.into_iter().enumerate() {
            let code = format!("required-{index}");
            seed(store.as_ref(), &code, VERIFIER).await;
            let body = set_field(&form(&code, CLIENT, REDIRECT, VERIFIER), field, value);
            expect_error(state.clone(), request(body), error).await;
            assert_redeems(state.clone(), &code, VERIFIER).await;
        }

        for (index, verifier) in [
            "a".repeat(42),
            "a".repeat(129),
            format!("{} ", "a".repeat(42)),
            "é".repeat(43),
            format!("{}!", "a".repeat(42)),
        ]
        .into_iter()
        .enumerate()
        {
            let code = format!("verifier-{index}");
            seed(store.as_ref(), &code, VERIFIER).await;
            let invalid = request(form(&code, CLIENT, REDIRECT, &verifier));
            expect_error(state.clone(), invalid, "invalid_request").await;
            assert_redeems(state.clone(), &code, VERIFIER).await;
        }
    }

    #[tokio::test]
    async fn bindings_are_indistinguishable_and_valid_boundary_verifiers_redeem() {
        let (state, store, _session) = state();
        let wrong_verifier = format!("{}B", "a".repeat(42));
        let mut canonical = None;
        for (index, (client, redirect, verifier)) in [
            ("wrong-client", REDIRECT, VERIFIER),
            (CLIENT, "http://127.0.0.1/wrong", VERIFIER),
            (CLIENT, REDIRECT, wrong_verifier.as_str()),
        ]
        .into_iter()
        .enumerate()
        {
            let code = format!("binding-{index}");
            seed(store.as_ref(), &code, VERIFIER).await;
            let invalid = request(form(&code, client, redirect, verifier));
            let body = expect_error(state.clone(), invalid, "invalid_grant").await;
            canonical.get_or_insert(body.clone());
            assert_eq!(canonical.as_ref(), Some(&body));
            assert_redeems(state.clone(), &code, VERIFIER).await;
        }
        let replay = invalid_grant(state.clone(), "binding-0").await;
        let unknown = invalid_grant(state.clone(), "unknown-code").await;
        assert_eq!(canonical, Some(replay));
        assert_eq!(canonical, Some(unknown));

        for (index, verifier) in [
            format!("{}.~", "a".repeat(41)),
            format!("{}.~", "a".repeat(126)),
        ]
        .into_iter()
        .enumerate()
        {
            let code = format!("boundary-{index}");
            seed(store.as_ref(), &code, &verifier).await;
            assert_redeems(state.clone(), &code, &verifier).await;
        }
    }

    #[tokio::test]
    async fn success_returns_valid_raw_identity_token_and_is_single_use() {
        let (state, store, session) = state();
        seed(store.as_ref(), "success-code", VERIFIER).await;
        let (_status, body) = redeem(state.clone(), "success-code", VERIFIER).await;
        let value: Value = serde_json::from_str(&body).expect("token JSON");
        let get = |key| value.get(key).expect("response field");
        assert_eq!(get("token_type"), "Bearer");
        assert_eq!(get("scope"), "stored:scope");
        let expires_in = get("expires_in").as_u64().expect("expires_in");
        assert!(expires_in > 0 && expires_in <= TTL);
        let validated = session
            .validate_access_token(get("access_token").as_str().expect("access token"))
            .expect("valid access token");
        assert_eq!(validated.provider, "alpha");
        assert_eq!(validated.subject, "raw/provider/subject");
        invalid_grant(state.clone(), "success-code").await;

        seed(store.as_ref(), "concurrent-code", VERIFIER).await;
        let first = redeem(state.clone(), "concurrent-code", VERIFIER);
        let second = redeem(state, "concurrent-code", VERIFIER);
        let (first, second) = tokio::join!(first, second);
        assert_eq!(
            [first.0, second.0]
                .into_iter()
                .filter(|status| *status == StatusCode::OK)
                .count(),
            1
        );
        assert!(first.1.contains("invalid_grant") || second.1.contains("invalid_grant"));
    }
}
