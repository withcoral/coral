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

use super::AuthorizationServerHttpState;
use super::query::{MAX_PARAMETER_NAME_BYTES, MAX_PARAMETER_VALUE_BYTES, MAX_PARAMETERS};
use super::response::security_headers;
use coral_auth::PrivateAudience;
use coral_auth::authorization_codes::{AuthorizationGrant, CodeRedemption};

/// The largest form body this endpoint reads.
///
/// This is the endpoint's own limit rather than one of the shared parameter
/// limits: it bounds a request body, which the parameter limits do not, and it
/// is enforced while reading rather than while parsing.
const MAX_BODY_BYTES: usize = 8 * 1024;

pub(super) async fn oauth_token(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
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
    let body = Zeroizing::new(Vec::from(body));
    let form = match parse_form(&body) {
        Ok(form) => form,
        Err(FormError::InvalidClient) => return token_error(TokenError::InvalidClient),
        Err(FormError::InvalidScope) => return token_error(TokenError::InvalidScope),
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
    let Some(resource) = form
        .resource
        .as_deref()
        .filter(|resource| !resource.is_empty())
    else {
        return token_error(TokenError::InvalidTarget);
    };
    let resource = match super::canonical_authorization_resource(resource) {
        Ok(resource) => resource,
        Err(_error) => return token_error(TokenError::InvalidTarget),
    };
    if code.is_empty()
        || client_id.is_empty()
        || redirect_uri.is_empty()
        || !valid_code_verifier(verifier)
    {
        return token_error(TokenError::InvalidRequest);
    }
    let Some(target) = TokenTarget::resolve(
        &state,
        &resource,
        form.coral_audience.as_deref().map(String::as_str),
    ) else {
        return token_error(TokenError::InvalidTarget);
    };
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let redemption = CodeRedemption {
        client_id,
        redirect_uri,
        code_challenge: &challenge,
        resource: &resource,
    };
    let authorization = match redeem(&state, code, redemption).await {
        Ok(authorization) => authorization,
        Err(error) => return token_error(error),
    };
    // This endpoint completes an interactive login, which only a person can
    // perform: the code being redeemed was issued to a browser that carried
    // someone through their identity provider. An agent obtains its own
    // credential elsewhere, and whatever mints one declares it there.
    let issued = match target.issue(&state.session_tokens, &authorization) {
        Ok(issued) => issued,
        Err(error) => {
            tracing::warn!(%error, "token endpoint could not issue an access token");
            return token_error(TokenError::ServerError);
        }
    };
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(error) => {
            tracing::warn!(%error, "token endpoint could not read the system clock");
            return token_error(TokenError::ServerError);
        }
    };
    let Some(expires_in) = issued.expires_at.checked_sub(now).filter(|ttl| *ttl > 0) else {
        tracing::warn!("token endpoint issued an access token that had already expired");
        return token_error(TokenError::ServerError);
    };
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "access_token": issued.access_token,
            "token_type": "Bearer",
            "expires_in": expires_in,
        }),
    )
}

/// Spends `code` when `redemption` repeats every binding it was issued with.
async fn redeem(
    state: &AuthorizationServerHttpState,
    code: &str,
    redemption: CodeRedemption<'_>,
) -> Result<AuthorizationGrant, TokenError> {
    let Ok(now_unix_nanos) = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ())
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).map_err(|_error| ()))
    else {
        tracing::warn!("token endpoint could not read the system clock");
        return Err(TokenError::ServerError);
    };
    match state.codes.redeem(code, redemption, now_unix_nanos).await {
        Ok(Some(authorization)) => Ok(authorization),
        Ok(None) => Err(TokenError::InvalidGrant),
        Err(error) => {
            tracing::warn!(%error, "token endpoint could not redeem the authorization code");
            Err(TokenError::ServerError)
        }
    }
}

// First-party selection narrows the resource approved at login; it never
// changes the User, creates a workload credential, or upgrades MCP to Reef.
enum TokenTarget {
    Public,
    Mcp(coral_workspace::WorkspaceId),
    Reef,
    /// The Organization MCP Endpoint: its public URL audience, plus the
    /// Workspace the User picked when they signed in.
    Organization,
}

impl TokenTarget {
    fn resolve(
        state: &AuthorizationServerHttpState,
        resource: &str,
        audience: Option<&str>,
    ) -> Option<Self> {
        if state.is_organization_resource(resource) {
            // Its token must carry the picked Workspace, which only the
            // public audience does; a private audience would drop or replace it.
            return audience.is_none().then_some(Self::Organization);
        }
        let workspace = state
            .workspace_resource_family
            .as_ref()
            .and_then(|family| family.parse_resource(resource));
        let Some(audience) = audience else {
            return Some(Self::Public);
        };
        match PrivateAudience::parse(audience)? {
            PrivateAudience::Mcp => workspace.map(Self::Mcp),
            PrivateAudience::Reef => (workspace.is_none()
                && state.authorization_resources.contains(resource))
            .then_some(Self::Reef),
        }
    }

    fn issue(
        &self,
        issuer: &coral_auth::SessionTokenIssuer,
        authorization: &AuthorizationGrant,
    ) -> Result<coral_auth::IssuedAccessToken, String> {
        match self {
            Self::Mcp(workspace) => issuer.issue_workspace_access_token(
                &authorization.user_id,
                &authorization.client_id,
                PrivateAudience::Mcp.as_str(),
                workspace,
            ),
            Self::Reef => issuer.issue_access_token(
                &authorization.user_id,
                &authorization.client_id,
                PrivateAudience::Reef.as_str(),
            ),
            Self::Public => issuer.issue_access_token(
                &authorization.user_id,
                &authorization.client_id,
                &authorization.resource,
            ),
            Self::Organization => {
                let workspace = authorization
                    .workspace_id
                    .as_deref()
                    .and_then(|workspace| coral_workspace::WorkspaceId::parse(workspace).ok())
                    .ok_or("an Organization MCP Endpoint code names no workspace")?;
                issuer.issue_workspace_access_token(
                    &authorization.user_id,
                    &authorization.client_id,
                    &authorization.resource,
                    &workspace,
                )
            }
        }
    }
}

fn client_auth_rejection(headers: &axum::http::HeaderMap) -> Option<Response> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next()?;
    // RFC 6749 section 5.2 requires the 401 challenge to name the scheme the
    // client authenticated with. A duplicated or malformed header names no
    // single scheme, so it stays a plain `invalid_client` without a challenge.
    let challenge = if values.next().is_some() {
        None
    } else {
        value
            .to_str()
            .ok()
            .and_then(|value| value.split_once(' '))
            .filter(|(scheme, _credentials)| {
                axum::http::HeaderName::from_bytes(scheme.as_bytes()).is_ok()
            })
            .and_then(|(scheme, _credentials)| {
                axum::http::HeaderValue::try_from(format!("{scheme} realm=\"Coral OAuth\"")).ok()
            })
    };
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
    resource: Option<Zeroizing<String>>,
    coral_audience: Option<Zeroizing<String>>,
}

enum FormError {
    Malformed,
    InvalidClient,
    InvalidScope,
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
        if name == "scope" {
            return Err(FormError::InvalidScope);
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
            "resource" => &mut form.resource,
            "coral_audience" => &mut form.coral_audience,
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
    InvalidScope,
    InvalidTarget,
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
        TokenError::InvalidScope => ("invalid_scope", "scope is not supported"),
        TokenError::InvalidTarget => (
            "invalid_target",
            "resource is required and must be a valid resource URI",
        ),
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
        security_headers(),
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{HeaderMap, HeaderValue};

    use serde_json::Value;
    use url::form_urlencoded;

    use super::super::query;
    use super::super::test_state::{database, http_state};
    use super::*;
    use coral_auth::SessionTokenIssuer;
    use coral_auth::authorization_codes::{AuthorizationCodes, CodeIssue};
    use coral_test_support::TestDatabase;

    const CLIENT: &str = "https://client.example.test/oauth/client.json";
    const REDIRECT: &str = "http://127.0.0.1:14554/oauth/callback";
    const VERIFIER: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";
    const RESOURCE: &str = "https://api.example.test/mcp";

    /// The token route's state over a fresh database of authorization codes.
    async fn state() -> (
        AuthorizationServerHttpState,
        AuthorizationCodes,
        SessionTokenIssuer,
        TestDatabase,
    ) {
        let database = database().await;
        let state = http_state(&database, "https://accounts.example.test", "", &[RESOURCE]);
        let codes = state.codes.clone();
        let session_tokens = state.session_tokens.clone();
        (state, codes, session_tokens, database)
    }

    fn now_unix_nanos() -> i64 {
        let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock");
        i64::try_from(elapsed.as_nanos()).expect("clock")
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one table follows each resource and audience to the token it issues"
    )]
    async fn first_party_tokens_keep_the_authorized_surface_and_workspace() {
        let workspace =
            coral_workspace::WorkspaceId::parse(&uuid::Uuid::new_v4().to_string()).unwrap();
        let workspace_resource = format!("https://mcp.example.test/workspaces/{workspace}/mcp");
        let organization = "https://mcp.example.test/mcp";
        let picked = Some(workspace.to_string());
        // (resource, coral_audience, Workspace the sign-in picked, status)
        for (resource, requested, chosen, expected_status) in [
            (
                workspace_resource.as_str(),
                Some("mcp"),
                None,
                StatusCode::OK,
            ),
            (
                workspace_resource.as_str(),
                Some("reef"),
                None,
                StatusCode::BAD_REQUEST,
            ),
            (
                workspace_resource.as_str(),
                Some("unknown"),
                None,
                StatusCode::BAD_REQUEST,
            ),
            (RESOURCE, Some("reef"), None, StatusCode::OK),
            (RESOURCE, Some("mcp"), None, StatusCode::BAD_REQUEST),
            (organization, None, picked.clone(), StatusCode::OK),
            (
                organization,
                Some("mcp"),
                picked.clone(),
                StatusCode::BAD_REQUEST,
            ),
            (
                organization,
                Some("reef"),
                picked.clone(),
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let (mut state, codes, _, _database) = state().await;
            state.workspace_resource_family = Some(Arc::new(
                crate::WorkspaceMcpUrls::new(
                    crate::CanonicalOauthUrl::parse("https://mcp.example.test").unwrap(),
                )
                .unwrap(),
            ));
            codes
                .issue(
                    "code",
                    "sign-in",
                    &AuthorizationGrant {
                        user_id: "user".into(),
                        client_id: CLIENT.into(),
                        redirect_uri: REDIRECT.into(),
                        code_challenge: URL_SAFE_NO_PAD.encode(Sha256::digest(VERIFIER.as_bytes())),
                        resource: resource.to_owned(),
                        workspace_id: chosen,
                    },
                    now_unix_nanos(),
                )
                .await
                .unwrap();
            let server = super::super::start_listener(
                "127.0.0.1:0".parse().unwrap(),
                super::super::auth_router(state),
            )
            .await
            .unwrap();
            let body = set_field(
                &set_field(
                    &form("code", CLIENT, REDIRECT, VERIFIER),
                    "resource",
                    Some(resource),
                ),
                "coral_audience",
                requested,
            );
            let response = reqwest::Client::new()
                .post(format!("{}/oauth/token", server.endpoint_uri()))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(body)
                .send()
                .await
                .unwrap();
            let case = format!("{resource} as {requested:?}");
            assert_eq!(response.status(), expected_status, "{case}");
            let json: Value = response.json().await.unwrap();
            if expected_status == StatusCode::OK {
                let token = json.get("access_token").unwrap().as_str().unwrap();
                let claims: Value = serde_json::from_slice(
                    &URL_SAFE_NO_PAD
                        .decode(token.split('.').nth(1).unwrap())
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(
                    claims.get("aud").unwrap(),
                    requested.unwrap_or(resource),
                    "{case}"
                );
                assert_eq!(claims.get("sub").unwrap(), "user");
                if requested == Some("mcp") || resource == organization {
                    assert_eq!(
                        claims.get("workspace_id").unwrap(),
                        &workspace.to_string(),
                        "{case}"
                    );
                } else {
                    assert!(claims.get("workspace_id").is_none(), "{case}");
                }
            } else {
                assert_eq!(json.get("error").unwrap(), "invalid_target", "{case}");
            }
            server.shutdown().await.unwrap();
        }
    }

    /// Issues `code` from a sign-in of its own, for a client proving `verifier`.
    async fn seed(codes: &AuthorizationCodes, code: &str, verifier: &str) {
        let issued = codes
            .issue(
                code,
                &format!("sign-in-for-{code}"),
                &AuthorizationGrant {
                    user_id: "raw/provider/subject".into(),
                    client_id: CLIENT.into(),
                    redirect_uri: REDIRECT.into(),
                    code_challenge: URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
                    resource: RESOURCE.into(),
                    workspace_id: None,
                },
                now_unix_nanos(),
            )
            .await
            .expect("seed code");
        assert_eq!(issued, CodeIssue::Issued, "each seeded code is distinct");
    }

    fn form(code: &str, client: &str, redirect: &str, verifier: &str) -> String {
        form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("grant_type", "authorization_code"),
                ("code", code),
                ("client_id", client),
                ("redirect_uri", redirect),
                ("code_verifier", verifier),
                ("resource", RESOURCE),
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

    async fn send(
        state: AuthorizationServerHttpState,
        request: Request,
    ) -> (StatusCode, HeaderMap, String) {
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

    async fn expect_error(
        state: AuthorizationServerHttpState,
        request: Request,
        expected: &str,
    ) -> String {
        let (status, headers, body) = send(state, request).await;
        assert_security(&headers);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!headers.contains_key(header::WWW_AUTHENTICATE));
        let value: Value = serde_json::from_str(&body).expect("error JSON");
        assert_eq!(value.get("error").and_then(Value::as_str), Some(expected));
        body
    }

    async fn redeem(
        state: AuthorizationServerHttpState,
        code: &str,
        verifier: &str,
    ) -> (StatusCode, String) {
        let (status, headers, body) =
            send(state, request(form(code, CLIENT, REDIRECT, verifier))).await;
        assert_security(&headers);
        (status, body)
    }

    async fn assert_redeems(state: AuthorizationServerHttpState, code: &str, verifier: &str) {
        assert_eq!(redeem(state, code, verifier).await.0, StatusCode::OK);
    }

    async fn invalid_grant(state: AuthorizationServerHttpState, code: &str) -> String {
        let request = request(form(code, CLIENT, REDIRECT, VERIFIER));
        expect_error(state, request, "invalid_grant").await
    }

    #[tokio::test]
    async fn rejects_content_type_body_and_malformed_or_unbounded_forms() {
        let (state, _codes, _session, _database) = state().await;
        let mut duplicate_type = request(Body::empty());
        let content_type = HeaderValue::from_static("application/x-www-form-urlencoded");
        duplicate_type
            .headers_mut()
            .append(header::CONTENT_TYPE, content_type);
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
            request(format!(
                "{}&resource=duplicate",
                form("code", CLIENT, REDIRECT, VERIFIER)
            )),
        ];
        let requests = requests
            .into_iter()
            .chain(query::rejected_parameter_lists().into_iter().map(request));
        for invalid in requests {
            let body = expect_error(state.clone(), invalid, "invalid_request").await;
            assert!(!body.contains("secret"));
        }
    }

    #[tokio::test]
    async fn client_auth_grant_and_verifier_failures_do_not_consume_codes() {
        let (state, codes, _session, _database) = state().await;
        for (index, client_secret) in [None, Some(""), Some("client-secret-must-not-leak")]
            .into_iter()
            .enumerate()
        {
            let code = format!("client-auth-{index}");
            seed(&codes, &code, VERIFIER).await;
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
        seed(&codes, "basic-auth", VERIFIER).await;
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
            ("resource", None, "invalid_target"),
            ("code", Some(""), "invalid_request"),
            ("client_id", Some(""), "invalid_request"),
            ("redirect_uri", Some(""), "invalid_request"),
            ("resource", Some(""), "invalid_target"),
            ("scope", Some("coral:access"), "invalid_scope"),
        ];
        for (index, (field, value, error)) in edits.into_iter().enumerate() {
            let code = format!("required-{index}");
            seed(&codes, &code, VERIFIER).await;
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
            seed(&codes, &code, VERIFIER).await;
            let invalid = request(form(&code, CLIENT, REDIRECT, &verifier));
            expect_error(state.clone(), invalid, "invalid_request").await;
            assert_redeems(state.clone(), &code, VERIFIER).await;
        }
    }

    #[tokio::test]
    async fn bindings_are_indistinguishable_and_valid_boundary_verifiers_redeem() {
        let (state, codes, _session, _database) = state().await;
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
            seed(&codes, &code, VERIFIER).await;
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

        seed(&codes, "wrong-resource", VERIFIER).await;
        let body = set_field(
            &form("wrong-resource", CLIENT, REDIRECT, VERIFIER),
            "resource",
            Some("https://other.example.test/mcp"),
        );
        expect_error(state.clone(), request(body), "invalid_grant").await;
        assert_redeems(state.clone(), "wrong-resource", VERIFIER).await;

        seed(&codes, "invalid-resource", VERIFIER).await;
        let body = set_field(
            &form("invalid-resource", CLIENT, REDIRECT, VERIFIER),
            "resource",
            Some("not a resource URI"),
        );
        expect_error(state.clone(), request(body), "invalid_target").await;
        assert_redeems(state.clone(), "invalid-resource", VERIFIER).await;

        seed(&codes, "canonical-resource", VERIFIER).await;
        let body = set_field(
            &form("canonical-resource", CLIENT, REDIRECT, VERIFIER),
            "resource",
            Some("https://API.EXAMPLE.TEST/mcp"),
        );
        assert_eq!(send(state.clone(), request(body)).await.0, StatusCode::OK);

        for (index, verifier) in [
            format!("{}.~", "a".repeat(41)),
            format!("{}.~", "a".repeat(126)),
        ]
        .into_iter()
        .enumerate()
        {
            let code = format!("boundary-{index}");
            seed(&codes, &code, &verifier).await;
            assert_redeems(state.clone(), &code, &verifier).await;
        }
    }

    #[tokio::test]
    async fn success_returns_valid_raw_identity_token_and_is_single_use() {
        let (state, codes, session, _database) = state().await;
        seed(&codes, "success-code", VERIFIER).await;
        let (_status, body) = redeem(state.clone(), "success-code", VERIFIER).await;
        let value: Value = serde_json::from_str(&body).expect("token JSON");
        let get = |key| value.get(key).expect("response field");
        assert_eq!(get("token_type"), "Bearer");
        assert!(value.get("scope").is_none());
        let expires_in = get("expires_in").as_u64().expect("expires_in");
        assert!(expires_in > 0 && expires_in <= session.access_token_ttl().as_secs());
        let fixture = database().await;
        let organization = coral_workspace::OrganizationModule::new(fixture.database());
        let user = coral_workspace::AuthenticatedUser::organization_member(
            coral_workspace::UserId::parse("raw/provider/subject").unwrap(),
            coral_workspace::OrganizationRole::Member,
        );
        organization
            .provision_login(&coral_workspace::ChangeContext::new(
                user.clone(),
                1,
                coral_workspace::TraceId::current_or_new(),
            ))
            .await
            .unwrap();
        let authentication = coral_auth::PrincipalAuthentication::new(
            &session,
            organization,
            coral_workspace::WorkspaceModule::new(fixture.database()),
        );
        let token = get("access_token").as_str().expect("access token");
        let principal = authentication
            .authenticate(
                &coral_workspace::WorkspaceId::parse("01234567-89ab-4cde-8fab-0123456789ab")
                    .unwrap(),
                Some(token),
                RESOURCE,
            )
            .await
            .expect("redeemed token authenticates through the domain Interface");
        assert_eq!(principal.principal().id().as_str(), user.user_id().as_str());
        // Wire-format assertions only; authentication above verifies the credential.
        let claims = jsonwebtoken::dangerous::insecure_decode::<Value>(token)
            .unwrap()
            .claims;
        assert_eq!(claims.get("aud").and_then(Value::as_str), Some(RESOURCE));
        assert_eq!(
            claims.get("client_id").and_then(Value::as_str),
            Some(CLIENT)
        );
        // Single use is the code store's guarantee rather than this handler's:
        // a redemption spends the code in one conditional update, so two
        // redemptions cannot both see it unspent. This handler only turns the
        // resulting `None` into `invalid_grant`, which is what the replay below
        // asserts. The atomicity itself is covered by `coral_auth`'s
        // `a_code_issued_by_one_process_is_redeemed_once_by_any`.
        invalid_grant(state, "success-code").await;
    }
}
