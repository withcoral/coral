use super::super::provider_client::OidcAuthorizationRequest;
use super::super::sealed_handshake::{ClientRequest, PendingApproval, SignInSession};
use super::client_metadata::OAuthClientRegistration;
use super::response::{TrustedRedirect, direct_error, redirect};
use super::{
    AuthorizationServerHttpState, canonical_authorization_resource, query, sign_in_cookie,
};
use axum::extract::{RawQuery, Request, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use coral_outbound::{BrowserRedirect, EndpointUrl};

use super::approval_page::{self, ApprovalDecision, ApprovalScope};

pub(super) async fn oauth_authorize_get(
    State(state): State<AuthorizationServerHttpState>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Ok(query) = parse_query(raw_query.as_deref().unwrap_or_default()) else {
        return direct_error("invalid_request", "authorization request is malformed");
    };
    let (Some(client_id), Some(redirect_uri)) =
        (query.client_id.as_deref(), query.redirect_uri.as_deref())
    else {
        return direct_error("invalid_request", "client_id and redirect_uri are required");
    };
    let Ok(registration) = resolve_client(&state, client_id).await else {
        return direct_error("invalid_request", "client_id or redirect_uri is invalid");
    };
    // The web client returning to its login callback is pinned to the web
    // resource below. Returning to any other callback its metadata document
    // names — Provider Setup consent — it is validated like any other client.
    let web_login = state
        .settings
        .authorization_server()
        .web_login(client_id, redirect_uri);
    let Some(callback) = registered_redirect(&registration.redirect_uris, redirect_uri) else {
        return direct_error("invalid_request", "client_id or redirect_uri is invalid");
    };
    let client_name = registration.client_name;
    let trusted = TrustedRedirect::new(callback.as_url().clone(), query.state.clone());

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
    let Some(target) = state
        .authorization_resource(&resource)
        .filter(|_| web_login.is_none_or(|web| web.public_url == resource))
    else {
        return trusted.error("invalid_target", "resource does not match this server");
    };
    let approval = PendingApproval {
        request: ClientRequest {
            client_id: client_id.to_string(),
            redirect_uri: redirect_uri.to_string(),
            client_state: query.state,
            code_challenge,
            resource,
        },
        client_name,
    };
    let sign_in = approval.clone().into_sign_in(target.picks_workspace());
    // The sign-in cookie carries these inputs, so they must fit in one.
    if !sign_in_cookie::app_input_fits(&sign_in) {
        let inputs = if sign_in.client_name.is_some() {
            "client_id, redirect_uri, state and client_name are too long together"
        } else {
            "client_id, redirect_uri and state are too long together"
        };
        return trusted.error("invalid_request", inputs);
    }
    let sealed_len = sign_in.sealed_len();
    if !sealed_len.is_ok_and(|length| sign_in_cookie::sealed_fits(length, state.secure_cookies())) {
        return trusted.error("invalid_request", "authorization request is too large");
    }
    // A trusted client proceeds as if the user had already pressed Allow.
    // The check sits after every validation above, so trust changes only the
    // confirmation step — never what counts as a valid request — and the
    // metadata document still had to resolve and name this redirect URI.
    // At the Organization MCP Endpoint the person approves after sign-in, on
    // the page where they pick the Workspace, so no page shows here.
    if target.picks_workspace()
        || state
            .settings
            .authorization_server()
            .is_trusted_client(client_id, redirect_uri)
    {
        return provider_sign_in(&state, sign_in, &trusted, &headers).await;
    }
    consent_page(&state, &approval, &callback, &headers)
        .unwrap_or_else(|| trusted.error("server_error", "authorization failed"))
}

/// Renders the consent page carrying `approval`, sealed for this browser.
fn consent_page(
    state: &AuthorizationServerHttpState,
    approval: &PendingApproval,
    callback: &EndpointUrl<BrowserRedirect>,
    headers: &HeaderMap,
) -> Option<Response> {
    let secure_cookie = state.secure_cookies();
    let browser_binding = approval_page::browser_binding_for_page(headers, secure_cookie).ok()?;
    let expires_at = sign_in_cookie::now_unix_secs()? + approval_page::APPROVAL_MAX_AGE_SECONDS;
    let ticket = approval
        .seal(
            state.settings.handshake_keys(),
            browser_binding.as_bytes(),
            expires_at,
        )
        .ok()?;
    approval_page::response(
        &ticket,
        &approval.client_name,
        &approval.request.client_id,
        callback,
        &ApprovalScope::Workspace,
        &browser_binding,
        secure_cookie,
    )
}

pub(super) async fn oauth_authorize_post(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    let headers = request.headers().clone();
    // Only the picker after an Organization MCP Endpoint sign-in sends a
    // Workspace.
    let Ok(approval_page::Submission {
        ticket,
        browser_binding,
        decision,
        workspace: None,
    }) = approval_page::parse_submission(
        request,
        state.settings.authorization_server().issuer(),
        state.secure_cookies(),
    )
    .await
    else {
        return approval_error();
    };
    let keys = state.settings.handshake_keys();
    let Some(approval) = sign_in_cookie::now_unix_secs().and_then(|now| {
        PendingApproval::open(keys, browser_binding.as_bytes(), ticket.as_bytes(), now)
    }) else {
        return approval_error();
    };
    let Some(trusted) = TrustedRedirect::parse(
        &approval.request.redirect_uri,
        approval.request.client_state.clone(),
    ) else {
        return direct_error("server_error", "authorization failed");
    };
    if decision == ApprovalDecision::Cancel {
        return trusted.error("access_denied", "authorization was denied");
    }
    let picks_workspace = state
        .authorization_resource(&approval.request.resource)
        .is_some_and(|target| target.picks_workspace());
    provider_sign_in(
        &state,
        approval.into_sign_in(picks_workspace),
        &trusted,
        &headers,
    )
    .await
}

/// Sends an approved sign-in on to the upstream provider.
///
/// This is the step an Allow submission continues into and a trusted client
/// skips to directly. The redirect hands this browser the sign-in cookie that
/// carries the sealed session, so the callback can never arrive ahead of the
/// state it needs, any process can finish it, and only this browser can.
/// Older sign-ins beyond the per-browser cap are expired on the same response.
async fn provider_sign_in(
    state: &AuthorizationServerHttpState,
    session: SignInSession,
    trusted: &TrustedRedirect,
    headers: &HeaderMap,
) -> Response {
    let keys = state.settings.handshake_keys();
    let Ok(OidcAuthorizationRequest {
        url,
        state: oidc_state,
    }) = state
        .provider_client
        .authorization_request(state.settings.provider(), keys)
        .await
    else {
        return trusted.error("server_error", "authorization failed");
    };
    let secure = state.secure_cookies();
    let Some(cookie) = sign_in_cookie::now_unix_secs().and_then(|now| {
        let sealed = session
            .seal(keys, &oidc_state, now + sign_in_cookie::MAX_AGE_SECONDS)
            .ok()?;
        sign_in_cookie::set_header(
            &sign_in_cookie::name(&oidc_state, now, secure),
            &sealed,
            secure,
        )
    }) else {
        return trusted.error("server_error", "authorization failed");
    };
    let mut response = redirect(url.as_str());
    for evicted in sign_in_cookie::evicted_by_new_sign_in(headers, secure) {
        if let Some(clear) = sign_in_cookie::clear_header(&evicted, secure) {
            response.headers_mut().append(header::SET_COOKIE, clear);
        }
    }
    response.headers_mut().append(header::SET_COOKIE, cookie);
    response
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
    let mut parsed = AuthorizeQuery::default();
    query::scan(raw, |name, value| {
        let target = match name {
            "response_type" => &mut parsed.response_type,
            "client_id" => &mut parsed.client_id,
            "redirect_uri" => &mut parsed.redirect_uri,
            "scope" => &mut parsed.scope,
            "state" => &mut parsed.state,
            "code_challenge" => &mut parsed.code_challenge,
            "code_challenge_method" => &mut parsed.code_challenge_method,
            "resource" => &mut parsed.resource,
            _ => return,
        };
        *target = Some(value.into_owned());
    })?;
    Ok(parsed)
}

async fn resolve_client(
    state: &AuthorizationServerHttpState,
    client_id: &str,
) -> Result<OAuthClientRegistration, ()> {
    state
        .client_metadata_resolver
        .resolve(client_id)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "OAuth client metadata resolution failed");
        })
}

/// Resolves the redirect URI a request names, under [`BrowserRedirect`].
///
/// Both sides are remote input — the candidates come from the client's own
/// metadata document, the requested URI from the query string — so the policy
/// is applied here to each of them rather than trusted to have been applied
/// where they arrived. A URI the policy rejects, and a requested URI no
/// registration covers, both resolve to `None`, and the request fails with a
/// direct error instead of becoming a redirect.
///
/// The resolved URI is the requested one, not the registration that matched it:
/// under [`loopback_port_variant`] the two can differ in port, and it is the
/// requested port that a native client is listening on.
fn registered_redirect(
    redirect_uris: &[String],
    redirect_uri: &str,
) -> Option<EndpointUrl<BrowserRedirect>> {
    let requested = EndpointUrl::<BrowserRedirect>::parse(redirect_uri).ok()?;
    let matched = redirect_uris.iter().any(|uri| {
        uri.as_str() == redirect_uri
            || EndpointUrl::<BrowserRedirect>::parse(uri)
                .is_ok_and(|registered| loopback_port_variant(&registered, &requested))
    });
    matched.then_some(requested)
}

/// Reports whether `requested` is `registered` listening on another port.
///
/// A native client asks the operating system for a free port as it starts the
/// flow, so the port it will receive its callback on cannot be in the metadata
/// document it published earlier. RFC 8252 §7.3 therefore requires an
/// authorization server to "allow any port to be specified at the time of the
/// request for loopback IP redirect URIs"; matching those byte for byte leaves
/// every native client unable to authorize at all.
///
/// The relaxation is confined to a registered URI that is loopback over plain
/// HTTP — the one shape that can only describe a listener on the user's own
/// machine — and it moves the port alone, by re-serializing the registered URI
/// on the requested port and requiring the result to equal the request. Scheme,
/// host, path, and query still have to match, and a registered URI that is not
/// loopback never reaches the comparison, so no other host becomes reachable on
/// a port its client never registered.
fn loopback_port_variant(
    registered: &EndpointUrl<BrowserRedirect>,
    requested: &EndpointUrl<BrowserRedirect>,
) -> bool {
    if !registered.is_loopback_http() {
        return false;
    }
    let mut candidate = registered.as_url().clone();
    candidate.set_port(requested.as_url().port()).is_ok() && candidate == *requested.as_url()
}

fn valid_s256_challenge(value: &str) -> bool {
    value.len() == 43
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn approval_error() -> Response {
    direct_error(
        "invalid_request",
        "authorization approval is invalid or expired",
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::body::{Body, to_bytes};
    use axum::http::{HeaderValue, StatusCode, header};
    use base64::Engine as _;
    use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};

    use serde_json::json;
    use sha2::{Digest as _, Sha256};
    use url::{Url, form_urlencoded};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::AuthorizationServerHttpState;
    use super::super::client_metadata::{
        ClientMetadataError, ClientMetadataResolver, OAuthClientRegistration,
    };
    use super::super::test_state::{database, http_state};
    use super::*;
    use crate::auth::sealed_handshake::SignInSession;
    use crate::auth::test_config::{AUTH_ISSUER, PROVIDER_SECRET, resolved_settings};
    use coral_test_support::TestDatabase;

    const RESOURCE: &str = "https://api.example.test/mcp";
    const CLIENT_ID: &str = "https://client.example.test/oauth/client.json";
    const REDIRECT_URI: &str = "https://client.example.test/callback?tenant=one";
    const CLIENT_STATE: &str = "client-state&error=injected";
    const CHALLENGE: &str = "0123456789012345678901234567890123456789012";
    const BROWSER_BINDING: [u8; 32] = [0x42; 32];

    fn approval_page_style_src() -> String {
        let digest = Sha256::digest(approval_page::APPROVAL_PAGE_STYLES.as_bytes());
        format!("style-src 'sha256-{}'", STANDARD.encode(digest))
    }

    struct FakeResolver {
        result: Result<OAuthClientRegistration, ClientMetadataError>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClientMetadataResolver for FakeResolver {
        async fn resolve(
            &self,
            _client_id: &str,
        ) -> Result<OAuthClientRegistration, ClientMetadataError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.result.clone()
        }
    }

    fn fake_resolver(
        result: Result<OAuthClientRegistration, ClientMetadataError>,
    ) -> (Arc<dyn ClientMetadataResolver>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(FakeResolver {
                result,
                calls: calls.clone(),
            }),
            calls,
        )
    }

    fn registration(redirect_uris: &[&str]) -> OAuthClientRegistration {
        OAuthClientRegistration {
            redirect_uris: redirect_uris.iter().map(ToString::to_string).collect(),
            client_name: "Test Client".into(),
        }
    }

    fn state_with_resolver(
        database: &TestDatabase,
        issuer: &str,
        resolver: Arc<dyn ClientMetadataResolver>,
    ) -> AuthorizationServerHttpState {
        state_with(database, issuer, "", resolver)
    }

    /// The authorize route's state over `database` for `issuer`, with `extra`
    /// settings and client metadata resolved by `resolver`.
    fn state_with(
        database: &TestDatabase,
        issuer: &str,
        extra: &str,
        resolver: Arc<dyn ClientMetadataResolver>,
    ) -> AuthorizationServerHttpState {
        let mut state = http_state(database, issuer, extra, &[RESOURCE]);
        state.client_metadata_resolver = resolver;
        state
    }

    /// The sign-in a browser carries after following `response`, opened as
    /// the callback would open it.
    fn returned_session(
        state: &AuthorizationServerHttpState,
        response: &Response,
        oidc_state: &str,
    ) -> SignInSession {
        let mut cookies = HeaderMap::new();
        for set_cookie in response.headers().get_all(header::SET_COOKIE) {
            let pair = set_cookie.to_str().expect("ASCII").split(';').next();
            cookies.append(
                header::COOKIE,
                HeaderValue::from_str(pair.expect("name and value")).expect("cookie"),
            );
        }
        let (_name, sealed) = sign_in_cookie::find(&cookies, oidc_state, state.secure_cookies())
            .expect("sign-in cookie");
        let now = sign_in_cookie::now_unix_secs().expect("clock");
        SignInSession::open(state.settings.handshake_keys(), oidc_state, &sealed, now)
            .expect("sealed sign-in")
    }

    /// The approval a consent page's ticket carries, opened as a submission
    /// from this browser would open it.
    fn opened_approval(ticket: &str) -> PendingApproval {
        let (settings, _session_tokens) = resolved_settings("https://provider.invalid", "");
        let now = sign_in_cookie::now_unix_secs().expect("clock");
        PendingApproval::open(
            settings.handshake_keys(),
            &BROWSER_BINDING,
            ticket.as_bytes(),
            now,
        )
        .expect("sealed approval")
    }

    fn sets_approval_cookie(response: &Response) -> bool {
        response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .any(|cookie| {
                cookie
                    .as_bytes()
                    .starts_with(b"__Host-coral_oauth_approval=")
            })
    }

    fn trusting(client_id: &str) -> String {
        format!("[auth]\ntrusted_clients = ['{client_id}']")
    }

    fn state(database: &TestDatabase, issuer: &str) -> AuthorizationServerHttpState {
        let (resolver, _calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
        state_with_resolver(database, issuer, resolver)
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
        oauth_authorize_get(
            State(state),
            RawQuery(Some(encode(pairs))),
            browser_headers(),
        )
        .await
    }

    fn browser_cookie() -> String {
        format!(
            "__Host-coral_oauth_approval={}",
            URL_SAFE_NO_PAD.encode(BROWSER_BINDING)
        )
    }

    fn browser_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            browser_cookie().parse().expect("browser cookie"),
        );
        headers
    }

    fn browser_binding() -> approval_page::BrowserBinding {
        approval_page::BrowserBinding::from_bytes(BROWSER_BINDING)
    }

    async fn response_body(response: Response) -> String {
        String::from_utf8(
            to_bytes(
                response.into_body(),
                16 * 1024 + approval_page::APPROVAL_PAGE_STYLES.len(),
            )
            .await
            .expect("body")
            .to_vec(),
        )
        .expect("UTF-8 body")
    }

    fn ticket_from_page(page: &str) -> String {
        page.split("name=\"ticket\" value=\"")
            .nth(1)
            .and_then(|tail| tail.split('"').next())
            .expect("approval ticket")
            .to_string()
    }

    fn set_cookie_pair(response: &Response) -> String {
        response.headers()[header::SET_COOKIE]
            .to_str()
            .expect("approval cookie")
            .split(';')
            .next()
            .expect("cookie pair")
            .to_string()
    }

    fn submission_request_with_cookie(
        ticket: &str,
        decision: &str,
        cookie: Option<&str>,
    ) -> Request {
        let body = form_urlencoded::Serializer::new(String::new())
            .extend_pairs([("ticket", ticket), ("decision", decision)])
            .finish();
        let mut request = Request::builder()
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::ORIGIN, AUTH_ISSUER);
        if let Some(cookie) = cookie {
            request = request.header(header::COOKIE, cookie);
        }
        request.body(Body::from(body)).expect("request")
    }

    fn submission_request(ticket: &str, decision: &str) -> Request {
        let cookie = browser_cookie();
        submission_request_with_cookie(ticket, decision, Some(&cookie))
    }

    async fn submit(state: AuthorizationServerHttpState, ticket: &str, decision: &str) -> Response {
        oauth_authorize_post(State(state), submission_request(ticket, decision)).await
    }

    async fn approval_ticket(
        state: AuthorizationServerHttpState,
        pairs: &[(String, String)],
    ) -> String {
        let response = request(state, pairs).await;
        assert_eq!(response.status(), StatusCode::OK);
        ticket_from_page(&response_body(response).await)
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

    /// Asserts the headers every response from this handler carries.
    ///
    /// The direct-error path is asserted as well as the redirect path: it is
    /// the one that shipped without `referrer-policy` while its sibling
    /// handler set it, and an unasserted header is how that went unnoticed.
    fn assert_security(response: &Response) {
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(response.headers()[header::PRAGMA], "no-cache");
        assert_eq!(response.headers()[header::REFERRER_POLICY], "no-referrer");
    }

    #[test]
    fn query_parser_bounds_and_rejects_decoded_duplicates() {
        let encoded = encode(&pairs());
        let mut cases = vec![format!("{encoded}&client%5Fid=duplicate")];
        cases.extend(query::rejected_queries());
        for raw in cases {
            let Err(()) = parse_query(&raw) else {
                panic!("accepted malformed authorization query: {raw}");
            };
        }
    }

    #[tokio::test]
    async fn confirmation_is_secure_and_stores_only_validated_data() {
        let database = database().await;
        let expected_style_src = approval_page_style_src();
        let expected_csp = format!(
            "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; {expected_style_src}"
        );
        let mut request_pairs = pairs();
        request_pairs.push(("approved".into(), "true".into()));
        request_pairs.push(("decision".into(), "continue".into()));
        let response = request(state(&database, "https://provider.invalid"), &request_pairs).await;
        assert_eq!(response.status(), StatusCode::OK);
        for (name, value) in [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
            // No `form-action`: see `approval_page_does_not_restrict_form_action`.
            (header::CONTENT_SECURITY_POLICY, expected_csp.as_str()),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            // Not `no-referrer`: that policy makes Chromium submit this page's
            // own form with `Origin: null`, which the exact-origin check on the
            // submission can never accept. See
            // `approval_page_referrer_policy_preserves_the_submission_origin`.
            (header::REFERRER_POLICY, "same-origin"),
            (header::X_FRAME_OPTIONS, "DENY"),
        ] {
            assert_eq!(response.headers()[name], value);
        }
        let set_cookie = response.headers()[header::SET_COOKIE]
            .to_str()
            .expect("approval cookie")
            .to_string();
        let page = response_body(response).await;
        assert!(page.contains("Requested by</dt>"));
        assert!(page.contains(">client.example.test</dd>"));
        assert!(page.contains("Returns to</dt>"));
        assert!(page.contains(">client.example.test:443</dd>"));
        let form = page
            .split("<form ")
            .nth(1)
            .and_then(|tail| tail.split("</form>").next())
            .expect("approval form");
        let attributes = form.split('>').next().expect("form attributes");
        assert!(attributes.contains("method=\"post\""));
        assert!(attributes.contains("action=\"/oauth/authorize\""));
        // Inspect the form, not matching attribute selectors in the stylesheet.
        let cancel = form.find("value=\"cancel\"").expect("Cancel button");
        let proceed = form.find("value=\"continue\"").expect("Allow button");
        assert!(
            cancel < proceed,
            "Cancel must be the form's first submit button, so a bare Enter key \
             does not approve access"
        );
        assert!(!page.contains(CLIENT_STATE));
        assert!(!page.contains(CHALLENGE));
        assert!(!page.contains("<script"));
        assert!(!page.contains(" style="));
        assert!(!page.contains("@@"));
        let styles = page
            .split_once("<style>")
            .and_then(|(_, rest)| rest.split_once("</style>"))
            .expect("one embedded stylesheet")
            .0;
        assert_eq!(styles, approval_page::APPROVAL_PAGE_STYLES);
        let ticket = ticket_from_page(&page);
        assert_eq!(
            set_cookie,
            format!(
                "{}; Path=/; Max-Age=300; HttpOnly; Secure; SameSite=Lax",
                browser_cookie()
            )
        );
        assert!(!set_cookie.contains(&ticket));
        let approval = opened_approval(&ticket);
        assert_eq!(approval.request.client_id, CLIENT_ID);
        assert_eq!(approval.client_name, "Test Client");
        assert_eq!(approval.request.redirect_uri, REDIRECT_URI);
        assert_eq!(approval.request.client_state.as_deref(), Some(CLIENT_STATE));
        assert_eq!(approval.request.code_challenge, CHALLENGE);
        assert_eq!(approval.request.resource, RESOURCE);
    }

    /// RFC 8252 §7.1: a native app such as Raycast names its own URI scheme as
    /// the callback. The page shows that scheme, since there is no host.
    #[tokio::test]
    async fn app_scheme_callback_reaches_the_approval_page() {
        const APP_CALLBACK: &str = "com.raycast:/oauth";
        let database = database().await;
        let (resolver, _calls) = fake_resolver(Ok(registration(&[APP_CALLBACK])));
        let mut request_pairs = pairs();
        replace(&mut request_pairs, "redirect_uri", Some(APP_CALLBACK));
        let response = request(
            state_with_resolver(&database, "https://provider.invalid", resolver),
            &request_pairs,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let page = response_body(response).await;
        assert!(page.contains("Opens app</dt>"));
        assert!(page.contains(">com.raycast</dd>"));
        let approval = opened_approval(&ticket_from_page(&page));
        assert_eq!(approval.request.redirect_uri, APP_CALLBACK);
    }

    #[tokio::test]
    async fn confirmation_page_displays_loopback_destination_and_escapes_client_text() {
        let loopback = approval_page::response(
            "sealed-approval",
            "Local Test Client",
            CLIENT_ID,
            &EndpointUrl::<BrowserRedirect>::parse("http://127.0.0.1:14554/oauth/callback")
                .expect("loopback redirect"),
            &approval_page::ApprovalScope::Workspace,
            &browser_binding(),
            false,
        )
        .expect("page");
        let loopback_cookie = loopback.headers()[header::SET_COOKIE]
            .to_str()
            .expect("loopback approval cookie")
            .to_string();
        let page = response_body(loopback).await;
        assert!(page.contains("<bdi>Local Test Client</bdi>"));
        assert!(page.contains("127.0.0.1:14554"));
        assert!(!page.contains("role=\"alert\""));
        assert_eq!(
            loopback_cookie,
            format!(
                "coral_oauth_approval={}; Path=/; Max-Age=300; HttpOnly; SameSite=Lax",
                URL_SAFE_NO_PAD.encode(BROWSER_BINDING)
            )
        );
        assert!(!loopback_cookie.contains("; Secure"));

        let escaped = approval_page::response(
            "sealed-approval",
            "</bdi><script>alert(\"x\")</script>&' @@ticket@@ @@styles@@",
            CLIENT_ID,
            &EndpointUrl::<BrowserRedirect>::parse(REDIRECT_URI).expect("redirect"),
            &approval_page::ApprovalScope::Workspace,
            &browser_binding(),
            true,
        )
        .expect("page");
        let escaped = response_body(escaped).await;
        assert!(!escaped.contains("</bdi><script>"));
        assert!(escaped.contains("&lt;/bdi&gt;&lt;script&gt;alert(&quot;x&quot;)"));
        assert!(escaped.contains("@@ticket@@ @@styles@@</bdi>"));
        assert_eq!(escaped.matches("<style>").count(), 1);
    }

    /// Every request here carries the `Origin` and `Cookie` a browser sends,
    /// because `parse_submission` checks those before it looks at the body: a
    /// case that omits them is rejected at the origin gate and proves nothing
    /// about the parser it names. The two accepting cases at the end exist for
    /// the same reason — they fail if a future guard rejects everything, which
    /// is how a set of rejection cases goes quietly vacuous.
    #[tokio::test]
    async fn submission_form_is_strict_bounded_and_ignores_no_query_bypass() {
        const FORM: &str = "application/x-www-form-urlencoded";

        let ticket = URL_SAFE_NO_PAD.encode([7; 32]);
        let submission = |body: String, cookie: HeaderValue, content_type: &'static str| {
            Request::builder()
                .uri(format!(
                    "/oauth/authorize?ticket={ticket}&decision=continue"
                ))
                .header(header::CONTENT_TYPE, content_type)
                .header(header::ORIGIN, AUTH_ISSUER)
                .header(header::COOKIE, cookie)
                .body(Body::from(body))
                .expect("request")
        };
        let cookie = || HeaderValue::from_str(&browser_cookie()).expect("cookie");
        let malformed = [
            format!("ticket={ticket}"),
            format!("ticket={ticket}&decision=continue&extra=x"),
            format!("ticket={ticket}&ticket={ticket}&decision=continue"),
            format!("ticket={ticket}&decision=approve"),
            format!("ticket={ticket}=&decision=continue"),
            "x".repeat(16 * 1024 + 1),
        ];
        for body in malformed {
            let request = submission(body, cookie(), FORM);
            let Err(()) = approval_page::parse_submission(request, AUTH_ISSUER, true).await else {
                panic!("accepted malformed approval submission");
            };
        }
        let request = submission(
            format!("ticket={ticket}&decision=continue"),
            cookie(),
            "text/plain",
        );
        let Err(()) = approval_page::parse_submission(request, AUTH_ISSUER, true).await else {
            panic!("accepted approval submission with the wrong content type");
        };

        // Every request `submission` builds says `decision=continue` in its
        // query, so a body that parses as `Cancel` is what shows the query
        // never reaches the decision. The second header is what a non-ASCII
        // cookie set by anything else on this host would produce; that stray
        // pair must not hide the binding beside it.
        let stray = format!("stray=caf\u{e9}; {}", browser_cookie());
        for cookie in [
            cookie(),
            HeaderValue::from_bytes(stray.as_bytes()).expect("cookie"),
        ] {
            let request = submission(format!("ticket={ticket}&decision=cancel"), cookie, FORM);
            let approval_page::Submission {
                ticket,
                browser_binding,
                decision,
                workspace: None,
            } = approval_page::parse_submission(request, AUTH_ISSUER, true)
                .await
                .expect("rejected a well-formed approval submission")
            else {
                panic!("an approval submission names no Workspace");
            };
            assert_eq!(ticket.as_str(), URL_SAFE_NO_PAD.encode([7; 32]));
            assert_eq!(browser_binding.as_bytes(), &BROWSER_BINDING);
            assert!(decision == ApprovalDecision::Cancel);
        }
    }

    #[tokio::test]
    async fn submission_requires_same_origin_browser_binding_without_consuming_the_approval() {
        let database = database().await;
        let auth_state = state(&database, "https://provider.invalid");
        let ticket = approval_ticket(auth_state.clone(), &pairs()).await;

        let missing_cookie = oauth_authorize_post(
            State(auth_state.clone()),
            submission_request_with_cookie(&ticket, "cancel", None),
        )
        .await;
        assert_eq!(missing_cookie.status(), StatusCode::BAD_REQUEST);

        let wrong_cookie = format!(
            "__Host-coral_oauth_approval={}",
            URL_SAFE_NO_PAD.encode([0x43; 32])
        );
        let mismatched_cookie = oauth_authorize_post(
            State(auth_state.clone()),
            submission_request_with_cookie(&ticket, "cancel", Some(&wrong_cookie)),
        )
        .await;
        assert_eq!(mismatched_cookie.status(), StatusCode::BAD_REQUEST);

        let mut cross_origin = submission_request(&ticket, "cancel");
        cross_origin
            .headers_mut()
            .insert(header::ORIGIN, "http://127.0.0.1:65535".parse().unwrap());
        let cross_origin = oauth_authorize_post(State(auth_state.clone()), cross_origin).await;
        assert_eq!(cross_origin.status(), StatusCode::BAD_REQUEST);

        let mut missing_origin = submission_request(&ticket, "cancel");
        missing_origin.headers_mut().remove(header::ORIGIN);
        let missing_origin = oauth_authorize_post(State(auth_state.clone()), missing_origin).await;
        assert_eq!(missing_origin.status(), StatusCode::BAD_REQUEST);

        let bound_browser = submit(auth_state, &ticket, "cancel").await;
        assert_eq!(bound_browser.status(), StatusCode::FOUND);
        assert!(
            location(&bound_browser).is_some_and(|location| location.contains("access_denied"))
        );
    }

    /// The approval page must not restrict `form-action`.
    ///
    /// Submitting the form starts a sign-in that navigates through this server,
    /// the provider, any hop the provider adds, this server's callback, and
    /// finally the client's redirect URI. Browsers apply `form-action` to every
    /// one of them, so any list short of all of them blocks the login, leaving
    /// the user on a page where nothing visibly happened.
    /// A real sign-in through a hosted provider crossed five origins this way.
    /// The remaining directives still deny every fetch, base URI and framing.
    #[tokio::test]
    async fn approval_page_does_not_restrict_form_action() {
        let database = database().await;
        let auth_state = state(&database, "https://provider.invalid");
        let page = request(auth_state, &pairs()).await;
        assert_eq!(page.status(), StatusCode::OK);
        let policy = page.headers()[header::CONTENT_SECURITY_POLICY]
            .to_str()
            .expect("policy");
        assert!(
            !policy.contains("form-action"),
            "form-action cannot enumerate an OAuth redirect chain, got: {policy}"
        );
        assert!(policy.contains("default-src 'none'"));
        assert!(policy.contains("base-uri 'none'"));
        assert!(policy.contains("frame-ancestors 'none'"));
        assert!(policy.contains(&approval_page_style_src()));
    }

    /// The approval page must not carry `no-referrer`.
    ///
    /// Its own form posts back to this handler, and that submission is checked
    /// against an exact `Origin`. Chromium derives a form submission's `Origin`
    /// from the document's referrer policy, so a page served with `no-referrer`
    /// submits `Origin: null` and every approval fails — the whole browser
    /// login flow, for every client. Asserting the header's presence is what
    /// missed this: both halves were tested, their interaction was not. The
    /// `Origin: null` case below is the submission this policy would produce.
    #[tokio::test]
    async fn approval_page_referrer_policy_preserves_the_submission_origin() {
        let database = database().await;
        let auth_state = state(&database, "https://provider.invalid");
        let page = request(auth_state.clone(), &pairs()).await;
        assert_eq!(page.status(), StatusCode::OK);
        assert_eq!(page.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(page.headers()[header::PRAGMA], "no-cache");
        assert_eq!(page.headers()[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
        assert_eq!(page.headers()[header::REFERRER_POLICY], "same-origin");

        let ticket = ticket_from_page(&response_body(page).await);
        let mut null_origin = submission_request(&ticket, "cancel");
        null_origin
            .headers_mut()
            .insert(header::ORIGIN, "null".parse().unwrap());
        let null_origin = oauth_authorize_post(State(auth_state.clone()), null_origin).await;
        assert_eq!(null_origin.status(), StatusCode::BAD_REQUEST);

        let bound_browser = submit(auth_state, &ticket, "cancel").await;
        assert_eq!(bound_browser.status(), StatusCode::FOUND);
    }

    #[tokio::test]
    async fn concurrent_consent_pages_reuse_the_binding_and_remain_valid() {
        let database = database().await;
        let auth_state = state(&database, "https://provider.invalid");
        let first = oauth_authorize_get(
            State(auth_state.clone()),
            RawQuery(Some(encode(&pairs()))),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let cookie = set_cookie_pair(&first);
        let first_ticket = ticket_from_page(&response_body(first).await);

        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, cookie.parse().expect("browser cookie"));
        let second = oauth_authorize_get(
            State(auth_state.clone()),
            RawQuery(Some(encode(&pairs()))),
            headers,
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(set_cookie_pair(&second), cookie);
        let second_ticket = ticket_from_page(&response_body(second).await);
        assert_ne!(first_ticket, second_ticket);

        for ticket in [first_ticket, second_ticket] {
            let response = oauth_authorize_post(
                State(auth_state.clone()),
                submission_request_with_cookie(&ticket, "cancel", Some(&cookie)),
            )
            .await;
            assert_eq!(response.status(), StatusCode::FOUND);
        }
    }

    #[tokio::test]
    async fn cimd_client_uses_resolved_exact_redirect_once() {
        let database = database().await;
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let (resolver, calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
        let request_pairs = pairs();
        let auth_state = state_with_resolver(&database, &provider.uri(), resolver);
        let response = request(auth_state.clone(), &request_pairs).await;
        assert_eq!(response.status(), StatusCode::OK);
        let page = response_body(response).await;
        assert!(page.contains("<bdi>Test Client</bdi>"));
        assert!(page.contains(">client.example.test</dd>"));
        let ticket = ticket_from_page(&page);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(
            provider
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );

        let response = submit(auth_state, &ticket, "continue").await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert!(location(&response).is_some_and(|url| url.starts_with(&provider.uri())));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cimd_undeclared_redirect_is_never_trusted() {
        let database = database().await;
        let (resolver, calls) = fake_resolver(Ok(registration(&[
            "https://client.example.test/other-callback",
        ])));
        let response = request(
            state_with_resolver(&database, "https://provider.invalid", resolver),
            &pairs(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(location(&response).is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn resolver_failures_are_generic_direct_errors() {
        let database = database().await;
        let (resolver, calls) = fake_resolver(Err(ClientMetadataError::HttpStatus));
        let client_id = format!("{CLIENT_ID}?secret=attacker-url-secret");
        let mut request_pairs = pairs();
        replace(&mut request_pairs, "client_id", Some(&client_id));
        let response = request(
            state_with_resolver(&database, "https://provider.invalid", resolver),
            &request_pairs,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(location(&response).is_none());
        let body = to_bytes(response.into_body(), 4096).await.expect("body");
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("invalid_request"));
        assert!(!body.contains("attacker-url-secret"));
        assert!(!body.contains("HttpStatus"));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn client_and_redirect_must_match_exactly_before_redirecting() {
        let database = database().await;
        let (resolver, _calls) = fake_resolver(Err(ClientMetadataError::InvalidMetadata));
        let mut wrong_client = pairs();
        replace(
            &mut wrong_client,
            "client_id",
            Some("https://client.example.test/oauth/%63lient.json"),
        );
        let response = request(
            state_with_resolver(&database, "https://provider.invalid", resolver),
            &wrong_client,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_security(&response);
        assert!(location(&response).is_none());

        let mut wrong_redirect = pairs();
        replace(
            &mut wrong_redirect,
            "redirect_uri",
            Some("https://client.example.test/other"),
        );
        let response = request(
            state(&database, "https://provider.invalid"),
            &wrong_redirect,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_security(&response);
        assert!(location(&response).is_none());

        let raw = format!("{}&client%5Fid=raw-secret", encode(&pairs()));
        let response = oauth_authorize_get(
            State(state(&database, "https://provider.invalid")),
            RawQuery(Some(raw)),
            browser_headers(),
        )
        .await;
        assert!(location(&response).is_none());
        let body = to_bytes(response.into_body(), 4096).await.expect("body");
        assert!(!String::from_utf8_lossy(&body).contains("raw-secret"));
    }

    #[tokio::test]
    async fn registered_redirect_outside_browser_policy_never_becomes_a_redirect() {
        let database = database().await;
        for uri in [
            "javascript:alert(1)",
            "data:text/html,<script>alert(1)</script>",
            "http://client.example.test/callback",
        ] {
            let (resolver, _calls) = fake_resolver(Ok(registration(&[uri])));
            let auth_state = state_with_resolver(&database, "https://provider.invalid", resolver);
            let mut request_pairs = pairs();
            replace(&mut request_pairs, "redirect_uri", Some(uri));
            let response = request(auth_state, &request_pairs).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "accepted {uri}");
            assert_security(&response);
            assert!(location(&response).is_none(), "redirected to {uri}");
        }
    }

    /// RFC 8252 §7.3: a native client takes an ephemeral port from the
    /// operating system as it starts the flow, so the port cannot be in the
    /// metadata document it published earlier. The requested port is also the
    /// one the client is listening on, so it is the resolved callback.
    #[test]
    fn loopback_callbacks_match_whatever_port_the_request_names() {
        for (registered, requested) in [
            (
                "http://127.0.0.1/callback",
                "http://127.0.0.1:3118/callback",
            ),
            (
                "http://localhost/callback",
                "http://localhost:51234/callback",
            ),
            // Ephemeral ports vary between runs, so a registration that named
            // one still has to accept the next.
            (
                "http://127.0.0.1:3118/callback",
                "http://127.0.0.1:51234/callback",
            ),
            ("http://[::1]/callback", "http://[::1]:3118/callback"),
            // A loopback address outside 127.0.0.1, and a registration whose
            // query the request repeats unchanged.
            (
                "http://127.42.0.1/callback?tenant=one",
                "http://127.42.0.1:3118/callback?tenant=one",
            ),
            // The client may equally drop a registered port.
            (
                "http://localhost:3118/callback",
                "http://localhost/callback",
            ),
        ] {
            let resolved = registered_redirect(&[registered.to_string()], requested);
            assert_eq!(
                resolved.as_ref().map(|url| url.as_url().as_str()),
                Some(requested),
                "registered {registered}"
            );
        }
    }

    /// The port is the only thing §7.3 relaxes, and only for the callback shape
    /// that can name nothing but the user's own machine. Everything here would
    /// otherwise be an open redirect or a callback handed to the wrong listener.
    #[test]
    fn nothing_but_a_loopback_port_is_relaxed() {
        for (registered, requested) in [
            // A public host never gains a port it did not register.
            (
                "https://app.example.com/cb",
                "https://app.example.com:8443/cb",
            ),
            // Loopback over HTTPS is not the native-client shape §7.3 describes.
            (
                "https://127.0.0.1/callback",
                "https://127.0.0.1:3118/callback",
            ),
            // Neither direction crosses schemes.
            (
                "http://127.0.0.1/callback",
                "https://127.0.0.1:3118/callback",
            ),
            (
                "https://127.0.0.1/callback",
                "http://127.0.0.1:3118/callback",
            ),
            // A host that merely reads as loopback is a public host.
            (
                "https://127.0.0.1.evil.com/callback",
                "https://127.0.0.1.evil.com:3118/callback",
            ),
            (
                "https://localhost.evil.com/callback",
                "https://localhost.evil.com:3118/callback",
            ),
            // Plain HTTP on those hosts does not even register.
            (
                "http://127.0.0.1.evil.com/callback",
                "http://127.0.0.1.evil.com:3118/callback",
            ),
            // A registered public host is not a loopback listener.
            ("https://app.example.com/cb", "http://127.0.0.1:3118/cb"),
            // Loopback spellings are distinct hosts, not aliases.
            (
                "http://127.0.0.1/callback",
                "http://localhost:3118/callback",
            ),
            (
                "http://127.0.0.1/callback",
                "http://127.0.0.2:3118/callback",
            ),
            // Path and query still match exactly.
            ("http://127.0.0.1/callback", "http://127.0.0.1:3118/other"),
            (
                "http://127.0.0.1/callback?tenant=one",
                "http://127.0.0.1:3118/callback?tenant=two",
            ),
            // The request is held to the redirect policy in its own right.
            (
                "http://127.0.0.1/callback",
                "http://user:password@127.0.0.1:3118/callback",
            ),
            (
                "http://127.0.0.1/callback",
                "http://127.0.0.1:3118/callback#fragment",
            ),
        ] {
            assert_eq!(
                registered_redirect(&[registered.to_string()], requested),
                None,
                "registered {registered} accepted {requested}"
            );
        }
    }

    /// A browser client, or a native app on its own URI scheme, registers the
    /// callback it will actually be sent to, and keeps matching byte for byte.
    #[test]
    fn non_loopback_registrations_still_match_exactly() {
        for (registered, mismatches) in [
            (
                REDIRECT_URI,
                &[
                    "https://client.example.test/callback",
                    "https://client.example.test/callback?tenant=two",
                    "https://client.example.test:8443/callback?tenant=one",
                ][..],
            ),
            (
                "com.raycast:/oauth",
                &[
                    "com.raycast:/other",
                    "com.raycast:/oauth?tenant=one",
                    "com.raycast-x:/oauth",
                    "com.raycast://oauth",
                ][..],
            ),
        ] {
            let registrations = [registered.to_string()];
            assert_eq!(
                registered_redirect(&registrations, registered)
                    .as_ref()
                    .map(|url| url.as_url().as_str()),
                Some(registered)
            );
            for requested in mismatches {
                assert_eq!(
                    registered_redirect(&registrations, requested),
                    None,
                    "{requested}"
                );
            }
        }
    }

    /// The port a native client names is the one it is listening on, so it has
    /// to reach the consent page and the `Location` the browser follows rather
    /// than the portless URI the metadata document registered.
    #[tokio::test]
    async fn loopback_client_returns_to_the_port_it_requested() {
        let database = database().await;
        let requested = "http://127.0.0.1:3118/callback";
        let (resolver, _calls) = fake_resolver(Ok(registration(&["http://127.0.0.1/callback"])));
        let auth_state = state_with_resolver(&database, "https://provider.invalid", resolver);
        let mut request_pairs = pairs();
        replace(&mut request_pairs, "redirect_uri", Some(requested));
        let response = request(auth_state.clone(), &request_pairs).await;
        assert_eq!(response.status(), StatusCode::OK);
        let page = response_body(response).await;
        assert!(page.contains(">127.0.0.1:3118</dd>"));
        let ticket = ticket_from_page(&page);

        let response = submit(auth_state, &ticket, "cancel").await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_security(&response);
        let location = location(&response).expect("cancel redirect");
        assert!(
            location.starts_with(&format!("{requested}?error=access_denied")),
            "redirected to {location}"
        );
    }

    #[tokio::test]
    async fn trusted_protocol_errors_redirect_with_one_encoded_client_state() {
        let database = database().await;
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
            let response =
                request(state(&database, "https://provider.invalid"), &request_pairs).await;
            assert_eq!(response.status(), StatusCode::FOUND);
            assert_security(&response);
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
        let database = database().await;
        let mut auth_state = state(&database, "https://provider.invalid");
        auth_state.authorization_resources = Arc::new(BTreeSet::new());
        let response = request(auth_state, &pairs()).await;
        let location = location(&response).expect("trusted error redirect");
        assert!(location.contains("error=invalid_target"));
    }

    /// A consent page holds no server state, so its submission may reach any
    /// process. Submitting it twice only repeats the person's own decision in
    /// the browser the page was shown to.
    #[tokio::test]
    async fn consent_is_honored_by_any_process_for_the_browser_it_was_shown_to() {
        let database = database().await;
        let ticket = approval_ticket(state(&database, "https://provider.invalid"), &pairs()).await;
        for process in ["another process", "the same page again"] {
            let cancelled = submit(
                state(&database, "https://provider.invalid"),
                &ticket,
                "cancel",
            )
            .await;
            let callback = Url::parse(location(&cancelled).expect("client redirect")).expect("URL");
            let query = callback.query_pairs().into_owned().collect::<Vec<_>>();
            assert!(
                query.contains(&("tenant".into(), "one".into())),
                "{process}"
            );
            assert!(
                query.contains(&("error".into(), "access_denied".into())),
                "{process}"
            );
            assert!(
                query.contains(&("state".into(), CLIENT_STATE.into())),
                "{process}"
            );
        }
        let middle = ticket.len() / 2;
        let flipped = if ticket.get(middle..=middle) == Some("A") {
            "B"
        } else {
            "A"
        };
        let altered = format!(
            "{}{flipped}{}",
            ticket.get(..middle).expect("head"),
            ticket.get(middle + 1..).expect("tail")
        );
        for forged in [altered, URL_SAFE_NO_PAD.encode([7; 32])] {
            let response = submit(
                state(&database, "https://provider.invalid"),
                &forged,
                "continue",
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(location(&response).is_none());
            let body = response_body(response).await;
            assert!(body.contains("authorization approval is invalid or expired"));
            assert!(!body.contains(REDIRECT_URI));
        }
    }

    /// Each Allow starts its own sign-in in its own cookie, so a double
    /// submission leaves two independent sign-ins rather than a race.
    #[tokio::test]
    async fn each_allow_starts_an_independent_sign_in() {
        let database = database().await;
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let auth_state = state(&database, &provider.uri());
        let ticket = approval_ticket(auth_state.clone(), &pairs()).await;
        let (first, second) = tokio::join!(
            submit(auth_state.clone(), &ticket, "continue"),
            submit(auth_state.clone(), &ticket, "continue")
        );
        let oidc_state = |response: &Response| {
            Url::parse(location(response).expect("provider redirect"))
                .expect("URL")
                .query_pairs()
                .find(|(key, _value)| key == "state")
                .expect("OIDC state")
                .1
                .into_owned()
        };
        let (first_state, second_state) = (oidc_state(&first), oidc_state(&second));
        assert_ne!(first_state, second_state);
        assert_eq!(
            returned_session(&auth_state, &first, &first_state)
                .request
                .client_id,
            CLIENT_ID
        );
        assert_eq!(
            returned_session(&auth_state, &second, &second_state)
                .request
                .client_id,
            CLIENT_ID
        );
    }

    #[tokio::test]
    async fn continue_hands_the_browser_a_sealed_sign_in_with_the_provider_redirect() {
        let database = database().await;
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let auth_state = state(&database, &provider.uri());
        let ticket = approval_ticket(auth_state.clone(), &pairs()).await;
        assert!(
            provider
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
        let response = submit(auth_state.clone(), &ticket, "continue").await;
        let provider_url = location(&response).expect("provider redirect");
        let query = Url::parse(provider_url)
            .expect("URL")
            .query_pairs()
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        let oidc_state = query.get("state").expect("OIDC state");
        let set_cookie = response.headers()[header::SET_COOKIE]
            .to_str()
            .expect("ASCII");
        for attribute in [
            "Path=/",
            "Max-Age=300",
            "HttpOnly",
            "Secure",
            "SameSite=Lax",
        ] {
            assert!(
                set_cookie.contains(attribute),
                "{attribute} in {set_cookie}"
            );
        }
        let session = returned_session(&auth_state, &response, oidc_state);
        assert_eq!(session.request.client_id, CLIENT_ID);
        assert_eq!(session.request.redirect_uri, REDIRECT_URI);
        assert_eq!(session.request.client_state.as_deref(), Some(CLIENT_STATE));
        assert_eq!(session.request.code_challenge, CHALLENGE);
        assert_eq!(session.request.resource, RESOURCE);
        let keys = auth_state.settings.handshake_keys();
        let verifier = keys.pkce_verifier(oidc_state);
        assert_eq!(
            query.get("code_challenge"),
            Some(&URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())))
        );
        assert_eq!(query.get("nonce"), Some(&keys.nonce(oidc_state)));
        assert!(!query.values().any(|value| value == CLIENT_STATE));
        assert!(!set_cookie.contains(CLIENT_STATE));
        for secret in [verifier.as_str(), PROVIDER_SECRET] {
            assert!(!provider_url.contains(secret));
            assert!(!set_cookie.contains(secret));
        }
    }

    /// The sign-in cookie must fit in one browser cookie, so a request that
    /// cannot be carried is refused before consent and holds nothing. The
    /// second case stays inside the app budget but names a long configured
    /// resource, which the cookie carries too.
    #[tokio::test]
    async fn requests_a_sign_in_cookie_cannot_carry_are_refused_before_consent() {
        let database = database().await;
        let long_resource = format!("https://api.example.test/{}", "r".repeat(2000));
        let (resolver, _calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
        let mut long_resource_state = http_state(
            &database,
            "https://provider.invalid",
            "",
            &[long_resource.as_str()],
        );
        long_resource_state.client_metadata_resolver = resolver;
        let mut oversized_app = pairs();
        replace(&mut oversized_app, "state", Some(&"s".repeat(1536)));
        let mut long_resource_pairs = pairs();
        replace(&mut long_resource_pairs, "state", Some(&"s".repeat(1000)));
        replace(&mut long_resource_pairs, "resource", Some(&long_resource));
        for (case, auth_state, request_pairs) in [
            (
                "app inputs over budget",
                state(&database, "https://provider.invalid"),
                oversized_app,
            ),
            ("a long resource", long_resource_state, long_resource_pairs),
        ] {
            let response = request(auth_state, &request_pairs).await;
            let error =
                Url::parse(location(&response).expect("client error redirect")).expect("URL");
            let query = error.query_pairs().into_owned().collect::<Vec<_>>();
            assert!(
                query.contains(&("error".into(), "invalid_request".into())),
                "{case}"
            );
            assert!(
                response.headers().get(header::SET_COOKIE).is_none(),
                "{case}"
            );
        }
    }

    /// The response that starts a sign-in expires whatever the per-browser cap
    /// evicts. Which cookies those are is `sign_in_cookie`'s rule.
    #[tokio::test]
    async fn a_new_sign_in_response_expires_what_the_cap_evicts() {
        let database = database().await;
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let (resolver, _calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
        let auth_state = state_with(&database, &provider.uri(), &trusting(CLIENT_ID), resolver);
        let mut headers = browser_headers();
        for started_at in [30, 10, 20] {
            let name = sign_in_cookie::name(&format!("held-{started_at}"), started_at, true);
            headers.append(
                header::COOKIE,
                HeaderValue::from_str(&format!("{name}=sealed")).expect("cookie"),
            );
        }
        let evicted = sign_in_cookie::evicted_by_new_sign_in(&headers, true);
        assert!(!evicted.is_empty(), "the setup must reach the cap");
        let response =
            oauth_authorize_get(State(auth_state), RawQuery(Some(encode(&pairs()))), headers).await;
        let set_cookies = response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .map(|value| value.to_str().expect("ASCII").to_owned())
            .collect::<Vec<_>>();
        let cleared = set_cookies
            .iter()
            .filter(|cookie| cookie.contains("Max-Age=0"))
            .filter_map(|cookie| cookie.split_once('=').map(|(name, _)| name.to_owned()))
            .collect::<Vec<_>>();
        assert_eq!(cleared, evicted);
        assert_eq!(
            set_cookies
                .iter()
                .filter(|cookie| cookie.contains("Max-Age=300"))
                .count(),
            1,
            "{set_cookies:?}"
        );
    }

    #[tokio::test]
    async fn web_configuration_skips_only_its_exact_client_with_pinned_callback_and_resource() {
        const WEB: &str = "https://web.example.test";
        const WEB_CLIENT: &str = "https://web.example.test/.well-known/oauth-client";
        const WEB_CALLBACK: &str = "https://web.example.test/auth/callback";
        let database = database().await;
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let build_state = |redirects: &[&str]| {
            let (resolver, _) = fake_resolver(Ok(registration(redirects)));
            let mut state = state_with(
                &database,
                &provider.uri(),
                "[listeners.web]\npublic_url = 'https://web.example.test'",
                resolver,
            );
            state.authorization_resources = Arc::new(BTreeSet::from([WEB.into(), RESOURCE.into()]));
            state
        };
        let mut web_pairs = pairs();
        replace(&mut web_pairs, "client_id", Some(WEB_CLIENT));
        replace(&mut web_pairs, "redirect_uri", Some(WEB_CALLBACK));
        replace(&mut web_pairs, "resource", Some(WEB));
        let response = request(build_state(&[WEB_CALLBACK]), &web_pairs).await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert!(
            location(&response)
                .expect("upstream sign-in")
                .starts_with(&provider.uri())
        );
        assert!(!sets_approval_cookie(&response));

        for client in [
            CLIENT_ID,
            "https://web.example.test/another-client",
            "https://auth.example.test/.well-known/oauth-client",
        ] {
            let mut untrusted = web_pairs.clone();
            replace(&mut untrusted, "client_id", Some(client));
            let response = request(build_state(&[WEB_CALLBACK]), &untrusted).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert!(response_body(response).await.contains("to Coral</h1>"));
        }
        // A metadata document that advertises another callback does not
        // extend the operator-designated web client's trust to it: see
        // `web_client_provider_setup_callback_is_confirmed_like_any_client`.
        for (field, value, error) in [
            ("resource", Some(RESOURCE), "invalid_target"),
            ("code_challenge", None, "invalid_request"),
        ] {
            let mut invalid = web_pairs.clone();
            replace(&mut invalid, field, value);
            let response = request(build_state(&[WEB_CALLBACK]), &invalid).await;
            let destination = location(&response).expect("safe web error redirect");
            assert!(destination.starts_with(WEB_CALLBACK));
            assert!(destination.contains(&format!("error={error}")));
        }
        let mut failed_metadata = build_state(&[WEB_CALLBACK]);
        failed_metadata.client_metadata_resolver = fake_resolver(Err(ClientMetadataError::Fetch)).0;
        assert_eq!(
            request(failed_metadata, &web_pairs).await.status(),
            StatusCode::BAD_REQUEST
        );
    }

    /// One workspace adding another's Coral MCP server on
    /// the same deployment makes Reef an OAuth client of its own authorization
    /// server. It presents the web client ID with its Provider Setup callback
    /// and the workspace's MCP resource. The login pins — one callback, the
    /// web resource, no consent — must not clamp that request: it is validated
    /// against the metadata document and confirmed like any other client's.
    #[tokio::test]
    async fn web_client_provider_setup_callback_is_confirmed_like_any_client() {
        const WEB: &str = "https://web.example.test";
        const WEB_CLIENT: &str = "https://web.example.test/.well-known/oauth-client";
        const WEB_CALLBACK: &str = "https://web.example.test/auth/callback";
        const SETUP_CALLBACK: &str = "https://web.example.test/provider-setup/oauth/callback";
        let database = database().await;
        let build_state = |redirects: &[&str]| {
            let (resolver, _) = fake_resolver(Ok(registration(redirects)));
            let mut state = state_with(
                &database,
                "https://provider.invalid",
                "[listeners.web]\npublic_url = 'https://web.example.test'",
                resolver,
            );
            state.authorization_resources = Arc::new(BTreeSet::from([WEB.into(), RESOURCE.into()]));
            state
        };
        let mut setup_pairs = pairs();
        replace(&mut setup_pairs, "client_id", Some(WEB_CLIENT));
        replace(&mut setup_pairs, "redirect_uri", Some(SETUP_CALLBACK));

        // The workspace MCP resource is accepted, and the user is asked.
        let response = request(build_state(&[WEB_CALLBACK, SETUP_CALLBACK]), &setup_pairs).await;
        assert_eq!(response.status(), StatusCode::OK);
        let page = response_body(response).await;
        assert!(page.contains("to Coral</h1>"));
        let approval = opened_approval(&ticket_from_page(&page));
        assert_eq!(approval.request.client_id, WEB_CLIENT);
        assert_eq!(approval.request.redirect_uri, SETUP_CALLBACK);
        assert_eq!(approval.request.resource, RESOURCE);

        // The web resource is still only one of the registered resources on
        // this path, never a reason to skip the approval page.
        let mut web_resource = setup_pairs.clone();
        replace(&mut web_resource, "resource", Some(WEB));
        let response = request(build_state(&[WEB_CALLBACK, SETUP_CALLBACK]), &web_resource).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response_body(response).await.contains("to Coral</h1>"));

        // Metadata still has to name the callback: the web identity gains no
        // callback its document does not advertise. The refusal is direct and
        // leaves nothing behind: no redirect, no session, no approval ticket.
        let response = request(build_state(&[WEB_CALLBACK]), &setup_pairs).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(location(&response).is_none());
        assert!(response.headers().get(header::SET_COOKIE).is_none());
        let body = response_body(response).await;
        assert!(body.contains(r#""error":"invalid_request""#));
        assert!(!body.contains("name=\"ticket\""));
    }

    /// A trusted client skips the approval page. At the Organization MCP
    /// Endpoint every client does: the person approves after sign-in, on the
    /// page where they pick the Workspace.
    #[tokio::test]
    async fn these_requests_skip_the_approval_page_and_carry_a_sealed_sign_in() {
        const ORGANIZATION_RESOURCE: &str = "https://mcp.example.test/mcp";
        let database = database().await;
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        for (case, extra, resource, client_name) in [
            ("trusted client", trusting(CLIENT_ID), RESOURCE, None),
            (
                "Organization MCP Endpoint",
                String::new(),
                ORGANIZATION_RESOURCE,
                Some("Test Client"),
            ),
        ] {
            let (resolver, calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
            let mut auth_state = state_with(&database, &provider.uri(), &extra, resolver);
            auth_state.workspace_resource_family = Some(Arc::new(
                crate::WorkspaceMcpUrls::new(
                    crate::CanonicalOauthUrl::parse("https://mcp.example.test").unwrap(),
                )
                .unwrap(),
            ));
            let mut query = pairs();
            replace(&mut query, "resource", Some(resource));
            let response = request(auth_state.clone(), &query).await;
            assert_eq!(response.status(), StatusCode::FOUND, "{case}");
            assert_security(&response);
            assert!(
                !sets_approval_cookie(&response),
                "{case}: a skipped approval page must not set an approval cookie"
            );
            let provider_url = location(&response).expect("provider redirect");
            assert!(
                provider_url.starts_with(&provider.uri()),
                "{case}: redirected to {provider_url}"
            );
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            let provider_query = Url::parse(provider_url)
                .expect("URL")
                .query_pairs()
                .into_owned()
                .collect::<BTreeMap<_, _>>();
            let oidc_state = provider_query.get("state").expect("OIDC state");
            let session = returned_session(&auth_state, &response, oidc_state);
            assert_eq!(session.request.client_id, CLIENT_ID);
            assert_eq!(session.request.redirect_uri, REDIRECT_URI);
            assert_eq!(session.request.client_state.as_deref(), Some(CLIENT_STATE));
            assert_eq!(session.request.code_challenge, CHALLENGE);
            assert_eq!(session.request.resource, resource);
            assert_eq!(session.client_name.as_deref(), client_name, "{case}");
        }
    }

    /// Trust skips only the approval page. Every validation the page sits
    /// behind still runs, so a trusted client's malformed request fails exactly
    /// like anyone else's rather than being waved through to the provider.
    #[tokio::test]
    async fn trusted_client_requests_are_validated_like_any_other() {
        let database = database().await;
        let trusted_state = |resolver| {
            state_with(
                &database,
                "https://provider.invalid",
                &trusting(CLIENT_ID),
                resolver,
            )
        };

        let (resolver, _calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
        let mut missing_challenge = pairs();
        replace(&mut missing_challenge, "code_challenge", None);
        let response = request(trusted_state(resolver), &missing_challenge).await;
        assert_eq!(response.status(), StatusCode::FOUND);
        let error_location = location(&response).expect("client error redirect");
        assert!(
            error_location.starts_with("https://client.example.test/callback"),
            "redirected to {error_location}"
        );
        assert!(error_location.contains("error=invalid_request"));

        let (resolver, _calls) = fake_resolver(Ok(registration(&[
            "https://client.example.test/other-callback",
        ])));
        let response = request(trusted_state(resolver), &pairs()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(location(&response).is_none());
    }

    #[tokio::test]
    async fn only_the_listed_client_skips_the_approval_page() {
        let database = database().await;
        let (resolver, _calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
        let auth_state = state_with(
            &database,
            "https://provider.invalid",
            &trusting("https://other.example.test/oauth/client.json"),
            resolver,
        );
        let response = request(auth_state, &pairs()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response_body(response).await.contains("to Coral</h1>"));
    }

    #[tokio::test]
    async fn trusted_client_provider_failures_are_generic_trusted_errors() {
        let database = database().await;
        let provider = MockServer::start().await;
        mount_discovery(&provider, 500).await;
        let (resolver, _calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
        let auth_state = state_with(&database, &provider.uri(), &trusting(CLIENT_ID), resolver);
        let response = request(auth_state, &pairs()).await;
        let error_location = location(&response).expect("error redirect");
        assert!(error_location.starts_with("https://client.example.test/callback"));
        assert!(error_location.contains("error=server_error"));
        assert!(!error_location.contains(PROVIDER_SECRET));
    }

    #[tokio::test]
    async fn provider_failures_after_consent_are_generic_trusted_errors() {
        let database = database().await;
        let provider = MockServer::start().await;
        mount_discovery(&provider, 500).await;
        let auth_state = state(&database, &provider.uri());
        let ticket = approval_ticket(auth_state.clone(), &pairs()).await;
        let response = submit(auth_state, &ticket, "continue").await;
        let error_location = location(&response).expect("error redirect");
        assert!(error_location.contains("error=server_error"));
        assert!(!error_location.contains(PROVIDER_SECRET));
    }

    #[tokio::test]
    async fn listener_exposes_authorize_route() {
        let database = database().await;
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let auth_state = state(&database, &provider.uri());
        let bind_addr = auth_state.settings.bind_addr();
        let server = super::super::start_listener(bind_addr, super::super::auth_router(auth_state))
            .await
            .expect("start");
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("client");
        let authorize_url = format!("{}/oauth/authorize", server.endpoint_uri());
        let response = client
            .get(format!(
                "{}/oauth/authorize?{}",
                server.endpoint_uri(),
                encode(&pairs())
            ))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::OK);
        let cookie = response.headers()[header::SET_COOKIE]
            .to_str()
            .expect("approval cookie")
            .split(';')
            .next()
            .expect("cookie pair")
            .to_string();
        let ticket = ticket_from_page(&response.text().await.expect("page"));
        let response = client
            .post(authorize_url)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::ORIGIN, AUTH_ISSUER)
            .header(header::COOKIE, cookie)
            .body(
                form_urlencoded::Serializer::new(String::new())
                    .extend_pairs([("ticket", ticket.as_str()), ("decision", "cancel")])
                    .finish(),
            )
            .send()
            .await
            .expect("submit");
        assert_eq!(response.status(), StatusCode::FOUND);
        server.shutdown().await.expect("shutdown");
    }
}
