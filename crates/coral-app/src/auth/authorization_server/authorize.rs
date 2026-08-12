use axum::extract::{RawQuery, Request, State};
use axum::http::HeaderMap;
use axum::response::Response;
use url::Url;

use super::super::provider_client::OidcAuthorizationRequest;
use super::super::state_store::{
    OAuthAuthorizationApprovalRecord, OAuthAuthorizationSessionRecord,
};
use super::client_metadata::OAuthClientRegistration;
use super::response::{TrustedRedirect, direct_error, redirect};
use super::{AuthorizationServerHttpState, canonical_authorization_resource, query};
use crate::outbound_url_policy::{BrowserRedirect, EndpointUrl};

mod confirmation;

use confirmation::ApprovalDecision;

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
    let Some(callback) = registered_redirect(&registration.redirect_uris, redirect_uri) else {
        return direct_error("invalid_request", "client_id or redirect_uri is invalid");
    };
    let client_name = registration.client_name;
    let trusted = TrustedRedirect::new(callback.clone(), query.state.clone());

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
    let approval = OAuthAuthorizationApprovalRecord {
        client_id: client_id.to_string(),
        client_name,
        redirect_uri: redirect_uri.to_string(),
        client_state: query.state,
        code_challenge,
        resource,
    };
    let Ok(ticket) = confirmation::new_ticket() else {
        return trusted.error("server_error", "authorization failed");
    };
    let secure_cookie = secure_approval_cookie(&state);
    let Ok(browser_binding) = confirmation::browser_binding_for_page(&headers, secure_cookie)
    else {
        return trusted.error("server_error", "authorization failed");
    };
    let Some(page) = confirmation::response(
        &ticket,
        &approval.client_name,
        &approval.client_id,
        &callback,
        &browser_binding,
        secure_cookie,
    ) else {
        return trusted.error("server_error", "authorization failed");
    };
    if state
        .approval_store
        .store_authorization_approval(&ticket, &browser_binding, approval)
        .await
        .is_err()
    {
        return trusted.error("server_error", "authorization failed");
    }
    page
}

pub(super) async fn oauth_authorize_post(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    let Ok((ticket, browser_binding, decision)) = confirmation::parse_submission(
        request,
        state.settings.authorization_server().issuer(),
        secure_approval_cookie(&state),
    )
    .await
    else {
        return approval_error();
    };
    let Ok(Some(approval)) = state
        .approval_store
        .take_authorization_approval(&ticket, &browser_binding)
        .await
    else {
        return approval_error();
    };
    let Some(trusted) =
        TrustedRedirect::parse(&approval.redirect_uri, approval.client_state.clone())
    else {
        return direct_error("server_error", "authorization failed");
    };
    if decision == ApprovalDecision::Cancel {
        return trusted.error("access_denied", "authorization was denied");
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
        client_id: approval.client_id,
        redirect_uri: approval.redirect_uri,
        client_state: approval.client_state,
        code_challenge: approval.code_challenge,
        resource: approval.resource,
        oidc_code_verifier,
        oidc_nonce,
    };
    if state
        .session_store
        .store_authorization_session(&oidc_state, session)
        .await
        .is_err()
    {
        return trusted.error("server_error", "authorization failed");
    }
    redirect(url.as_str())
}

fn secure_approval_cookie(state: &AuthorizationServerHttpState) -> bool {
    state
        .settings
        .authorization_server()
        .issuer()
        .starts_with("https://")
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
fn registered_redirect(redirect_uris: &[String], redirect_uri: &str) -> Option<Url> {
    let requested = EndpointUrl::<BrowserRedirect>::parse(redirect_uri).ok()?;
    let matched = redirect_uris.iter().any(|uri| {
        uri.as_str() == redirect_uri
            || EndpointUrl::<BrowserRedirect>::parse(uri)
                .is_ok_and(|registered| loopback_port_variant(&registered, &requested))
    });
    matched.then(|| requested.into_url())
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
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{HeaderValue, StatusCode, header};
    use base64::Engine as _;
    use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
    use serde_json::json;
    use sha2::{Digest as _, Sha256};
    use tokio::sync::Barrier;
    use url::form_urlencoded;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::AuthorizationServerHttpState;
    use super::super::client_metadata::{
        ClientMetadataError, ClientMetadataResolver, OAuthClientRegistration,
    };
    use super::*;
    use crate::auth::config::AuthSettings;
    use crate::auth::config::ResolvedAuthSettings;
    use crate::auth::session::{SessionTokenIssuer, test_signing_key};
    use crate::auth::state_store::{
        ApprovalStore, InMemoryStateStore, OAuthAuthorizationApprovalBrowserBinding,
        OAuthAuthorizationApprovalTicket, SessionStore,
    };

    const AUTH_ISSUER: &str = "https://auth.example.test";
    const RESOURCE: &str = "https://api.example.test/mcp";
    const CLIENT_ID: &str = "https://client.example.test/oauth/client.json";
    const REDIRECT_URI: &str = "https://client.example.test/callback?tenant=one";
    const CLIENT_STATE: &str = "client-state&error=injected";
    const CHALLENGE: &str = "0123456789012345678901234567890123456789012";
    const PROVIDER_SECRET: &str = "provider-secret-must-not-leak";
    const BROWSER_BINDING: [u8; 32] = [0x42; 32];

    fn approval_page_style_src() -> String {
        let digest = Sha256::digest(confirmation::APPROVAL_PAGE_STYLES.as_bytes());
        format!("style-src 'sha256-{}'", STANDARD.encode(digest))
    }

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
        issuer: &str,
        store: Arc<InMemoryStateStore>,
        resolver: Arc<dyn ClientMetadataResolver>,
    ) -> AuthorizationServerHttpState {
        let session_tokens = SessionTokenIssuer::new(
            Some(AUTH_ISSUER),
            test_signing_key(),
            Duration::from_mins(5),
        )
        .expect("session");
        AuthorizationServerHttpState::with_client_metadata_resolver(
            Arc::new(settings(issuer)),
            session_tokens,
            store,
            Arc::new(BTreeSet::from([RESOURCE.into()])),
            resolver,
        )
        .expect("auth state")
    }

    fn state(issuer: &str, store: Arc<InMemoryStateStore>) -> AuthorizationServerHttpState {
        let (resolver, _calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
        state_with_resolver(issuer, store, resolver)
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

    fn browser_binding() -> OAuthAuthorizationApprovalBrowserBinding {
        OAuthAuthorizationApprovalBrowserBinding::from_bytes(BROWSER_BINDING)
    }

    async fn response_body(response: Response) -> String {
        String::from_utf8(
            to_bytes(response.into_body(), 16 * 1024)
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
        let store = Arc::new(InMemoryStateStore::new());
        let expected_style_src = approval_page_style_src();
        let expected_csp = format!(
            "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; {expected_style_src}"
        );
        let mut request_pairs = pairs();
        request_pairs.push(("approved".into(), "true".into()));
        request_pairs.push(("decision".into(), "continue".into()));
        let response = request(
            state("https://provider.invalid", store.clone()),
            &request_pairs,
        )
        .await;
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
        assert!(page.contains("Client ID hostname</dt><dd><code>client.example.test"));
        assert!(page.contains("Redirect host and port</dt><dd><code>client.example.test:443"));
        assert!(page.contains("method=\"post\" action=\"/oauth/authorize\""));
        assert!(
            page.find("value=\"cancel\"") < page.find("value=\"continue\""),
            "Cancel must be the form's first submit button, so a bare Enter key \
             does not approve access"
        );
        assert!(!page.contains(CLIENT_STATE));
        assert!(!page.contains(CHALLENGE));
        assert!(!page.contains("<script"));
        let ticket = ticket_from_page(&page);
        assert_eq!(
            set_cookie,
            format!(
                "{}; Path=/; Max-Age=300; HttpOnly; Secure; SameSite=Lax",
                browser_cookie()
            )
        );
        assert!(!set_cookie.contains(&ticket));
        let ticket = OAuthAuthorizationApprovalTicket::from_bytes(
            URL_SAFE_NO_PAD
                .decode(ticket)
                .expect("ticket")
                .try_into()
                .expect("ticket length"),
        );
        let approval = store
            .take_authorization_approval(&ticket, &browser_binding())
            .await
            .expect("store")
            .expect("approval");
        assert_eq!(approval.client_id, CLIENT_ID);
        assert_eq!(approval.client_name, "Test Client");
        assert_eq!(approval.redirect_uri, REDIRECT_URI);
        assert_eq!(approval.client_state.as_deref(), Some(CLIENT_STATE));
        assert_eq!(approval.code_challenge, CHALLENGE);
        assert_eq!(approval.resource, RESOURCE);
    }

    #[tokio::test]
    async fn confirmation_page_warns_on_loopback_and_escapes_client_text() {
        let loopback = confirmation::response(
            &OAuthAuthorizationApprovalTicket::from_bytes([6; 32]),
            "Local Test Client",
            CLIENT_ID,
            &Url::parse("http://127.0.0.1:14554/oauth/callback").expect("loopback redirect"),
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
        assert!(page.contains("Local redirect warning"));
        assert_eq!(
            loopback_cookie,
            format!(
                "coral_oauth_approval={}; Path=/; Max-Age=300; HttpOnly; SameSite=Lax",
                URL_SAFE_NO_PAD.encode(BROWSER_BINDING)
            )
        );
        assert!(!loopback_cookie.contains("; Secure"));

        let escaped = confirmation::response(
            &OAuthAuthorizationApprovalTicket::from_bytes([7; 32]),
            "</bdi><script>alert(\"x\")</script>&'",
            CLIENT_ID,
            &Url::parse(REDIRECT_URI).expect("redirect"),
            &browser_binding(),
            true,
        )
        .expect("page");
        let escaped = response_body(escaped).await;
        assert!(!escaped.contains("</bdi><script>"));
        assert!(escaped.contains("&lt;/bdi&gt;&lt;script&gt;alert(&quot;x&quot;)"));
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
            "x".repeat(257),
        ];
        for body in malformed {
            let request = submission(body, cookie(), FORM);
            let Err(()) = confirmation::parse_submission(request, AUTH_ISSUER, true).await else {
                panic!("accepted malformed approval submission");
            };
        }
        let request = submission(
            format!("ticket={ticket}&decision=continue"),
            cookie(),
            "text/plain",
        );
        let Err(()) = confirmation::parse_submission(request, AUTH_ISSUER, true).await else {
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
            let (ticket, browser_binding, decision) =
                confirmation::parse_submission(request, AUTH_ISSUER, true)
                    .await
                    .expect("rejected a well-formed approval submission");
            assert_eq!(ticket.as_bytes(), &[7; 32]);
            assert_eq!(browser_binding.as_bytes(), &BROWSER_BINDING);
            assert!(decision == ApprovalDecision::Cancel);
        }
    }

    #[tokio::test]
    async fn submission_requires_same_origin_browser_binding_without_consuming_the_approval() {
        let auth_state = state(
            "https://provider.invalid",
            Arc::new(InMemoryStateStore::new()),
        );
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
    /// one of them, so any list short of all of them blocks the login — after
    /// the POST has already consumed the approval, leaving the user on a page
    /// where nothing happened and a retry reporting the approval as expired.
    /// A real sign-in through a hosted provider crossed five origins this way.
    /// The remaining directives still deny every fetch, base URI and framing.
    #[tokio::test]
    async fn approval_page_does_not_restrict_form_action() {
        let auth_state = state(
            "https://provider.invalid",
            Arc::new(InMemoryStateStore::new()),
        );
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
        let auth_state = state(
            "https://provider.invalid",
            Arc::new(InMemoryStateStore::new()),
        );
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
        let auth_state = state(
            "https://provider.invalid",
            Arc::new(InMemoryStateStore::new()),
        );
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
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let (resolver, calls) = fake_resolver(Ok(registration(&[REDIRECT_URI])));
        let request_pairs = pairs();
        let auth_state = state_with_resolver(
            &provider.uri(),
            Arc::new(InMemoryStateStore::new()),
            resolver,
        );
        let response = request(auth_state.clone(), &request_pairs).await;
        assert_eq!(response.status(), StatusCode::OK);
        let page = response_body(response).await;
        assert!(page.contains("<bdi>Test Client</bdi>"));
        assert!(page.contains("Client ID hostname</dt><dd><code>client.example.test"));
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
        let (resolver, calls) = fake_resolver(Ok(registration(&[
            "https://client.example.test/other-callback",
        ])));
        let response = request(
            state_with_resolver(
                "https://provider.invalid",
                Arc::new(InMemoryStateStore::new()),
                resolver,
            ),
            &pairs(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(location(&response).is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn resolver_failures_are_generic_direct_errors() {
        let (resolver, calls) = fake_resolver(Err(ClientMetadataError::HttpStatus));
        let client_id = format!("{CLIENT_ID}?secret=attacker-url-secret");
        let mut request_pairs = pairs();
        replace(&mut request_pairs, "client_id", Some(&client_id));
        let response = request(
            state_with_resolver(
                "https://provider.invalid",
                Arc::new(InMemoryStateStore::new()),
                resolver,
            ),
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
        let store = Arc::new(InMemoryStateStore::new());
        let (resolver, _calls) = fake_resolver(Err(ClientMetadataError::InvalidMetadata));
        let mut wrong_client = pairs();
        replace(
            &mut wrong_client,
            "client_id",
            Some("https://client.example.test/oauth/%63lient.json"),
        );
        let response = request(
            state_with_resolver("https://provider.invalid", store.clone(), resolver),
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
            state("https://provider.invalid", store.clone()),
            &wrong_redirect,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_security(&response);
        assert!(location(&response).is_none());

        let raw = format!("{}&client%5Fid=raw-secret", encode(&pairs()));
        let response = oauth_authorize_get(
            State(state("https://provider.invalid", store)),
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
        let store = Arc::new(InMemoryStateStore::new());
        for uri in [
            "javascript:alert(1)",
            "data:text/html,<script>alert(1)</script>",
            "http://client.example.test/callback",
        ] {
            let (resolver, _calls) = fake_resolver(Ok(registration(&[uri])));
            let auth_state =
                state_with_resolver("https://provider.invalid", store.clone(), resolver);
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
                resolved.as_ref().map(Url::as_str),
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

    /// A browser client registers the callback it will actually be sent to, and
    /// keeps matching byte for byte.
    #[test]
    fn https_registrations_still_match_exactly() {
        let registered = [REDIRECT_URI.to_string()];
        assert_eq!(
            registered_redirect(&registered, REDIRECT_URI)
                .as_ref()
                .map(Url::as_str),
            Some(REDIRECT_URI)
        );
        for requested in [
            "https://client.example.test/callback",
            "https://client.example.test/callback?tenant=two",
            "https://client.example.test:8443/callback?tenant=one",
        ] {
            assert_eq!(
                registered_redirect(&registered, requested),
                None,
                "{requested}"
            );
        }
    }

    /// The port a native client names is the one it is listening on, so it has
    /// to reach the consent page and the `Location` the browser follows rather
    /// than the portless URI the metadata document registered.
    #[tokio::test]
    async fn loopback_client_returns_to_the_port_it_requested() {
        let requested = "http://127.0.0.1:3118/callback";
        let (resolver, _calls) = fake_resolver(Ok(registration(&["http://127.0.0.1/callback"])));
        let auth_state = state_with_resolver(
            "https://provider.invalid",
            Arc::new(InMemoryStateStore::new()),
            resolver,
        );
        let mut request_pairs = pairs();
        replace(&mut request_pairs, "redirect_uri", Some(requested));
        let response = request(auth_state.clone(), &request_pairs).await;
        assert_eq!(response.status(), StatusCode::OK);
        let page = response_body(response).await;
        assert!(page.contains("<code>127.0.0.1:3118</code>"));
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
    async fn cancel_is_trusted_once_while_replay_and_restart_are_direct_errors() {
        let store = Arc::new(InMemoryStateStore::new());
        let auth_state = state("https://provider.invalid", store);
        let ticket = approval_ticket(auth_state.clone(), &pairs()).await;
        let cancelled = submit(auth_state.clone(), &ticket, "cancel").await;
        let callback = Url::parse(location(&cancelled).expect("client redirect")).expect("URL");
        let query = callback.query_pairs().into_owned().collect::<Vec<_>>();
        assert!(query.contains(&("tenant".into(), "one".into())));
        assert!(query.contains(&("error".into(), "access_denied".into())));
        assert!(query.contains(&("state".into(), CLIENT_STATE.into())));

        let replay = submit(auth_state, &ticket, "cancel").await;
        let restarted = submit(
            state(
                "https://provider.invalid",
                Arc::new(InMemoryStateStore::new()),
            ),
            &ticket,
            "continue",
        )
        .await;
        for response in [replay, restarted] {
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(location(&response).is_none());
            let body = response_body(response).await;
            assert!(body.contains("authorization approval is invalid or expired"));
            assert!(!body.contains(&ticket));
            assert!(!body.contains(REDIRECT_URI));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expired_approval_is_a_direct_error() {
        let auth_state = state(
            "https://provider.invalid",
            Arc::new(InMemoryStateStore::new()),
        );
        let ticket = approval_ticket(auth_state.clone(), &pairs()).await;
        tokio::time::advance(Duration::from_mins(6)).await;
        let response = submit(auth_state, &ticket, "continue").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(location(&response).is_none());
    }

    #[tokio::test]
    async fn concurrent_double_submit_has_one_winner() {
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let auth_state = state(&provider.uri(), Arc::new(InMemoryStateStore::new()));
        let ticket = approval_ticket(auth_state.clone(), &pairs()).await;
        let barrier = Arc::new(Barrier::new(3));
        let spawn = |state: AuthorizationServerHttpState| {
            let barrier = barrier.clone();
            let ticket = ticket.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                submit(state, &ticket, "continue").await
            })
        };
        let first = spawn(auth_state.clone());
        let second = spawn(auth_state);
        barrier.wait().await;
        let (first, second) = tokio::join!(first, second);
        let (first, second) = (first.expect("first submit"), second.expect("second submit"));
        assert!(
            matches!(
                (first.status(), second.status()),
                (StatusCode::FOUND, StatusCode::BAD_REQUEST)
                    | (StatusCode::BAD_REQUEST, StatusCode::FOUND)
            ),
            "one submission must win"
        );
        assert_eq!(
            provider.received_requests().await.expect("requests").len(),
            1
        );
    }

    #[tokio::test]
    async fn continue_stores_single_use_session_before_provider_redirect() {
        let provider = MockServer::start().await;
        mount_discovery(&provider, 200).await;
        let store = Arc::new(InMemoryStateStore::new());
        let auth_state = state(&provider.uri(), store.clone());
        let ticket = approval_ticket(auth_state.clone(), &pairs()).await;
        assert!(
            provider
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
        let response = submit(auth_state, &ticket, "continue").await;
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
        let auth_state = state(&provider.uri(), store.clone());
        let ticket = approval_ticket(auth_state.clone(), &pairs()).await;
        let response = submit(auth_state, &ticket, "continue").await;
        let error_location = location(&response).expect("error redirect");
        assert!(error_location.contains("error=server_error"));
        assert!(!error_location.contains(PROVIDER_SECRET));

        // Every filler entry names its own client, because the per-client
        // bound is what stops one client from reaching the shared budget on
        // its own. Reaching it at all is the setup for the assertion below,
        // not the behavior under test.
        let filler = OAuthAuthorizationApprovalRecord {
            client_id: CLIENT_ID.into(),
            client_name: "Test Client".into(),
            redirect_uri: REDIRECT_URI.into(),
            client_state: None,
            code_challenge: CHALLENGE.into(),
            resource: RESOURCE.into(),
        };
        for index in 0_u64..4096 {
            let mut ticket = [0; 32];
            ticket[..8].copy_from_slice(&index.to_le_bytes());
            let ticket = OAuthAuthorizationApprovalTicket::from_bytes(ticket);
            store
                .store_authorization_approval(
                    &ticket,
                    &browser_binding(),
                    OAuthAuthorizationApprovalRecord {
                        client_id: format!("https://filler-{index}.example.test/client.json"),
                        ..filler.clone()
                    },
                )
                .await
                .expect("fill approval store");
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
        let auth_state = state(&provider.uri(), Arc::new(InMemoryStateStore::new()));
        let bind_addr = auth_state.settings.bind_addr();
        let server = super::super::start_listener(bind_addr, auth_state)
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
