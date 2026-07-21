use std::collections::BTreeSet;

use axum::extract::{RawQuery, Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use url::{Url, form_urlencoded};

use super::super::provider_client::OidcAuthorizationRequest;
use super::super::state_store::{
    OAuthAuthorizationApprovalRecord, OAuthAuthorizationSessionRecord,
};
use super::{AuthState, OAuthServerConfig, oauth_client_id, oauth_client_name};

mod confirmation;

use confirmation::ApprovalDecision;

const MAX_QUERY_BYTES: usize = 8 * 1024;
const MAX_PARAMETERS: usize = 32;
const MAX_PARAMETER_NAME_BYTES: usize = 64;
const MAX_PARAMETER_VALUE_BYTES: usize = 2 * 1024;

pub(super) async fn oauth_authorize_get(
    State(state): State<AuthState>,
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
    let Some((client_name, callback)) =
        registered_client(&state.config.oauth, client_id, redirect_uri)
    else {
        return direct_error("invalid_request", "client_id or redirect_uri is invalid");
    };
    let trusted = TrustedRedirect {
        url: callback.clone(),
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
    let oauth = &state.config.oauth;
    if query
        .scope
        .as_deref()
        .is_some_and(|scope| scope != oauth.scope.as_str())
    {
        return trusted.error("invalid_scope", "scope is not supported");
    }
    if query
        .resource
        .as_deref()
        .is_some_and(|resource| resource != oauth.resource.as_str())
    {
        return trusted.error("invalid_target", "resource does not match this server");
    }
    let provider_id = match query.provider.as_deref() {
        Some(name) => match state.config.providers.get_key_value(name) {
            Some((provider_id, _provider)) => provider_id,
            None => return trusted.error("invalid_request", "OIDC provider is unknown"),
        },
        None => match state.config.providers.first_key_value() {
            Some((provider_id, _provider)) => provider_id,
            None => return trusted.error("server_error", "authorization failed"),
        },
    };
    let approval = OAuthAuthorizationApprovalRecord {
        provider_id: provider_id.clone(),
        client_id: client_id.to_string(),
        client_name,
        redirect_uri: redirect_uri.to_string(),
        client_state: query.state,
        code_challenge,
        scope: oauth.scope.clone(),
    };
    let Ok(ticket) = confirmation::new_ticket() else {
        return trusted.error("server_error", "authorization failed");
    };
    let Some(page) = confirmation::response(
        &ticket,
        &approval.client_name,
        &approval.client_id,
        &callback,
    ) else {
        return trusted.error("server_error", "authorization failed");
    };
    if state
        .store
        .store_authorization_approval(&ticket, approval)
        .await
        .is_err()
    {
        return trusted.error("server_error", "authorization failed");
    }
    page
}

pub(super) async fn oauth_authorize_post(
    State(state): State<AuthState>,
    request: Request,
) -> Response {
    let Ok((ticket, decision)) = confirmation::parse_submission(request).await else {
        return approval_error();
    };
    let Ok(Some(approval)) = state.store.take_authorization_approval(&ticket).await else {
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
    let Some(provider_config) = state.config.providers.get(&approval.provider_id) else {
        return trusted.error("server_error", "authorization failed");
    };
    let request = match state
        .provider_client
        .authorization_request(provider_config)
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
        provider_id: approval.provider_id,
        client_id: approval.client_id,
        redirect_uri: approval.redirect_uri,
        client_state: approval.client_state,
        code_challenge: approval.code_challenge,
        scope: approval.scope,
        oidc_code_verifier,
        oidc_nonce,
    };
    if state
        .store
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
    provider: Option<String>,
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
            "provider" => &mut query.provider,
            _ => continue,
        };
        *target = Some(value.into_owned());
    }
    Ok(query)
}

fn registered_client(
    oauth: &OAuthServerConfig,
    client_id: &str,
    redirect_uri: &str,
) -> Option<(String, Url)> {
    oauth.clients.iter().find_map(|(name, redirect_uris)| {
        (oauth_client_id(oauth, name) == client_id)
            .then_some(redirect_uris)
            .and_then(|uris| uris.iter().find(|uri| uri.as_str() == redirect_uri))
            .and_then(|uri| Url::parse(uri).ok())
            .map(|redirect| (oauth_client_name(name).to_string(), redirect))
    })
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
    fn parse(redirect_uri: &str, client_state: Option<String>) -> Option<Self> {
        Some(Self {
            url: Url::parse(redirect_uri).ok()?,
            client_state,
        })
    }

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

fn approval_error() -> Response {
    direct_error(
        "invalid_request",
        "authorization approval is invalid or expired",
    )
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
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::REFERRER_POLICY, "no-referrer"),
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
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use serde_json::json;
    use tokio::sync::Barrier;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::{
        CLI_CLIENT, CLI_REDIRECT_URI, OAuthClientConfigFile, OAuthServerConfig, OidcAuthConfig,
    };
    use super::*;
    use crate::auth::provider::{OidcProviderConfig, ProviderConfigFile};
    use crate::auth::provider_client::OidcProviderClient;
    use crate::auth::session::SessionTokenConfig;
    use crate::auth::state_store::{
        InMemoryStateStore, OAuthAuthorizationApprovalTicket, StateStore,
    };

    const AUTH_ISSUER: &str = "https://auth.example.test";
    const RESOURCE: &str = "https://api.example.test/mcp";
    const SCOPE: &str = "coral:access";
    const CLIENT_ID: &str = "https://auth.example.test/oauth/clients/web";
    const REDIRECT_URI: &str = "https://client.example.test/callback?tenant=one";
    const CLIENT_STATE: &str = "client-state&error=injected";
    const CHALLENGE: &str = "0123456789012345678901234567890123456789012";
    const PROVIDER_SECRET: &str = "provider-secret-must-not-leak";

    fn provider(issuer: &str) -> OidcProviderConfig {
        toml::from_str::<ProviderConfigFile>(&format!(
            "issuer = '{issuer}'
             client_id = 'provider-client'
             client_secret = '{PROVIDER_SECRET}'
             redirect_uri = 'http://localhost/provider-callback'"
        ))
        .expect("provider file")
        .build("provider", &|_| Ok(None))
        .expect("provider")
    }

    fn state(issuer: &str, store: Arc<dyn StateStore>) -> AuthState {
        let providers = BTreeMap::from([
            ("zeta".to_string(), provider(issuer)),
            ("alpha".to_string(), provider(issuer)),
        ]);
        let oauth = OAuthServerConfig {
            issuer: AUTH_ISSUER.into(),
            resource: RESOURCE.into(),
            scope: SCOPE.into(),
            clients: BTreeMap::from([
                ("web".into(), vec![REDIRECT_URI.into()]),
                (CLI_CLIENT.into(), vec![CLI_REDIRECT_URI.into()]),
            ]),
        };
        let session = SessionTokenConfig::new(
            Some(AUTH_ISSUER),
            Some(RESOURCE),
            [b'k'; 32],
            Duration::from_mins(5),
        )
        .expect("session");
        AuthState {
            config: Arc::new(OidcAuthConfig {
                bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                session,
                providers,
                oauth,
            }),
            store,
            provider_client: OidcProviderClient::new().expect("provider client"),
        }
    }

    fn pairs() -> Vec<(String, String)> {
        [
            ("response_type", "code"),
            ("client_id", CLIENT_ID),
            ("redirect_uri", REDIRECT_URI),
            ("scope", SCOPE),
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

    async fn request(state: AuthState, pairs: &[(String, String)]) -> Response {
        oauth_authorize_get(State(state), RawQuery(Some(encode(pairs)))).await
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

    fn submission_request(ticket: &str, decision: &str) -> Request {
        let body = form_urlencoded::Serializer::new(String::new())
            .extend_pairs([("ticket", ticket), ("decision", decision)])
            .finish();
        Request::builder()
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .expect("request")
    }

    async fn submit(state: AuthState, ticket: &str, decision: &str) -> Response {
        oauth_authorize_post(State(state), submission_request(ticket, decision)).await
    }

    async fn approval_ticket(state: AuthState, pairs: &[(String, String)]) -> String {
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
    async fn confirmation_is_secure_stores_only_validated_data_and_supports_cli() {
        let store = Arc::new(InMemoryStateStore::new());
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
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
            ),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::REFERRER_POLICY, "no-referrer"),
            (header::X_FRAME_OPTIONS, "DENY"),
        ] {
            assert_eq!(response.headers()[name], value);
        }
        let page = response_body(response).await;
        assert!(page.contains("Client ID hostname</dt><dd><code>auth.example.test"));
        assert!(page.contains("Redirect host and port</dt><dd><code>client.example.test:443"));
        assert!(page.contains("method=\"post\" action=\"/oauth/authorize\""));
        assert!(!page.contains(CLIENT_STATE));
        assert!(!page.contains(CHALLENGE));
        assert!(!page.contains("<script"));
        let ticket = ticket_from_page(&page);
        let ticket = OAuthAuthorizationApprovalTicket::from_bytes(
            URL_SAFE_NO_PAD
                .decode(ticket)
                .expect("ticket")
                .try_into()
                .expect("ticket length"),
        );
        let approval = store
            .take_authorization_approval(&ticket)
            .await
            .expect("store")
            .expect("approval");
        assert_eq!(approval.provider_id, "alpha");
        assert_eq!(approval.client_id, CLIENT_ID);
        assert_eq!(approval.client_name, "web");
        assert_eq!(approval.redirect_uri, REDIRECT_URI);
        assert_eq!(approval.client_state.as_deref(), Some(CLIENT_STATE));
        assert_eq!(approval.code_challenge, CHALLENGE);
        assert_eq!(approval.scope, SCOPE);

        let mut cli = pairs();
        replace(
            &mut cli,
            "client_id",
            Some(&format!("{AUTH_ISSUER}/oauth/clients/{CLI_CLIENT}")),
        );
        replace(&mut cli, "redirect_uri", Some(CLI_REDIRECT_URI));
        let page =
            response_body(request(state("https://provider.invalid", store), &cli).await).await;
        assert!(page.contains("<bdi>Coral CLI</bdi>"));
        assert!(page.contains("127.0.0.1:14554"));
        assert!(page.contains("Local redirect warning"));

        let escaped = confirmation::response(
            &OAuthAuthorizationApprovalTicket::from_bytes([7; 32]),
            "</bdi><script>alert(\"x\")</script>&'",
            CLIENT_ID,
            &Url::parse(REDIRECT_URI).expect("redirect"),
        )
        .expect("page");
        let escaped = response_body(escaped).await;
        assert!(!escaped.contains("</bdi><script>"));
        assert!(escaped.contains("&lt;/bdi&gt;&lt;script&gt;alert(&quot;x&quot;)"));
    }

    #[tokio::test]
    async fn submission_form_is_strict_bounded_and_ignores_no_query_bypass() {
        let ticket = URL_SAFE_NO_PAD.encode([7; 32]);
        let malformed = [
            format!("ticket={ticket}"),
            format!("ticket={ticket}&decision=continue&extra=x"),
            format!("ticket={ticket}&ticket={ticket}&decision=continue"),
            format!("ticket={ticket}&decision=approve"),
            format!("ticket={ticket}=&decision=continue"),
            "x".repeat(257),
        ];
        for body in malformed {
            let request = Request::builder()
                .uri(format!(
                    "/oauth/authorize?ticket={ticket}&decision=continue"
                ))
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .expect("request");
            assert!(confirmation::parse_submission(request).await.is_err());
        }
        let request = Request::builder()
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from(format!("ticket={ticket}&decision=continue")))
            .expect("request");
        assert!(confirmation::parse_submission(request).await.is_err());
    }

    #[tokio::test]
    async fn client_and_redirect_must_match_exactly_before_redirecting() {
        let store = Arc::new(InMemoryStateStore::new());
        for (field, value) in [
            ("client_id", "https://auth.example.test/oauth/clients/%77eb"),
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
        let response = oauth_authorize_get(
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
            ("scope", Some("other scope"), "invalid_scope"),
            ("resource", Some("https://other.test"), "invalid_target"),
            ("provider", Some("unknown"), "invalid_request"),
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
        let spawn = |state: AuthState| {
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
        assert_eq!(session.provider_id, "alpha");
        assert_eq!(session.client_id, CLIENT_ID);
        assert_eq!(session.redirect_uri, REDIRECT_URI);
        assert_eq!(session.client_state.as_deref(), Some(CLIENT_STATE));
        assert_eq!(session.code_challenge, CHALLENGE);
        assert_eq!(session.scope, SCOPE);
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

        let mut explicit = pairs();
        replace(&mut explicit, "provider", Some("zeta"));
        let auth_state = state(&provider.uri(), store.clone());
        let ticket = approval_ticket(auth_state.clone(), &explicit).await;
        let response = submit(auth_state, &ticket, "continue").await;
        let url = Url::parse(location(&response).expect("provider redirect")).expect("URL");
        let state_key = url
            .query_pairs()
            .find(|(key, _value)| key == "state")
            .expect("state")
            .1;
        let session = store
            .take_authorization_session(&state_key)
            .await
            .expect("store")
            .expect("session");
        assert_eq!(session.provider_id, "zeta");
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

        provider.reset().await;
        mount_discovery(&provider, 200).await;
        let filler = OAuthAuthorizationSessionRecord {
            provider_id: "alpha".into(),
            client_id: CLIENT_ID.into(),
            redirect_uri: REDIRECT_URI.into(),
            client_state: None,
            code_challenge: CHALLENGE.into(),
            scope: SCOPE.into(),
            oidc_code_verifier: "verifier".into(),
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
        let config = state(&provider.uri(), Arc::new(InMemoryStateStore::new()))
            .config
            .as_ref()
            .clone();
        let server = config.start().await.expect("start");
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
        let ticket = ticket_from_page(&response.text().await.expect("page"));
        let response = client
            .post(authorize_url)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
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

    #[test]
    fn redirect_config_rejects_response_parameter_collisions() {
        for key in ["code", "STATE", "%65rror", "error_description"] {
            let file: OAuthClientConfigFile = toml::from_str(&format!(
                "redirect_uris = ['https://client.example.test/callback?{key}=x']"
            ))
            .expect("client file");
            assert!(file.validate("web").is_err(), "accepted {key}");
        }
    }
}
