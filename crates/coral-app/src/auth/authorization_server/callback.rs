use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::{SecureRandom as _, SystemRandom};
use zeroize::Zeroizing;

use super::super::id_token::ValidatedOidcIdentity;
use super::super::sealed_handshake::SignInSession;
use super::response::{TrustedRedirect, direct_error};
use super::{AuthorizationServerHttpState, query, sign_in_cookie};
use coral_auth::authorization_codes::{AuthorizationGrant, CodeIssue};
use coral_auth::{LoginProvisioning, UserDirectory, VerifiedOidcLogin};
use coral_workspace::UserId;

pub(super) async fn oidc_callback(
    State(state): State<AuthorizationServerHttpState>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Ok(query) = parse_query(raw_query.as_deref().unwrap_or_default()) else {
        return direct_error("invalid_request", "OIDC callback request is malformed");
    };
    let Some(oidc_state) = query.state.as_deref().filter(|state| !state.is_empty()) else {
        return direct_error("invalid_request", "OIDC callback state is required");
    };
    // A browser that did not start this sign-in has no cookie for it.
    let secure = state.secure_cookies();
    let Some((cookie_name, sealed)) = sign_in_cookie::find(&headers, oidc_state, secure) else {
        return invalid_state();
    };
    let session = sign_in_cookie::now_unix_secs().and_then(|now| {
        SignInSession::open(state.settings.handshake_keys(), oidc_state, &sealed, now)
    });
    let mut response = match session {
        Some(session) => complete(&state, &query, oidc_state, session, &headers).await,
        None => invalid_state(),
    };
    if let Some(clear) = sign_in_cookie::clear_header(&cookie_name, secure) {
        response.headers_mut().append(header::SET_COOKIE, clear);
    }
    response
}

fn invalid_state() -> Response {
    direct_error(
        "invalid_request",
        "OIDC callback state is invalid or expired",
    )
}

/// Finishes the sign-in `session` describes with the provider's answer in `query`.
async fn complete(
    state: &AuthorizationServerHttpState,
    query: &CallbackQuery,
    oidc_state: &str,
    session: SignInSession,
    headers: &HeaderMap,
) -> Response {
    let Some(trusted) = trusted_redirect(&session) else {
        return direct_error("server_error", "authorization failed");
    };
    let provider_code = match classify_result(query) {
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
    let exchange = match state
        .provider_client
        .exchange_code(
            provider,
            &provider_code,
            &state.settings.handshake_keys().pkce_verifier(oidc_state),
        )
        .await
    {
        Ok(exchange) => exchange,
        Err(error) => {
            tracing::warn!(
                error.type = error.code(),
                "OIDC callback could not exchange the provider code"
            );
            return trusted.error("server_error", "authorization failed");
        }
    };
    let identity = match state
        .provider_client
        .validate_code_exchange(
            provider,
            exchange,
            &state.settings.handshake_keys().nonce(oidc_state),
        )
        .await
    {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(
                error.type = error.code(),
                "OIDC callback could not validate the provider identity"
            );
            return trusted.error("server_error", "authorization failed");
        }
    };
    let user_id = match provision(&state.user_directory, &identity).await {
        Ok(user_id) => user_id,
        Err(outcome) => return outcome.into_error(&trusted),
    };
    // At the Organization MCP Endpoint the User picks the Workspace first;
    // the picker's submission issues the code.
    if state
        .authorization_resource(&session.request.resource)
        .is_some_and(|target| target.picks_workspace())
    {
        let signed_in_as = identity
            .email
            .as_deref()
            .or(identity.display_name.as_deref());
        return super::workspace_choice::offer(
            state,
            headers,
            &trusted,
            oidc_state,
            user_id,
            signed_in_as,
            session,
        )
        .await;
    }
    issue_code(
        state,
        &trusted,
        oidc_state,
        &session.request.grant(&user_id, None),
    )
    .await
}

/// Stores a new code for `grant`, issued by the sign-in named `oidc_state`,
/// and returns the client redirect that carries it. A sign-in that already
/// issued its code, or a code that cannot be stored, is refused with the
/// client's error redirect.
pub(super) async fn issue_code(
    state: &AuthorizationServerHttpState,
    trusted: &TrustedRedirect,
    oidc_state: &str,
    grant: &AuthorizationGrant,
) -> Response {
    let Ok(authorization_code) = random_code() else {
        tracing::warn!("Sign-in could not generate an authorization code");
        return trusted.error("server_error", "authorization failed");
    };
    let Ok(now_unix_nanos) = now_unix_nanos_i64() else {
        tracing::warn!("Sign-in could not read the system clock");
        return trusted.error("server_error", "authorization failed");
    };
    match state
        .codes
        .issue(&authorization_code, oidc_state, grant, now_unix_nanos)
        .await
    {
        Ok(CodeIssue::Issued) => trusted.success(&authorization_code),
        Ok(CodeIssue::SignInAlreadyUsed) => {
            trusted.error("invalid_request", "authorization was already completed")
        }
        Err(error) => {
            tracing::warn!(
                error.type = error.code(),
                "Sign-in could not store the authorization code"
            );
            trusted.error("server_error", "authorization failed")
        }
    }
}

/// Guidance for the operator whose directory already binds this subject.
///
/// A subject arriving under a second issuer is a directory question, not
/// something the client or the person signing in can retry their way out of, so
/// the description names the one action that resolves it.
const ISSUER_MISMATCH_DESCRIPTION: &str = "this account is already registered under a different \
     identity provider; ask your Coral operator to reconcile the user directory";

/// Why a verified login could not be turned into an internal `user_id`.
enum ProvisioningFailure {
    /// The subject is already bound to a different issuer.
    IssuerMismatch,
    /// Coral-side disablement cannot be undone by upstream sign-in.
    Disabled,
    /// Coral could not record the login at all.
    Unavailable,
}

impl ProvisioningFailure {
    fn into_error(self, trusted: &TrustedRedirect) -> Response {
        match self {
            Self::IssuerMismatch => trusted.error("access_denied", ISSUER_MISMATCH_DESCRIPTION),
            Self::Unavailable => trusted.error("server_error", "authorization failed"),
            Self::Disabled => trusted.error("access_denied", "access is disabled"),
        }
    }
}

/// Records the verified login and returns the internal `user_id` a stored
/// authorization code may name.
///
/// This is the boundary the upstream identity stops at: issuer, subject, and
/// display name reach the user directory and go no further, so the record a
/// client redeems — and every token and principal id minted from it — carries
/// only Coral's own identifier. A login that cannot be recorded issues no code.
async fn provision(
    user_directory: &UserDirectory,
    identity: &ValidatedOidcIdentity,
) -> Result<UserId, ProvisioningFailure> {
    let Ok(now_unix_nanos) = now_unix_nanos_i64() else {
        tracing::warn!("OIDC callback could not read the system clock");
        return Err(ProvisioningFailure::Unavailable);
    };
    let provisioning = user_directory
        .provision_login(
            VerifiedOidcLogin::new(
                &identity.issuer,
                &identity.subject,
                identity.display_name.as_deref(),
                now_unix_nanos,
            )
            .with_profile(coral_workspace::VerifiedUserProfile::new(
                identity.profile_name.as_deref(),
                identity.email.as_deref(),
                identity.email_verified,
                identity.hosted_domain.as_deref(),
            ))
            .with_profile_observed_at(identity.profile_observed_at_unix_nanos),
        )
        .await;
    match provisioning {
        Ok(LoginProvisioning::Provisioned(user_id)) => Ok(user_id),
        Ok(LoginProvisioning::Disabled) => Err(ProvisioningFailure::Disabled),
        Ok(LoginProvisioning::IssuerMismatch) => {
            tracing::warn!(
                "OIDC callback refused a login whose subject is already bound to a different \
                 provider issuer; reconcile that user directory row before this login can succeed"
            );
            Err(ProvisioningFailure::IssuerMismatch)
        }
        Err(error) => {
            let _error = error;
            tracing::warn!("OIDC callback could not provision the verified login");
            Err(ProvisioningFailure::Unavailable)
        }
    }
}

fn now_unix_nanos_i64() -> Result<i64, ()> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_error| ())?
        .as_nanos();
    i64::try_from(nanos).map_err(|_error| ())
}

#[derive(Default)]
struct CallbackQuery {
    state: Option<String>,
    code: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

fn parse_query(raw: &str) -> Result<CallbackQuery, ()> {
    let mut query = CallbackQuery::default();
    query::scan(raw, |name, value| {
        let target = match name {
            "state" => &mut query.state,
            "code" => &mut query.code,
            "error" => &mut query.error,
            "error_description" => &mut query.error_description,
            _ => return,
        };
        *target = Some(value.into_owned());
    })?;
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

/// Rebuilds the client callback this login was started for.
///
/// `redirect_uri` is only ever sealed after `authorize`'s `registered_redirect`
/// matched it against a registered URI and parsed it under
/// [`BrowserRedirect`](coral_outbound::BrowserRedirect), so
/// re-applying `BrowserRedirect` here would not add a check — it would only
/// move where the same policy was applied. [`TrustedRedirect::parse`] carries
/// the reason the `None` arm is checked rather than unwrapped.
fn trusted_redirect(session: &SignInSession) -> Option<TrustedRedirect> {
    TrustedRedirect::parse(
        &session.request.redirect_uri,
        session.request.client_state.clone(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use axum::body::to_bytes;
    use axum::http::{HeaderValue, StatusCode, header};

    use serde_json::{Value, json};
    use tracing_subscriber::layer::SubscriberExt as _;
    use url::{Url, form_urlencoded};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::AuthorizationServerHttpState;
    use super::super::test_state::{database, http_state};
    use super::*;
    use crate::auth::id_token::tests::{
        claims as id_token_claims, rsa_key, set_claim, token as id_token,
    };
    use crate::auth::sealed_handshake::ClientRequest;
    use crate::auth::test_config::{PROVIDER_SECRET, resolved_settings};
    use coral_auth::authorization_codes::{AuthorizationCodes, CodeRedemption};
    use coral_db::Database;
    use coral_test_support::TestDatabase;

    const OIDC_STATE: &str = "oidc-state";
    const CLIENT_STATE: &str = "client&error=injected";
    const REDIRECT_URI: &str = "https://client.example.test/callback?tenant=one";
    const PROVIDER_CODE: &str = "provider-code-secret";
    const RESOURCE: &str = "https://api.example.test/mcp";
    const UPSTREAM_SUBJECT: &str = "raw-principal";
    const BOUND_ISSUER: &str = "https://already.bound.example.test";

    fn state(database: &TestDatabase, issuer: &str) -> AuthorizationServerHttpState {
        http_state(database, issuer, "", &[RESOURCE])
    }

    /// A database with no schema, standing in for one Coral cannot write.
    async fn unmigrated_database() -> TestDatabase {
        TestDatabase::new(&coral_db::MigrationPlan::shared(&[]))
            .await
            .expect("test database")
    }

    /// Redeems the code a successful callback returned and reports the identity
    /// the authorization record binds it to.
    async fn stored_user_id(database: &Arc<Database>, response: &Response) -> String {
        let code = redirect_query(response)
            .into_iter()
            .find(|(key, _value)| key == "code")
            .expect("authorization code")
            .1;
        redeem(database, &code)
            .await
            .expect("authorization")
            .user_id
    }

    /// Redeems `code` with the bindings of the seeded sign-in.
    async fn redeem(database: &Arc<Database>, code: &str) -> Option<AuthorizationGrant> {
        let session = session();
        AuthorizationCodes::new(database.clone())
            .redeem(
                code,
                CodeRedemption {
                    client_id: &session.request.client_id,
                    redirect_uri: REDIRECT_URI,
                    code_challenge: &session.request.code_challenge,
                    resource: RESOURCE,
                },
                now_unix_nanos_i64().expect("clock"),
            )
            .await
            .expect("code storage")
    }

    fn session() -> SignInSession {
        SignInSession {
            request: ClientRequest {
                client_id: "https://client.example.test/oauth/client.json".into(),
                redirect_uri: REDIRECT_URI.into(),
                client_state: Some(CLIENT_STATE.into()),
                code_challenge: "client-code-challenge".into(),
                resource: RESOURCE.into(),
            },
            client_name: None,
        }
    }

    /// The nonce Coral expects in the ID token for the sign-in named `OIDC_STATE`.
    fn nonce() -> String {
        resolved_settings("https://unused.invalid", "")
            .0
            .handshake_keys()
            .nonce(OIDC_STATE)
    }

    /// The PKCE verifier Coral sends the provider for the sign-in named `OIDC_STATE`.
    fn pkce_verifier() -> Zeroizing<String> {
        resolved_settings("https://unused.invalid", "")
            .0
            .handshake_keys()
            .pkce_verifier(OIDC_STATE)
    }

    fn query(pairs: &[(&str, &str)]) -> String {
        let mut serializer = form_urlencoded::Serializer::new(String::new());
        for (key, value) in pairs {
            serializer.append_pair(key, value);
        }
        serializer.finish()
    }

    /// The `Cookie` a browser holds after starting the sign-in named by
    /// `oidc_state` for `session`.
    fn browser_cookie(
        state: &AuthorizationServerHttpState,
        oidc_state: &str,
        session: &SignInSession,
    ) -> HeaderValue {
        let now = sign_in_cookie::now_unix_secs().expect("clock");
        let sealed = session
            .seal(state.settings.handshake_keys(), oidc_state, now + 300)
            .expect("sealed session");
        let name = sign_in_cookie::name(oidc_state, now, state.secure_cookies());
        HeaderValue::from_str(&format!("{name}={sealed}")).expect("cookie")
    }

    /// Runs the callback from the browser that started each sign-in `raw` names.
    async fn callback_raw(state: AuthorizationServerHttpState, raw: String) -> Response {
        let mut headers = HeaderMap::new();
        for (name, oidc_state) in form_urlencoded::parse(raw.as_bytes()) {
            if name == "state" {
                headers.append(
                    header::COOKIE,
                    browser_cookie(&state, &oidc_state, &session()),
                );
            }
        }
        oidc_callback(State(state), RawQuery(Some(raw)), headers).await
    }

    async fn callback(state: AuthorizationServerHttpState, pairs: &[(&str, &str)]) -> Response {
        callback_raw(state, query(pairs)).await
    }

    /// Whether `response` removes the sign-in cookie it answered.
    fn clears_sign_in_cookie(response: &Response) -> bool {
        response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .any(|cookie| {
                cookie.as_bytes().starts_with(b"__Host-coral_oauth_signin_")
                    && cookie
                        .to_str()
                        .is_ok_and(|cookie| cookie.contains("Max-Age=0"))
            })
    }

    fn error_code(body: &[u8]) -> String {
        serde_json::from_slice::<Value>(body)
            .expect("JSON error")
            .get("error")
            .and_then(Value::as_str)
            .expect("error code")
            .to_owned()
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
        set_claim(&mut claims, "sub", Value::String(UPSTREAM_SUBJECT.into()));
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
            .respond_with(ResponseTemplate::new(token_status).set_body_json(
                json!({"id_token": id_token(&claims), "detail": "internal-provider-body"}),
            ))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(
                ResponseTemplate::new(jwks_status).set_body_json(
                    json!({"keys": [rsa_key()], "detail": "internal-provider-body"}),
                ),
            )
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn malformed_queries_are_bounded_and_leave_the_sign_in() {
        for raw in query::rejected_queries() {
            let Err(()) = parse_query(&raw) else {
                panic!("malformed callback query must be rejected");
            };
        }
        let database = database().await;
        let response = callback_raw(
            state(&database, "https://provider.invalid"),
            format!("state={OIDC_STATE}&%73tate=duplicate&code=x"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!response.headers().contains_key(header::LOCATION));
        assert_security(&response);
        assert!(
            !clears_sign_in_cookie(&response),
            "a malformed callback leaves the sign-in to finish"
        );
    }

    /// An identity provider link forwarded to another browser carries the
    /// sign-in's `state` but not its cookie. The callback stops before it
    /// contacts the identity provider, so nothing is exchanged or issued for the
    /// wrong browser. Whether a cookie that is present opens is
    /// `SignInSession`'s rule.
    #[tokio::test]
    async fn callback_needs_the_cookie_of_the_browser_that_started_sign_in() {
        let provider = MockServer::start().await;
        let database = database().await;
        let auth_state = state(&database, &provider.uri());
        let own = browser_cookie(&auth_state, OIDC_STATE, &session());
        let mut shadowed = HeaderMap::new();
        shadowed.append(header::COOKIE, own.clone());
        shadowed.append(header::COOKIE, own);
        for (case, headers) in [
            ("no cookie", HeaderMap::new()),
            ("a repeated cookie", shadowed),
        ] {
            let response = oidc_callback(
                State(auth_state.clone()),
                RawQuery(Some(query(&[
                    ("state", OIDC_STATE),
                    ("code", PROVIDER_CODE),
                ]))),
                headers,
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{case}");
            assert!(!response.headers().contains_key(header::LOCATION), "{case}");
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            assert_eq!(error_code(&body), "invalid_request", "{case}");
        }
        assert!(
            provider
                .received_requests()
                .await
                .expect("requests")
                .is_empty(),
            "no provider exchange may start without the starting browser's cookie"
        );
    }

    #[tokio::test]
    async fn provider_errors_are_sanitized_and_end_the_sign_in() {
        let database = database().await;
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
            let response = callback_raw(
                state(&database, "https://provider.invalid"),
                format!("state={state_key}&{suffix}"),
            )
            .await;
            let values = redirect_query(&response);
            assert!(values.contains(&("error".into(), expected.into())));
            assert!(values.contains(&("state".into(), CLIENT_STATE.into())));
            assert!(!values.iter().any(|(_key, value)| value == "secret"));
            assert_security(&response);
            assert!(clears_sign_in_cookie(&response));
        }
        for (index, suffix) in [
            (2, "code=x&error=access_denied"),
            (3, "error="),
            (4, "error_description=orphan"),
            (5, ""),
        ] {
            let state_key = format!("state-{index}");
            let response = callback_raw(
                state(&database, "https://provider.invalid"),
                format!("state={state_key}&{suffix}"),
            )
            .await;
            assert!(
                redirect_query(&response).contains(&("error".into(), "invalid_request".into()))
            );
            assert!(clears_sign_in_cookie(&response));
        }
    }

    #[tokio::test]
    async fn upstream_organization_admission_gates_codes_without_importing_roles() {
        let server = MockServer::start().await;
        let fixture = database().await;
        let db = fixture.database();
        let app = http_state(
            &fixture,
            &server.uri(),
            "[auth.provider.access_token]\naudience = 'environment'\n\
             [auth.provider.access_token.required_claims]\norg_id = 'org_allowed'",
            &[RESOURCE],
        );
        for (organization, accepted) in [("org_other", false), ("org_allowed", true)] {
            server.reset().await;
            mount_provider(&server, 200, 200, &nonce()).await;
            let mut identity = id_token_claims();
            set_claim(&mut identity, "iss", json!(server.uri()));
            set_claim(&mut identity, "nonce", json!(nonce()));
            set_claim(&mut identity, "sub", json!(UPSTREAM_SUBJECT));
            let mut access = identity.clone();
            access.as_object_mut().expect("claims").remove("nonce");
            set_claim(&mut access, "aud", json!("environment"));
            set_claim(&mut access, "org_id", json!(organization));
            set_claim(&mut access, "role", json!("owner"));
            Mock::given(method("POST"))
                .and(path("/token"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "id_token": id_token(&identity), "access_token": id_token(&access),
                })))
                .with_priority(1)
                .mount(&server)
                .await;
            let response = callback(
                app.clone(),
                &[("state", OIDC_STATE), ("code", PROVIDER_CODE)],
            )
            .await;
            if accepted {
                let user_id = stored_user_id(&db, &response).await;
                let authority = coral_workspace::OrganizationModule::new(db.clone())
                    .resolve_user_authority(
                        &coral_workspace::UserId::parse(&user_id).expect("User ID"),
                    )
                    .await
                    .expect("authority");
                assert_eq!(
                    authority.organization_role(),
                    Some(coral_workspace::OrganizationRole::Member)
                );
            } else {
                let values = redirect_query(&response);
                assert!(values.contains(&("error".into(), "server_error".into())));
                assert!(!values.iter().any(|(key, _)| key == "code"));
                assert!(!values.iter().any(|(_, value)| value.contains(organization)));
                assert!(clears_sign_in_cookie(&response));
            }
        }
    }

    #[tokio::test]
    async fn success_is_single_use_and_stores_the_internal_identity_with_bindings() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, &nonce()).await;
        let fixture = database().await;
        let db = fixture.database();
        let app = state(&fixture, &server.uri());
        let raw = query(&[("state", OIDC_STATE), ("code", PROVIDER_CODE)]);
        // The mock provider accepts its code twice, so only Coral's own
        // one-code-per-sign-in guard stops the replay.
        let (first, second) = tokio::join!(
            callback_raw(app.clone(), raw.clone()),
            callback_raw(app, raw)
        );
        let issued_code = |response: &Response| {
            redirect_query(response)
                .iter()
                .any(|(key, _value)| key == "code")
        };
        let (success, replay) = if issued_code(&first) {
            (first, second)
        } else {
            (second, first)
        };
        let replayed = redirect_query(&replay);
        assert!(replayed.contains(&("error".into(), "invalid_request".into())));
        assert!(!replayed.iter().any(|(key, _value)| key == "code"));
        assert_security(&success);
        assert!(
            clears_sign_in_cookie(&success),
            "a finished sign-in removes its cookie"
        );
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
        let authorization = redeem(&db, &code).await.expect("authorization");
        assert_ne!(
            authorization.user_id, UPSTREAM_SUBJECT,
            "the stored code must name Coral's opaque User id, never the upstream subject"
        );
        uuid::Uuid::parse_str(&authorization.user_id).expect("Coral User id is a UUID");
        let user = coral_workspace::OrganizationModule::new(db)
            .resolve_user_authority(
                &coral_workspace::UserId::parse(&authorization.user_id).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            user.organization_role(),
            Some(coral_workspace::OrganizationRole::Member)
        );
        assert_eq!(authorization.resource, RESOURCE);
        let verifier = pkce_verifier();
        let requests = server.received_requests().await.expect("requests");
        for token_request in requests
            .iter()
            .filter(|request| request.url.path() == "/token")
        {
            let form = form_urlencoded::parse(&token_request.body)
                .into_owned()
                .collect::<BTreeMap<_, _>>();
            assert_eq!(
                form.get("code_verifier").map(String::as_str),
                Some(verifier.as_str()),
                "the verifier is derived from the sign-in's state"
            );
        }
        let location = success.headers()[header::LOCATION]
            .to_str()
            .expect("location");
        for secret in [
            verifier.as_str(),
            PROVIDER_SECRET,
            PROVIDER_CODE,
            UPSTREAM_SUBJECT,
        ] {
            assert!(!location.contains(secret));
        }
    }

    /// The configured bootstrap owner reaches the User directory the callback
    /// provisions into, so its first sign-in makes it the Organization owner.
    #[tokio::test]
    async fn the_configured_bootstrap_owner_signs_in_as_the_organization_owner() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, &nonce()).await;
        let fixture = database().await;
        let app = http_state(
            &fixture,
            &server.uri(),
            &format!("bootstrap_owner_subject = '{UPSTREAM_SUBJECT}'"),
            &[RESOURCE],
        );

        let response = callback(app, &[("state", OIDC_STATE), ("code", PROVIDER_CODE)]).await;

        let user_id = stored_user_id(&fixture.database(), &response).await;
        let authority = coral_workspace::OrganizationModule::new(fixture.database())
            .resolve_user_authority(&coral_workspace::UserId::parse(&user_id).expect("User ID"))
            .await
            .expect("authority");
        assert_eq!(
            authority.organization_role(),
            Some(coral_workspace::OrganizationRole::Owner)
        );
    }

    #[tokio::test]
    async fn issuer_mismatch_fails_login_without_logging_the_upstream_identity() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, &nonce()).await;
        let fixture = database().await;
        UserDirectory::new(fixture.database())
            .provision_login(VerifiedOidcLogin::new(
                BOUND_ISSUER,
                UPSTREAM_SUBJECT,
                None,
                1,
            ))
            .await
            .expect("seed the directory");
        let app = state(&fixture, &server.uri());

        let logs = CapturedLogs::default();
        let response = {
            let _guard = tracing::subscriber::set_default(
                tracing_subscriber::Registry::default().with(logs.clone()),
            );
            callback(app, &[("state", OIDC_STATE), ("code", PROVIDER_CODE)]).await
        };

        let values = redirect_query(&response);
        assert!(values.contains(&("error".into(), "access_denied".into())));
        assert!(values.contains(&(
            "error_description".into(),
            ISSUER_MISMATCH_DESCRIPTION.into()
        )));
        assert!(!values.iter().any(|(key, _value)| key == "code"));
        assert_security(&response);

        let captured = logs.captured();
        assert!(
            captured.contains("reconcile"),
            "an operator must be told how to resolve the binding: {captured}"
        );
        for upstream in [BOUND_ISSUER, UPSTREAM_SUBJECT, server.uri().as_str()] {
            assert!(
                !captured.contains(upstream),
                "upstream identity must stay below info level: {captured}"
            );
        }
    }

    #[tokio::test]
    async fn logins_that_cannot_be_provisioned_issue_no_code() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, &nonce()).await;
        let unmigrated = unmigrated_database().await;
        let fixture = database().await;
        let mut app = state(&fixture, &server.uri());
        app.user_directory = UserDirectory::new(unmigrated.database());

        let response = callback(app, &[("state", OIDC_STATE), ("code", PROVIDER_CODE)]).await;

        let values = redirect_query(&response);
        assert!(values.contains(&("error".into(), "server_error".into())));
        assert!(!values.iter().any(|(key, _value)| key == "code"));
        assert_security(&response);
    }

    /// Records events at `info` and above, so a test can prove what a failing
    /// login did *not* put in an operator's logs.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<String>>);

    impl CapturedLogs {
        fn captured(&self) -> String {
            self.0.lock().expect("captured logs").clone()
        }
    }

    impl<S> tracing_subscriber::Layer<S> for CapturedLogs
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            // `Level` orders by verbosity, so `info` and above compare as `<=`.
            if *event.metadata().level() > tracing::Level::INFO {
                return;
            }
            let mut captured = self.0.lock().expect("captured logs");
            event.record(&mut FieldRecorder(&mut captured));
        }
    }

    struct FieldRecorder<'a>(&'a mut String);

    impl tracing::field::Visit for FieldRecorder<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            use std::fmt::Write as _;
            let _result = write!(self.0, " {field}={value:?}");
        }
    }

    async fn assert_provider_failure(token_status: u16, jwks_status: u16, nonce: &str) {
        let server = MockServer::start().await;
        mount_provider(&server, token_status, jwks_status, nonce).await;
        let database = database().await;
        let response = callback(
            state(&database, &server.uri()),
            &[("state", OIDC_STATE), ("code", PROVIDER_CODE)],
        )
        .await;
        let values = redirect_query(&response);
        assert!(values.contains(&("error".into(), "server_error".into())));
        assert!(!values.iter().any(|(key, _value)| key == "code"));
        assert_security(&response);
        let location = response.headers()[header::LOCATION]
            .to_str()
            .expect("location");
        let verifier = pkce_verifier();
        for secret in [
            verifier.as_str(),
            PROVIDER_SECRET,
            PROVIDER_CODE,
            "internal-provider-body",
        ] {
            assert!(!location.contains(secret));
        }
    }

    #[tokio::test]
    async fn exchange_jwks_and_nonce_failures_are_equally_sanitized() {
        assert_provider_failure(500, 200, &nonce()).await;
        assert_provider_failure(200, 500, &nonce()).await;
        assert_provider_failure(200, 200, "wrong-nonce").await;
    }

    #[tokio::test]
    async fn code_storage_failure_exposes_no_client_code() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, &nonce()).await;
        let fixture = database().await;
        let unwritable = unmigrated_database().await;
        let mut state = state(&fixture, &server.uri());
        state.codes = AuthorizationCodes::new(unwritable.database());
        let response = callback(state, &[("state", OIDC_STATE), ("code", PROVIDER_CODE)]).await;
        let values = redirect_query(&response);
        assert!(values.contains(&("error".into(), "server_error".into())));
        assert!(!values.iter().any(|(key, _value)| key == "code"));
        assert_security(&response);
    }

    const ORGANIZATION_RESOURCE: &str = "https://mcp.example.test/mcp";

    /// A sign-in at the Organization MCP Endpoint, by a client with markup in its name.
    fn organization_session() -> SignInSession {
        SignInSession {
            request: ClientRequest {
                resource: ORGANIZATION_RESOURCE.into(),
                ..session().request
            },
            client_name: Some("Claude <b>".into()),
        }
    }

    fn organization_state(database: &TestDatabase, issuer: &str) -> AuthorizationServerHttpState {
        let mut state = state(database, issuer);
        state.workspace_resource_family = Some(Arc::new(
            crate::WorkspaceMcpUrls::new(
                crate::CanonicalOauthUrl::parse("https://mcp.example.test").unwrap(),
            )
            .unwrap(),
        ));
        state
    }

    /// The User the mock provider's identity signs in as, provisioned ahead of
    /// the callback so the test can give them Workspaces.
    async fn signed_in_user(
        database: &Arc<Database>,
        issuer: &str,
    ) -> coral_workspace::AuthenticatedUser {
        let Ok(LoginProvisioning::Provisioned(user_id)) = UserDirectory::new(database.clone())
            .provision_login(VerifiedOidcLogin::new(
                issuer,
                UPSTREAM_SUBJECT,
                None,
                now_unix_nanos_i64().unwrap(),
            ))
            .await
        else {
            panic!("provision the signed-in User");
        };
        coral_workspace::OrganizationModule::new(database.clone())
            .resolve_user_authority(&user_id)
            .await
            .unwrap()
    }

    async fn create_workspace(
        database: &Arc<Database>,
        user: &coral_workspace::AuthenticatedUser,
        name: &str,
    ) -> coral_workspace::WorkspaceId {
        coral_workspace::WorkspaceModule::new(database.clone())
            .create(
                &coral_workspace::ChangeContext::new(
                    user.clone(),
                    now_unix_nanos_i64().unwrap(),
                    coral_workspace::TraceId::current_or_new(),
                ),
                coral_workspace::WorkspaceDisplayName::parse(name).unwrap(),
            )
            .await
            .unwrap()
            .id()
            .clone()
    }

    /// Finishes the provider sign-in of an Organization MCP Endpoint request.
    async fn organization_callback(state: &AuthorizationServerHttpState) -> Response {
        let mut headers = HeaderMap::new();
        headers.append(
            header::COOKIE,
            browser_cookie(state, OIDC_STATE, &organization_session()),
        );
        oidc_callback(
            State(state.clone()),
            RawQuery(Some(query(&[
                ("state", OIDC_STATE),
                ("code", PROVIDER_CODE),
            ]))),
            headers,
        )
        .await
    }

    /// The page a picker response renders and the approval cookie it binds.
    async fn picker(response: Response) -> (String, String) {
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.headers().contains_key(header::LOCATION));
        let cookie = response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|cookie| cookie.to_str().ok())
            .find(|cookie| cookie.starts_with("__Host-coral_oauth_approval="))
            .and_then(|cookie| cookie.split(';').next())
            .map(str::to_owned)
            .unwrap_or_default();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (String::from_utf8(body.to_vec()).unwrap(), cookie)
    }

    fn ticket(page: &str) -> String {
        page.split("name=\"ticket\" value=\"")
            .nth(1)
            .and_then(|tail| tail.split('"').next())
            .expect("picker ticket")
            .to_owned()
    }

    async fn choose(
        state: &AuthorizationServerHttpState,
        cookie: &str,
        fields: &[(&str, &str)],
    ) -> Response {
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/oauth/workspace")
            .header(
                header::ORIGIN,
                state.settings.authorization_server().issuer(),
            )
            .header(header::COOKIE, cookie)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(axum::body::Body::from(query(fields)))
            .unwrap();
        super::super::workspace_choice::oauth_workspace_post(State(state.clone()), request).await
    }

    async fn redeem_organization(
        database: &Arc<Database>,
        response: &Response,
    ) -> Option<AuthorizationGrant> {
        let code = redirect_query(response)
            .into_iter()
            .find(|(key, _value)| key == "code")?
            .1;
        let session = organization_session();
        AuthorizationCodes::new(database.clone())
            .redeem(
                &code,
                CodeRedemption {
                    client_id: &session.request.client_id,
                    redirect_uri: REDIRECT_URI,
                    code_challenge: &session.request.code_challenge,
                    resource: ORGANIZATION_RESOURCE,
                },
                now_unix_nanos_i64().unwrap(),
            )
            .await
            .unwrap()
    }

    /// A sign-in at `/mcp` issues no code until its User picks one of their
    /// active Workspaces; the code then carries that choice.
    #[tokio::test]
    async fn an_organization_sign_in_issues_its_code_for_the_workspace_the_user_picks() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, &nonce()).await;
        let fixture = database().await;
        let db = fixture.database();
        let app = organization_state(&fixture, &server.uri());
        let user = signed_in_user(&db, &server.uri()).await;
        let beta = create_workspace(&db, &user, "Beta").await;
        let alpha = create_workspace(&db, &user, "Alpha").await;

        let (page, cookie) = picker(organization_callback(&app).await).await;
        assert!(
            page.contains("Connect <bdi>Claude &lt;b&gt;</bdi> to Coral"),
            "the person approves the client on the page where they pick"
        );
        assert!(
            page.contains("<p class=\"account\"><span>Signed in as <bdi>"),
            "the account is named"
        );
        assert!(page.contains("action=\"/oauth/workspace\""));
        assert!(page.contains("client.example.test:443"), "redirect shown");
        assert!(page.contains("Claude &lt;b&gt;"), "client text is escaped");
        assert!(!page.contains("Claude <b>"));
        assert!(
            page.contains(&format!("value=\"{alpha}\" required>")) && !page.contains(" checked"),
            "nothing is preselected, so the person must pick"
        );
        assert!(
            page.contains("<span class=\"choice-meta\"></span>"),
            "a role that every choice shares is not shown, and neither is an empty provider list"
        );
        assert!(
            page.contains("role=\"radiogroup\" aria-labelledby=\"decision-title\""),
            "the choices are one labelled group"
        );

        let ticket = ticket(&page);
        let chosen = choose(
            &app,
            &cookie,
            &[
                ("ticket", &ticket),
                ("decision", "continue"),
                ("workspace", &beta.to_string()),
            ],
        )
        .await;
        assert!(
            redirect_query(&chosen).contains(&("state".into(), CLIENT_STATE.into())),
            "the client's state comes back"
        );
        let grant = redeem_organization(&db, &chosen).await.expect("code");
        assert_eq!(grant.workspace_id, Some(beta.clone()));
        assert_eq!(grant.resource, ORGANIZATION_RESOURCE);

        let replayed = choose(
            &app,
            &cookie,
            &[
                ("ticket", &ticket),
                ("decision", "continue"),
                ("workspace", &alpha.to_string()),
            ],
        )
        .await;
        assert!(redirect_query(&replayed).contains(&("error".into(), "invalid_request".into())));
        assert_eq!(redeem_organization(&db, &replayed).await, None);
    }

    /// Another Organization member, who owns Workspaces the signed-in User
    /// may or may not belong to.
    async fn stranger(db: &Arc<Database>) -> coral_workspace::AuthenticatedUser {
        let stranger = coral_workspace::AuthenticatedUser::organization_member(
            coral_workspace::UserId::parse("stranger").unwrap(),
            coral_workspace::OrganizationRole::Member,
        );
        coral_workspace::OrganizationModule::new(db.clone())
            .provision_login(&coral_workspace::ChangeContext::new(
                stranger.clone(),
                1,
                coral_workspace::TraceId::current_or_new(),
            ))
            .await
            .unwrap();
        stranger
    }

    /// A role tells the choices apart only when the User holds different
    /// roles; the happy-path test covers a User who owns every choice.
    #[tokio::test]
    async fn roles_show_when_they_tell_the_choices_apart() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, &nonce()).await;
        let fixture = database().await;
        let db = fixture.database();
        let app = organization_state(&fixture, &server.uri());
        let user = signed_in_user(&db, &server.uri()).await;
        create_workspace(&db, &user, "Owned").await;
        let stranger = stranger(&db).await;
        let shared = create_workspace(&db, &stranger, "Shared").await;
        coral_workspace::WorkspaceModule::new(db.clone())
            .set_membership(
                &coral_workspace::ChangeContext::new(
                    stranger,
                    now_unix_nanos_i64().unwrap(),
                    coral_workspace::TraceId::current_or_new(),
                ),
                &shared,
                &user,
                coral_workspace::WorkspaceRole::Member,
            )
            .await
            .unwrap();
        let (page, _cookie) = picker(organization_callback(&app).await).await;
        assert!(page.contains("<span class=\"choice-meta\">Member</span>"));
        assert!(page.contains("<span class=\"choice-meta\">Owner</span>"));
    }

    /// The choice is rechecked when it is submitted. A refused choice issues
    /// nothing, so the same page can still finish with an allowed one.
    #[tokio::test]
    async fn a_refused_workspace_choice_issues_no_code() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, &nonce()).await;
        let fixture = database().await;
        let db = fixture.database();
        let app = organization_state(&fixture, &server.uri());
        let user = signed_in_user(&db, &server.uri()).await;
        let kept = create_workspace(&db, &user, "Kept").await;
        let stranger = stranger(&db).await;
        let foreign = create_workspace(&db, &stranger, "Foreign").await;
        let (page, cookie) = picker(organization_callback(&app).await).await;
        let ticket = ticket(&page);

        for (case, decision, workspace) in [
            (
                "a Workspace without a Membership",
                "continue",
                foreign.to_string(),
            ),
            ("not a Workspace ID", "continue", "not-an-id".to_owned()),
            ("Cancel", "cancel", kept.to_string()),
        ] {
            let refused = choose(
                &app,
                &cookie,
                &[
                    ("ticket", &ticket),
                    ("decision", decision),
                    ("workspace", &workspace),
                ],
            )
            .await;
            assert!(
                redirect_query(&refused).contains(&("error".into(), "access_denied".into())),
                "{case}"
            );
            assert_eq!(redeem_organization(&db, &refused).await, None, "{case}");
        }
        let malformed = choose(
            &app,
            &cookie,
            &[("ticket", &ticket), ("decision", "continue")],
        )
        .await;
        assert_eq!(
            malformed.status(),
            StatusCode::BAD_REQUEST,
            "no Workspace named"
        );
        let chosen = choose(
            &app,
            &cookie,
            &[
                ("ticket", &ticket),
                ("decision", "continue"),
                ("workspace", &kept.to_string()),
            ],
        )
        .await;
        assert_eq!(
            redeem_organization(&db, &chosen)
                .await
                .and_then(|grant| grant.workspace_id),
            Some(kept.clone())
        );
    }

    /// With no active Workspace there is nothing to pick, so no code is issued
    /// and the page says how to get one.
    #[tokio::test]
    async fn a_user_without_workspaces_is_told_to_create_one() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, &nonce()).await;
        let fixture = database().await;
        let db = fixture.database();
        let app = organization_state(&fixture, &server.uri());
        let (page, cookie) = picker(organization_callback(&app).await).await;
        assert!(page.contains("No workspace to choose</span></h2>"));
        assert!(
            page.contains("<p class=\"account\"><span>Signed in as <bdi>"),
            "the account is named, since signing in with another may help"
        );
        assert!(!page.contains("name=\"workspace\""));
        assert!(
            page.contains("formnovalidate>Back to <bdi>Claude &lt;b&gt;</bdi></button>"),
            "the person can end the sign-in at the client"
        );
        let back = choose(
            &app,
            &cookie,
            &[("ticket", &ticket(&page)), ("decision", "cancel")],
        )
        .await;
        assert!(
            redirect_query(&back).contains(&("error".into(), "access_denied".into())),
            "the client stops waiting"
        );
        assert_eq!(redeem_organization(&db, &back).await, None);
    }
}
