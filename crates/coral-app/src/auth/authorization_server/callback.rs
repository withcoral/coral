use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::{SecureRandom as _, SystemRandom};
use zeroize::Zeroizing;

use super::super::sealed_handshake::SignInSession;
use super::response::{TrustedRedirect, body_too_large, direct_error, direct_refusal, see_other};
use super::{AuthorizationServerHttpState, sign_in_cookie};
use coral_auth::authorization_codes::{AuthorizationGrant, CodeIssue};
use coral_auth::{LoginProvisioning, UserDirectory, VerifiedLogin};
use coral_db::UnixNanos;
use coral_sign_in::{ModeRequest, Named, PersonIdentifier, ProvenPerson, SignInValue};
use coral_telemetry::safe;
use coral_workspace::UserId;

/// A GET at one of the sign-in mode's URLs.
pub(super) async fn mode_get(
    State(state): State<AuthorizationServerHttpState>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let request = ModeRequest {
        query: raw_query.as_deref().unwrap_or_default(),
        body: b"",
    };
    finish(&state, &request, &headers).await
}

/// A POST at one of the sign-in mode's URLs. The route limits its body to
/// the mode's limit.
pub(super) async fn mode_post(
    State(state): State<AuthorizationServerHttpState>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    // A body that cannot be read is refused before any cookie is found, so
    // the sign-in stays.
    let body = match body {
        Ok(body) => body,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return body_too_large();
        }
        Err(_) => return direct_error("invalid_request", "request body could not be read"),
    };
    let request = ModeRequest {
        query: raw_query.as_deref().unwrap_or_default(),
        body: &body,
    };
    finish(&state, &request, &headers).await
}

/// Finishes the sign-in that `request`, at one of the sign-in mode's URLs,
/// names.
async fn finish(
    state: &AuthorizationServerHttpState,
    request: &ModeRequest<'_>,
    headers: &HeaderMap,
) -> Response {
    // The sign-in is live if it has not expired when its answer arrives.
    let arrived = state.clock.now();
    let sign_in = match state.sign_in_mode.names(request, arrived) {
        Named::SignIn(sign_in) => sign_in,
        // The web interface begins a sign-in for the person the browser
        // brings; with none, the mode's refusal is the answer.
        Named::NoSignIn(refusal) => {
            return match state.settings.authorization_server().fresh_sign_in() {
                Some(page) => see_other(page),
                None => direct_refusal(&refusal),
            };
        }
        Named::Refused(refusal) => return direct_refusal(&refusal),
    };
    // A browser that did not start this sign-in has no cookie for it.
    let attributes = state.sign_in_cookie();
    let Some((cookie_name, sealed)) = sign_in_cookie::find(headers, sign_in.as_str(), attributes)
    else {
        return direct_refusal(&state.sign_in_mode.no_sign_in_cookie(arrived));
    };
    let session = SignInSession::open(
        state.settings.handshake_keys(),
        sign_in.as_str(),
        &sealed,
        arrived.whole_seconds(),
    );
    let mut response = match session {
        Some(session) => complete(state, request, &sign_in, session, headers).await,
        None => direct_refusal(&state.sign_in_mode.no_sign_in_cookie(arrived)),
    };
    if let Some(clear) = sign_in_cookie::clear_header(&cookie_name, attributes) {
        response.headers_mut().append(header::SET_COOKIE, clear);
    }
    response
}

/// Finishes the sign-in `session` describes with the mode's answer in
/// `request`.
async fn complete(
    state: &AuthorizationServerHttpState,
    request: &ModeRequest<'_>,
    sign_in: &SignInValue,
    session: SignInSession,
    headers: &HeaderMap,
) -> Response {
    let Some(trusted) = trusted_redirect(&session) else {
        return direct_error("server_error", "authorization failed");
    };
    let person = match state
        .sign_in_mode
        .check(request, sign_in, state.clock.as_ref())
        .await
    {
        Ok(person) => person,
        Err(refusal) => return trusted.refused(&refusal),
    };
    // Read after the mode's last answer, `now` dates everything the sign-in
    // decides from here: the login, the Workspace choice and the code.
    let now = state.clock.now();
    let user_id = match provision(&state.user_directory, &person, now).await {
        Ok(user_id) => user_id,
        Err(outcome) => return outcome.into_error(&trusted),
    };
    // At the Organization MCP Endpoint the User picks the Workspace first;
    // the picker's submission issues the code.
    if state
        .authorization_resource(&session.request.resource)
        .is_some_and(|target| target.picks_workspace())
    {
        let signed_in_as = person.email().or(person.display_name());
        return super::workspace_choice::offer(
            state,
            headers,
            &trusted,
            sign_in.as_str(),
            user_id,
            signed_in_as,
            session,
            now,
        )
        .await;
    }
    issue_code(
        state,
        &trusted,
        sign_in.as_str(),
        &session.request.grant(&user_id, None),
        now,
    )
    .await
}

/// Stores a new code for `grant`, issued at `now` by the sign-in named
/// `sign_in`, and returns the client redirect that carries it. A sign-in
/// that already issued its code, or a code that cannot be stored, is refused
/// with the client's error redirect.
pub(super) async fn issue_code(
    state: &AuthorizationServerHttpState,
    trusted: &TrustedRedirect,
    sign_in: &str,
    grant: &AuthorizationGrant,
    now: UnixNanos,
) -> Response {
    let Ok(authorization_code) = random_token() else {
        tracing::warn!("Sign-in could not generate an authorization code");
        return trusted.error("server_error", "authorization failed");
    };
    match state
        .codes
        .issue(&authorization_code, sign_in, grant, now)
        .await
    {
        Ok(CodeIssue::Issued) => trusted.success(&authorization_code),
        Ok(CodeIssue::SignInAlreadyUsed) => {
            trusted.error("invalid_request", "authorization was already completed")
        }
        Err(error) => {
            tracing::warn!(
                error.type = safe::kind(error.code()),
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
/// The login is recorded at `now`.
async fn provision(
    user_directory: &UserDirectory,
    person: &ProvenPerson,
    now: UnixNanos,
) -> Result<UserId, ProvisioningFailure> {
    let login = match person.identifier() {
        PersonIdentifier::Subject { issuer, subject } => {
            VerifiedLogin::oidc(issuer, subject, person.display_name(), now)
        }
        PersonIdentifier::NameId(value) => {
            VerifiedLogin::name_id(value, person.display_name(), now)
        }
    }
    .with_profile(coral_workspace::VerifiedUserProfile::new(
        person.name(),
        person.email(),
        person.email_verified(),
        person.hosted_domain(),
    ));
    let login = match person.observed_at() {
        Some(observed_at) => login.with_profile_observed_at(observed_at),
        None => login,
    };
    let provisioning = user_directory.provision_login(login).await;
    match provisioning {
        Ok(LoginProvisioning::Provisioned(user_id)) => Ok(user_id),
        Ok(LoginProvisioning::Disabled) => Err(ProvisioningFailure::Disabled),
        Ok(LoginProvisioning::IssuerMismatch) => {
            tracing::warn!(
                "Sign-in refused a login whose subject is already bound to a different \
                 provider issuer; reconcile that user directory row before this login can succeed"
            );
            Err(ProvisioningFailure::IssuerMismatch)
        }
        Err(error) => {
            tracing::warn!(
                error.type = safe::kind(provisioning_error_code(&error)),
                "Sign-in could not provision the verified login"
            );
            Err(ProvisioningFailure::Unavailable)
        }
    }
}

/// The kind of a login that could not be recorded, for an event's
/// `error.type`. A database failure keeps the database's own code, so an
/// outage does not read as a corrupt directory row. The error's text can
/// quote stored data and is never read.
const fn provisioning_error_code(error: &coral_auth::Error) -> &'static str {
    match error {
        coral_auth::Error::CorruptData { .. } => "corrupt_data",
        coral_auth::Error::Database(error) => error.code(),
        coral_auth::Error::Organization(error) => match error.database_error() {
            Some(error) => error.code(),
            None => "organization",
        },
    }
}

/// A fresh URL-safe 256-bit token: a sign-in's value, or an
/// authorization code.
pub(super) fn random_token() -> Result<String, ()> {
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

    use axum::body::{Bytes, to_bytes};
    use axum::http::{HeaderValue, StatusCode, header};

    use serde_json::{Value, json};
    use tracing_subscriber::layer::SubscriberExt as _;
    use url::{Url, form_urlencoded};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::AuthorizationServerHttpState;
    use super::super::test_state::{authenticated, database, http_state};
    use super::*;
    use crate::auth::id_token::tests::{
        claims_at as id_token_claims_at, rsa_key, set_claim, token as id_token,
    };
    use crate::auth::sealed_handshake::ClientRequest;
    use crate::auth::test_config::{PROVIDER_SECRET, resolved_settings};
    use coral_auth::authorization_codes::{
        AUTHORIZATION_CODE_TTL, AuthorizationCodes, CodeRedemption,
    };
    use coral_db::{Clock as _, Database};
    use coral_test_support::ManualClock;
    use coral_test_support::TestDatabase;
    use std::time::Duration;

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

    /// The claims of an ID token the provider issues on the system clock,
    /// which the state these tests build reads.
    fn id_token_claims() -> Value {
        id_token_claims_at(UnixNanos::new(coral_test_support::now_unix_nanos()))
    }

    /// Redeems `code` on the system clock with the bindings of the seeded
    /// sign-in.
    async fn redeem(database: &Arc<Database>, code: &str) -> Option<AuthorizationGrant> {
        redeem_at(
            database,
            code,
            UnixNanos::new(coral_test_support::now_unix_nanos()),
        )
        .await
    }

    /// Redeems `code` at `now` with the bindings of the seeded sign-in.
    async fn redeem_at(
        database: &Arc<Database>,
        code: &str,
        now: UnixNanos,
    ) -> Option<AuthorizationGrant> {
        let session = session();
        let client = authenticated(database.clone(), &session.request.client_id).await;
        AuthorizationCodes::new(database.clone())
            .redeem(
                code,
                CodeRedemption {
                    client: &client,
                    redirect_uri: REDIRECT_URI,
                    code_challenge: &session.request.code_challenge,
                    resource: RESOURCE,
                },
                now,
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
                refresh_eligible: false,
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
        sign_in: &str,
        session: &SignInSession,
    ) -> HeaderValue {
        let now = state.clock.now().whole_seconds();
        let sealed = session
            .seal(state.settings.handshake_keys(), sign_in, now + 300)
            .expect("sealed session");
        let name = sign_in_cookie::name(sign_in, now, state.sign_in_cookie());
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
        mode_get(State(state), RawQuery(Some(raw)), headers).await
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

    fn error_description(body: &[u8]) -> Option<String> {
        serde_json::from_slice::<Value>(body)
            .expect("JSON error")
            .get("error_description")
            .and_then(Value::as_str)
            .map(str::to_owned)
    }

    /// The description `response` sends: in the client's redirect, or in
    /// the body of a direct answer.
    async fn sent_description(response: Response) -> Option<String> {
        if response.headers().contains_key(header::LOCATION) {
            return redirect_query(&response)
                .into_iter()
                .find(|(key, _)| key == "error_description")
                .map(|(_, value)| value);
        }
        error_description(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
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

    /// Mounts a provider whose ID token is issued on the system clock.
    async fn mount_provider(server: &MockServer, token_status: u16, jwks_status: u16, nonce: &str) {
        mount_provider_with(server, token_status, jwks_status, nonce, id_token_claims()).await;
    }

    /// Mounts a provider that answers a code with an ID token of `claims`,
    /// for this server, sign-in and subject.
    async fn mount_provider_with(
        server: &MockServer,
        token_status: u16,
        jwks_status: u16,
        nonce: &str,
        mut claims: Value,
    ) {
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
    async fn a_malformed_callback_leaves_the_sign_in() {
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
            let response = mode_get(
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
            // The OIDC mode's answer, which core sends as it is.
            assert_eq!(
                error_description(&body).as_deref(),
                Some("OIDC callback state is invalid or expired"),
                "{case}"
            );
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

    /// The server's clock alone dates a sign-in: the five minutes its cookie
    /// lasts, the provider's tokens, and the five minutes its code lasts.
    /// The clock is in 2100, so a reading of the host's clock anywhere on the
    /// way fails here.
    #[tokio::test]
    async fn the_servers_clock_dates_a_sign_in_and_the_code_it_issues() {
        let server = MockServer::start().await;
        let fixture = database().await;
        let clock = ManualClock::starting_at(UnixNanos::new(4_102_444_800 * 1_000_000_000));
        let mut app = state(&fixture, &server.uri());
        app.clock = Arc::new(clock.clone());
        let mut headers = HeaderMap::new();
        headers.append(header::COOKIE, browser_cookie(&app, OIDC_STATE, &session()));
        let callback = || {
            mode_get(
                State(app.clone()),
                RawQuery(Some(query(&[
                    ("state", OIDC_STATE),
                    ("code", PROVIDER_CODE),
                ]))),
                headers.clone(),
            )
        };

        // The last second of the sign-in's five minutes. The provider issues
        // its token then.
        clock.advance(Duration::from_secs(sign_in_cookie::MAX_AGE_SECONDS - 1));
        let issued_at = clock.now();
        mount_provider_with(&server, 200, 200, &nonce(), id_token_claims_at(issued_at)).await;
        let finished = callback().await;
        let code = redirect_query(&finished)
            .into_iter()
            .find(|(key, _value)| key == "code")
            .expect("authorization code")
            .1;
        let expires_at = issued_at.saturating_add(AUTHORIZATION_CODE_TTL);
        assert!(
            redeem_at(&fixture.database(), &code, expires_at)
                .await
                .is_none(),
            "the code has expired five minutes after the callback"
        );
        let last_instant = expires_at
            .checked_sub(Duration::from_nanos(1))
            .expect("an earlier instant");
        assert!(
            redeem_at(&fixture.database(), &code, last_instant)
                .await
                .is_some(),
            "the code lasts until then"
        );

        // A second later the sign-in is over.
        clock.advance(Duration::from_secs(1));
        let expired = callback().await;
        assert_eq!(expired.status(), StatusCode::BAD_REQUEST);
        assert!(!expired.headers().contains_key(header::LOCATION));
        let body = to_bytes(expired.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(error_code(&body), "invalid_request");
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
            let (response, captured) = callback_with_logs(
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
                assert_logged_only_the_code(
                    &captured,
                    "access_token_required_claim",
                    &[organization, UPSTREAM_SUBJECT, &server.uri()],
                );
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
            .provision_login(VerifiedLogin::oidc(
                BOUND_ISSUER,
                UPSTREAM_SUBJECT,
                None,
                UnixNanos::new(1),
            ))
            .await
            .expect("seed the directory");
        let app = state(&fixture, &server.uri());

        let (response, captured) =
            callback_with_logs(app, &[("state", OIDC_STATE), ("code", PROVIDER_CODE)]).await;

        let values = redirect_query(&response);
        assert!(values.contains(&("error".into(), "access_denied".into())));
        assert!(values.contains(&(
            "error_description".into(),
            ISSUER_MISMATCH_DESCRIPTION.into()
        )));
        assert!(!values.iter().any(|(key, _value)| key == "code"));
        assert_security(&response);

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

        let (response, captured) =
            callback_with_logs(app, &[("state", OIDC_STATE), ("code", PROVIDER_CODE)]).await;

        let values = redirect_query(&response);
        assert!(values.contains(&("error".into(), "server_error".into())));
        assert!(!values.iter().any(|(key, _value)| key == "code"));
        assert_security(&response);
        // The database's own code tells an outage from a corrupt directory row.
        assert_logged_only_the_code(&captured, "sqlx", &[UPSTREAM_SUBJECT, &server.uri()]);
    }

    /// A corrupt directory row has its own code, a database failure keeps the
    /// database's code whichever module met it, and any other Organization
    /// failure is `organization`.
    #[test]
    fn a_login_that_cannot_be_recorded_is_logged_by_its_kind() {
        for (error, code) in [
            (
                coral_auth::Error::CorruptData("row text".into()),
                "corrupt_data",
            ),
            (
                coral_auth::Error::Database(coral_db::Error::WriterBusy),
                "writer_busy",
            ),
            (
                coral_auth::Error::Organization(coral_db::Error::WriterBusy.into()),
                "writer_busy",
            ),
            (
                coral_auth::Error::Organization(coral_workspace::Error::WorkspaceNotFound),
                "organization",
            ),
        ] {
            assert_eq!(provisioning_error_code(&error), code);
        }
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

    /// Runs the callback and returns its response beside what it logged at
    /// `info` and above.
    async fn callback_with_logs(
        state: AuthorizationServerHttpState,
        pairs: &[(&str, &str)],
    ) -> (Response, String) {
        with_logs(callback(state, pairs)).await
    }

    /// Awaits `response`, and returns it with what the operator's log got
    /// meanwhile.
    async fn with_logs(response: impl Future<Output = Response>) -> (Response, String) {
        let logs = CapturedLogs::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(logs.clone()),
        );
        let response = Box::pin(response).await;
        (response, logs.captured())
    }

    /// Asserts that `captured` names the failure as `error_type`, once, and
    /// holds none of `provider_data`.
    fn assert_logged_only_the_code(captured: &str, error_type: &str, provider_data: &[&str]) {
        assert_eq!(
            captured.matches(" error.type=").count(),
            1,
            "a failed sign-in is logged once: {captured}"
        );
        assert!(
            captured.contains(&format!(" error.type={error_type:?}")),
            "the operator's log must name the failure as {error_type}: {captured}"
        );
        for data in provider_data {
            assert!(
                !captured.contains(data),
                "provider data must stay out of the log: {captured}"
            );
        }
    }

    /// Fails the sign-in at the provider, asserts that the log names the
    /// failure as `error_type`, and returns the query of the redirect the
    /// browser gets.
    async fn assert_provider_failure(
        token_status: u16,
        jwks_status: u16,
        nonce: &str,
        error_type: &str,
    ) -> Vec<(String, String)> {
        let server = MockServer::start().await;
        mount_provider(&server, token_status, jwks_status, nonce).await;
        let database = database().await;
        let (response, captured) = callback_with_logs(
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
        let secrets = [
            verifier.as_str(),
            PROVIDER_SECRET,
            PROVIDER_CODE,
            "internal-provider-body",
        ];
        for secret in secrets {
            assert!(!location.contains(secret));
        }
        let issuer = server.uri();
        assert_logged_only_the_code(
            &captured,
            error_type,
            &[&secrets[..], &[UPSTREAM_SUBJECT, &issuer, nonce][..]].concat(),
        );
        values
    }

    /// The operator's log tells the failures apart. The browser's redirect
    /// does not: it is the same fixed error for all of them.
    #[tokio::test]
    async fn exchange_jwks_and_nonce_failures_are_equally_sanitized() {
        let exchange = assert_provider_failure(500, 200, &nonce(), "token_exchange").await;
        let jwks = assert_provider_failure(200, 500, &nonce(), "jwks").await;
        let rejected = assert_provider_failure(200, 200, "wrong-nonce", "id_token_nonce").await;
        let fixed_error = [
            ("tenant", "one"),
            ("error", "server_error"),
            ("error_description", "authorization failed"),
            ("state", CLIENT_STATE),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned()));
        for values in [exchange, jwks, rejected] {
            assert_eq!(values, fixed_error);
        }
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
                refresh_eligible: false,
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
            .provision_login(VerifiedLogin::oidc(
                issuer,
                UPSTREAM_SUBJECT,
                None,
                UnixNanos::new(coral_test_support::now_unix_nanos()),
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
                    UnixNanos::new(coral_test_support::now_unix_nanos()),
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
        mode_get(
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
            .map_or_default(str::to_owned);
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
        let client = authenticated(database.clone(), &session.request.client_id).await;
        AuthorizationCodes::new(database.clone())
            .redeem(
                &code,
                CodeRedemption {
                    client: &client,
                    redirect_uri: REDIRECT_URI,
                    code_challenge: &session.request.code_challenge,
                    resource: ORGANIZATION_RESOURCE,
                },
                UnixNanos::new(coral_test_support::now_unix_nanos()),
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
                UnixNanos::new(1),
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
                    UnixNanos::new(coral_test_support::now_unix_nanos()),
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

    /// The state over `database` with `mode` in place of the OIDC sign-in.
    fn mode_state(
        database: &TestDatabase,
        mode: Arc<dyn coral_sign_in::SignInMode>,
    ) -> AuthorizationServerHttpState {
        super::super::test_state::http_state_with_mode(
            database,
            "https://provider.invalid",
            "",
            &[RESOURCE],
            mode,
        )
    }

    /// Runs the mode's answer `raw` from the browser that started the
    /// sign-in `sign_in`, and returns what the operator's log got.
    async fn mode_answer(
        state: AuthorizationServerHttpState,
        sign_in: &str,
        raw: &str,
    ) -> (Response, String) {
        let mut headers = HeaderMap::new();
        headers.append(header::COOKIE, browser_cookie(&state, sign_in, &session()));
        with_logs(mode_get(
            State(state),
            RawQuery(Some(raw.to_owned())),
            headers,
        ))
        .await
    }

    /// Core finishes a sign-in that a mode other than OIDC proves: it finds
    /// the cookie the mode's answer names, and makes the User by the person
    /// the mode returns.
    #[tokio::test]
    async fn a_sign_in_finishes_through_the_configured_mode() {
        let database = database().await;
        // Each cookie is found and cleared with the mode's `SameSite`.
        for (settings, sign_in, person, same_site) in [
            ("", "value-1", "00u1", "SameSite=Lax"),
            ("same_site = 'none'", "value-2", "00u2", "SameSite=None"),
        ] {
            let app = mode_state(&database, super::super::test_state::test_mode(settings));
            let answer = format!("sign_in={sign_in}&person={person}");
            let (response, _logs) = mode_answer(app, sign_in, &answer).await;
            assert!(
                response
                    .headers()
                    .get_all(header::SET_COOKIE)
                    .iter()
                    .any(|cookie| cookie.to_str().is_ok_and(|cookie| {
                        cookie.contains("Max-Age=0;") && cookie.ends_with(same_site)
                    })),
                "{settings}: {response:?}"
            );
            let user_id = stored_user_id(&database.database(), &response).await;
            let again = UserDirectory::new(database.database())
                .provision_login(VerifiedLogin::name_id(
                    person,
                    None,
                    UnixNanos::new(coral_test_support::now_unix_nanos()),
                ))
                .await
                .expect("provision");
            assert_eq!(
                again,
                LoginProvisioning::Provisioned(UserId::parse(&user_id).expect("User ID")),
                "{settings}"
            );
        }
    }

    /// Core sends each refusal of a mode with its OAuth error and a text that
    /// is fixed or in the checked form, and logs one the mode described. A
    /// refusal of the check ends the sign-in. A request that the mode refuses
    /// before it names a sign-in, one that names none in a deployment with no
    /// web interface, and one whose sign-in has no cookie in this browser
    /// each get the mode's refusal directly, and leave the browser's sign-in.
    #[tokio::test]
    async fn each_refusal_of_a_mode_gets_its_answer() {
        let database = database().await;
        let epoch = UnixNanos::new(0);
        let form = "sign_in_refused 1970-01-01T00:00:00Z";
        for (refusal, error, description, logged) in [
            (
                coral_sign_in::Refusal::new("sign_in_refused", epoch),
                "access_denied",
                form,
                Some("sign_in_refused"),
            ),
            (
                coral_sign_in::Refusal::new("Not a code", epoch),
                "access_denied",
                coral_sign_in::DENIED,
                None,
            ),
            (
                coral_sign_in::Refusal::failed("mode_failed"),
                "server_error",
                "authorization failed",
                None,
            ),
        ] {
            let app = mode_state(
                &database,
                Arc::new(super::super::test_state::Refusing(refusal)),
            );
            let (response, logs) = mode_answer(app, "value-1", "value-1").await;
            let values = redirect_query(&response);
            assert!(
                values.contains(&("error".into(), error.into())),
                "{values:?}"
            );
            assert!(
                values.contains(&("error_description".into(), description.into())),
                "{values:?}"
            );
            assert!(clears_sign_in_cookie(&response));
            match logged {
                Some(code) => assert_logged_only_the_code(&logs, code, &["value-1"]),
                None => assert!(!logs.contains(" error.type="), "{logs}"),
            }
            assert!(!values.iter().any(|(key, _)| key == "code"), "{values:?}");
        }

        let mut app = mode_state(&database, super::super::test_state::test_mode(""));
        app.clock = Arc::new(ManualClock::starting_at(epoch));
        for (raw, error, description, logged) in [
            (
                "sign_in=value-1&sign_in=value-2",
                "access_denied",
                "test_malformed 1970-01-01T00:00:00Z",
                Some("test_malformed"),
            ),
            (
                "person=00u1",
                "access_denied",
                "test_not_requested 1970-01-01T00:00:00Z",
                Some("test_not_requested"),
            ),
            (
                "sign_in=value-2&person=00u1",
                "access_denied",
                "test_no_sign_in 1970-01-01T00:00:00Z",
                Some("test_no_sign_in"),
            ),
        ] {
            let (response, logs) = mode_answer(app.clone(), "value-1", raw).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{raw}");
            assert!(!response.headers().contains_key(header::LOCATION), "{raw}");
            assert!(!clears_sign_in_cookie(&response), "{raw}");
            let body: Value = serde_json::from_slice(
                &to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("body"),
            )
            .expect("JSON error");
            assert_eq!(
                (
                    body.get("error").and_then(Value::as_str),
                    body.get("error_description").and_then(Value::as_str)
                ),
                (Some(error), Some(description)),
                "{raw}"
            );
            match logged {
                Some(code) => assert_logged_only_the_code(&logs, code, &["value-1"]),
                None => assert!(!logs.contains(" error.type="), "{raw}: {logs}"),
            }
        }
    }

    /// The first Owner is the person the mode's settings name: they sign in
    /// as the Organization's Owner, and another person does not.
    #[tokio::test]
    async fn the_first_owner_comes_from_the_mode() {
        let database = database().await;
        let app = mode_state(
            &database,
            super::super::test_state::test_mode("first_owner = 'owner-1'"),
        );
        for (sign_in, person, owner) in
            [("value-1", "member-1", false), ("value-2", "owner-1", true)]
        {
            let (response, _logs) = mode_answer(
                app.clone(),
                sign_in,
                &format!("sign_in={sign_in}&person={person}"),
            )
            .await;
            let user_id = stored_user_id(&database.database(), &response).await;
            let authority = coral_workspace::OrganizationModule::new(database.database())
                .resolve_user_authority(&UserId::parse(&user_id).expect("User ID"))
                .await
                .expect("authority");
            assert_eq!(
                authority.organization_role() == Some(coral_workspace::OrganizationRole::Owner),
                owner,
                "{person}"
            );
        }
    }

    /// The test mode's answer for `sign_in`, padded with a form field to
    /// exactly `len` bytes.
    fn posted_answer(sign_in: &str, len: usize) -> String {
        let answer = format!("sign_in={sign_in}&person=00u1&pad=");
        let pad = len.checked_sub(answer.len()).expect("room for the answer");
        format!("{answer}{}", "x".repeat(pad))
    }

    /// POSTs `body` to `path` chunked, so no `Content-Length` announces
    /// its size, and returns the status line.
    async fn post_chunked(
        server: &super::super::RunningCoralAuthorizationServer,
        path: &str,
        cookie: &HeaderValue,
        body: &str,
    ) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let mut stream = tokio::net::TcpStream::connect(server.local_addr())
            .await
            .expect("connect");
        let request = format!(
            "POST {path} HTTP/1.1\r\nhost: {}\r\ncookie: {}\r\n\
             content-type: application/x-www-form-urlencoded\r\n\
             transfer-encoding: chunked\r\nconnection: close\r\n\r\n\
             {:x}\r\n{body}\r\n0\r\n\r\n",
            server.local_addr(),
            cookie.to_str().expect("ASCII"),
            body.len()
        );
        stream.write_all(request.as_bytes()).await.expect("request");
        let mut response = String::new();
        // A guard, not a wait: the server closes the connection at once.
        tokio::time::timeout(
            Duration::from_secs(10),
            stream.read_to_string(&mut response),
        )
        .await
        .expect("the server answers")
        .expect("response");
        response.lines().next().unwrap_or_default().to_owned()
    }

    /// The test mode's POST is read up to its limit. A longer body, with or
    /// without its length announced, gets core's answer before any cookie
    /// is found: the sign-in stays, so the answer of exactly the limit then
    /// finishes it.
    #[tokio::test]
    async fn a_modes_post_is_read_up_to_its_limit() {
        let database = database().await;
        let state = super::super::test_state::test_mode_state(&database, "", "", &[RESOURCE]);
        let cookie = browser_cookie(&state, "value-1", &session());
        let server = super::super::test_state::serve(state).await;
        let url = format!("{}/auth/test/answer", server.endpoint_uri());
        let browser = super::super::test_state::browser();
        let post = |body: String| {
            browser
                .post(&url)
                .header(header::COOKIE, cookie.clone())
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(body)
                .send()
        };
        let limit = coral_sign_in::testing::BODY_LIMIT;

        let over = post(posted_answer("value-1", limit + 1))
            .await
            .expect("post");
        assert_eq!(over.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(!over.headers().contains_key(header::SET_COOKIE));
        let body: Value = over.json().await.expect("JSON error");
        assert_eq!(
            (
                body.get("error").and_then(Value::as_str),
                body.get("error_description").and_then(Value::as_str)
            ),
            (Some("invalid_request"), Some("request body is too large"))
        );
        assert_eq!(
            post_chunked(
                &server,
                "/auth/test/answer",
                &cookie,
                &posted_answer("value-1", limit + 1)
            )
            .await,
            "HTTP/1.1 413 Payload Too Large"
        );

        let at_limit = post(posted_answer("value-1", limit)).await.expect("post");
        assert_eq!(at_limit.status(), StatusCode::FOUND);
        let location = Url::parse(
            at_limit.headers()[header::LOCATION]
                .to_str()
                .expect("location"),
        )
        .expect("client redirect");
        assert!(
            location.query_pairs().any(|(key, _)| key == "code"),
            "{location}"
        );
        server.shutdown().await.expect("shutdown");
    }

    /// A deployment serves the URLs of its own sign-in mode, each with its
    /// methods, and no other mode's.
    #[tokio::test]
    async fn each_deployment_serves_only_its_modes_urls() {
        let database = database().await;
        let test_mode = super::super::test_state::test_mode_state(&database, "", "", &[RESOURCE]);
        let oidc = http_state(&database, "https://provider.invalid", "", &[RESOURCE]);
        for (state, served, unserved) in [
            (test_mode, "/auth/test/answer", "/auth/oidc/callback"),
            (oidc, "/auth/oidc/callback", "/auth/test/answer"),
        ] {
            let server = super::super::test_state::serve(state).await;
            let at = |path: &str| format!("{}{path}", server.endpoint_uri());
            let browser = super::super::test_state::browser();
            // The mode refuses an answer that names no sign-in, but the URL
            // is served.
            let get = browser.get(at(served)).send().await.expect("GET");
            assert_eq!(get.status(), StatusCode::BAD_REQUEST, "{served}");
            let put = browser.put(at(served)).send().await.expect("PUT");
            assert_eq!(put.status(), StatusCode::METHOD_NOT_ALLOWED, "{served}");
            for method in [reqwest::Method::GET, reqwest::Method::POST] {
                let response = browser
                    .request(method.clone(), at(unserved))
                    .send()
                    .await
                    .expect("request");
                assert_eq!(
                    response.status(),
                    StatusCode::NOT_FOUND,
                    "{method} {unserved}"
                );
            }
            server.shutdown().await.expect("shutdown");
        }
    }

    /// A request that names no sign-in, by GET or POST, sends the browser
    /// to the web interface's page that begins one, whatever its scheme. It
    /// carries nothing from the request, logs nothing, and leaves the
    /// browser's sign-in. The mode's other direct answers do not change.
    #[tokio::test]
    async fn a_request_that_names_no_sign_in_goes_to_the_web_interface() {
        let database = database().await;
        for web in ["https://reef.example.test", "http://localhost:5173"] {
            let state = super::super::test_state::test_mode_state(
                &database,
                "",
                &format!("[listeners.web]\npublic_url = '{web}'\n"),
                &[RESOURCE],
            );
            let mut headers = HeaderMap::new();
            headers.append(
                header::COOKIE,
                browser_cookie(&state, "value-1", &session()),
            );
            let unasked = "person=00u1&returnTo=%2Fsettings";
            for (method, response) in [
                (
                    "GET",
                    with_logs(mode_get(
                        State(state.clone()),
                        RawQuery(Some(unasked.to_owned())),
                        headers.clone(),
                    ))
                    .await,
                ),
                (
                    "POST",
                    with_logs(mode_post(
                        State(state.clone()),
                        RawQuery(None),
                        headers.clone(),
                        Ok(Bytes::from(unasked)),
                    ))
                    .await,
                ),
            ] {
                let (response, logs) = response;
                assert_eq!(response.status(), StatusCode::SEE_OTHER, "{web} {method}");
                assert_eq!(
                    response.headers()[header::LOCATION],
                    format!("{web}/login?fresh=1"),
                    "{web} {method}"
                );
                assert_security(&response);
                assert!(
                    !response.headers().contains_key(header::SET_COOKIE),
                    "{web} {method}"
                );
                assert!(logs.is_empty(), "{web} {method}: {logs}");
            }
            for raw in [
                "sign_in=value-1&sign_in=value-2",
                "sign_in=value-2&person=00u1",
            ] {
                let (response, _logs) = mode_answer(state.clone(), "value-1", raw).await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{web} {raw}");
            }
        }
    }

    /// How the browser holds the sign-in's cookie.
    #[derive(Clone, Copy, Debug)]
    enum Held {
        /// As the begin set it.
        Sealed,
        /// Past its five minutes.
        Expired,
        /// Under its name, with no sealed sign-in.
        Unsealed,
    }

    /// Once core finds the sign-in's cookie, every answer clears it: a
    /// refusal of the check, and a cookie that does not open because it
    /// expired or holds no sealed sign-in. None of them issues a code.
    #[tokio::test]
    async fn once_found_the_sign_in_cookie_is_cleared_with_every_answer() {
        let database = database().await;
        for (settings, held, status, description) in [
            (
                "refuse = true",
                Held::Sealed,
                StatusCode::FOUND,
                "test_refused 1970-01-01T00:00:00Z",
            ),
            (
                "",
                Held::Expired,
                StatusCode::BAD_REQUEST,
                "test_no_sign_in 1970-01-01T00:05:01Z",
            ),
            (
                "",
                Held::Unsealed,
                StatusCode::BAD_REQUEST,
                "test_no_sign_in 1970-01-01T00:00:00Z",
            ),
        ] {
            let clock = Arc::new(ManualClock::starting_at(UnixNanos::new(0)));
            let mut app = mode_state(&database, super::super::test_state::test_mode(settings));
            app.clock = clock.clone();
            let name = sign_in_cookie::name("value-1", 0, app.sign_in_cookie());
            let cookie = match held {
                Held::Sealed | Held::Expired => browser_cookie(&app, "value-1", &session()),
                Held::Unsealed => {
                    HeaderValue::from_str(&format!("{name}=unsealed")).expect("cookie")
                }
            };
            if matches!(held, Held::Expired) {
                clock.advance(Duration::from_secs(sign_in_cookie::MAX_AGE_SECONDS + 1));
            }
            let mut headers = HeaderMap::new();
            headers.append(header::COOKIE, cookie);
            let response = mode_get(
                State(app),
                RawQuery(Some("sign_in=value-1&person=00u1".to_owned())),
                headers,
            )
            .await;
            assert_eq!(response.status(), status, "{held:?}");
            assert!(
                response
                    .headers()
                    .get_all(header::SET_COOKIE)
                    .iter()
                    .any(|value| value.as_bytes().starts_with(format!("{name}=;").as_bytes())),
                "{held:?}: the cookie is cleared"
            );
            if status == StatusCode::FOUND {
                assert!(
                    !redirect_query(&response)
                        .iter()
                        .any(|(key, _)| key == "code"),
                    "{held:?}"
                );
            }
            assert_eq!(
                sent_description(response).await.as_deref(),
                Some(description),
                "{held:?}"
            );
        }
    }
}
