use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::{SecureRandom as _, SystemRandom};
use zeroize::Zeroizing;

use super::super::id_token::ValidatedOidcIdentity;
use super::super::state_store::{OAuthAuthorizationCodeRecord, OAuthAuthorizationSessionRecord};
use super::response::{TrustedRedirect, direct_error};
use super::{AuthorizationServerHttpState, query, sign_in_cookie};
use coral_auth::{LoginProvisioning, UserDirectory, VerifiedOidcLogin};

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
    // A browser that did not start this sign-in has no cookie for it. Stop
    // before the take, so the handshake stays for the browser that did.
    let secure_cookies = state.secure_cookies();
    let Some(binding) = sign_in_cookie::binding(&headers, oidc_state, secure_cookies) else {
        return invalid_state();
    };
    let mut response = match state
        .session_store
        .take_authorization_session(oidc_state, &binding)
        .await
    {
        Ok(Some(session)) => complete(&state, &query, session).await,
        _ => invalid_state(),
    };
    if let Some(clear) = sign_in_cookie::clear_header(oidc_state, secure_cookies) {
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
    session: OAuthAuthorizationSessionRecord,
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
        .exchange_code(provider, &provider_code, &session.oidc_code_verifier)
        .await
    {
        Ok(exchange) => exchange,
        Err(error) => {
            tracing::warn!(%error, "OIDC callback could not exchange the provider code");
            return trusted.error("server_error", "authorization failed");
        }
    };
    let identity = match state
        .provider_client
        .validate_code_exchange(provider, exchange, &session.oidc_nonce)
        .await
    {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(%error, "OIDC callback could not validate the provider identity");
            return trusted.error("server_error", "authorization failed");
        }
    };
    let user_id = match provision(state.user_directory.as_ref(), &identity).await {
        Ok(user_id) => user_id,
        Err(outcome) => return outcome.into_error(&trusted),
    };
    let Ok(authorization_code) = random_code() else {
        tracing::warn!("OIDC callback could not generate an authorization code");
        return trusted.error("server_error", "authorization failed");
    };
    let authorization = OAuthAuthorizationCodeRecord {
        user_id,
        client_id: session.client_id,
        redirect_uri: session.redirect_uri,
        code_challenge: session.code_challenge,
        resource: session.resource,
    };
    if let Err(error) = state
        .code_store
        .store_authorization_code(&authorization_code, authorization)
        .await
    {
        tracing::warn!(%error, "OIDC callback could not store the authorization code");
        return trusted.error("server_error", "authorization failed");
    }
    trusted.success(&authorization_code)
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
    user_directory: Option<&UserDirectory>,
    identity: &ValidatedOidcIdentity,
) -> Result<String, ProvisioningFailure> {
    let Some(user_directory) = user_directory else {
        tracing::warn!("OIDC callback has no User directory to provision a verified login into");
        return Err(ProvisioningFailure::Unavailable);
    };
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
        Ok(LoginProvisioning::Provisioned(user_id)) => Ok(user_id.to_string()),
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
/// `redirect_uri` is only ever stored after `authorize`'s `registered_redirect`
/// matched it against a registered URI and parsed it under
/// [`BrowserRedirect`](coral_outbound::BrowserRedirect), so
/// re-applying `BrowserRedirect` here would not add a check — it would only
/// move where the same policy was applied. [`TrustedRedirect::parse`] carries
/// the reason the `None` arm is checked rather than unwrapped.
fn trusted_redirect(session: &OAuthAuthorizationSessionRecord) -> Option<TrustedRedirect> {
    TrustedRedirect::parse(&session.redirect_uri, session.client_state.clone())
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
    use super::super::test_state::http_state;
    use super::*;
    use crate::auth::id_token::tests::{
        claims as id_token_claims, rsa_key, set_claim, token as id_token,
    };
    use crate::auth::state_store::{
        CodeStore, InMemoryStateStore, OAuthSignInBinding, SessionStore, StateStoreError,
    };
    use crate::auth::test_config::PROVIDER_SECRET;
    use coral_db::Database;
    use coral_test_support::TestDatabase;

    const OIDC_STATE: &str = "oidc-state";
    const CLIENT_STATE: &str = "client&error=injected";
    const REDIRECT_URI: &str = "https://client.example.test/callback?tenant=one";
    const VERIFIER: &str = "stored-upstream-verifier";
    const NONCE: &str = "stored-upstream-nonce";
    const PROVIDER_CODE: &str = "provider-code-secret";
    const RESOURCE: &str = "https://api.example.test/mcp";
    const UPSTREAM_SUBJECT: &str = "raw-principal";
    const BOUND_ISSUER: &str = "https://already.bound.example.test";

    fn state(issuer: &str, store: Arc<InMemoryStateStore>) -> AuthorizationServerHttpState {
        http_state(issuer, "", store, &[RESOURCE])
    }

    fn state_with(
        issuer: &str,
        store: Arc<InMemoryStateStore>,
        database: Arc<Database>,
    ) -> AuthorizationServerHttpState {
        let mut state = state(issuer, store);
        state.user_directory = Some(UserDirectory::new(database));
        state
    }

    /// Opens the migrated database a callback provisions logins into.
    async fn database() -> (Arc<Database>, TestDatabase) {
        let (database, fixture) = database_with(coral_db::migration_plan()).await;
        (database, fixture)
    }

    /// A database with no schema, standing in for one Coral cannot write.
    async fn unmigrated_database() -> (Arc<Database>, TestDatabase) {
        database_with(&coral_db::MigrationPlan::shared(&[])).await
    }

    async fn database_with(plan: &coral_db::MigrationPlan) -> (Arc<Database>, TestDatabase) {
        let fixture = TestDatabase::new(plan).await.expect("test database");
        (fixture.database(), fixture)
    }

    /// Redeems the code a successful callback returned and reports the identity
    /// the authorization record binds it to.
    async fn stored_user_id(store: &dyn CodeStore, response: &Response) -> String {
        let code = redirect_query(response)
            .into_iter()
            .find(|(key, _value)| key == "code")
            .expect("authorization code")
            .1;
        store
            .take_authorization_code_for_request(
                &code,
                &session().client_id,
                REDIRECT_URI,
                &session().code_challenge,
                RESOURCE,
            )
            .await
            .expect("store")
            .expect("authorization")
            .user_id
    }

    fn session() -> OAuthAuthorizationSessionRecord {
        OAuthAuthorizationSessionRecord {
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

    /// The binding of the browser that started every seeded sign-in.
    fn binding() -> OAuthSignInBinding {
        OAuthSignInBinding::from_bytes([0x24; 32])
    }

    /// The `Cookie` a browser sends back after the sign-in named by
    /// `oidc_state` handed it the cookie for `binding`.
    fn browser_cookie(
        state: &AuthorizationServerHttpState,
        oidc_state: &str,
        binding: &OAuthSignInBinding,
    ) -> HeaderValue {
        let set_cookie = sign_in_cookie::set_header(oidc_state, binding, state.secure_cookies())
            .expect("sign-in cookie");
        let pair = set_cookie.to_str().expect("ASCII").split(';').next();
        HeaderValue::from_str(pair.expect("name and value")).expect("cookie")
    }

    /// Runs the callback from the browser that started each sign-in `raw` names.
    async fn callback_raw(state: AuthorizationServerHttpState, raw: String) -> Response {
        let mut headers = HeaderMap::new();
        for (name, oidc_state) in form_urlencoded::parse(raw.as_bytes()) {
            if name == "state" {
                headers.append(
                    header::COOKIE,
                    browser_cookie(&state, &oidc_state, &binding()),
                );
            }
        }
        oidc_callback(State(state), RawQuery(Some(raw)), headers).await
    }

    async fn callback(state: AuthorizationServerHttpState, pairs: &[(&str, &str)]) -> Response {
        callback_raw(state, query(pairs)).await
    }

    async fn seed(store: &dyn SessionStore, key: &str) {
        store
            .store_authorization_session(key, &binding(), session())
            .await
            .expect("store");
    }

    async fn take(store: &dyn SessionStore, key: &str) -> Option<OAuthAuthorizationSessionRecord> {
        store
            .take_authorization_session(key, &binding())
            .await
            .expect("store")
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
    async fn malformed_queries_are_bounded_and_do_not_consume_state() {
        for raw in query::rejected_queries() {
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

    /// A provider link forwarded to another browser carries the sign-in's
    /// `state` but not its cookie. The callback stops before it takes the
    /// handshake or contacts the provider, so the browser that started the
    /// sign-in can still finish it.
    #[tokio::test]
    async fn callback_needs_the_cookie_of_the_browser_that_started_sign_in() {
        let provider = MockServer::start().await;
        let store = Arc::new(InMemoryStateStore::new());
        seed(store.as_ref(), OIDC_STATE).await;
        let auth_state = state(&provider.uri(), store.clone());
        let other_browser = || {
            browser_cookie(
                &auth_state,
                OIDC_STATE,
                &OAuthSignInBinding::from_bytes([0x99; 32]),
            )
        };
        let mut shadowed = HeaderMap::new();
        shadowed.append(
            header::COOKIE,
            browser_cookie(&auth_state, OIDC_STATE, &binding()),
        );
        shadowed.append(header::COOKIE, other_browser());
        for (case, headers) in [
            ("no cookie", HeaderMap::new()),
            (
                "another browser's cookie",
                HeaderMap::from_iter([(header::COOKIE, other_browser())]),
            ),
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
        assert!(take(store.as_ref(), OIDC_STATE).await.is_some());
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
    async fn upstream_organization_admission_gates_codes_without_importing_roles() {
        let server = MockServer::start().await;
        let store = Arc::new(InMemoryStateStore::new());
        let (db, _temp) = database().await;
        let mut app = http_state(
            &server.uri(),
            "[auth.provider.access_token]\naudience = 'environment'\n\
             [auth.provider.access_token.required_claims]\norg_id = 'org_allowed'",
            store.clone(),
            &[RESOURCE],
        );
        app.user_directory = Some(UserDirectory::new(db.clone()));
        for (organization, accepted) in [("org_other", false), ("org_allowed", true)] {
            server.reset().await;
            mount_provider(&server, 200, 200, NONCE).await;
            let mut identity = id_token_claims();
            set_claim(&mut identity, "iss", json!(server.uri()));
            set_claim(&mut identity, "nonce", json!(NONCE));
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
            seed(store.as_ref(), OIDC_STATE).await;
            let response = callback(
                app.clone(),
                &[("state", OIDC_STATE), ("code", PROVIDER_CODE)],
            )
            .await;
            if accepted {
                let user_id = stored_user_id(store.as_ref(), &response).await;
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
                assert!(take(store.as_ref(), OIDC_STATE).await.is_none());
            }
        }
    }

    #[tokio::test]
    async fn success_is_single_use_and_stores_the_internal_identity_with_bindings() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, NONCE).await;
        let store = Arc::new(InMemoryStateStore::new());
        let (db, _temp) = database().await;
        seed(store.as_ref(), OIDC_STATE).await;
        let app = state_with(&server.uri(), store.clone(), db.clone());
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
        let cleared = success.headers()[header::SET_COOKIE]
            .to_str()
            .expect("ASCII");
        assert!(
            cleared.starts_with("__Host-coral_oauth_signin_") && cleared.contains("Max-Age=0"),
            "a finished sign-in removes its cookie: {cleared}"
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
        for secret in [VERIFIER, PROVIDER_SECRET, PROVIDER_CODE, UPSTREAM_SUBJECT] {
            assert!(!location.contains(secret));
        }
    }

    #[tokio::test]
    async fn issuer_mismatch_fails_login_without_logging_the_upstream_identity() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, NONCE).await;
        let store = Arc::new(InMemoryStateStore::new());
        let (db, _temp) = database().await;
        UserDirectory::new(db.clone())
            .provision_login(VerifiedOidcLogin::new(
                BOUND_ISSUER,
                UPSTREAM_SUBJECT,
                None,
                1,
            ))
            .await
            .expect("seed the directory");
        seed(store.as_ref(), OIDC_STATE).await;
        let mut app = state_with(&server.uri(), store, db);
        app.code_store = Arc::new(RejectCodeStore);

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
        mount_provider(&server, 200, 200, NONCE).await;
        let (unmigrated, _temp) = unmigrated_database().await;
        for user_directory in [None, Some(UserDirectory::new(unmigrated))] {
            let store = Arc::new(InMemoryStateStore::new());
            seed(store.as_ref(), OIDC_STATE).await;
            let mut app = state(&server.uri(), store);
            app.user_directory = user_directory;
            app.code_store = Arc::new(RejectCodeStore);

            let response = callback(app, &[("state", OIDC_STATE), ("code", PROVIDER_CODE)]).await;

            let values = redirect_query(&response);
            assert!(values.contains(&("error".into(), "server_error".into())));
            assert!(!values.iter().any(|(key, _value)| key == "code"));
            assert_security(&response);
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

    /// Code store that fails the test if an unprovisioned login reaches storage.
    struct RejectCodeStore;

    #[async_trait::async_trait]
    impl CodeStore for RejectCodeStore {
        async fn store_authorization_code(
            &self,
            _code: &str,
            _authorization: OAuthAuthorizationCodeRecord,
        ) -> Result<(), StateStoreError> {
            unreachable!("a login that was not provisioned must not store an authorization code")
        }

        async fn take_authorization_code_for_request(
            &self,
            _code: &str,
            _client_id: &str,
            _redirect_uri: &str,
            _challenge: &str,
            _resource: &str,
        ) -> Result<Option<OAuthAuthorizationCodeRecord>, StateStoreError> {
            unreachable!("the OIDC callback issues authorization codes, it never redeems them")
        }
    }

    async fn assert_provider_failure(token_status: u16, jwks_status: u16, nonce: &str) {
        let server = MockServer::start().await;
        mount_provider(&server, token_status, jwks_status, nonce).await;
        let store = Arc::new(InMemoryStateStore::new());
        seed(store.as_ref(), OIDC_STATE).await;
        let response = callback(
            state(&server.uri(), store),
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
        for secret in [
            VERIFIER,
            PROVIDER_SECRET,
            PROVIDER_CODE,
            "internal-provider-body",
        ] {
            assert!(!location.contains(secret));
        }
    }

    #[tokio::test]
    async fn exchange_jwks_and_nonce_failures_are_equally_sanitized() {
        assert_provider_failure(500, 200, NONCE).await;
        assert_provider_failure(200, 500, NONCE).await;
        assert_provider_failure(200, 200, "wrong-nonce").await;
    }

    /// Code store whose writes always fail, standing in for a full store.
    struct FailCodeStore;

    #[async_trait::async_trait]
    impl CodeStore for FailCodeStore {
        async fn store_authorization_code(
            &self,
            _code: &str,
            _authorization: OAuthAuthorizationCodeRecord,
        ) -> Result<(), StateStoreError> {
            Err(StateStoreError::CapacityExceeded { max_entries: 0 })
        }

        async fn take_authorization_code_for_request(
            &self,
            _code: &str,
            _client_id: &str,
            _redirect_uri: &str,
            _challenge: &str,
            _resource: &str,
        ) -> Result<Option<OAuthAuthorizationCodeRecord>, StateStoreError> {
            unreachable!("the OIDC callback issues authorization codes, it never redeems them")
        }
    }

    #[tokio::test]
    async fn code_store_capacity_failure_exposes_no_client_code() {
        let server = MockServer::start().await;
        mount_provider(&server, 200, 200, NONCE).await;
        let store = Arc::new(InMemoryStateStore::new());
        let (db, _temp) = database().await;
        seed(store.as_ref(), OIDC_STATE).await;
        let mut state = state_with(&server.uri(), store, db);
        state.code_store = Arc::new(FailCodeStore);
        let response = callback(state, &[("state", OIDC_STATE), ("code", PROVIDER_CODE)]).await;
        let values = redirect_query(&response);
        assert!(values.contains(&("error".into(), "server_error".into())));
        assert!(!values.iter().any(|(key, _value)| key == "code"));
        assert_security(&response);
    }
}
