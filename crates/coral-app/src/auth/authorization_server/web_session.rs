//! Reef's Web Sessions, held by the authorization server.
//!
//! Reef starts a session with the refresh token its sign-in returned, and
//! from then on keeps only the session handle: the server holds the Refresh
//! Grant and mints Reef's access tokens on request. Minting writes nothing, so
//! any number of Reef processes resolve one session at once without
//! coordinating. Ending a session ends its grant.
//!
//! `POST /oauth/session` takes `refresh_token`, `client_id` and `max_age`
//! (seconds) and answers `201 {session, expires_in}`.
//! `POST /oauth/session/token` takes `session` and `client_id` and answers
//! `200 {access_token, token_type, expires_in, session_expires_in}`.
//! `POST /oauth/session/end` takes `session` and `client_id` and answers an
//! empty `200`, whether or not the session existed.
//!
//! `invalid_client` means the client did not authenticate as it registered.
//! `invalid_grant` means the client must sign in again. `503` means storage
//! did not answer and the request may be retried: none of these requests
//! changes anything that a retry could repeat. `500` means a retry fails the
//! same way: a token could not be signed, or stored state does not decode.

use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use coral_auth::oauth_clients::{AuthenticatedClient, ClientAuthenticationError};
use coral_auth::user_tokens::TokenRefusal;
use coral_telemetry::safe;
use zeroize::Zeroizing;

use super::AuthorizationServerHttpState;
use super::client_authentication::{HeaderCredential, authenticate};
use super::response::security_headers;
use super::token::{
    FormError, MAX_BODY_BYTES, TokenError, TokenForm, expires_in, json_response, parse_form,
    token_error, valid_content_type,
};

pub(super) async fn start(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    let (client, form) = match read_form(&state, request).await {
        Ok(read) => read,
        Err(response) => return *response,
    };
    let (Some(refresh_token), Some(max_age)) = (
        present(form.refresh_token.as_ref()),
        form.max_age.as_deref().and_then(|value| session_age(value)),
    ) else {
        return token_error(TokenError::InvalidRequest);
    };
    let now = state.clock.now();
    match state
        .tokens
        .start_web_session(refresh_token, &client, max_age, now.get())
        .await
    {
        Ok(started) => json_response(
            StatusCode::CREATED,
            &serde_json::json!({
                "session": started.handle.expose(),
                "expires_in": seconds_until(started.expires_at_unix_nanos, now),
            }),
        ),
        Err(refusal) => refused(refusal),
    }
}

pub(super) async fn token(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    let (client, form) = match read_form(&state, request).await {
        Ok(read) => read,
        Err(response) => return *response,
    };
    let Some(session) = present(form.session.as_ref()) else {
        return token_error(TokenError::InvalidRequest);
    };
    let now = state.clock.now();
    match state
        .tokens
        .web_session_token(session, &client, now.get())
        .await
    {
        Ok(minted) => json_response(
            StatusCode::OK,
            &serde_json::json!({
                "access_token": minted.access_token.access_token,
                "token_type": "Bearer",
                "expires_in": expires_in(&minted.access_token, now),
                "session_expires_in": seconds_until(minted.session_expires_at_unix_nanos, now),
            }),
        ),
        Err(refusal) => refused(refusal),
    }
}

pub(super) async fn end(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    let (client, form) = match read_form(&state, request).await {
        Ok(read) => read,
        Err(response) => return *response,
    };
    let Some(session) = present(form.session.as_ref()) else {
        return token_error(TokenError::InvalidRequest);
    };
    match state.tokens.end_web_session(session, &client).await {
        Ok(()) => (StatusCode::OK, security_headers()).into_response(),
        Err(refusal) => refused(refusal),
    }
}

/// Reads a bounded OAuth form and authenticates the client it names, or
/// returns the response refusing it.
async fn read_form(
    state: &AuthorizationServerHttpState,
    request: Request,
) -> Result<(AuthenticatedClient, TokenForm), Box<Response>> {
    let header = HeaderCredential::parse(request.headers())?;
    if !valid_content_type(request.headers()) {
        return Err(Box::new(token_error(TokenError::InvalidRequest)));
    }
    let Ok(body) = to_bytes(request.into_body(), MAX_BODY_BYTES).await else {
        return Err(Box::new(token_error(TokenError::InvalidRequest)));
    };
    let body = Zeroizing::new(Vec::from(body));
    let mut form = parse_form(&body).map_err(|error| {
        Box::new(token_error(match error {
            FormError::Malformed | FormError::InvalidScope => TokenError::InvalidRequest,
        }))
    })?;
    let client = authenticate(state, header, &mut form, |error| {
        refused(match error {
            ClientAuthenticationError::Corrupt => TokenRefusal::Corrupt(error.code()),
            _ => TokenRefusal::Unavailable,
        })
    })
    .await?;
    Ok((client, form))
}

fn present(value: Option<&Zeroizing<String>>) -> Option<&str> {
    value
        .map(|value| value.as_str())
        .filter(|value| !value.is_empty())
}

/// A positive whole number of seconds. `UserTokens` bounds the session.
fn session_age(value: &str) -> Option<Duration> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let seconds = value.parse::<u64>().ok().filter(|seconds| *seconds > 0)?;
    Some(Duration::from_secs(seconds))
}

/// Whole seconds from `now` until `deadline`, rounded up: a session with any
/// time left reports at least one second.
fn seconds_until(deadline_unix_nanos: i64, now: coral_db::UnixNanos) -> i64 {
    let remaining = deadline_unix_nanos.saturating_sub(now.get()).max(0);
    remaining / 1_000_000_000 + i64::from(remaining % 1_000_000_000 != 0)
}

fn refused(refusal: TokenRefusal) -> Response {
    if refusal == TokenRefusal::InvalidGrant {
        return json_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({
                "error": "invalid_grant",
                "error_description": "the session or grant is invalid or expired",
            }),
        );
    }
    tracing::warn!(
        error.type = safe::kind(refusal.code()),
        "web session endpoint could not answer"
    );
    // Storage that did not answer may answer a retry. A failed signature or
    // stored state that does not decode will not, so each reports as the
    // token endpoint does.
    let unavailable = "the session is temporarily unavailable";
    let (status, description) = match refusal {
        TokenRefusal::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, unavailable),
        TokenRefusal::Corrupt(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "the session request failed",
        ),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, unavailable),
    };
    json_response(
        status,
        &serde_json::json!({
            "error": "server_error",
            "error_description": description,
        }),
    )
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    reason = "tests index the responses they just received"
)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::extract::State;
    use axum::http::{Request, StatusCode, header};
    use serde_json::Value;
    use url::form_urlencoded;

    use super::super::token::tests::{
        CLIENT, confidential_sign_in, request, send, signed_in, state,
    };
    use super::{end, start, token};

    fn form(pairs: &[(&str, &str)]) -> Request<Body> {
        request(
            form_urlencoded::Serializer::new(String::new())
                .extend_pairs(pairs)
                .finish(),
        )
    }

    /// The status and body of `response`, which must never be cached.
    async fn read(response: axum::response::Response) -> (StatusCode, String) {
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let status = response.status();
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    /// A session starts from a Reef refresh token, mints Reef tokens, and
    /// ends; malformed requests map to `invalid_request` without changing
    /// anything, and an ended session to `invalid_grant`.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one table of malformed starts, then one session's whole life"
    )]
    async fn a_session_starts_mints_and_ends_over_the_wire() {
        let (state, _codes, _session, database) = state().await;
        let issued = signed_in(&state, &database, Some("reef")).await;
        let refresh = issued["refresh_token"].as_str().unwrap();
        for (pairs, expected) in [
            (
                vec![("client_id", CLIENT), ("max_age", "3600")],
                "invalid_request",
            ),
            (
                vec![("refresh_token", refresh), ("max_age", "3600")],
                "invalid_request",
            ),
            (
                vec![("refresh_token", refresh), ("client_id", CLIENT)],
                "invalid_request",
            ),
            (
                vec![
                    ("refresh_token", refresh),
                    ("client_id", CLIENT),
                    ("max_age", "0"),
                ],
                "invalid_request",
            ),
            (
                vec![
                    ("refresh_token", refresh),
                    ("client_id", CLIENT),
                    ("max_age", "-5"),
                ],
                "invalid_request",
            ),
            (
                vec![
                    ("refresh_token", refresh),
                    ("client_id", CLIENT),
                    ("max_age", "18446744073709551616"),
                ],
                "invalid_request",
            ),
            (
                vec![
                    ("refresh_token", refresh),
                    ("client_id", CLIENT),
                    ("max_age", "3600"),
                    ("client_secret", "secret"),
                ],
                "invalid_client",
            ),
        ] {
            let (status, body) = read(start(State(state.clone()), form(&pairs)).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{pairs:?}");
            let value: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(value["error"], expected, "{pairs:?}");
        }

        let (status, body) = read(
            start(
                State(state.clone()),
                form(&[
                    ("refresh_token", refresh),
                    ("client_id", CLIENT),
                    ("max_age", "3600"),
                ]),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let started: Value = serde_json::from_str(&body).unwrap();
        let session = started["session"].as_str().unwrap();
        assert!(started["expires_in"].as_i64().unwrap() > 3590);

        let (status, body) = read(
            token(
                State(state.clone()),
                form(&[("session", session), ("client_id", CLIENT)]),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let minted: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(minted["token_type"], "Bearer");
        assert!(minted["expires_in"].as_u64().unwrap() > 0);
        assert!(minted["session_expires_in"].as_i64().unwrap() > 3590);

        let (status, _body) =
            read(token(State(state.clone()), form(&[("client_id", CLIENT)])).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        for _ in 0..2 {
            let (status, body) = read(
                end(
                    State(state.clone()),
                    form(&[("session", session), ("client_id", CLIENT)]),
                )
                .await,
            )
            .await;
            assert_eq!((status, body.as_str()), (StatusCode::OK, ""));
        }
        let (status, body) = read(
            token(
                State(state.clone()),
                form(&[("session", session), ("client_id", CLIENT)]),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["error"], "invalid_grant");
        assert!(!body.contains(session));
    }

    /// Storage that does not answer is a 503, which the client may retry. A
    /// stored grant that does not decode fails every retry, so it is a 500.
    /// Both are `server_error`, and neither spends the token or ends the
    /// session.
    #[tokio::test]
    async fn an_outage_is_a_503_and_a_corrupt_grant_a_500() {
        async fn server_error(response: axum::response::Response) -> StatusCode {
            let (status, body) = read(response).await;
            let value: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(value["error"], "server_error", "{body}");
            status
        }
        let (state, _codes, _session, database) = state().await;
        let issued = signed_in(&state, &database, Some("reef")).await;
        let refresh = issued["refresh_token"].as_str().unwrap();
        let pairs = [
            ("refresh_token", refresh),
            ("client_id", CLIENT),
            ("max_age", "3600"),
        ];
        // (fault, repair, status), with an outage of `table`.
        let faults = |table: &str| {
            [
                (
                    format!("ALTER TABLE {table} RENAME TO away"),
                    format!("ALTER TABLE away RENAME TO {table}"),
                    StatusCode::SERVICE_UNAVAILABLE,
                ),
                (
                    "UPDATE oauth_refresh_grants SET private_audience = 'neither'".to_owned(),
                    "UPDATE oauth_refresh_grants SET private_audience = 'reef'".to_owned(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                ),
            ]
        };
        for (fault, repair, expected) in faults("oauth_refresh_tokens") {
            database.execute_fixture_sql(&fault).await.unwrap();
            let status = server_error(start(State(state.clone()), form(&pairs)).await).await;
            assert_eq!(status, expected, "{fault}");
            database.execute_fixture_sql(&repair).await.unwrap();
        }
        let (status, body) = read(start(State(state.clone()), form(&pairs)).await).await;
        assert_eq!(status, StatusCode::CREATED, "no refusal spent it: {body}");
        let started: Value = serde_json::from_str(&body).unwrap();
        let session = [
            ("session", started["session"].as_str().unwrap()),
            ("client_id", CLIENT),
        ];
        for (fault, repair, expected) in faults("oauth_web_sessions") {
            database.execute_fixture_sql(&fault).await.unwrap();
            let status = server_error(token(State(state.clone()), form(&session)).await).await;
            assert_eq!(status, expected, "{fault}");
            let status = server_error(end(State(state.clone()), form(&session)).await).await;
            assert_eq!(status, expected, "{fault}");
            database.execute_fixture_sql(&repair).await.unwrap();
        }
        let (status, body) = read(token(State(state.clone()), form(&session)).await).await;
        assert_eq!(status, StatusCode::OK, "no refusal ended it: {body}");
    }

    /// A confidential client's refresh token starts no session without the
    /// client's secret, and the refusal leaves its grant live.
    #[tokio::test]
    async fn a_session_start_authenticates_a_confidential_client_first() {
        let (state, _codes, _session, database) = state().await;
        let sign_in = confidential_sign_in(&state, &database).await;
        let token = sign_in.refresh_token(&state).await;
        let pairs = [
            ("refresh_token", token.as_str()),
            ("client_id", sign_in.client.as_str()),
            ("max_age", "3600"),
        ];
        let (status, body) = read(start(State(state.clone()), form(&pairs)).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["error"], "invalid_client");
        let refresh = form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("grant_type", "refresh_token"),
                ("refresh_token", token.as_str()),
                ("client_id", sign_in.client.as_str()),
            ])
            .finish();
        let (status, _headers, body) = send(state, request(sign_in.with_secret(&refresh))).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "the refusal left the grant live: {body}"
        );
    }
}
