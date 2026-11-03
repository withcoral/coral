//! RFC 7009 revocation of refresh tokens.
//!
//! A confidential client authenticates first (RFC 7009 section 2.1), so a
//! leaked refresh token without the client's secret cannot end its grant.
//!
//! Revocation ends the Refresh Grant a refresh token belongs to, whether the
//! token is current or already rotated, so a logout racing a refresh still
//! ends the grant. It answers the same for an unknown token, an access token,
//! and a token issued to another client, so it discloses nothing about which
//! tokens exist. Access tokens are not revocable here: they keep their expiry.

use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use coral_telemetry::safe;
use zeroize::Zeroizing;

use super::AuthorizationServerHttpState;
use super::client_authentication::{HeaderCredential, authenticate};
use super::response::security_headers;
use super::token::{
    FormError, MAX_BODY_BYTES, TokenError, json_response, parse_form, token_error,
    valid_content_type,
};

pub(super) async fn oauth_revoke(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    let header = match HeaderCredential::parse(request.headers()) {
        Ok(header) => header,
        Err(response) => return *response,
    };
    if !valid_content_type(request.headers()) {
        return token_error(TokenError::InvalidRequest);
    }
    let Ok(body) = to_bytes(request.into_body(), MAX_BODY_BYTES).await else {
        return token_error(TokenError::InvalidRequest);
    };
    let body = Zeroizing::new(Vec::from(body));
    let mut form = match parse_form(&body) {
        Ok(form) => form,
        Err(FormError::Malformed | FormError::InvalidScope) => {
            return token_error(TokenError::InvalidRequest);
        }
    };
    // Every client identifies itself; without that, no grant can be bound.
    let client = match authenticate(&state, header, &mut form, unavailable).await {
        Ok(client) => client,
        Err(response) => return *response,
    };
    let Some(token) = form.token.as_deref().filter(|token| !token.is_empty()) else {
        return token_error(TokenError::InvalidRequest);
    };
    match state.tokens.revoke(token, &client).await {
        Ok(()) => (StatusCode::OK, security_headers()).into_response(),
        Err(refusal) => {
            tracing::warn!(
                error.type = safe::kind(refusal.code()),
                "revocation endpoint could not revoke the token"
            );
            unavailable()
        }
    }
}

fn unavailable() -> Response {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        &serde_json::json!({
            "error": "server_error",
            "error_description": "revocation is unavailable",
        }),
    )
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    reason = "tests index the token responses they just received"
)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::extract::State;
    use axum::http::{HeaderValue, Request, StatusCode, header};
    use url::form_urlencoded;

    use super::super::token::tests::{
        CLIENT, confidential_sign_in, expect_error, refresh_form, request, send, signed_in, state,
    };
    use super::oauth_revoke;

    fn revocation(token: Option<&str>, client: Option<&str>) -> Request<Body> {
        let mut form = form_urlencoded::Serializer::new(String::new());
        if let Some(token) = token {
            form.append_pair("token", token);
            form.append_pair("token_type_hint", "refresh_token");
        }
        if let Some(client) = client {
            form.append_pair("client_id", client);
        }
        request(form.finish())
    }

    async fn revoke(
        state: &super::AuthorizationServerHttpState,
        request: Request<Body>,
    ) -> StatusCode {
        let response = oauth_revoke(State(state.clone()), request).await;
        let status = response.status();
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        if status == StatusCode::OK {
            assert!(body.is_empty());
        }
        status
    }

    /// Revocation validates its form, answers an empty no-store 200 whether
    /// or not the token is known, and ends refresh for a known one. Client
    /// binding and consumed tokens are `UserTokens`' rules.
    #[tokio::test]
    async fn revocation_validates_its_form_and_answers_alike() {
        let (state, _codes, _session, database) = state().await;
        let issued = signed_in(&state, &database, Some("reef")).await;
        let token = issued["refresh_token"].as_str().unwrap();

        let mut typed = revocation(Some(token), Some(CLIENT));
        typed.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let secret = format!("{}&client_secret=secret", refresh_form(token));
        for (request, expected) in [
            (typed, StatusCode::BAD_REQUEST),
            (revocation(None, Some(CLIENT)), StatusCode::BAD_REQUEST),
            (revocation(Some(token), None), StatusCode::BAD_REQUEST),
            (request(secret), StatusCode::BAD_REQUEST),
            (
                revocation(Some("coral_rt_unknown"), Some(CLIENT)),
                StatusCode::OK,
            ),
            (revocation(Some(token), Some(CLIENT)), StatusCode::OK),
        ] {
            assert_eq!(revoke(&state, request).await, expected);
        }
        expect_error(state, request(refresh_form(token)), "invalid_grant").await;
    }

    /// Revocation authenticates the client before it ends anything: without
    /// its secret a confidential client is `invalid_client` and the grant
    /// still refreshes.
    #[tokio::test]
    async fn revocation_authenticates_a_confidential_client_first() {
        let (state, _codes, _session, database) = state().await;
        let sign_in = confidential_sign_in(&state, &database).await;
        let client = sign_in.client.as_str();
        let form = |pairs: &[(&str, &str)]| {
            let mut form = form_urlencoded::Serializer::new(String::new());
            form.extend_pairs(pairs).append_pair("client_id", client);
            sign_in.with_secret(&form.finish())
        };
        let token = sign_in.refresh_token(&state).await;
        expect_error(
            state.clone(),
            revocation(Some(&token), Some(client)),
            "invalid_client",
        )
        .await;
        let refresh = form(&[("grant_type", "refresh_token"), ("refresh_token", &token)]);
        let (status, _headers, body) = send(state.clone(), request(refresh)).await;
        assert_eq!(status, StatusCode::OK, "the refusal left the grant live");
        let issued: serde_json::Value = serde_json::from_str(&body).unwrap();
        let token = issued["refresh_token"].as_str().unwrap();
        assert_eq!(
            revoke(&state, request(form(&[("token", token)]))).await,
            StatusCode::OK
        );
        let refresh = form(&[("grant_type", "refresh_token"), ("refresh_token", token)]);
        expect_error(state, request(refresh), "invalid_grant").await;
    }
}
