//! RFC 7009 revocation of refresh tokens, for public clients.
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
use super::response::security_headers;
use super::token::{
    FormError, MAX_BODY_BYTES, TokenError, client_auth_rejection, json_response, parse_form,
    token_error, valid_content_type,
};

pub(super) async fn oauth_revoke(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    if let Some(response) = client_auth_rejection(request.headers()) {
        return response;
    }
    if !valid_content_type(request.headers()) {
        return token_error(TokenError::InvalidRequest);
    }
    let Ok(body) = to_bytes(request.into_body(), MAX_BODY_BYTES).await else {
        return token_error(TokenError::InvalidRequest);
    };
    let body = Zeroizing::new(Vec::from(body));
    let form = match parse_form(&body) {
        Ok(form) => form,
        Err(FormError::InvalidClient) => return token_error(TokenError::InvalidClient),
        Err(FormError::Malformed | FormError::InvalidScope) => {
            return token_error(TokenError::InvalidRequest);
        }
    };
    // A public client identifies itself; without that, no grant can be bound.
    let (Some(token), Some(client_id)) = (
        form.token.as_deref().filter(|token| !token.is_empty()),
        form.client_id
            .as_deref()
            .filter(|client| !client.is_empty()),
    ) else {
        return token_error(TokenError::InvalidRequest);
    };
    match state.tokens.revoke(token, client_id).await {
        Ok(()) => (StatusCode::OK, security_headers()).into_response(),
        Err(refusal) => {
            tracing::warn!(
                error.type = safe::kind(refusal.code()),
                "revocation endpoint could not revoke the token"
            );
            json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &serde_json::json!({
                    "error": "server_error",
                    "error_description": "revocation is unavailable",
                }),
            )
        }
    }
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
        CLIENT, assert_security, expect_error, refresh_form, request, send, signed_in, state,
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

    /// Revocation answers alike for unknown tokens and other clients' tokens,
    /// and ends refresh only for the grant's own client.
    #[tokio::test]
    async fn revocation_is_idempotent_and_bound_to_the_grants_client() {
        let (state, _codes, _session, database) = state().await;
        let issued = signed_in(&state, &database, Some("reef")).await;
        let token = issued["refresh_token"].as_str().unwrap();
        let access_token = issued["access_token"].as_str().unwrap();

        let mut typed = revocation(Some(token), Some(CLIENT));
        typed.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        for (request, expected) in [
            (typed, StatusCode::BAD_REQUEST),
            (revocation(None, Some(CLIENT)), StatusCode::BAD_REQUEST),
            (revocation(Some(token), None), StatusCode::BAD_REQUEST),
            (
                revocation(Some("coral_rt_unknown"), Some(CLIENT)),
                StatusCode::OK,
            ),
            (revocation(Some(access_token), Some(CLIENT)), StatusCode::OK),
            (
                revocation(Some(token), Some("https://other.example.test/client.json")),
                StatusCode::OK,
            ),
        ] {
            assert_eq!(revoke(&state, request).await, expected);
        }
        let secret = format!("{}&client_secret=secret", refresh_form(token));
        let response = oauth_revoke(State(state.clone()), request(secret)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let (status, headers, body) = send(state.clone(), request(refresh_form(token))).await;
        assert_security(&headers);
        assert_eq!(status, StatusCode::OK, "nothing above ended the grant");
        let refreshed: serde_json::Value = serde_json::from_str(&body).unwrap();
        let current = refreshed["refresh_token"].as_str().unwrap();

        // The consumed token still revokes, as a logout racing a refresh would.
        for _ in 0..2 {
            assert_eq!(
                revoke(&state, revocation(Some(token), Some(CLIENT))).await,
                StatusCode::OK
            );
        }
        expect_error(state, request(refresh_form(current)), "invalid_grant").await;
    }
}
