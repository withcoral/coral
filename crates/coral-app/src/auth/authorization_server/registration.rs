//! Bounded RFC 7591 transport Adapter. No registration initiates outbound HTTP.

use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse as _, Response};
use coral_auth::oauth_clients::{MAX_REGISTRATION_BYTES, RegistrationError, RegistrationRequest};

use super::AuthorizationServerHttpState;

pub(super) async fn register(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    if !request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"))
        })
    {
        return failure(RegistrationError::InvalidMetadata);
    }
    let Ok(body) = to_bytes(request.into_body(), MAX_REGISTRATION_BYTES).await else {
        return failure(RegistrationError::InvalidMetadata);
    };
    // Serde structs also accept positional arrays. RFC 7591 requires an
    // object; retain typed decoding below so duplicate known fields still fail.
    if body.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return failure(RegistrationError::InvalidMetadata);
    }
    let Ok(request) = serde_json::from_slice::<RegistrationRequest>(&body) else {
        return failure(RegistrationError::InvalidMetadata);
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok());
    let Some(now) = now else {
        return failure(RegistrationError::Unavailable);
    };
    match state.clients.register(request, now).await {
        Ok(client) => match serde_json::to_value(client) {
            Ok(value) => response(StatusCode::CREATED, super::json_response(&value)),
            Err(_) => failure(RegistrationError::Unavailable),
        },
        Err(error) => failure(error),
    }
}

fn failure(error: RegistrationError) -> Response {
    let (status, code) = match error {
        RegistrationError::InvalidMetadata => (StatusCode::BAD_REQUEST, "invalid_client_metadata"),
        RegistrationError::InvalidRedirectUri => (StatusCode::BAD_REQUEST, "invalid_redirect_uri"),
        RegistrationError::Throttled => (StatusCode::TOO_MANY_REQUESTS, "temporarily_unavailable"),
        RegistrationError::Capacity | RegistrationError::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable")
        }
    };
    let retry = error == RegistrationError::Throttled;
    let mut result = response(
        status,
        super::json_response(
            &serde_json::json!({"error": code, "error_description": error.to_string()}),
        ),
    );
    if retry {
        result
            .headers_mut()
            .insert(header::RETRY_AFTER, "60".parse().expect("static header"));
    }
    result
}

fn response(status: StatusCode, body: impl axum::response::IntoResponse) -> Response {
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
        ],
        body,
    )
        .into_response()
}
