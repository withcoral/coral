//! Parses the credential a token, revocation or web-session request presents
//! for its OAuth Client, and answers a refusal. Parsing refuses only what no
//! client can present: two credentials at once (RFC 6749 section 2.3), or an
//! `Authorization` header that is not one HTTP Basic header.
//! `OAuthClients::authenticate` owns every rule about which credential a
//! client must present.

use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::Response;
use coral_auth::oauth_clients::{
    AuthenticatedClient, ClientAuthenticationError, ClientSecret, PresentedCredential,
};
use coral_telemetry::safe;

use super::AuthorizationServerHttpState;
use super::token::{TokenError, TokenForm, token_error};

/// The credential a request's `Authorization` header carries.
pub(super) enum HeaderCredential {
    /// No `Authorization` header.
    None,
    /// One HTTP Basic header, and the challenge a refusal of it carries.
    Basic(HeaderValue),
}

impl HeaderCredential {
    /// Reads the `Authorization` header before the body is read.
    ///
    /// # Errors
    ///
    /// An `invalid_client` response for a header that is duplicated,
    /// malformed, or not HTTP Basic: no client authenticates that way.
    pub(super) fn parse(headers: &HeaderMap) -> Result<Self, Box<Response>> {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let Some(value) = values.next() else {
            return Ok(Self::None);
        };
        // RFC 6749 section 5.2 requires the 401 challenge to name the scheme
        // the client authenticated with. A duplicated or malformed header
        // names no single scheme, so it stays a plain `invalid_client`.
        let scheme = if values.next().is_some() {
            None
        } else {
            value
                .to_str()
                .ok()
                .and_then(|value| value.split_once(' '))
                .map(|(scheme, _credentials)| scheme)
                .filter(|scheme| HeaderName::from_bytes(scheme.as_bytes()).is_ok())
        };
        let challenge = scheme.and_then(|scheme| {
            HeaderValue::try_from(format!("{scheme} realm=\"Coral OAuth\"")).ok()
        });
        match (scheme, challenge) {
            (Some(scheme), Some(challenge)) if scheme.eq_ignore_ascii_case("basic") => {
                Ok(Self::Basic(challenge))
            }
            (_, challenge) => Err(Box::new(invalid_client(challenge))),
        }
    }
}

/// Authenticates the client `form` names by the credential the request
/// presents in its header or its form, before anything spends a code or a
/// refresh token.
///
/// # Errors
///
/// `invalid_client`, 401 with a challenge for a header credential, when the
/// request presents two credentials or the client does not authenticate;
/// otherwise `invalid_request` when `form` names no client, and
/// `unavailable()` when storage did not answer.
pub(super) async fn authenticate(
    state: &AuthorizationServerHttpState,
    header: HeaderCredential,
    form: &mut TokenForm,
    unavailable: impl FnOnce() -> Response,
) -> Result<AuthenticatedClient, Box<Response>> {
    let (presented, challenge) = match (header, form.client_secret.take()) {
        (HeaderCredential::None, None) => (PresentedCredential::None, None),
        (HeaderCredential::None, Some(secret)) => (
            PresentedCredential::ClientSecretPost(ClientSecret::from(secret)),
            None,
        ),
        (HeaderCredential::Basic(challenge), None) => {
            (PresentedCredential::ClientSecretBasic, Some(challenge))
        }
        (HeaderCredential::Basic(challenge), Some(_secret)) => {
            return Err(Box::new(invalid_client(Some(challenge))));
        }
    };
    let Some(client_id) = form
        .client_id
        .as_deref()
        .filter(|client_id| !client_id.is_empty())
    else {
        // A Basic header names its client inside the header, which Coral
        // never decodes: no client may authenticate that way.
        return Err(Box::new(match challenge {
            Some(challenge) => invalid_client(Some(challenge)),
            None => token_error(TokenError::InvalidRequest),
        }));
    };
    match state.clients.authenticate(client_id, &presented).await {
        Ok(client) => Ok(client),
        Err(ClientAuthenticationError::InvalidClient) => Err(Box::new(invalid_client(challenge))),
        Err(error @ ClientAuthenticationError::Unavailable) => {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "OAuth client authentication could not read the client"
            );
            Err(Box::new(unavailable()))
        }
    }
}

fn invalid_client(challenge: Option<HeaderValue>) -> Response {
    let mut response = token_error(TokenError::InvalidClient);
    if let Some(challenge) = challenge {
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, challenge);
    }
    response
}
