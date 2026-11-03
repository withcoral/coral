//! Deployment-level OAuth Clients, independent of Principals and grants.
//!
//! Registration accepts the supported authorization-code profile, for a public
//! client or a `client_secret_post` confidential one. Resolution dispatches
//! opaque IDs to durable evidence and URL IDs to the hardened CIMD Adapter.
//! Authentication checks the credential a token, revocation or web-session
//! request presents against the client's own method. None of them grants
//! authority.

use std::fmt;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use coral_db::Database;
use coral_outbound::{BrowserRedirect, EndpointUrl};
use ring::rand::{SecureRandom as _, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use thiserror::Error;
use zeroize::Zeroizing;

mod repository;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

/// Maximum registration request size, enforced before JSON parsing by Adapters.
pub const MAX_REGISTRATION_BYTES: usize = 5 * 1024;
const MAX_REGISTRATIONS: i64 = 1024;
const REGISTRATIONS_PER_MINUTE: i64 = 60;
const CLIENT_SECRET_PREFIX: &str = "dcrs_";

/// Validated, self-asserted client presentation and permitted redirects.
#[derive(Clone, Serialize)]
pub struct OAuthClientRegistration {
    /// Exact registered redirect URIs; authorization retains its matching policy.
    pub redirect_uris: Vec<String>,
    /// Unverified display name. Presentation must escape it, never trust it.
    pub client_name: String,
    /// Whether the client declared the `refresh_token` grant, so a sign-in
    /// it starts may also start a Refresh Grant.
    #[serde(skip)]
    pub refresh_eligible: bool,
}

impl std::fmt::Debug for OAuthClientRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthClientRegistration")
            .field("redirect_uri_count", &self.redirect_uris.len())
            .field("refresh_eligible", &self.refresh_eligible)
            .finish_non_exhaustive()
    }
}

/// Resolves client identifiers without exposing metadata fetching to callers.
#[async_trait::async_trait]
pub trait ClientMetadataResolver: Send + Sync {
    /// Returns validated client information, never authority or verified branding.
    /// # Errors
    /// Rejects invalid identifiers, unsupported metadata, or unavailable evidence.
    async fn resolve(
        &self,
        client_id: &str,
    ) -> Result<OAuthClientRegistration, ClientMetadataError>;
}

/// Supported registration input. Unknown RFC metadata is ignored, never stored.
#[derive(Deserialize)]
pub struct RegistrationRequest {
    redirect_uris: Vec<String>,
    client_name: Option<String>,
    token_endpoint_auth_method: Option<String>,
    grant_types: Option<Vec<String>>,
    response_types: Option<Vec<String>>,
}

/// Accepted registration, including explicit server policy overrides of defaults.
#[derive(Serialize)]
pub struct RegisteredClient {
    /// Opaque deployment-local identity, not a credential or Principal.
    pub client_id: String,
    /// Unix seconds when the registration was accepted.
    pub client_id_issued_at: i64,
    #[serde(flatten)]
    metadata: OAuthClientRegistration,
    token_endpoint_auth_method: &'static str,
    /// A confidential client's secret. This response is the only place it
    /// appears: Coral keeps only its verifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    client_secret: Option<ClientSecret>,
    /// `0` beside a secret: it never expires.
    #[serde(skip_serializing_if = "Option::is_none")]
    client_secret_expires_at: Option<i64>,
    grant_types: &'static [&'static str],
    response_types: [&'static str; 1],
    scope: &'static str,
}

/// How a client authenticates at the token and revocation endpoints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenEndpointAuthMethod {
    /// A public client: it proves only the redirect and the PKCE verifier.
    None,
    /// A confidential client: it also sends its secret as `client_secret`.
    ClientSecretPost,
}

impl TokenEndpointAuthMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ClientSecretPost => "client_secret_post",
        }
    }
}

/// How a registered client authenticates, as the repository stores it.
enum StoredAuthentication {
    None,
    /// The base64url SHA-256 of the client's secret.
    ClientSecretPost {
        verifier: String,
    },
}

/// An OAuth Client's secret. It proves the same software is calling back; it
/// is never a Coral credential and grants no authority.
pub struct ClientSecret(Zeroizing<String>);

impl Serialize for ClientSecret {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl ClientSecret {
    fn generate() -> Option<Self> {
        let mut bytes = Zeroizing::new([0_u8; 32]);
        SystemRandom::new().fill(bytes.as_mut()).ok()?;
        Some(Self(Zeroizing::new(format!(
            "{CLIENT_SECRET_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(bytes.as_ref())
        ))))
    }

    /// The base64url SHA-256 that Coral stores in place of the secret.
    fn verifier(&self) -> String {
        URL_SAFE_NO_PAD.encode(Sha256::digest(self.0.as_bytes()))
    }
}

impl From<Zeroizing<String>> for ClientSecret {
    fn from(secret: Zeroizing<String>) -> Self {
        Self(secret)
    }
}

impl fmt::Debug for ClientSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientSecret(<redacted>)")
    }
}

/// The credential one token, revocation or web-session request presents for
/// its OAuth Client. A request that carries more than one is refused before it gets
/// here (RFC 6749 section 2.3).
#[derive(Debug)]
pub enum PresentedCredential {
    /// No credential, as a public client sends.
    None,
    /// `client_secret` in the form body, which may be empty.
    ClientSecretPost(ClientSecret),
    /// An HTTP Basic `Authorization` header. No client may use it, so Coral
    /// never decodes it.
    ClientSecretBasic,
}

/// Evidence that a request authenticated as one OAuth Client by the method
/// that client registered. It grants no authority.
#[derive(Clone, Debug)]
pub struct AuthenticatedClient {
    client_id: String,
}

impl AuthenticatedClient {
    /// The client that authenticated.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
}

/// Registration and resolution over one deployment database and CIMD Adapter.
#[derive(Clone)]
pub struct OAuthClients {
    database: Arc<Database>,
    cimd: Arc<dyn ClientMetadataResolver>,
}

impl OAuthClients {
    /// Composes durable opaque identities with the existing hardened URL resolver.
    #[must_use]
    pub fn new(database: Arc<Database>, cimd: Arc<dyn ClientMetadataResolver>) -> Self {
        Self { database, cimd }
    }

    /// Accepts a public client without creating a User, consent, or authority.
    /// `now` is host time in Unix seconds, never supplied by the OAuth caller.
    /// # Errors
    /// Invalid metadata or a failed commit creates no registration.
    pub async fn register(
        &self,
        request: RegistrationRequest,
        now: i64,
    ) -> Result<RegisteredClient, RegistrationError> {
        if now < 0 {
            return Err(RegistrationError::Unavailable);
        }
        let (metadata, method) = request.validate()?;
        let (client_secret, authentication) = match method {
            TokenEndpointAuthMethod::None => (None, StoredAuthentication::None),
            TokenEndpointAuthMethod::ClientSecretPost => {
                let secret = ClientSecret::generate().ok_or(RegistrationError::Unavailable)?;
                let verifier = secret.verifier();
                (
                    Some(secret),
                    StoredAuthentication::ClientSecretPost { verifier },
                )
            }
        };
        let grant_types: &'static [&'static str] = if metadata.refresh_eligible {
            &["authorization_code", "refresh_token"]
        } else {
            &["authorization_code"]
        };
        let registered = RegisteredClient {
            client_id: format!("dcr_{}", uuid::Uuid::new_v4().simple()),
            client_id_issued_at: now,
            metadata,
            token_endpoint_auth_method: method.as_str(),
            client_secret_expires_at: client_secret.as_ref().map(|_secret| 0),
            client_secret,
            grant_types,
            response_types: ["code"],
            scope: "",
        };
        let mut tx = self
            .database
            .begin()
            .await
            .map_err(|_error| RegistrationError::Unavailable)?;
        let admission = repository::lock_admission(&mut tx).await?;
        if admission.total >= MAX_REGISTRATIONS {
            return Err(RegistrationError::Capacity);
        }
        let (window_started_at, accepted) = if now.saturating_sub(admission.window_started_at) >= 60
        {
            (now, 0)
        } else {
            (admission.window_started_at, admission.accepted)
        };
        if accepted >= REGISTRATIONS_PER_MINUTE {
            return Err(RegistrationError::Throttled);
        }
        repository::insert(&mut tx, &registered, &authentication).await?;
        repository::record_admission(&mut tx, window_started_at, accepted + 1).await?;
        tx.commit()
            .await
            .map_err(|_error| RegistrationError::Unavailable)?;
        Ok(registered)
    }

    /// Checks the credential a request presents for `client_id` against the
    /// method that client registered. A client must use exactly that method:
    /// a public client that sends any secret, even an empty one, is refused.
    /// A URL client ID is a public CIMD client and authenticates as `none`
    /// without fetching its document.
    /// # Errors
    /// [`ClientAuthenticationError::InvalidClient`] for an unknown client or a
    /// missing, wrong, or unexpected credential; `Unavailable` when storage
    /// did not answer. Neither changes anything.
    pub async fn authenticate(
        &self,
        client_id: &str,
        presented: &PresentedCredential,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        let stored = match ClientIdentifier::parse(client_id)
            .ok_or(ClientAuthenticationError::InvalidClient)?
        {
            ClientIdentifier::Metadata => StoredAuthentication::None,
            ClientIdentifier::Registered => repository::authentication(&self.database, client_id)
                .await?
                .ok_or(ClientAuthenticationError::InvalidClient)?,
        };
        let authenticated = match (&stored, presented) {
            (StoredAuthentication::None, PresentedCredential::None) => true,
            (
                StoredAuthentication::ClientSecretPost { verifier },
                PresentedCredential::ClientSecretPost(secret),
            ) => bool::from(secret.verifier().as_bytes().ct_eq(verifier.as_bytes())),
            _ => false,
        };
        if !authenticated {
            return Err(ClientAuthenticationError::InvalidClient);
        }
        Ok(AuthenticatedClient {
            client_id: client_id.to_owned(),
        })
    }
}

/// Which kind of OAuth Client an identifier names.
enum ClientIdentifier {
    /// A URL: a public client described by its metadata document (CIMD).
    Metadata,
    /// A `dcr_` identifier that registration issued.
    Registered,
}

impl ClientIdentifier {
    fn parse(client_id: &str) -> Option<Self> {
        if client_id.starts_with("https://") || client_id.starts_with("http://") {
            return Some(Self::Metadata);
        }
        let id = client_id.strip_prefix("dcr_")?;
        (id.len() == 32
            && id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
        .then_some(Self::Registered)
    }
}

#[async_trait::async_trait]
impl ClientMetadataResolver for OAuthClients {
    async fn resolve(
        &self,
        client_id: &str,
    ) -> Result<OAuthClientRegistration, ClientMetadataError> {
        match ClientIdentifier::parse(client_id).ok_or(ClientMetadataError::InvalidClientId)? {
            ClientIdentifier::Metadata => self.cimd.resolve(client_id).await,
            ClientIdentifier::Registered => repository::resolve(&self.database, client_id).await,
        }
    }
}

impl RegistrationRequest {
    fn validate(
        self,
    ) -> Result<(OAuthClientRegistration, TokenEndpointAuthMethod), RegistrationError> {
        // RFC 7591 defaults grants/responses to authorization_code/code. Its
        // default confidential method is deliberately overridden: an omitted
        // method is `none`, and the response says so.
        let method = match self.token_endpoint_auth_method.as_deref() {
            None | Some("none") => TokenEndpointAuthMethod::None,
            Some("client_secret_post") => TokenEndpointAuthMethod::ClientSecretPost,
            Some(_) => return Err(RegistrationError::InvalidMetadata),
        };
        if [&self.grant_types, &self.response_types]
            .into_iter()
            .flatten()
            .any(|values| values.len() > 8 || values.iter().any(|value| value.len() > 64))
            || self
                .grant_types
                .as_ref()
                .is_some_and(|v| !v.iter().any(|g| g == "authorization_code"))
            || self
                .response_types
                .as_ref()
                .is_some_and(|v| !v.iter().any(|r| r == "code"))
        {
            return Err(RegistrationError::InvalidMetadata);
        }
        let client_name = self
            .client_name
            .unwrap_or_else(|| "Unnamed OAuth client".into());
        if !valid_client_name(&client_name) {
            return Err(RegistrationError::InvalidMetadata);
        }
        if self.redirect_uris.is_empty()
            || self.redirect_uris.len() > 8
            || self
                .redirect_uris
                .iter()
                .any(|uri| uri.len() > 2048 || validate_oauth_redirect_uri(uri).is_err())
        {
            return Err(RegistrationError::InvalidRedirectUri);
        }
        let registration = OAuthClientRegistration {
            client_name,
            redirect_uris: self.redirect_uris,
            refresh_eligible: self
                .grant_types
                .is_some_and(|grants| grants.iter().any(|grant| grant == "refresh_token")),
        };
        Ok((registration, method))
    }
}

/// Shared CIMD/DCR display-name validation; a name never establishes trust.
#[must_use]
pub fn valid_client_name(name: &str) -> bool {
    !name.trim().is_empty() && name.trim() == name && name.len() <= 255
        && !name.chars().any(|c| c.is_control() || matches!(c, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'))
}

/// Validates a browser redirect without fetching it.
/// # Errors
/// Rejects non-HTTPS/non-loopback URLs, wildcards, and response-parameter collisions.
pub fn validate_oauth_redirect_uri(uri: &str) -> Result<(), String> {
    if uri.trim() != uri || uri.chars().any(char::is_control) || uri.contains('*') {
        return Err("redirect URI contains whitespace, controls, or a wildcard".into());
    }
    let url = EndpointUrl::<BrowserRedirect>::parse(uri)
        .map_err(|error| format!("OAuth client redirect URI is invalid: {error}"))?;
    // Host parsing decodes percent escapes, including an encoded wildcard.
    if url
        .as_url()
        .host_str()
        .is_some_and(|host| host.contains('*'))
    {
        return Err("redirect URI contains a wildcard".into());
    }
    if url.as_url().query_pairs().any(|(key, _)| {
        matches!(
            key.to_ascii_lowercase().as_str(),
            "code" | "state" | "error" | "error_description" | "error_uri" | "iss"
        )
    }) {
        return Err("OAuth client redirect URI must not contain OAuth response parameters".into());
    }
    Ok(())
}

/// Bounded registration failures. No payload or database error is exposed.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RegistrationError {
    /// Unsupported or unsafe metadata.
    #[error("client metadata is not supported")]
    InvalidMetadata,
    /// Missing, unsafe, or excessive redirects.
    #[error("redirect URIs are invalid")]
    InvalidRedirectUri,
    /// Deployment-wide durable rate limit reached.
    #[error("registration rate limit reached; try again later")]
    Throttled,
    /// Durable registry is full; no working registration is evicted.
    #[error("registration capacity reached; contact the operator")]
    Capacity,
    /// Persistence is unavailable.
    #[error("registration is unavailable")]
    Unavailable,
}

/// Why a request did not authenticate as an OAuth Client. Neither changes
/// anything.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ClientAuthenticationError {
    /// An unknown client, or a credential that is missing, wrong, or not the
    /// client's registered method. One answer for all, so a refusal
    /// discloses nothing about which clients exist.
    #[error("client authentication failed")]
    InvalidClient,
    /// Storage did not answer, or holds a client row the schema forbids.
    #[error("client authentication is unavailable")]
    Unavailable,
}

impl ClientAuthenticationError {
    /// The error's stable kind, for an event's `error.type`.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidClient => "invalid_client",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Sanitized client metadata resolution failures.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ClientMetadataError {
    /// Malformed or unknown client identity.
    #[error("OAuth client_id is invalid")]
    InvalidClientId,
    /// Invalid bootstrap settings.
    #[error("OAuth client metadata resolver configuration is invalid")]
    InvalidConfiguration,
    /// Cannot construct the outbound Adapter.
    #[error("failed to build the OAuth client metadata HTTP client")]
    ClientBuild,
    /// Fetch or durable resolution failed.
    #[error("OAuth client metadata resolution failed")]
    Fetch,
    /// CIMD does not permit redirects.
    #[error("OAuth client metadata endpoint returned a redirect")]
    Redirect,
    /// CIMD requires HTTP 200.
    #[error("OAuth client metadata endpoint returned an unsuccessful status")]
    HttpStatus,
    /// Bounded CIMD body exceeded its limit.
    #[error("OAuth client metadata document exceeded 5120 bytes")]
    BodyTooLarge,
    /// Invalid CIMD JSON.
    #[error("OAuth client metadata document was not valid JSON")]
    InvalidJson,
    /// Unsupported CIMD profile.
    #[error("OAuth client metadata document is not supported")]
    InvalidMetadata,
}

impl ClientMetadataError {
    /// The error's stable kind, for an event's `error.type`: a bounded
    /// code that never carries the error's text.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidClientId { .. } => "invalid_client_id",
            Self::InvalidConfiguration { .. } => "invalid_configuration",
            Self::ClientBuild { .. } => "client_build",
            Self::Fetch { .. } => "fetch",
            Self::Redirect { .. } => "redirect",
            Self::HttpStatus { .. } => "http_status",
            Self::BodyTooLarge { .. } => "body_too_large",
            Self::InvalidJson { .. } => "invalid_json",
            Self::InvalidMetadata { .. } => "invalid_metadata",
        }
    }
}
