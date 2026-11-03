//! Deployment-level public OAuth clients, independent of Principals and grants.
//!
//! Registration accepts only the supported public authorization-code profile.
//! Resolution dispatches opaque IDs to durable evidence and URL IDs to the
//! hardened CIMD Adapter. Neither registration nor resolution grants authority.

use std::sync::Arc;

use coral_db::Database;
use coral_outbound::{BrowserRedirect, EndpointUrl};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod repository;
#[cfg(test)]
mod tests;

/// Maximum registration request size, enforced before JSON parsing by Adapters.
pub const MAX_REGISTRATION_BYTES: usize = 5 * 1024;
const MAX_REGISTRATIONS: i64 = 1024;
const REGISTRATIONS_PER_MINUTE: i64 = 60;

/// Validated, self-asserted client presentation and permitted redirects.
#[derive(Clone, Serialize)]
pub struct OAuthClientRegistration {
    /// Exact registered redirect URIs; authorization retains its matching policy.
    pub redirect_uris: Vec<String>,
    /// Unverified display name. Presentation must escape it, never trust it.
    pub client_name: String,
}

impl std::fmt::Debug for OAuthClientRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthClientRegistration")
            .field("redirect_uri_count", &self.redirect_uris.len())
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
    grant_types: [&'static str; 1],
    response_types: [&'static str; 1],
    scope: &'static str,
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
        let metadata = request.validate()?;
        let registered = RegisteredClient {
            client_id: format!("dcr_{}", uuid::Uuid::new_v4().simple()),
            client_id_issued_at: now,
            metadata,
            token_endpoint_auth_method: "none",
            grant_types: ["authorization_code"],
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
        repository::insert(&mut tx, &registered).await?;
        repository::record_admission(&mut tx, window_started_at, accepted + 1).await?;
        tx.commit()
            .await
            .map_err(|_error| RegistrationError::Unavailable)?;
        Ok(registered)
    }
}

#[async_trait::async_trait]
impl ClientMetadataResolver for OAuthClients {
    async fn resolve(
        &self,
        client_id: &str,
    ) -> Result<OAuthClientRegistration, ClientMetadataError> {
        if client_id.starts_with("https://") || client_id.starts_with("http://") {
            return self.cimd.resolve(client_id).await;
        }
        let Some(id) = client_id.strip_prefix("dcr_") else {
            return Err(ClientMetadataError::InvalidClientId);
        };
        if id.len() != 32
            || !id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ClientMetadataError::InvalidClientId);
        }
        repository::resolve(&self.database, client_id).await
    }
}

impl RegistrationRequest {
    fn validate(self) -> Result<OAuthClientRegistration, RegistrationError> {
        // RFC 7591 defaults grants/responses to authorization_code/code. The
        // default confidential method is deliberately overridden and returned.
        if [&self.grant_types, &self.response_types]
            .into_iter()
            .flatten()
            .any(|values| values.len() > 8 || values.iter().any(|value| value.len() > 64))
            || self
                .token_endpoint_auth_method
                .as_deref()
                .is_some_and(|v| v != "none")
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
        Ok(OAuthClientRegistration {
            client_name,
            redirect_uris: self.redirect_uris,
        })
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
