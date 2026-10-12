//! Persistence formats for Account authorization replay, sealed OAuth flows and sealed tokens.
//! These codecs neither authorize a change nor seal/open credentials. The domain
//! owns those decisions and the vault; repositories own bytes and row mapping.
use crate::Error;
use crate::ProviderId;
use crate::account::oauth::{Flow, TokenResponse};
use crate::account::{AuthorizationRequest, AuthorizationTarget, OAuthClient, ProviderAccountId};
use coral_credentials::{CredentialValues, Secret};
use coral_workspace::PrincipalId;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

#[derive(Serialize, Deserialize)]
struct StoredClient {
    client_id: Option<String>,
    redirect_uri: String,
}
impl StoredClient {
    fn from_domain(client: &OAuthClient) -> Self {
        Self {
            client_id: client.client_id.clone(),
            redirect_uri: client.redirect_uri.clone(),
        }
    }
    fn into_domain(self) -> Result<OAuthClient, Error> {
        match self.client_id {
            Some(id) => OAuthClient::new(id, self.redirect_uri),
            None => OAuthClient::from_provider(self.redirect_uri),
        }
    }
}

// The three target shapes are the existing request-content storage format.
// Domain replay compares decoded facts, never this serialization.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum StoredRequest {
    Reconnect((ReconnectMarker, String, Option<StoredClient>)),
    Caller((Option<StoredClient>, String, Option<String>)),
    Fixed(Option<StoredClient>),
}
#[derive(Serialize, Deserialize)]
enum ReconnectMarker {
    #[serde(rename = "reconnect")]
    Reconnect,
}

pub(super) fn encode_authorization_request(
    request: &AuthorizationRequest,
) -> Result<String, Error> {
    let oauth = request.oauth.as_ref().map(StoredClient::from_domain);
    let stored = match &request.target {
        AuthorizationTarget::Reconnect(id) => {
            StoredRequest::Reconnect((ReconnectMarker::Reconnect, id.to_string(), oauth))
        }
        AuthorizationTarget::User(target_provider) => {
            StoredRequest::Caller((oauth, target_provider.to_string(), None))
        }
        AuthorizationTarget::Agent {
            target_provider,
            agent,
        } => StoredRequest::Caller((oauth, target_provider.to_string(), Some(agent.to_string()))),
        AuthorizationTarget::Shared => StoredRequest::Fixed(oauth),
    };
    serde_json::to_string(&stored).map_err(|_invalid| Error::AccountInput)
}

pub(super) fn decode_authorization_request(value: &str) -> Result<AuthorizationRequest, Error> {
    let stored: StoredRequest =
        serde_json::from_str(value).map_err(|_invalid| Error::AccountInput)?;
    let (oauth, target) = match stored {
        StoredRequest::Reconnect((ReconnectMarker::Reconnect, id, oauth)) => (
            oauth,
            AuthorizationTarget::Reconnect(ProviderAccountId::parse(&id)?),
        ),
        StoredRequest::Caller((oauth, target_provider, agent)) => {
            let target_provider = ProviderId::parse(&target_provider)?;
            let target = match agent {
                None => AuthorizationTarget::User(target_provider),
                Some(id) => AuthorizationTarget::Agent {
                    target_provider,
                    agent: PrincipalId::parse(&id)?,
                },
            };
            (oauth, target)
        }
        StoredRequest::Fixed(oauth) => (oauth, AuthorizationTarget::Shared),
    };
    Ok(AuthorizationRequest {
        oauth: oauth.map(StoredClient::into_domain).transpose()?,
        target,
    })
}

// Borrow fields for encoding; own them for decoding. Secret-bearing fields are
// not cloned into a temporary DTO, and serialized plaintext is zeroized on drop.
#[derive(Serialize, Deserialize)]
struct StoredFlow<T> {
    state: T,
    verifier: T,
    client_id: T,
    redirect_uri: T,
}

pub(in crate::account) fn encode_flow(flow: &Flow) -> Result<Zeroizing<String>, Error> {
    serde_json::to_string(&StoredFlow {
        state: &flow.state,
        verifier: &flow.verifier,
        client_id: &flow.client_id,
        redirect_uri: &flow.redirect_uri,
    })
    .map(Zeroizing::new)
    .map_err(|_invalid| Error::AccountInput)
}
pub(in crate::account) fn decode_flow(value: &str) -> Result<Flow, Error> {
    let stored: StoredFlow<String> =
        serde_json::from_str(value).map_err(|_invalid| Error::AccountInput)?;
    Ok(Flow {
        state: stored.state,
        verifier: stored.verifier,
        client_id: stored.client_id,
        redirect_uri: stored.redirect_uri,
    })
}

#[derive(Serialize, Deserialize)]
struct StoredToken<T> {
    access_token: T,
    token_type: T,
    scope: Option<T>,
    refresh_token: Option<T>,
    expires_in: Option<u64>,
    client_id: T,
    expires_at: Option<i64>,
}

pub(in crate::account) fn encode_token(token: &TokenResponse) -> Result<Zeroizing<String>, Error> {
    serde_json::to_string(&StoredToken {
        access_token: &token.access_token,
        token_type: &token.token_type,
        scope: token.scope.as_ref(),
        refresh_token: token.refresh_token.as_ref(),
        expires_in: token.expires_in,
        client_id: &token.client_id,
        expires_at: token.expires_at,
    })
    .map(Zeroizing::new)
    .map_err(|_invalid| Error::AccountInput)
}
pub(in crate::account) fn decode_token(value: &str) -> Result<TokenResponse, Error> {
    let stored: StoredToken<String> =
        serde_json::from_str(value).map_err(|_invalid| Error::AccountInput)?;
    Ok(TokenResponse {
        access_token: stored.access_token,
        token_type: stored.token_type,
        scope: stored.scope,
        refresh_token: stored.refresh_token,
        expires_in: stored.expires_in,
        client_id: stored.client_id,
        expires_at: stored.expires_at,
    })
}

/// Seals field values as one JSON object; a bare string is not a field record.
pub(in crate::account) fn encode_values(
    values: &CredentialValues,
) -> Result<Zeroizing<String>, Error> {
    let exposed: std::collections::BTreeMap<&str, &str> = values
        .names()
        .filter_map(|name| values.get(name).map(|value| (name, value.expose())))
        .collect();
    serde_json::to_string(&exposed)
        .map(Zeroizing::new)
        .map_err(|_invalid| Error::AccountInput)
}
pub(in crate::account) fn decode_values(value: &str) -> Result<CredentialValues, Error> {
    let stored: std::collections::BTreeMap<String, String> =
        serde_json::from_str(value).map_err(|_invalid| Error::AccountInput)?;
    // Each value moves into a zeroizing Secret without another copy.
    let values = stored
        .into_iter()
        .map(|(name, value)| {
            Secret::new(value)
                .map(|secret| (name, secret))
                .map_err(|_invalid| Error::AccountInput)
        })
        .collect::<Result<_, _>>()?;
    CredentialValues::new(values).map_err(|_invalid| Error::AccountInput)
}

pub(in crate::account) fn encode_device(
    device: &crate::account::device::DeviceResponse,
) -> Result<Zeroizing<String>, Error> {
    // Serialize borrowed values so plaintext does not acquire another owning copy.
    serde_json::to_string(&(
        &device.device_code,
        &device.user_code,
        &device.verification_uri,
        device.expires_in,
        device.interval,
    ))
    .map(Zeroizing::new)
    .map_err(|_invalid| Error::AccountInput)
}

pub(in crate::account) fn decode_device(
    value: &str,
) -> Result<crate::account::device::DeviceResponse, Error> {
    let (device_code, user_code, verification_uri, expires_in, interval) =
        serde_json::from_str(value).map_err(|_invalid| Error::AccountInput)?;
    Ok(crate::account::device::DeviceResponse {
        device_code,
        user_code,
        verification_uri,
        expires_in,
        interval,
    })
}
