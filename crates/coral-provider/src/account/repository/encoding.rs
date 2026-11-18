//! Persistence formats for Account authorization replay, sealed OAuth flows and sealed tokens.
//! These codecs neither authorize a change nor seal/open credentials. The domain
//! owns those decisions and the vault; repositories own bytes and row mapping.
use crate::Error;
use crate::ProviderId;
use crate::account::oauth::{Flow, TokenResponse};
use crate::account::{AuthorizationRequest, AuthorizationTarget, OAuthClient, ProviderAccountId};
use coral_credentials::{CredentialValues, Secret, SecretReference};
use coral_db::UnixNanos;
use coral_workspace::PrincipalId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
    serde_json::to_string(&stored)
        .map_err(|_invalid| Error::Internal(crate::InternalFault::Encoding))
}

pub(super) fn decode_authorization_request(value: &str) -> Result<AuthorizationRequest, Error> {
    let stored: StoredRequest = serde_json::from_str(value).map_err(|_invalid| {
        Error::CorruptData("stored authorization request does not decode".into())
    })?;
    // Each value was valid when Coral stored it, so one that no longer
    // parses is corrupt data, not the caller's input.
    let corrupt =
        || Error::CorruptData("stored authorization request holds an invalid value".into());
    let (oauth, target) = match stored {
        StoredRequest::Reconnect((ReconnectMarker::Reconnect, id, oauth)) => (
            oauth,
            AuthorizationTarget::Reconnect(
                ProviderAccountId::parse(&id).map_err(|_invalid| corrupt())?,
            ),
        ),
        StoredRequest::Caller((oauth, target_provider, agent)) => {
            let target_provider =
                ProviderId::parse(&target_provider).map_err(|_invalid| corrupt())?;
            let target = match agent {
                None => AuthorizationTarget::User(target_provider),
                Some(id) => AuthorizationTarget::Agent {
                    target_provider,
                    agent: PrincipalId::parse(&id).map_err(|_invalid| corrupt())?,
                },
            };
            (oauth, target)
        }
        StoredRequest::Fixed(oauth) => (oauth, AuthorizationTarget::Shared),
    };
    Ok(AuthorizationRequest {
        oauth: oauth
            .map(StoredClient::into_domain)
            .transpose()
            .map_err(|_invalid| corrupt())?,
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
    .map_err(|_invalid| Error::Internal(crate::InternalFault::Encoding))
}
pub(in crate::account) fn decode_flow(value: &str) -> Result<Flow, Error> {
    let stored: StoredFlow<String> = serde_json::from_str(value).map_err(|_invalid| {
        Error::CorruptData("stored authorization flow does not decode".into())
    })?;
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
        expires_at: token.expires_at.map(UnixNanos::get),
    })
    .map(Zeroizing::new)
    .map_err(|_invalid| Error::Internal(crate::InternalFault::Encoding))
}
pub(in crate::account) fn decode_token(value: &str) -> Result<TokenResponse, Error> {
    let stored: StoredToken<String> = serde_json::from_str(value)
        .map_err(|_invalid| Error::CorruptData("stored token does not decode".into()))?;
    Ok(TokenResponse {
        access_token: stored.access_token,
        token_type: stored.token_type,
        scope: stored.scope,
        refresh_token: stored.refresh_token,
        expires_in: stored.expires_in,
        client_id: stored.client_id,
        expires_at: stored.expires_at.map(UnixNanos::new),
    })
}

/// Seals field values as one JSON object; a bare string is not a field record.
pub(in crate::account) fn encode_values(
    values: &CredentialValues,
) -> Result<Zeroizing<String>, Error> {
    let exposed: BTreeMap<&str, &str> = values
        .names()
        .filter_map(|name| values.get(name).map(|value| (name, value.expose())))
        .collect();
    serde_json::to_string(&exposed)
        .map(Zeroizing::new)
        .map_err(|_invalid| Error::Internal(crate::InternalFault::Encoding))
}
/// A record of typed values, which may be empty when a Secret Store holds every field.
pub(in crate::account) fn encode_sealed_fields(
    values: &BTreeMap<String, Secret>,
) -> Result<Zeroizing<String>, Error> {
    let exposed: BTreeMap<&str, &str> = values
        .iter()
        .map(|(name, value)| (name.as_str(), value.expose()))
        .collect();
    serde_json::to_string(&exposed)
        .map(Zeroizing::new)
        .map_err(|_invalid| Error::Internal(crate::InternalFault::Encoding))
}
pub(in crate::account) fn decode_values(value: &str) -> Result<CredentialValues, Error> {
    CredentialValues::new(decode_sealed_fields(value)?)
        .map_err(|_invalid| Error::CorruptData("stored credential values are not valid".into()))
}
pub(in crate::account) fn decode_sealed_fields(
    value: &str,
) -> Result<BTreeMap<String, Secret>, Error> {
    let stored: BTreeMap<String, String> = serde_json::from_str(value)
        .map_err(|_invalid| Error::CorruptData("stored credential fields do not decode".into()))?;
    // Each value moves into a zeroizing Secret without another copy.
    stored
        .into_iter()
        .map(|(name, value)| {
            Secret::new(value)
                .map(|secret| (name, secret))
                .map_err(|_invalid| {
                    Error::CorruptData("stored credential field is not a valid secret".into())
                })
        })
        .collect()
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredReference {
    store: String,
    path: String,
    key: Option<String>,
}

/// The references to store beside an Account's sealed record, by field name.
pub(in crate::account) fn encode_references(
    references: &BTreeMap<String, SecretReference>,
) -> Result<String, Error> {
    let stored: BTreeMap<&str, StoredReference> = references
        .iter()
        .map(|(name, reference)| {
            (
                name.as_str(),
                StoredReference {
                    store: reference.store().to_owned(),
                    path: reference.path().to_owned(),
                    key: reference.key().map(str::to_owned),
                },
            )
        })
        .collect();
    serde_json::to_string(&stored)
        .map_err(|_invalid| Error::Internal(crate::InternalFault::Encoding))
}

/// The references stored beside an Account's sealed record, by field name.
pub(super) fn decode_references(value: &str) -> Result<BTreeMap<String, SecretReference>, Error> {
    let stored: BTreeMap<String, StoredReference> = serde_json::from_str(value)
        .map_err(|_invalid| Error::CorruptData("stored Secret References do not decode".into()))?;
    stored
        .into_iter()
        .map(|(name, reference)| {
            SecretReference::new(reference.store, reference.path, reference.key)
                .map(|reference| (name, reference))
                .map_err(|_invalid| {
                    Error::CorruptData("stored Secret Reference is not valid".into())
                })
        })
        .collect()
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
    .map_err(|_invalid| Error::Internal(crate::InternalFault::Encoding))
}

pub(in crate::account) fn decode_device(
    value: &str,
) -> Result<crate::account::device::DeviceResponse, Error> {
    let (device_code, user_code, verification_uri, expires_in, interval) =
        serde_json::from_str(value).map_err(|_invalid| {
            Error::CorruptData("stored device authorization does not decode".into())
        })?;
    Ok(crate::account::device::DeviceResponse {
        device_code,
        user_code,
        verification_uri,
        expires_in,
        interval,
    })
}

#[cfg(test)]
mod tests {
    use crate::Error;

    // Catches: stored data that does not decode reported as invalid input
    // from the caller, which a client reads as its own mistake.
    #[test]
    fn stored_data_that_does_not_decode_is_corrupt_data() {
        let undecodable = "{";
        let outcomes: [(&str, Option<Error>); 6] = [
            (
                "authorization request",
                super::decode_authorization_request(undecodable).err(),
            ),
            ("flow", super::decode_flow(undecodable).err()),
            ("token", super::decode_token(undecodable).err()),
            ("values", super::decode_values(undecodable).err()),
            ("references", super::decode_references(undecodable).err()),
            ("device", super::decode_device(undecodable).err()),
        ];
        for (stored, error) in outcomes {
            assert!(
                matches!(error, Some(Error::CorruptData(_))),
                "{stored}: {error:?}"
            );
        }
        // A stored request that decodes and holds a malformed Provider
        // Account id is corrupt as well.
        let id = "00000000-0000-4000-8000-000000000000";
        let stored = super::encode_authorization_request(&super::AuthorizationRequest {
            oauth: None,
            target: super::AuthorizationTarget::Reconnect(
                super::ProviderAccountId::parse(id).unwrap(),
            ),
        })
        .unwrap();
        assert!(matches!(
            super::decode_authorization_request(&stored.replace(id, "not-an-id")),
            Err(Error::CorruptData(_))
        ));
        super::decode_authorization_request(&stored).expect("the stored request decodes");
        // A reference that decodes and names no store is corrupt as well.
        assert!(matches!(
            super::decode_references(r#"{"token":{"store":"","path":"p","key":"k"}}"#),
            Err(Error::CorruptData(_))
        ));
    }
}
