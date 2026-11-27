//! The OIDC sign-in mode: the authorization request that begins a sign-in,
//! the callback that names it, and the code exchange and token checks that
//! prove its person. Core does the rest.
//!
//! The nonce and the PKCE verifier are derived from the sign-in's value with
//! the handshake keys, so the callback recomputes them instead of anything
//! holding them until it arrives.

use std::sync::Arc;

use coral_db::{Clock, UnixNanos};
use coral_sign_in::{
    ModeRequest, ModeUrl, Named, PersonIdentifier, ProvenPerson, Refusal, SignInMode, SignInValue,
};
use coral_telemetry::safe;
use url::Url;

use super::ResolvedOidcProvider;
use crate::auth::id_token::ValidatedOidcIdentity;
use crate::auth::provider_client::{OidcProviderClient, OidcProviderClientError};
use crate::auth::query;
use crate::auth::sealed_handshake::HandshakeKeys;

/// The OIDC sign-in of one configured provider.
pub(in crate::auth) struct OidcMode {
    provider: ResolvedOidcProvider,
    client: OidcProviderClient,
    keys: Arc<HandshakeKeys>,
}

impl OidcMode {
    /// The mode of `provider`, which reaches the provider under `policy`
    /// and derives each sign-in's secrets with `keys`.
    pub(in crate::auth) fn new(
        provider: ResolvedOidcProvider,
        keys: Arc<HandshakeKeys>,
        policy: coral_outbound::DestinationPolicy,
    ) -> Result<Self, OidcProviderClientError> {
        Ok(Self {
            provider,
            client: OidcProviderClient::new(policy)?,
            keys,
        })
    }
}

#[async_trait::async_trait]
impl SignInMode for OidcMode {
    fn urls(&self) -> Vec<ModeUrl> {
        vec![super::CALLBACK]
    }

    async fn begin(&self, sign_in: &SignInValue, _now: UnixNanos) -> Result<Url, Refusal> {
        self.client
            .authorization_request(&self.provider, &self.keys, sign_in.as_str())
            .await
            .map_err(|error| Refusal::failed(error.code()))
    }

    fn names(&self, request: &ModeRequest<'_>, _now: UnixNanos) -> Named {
        let Ok(query) = parse_query(request.query) else {
            return Named::Refused(malformed());
        };
        match query.state.filter(|state| !state.is_empty()) {
            Some(state) => Named::SignIn(SignInValue::new(state)),
            None => Named::Refused(Refusal::invalid(
                "callback_state_required",
                "OIDC callback state is required",
            )),
        }
    }

    async fn check(
        &self,
        request: &ModeRequest<'_>,
        sign_in: &SignInValue,
        clock: &dyn Clock,
    ) -> Result<ProvenPerson, Refusal> {
        let query = parse_query(request.query).map_err(|()| malformed())?;
        let provider_code = match classify_result(&query) {
            Ok(CallbackResult::Code(code)) => code,
            Ok(CallbackResult::AccessDenied) => return Err(Refusal::denied("provider_denied")),
            Ok(CallbackResult::ProviderError) => return Err(Refusal::failed("provider_error")),
            Err(()) => {
                return Err(Refusal::invalid(
                    "callback_response_invalid",
                    "OIDC callback response is invalid",
                ));
            }
        };
        let exchange = self
            .client
            .exchange_code(
                &self.provider,
                &provider_code,
                &self.keys.pkce_verifier(sign_in.as_str()),
            )
            .await
            .map_err(|error| {
                tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "OIDC callback could not exchange the provider code"
                );
                Refusal::failed(error.code())
            })?;
        let identity = self
            .client
            .validate_code_exchange(
                &self.provider,
                exchange,
                &self.keys.nonce(sign_in.as_str()),
                clock,
            )
            .await
            .map_err(|error| {
                tracing::warn!(
                    error.type = safe::kind(error.code()),
                    "OIDC callback could not validate the provider identity"
                );
                Refusal::failed(error.code())
            })?;
        Ok(proven(identity))
    }

    fn first_owner(&self) -> Option<PersonIdentifier<'_>> {
        self.provider
            .bootstrap_owner_subject
            .as_deref()
            .map(|subject| PersonIdentifier::Subject {
                issuer: &self.provider.issuer,
                subject,
            })
    }
}

fn malformed() -> Refusal {
    Refusal::invalid("callback_malformed", "OIDC callback request is malformed")
}

/// The person the provider's tokens prove, with the whole profile they
/// carry.
fn proven(identity: ValidatedOidcIdentity) -> ProvenPerson {
    let ValidatedOidcIdentity {
        issuer,
        subject,
        display_name,
        profile_name,
        email,
        email_verified,
        hosted_domain,
        profile_observed_at,
    } = identity;
    ProvenPerson::subject(issuer, subject)
        .with_display_name(display_name)
        .with_profile(profile_name, email, email_verified, hosted_domain)
        .with_observed_at(profile_observed_at)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_queries_are_bounded() {
        for raw in query::rejected_queries() {
            let Err(()) = parse_query(&raw) else {
                panic!("malformed callback query must be rejected");
            };
        }
    }
}
