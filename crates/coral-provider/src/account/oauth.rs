//! Host-owned authorization-code OAuth. No token exchange occurs in a transaction.
use coral_workspace::WorkspaceId;

use super::callback::CallbackResponse;
use super::{
    AccountModule, Authorization, AuthorizationId, AuthorizationState, CredentialMaterial, event,
    expire, load_authorization, repository,
};
use crate::{Error, LifecycleAction, LifecycleOutcome, ProviderId};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use coral_catalog::AuthenticationConfiguration;
use coral_catalog::OAuthClientAuthenticationMethod as Method;
use coral_catalog::ProviderCredentialContract;
use coral_credentials::Secret;
use coral_workspace::ChangeContext;
use ring::rand::{SecureRandom as _, SystemRandom};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

/// Registered public OAuth client selected by host management, not a Program.
#[derive(Clone, PartialEq, Eq)]
pub struct OAuthClient {
    pub(super) client_id: Option<String>,
    pub(super) redirect_uri: String,
}
impl OAuthClient {
    /// Validates registered client metadata for mandatory S256 PKCE.
    /// # Errors
    /// Rejects empty IDs, unsafe redirects, and unbounded input.
    pub fn new(client_id: String, redirect_uri: String) -> Result<Self, Error> {
        validate_client_id(&client_id)?;
        validate_redirect_uri(&redirect_uri)?;
        Ok(Self {
            client_id: Some(client_id),
            redirect_uri,
        })
    }

    /// Supplies the host callback without interpreting mutable Provider configuration.
    /// The pinned contract chooses its OAuth client; secret-entry methods ignore the callback.
    /// Exact replay therefore keeps the original workflow even after configuration changes.
    /// # Errors
    /// Rejects unsafe or unbounded redirects.
    pub fn from_provider(redirect_uri: String) -> Result<Self, Error> {
        validate_redirect_uri(&redirect_uri)?;
        Ok(Self {
            client_id: None,
            redirect_uri,
        })
    }
}

/// Bounded, control-free client identifier.
pub(super) fn validate_client_id(client_id: &str) -> Result<(), Error> {
    coral_catalog::validate_oauth_client_id(client_id).map_err(|_invalid| Error::AccountInput)
}

/// HTTPS, or HTTP on an explicit loopback host, without credentials or fragment.
pub(super) fn validate_redirect_uri(redirect_uri: &str) -> Result<(), Error> {
    let url = url::Url::parse(redirect_uri).map_err(|_error| Error::AccountInput)?;
    let secure = match (url.scheme(), url.host_str()) {
        ("https", Some(_)) => true,
        ("http", Some(host)) => is_loopback_host(host),
        _ => false,
    };
    if redirect_uri.len() > 4096
        || !secure
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::AccountInput);
    }
    Ok(())
}

/// The explicit loopback exception to the HTTPS requirement.
pub(crate) fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

pub(super) struct Flow {
    pub(super) state: String,
    pub(super) verifier: String,
    pub(super) client_id: String,
    pub(super) redirect_uri: String,
}

impl Flow {
    /// Mints a fresh state and PKCE verifier for one client.
    pub(super) fn new(client_id: String, redirect_uri: String) -> Result<Self, Error> {
        Ok(Self {
            state: random()?,
            verifier: random()?,
            client_id,
            redirect_uri,
        })
    }

    /// Builds the navigation URL with S256 PKCE and the optional bound resource.
    pub(super) fn authorization_url(
        &self,
        authorization_endpoint: &str,
        scopes: &[String],
        scope_format: coral_catalog::OAuthScopeFormat,
        resource: Option<&str>,
        policy: Option<&coral_catalog::OAuthClientPolicy>,
    ) -> Result<String, Error> {
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(self.verifier.as_bytes()));
        let mut url =
            url::Url::parse(authorization_endpoint).map_err(|_error| Error::AccountInput)?;
        let default_policy = coral_catalog::OAuthClientPolicy::public(
            coral_catalog::OAuthClientBootstrap::Manual,
            self.client_id.clone(),
        );
        let policy = policy.unwrap_or(&default_policy);
        policy
            .validate(false, authorization_endpoint)
            .map_err(|_error| Error::AccountInput)?;
        {
            let mut query = url.query_pairs_mut();
            query.extend_pairs([
                ("response_type", "code"),
                ("client_id", &self.client_id),
                ("redirect_uri", &self.redirect_uri),
                ("state", &self.state),
            ]);
            if policy.pkce == coral_catalog::OAuthPkcePolicy::S256 {
                query.extend_pairs([
                    ("code_challenge_method", "S256"),
                    ("code_challenge", &challenge),
                ]);
            }
            query.extend_pairs(&policy.authorization_parameters);
            if !scopes.is_empty() {
                query.append_pair("scope", &scope_format.encode(scopes));
            }
            if let Some(resource) = resource {
                query.append_pair("resource", resource);
            }
        }
        Ok(url.into())
    }

    /// Verifies a callback state in constant time against the sealed flow.
    pub(super) fn verify_state(&self, state: &str) -> Result<(), Error> {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, self.verifier.as_bytes());
        let expected = ring::hmac::sign(&key, self.state.as_bytes());
        ring::hmac::verify(&key, state.as_bytes(), expected.as_ref())
            .map_err(|_error| Error::AccountInput)
    }
}

pub(super) fn random() -> Result<String, Error> {
    let mut bytes = [0_u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_error| Error::AccountInput)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

impl AccountModule {
    pub(super) fn prepare_oauth(
        &self,
        id: AuthorizationId,
        owner: &WorkspaceId,
        contract: &ProviderCredentialContract,
        client: Option<OAuthClient>,
    ) -> Result<(Option<String>, Option<String>), Error> {
        match (contract.authentication(), client) {
            // Coral offers no Connect for a sign-in that names no application.
            (method, client)
                if super::oauth_client::oauth_application_missing(method)
                    && client
                        .as_ref()
                        .is_none_or(|client| client.client_id.is_none()) =>
            {
                Err(Error::OAuthClientConfiguration)
            }
            (
                AuthenticationConfiguration::OAuthDeviceCode {
                    client: Some(policy),
                    ..
                },
                _,
            ) => {
                policy
                    .validate_device()
                    .map_err(|_error| Error::AccountInput)?;
                Ok((None, None))
            }
            // Every method a person types values into, whatever its shape, needs no
            // OAuth flow; listing them would leave the next such method behind.
            (method, client)
                if !method.credential_fields().is_empty()
                    && client
                        .as_ref()
                        .is_none_or(|client| client.client_id.is_none()) =>
            {
                Ok((None, None))
            }
            (
                AuthenticationConfiguration::OAuthAuthorizationCode {
                    authorization_url,
                    scopes,
                    scope_format,
                    resource,
                    client: policy,
                    ..
                },
                Some(client),
            ) => {
                // A contract that carries its own client ownership fixes the client;
                // a manually supplied identifier only serves contracts without one.
                let client_id = policy
                    .as_ref()
                    .map(|policy| policy.client_id.clone())
                    .or(client.client_id)
                    .ok_or(Error::AccountInput)?;
                let mut flow = Flow::new(client_id, client.redirect_uri)?;
                // Opaque, sealed callback routing survives browser-cookie loss without
                // exposing Workspace or authorization identifiers to the upstream service.
                let scope = owner.to_string();
                flow.state = format!(
                    "account.{}",
                    self.vault.seal(
                        "account-oauth-state",
                        &format!("{scope}.{id}.{}", flow.state)
                    )?
                );
                let url = flow.authorization_url(
                    authorization_url,
                    scopes,
                    *scope_format,
                    resource.as_deref(),
                    policy.as_ref(),
                )?;
                let json = repository::encode_flow(&flow)?;
                Ok((Some(url), Some(self.vault.seal(&id.to_string(), &json)?)))
            }
            _ => Err(Error::AccountInput),
        }
    }

    /// Starts a shared Account authorization from explicit host connection configuration.
    /// The domain pins secret entry or OAuth with mandatory S256 PKCE after exact replay.
    /// # Errors
    /// Requires a matching Provider contract and current Owner authority.
    pub async fn start_authorization(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider: &ProviderId,
        client: OAuthClient,
        request_id: crate::ManagementRequestId,
    ) -> Result<Authorization, Error> {
        self.start(
            context,
            owner,
            provider,
            request_id,
            Some(client),
            &super::AuthorizationTarget::Shared,
        )
        .await
    }

    /// Resolves host-issued callback state without a browser cookie or account selector.
    /// # Errors
    /// Rejects malformed or foreign workflows before exchanging a code.
    pub(super) async fn complete_account_callback(
        &self,
        context: &ChangeContext,
        state: &str,
        response: CallbackResponse,
    ) -> Result<(WorkspaceId, Authorization), Error> {
        if state.len() > 1024 {
            return Err(Error::AccountInput);
        }
        let routing = self
            .vault
            .open_sealed(
                "account-oauth-state",
                state.strip_prefix("account.").ok_or(Error::AccountInput)?,
            )
            .map_err(|_invalid| Error::AccountInput)?;
        let mut parts = routing.split('.');
        let scope = parts.next().ok_or(Error::AccountInput)?;
        let id = AuthorizationId::parse(parts.next().ok_or(Error::AccountInput)?)?;
        if parts.next().is_none() || parts.next().is_some() {
            return Err(Error::AccountInput);
        }
        let owner = coral_workspace::WorkspaceId::parse(scope)?;
        let authorization = self
            .complete_oauth_response(context, &owner, id, state, response)
            .await?;
        Ok((owner, authorization))
    }

    /// Validates an exact callback, then exchanges the code outside persistence.
    /// The exchange task survives caller disconnection; duplicate callbacks never
    /// exchange a code twice. Canceled/expired work cannot connect an Account.
    /// # Errors
    /// Rejects mismatched state, actor, or callback input without exposing tokens.
    pub async fn complete_oauth(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
        state: &str,
        code: Secret,
    ) -> Result<Authorization, Error> {
        self.complete_oauth_response(context, owner, id, state, CallbackResponse::Code(code))
            .await
    }

    async fn complete_oauth_response(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
        state: &str,
        response: CallbackResponse,
    ) -> Result<Authorization, Error> {
        if state.len() > 1024 {
            return Err(Error::AccountInput);
        }
        let module = self.clone();
        let context = context.clone();
        let owner = owner.clone();
        let state = state.to_string();
        // Detach before the first await, including the acceptance transaction:
        // losing its commit response cannot strand an accepted exchange.
        tokio::spawn(async move {
            module
                .process_callback(&context, &owner, id, &state, response)
                .await
        })
        .await
        .map_err(|_error| Error::AccountInput)?
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one callback owns the dispatch claim and completion fences"
    )]
    async fn process_callback(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
        state: &str,
        response: CallbackResponse,
    ) -> Result<Authorization, Error> {
        let started = std::time::Instant::now();
        let actor = context.actor();
        let mut tx = self.providers.database.begin().await?;
        let authority = self.scope_authority(&mut tx, actor, owner).await?;
        let mut row = load_authorization(&mut tx, owner, id).await?;
        self.check_authorization_actor(&mut tx, actor, owner, &row)
            .await?;
        expire(&mut tx, context, owner, authority, &mut row).await?;
        if row.state != AuthorizationState::Pending {
            tx.commit().await?;
            return Ok(row.view());
        }
        let private = self.vault.open_sealed(
            &id.to_string(),
            row.private_flow.as_deref().ok_or(Error::AccountInput)?,
        )?;
        let flow = repository::decode_flow(&private)?;
        flow.verify_state(state)?;
        let code = match response {
            CallbackResponse::Code(code) => code,
            CallbackResponse::Error(failure) => {
                super::finish_authorization(
                    &mut tx,
                    context,
                    owner,
                    authority,
                    &mut row,
                    AuthorizationState::Failed,
                    Some(failure),
                )
                .await?;
                tx.commit().await?;
                return Ok(row.view());
            }
        };
        let client_secret = self
            .provider_client_secret(
                &mut tx,
                owner,
                &row.provider_id,
                row.contract.oauth_client(),
            )
            .await?;
        let AuthenticationConfiguration::OAuthAuthorizationCode {
            token_url,
            scopes,
            resource,
            ..
        } = row.contract.authentication()
        else {
            return Err(Error::AccountInput);
        };
        repository::start_exchange(&mut tx, owner, id).await?;
        event(
            &mut tx,
            context,
            owner,
            authority,
            LifecycleAction::AccountAuthorizationExchanging,
            row.is_user_caller(),
            &id.to_string(),
            LifecycleOutcome::Accepted,
        )
        .await?;
        tx.commit().await?;
        #[cfg(test)]
        if let Some(gate) = &self.exchange_gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let result = exchange(
            TokenEndpoint {
                url: token_url,
                destination_policy: self.providers.outbound_policy,
            },
            &flow,
            &code,
            scopes,
            context.occurred_at_unix_nanos(),
            resource.as_deref(),
            &TokenAuthentication {
                policy: row.contract.oauth_client(),
                secret: client_secret.as_ref(),
            },
        )
        .await;
        let elapsed =
            i64::try_from(started.elapsed().as_nanos()).map_err(|_error| Error::AccountInput)?;
        let record_status =
            result.is_ok() || matches!(result, Err(Error::OAuthClientConfiguration));
        let client_failed = result.is_err();
        let context = ChangeContext::new(
            context.actor().clone(),
            context.occurred_at_unix_nanos().saturating_add(elapsed),
            context.trace_id().clone(),
        );
        let completion = match result {
            Ok(token) => {
                self.complete(&context, owner, id, CredentialMaterial::OAuth(token))
                    .await
            }
            Err(error) => {
                self.end_authorization(
                    &context,
                    owner,
                    id,
                    AuthorizationState::Failed,
                    Some(authorization_failure_code(&error)),
                )
                .await
            }
        };
        if record_status {
            // Application diagnostics must never discard a one-use token or prevent
            // the authorization from reaching its durable terminal state.
            drop(
                self.record_client_result(
                    owner,
                    &row.provider_id,
                    &row.contract,
                    client_secret.as_ref(),
                    client_failed,
                )
                .await,
            );
        }
        completion
    }
}

/// Stable safe code for an authorization failure; never an upstream description.
pub(super) const fn authorization_failure_code(error: &Error) -> &'static str {
    match error {
        Error::OutboundDestinationRejected => "outbound_destination_rejected",
        Error::OAuthClientConfiguration => "oauth_client_configuration",
        Error::OAuthTokenExchange(code) => code,
        _ => "authorization_failed",
    }
}

// Deserialize is the upstream OAuth protocol projection, not persistence encoding.
#[derive(Deserialize)]
pub(super) struct TokenResponse {
    pub(super) access_token: String,
    pub(super) token_type: String,
    pub(super) scope: Option<String>,
    pub(super) refresh_token: Option<String>,
    pub(super) expires_in: Option<u64>,
    // Token endpoint responses omit host metadata; stamp supplies it before sealing.
    #[serde(skip_deserializing)]
    pub(super) client_id: String,
    #[serde(skip_deserializing)]
    pub(super) expires_at: Option<i64>,
}
pub(super) struct TokenEndpoint<'a> {
    pub(super) url: &'a str,
    pub(super) destination_policy: coral_outbound::DestinationPolicy,
}

pub(super) async fn exchange(
    endpoint: TokenEndpoint<'_>,
    flow: &Flow,
    code: &Secret,
    scopes: &[String],
    now: i64,
    resource: Option<&str>,
    authentication: &TokenAuthentication<'_>,
) -> Result<TokenResponse, Error> {
    let mut form = vec![
        ("grant_type", "authorization_code"),
        ("code", code.expose()),
        ("client_id", &flow.client_id),
        ("redirect_uri", &flow.redirect_uri),
    ];
    if authentication
        .policy
        .is_none_or(|policy| policy.pkce == coral_catalog::OAuthPkcePolicy::S256)
    {
        form.push(("code_verifier", &flow.verifier));
    }
    if let Some(resource) = resource {
        form.push(("resource", resource));
    }
    let mut token = token_request(endpoint, &form, authentication).await?;
    token
        .accept(scopes)
        .map_err(|_error| Error::OAuthTokenExchange("oauth_token_response_invalid"))?;
    token
        .stamp(&flow.client_id, now)
        .map_err(|_error| Error::OAuthTokenExchange("oauth_token_response_invalid"))?;
    Ok(token)
}

pub(super) struct TokenAuthentication<'a> {
    pub(super) policy: Option<&'a coral_catalog::OAuthClientPolicy>,
    pub(super) secret: Option<&'a Secret>,
}

pub(super) async fn token_request(
    endpoint: TokenEndpoint<'_>,
    form: &[(&str, &str)],
    authentication: &TokenAuthentication<'_>,
) -> Result<TokenResponse, Error> {
    let (status, body) = form_request(endpoint, form, authentication).await?;
    parse_token_response(status, &body)
}

pub(super) async fn form_request(
    endpoint: TokenEndpoint<'_>,
    form: &[(&str, &str)],
    authentication: &TokenAuthentication<'_>,
) -> Result<(reqwest::StatusCode, zeroize::Zeroizing<Vec<u8>>), Error> {
    let client = coral_outbound::Client::new(
        endpoint.destination_policy,
        std::time::Duration::from_secs(30),
    )
    .map_err(token_transport_error)?;
    let mut request = client
        .post(endpoint.url)
        .map_err(|_error| Error::OutboundDestinationRejected)?
        .header(reqwest::header::ACCEPT, "application/json");
    let mut form = form.to_vec();
    match authentication
        .policy
        .map_or(Method::None, |policy| policy.authentication)
    {
        Method::None => {}
        Method::ClientSecretBasic => {
            let id = form
                .iter()
                .find(|(key, _)| *key == "client_id")
                .map(|(_, value)| *value)
                .ok_or(Error::AccountInput)?;
            let secret = authentication
                .secret
                .ok_or(Error::OAuthClientConfiguration)?;
            let encoded_id: String = url::form_urlencoded::byte_serialize(id.as_bytes()).collect();
            let encoded_secret = zeroize::Zeroizing::new(
                url::form_urlencoded::byte_serialize(secret.expose().as_bytes())
                    .collect::<String>(),
            );
            request = request.basic_auth(encoded_id, Some(encoded_secret.as_str()));
            form.retain(|(key, _)| *key != "client_id");
        }
        Method::ClientSecretPost => {
            form.push((
                "client_secret",
                authentication
                    .secret
                    .ok_or(Error::OAuthClientConfiguration)?
                    .expose(),
            ));
        }
    }
    let mut response = request
        .form(&form)
        .send()
        .await
        .map_err(token_transport_error)?;
    let status = response.status();
    let mut body = zeroize::Zeroizing::new(Vec::new());
    while let Some(chunk) = response.chunk().await.map_err(token_transport_error)? {
        if body.len().saturating_add(chunk.len()) > 64 * 1024 {
            return Err(Error::OAuthTokenExchange("oauth_token_response_invalid"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok((status, body))
}

fn token_transport_error(error: reqwest::Error) -> Error {
    match coral_outbound::FetchError::from(error) {
        coral_outbound::FetchError::DestinationRejected => Error::OutboundDestinationRejected,
        coral_outbound::FetchError::Http(error) => {
            Error::OAuthTokenExchange(if error.is_timeout() {
                "oauth_token_timeout"
            } else {
                "oauth_token_unavailable"
            })
        }
    }
}

fn parse_token_response(status: reqwest::StatusCode, body: &[u8]) -> Result<TokenResponse, Error> {
    check_response(status, body)?;
    serde_json::from_slice(body)
        .map_err(|_error| Error::OAuthTokenExchange("oauth_token_response_invalid"))
}

pub(super) fn check_response(status: reqwest::StatusCode, body: &[u8]) -> Result<(), Error> {
    #[derive(Deserialize)]
    struct OAuthError<'a> {
        error: &'a str,
    }
    if status.is_server_error() || status.is_redirection() {
        return Err(Error::OAuthTokenExchange("oauth_token_unavailable"));
    }
    // Some services report OAuth errors with HTTP 200. Only known codes cross
    // this boundary; descriptions, URLs, and response bodies can contain secrets.
    if let Ok(response) = serde_json::from_slice::<OAuthError>(body) {
        return Err(match response.error {
            "invalid_client" | "unauthorized_client" | "incorrect_client_credentials" => {
                Error::OAuthClientConfiguration
            }
            "invalid_grant" | "bad_verification_code" | "bad_refresh_token" => {
                Error::OAuthTokenExchange("oauth_code_rejected")
            }
            "redirect_uri_mismatch" => Error::OAuthTokenExchange("oauth_redirect_mismatch"),
            "authorization_pending" => Error::OAuthTokenExchange("oauth_authorization_pending"),
            "slow_down" => Error::OAuthTokenExchange("oauth_slow_down"),
            "access_denied" => Error::OAuthTokenExchange("oauth_access_denied"),
            "expired_token" => Error::OAuthTokenExchange("oauth_device_expired"),
            "invalid_scope" => Error::OAuthTokenExchange("oauth_scope_rejected"),
            _ => Error::OAuthTokenExchange("oauth_request_rejected"),
        });
    }
    if !status.is_success() {
        return Err(Error::OAuthTokenExchange("oauth_request_rejected"));
    }
    Ok(())
}

pub(super) async fn refresh_token(
    endpoint: TokenEndpoint<'_>,
    previous: &TokenResponse,
    scopes: &[String],
    now: i64,
    resource: Option<&str>,
    authentication: &TokenAuthentication<'_>,
) -> Result<TokenResponse, Error> {
    let refresh = previous
        .refresh_token
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or(Error::AccountRequired)?;
    if previous.client_id.is_empty() {
        return Err(Error::AccountRequired);
    }
    let mut form = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh),
        ("client_id", &previous.client_id),
    ];
    if let Some(resource) = resource {
        form.push(("resource", resource));
    }
    let mut token = token_request(endpoint, &form, authentication)
        .await
        .map_err(|error| match error {
            Error::OAuthTokenExchange("oauth_code_rejected" | "oauth_scope_rejected") => {
                Error::AccountRequired
            }
            Error::OAuthClientConfiguration | Error::OutboundDestinationRejected => error,
            _ => Error::CredentialRefreshFailed,
        })?;
    token
        .accept(scopes)
        .map_err(|_error| Error::CredentialRefreshFailed)?;
    token
        .stamp(&previous.client_id, now)
        .map_err(|_error| Error::CredentialRefreshFailed)?;
    if token.refresh_token.is_none() {
        token.refresh_token.clone_from(&previous.refresh_token);
    }
    Ok(token)
}

impl TokenResponse {
    pub(super) fn stamp(&mut self, client_id: &str, now: i64) -> Result<(), Error> {
        // Never trust host-only fields supplied by the upstream response.
        self.client_id = client_id.to_string();
        self.expires_at = self
            .expires_in
            .map(|seconds| {
                i64::try_from(seconds)
                    .ok()
                    .and_then(|seconds| seconds.checked_mul(1_000_000_000))
                    .and_then(|duration| now.checked_add(duration))
                    .ok_or(Error::AccountInput)
            })
            .transpose()?;
        Ok(())
    }

    /// The grant exactly as the token endpoint reported it.
    ///
    /// Services delimit and name scopes differently (GitHub and Slack use
    /// commas), and only the upstream service can say what a token may do.
    /// Coral therefore keeps the reported value for people to read and never
    /// compares it with the requested scopes. A grant the service later finds
    /// insufficient shows up as that Operation's upstream failure, which a
    /// person resolves by reconnecting the Account.
    pub(super) fn granted_scope(&self) -> String {
        self.scope.clone().unwrap_or_default()
    }

    /// Checks the response shape without judging the grant.
    ///
    /// An omitted `scope` means the grant equals the request (RFC 6749 §5.1),
    /// so the requested scopes are recorded in its place.
    pub(super) fn accept(&mut self, requested: &[String]) -> Result<(), Error> {
        if !self.token_type.eq_ignore_ascii_case("bearer") {
            return Err(Error::AccountInput);
        }
        // RFC 6749 scope uses printable ASCII; anything else is not a grant.
        if self.scope.as_ref().is_some_and(|reported| {
            reported.bytes().any(|byte| {
                !(byte == b' '
                    || byte == b'!'
                    || (b'#'..=b'[').contains(&byte)
                    || (b']'..=b'~').contains(&byte))
            })
        }) {
            return Err(Error::AccountInput);
        }
        if self.scope.is_none() {
            self.scope = Some(requested.join(" "));
        }
        Secret::new(self.access_token.clone())?;
        Ok(())
    }
}
