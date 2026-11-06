//! The `HashiCorp` Vault and `OpenBao` KV version 2 adapter, with `AppRole` login.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use coral_outbound::OutboundUrlPolicyError;
use reqwest::StatusCode;
use reqwest::header::HeaderValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use url::Url;
use zeroize::{Zeroize as _, Zeroizing};

use crate::{Error, InvalidSetting, Secret, SecretReference, SecretStore};

/// How long one request to a store may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest answer Coral reads from a store.
const BODY_LIMIT: usize = 1 << 20;

/// How long reads share a failed login or renewal instead of each trying their own.
const RETRY_AFTER: Duration = Duration::from_secs(5);

const TOKEN_HEADER: &str = "x-vault-token";

/// Reads the time. A test moves it; production passes [`Instant::now`].
type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// Where and how to reach one KV version 2 mount.
pub struct KvV2Settings {
    /// The store's origin.
    pub address: StoreAddress,
    /// Where the KV engine is mounted.
    pub mount: MountPath,
    /// How Coral signs in.
    pub auth: AppRole,
}

/// `AppRole` sign-in settings.
pub struct AppRole {
    /// Where the auth method is mounted.
    pub mount: MountPath,
    /// The role's ID.
    pub role_id: String,
    /// The role's secret ID.
    pub secret_id: Secret,
}

/// A store's origin. This checks its shape only: the destination policy decides
/// whether Coral may reach it, and over which scheme, for every request.
pub struct StoreAddress(Url);

impl StoreAddress {
    /// # Errors
    /// Returns [`InvalidSetting`] for anything but an http or https origin.
    pub fn parse(value: &str) -> Result<Self, InvalidSetting> {
        Url::parse(value)
            .ok()
            .filter(|url| {
                matches!(url.scheme(), "http" | "https")
                    && url.host().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none()
            })
            .map(Self)
            .ok_or(InvalidSetting(
                "must be an http or https origin, without a path, query or credentials",
            ))
    }
}

/// A mount path of one or more `/`-separated segments.
pub struct MountPath(Vec<String>);

impl MountPath {
    /// # Errors
    /// Returns [`InvalidSetting`] for an empty segment or a dot segment.
    pub fn parse(value: &str) -> Result<Self, InvalidSetting> {
        safe_segments(value)
            .map(|segments| Self(segments.into_iter().map(str::to_owned).collect()))
            .ok_or(InvalidSetting(
                "must be `/`-separated names, with no empty, `.` or `..` part",
            ))
    }
}

/// The parts of a path, unless one would change which endpoint the URL names.
fn safe_segments(path: &str) -> Option<Vec<&str>> {
    let segments: Vec<&str> = path.split('/').collect();
    segments
        .iter()
        .all(|segment| !matches!(*segment, "" | "." | ".."))
        .then_some(segments)
}

/// One KV version 2 mount, signed in as one role.
pub struct KvV2Store {
    client: coral_outbound::Client,
    origin: Url,
    mount: Vec<String>,
    auth: AppRole,
    clock: Clock,
    /// Locked across a sign-in or renewal, so concurrent reads share one.
    state: Mutex<TokenState>,
}

#[derive(Default)]
struct TokenState {
    token: Option<Token>,
    login_failure: Option<(Instant, Error)>,
    renewal_failure: Option<Instant>,
}

/// A store token and the lease the store gave it.
struct Token {
    value: Arc<Secret>,
    obtained: Instant,
    /// `None` when the lease never ends.
    lease: Option<Duration>,
    renewable: bool,
}

impl Token {
    fn new(auth: &Auth, now: Instant) -> Result<Self, Error> {
        Ok(Self {
            value: Arc::new(
                Secret::new(auth.client_token.to_string())
                    .map_err(|_error| Error::MalformedResponse)?,
            ),
            obtained: now,
            lease: (auth.lease_duration > 0).then(|| Duration::from_secs(auth.lease_duration)),
            renewable: auth.renewable,
        })
    }

    /// How much of a lease to leave unused, so a read that starts now can
    /// finish: a tenth of it, or one request's timeout if that is more. A lease
    /// too short for that keeps half, so a new token is not replaced on every read.
    fn margin(lease: Duration) -> Duration {
        (lease / 10).max(REQUEST_TIMEOUT).min(lease / 2)
    }

    /// What to do before this token's next use. It renews past half of its
    /// lease, and logs in again once only the margin is left.
    fn next_step(&self, now: Instant) -> NextStep {
        let Some(lease) = self.lease else {
            return NextStep::Use;
        };
        let held = now.saturating_duration_since(self.obtained);
        if held >= lease.saturating_sub(Self::margin(lease)) {
            NextStep::Login
        } else if self.renewable && held >= lease / 2 {
            NextStep::Renew
        } else {
            NextStep::Use
        }
    }
}

/// A token ready to send, and whether this read signed in to get it.
struct UsableToken {
    value: Arc<Secret>,
    from_login: bool,
}

enum NextStep {
    Use,
    Renew,
    Login,
}

impl KvV2Store {
    /// Prepares a store. Nothing is sent until the first read, so an unreachable
    /// store does not stop Coral from starting.
    #[must_use]
    pub fn new(client: coral_outbound::Client, settings: KvV2Settings) -> Self {
        Self::with_clock(client, settings, Arc::new(Instant::now))
    }

    fn with_clock(client: coral_outbound::Client, settings: KvV2Settings, clock: Clock) -> Self {
        Self {
            client,
            origin: settings.address.0,
            mount: settings.mount.0,
            auth: settings.auth,
            clock,
            state: Mutex::default(),
        }
    }

    /// An API URL. Each segment is percent-encoded, so none can add a level.
    fn url<'a>(&self, segments: impl IntoIterator<Item = &'a str>) -> Result<Url, Error> {
        let mut url = self.origin.clone();
        url.path_segments_mut()
            .map_err(|()| Error::Unreachable)?
            .clear()
            .push("v1")
            .extend(segments);
        Ok(url)
    }

    /// A token to use now.
    async fn token(&self) -> Result<UsableToken, Error> {
        let mut state = self.state.lock().await;
        let now = (self.clock)();
        if let Some(token) = state.token.as_ref() {
            let held = UsableToken {
                value: Arc::clone(&token.value),
                from_login: false,
            };
            match token.next_step(now) {
                NextStep::Use => return Ok(held),
                NextStep::Renew => {
                    if state
                        .renewal_failure
                        .is_some_and(|failed| now.saturating_duration_since(failed) < RETRY_AFTER)
                    {
                        return Ok(held);
                    }
                    match self.renew(&held.value, now).await {
                        Ok(renewed) => {
                            let value = Arc::clone(&renewed.value);
                            state.token = Some(renewed);
                            state.renewal_failure = None;
                            return Ok(UsableToken {
                                value,
                                from_login: false,
                            });
                        }
                        // The lease still has time to run, and the store may be
                        // back before it ends.
                        Err(Error::Unreachable) => {
                            state.renewal_failure = Some((self.clock)());
                            return Ok(held);
                        }
                        Err(_) => {}
                    }
                }
                NextStep::Login => {}
            }
        }
        state.token = None;
        if let Some((failed, error)) = state.login_failure
            && now.saturating_duration_since(failed) < RETRY_AFTER
        {
            return Err(error);
        }
        match self.login(now).await {
            Ok(token) => {
                let value = Arc::clone(&token.value);
                state.token = Some(token);
                state.login_failure = None;
                state.renewal_failure = None;
                Ok(UsableToken {
                    value,
                    from_login: true,
                })
            }
            Err(error) => {
                state.login_failure = Some(((self.clock)(), error));
                Err(error)
            }
        }
    }

    /// Drops this token unless a concurrent read has already replaced it.
    async fn forget(&self, token: &Arc<Secret>) {
        let mut state = self.state.lock().await;
        if state
            .token
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(&current.value, token))
        {
            state.token = None;
        }
    }

    async fn renew(&self, token: &Secret, now: Instant) -> Result<Token, Error> {
        let url = self.url(["auth", "token", "renew-self"])?;
        let response = self
            .client
            .post(url.as_str())
            .map_err(|_error| Error::Unreachable)?
            .header(TOKEN_HEADER, token_header(token)?)
            .send()
            .await
            .map_err(|_error| Error::Unreachable)?;
        let status = response.status();
        if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::Unreachable);
        }
        if !status.is_success() {
            return Err(Error::PermissionDenied);
        }
        auth_token(response, now).await
    }

    /// Whether the store still honors this token. A denied read cannot say,
    /// because a policy denial and a revoked token answer alike.
    async fn token_is_live(&self, token: &Secret) -> Result<bool, Error> {
        let url = self.url(["auth", "token", "lookup-self"])?;
        let response = self
            .client
            .get(url.as_str())
            .map_err(|_error| Error::Unreachable)?
            .header(TOKEN_HEADER, token_header(token)?)
            .send()
            .await
            .map_err(|_error| Error::Unreachable)?;
        match response.status() {
            status if status.is_success() => Ok(true),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Ok(false),
            _ => Err(Error::Unreachable),
        }
    }

    async fn login(&self, now: Instant) -> Result<Token, Error> {
        #[derive(Serialize)]
        struct Login<'a> {
            role_id: &'a str,
            secret_id: &'a str,
        }
        let url = self.url(
            ["auth"]
                .into_iter()
                .chain(self.auth.mount.0.iter().map(String::as_str))
                .chain(["login"]),
        )?;
        let response = self
            .client
            .post(url.as_str())
            .map_err(|_error| Error::Unreachable)?
            .json(&Login {
                role_id: &self.auth.role_id,
                secret_id: self.auth.secret_id.expose(),
            })
            .send()
            .await
            .map_err(|_error| Error::Unreachable)?;
        let status = response.status();
        // Vault answers a wrong role or secret ID with 400, and a missing auth
        // mount with 404: either way the store refused this identity.
        if status.is_client_error() && status != StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::PermissionDenied);
        }
        if !status.is_success() {
            return Err(Error::Unreachable);
        }
        auth_token(response, now).await
    }

    async fn read_entry(&self, url: &Url, token: &Secret, key: &str) -> Result<Secret, Error> {
        let response = self
            .client
            .get(url.as_str())
            .map_err(|_error| Error::Unreachable)?
            .header(TOKEN_HEADER, token_header(token)?)
            .send()
            .await
            .map_err(|_error| Error::Unreachable)?;
        match response.status() {
            StatusCode::OK => {}
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                return Err(Error::PermissionDenied);
            }
            StatusCode::NOT_FOUND => return Err(Error::EntryNotFound),
            status
                if status.is_server_error()
                    || status.is_redirection()
                    || status == StatusCode::TOO_MANY_REQUESTS =>
            {
                return Err(Error::Unreachable);
            }
            _ => return Err(Error::MalformedResponse),
        }
        let body = read_body(response).await?;
        let answer: EntryAnswer =
            serde_json::from_slice(&body).map_err(|_error| Error::MalformedResponse)?;
        // Vault answers a deleted latest version with no fields.
        let fields = answer.data.data.ok_or(Error::EntryNotFound)?;
        match fields.0.get(key) {
            Some(Value::String(value)) => {
                Secret::new(value.clone()).map_err(|_error| Error::MalformedResponse)
            }
            Some(_) => Err(Error::MalformedResponse),
            None => Err(Error::KeyNotFound),
        }
    }
}

#[async_trait]
impl SecretStore for KvV2Store {
    async fn read(&self, reference: &SecretReference) -> Result<Secret, Error> {
        let key = reference.key().ok_or(Error::KeyNotFound)?;
        let segments = safe_segments(reference.path()).ok_or(Error::EntryNotFound)?;
        let url = self.url(
            self.mount
                .iter()
                .map(String::as_str)
                .chain(["data"])
                .chain(segments),
        )?;
        let token = self.token().await?;
        let denied = match self.read_entry(&url, &token.value, key).await {
            Err(Error::PermissionDenied) if !token.from_login => Error::PermissionDenied,
            result => return result,
        };
        if self.token_is_live(&token.value).await? {
            return Err(denied);
        }
        self.forget(&token.value).await;
        let token = self.token().await?;
        self.read_entry(&url, &token.value, key).await
    }
}

fn token_header(token: &Secret) -> Result<HeaderValue, Error> {
    let mut header =
        HeaderValue::from_str(token.expose()).map_err(|_error| Error::MalformedResponse)?;
    header.set_sensitive(true);
    Ok(header)
}

async fn auth_token(response: reqwest::Response, now: Instant) -> Result<Token, Error> {
    let body = read_body(response).await?;
    let answer: AuthAnswer =
        serde_json::from_slice(&body).map_err(|_error| Error::MalformedResponse)?;
    Token::new(&answer.auth, now)
}

async fn read_body(response: reqwest::Response) -> Result<Zeroizing<Vec<u8>>, Error> {
    coral_outbound::read_bounded_body(response, BODY_LIMIT)
        .await
        .map(Zeroizing::new)
        .map_err(|error| match error {
            OutboundUrlPolicyError::BodyTooLarge { .. } => Error::MalformedResponse,
            _ => Error::Unreachable,
        })
}

/// An entry's fields, wiped when dropped: the answer holds every value in the
/// entry, including those nobody asked for.
#[derive(Deserialize)]
struct Fields(BTreeMap<String, Value>);

impl Drop for Fields {
    fn drop(&mut self) {
        fn wipe(value: &mut Value) {
            match value {
                Value::String(text) => text.zeroize(),
                Value::Array(items) => items.iter_mut().for_each(wipe),
                Value::Object(fields) => fields.values_mut().for_each(wipe),
                Value::Null | Value::Bool(_) | Value::Number(_) => {}
            }
        }
        self.0.values_mut().for_each(wipe);
    }
}

#[derive(Deserialize)]
struct EntryAnswer {
    data: EntryData,
}

#[derive(Deserialize)]
struct EntryData {
    /// Must be present. Vault answers a deleted latest version with `null`.
    #[serde(deserialize_with = "Option::deserialize")]
    data: Option<Fields>,
}

#[derive(Deserialize)]
struct AuthAnswer {
    auth: Auth,
}

#[derive(Deserialize)]
struct Auth {
    client_token: Zeroizing<String>,
    #[serde(default)]
    lease_duration: u64,
    #[serde(default)]
    renewable: bool,
}

#[cfg(test)]
mod tests;
