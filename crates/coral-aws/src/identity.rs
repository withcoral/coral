//! One AWS identity: its credentials, the account check, and signing.

use std::fmt;
use std::time::{Duration, SystemTime};

use aws_config::sts::AssumeRoleProvider;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sdk_sts::error::SdkError;
use aws_sdk_sts::operation::assume_role::AssumeRoleError;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls;
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use aws_smithy_runtime_api::client::identity::Identity as SigningIdentity;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::ConnectorError;
use tokio::sync::Mutex;

use crate::settings::session_name;
use crate::{Environment, Settings, SettingsError};

/// Credentials this close to expiry are refreshed before use, so that a
/// signed request stays valid while it travels.
const REFRESH_BEFORE_EXPIRY: Duration = Duration::from_mins(5);

/// The longest one credential lookup or STS call may take.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Why an identity could not sign. No variant carries a credential, a token,
/// a signature or AWS's own message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// No credential source gave credentials, or a refresh failed.
    #[error("no AWS credentials could be loaded")]
    NoCredentials,
    /// STS refused the credentials.
    #[error("AWS refused the credentials")]
    Denied,
    /// The credentials belong to another account than `account_id`. Every
    /// use fails so until the credentials are next refreshed.
    #[error("the AWS credentials belong to another account than account_id")]
    WrongAccount,
    /// A credential source or STS did not answer, or throttled the call.
    #[error("AWS did not answer")]
    Unreachable,
    /// The request has a header value that is not visible ASCII.
    #[error("the request cannot be signed")]
    Unsignable,
}

impl Error {
    /// Whether the caller should report this as permission denied: AWS gave
    /// no usable credentials for the expected account.
    #[must_use]
    pub const fn is_permission_denied(self) -> bool {
        matches!(
            self,
            Self::NoCredentials | Self::Denied | Self::WrongAccount
        )
    }
}

/// An AWS identity built from [`Settings`].
///
/// It loads its credentials through `aws-config` on first use, never at
/// construction, and keeps them until they near expiry. After each load it
/// asks STS which account they belong to.
pub struct AwsIdentity {
    settings: Settings,
    session_name: String,
    http_client: SharedHttpClient,
    env: Environment,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    loaded: Option<Loaded>,
    checked: Option<Checked>,
}

/// What `aws-config` built from the settings: the config every STS call uses,
/// and the credentials provider.
struct Loaded {
    config: SdkConfig,
    credentials: SharedCredentialsProvider,
}

/// The credentials in use, and whether they are in `account_id`.
struct Checked {
    credentials: Credentials,
    in_account: bool,
}

impl AwsIdentity {
    /// An identity called `name`: if the settings have a role, its session is
    /// `coral-<name>`. Nothing is read or sent until the first use.
    ///
    /// Its HTTPS calls go through the proxy that `env` names, if any. AWS's
    /// credential addresses for EC2, ECS and EKS are plain HTTP and
    /// link-local, so they never go through a proxy.
    ///
    /// # Errors
    /// Returns [`SettingsError::SessionName`] when `coral-<name>` is not a
    /// valid role session name, and [`SettingsError::HttpsProxy`] or
    /// [`SettingsError::AllProxy`] when the proxy is not a URL.
    pub fn new(settings: Settings, name: &str, env: &Environment) -> Result<Self, SettingsError> {
        Ok(Self {
            settings,
            session_name: session_name(name)?,
            http_client: http_client(env)?,
            env: env.clone(),
            state: Mutex::default(),
        })
    }

    /// Signs `request` with `SigV4` for `service` in `region` at `now`, adding
    /// the signature headers. The caller sends the request, or hands it on
    /// for another party to send.
    ///
    /// # Errors
    /// Returns why no checked credentials were at hand, or
    /// [`Error::Unsignable`].
    pub async fn sign(
        &self,
        request: &mut http::Request<Vec<u8>>,
        service: &str,
        region: &str,
        now: SystemTime,
    ) -> Result<(), Error> {
        let identity: SigningIdentity = self.credentials(now).await?.into();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(region)
            .name(service)
            .time(now)
            .settings(SigningSettings::default())
            .build()
            .map_err(|_incomplete| Error::Unsignable)?
            .into();
        let uri = request.uri().to_string();
        let headers = request
            .headers()
            .iter()
            .map(|(name, value)| Ok((name.as_str(), value.to_str()?)))
            .collect::<Result<Vec<_>, http::header::ToStrError>>()
            .map_err(|_not_ascii| Error::Unsignable)?;
        let signable = SignableRequest::new(
            request.method().as_str(),
            uri,
            headers.into_iter(),
            SignableBody::Bytes(request.body()),
        )
        .map_err(|_unsignable| Error::Unsignable)?;
        let (instructions, _signature) = sign(signable, &params)
            .map_err(|_unsignable| Error::Unsignable)?
            .into_parts();
        instructions.apply_to_request_http1x(request);
        Ok(())
    }

    /// Credentials checked to be in `account_id`, loaded again when the ones
    /// in hand near expiry, and checked again when their access key changes.
    async fn credentials(&self, now: SystemTime) -> Result<Credentials, Error> {
        let mut state = self.state.lock().await;
        let State { loaded, checked } = &mut *state;
        let fresh = checked.as_ref().filter(|checked| {
            checked
                .credentials
                .expiry()
                .is_none_or(|expiry| now + REFRESH_BEFORE_EXPIRY < expiry)
        });
        let checked = if let Some(checked) = fresh {
            checked
        } else {
            // A failed refresh leaves no earlier credentials to fall back to.
            // A source may still hand back unexpired ones it cached, which
            // are good to use until they expire.
            let previous = checked.take();
            let loaded = match loaded {
                Some(loaded) => loaded,
                None => loaded.insert(self.load().await?),
            };
            let credentials = loaded
                .credentials
                .provide_credentials()
                .await
                .map_err(|error| credentials_error(&error))?;
            // When a refresh fails, the EC2 instance metadata provider hands
            // back the credentials it last fetched rather than an error.
            if credentials.expiry().is_some_and(|expiry| expiry <= now) {
                return Err(Error::NoCredentials);
            }
            // An access key belongs to one account, so a provider that hands
            // back the same key near its expiry needs no second check.
            let in_account = match previous {
                Some(previous)
                    if previous.credentials.access_key_id() == credentials.access_key_id() =>
                {
                    previous.in_account
                }
                _ => {
                    caller_account(&loaded.config, credentials.clone()).await?
                        == self.settings.aws_account_id
                }
            };
            checked.insert(Checked {
                credentials,
                in_account,
            })
        };
        if checked.in_account {
            Ok(checked.credentials.clone())
        } else {
            Err(Error::WrongAccount)
        }
    }

    async fn load(&self) -> Result<Loaded, Error> {
        let region = Region::new(self.settings.region.clone());
        let mut loader = aws_config::defaults(BehaviorVersion::v2026_11_17())
            .env(self.env.to_aws())
            .http_client(self.http_client.clone())
            .region(region.clone())
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(CALL_TIMEOUT)
                    .build(),
            );
        if let Some(profile) = &self.settings.profile {
            loader = loader.profile_name(profile);
        }
        let config = loader.load().await;
        let credentials = match &self.settings.role_arn {
            Some(role_arn) => SharedCredentialsProvider::new(
                AssumeRoleProvider::builder(role_arn)
                    .session_name(&self.session_name)
                    .region(region)
                    .configure(&config)
                    .build()
                    .await,
            ),
            None => config.credentials_provider().ok_or(Error::NoCredentials)?,
        };
        Ok(Loaded {
            config,
            credentials,
        })
    }
}

impl fmt::Debug for AwsIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsIdentity")
            .field("settings", &self.settings)
            .field("session_name", &self.session_name)
            .finish_non_exhaustive()
    }
}

/// `aws-config`'s HTTPS client, with the proxy from `env` rather than from
/// the process environment.
fn http_client(env: &Environment) -> Result<SharedHttpClient, SettingsError> {
    let proxy = match env.https_proxy() {
        Some((variable, proxy, bypass)) => {
            let proxy = ProxyConfig::https(proxy).map_err(|_not_a_url| {
                if variable == "ALL_PROXY" {
                    SettingsError::AllProxy
                } else {
                    SettingsError::HttpsProxy
                }
            })?;
            match bypass {
                Some(bypass) => proxy.no_proxy(bypass),
                None => proxy,
            }
        }
        None => ProxyConfig::disabled(),
    };
    Ok(
        aws_smithy_http_client::Builder::new().build_with_connector_fn(
            move |settings, components| {
                let mut connector = aws_smithy_http_client::Connector::builder().tls_provider(
                    tls::Provider::Rustls(tls::rustls_provider::CryptoMode::AwsLc),
                );
                connector.set_connector_settings(settings.cloned());
                if let Some(components) = components {
                    connector.set_sleep_impl(components.sleep_impl());
                }
                connector.set_proxy_config(Some(proxy.clone()));
                connector.build()
            },
        ),
    )
}

/// The account `credentials` belong to, by STS `GetCallerIdentity`.
async fn caller_account(config: &SdkConfig, credentials: Credentials) -> Result<String, Error> {
    let config = aws_sdk_sts::config::Builder::from(config)
        .credentials_provider(credentials)
        .build();
    let output = aws_sdk_sts::Client::from_conf(config)
        .get_caller_identity()
        .send()
        .await
        .map_err(|error| match error {
            SdkError::ServiceError(error)
                if error.raw().status().is_client_error()
                    && error.raw().status().as_u16() != 429
                    && !is_throttling(error.err().meta().code()) =>
            {
                Error::Denied
            }
            _ => Error::Unreachable,
        })?;
    output.account.ok_or(Error::Unreachable)
}

fn is_throttling(code: Option<&str>) -> bool {
    matches!(code, Some("Throttling" | "ThrottlingException"))
}

/// Why no credentials came: an outage when a source could not be reached
/// or STS was down or throttling, and otherwise none to be had, such as a
/// refusal or a failed `credential_process`.
fn credentials_error(error: &CredentialsError) -> Error {
    if matches!(error, CredentialsError::ProviderTimedOut(_)) {
        return Error::Unreachable;
    }
    let mut cause: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(error);
    while let Some(current) = cause {
        if current.is::<ConnectorError>() {
            return Error::Unreachable;
        }
        if let Some(SdkError::ServiceError(failure)) =
            current.downcast_ref::<SdkError<AssumeRoleError, HttpResponse>>()
        {
            let status = failure.raw().status();
            if status.is_server_error()
                || status.as_u16() == 429
                || is_throttling(failure.err().meta().code())
            {
                return Error::Unreachable;
            }
        }
        if let Some(SdkError::TimeoutError(_) | SdkError::DispatchFailure(_)) =
            current.downcast_ref::<SdkError<AssumeRoleError, HttpResponse>>()
        {
            return Error::Unreachable;
        }
        cause = current.source();
    }
    Error::NoCredentials
}
