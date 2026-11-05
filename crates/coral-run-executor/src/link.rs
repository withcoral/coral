//! The Run Executor's side of the link: one HTTP/2 connection to Coral's one
//! address, and the pull stream on it.

use std::future::{Future, pending};
use std::str::FromStr;
use std::time::Duration;

use coral_telemetry::safe;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::http::Uri;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Endpoint, Server};
use tonic_health::ServingStatus;

use crate::wire::v1::link_service_client::LinkServiceClient;
use crate::wire::v1::{Hello, Limits, PullRequest, PullResponse, pull_request, pull_response};

/// The level of the link's messages and the Program Runtime Surface. A
/// release that changes either raises it by exactly one.
pub const PROTOCOL_LEVEL: u32 = 1;

/// The longest Run this Run Executor holds: the Run deadline's maximum.
const MAX_RUN_DURATION_SECONDS: u32 = 900;
/// A PING after 10 s without a frame, and the connection closed when the
/// PING goes 10 s unanswered: 20 s silent in all.
const KEEPALIVE: Duration = Duration::from_secs(10);
/// The first redial waits about this long, and each next one twice as long.
const FIRST_BACKOFF: Duration = Duration::from_millis(100);
/// The longest a redial waits.
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// The health service that reports whether the Run Executor is linked.
const READINESS_SERVICE: &str = "coral.readiness";

/// What a Run Executor needs to hold its link.
#[derive(Debug)]
pub struct RunExecutorConfig {
    /// Coral's one address.
    pub coral_address: CoralAddress,
    /// The credential every call carries.
    pub token: Token,
    /// The most Runs it holds at once: its free room while it holds none.
    pub max_runs_held: u32,
}

/// Holds the link to Coral until `retire` completes. Serves the gRPC health
/// service on `health`, when given, until then.
///
/// # Errors
///
/// Returns the health listener's failure.
pub async fn run(
    config: RunExecutorConfig,
    health: Option<TcpListener>,
    retire: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let (linked, linked_rx) = watch::channel(false);
    let link = Link::new(config, linked);
    let health = async {
        match health {
            Some(listener) => serve_health(listener, linked_rx).await,
            None => pending().await,
        }
    };
    tokio::select! {
        () = link.hold() => Ok(()),
        result = health => result,
        () = retire => Ok(()),
    }
}

/// Coral's one address: `http://` only for now.
#[derive(Clone, Debug)]
pub struct CoralAddress(Uri);

/// An address the Run Executor cannot dial.
#[derive(Debug, thiserror::Error)]
#[error("the Coral address must be an http:// URL with a host")]
pub struct AddressError;

impl FromStr for CoralAddress {
    type Err = AddressError;

    fn from_str(address: &str) -> Result<Self, Self::Err> {
        let uri = Uri::from_str(address).map_err(|_invalid| AddressError)?;
        if uri.scheme_str() == Some("http") && uri.host().is_some() {
            Ok(Self(uri))
        } else {
            Err(AddressError)
        }
    }
}

/// The bearer credential every call carries. Its `Debug` hides it.
#[derive(Clone)]
pub struct Token(AsciiMetadataValue);

/// A token that no `authorization` header can carry.
#[derive(Debug, thiserror::Error)]
#[error("the Run Executor token must be non-empty visible ASCII")]
pub struct TokenError;

impl Token {
    /// The credential for `token`.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError`] for an empty token, or one outside visible ASCII.
    pub fn new(token: &str) -> Result<Self, TokenError> {
        if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(TokenError);
        }
        let mut bearer = AsciiMetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|_invalid| TokenError)?;
        bearer.set_sensitive(true);
        Ok(Self(bearer))
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Token(..)")
    }
}

/// How a pull stream ended.
enum End {
    /// Coral ended its side cleanly: reopen at once.
    GoAway,
    /// Anything else: redial after the back-off.
    Failed,
}

struct Link {
    endpoint: Endpoint,
    token: Token,
    hello: Hello,
    /// Whether a pull stream Coral welcomed is open.
    linked: watch::Sender<bool>,
}

impl Link {
    fn new(config: RunExecutorConfig, linked: watch::Sender<bool>) -> Self {
        let endpoint = Endpoint::from(config.coral_address.0)
            .http2_keep_alive_interval(KEEPALIVE)
            .keep_alive_timeout(KEEPALIVE);
        let hello = Hello {
            run_executor_id: uuid::Uuid::new_v4().to_string(),
            protocol_level: PROTOCOL_LEVEL,
            free: config.max_runs_held,
            limits: Some(Limits {
                max_run_duration_seconds: MAX_RUN_DURATION_SECONDS,
                fixed: None,
            }),
        };
        Self {
            endpoint,
            token: config.token,
            hello,
            linked,
        }
    }

    /// Opens the pull stream again each time it ends, forever.
    async fn hold(&self) {
        let mut backoff = Backoff::default();
        loop {
            let end = self.pull().await;
            if self
                .linked
                .send_if_modified(|linked| std::mem::replace(linked, false))
            {
                // Coral welcomed the stream that ended.
                backoff.reset();
            }
            if let End::Failed = end {
                tokio::time::sleep(backoff.next_delay()).await;
            }
        }
    }

    /// Dials a fresh connection and holds the pull stream on it until it ends.
    async fn pull(&self) -> End {
        let Ok(channel) = self.endpoint.connect().await else {
            return End::Failed;
        };
        let bearer = self.token.0.clone();
        let mut client =
            LinkServiceClient::with_interceptor(channel, move |mut request: tonic::Request<()>| {
                request
                    .metadata_mut()
                    .insert("authorization", bearer.clone());
                Ok(request)
            });
        // The Run Executor's side of the stream ends when `outbound` drops,
        // as this call returns. The room it reports is its whole room: it
        // holds nothing yet.
        let (outbound, requests) = mpsc::channel(1);
        let hello = PullRequest {
            message: Some(pull_request::Message::Hello(self.hello.clone())),
        };
        if outbound.try_send(hello).is_err() {
            return End::Failed;
        }
        let Ok(response) = client.pull(ReceiverStream::new(requests)).await else {
            return End::Failed;
        };
        let mut inbound = response.into_inner();
        loop {
            let welcomed = *self.linked.borrow();
            match inbound.message().await {
                Ok(None) if welcomed => return End::GoAway,
                Ok(Some(PullResponse {
                    message: Some(pull_response::Message::Welcome(welcome)),
                })) if !welcomed => {
                    if welcome.protocol_level.abs_diff(PROTOCOL_LEVEL) > 1 {
                        tracing::warn!(
                            coral.run_executor.protocol_level = safe::number(PROTOCOL_LEVEL),
                            coral.run_executor.peer_protocol_level =
                                safe::number(welcome.protocol_level),
                            "Coral's protocol level is more than one apart; closing the pull stream"
                        );
                        return End::Failed;
                    }
                    self.linked.send_replace(true);
                }
                // Taken and Compile are not served yet, so they break the
                // protocol here as any other message does. A clean end
                // before Welcome is no go-away.
                Ok(_) | Err(_) => return End::Failed,
            }
        }
    }
}

/// The redial delay: 100 ms doubling to 5 s, each jittered down by up to
/// half, and reset once Coral welcomes a pull stream.
struct Backoff {
    next: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            next: FIRST_BACKOFF,
        }
    }
}

impl Backoff {
    fn next_delay(&mut self) -> Duration {
        let ceiling = self.next;
        self.next = (ceiling * 2).min(MAX_BACKOFF);
        let half = ceiling / 2;
        let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        let (random, _) = uuid::Uuid::new_v4().as_u64_pair();
        half + Duration::from_nanos(random % spread.saturating_add(1))
    }

    fn reset(&mut self) {
        self.next = FIRST_BACKOFF;
    }
}

/// Serves the standard gRPC health service on `listener`: the readiness
/// service serves while `linked` holds.
async fn serve_health(
    listener: TcpListener,
    mut linked: watch::Receiver<bool>,
) -> Result<(), tonic::transport::Error> {
    let (reporter, service) = tonic_health::server::health_reporter();
    let report = async move {
        loop {
            let status = if *linked.borrow_and_update() {
                ServingStatus::Serving
            } else {
                ServingStatus::NotServing
            };
            reporter.set_service_status(READINESS_SERVICE, status).await;
            if linked.changed().await.is_err() {
                break;
            }
        }
    };
    let serve = Server::builder()
        .add_service(service)
        .serve_with_incoming(TcpIncoming::from(listener));
    tokio::select! {
        result = serve => result,
        // `linked` closes only as `run` returns.
        () = report => pending().await,
    }
}

#[cfg(test)]
mod tests;
