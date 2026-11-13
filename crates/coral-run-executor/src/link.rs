//! The Run Executor's side of the link: one HTTP/2 connection to Coral's one
//! address, and the pull stream on it.

use std::future::{Future, pending};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use coral_credentials::Secret;
use coral_telemetry::safe;
use coral_v8::IsolateLimits;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::http::Uri;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Endpoint, Server};
use tonic_health::ServingStatus;

use crate::compile_wire::{self, ProtocolError};
use crate::wire::v1::link_service_client::LinkServiceClient;
use crate::wire::v1::{
    Compile, Compiled, Hello, Limits, PullRequest, PullResponse, Room, pull_request, pull_response,
};
use crate::{CompilerClient, InProcessCompiler};

/// The level of the link's messages and the Program Runtime Surface. A
/// release that changes either raises it by exactly one.
pub const PROTOCOL_LEVEL: u32 = 1;
/// The largest message either side of the link sends or accepts: the Run
/// Executor's client and Coral's listener, [`crate::LinkServer`], both set
/// it. It holds a compile's TypeScript, at most 512 KiB in all, and a
/// package well past the 1 MiB script and 2 MiB source map Coral accepts, so
/// Coral, not the link, refuses an oversized package. A typed prepare carries
/// at most 1 MiB of Tool declarations, but one declaration of up to 256 KiB
/// for each Private or Shared Tool it calls, with no total bound: Coral sends no
/// `Compile` past this one, and that call alone fails.
pub const MESSAGE_MAX_BYTES: usize = 16 * 1024 * 1024;

/// The longest Run this Run Executor holds: the Run deadline's maximum.
const MAX_RUN_DURATION_SECONDS: u32 = 900;
/// A PING after 10 s without a frame, and the connection closed when the
/// PING goes 10 s unanswered: 20 s silent in all.
pub(crate) const KEEPALIVE: Duration = Duration::from_secs(10);
/// A dial that opens no connection in 20 s fails, as a silent connection
/// closes after 20 s.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
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
    /// The Program Compiler's heap limit, in bytes.
    pub compiler_heap_bytes: usize,
}

/// Why a Run Executor stopped before it retired.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The warm Program Compiler could not start.
    #[error("the Program Compiler could not start")]
    Compiler(#[source] coral_v8::Error),
    /// The health listener failed.
    #[error("the health listener failed")]
    Health(#[source] tonic::transport::Error),
}

/// Warms the Program Compiler, then holds the link to Coral until `retire`
/// completes. Serves the gRPC health service on `health`, when given, until
/// then.
///
/// # Errors
///
/// Returns a compiler that cannot start, before any dial, or the health
/// listener's failure.
pub async fn run(
    config: RunExecutorConfig,
    health: Option<TcpListener>,
    retire: impl Future<Output = ()>,
) -> Result<(), RunError> {
    let limits = IsolateLimits::new(
        IsolateLimits::defaults().execution_heap_bytes(),
        config.compiler_heap_bytes,
    )
    .map_err(RunError::Compiler)?;
    // `new` returns once the compiler is warm, so Hello waits for it.
    let compiler = tokio::task::spawn_blocking(move || InProcessCompiler::new(limits))
        .await
        .map_err(|_panicked| RunError::Compiler(coral_v8::Error::CompilerUnavailable))?
        .map_err(RunError::Compiler)?;
    let (linked, linked_rx) = watch::channel(false);
    let link = Link::new(config, Arc::new(compiler), linked);
    let health = async {
        match health {
            Some(listener) => serve_health(listener, linked_rx).await,
            None => pending().await,
        }
    };
    tokio::select! {
        () = link.hold() => Ok(()),
        result = health => result.map_err(RunError::Health),
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

/// The bearer credential every call carries, held as a [`Secret`].
#[derive(Clone, Debug)]
pub struct Token(Arc<Secret>);

/// A token that no `authorization` header can carry.
#[derive(Debug, thiserror::Error)]
#[error("the Run Executor token must be non-empty visible ASCII, at most 16 KiB")]
pub struct TokenError;

impl Token {
    /// The credential for `token`.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError`] for an empty token, one outside visible ASCII,
    /// or one longer than 16 KiB.
    pub fn new(token: String) -> Result<Self, TokenError> {
        if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(TokenError);
        }
        let secret = Secret::new(token).map_err(|_invalid| TokenError)?;
        Ok(Self(Arc::new(secret)))
    }

    /// The token itself, for the stdin of the local child that carries it.
    pub(crate) fn expose(&self) -> &str {
        self.0.expose()
    }

    /// The token's SHA-256 verifier, which Coral's side keeps in place of it.
    pub(crate) fn verifier(&self) -> [u8; 32] {
        crate::pull::verifier(self.0.expose())
    }

    /// The `authorization` value for one call.
    fn bearer(&self) -> Result<AsciiMetadataValue, tonic::Status> {
        let mut bearer = AsciiMetadataValue::try_from(format!("Bearer {}", self.0.expose()))
            .map_err(|_invalid| tonic::Status::internal("the token is no header value"))?;
        bearer.set_sensitive(true);
        Ok(bearer)
    }
}

/// How a pull stream ended.
enum End {
    /// Coral ended its side cleanly after Welcome, to move the stream (the
    /// go-away): reopen at once.
    Moved,
    /// Anything else: redial after the back-off.
    Failed,
}

struct Link {
    endpoint: Endpoint,
    token: Token,
    hello: Hello,
    compiler: Arc<dyn CompilerClient>,
    /// Whether a pull stream Coral welcomed is open.
    linked: watch::Sender<bool>,
    /// Told after each `Compile` is queued, so a test can wait for one.
    #[cfg(test)]
    on_queued: Option<mpsc::UnboundedSender<()>>,
}

impl Link {
    fn new(
        config: RunExecutorConfig,
        compiler: Arc<dyn CompilerClient>,
        linked: watch::Sender<bool>,
    ) -> Self {
        let endpoint = Endpoint::from(config.coral_address.0)
            .connect_timeout(CONNECT_TIMEOUT)
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
            compiler,
            linked,
            #[cfg(test)]
            on_queued: None,
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
            if let Some(delay) = backoff.after(&end) {
                tokio::time::sleep(delay).await;
            }
        }
    }

    /// Dials a fresh connection and holds the pull stream on it until it ends.
    async fn pull(&self) -> End {
        let Ok(channel) = self.endpoint.connect().await else {
            return End::Failed;
        };
        let token = self.token.clone();
        let mut client =
            LinkServiceClient::with_interceptor(channel, move |mut request: tonic::Request<()>| {
                request
                    .metadata_mut()
                    .insert("authorization", token.bearer()?);
                Ok(request)
            })
            .max_decoding_message_size(MESSAGE_MAX_BYTES)
            .max_encoding_message_size(MESSAGE_MAX_BYTES);
        // The Run Executor's side of the stream ends when `outbound` drops,
        // as this call returns. The room it reports is its whole room, though
        // the last stream's compile can still hold the compiler thread: it
        // finishes unanswered, as a report and a take can cross.
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
        let (queue, queued) = mpsc::unbounded_channel();
        let (answered, mut answers) = mpsc::unbounded_channel();
        tokio::spawn(serve(Arc::clone(&self.compiler), queued, answered));
        // A compile holds one unit of room from Compile to Compiled. A report
        // and a take can cross, so `held` can pass the room for a moment.
        let mut held: u32 = 0;
        loop {
            let welcomed = *self.linked.borrow();
            tokio::select! {
                message = inbound.message() => match message {
                    Ok(None) if welcomed => return End::Moved,
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
                    Ok(Some(PullResponse {
                        message: Some(pull_response::Message::Compile(compile)),
                    })) if welcomed => {
                        held = held.saturating_add(1);
                        if queue.send((Instant::now(), compile)).is_err() {
                            return End::Failed;
                        }
                        #[cfg(test)]
                        if let Some(on_queued) = &self.on_queued {
                            // A test that waits for none drops its receiver.
                            let _unwatched = on_queued.send(()).is_err();
                        }
                    }
                    // Taken is not served yet, so it breaks the protocol here
                    // as any other message does. A clean end before Welcome
                    // is no move.
                    Ok(_) | Err(_) => return End::Failed,
                },
                answer = answers.recv() => {
                    // A Compile that broke the protocol, or a serving task
                    // that stopped, ends the stream.
                    let compiled = match answer {
                        Some(Ok(compiled)) => compiled,
                        Some(Err(ProtocolError)) => {
                            tracing::warn!("a Compile broke the protocol; closing the pull stream");
                            return End::Failed;
                        }
                        None => return End::Failed,
                    };
                    held = held.saturating_sub(1);
                    let free = self.hello.free.saturating_sub(held);
                    for message in [
                        pull_request::Message::Compiled(compiled),
                        pull_request::Message::Room(Room { free }),
                    ] {
                        let message = PullRequest { message: Some(message) };
                        if outbound.send(message).await.is_err() {
                            return End::Failed;
                        }
                    }
                }
            }
        }
    }
}

/// Serves one pull stream's compiles on the warm compiler, one at a time in
/// the order they came: the queue is the room. It stops once the stream
/// ends, so a call queued on it is not compiled.
async fn serve(
    compiler: Arc<dyn CompilerClient>,
    mut queued: mpsc::UnboundedReceiver<(Instant, Compile)>,
    answered: mpsc::UnboundedSender<Result<Compiled, ProtocolError>>,
) {
    while let Some((received, compile)) = queued.recv().await {
        if answered.is_closed() {
            return;
        }
        let answer = compile_wire::serve(compiler.as_ref(), received, compile).await;
        if answered.send(answer).is_err() {
            return;
        }
    }
}

/// The redial delay: 100 ms doubling to 5 s, each jittered down by up to
/// half, and reset once a pull stream Coral welcomed ends.
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
    /// The wait before the next dial after `end`: none after a move, which
    /// leaves the back-off where it is.
    fn after(&mut self, end: &End) -> Option<Duration> {
        if let End::Moved = end {
            return None;
        }
        let ceiling = self.next;
        self.next = (ceiling * 2).min(MAX_BACKOFF);
        let half = ceiling / 2;
        let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        // A v4 UUID's random bits: the workspace has no random number crate.
        let (random, _) = uuid::Uuid::new_v4().as_u64_pair();
        Some(half + Duration::from_nanos(random % spread.saturating_add(1)))
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
