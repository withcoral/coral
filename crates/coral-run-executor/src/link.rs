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
use tokio::sync::{Notify, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::http::Uri;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Streaming};
use tonic_health::ServingStatus;

use crate::compile_wire::{self, ProtocolError};
use crate::run::{self, RunLimits};
use crate::wire::v1::link_service_client::LinkServiceClient;
use crate::wire::v1::{
    Compile, Compiled, Hello, Limits, PullRequest, PullResponse, Room, TakenRun, pull_request,
    pull_response,
};
use crate::{CompilerClient, InProcessCompiler};

/// A client of Coral on one connection: every call carries the bearer
/// credential.
pub(crate) type Client = LinkServiceClient<InterceptedService<Channel, Bearer>>;

/// The connection every Run stream opens on.
pub(crate) enum Connection {
    /// No pull stream Coral welcomed holds: a Run stream waits for one.
    Waiting,
    /// Coral welcomed the pull stream on this connection.
    Welcomed(Client),
    /// Coral refused the token: a lost Run stream reopens no more.
    Refused,
}
/// The Run Executor's side of a pull stream.
type Outbound = mpsc::Sender<PullRequest>;

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

/// Whether a peer at `level` may share a stream: its level equals this one
/// or differs by one.
pub(crate) const fn compatible(level: u32) -> bool {
    level.abs_diff(PROTOCOL_LEVEL) <= 1
}

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
    /// Coral started it as a local child, its token down stdin. Once it is
    /// ordered to retire, a dial that opens no connection means its parent
    /// is gone:
    /// it exits at once, and Coral's sweeper ends the Runs it holds.
    pub local_child: bool,
    /// The most Runs it holds at once: its free room while it holds none.
    pub max_runs_held: u32,
    /// What each Run runs within.
    pub run_limits: RunLimits,
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
    /// Coral refused the token, so the Run Executor retired.
    #[error("Coral refused the Run Executor's token")]
    Refused,
    /// The local child retired and its parent's address opened no
    /// connection: the Coral that started it is gone.
    #[error("the Coral that started this local Run Executor is gone")]
    ParentGone,
}

/// Warms the Program Compiler, then holds the link to Coral until it has
/// retired: once `retire` completes, Coral refuses its token, or a Program
/// misses its stop, it reports no room and returns after its last Run ends.
/// A retired local child returns at once when its parent is gone. Serves
/// the gRPC health service on `health`, when given, until then.
///
/// # Errors
///
/// Returns a compiler that cannot start, before any dial, the health
/// listener's failure, [`RunError::Refused`] once retired after Coral
/// refused the token, or [`RunError::ParentGone`].
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
            Some(listener) => {
                serve_health(listener, linked_rx, link.runs.holding.subscribe()).await
            }
            None => pending().await,
        }
    };
    let retire = async {
        retire.await;
        link.runs.order_to_retire();
        pending::<std::convert::Infallible>().await
    };
    tokio::select! {
        held = link.hold() => held,
        result = health => result.map_err(RunError::Health),
        never = retire => match never {},
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

/// Puts the token on every call.
#[derive(Clone)]
pub(crate) struct Bearer(Token);

impl tonic::service::Interceptor for Bearer {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.bearer()?);
        Ok(request)
    }
}

/// How a pull stream or a Run stream ended.
pub(crate) enum End {
    /// Coral ended its side cleanly after its first answer, to move the
    /// stream, or took a Run as a retiring stream closed: reopen at once.
    Moved,
    /// Anything else: redial after the back-off.
    Failed,
    /// The dial opened no connection: redial after the back-off, unless a
    /// retired local child's parent is gone.
    Unreachable,
    /// Coral refused the token: retire, and dial no more.
    Refused,
    /// The Run Executor retired and holds nothing: exit.
    Retired,
}

struct Link {
    endpoint: Endpoint,
    token: Token,
    local_child: bool,
    hello: Hello,
    compiler: Arc<dyn CompilerClient>,
    /// Whether a pull stream Coral welcomed is open.
    linked: watch::Sender<bool>,
    /// Told after each `Compile` is queued, so a test can wait for one.
    #[cfg(test)]
    on_queued: Option<mpsc::UnboundedSender<()>>,
    /// Each Run's limits and isolate, and the warm isolate.
    isolates: Arc<run::Isolates>,
    runs: Arc<Runs>,
    current: watch::Sender<Connection>,
}

/// The Runs a Run Executor holds, and whether it retires.
#[derive(Default)]
pub(crate) struct Runs {
    pub(crate) holding: watch::Sender<Holding>,
    /// A held Run's unit freed.
    freed: Notify,
}

/// What [`Runs`] counts.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Holding {
    /// Units of room held, each from its `Taken` until its isolate is gone.
    held: u32,
    /// Run streams waited for, each from its `Taken` until it ends or its
    /// isolate misses its stop.
    streams: u32,
    /// The Run Executor retires.
    retiring: bool,
    /// The order to retire came from outside, a closed stdin or a signal,
    /// not from an isolate that missed its stop.
    ordered: bool,
}

impl Holding {
    /// Retiring, with no Run stream to wait for.
    fn drained(&self) -> bool {
        self.retiring && self.streams == 0
    }

    #[cfg(test)]
    pub(crate) fn held(&self) -> u32 {
        self.held
    }
}

impl Runs {
    /// The order to retire from outside the Run Executor.
    fn order_to_retire(&self) {
        self.holding.send_if_modified(|holding| {
            let changed = !(holding.retiring && holding.ordered);
            (holding.retiring, holding.ordered) = (true, true);
            changed
        });
    }

    /// Orders the Run Executor to retire.
    pub(crate) fn retire(&self) {
        self.holding
            .send_if_modified(|holding| !std::mem::replace(&mut holding.retiring, true));
    }

    /// Once the Run Executor retired and holds nothing.
    async fn drained(&self) {
        let mut holding = self.holding.subscribe();
        // The sender lives as long as `self`. The read lock drops at once.
        drop(holding.wait_for(Holding::drained).await);
    }
}

/// One Run's hold on the Run Executor: its unit of room and its stream.
pub(crate) struct HeldRun {
    pub(crate) unit: HeldUnit,
    pub(crate) stream: HeldRunStream,
}

/// One unit of room a held Run takes. It frees when dropped: by the Run's
/// Program thread once its isolate is gone, or by the Run's stream task when
/// no Program started, even by a panic.
pub(crate) struct HeldUnit(Arc<Runs>);

/// One Run a retiring Run Executor waits for, until it ends here, across its
/// reopened streams.
pub(crate) struct HeldRunStream(Arc<Runs>);

impl HeldRunStream {
    /// The Run's isolate missed its stop: the Run Executor retires, and no
    /// longer waits for this Run.
    pub(crate) fn abandon(self) {
        self.0.retire();
    }
}

impl HeldRun {
    pub(crate) fn take(runs: &Arc<Runs>) -> Self {
        runs.holding.send_modify(|holding| {
            holding.held = holding.held.saturating_add(1);
            holding.streams = holding.streams.saturating_add(1);
        });
        Self {
            unit: HeldUnit(runs.clone()),
            stream: HeldRunStream(runs.clone()),
        }
    }
}

impl Drop for HeldUnit {
    fn drop(&mut self) {
        self.0
            .holding
            .send_modify(|holding| holding.held = holding.held.saturating_sub(1));
        self.0.freed.notify_one();
    }
}

impl Drop for HeldRunStream {
    fn drop(&mut self) {
        self.0
            .holding
            .send_modify(|holding| holding.streams = holding.streams.saturating_sub(1));
    }
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
            local_child: config.local_child,
            hello,
            compiler,
            linked,
            isolates: Arc::new(run::Isolates::new(config.run_limits, config.max_runs_held)),
            runs: Arc::default(),
            current: watch::Sender::new(Connection::Waiting),
            #[cfg(test)]
            on_queued: None,
        }
    }

    /// The units of room free for Runs: none while it retires. Compiles
    /// take none, since they run on the one warm compiler, not in a Run's
    /// room. A `Taken` that crossed a report of none holds a unit above the
    /// room.
    fn free(&self) -> u32 {
        let holding = *self.runs.holding.borrow();
        if holding.retiring {
            return 0;
        }
        self.hello.free.saturating_sub(holding.held)
    }

    /// Opens the pull stream again each time it ends, until it has retired,
    /// or until a retired local child's parent is gone.
    async fn hold(&self) -> Result<(), RunError> {
        let mut backoff = Backoff::default();
        loop {
            let end = self.pull().await;
            self.current.send_replace(Connection::Waiting);
            if self
                .linked
                .send_if_modified(|linked| std::mem::replace(linked, false))
            {
                // Coral welcomed the stream that ended.
                backoff.reset();
            }
            match end {
                End::Retired => return Ok(()),
                End::Refused => {
                    tracing::warn!("Coral refused the Run Executor's token; retiring");
                    self.current.send_replace(Connection::Refused);
                    self.runs.retire();
                    self.runs.drained().await;
                    return Err(RunError::Refused);
                }
                End::Unreachable if self.local_child && self.runs.holding.borrow().ordered => {
                    // Its stdin closed and nothing listens at its parent's
                    // address: no Coral is left to end its Runs here.
                    tracing::warn!("the Coral that started this Run Executor is gone; exiting");
                    return Err(RunError::ParentGone);
                }
                End::Moved | End::Failed | End::Unreachable => {}
            }
            if let Some(delay) = backoff.after(&end) {
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = self.runs.drained() => return Ok(()),
                }
            }
        }
    }

    /// Coral welcomed the pull stream on `client`'s connection: every Run
    /// stream opens on it while it holds.
    fn welcomed(&self, client: &Client) {
        self.linked.send_replace(true);
        self.current
            .send_replace(Connection::Welcomed(client.clone()));
    }

    /// Holds one Run Coral took, from its unit of room to its end.
    fn hold_run(&self, taken: TakenRun) {
        let held = HeldRun::take(&self.runs);
        let connection = self.current.subscribe();
        tokio::spawn(run::hold(connection, taken, self.isolates.clone(), held));
    }

    /// Dials a fresh connection and opens the pull stream on it with Hello,
    /// unless the Run Executor retired and holds nothing before it connects.
    async fn open(&self) -> Result<(Client, Outbound, Streaming<PullResponse>), End> {
        let connected = tokio::select! {
            connected = self.endpoint.connect() => connected,
            () = self.runs.drained() => return Err(End::Retired),
        };
        let Ok(channel) = connected else {
            return Err(End::Unreachable);
        };
        let mut client = LinkServiceClient::with_interceptor(channel, Bearer(self.token.clone()))
            .max_decoding_message_size(MESSAGE_MAX_BYTES)
            .max_encoding_message_size(MESSAGE_MAX_BYTES);
        // The Run Executor's side of the stream ends when `outbound` drops,
        // as this call returns. The room it reports leaves out the Runs it
        // holds, though the last stream's compile can still hold the
        // compiler thread: it finishes unanswered, as a report and a take can
        // cross.
        let (outbound, requests) = mpsc::channel(1);
        let hello = PullRequest {
            message: Some(pull_request::Message::Hello(Hello {
                free: self.free(),
                ..self.hello.clone()
            })),
        };
        if outbound.try_send(hello).is_err() {
            return Err(End::Failed);
        }
        // Coral may take Runs on a Hello it read: its answer is awaited, so a
        // retire reads them on the close.
        match client.pull(ReceiverStream::new(requests)).await {
            Ok(response) => Ok((client, outbound, response.into_inner())),
            Err(status) if status.code() == Code::Unauthenticated => Err(End::Refused),
            Err(_) => Err(End::Failed),
        }
    }

    /// Closes a retiring pull stream that holds nothing: ends its side, so
    /// its report of none goes out, and reads until Coral ends its own.
    /// Each Run Coral took before then is held, and `End::Moved` redials for it.
    async fn close(
        &self,
        client: &Client,
        outbound: Outbound,
        mut inbound: Streaming<PullResponse>,
    ) -> End {
        drop(outbound);
        while let Ok(Some(response)) = inbound.message().await {
            match response.message {
                // A Welcome that crossed the close: its Runs are held.
                Some(pull_response::Message::Welcome(welcome))
                    if !*self.linked.borrow() && compatible(welcome.protocol_level) =>
                {
                    self.welcomed(client);
                }
                Some(pull_response::Message::Taken(taken)) if *self.linked.borrow() => {
                    taken
                        .runs
                        .into_iter()
                        .for_each(|taken| self.hold_run(taken));
                }
                _ => {}
            }
        }
        if self.runs.holding.borrow().drained() {
            End::Retired
        } else {
            End::Moved
        }
    }

    /// Retiring is a report of none, sent once the stream is welcomed, even
    /// when it holds nothing. `End::Retired` once it holds no Run and no
    /// compile: the stream closes.
    async fn retiring(
        &self,
        outbound: &Outbound,
        welcomed: bool,
        compiling: u32,
        reported: &mut bool,
    ) -> Option<End> {
        if welcomed && !*reported {
            let room = PullRequest {
                message: Some(pull_request::Message::Room(Room { free: 0 })),
            };
            if outbound.send(room).await.is_err() {
                return Some(End::Failed);
            }
            *reported = true;
        }
        let drained = self.runs.holding.borrow().drained();
        (drained && compiling == 0).then_some(End::Retired)
    }

    /// Holds a fresh pull stream until it ends, or until the Run Executor
    /// retired and holds nothing.
    async fn pull(&self) -> End {
        let mut holding = self.runs.holding.subscribe();
        let (client, outbound, mut inbound) = match self.open().await {
            Ok(opened) => opened,
            Err(end) => return end,
        };
        // Whether this stream reported the retire. A Hello while it retires
        // reported none already, so a second report repeats it.
        let mut reported = false;
        let (queue, queued) = mpsc::unbounded_channel();
        let (answered, mut answers) = mpsc::unbounded_channel();
        tokio::spawn(serve(Arc::clone(&self.compiler), queued, answered));
        // The compiles from Compile to Compiled, which the retire waits for.
        let mut compiling: u32 = 0;
        loop {
            let welcomed = *self.linked.borrow();
            let retiring = holding.borrow_and_update().retiring;
            if retiring {
                match self
                    .retiring(&outbound, welcomed, compiling, &mut reported)
                    .await
                {
                    Some(End::Retired) => return self.close(&client, outbound, inbound).await,
                    Some(end) => return end,
                    None => {}
                }
            }
            tokio::select! {
                // The order to retire, and each Run that ends.
                _ = holding.changed() => {}
                // A Run that freed before Hello leaves a permit here too, so
                // a Room may repeat the room Hello reported. A free between
                // Hello and Welcome reaches Coral only this way.
                () = self.runs.freed.notified(), if welcomed && !retiring => {
                    let room = PullRequest {
                        message: Some(pull_request::Message::Room(Room {
                            free: self.free(),
                        })),
                    };
                    if outbound.send(room).await.is_err() {
                        return End::Failed;
                    }
                }
                message = inbound.message() => match message {
                    Ok(None) if welcomed => return End::Moved,
                    Ok(Some(PullResponse {
                        message: Some(pull_response::Message::Welcome(welcome)),
                    })) if !welcomed => {
                        if !compatible(welcome.protocol_level) {
                            tracing::warn!(
                                coral.run_executor.protocol_level = safe::number(PROTOCOL_LEVEL),
                                coral.run_executor.peer_protocol_level =
                                    safe::number(welcome.protocol_level),
                                "Coral's protocol level is more than one apart; closing the pull stream"
                            );
                            return End::Failed;
                        }
                        self.welcomed(&client);
                    }
                    Ok(Some(PullResponse {
                        message: Some(pull_response::Message::Compile(compile)),
                    })) if welcomed => {
                        compiling = compiling.saturating_add(1);
                        if queue.send((Instant::now(), compile)).is_err() {
                            return End::Failed;
                        }
                        #[cfg(test)]
                        if let Some(on_queued) = &self.on_queued {
                            // A test that waits for none drops its receiver.
                            let _unwatched = on_queued.send(()).is_err();
                        }
                    }
                    Ok(Some(PullResponse {
                        message: Some(pull_response::Message::Taken(taken)),
                    })) if welcomed => {
                        // A Run that crossed a report of none is honored, as
                        // is one that crossed the stream's close.
                        for taken in taken.runs {
                            self.hold_run(taken);
                        }
                    }
                    // Anything else breaks the protocol. A clean end before
                    // Welcome is no move.
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
                    compiling = compiling.saturating_sub(1);
                    // While it retires, the room stays none. A Coral Instance
                    // of the release before spends a unit of room on each
                    // compile, and this report gives it back.
                    let free = self.free();
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
/// half, and reset once a stream Coral answered ends. The pull stream and
/// each Run stream keep one.
pub(crate) struct Backoff {
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
    pub(crate) fn after(&mut self, end: &End) -> Option<Duration> {
        if let End::Moved | End::Refused | End::Retired = end {
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

    pub(crate) fn reset(&mut self) {
        self.next = FIRST_BACKOFF;
    }
}

/// Serves the standard gRPC health service on `listener`: the readiness
/// service serves while `linked` holds and the Run Executor does not retire.
async fn serve_health(
    listener: TcpListener,
    mut linked: watch::Receiver<bool>,
    mut holding: watch::Receiver<Holding>,
) -> Result<(), tonic::transport::Error> {
    let (reporter, service) = tonic_health::server::health_reporter();
    let report = async move {
        loop {
            let retiring = holding.borrow_and_update().retiring;
            let status = if *linked.borrow_and_update() && !retiring {
                ServingStatus::Serving
            } else {
                ServingStatus::NotServing
            };
            reporter.set_service_status(READINESS_SERVICE, status).await;
            // Retiring never ends, so it is told once.
            let changed = tokio::select! {
                changed = linked.changed() => changed,
                retired = holding.wait_for(|holding| holding.retiring), if !retiring => {
                    retired.map(drop)
                }
            };
            if changed.is_err() {
                break;
            }
        }
    };
    let serve = Server::builder()
        .add_service(service)
        .serve_with_incoming(TcpIncoming::from(listener));
    tokio::select! {
        result = serve => result,
        // `linked` and `holding` close only as `run` returns.
        () = report => pending().await,
    }
}

#[cfg(test)]
mod tests;
