//! Coral's side of the link: the listener that Run Executors dial, the
//! credentials it admits, and the pull streams this Coral Instance holds.

use std::collections::HashMap;
use std::future::{Future, pending};
use std::hash::{BuildHasher as _, RandomState};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, mpsc as sync_mpsc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_db::Database;
use coral_telemetry::safe;
use sha2::{Digest as _, Sha256};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, Sleep, interval_at, sleep_until, timeout};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Code, Request, Response, Status, Streaming};

use crate::compile_wire::{ProtocolError, Read, Takes};
use crate::link::{KEEPALIVE, Token};
use crate::wire::v1::compile::Call;
use crate::wire::v1::link_service_server::{LinkService, LinkServiceServer};
use crate::wire::v1::{
    Compile, Compiled, PullRequest, PullResponse, Taken, TakenRun, Welcome, pull_request,
    pull_response,
};
use crate::{MESSAGE_MAX_BYTES, PROTOCOL_LEVEL, lease_repository};

/// How long the listener keeps a connection, and a pull stream its open
/// time: then a GOAWAY, and the pull stream ends cleanly (the move).
pub(crate) const CONNECTION_AGE: Duration = Duration::from_mins(5);
/// How far past each renewal a Run Executor's lease lasts.
pub(crate) const LEASE: Duration = Duration::from_secs(60);
/// How often the holder of a pull stream renews its Run Executor's lease.
pub(crate) const RENEWAL: Duration = Duration::from_secs(20);
/// HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE`, as Coral's private listener sets
/// it: room for a token as long as [`Token`] admits.
const HTTP2_MAX_HEADER_LIST_SIZE: u32 = 128 * 1024;

/// The link's periods: [`CONNECTION_AGE`], [`LEASE`] and [`RENEWAL`] by
/// default.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Timing {
    /// How long a connection and a pull stream stay open.
    pub(crate) connection_age: Duration,
    /// How far past each renewal a lease lasts.
    pub(crate) lease: Duration,
    /// How often a held pull stream renews its lease.
    pub(crate) renewal: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            connection_age: CONNECTION_AGE,
            lease: LEASE,
            renewal: RENEWAL,
        }
    }
}

/// Which credential admitted a Run Executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A local child's token, which Coral wrote to its stdin.
    Local,
    /// An operator token: a Run Executor that Coral did not start.
    Dialed,
}

type Verifier = [u8; 32];

pub(crate) fn verifier(token: &str) -> Verifier {
    Sha256::digest(token.as_bytes()).into()
}

/// The credentials the listener admits, each kept as its SHA-256 verifier:
/// the operator's tokens, and one per live local child.
#[derive(Clone, Default)]
pub struct Credentials(Arc<Mutex<HashMap<Verifier, Kind>>>);

impl Credentials {
    /// Admits each of the operator's `tokens`, two during a rotation, as
    /// [`Kind::Dialed`].
    #[must_use]
    pub fn new(tokens: impl IntoIterator<Item = Token>) -> Self {
        let credentials = Self::default();
        lock(&credentials.0).extend(
            tokens
                .into_iter()
                .map(|token| (token.verifier(), Kind::Dialed)),
        );
        credentials
    }

    /// Admits a local child's `token` as [`Kind::Local`] until the returned
    /// credential drops, when the child exits.
    #[must_use]
    pub fn admit_local(&self, token: &Token) -> LocalCredential {
        let verifier = token.verifier();
        lock(&self.0).insert(verifier, Kind::Local);
        LocalCredential {
            credentials: self.clone(),
            verifier,
        }
    }

    /// The kind `authorization`, a request's header value, admits, and the
    /// credential that admits it.
    pub(crate) fn admit(&self, authorization: Option<&str>) -> Option<(Kind, CredentialId)> {
        let (scheme, token) = authorization?.split_once(' ')?;
        // An authentication scheme's name ignores case.
        if !scheme.eq_ignore_ascii_case("Bearer") {
            return None;
        }
        let verifier = verifier(token);
        let kind = lock(&self.0).get(&verifier).copied()?;
        Some((kind, CredentialId(verifier)))
    }
}

/// Which admitted credential opened a pull stream: how the local child
/// supervisor finds its child's stream.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct CredentialId(Verifier);

/// A dialed admission for a stream a test inserts without the listener.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn dialed_admission() -> (Kind, CredentialId) {
    (Kind::Dialed, CredentialId(verifier("dialed")))
}

impl std::fmt::Debug for CredentialId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CredentialId")
    }
}

/// A local child's credential, admitted until this drops.
pub struct LocalCredential {
    credentials: Credentials,
    verifier: Verifier,
}

impl LocalCredential {
    /// The id its pull streams carry.
    pub(crate) fn id(&self) -> CredentialId {
        CredentialId(self.verifier)
    }
}

impl Drop for LocalCredential {
    fn drop(&mut self) {
        lock(&self.credentials.0).remove(&self.verifier);
    }
}

/// One pull stream this Coral Instance holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamId(u64);

/// A pull stream that takes sends, with its room.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldStream {
    /// The stream.
    pub stream: StreamId,
    /// The id its Run Executor declared, for operators only.
    pub run_executor_id: String,
    /// The kind of credential that admitted it.
    pub kind: Kind,
    /// The credential that admitted it.
    pub(crate) credential: CredentialId,
    /// The units Coral may still send it.
    pub free: u32,
    /// The room its `Hello` declared, which no send or report changes.
    pub(crate) hello_free: u32,
    /// When it last took a unit or freed one: Coral sent it a Run or a
    /// compile, or it reported its room. None since it opened, which is no
    /// work: a stream reopens at each connection age.
    pub(crate) last_work: Option<Instant>,
}

/// Why a send went nowhere.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refused {
    /// The Run Executor's room is less than the send's units.
    #[error("the Run Executor has no room for it")]
    NoRoom,
    /// The pull stream ended, or is ending.
    #[error("the pull stream is closed")]
    Closed,
    /// The message is past [`MESSAGE_MAX_BYTES`]. Sent, it would end the
    /// stream and every compile in flight on it.
    #[error("the message is too large for the pull stream")]
    TooLarge,
}

/// The pull streams this Coral Instance holds, for the compiler client and
/// the take loop. Each send spends a unit of its stream's room per Run or
/// compile, and a `Room` report replaces the count.
#[derive(Clone, Default)]
pub struct PullStreams {
    held: Arc<Mutex<HashMap<StreamId, Held>>>,
    /// Signalled when a stream opens or reports its room.
    roomed: Arc<Condvar>,
    next: Arc<AtomicU64>,
}

struct Held {
    /// What the take loop and the compiler client see.
    view: HeldStream,
    /// Set at the connection age or shutdown: nothing more is sent.
    closing: bool,
    outbound: mpsc::UnboundedSender<Result<PullResponse, Status>>,
    compiles_sent: u64,
    /// Each compile in flight, by number.
    compiles: HashMap<u64, InFlight>,
}

/// A compile sent and not yet answered.
struct InFlight {
    /// When its time runs out.
    due: Instant,
    /// The answers it accepts.
    takes: Takes,
    /// Where its answer goes: a channel of one, sent on once.
    answered: sync_mpsc::SyncSender<Read>,
}

impl Held {
    fn spend(&mut self, units: u32) -> Result<(), Refused> {
        if self.closing {
            return Err(Refused::Closed);
        }
        self.view.free = self.view.free.checked_sub(units).ok_or(Refused::NoRoom)?;
        self.view.last_work = Some(Instant::now());
        Ok(())
    }

    fn send(&self, response: PullResponse) {
        // A closed receiver means the stream is ending; its holder removes it.
        drop(self.outbound.send(Ok(response)));
    }
}

impl PullStreams {
    /// The pull streams that take sends now, each with its room.
    #[must_use]
    pub fn held(&self) -> Vec<HeldStream> {
        lock(&self.held)
            .values()
            .filter(|held| !held.closing)
            .map(|held| held.view.clone())
            .collect()
    }

    /// The stream that takes sends with the most room, ties at random, or
    /// none when no stream has room.
    pub(crate) fn most_room(&self) -> Option<StreamId> {
        let random = RandomState::new();
        lock(&self.held)
            .values()
            .filter(|held| !held.closing && held.view.free > 0)
            .max_by_key(|held| (held.view.free, random.hash_one(held.view.stream)))
            .map(|held| held.view.stream)
    }

    /// Blocks until a stream opens or reports its room, or for `wait`.
    pub(crate) fn wait_for_room(&self, wait: Duration) {
        let held = lock(&self.held);
        if held
            .values()
            .any(|held| !held.closing && held.view.free > 0)
        {
            return;
        }
        drop(
            self.roomed
                .wait_timeout(held, wait)
                .unwrap_or_else(PoisonError::into_inner),
        );
    }

    /// Sends `runs` on `stream`, one unit each; none sends nothing.
    ///
    /// # Errors
    ///
    /// Returns [`Refused`] when the stream is gone or has too little room.
    pub fn take(&self, stream: StreamId, runs: Vec<TakenRun>) -> Result<(), Refused> {
        let mut held = lock(&self.held);
        let held = held.get_mut(&stream).ok_or(Refused::Closed)?;
        if runs.is_empty() {
            return Ok(());
        }
        held.spend(u32::try_from(runs.len()).unwrap_or(u32::MAX))?;
        held.send(response(pull_response::Message::Taken(Taken { runs })));
        Ok(())
    }

    /// Sends `call` on `stream` as its next numbered `Compile`, with
    /// `time_left_ms` to answer. It spends one unit. The answer, read and
    /// checked against the call, arrives on the receiver, which disconnects
    /// unanswered if the stream ends first.
    ///
    /// # Errors
    ///
    /// Returns [`Refused`] when the stream is gone or has no room, or the
    /// `Compile` is too large to send. A refused call sends and spends
    /// nothing.
    pub(crate) fn compile(
        &self,
        stream: StreamId,
        time_left_ms: u32,
        call: Call,
    ) -> Result<sync_mpsc::Receiver<Read>, Refused> {
        if too_large(&call) {
            return Err(Refused::TooLarge);
        }
        let takes = Takes::of(&call);
        let mut message = pull_response::Message::Compile(Compile {
            number: 0,
            time_left_ms,
            call: Some(call),
        });
        let mut held = lock(&self.held);
        let held = held.get_mut(&stream).ok_or(Refused::Closed)?;
        held.spend(1)?;
        held.compiles_sent += 1;
        let number = held.compiles_sent;
        if let pull_response::Message::Compile(compile) = &mut message {
            compile.number = number;
        }
        let (answered, answer) = sync_mpsc::sync_channel(1);
        let in_flight = InFlight {
            due: Instant::now() + Duration::from_millis(time_left_ms.into()),
            takes,
            answered,
        };
        held.compiles.insert(number, in_flight);
        held.send(response(message));
        Ok(answer)
    }

    pub(crate) fn insert(
        &self,
        run_executor_id: String,
        (kind, credential): (Kind, CredentialId),
        free: u32,
        outbound: mpsc::UnboundedSender<Result<PullResponse, Status>>,
    ) -> StreamId {
        let stream = StreamId(self.next.fetch_add(1, Ordering::Relaxed));
        let held = Held {
            view: HeldStream {
                stream,
                run_executor_id,
                kind,
                credential,
                free,
                hello_free: free,
                last_work: None,
            },
            closing: false,
            outbound,
            compiles_sent: 0,
            compiles: HashMap::new(),
        };
        lock(&self.held).insert(stream, held);
        self.roomed.notify_all();
        stream
    }

    /// Applies one message from `stream`'s Run Executor. A message that
    /// breaks the protocol changes nothing; its holder ends the stream. Only
    /// that holder calls it, one message at a time.
    pub(crate) fn receive(&self, stream: StreamId, request: PullRequest) -> Result<(), Status> {
        match request.message {
            Some(pull_request::Message::Room(room)) => {
                if let Some(held) = lock(&self.held).get_mut(&stream) {
                    held.view.free = room.free;
                    held.view.last_work = Some(Instant::now());
                }
                // Only room wakes the calls that wait for it.
                if room.free > 0 {
                    self.roomed.notify_all();
                }
                Ok(())
            }
            Some(pull_request::Message::Compiled(compiled)) => self.answer(stream, compiled),
            Some(pull_request::Message::Hello(_)) => {
                Err(Status::invalid_argument("Hello opens the pull stream once"))
            }
            None => Err(Status::invalid_argument("the message is empty")),
        }
    }

    /// Hands `compiled` to the compile in flight it answers, once it reads
    /// as an answer to that call.
    fn answer(&self, stream: StreamId, compiled: Compiled) -> Result<(), Status> {
        let Some(in_flight) = lock(&self.held)
            .get(&stream)
            .map(|held| held.compiles.get(&compiled.number).map(|call| call.takes))
        else {
            return Ok(());
        };
        let takes = in_flight
            .ok_or_else(|| Status::invalid_argument("Compiled answers no Compile in flight"))?;
        // Read outside the lock: a package's digest covers megabytes. Only
        // this stream's holder answers its compiles, so the call still waits.
        let read = compiled
            .answer
            .ok_or(ProtocolError)
            .and_then(Read::answer)
            .ok()
            .filter(|read| read.answers(takes))
            .ok_or_else(|| Status::invalid_argument("Compiled breaks the protocol"))?;
        let answered = lock(&self.held).get_mut(&stream).and_then(|held| {
            // An answer is work, as the room report after it is.
            held.view.last_work = Some(Instant::now());
            held.compiles.remove(&compiled.number)
        });
        if let Some(in_flight) = answered {
            // An answer nobody waits for any more is dropped.
            drop(in_flight.answered.try_send(read));
        }
        Ok(())
    }

    /// Stops sends on `stream`. Returns when its compiles in flight are
    /// due: the last deadline among them, or now.
    pub(crate) fn close(&self, stream: StreamId) -> Instant {
        let mut held = lock(&self.held);
        let now = Instant::now();
        held.get_mut(&stream).map_or(now, |held| {
            held.closing = true;
            held.compiles
                .values()
                .map(|in_flight| in_flight.due)
                .fold(now, Instant::max)
        })
    }

    fn compiles_in_flight(&self, stream: StreamId) -> bool {
        lock(&self.held)
            .get(&stream)
            .is_some_and(|held| !held.compiles.is_empty())
    }

    /// Forgets `stream`, which fails its compiles in flight, and ends
    /// Coral's side with `violation`, or cleanly without one: the move.
    pub(crate) fn end(&self, stream: StreamId, violation: Option<Status>) {
        let held = lock(&self.held).remove(&stream);
        if let (Some(held), Some(violation)) = (held, violation) {
            drop(held.outbound.send(Err(violation)));
        }
    }
}

/// Whether `call`, sent as a `Compile`, passes [`MESSAGE_MAX_BYTES`]. Sent,
/// such a message would end the stream and every compile on it.
pub(crate) fn too_large(call: &Call) -> bool {
    // A `Compile`'s number, time left and framing around its call, at their
    // longest.
    const FRAMING_BYTES: usize = 32;
    call.encoded_len() + FRAMING_BYTES > MESSAGE_MAX_BYTES
}

/// Coral's side of the link: the pull streams it holds, the credentials it
/// admits, and the lease rows it writes.
pub struct LinkServer {
    pub(crate) credentials: Credentials,
    pub(crate) streams: PullStreams,
    pub(crate) database: Arc<Database>,
    /// This Coral Instance's id, which Welcome carries.
    pub(crate) coral_instance_id: String,
    pub(crate) timing: Timing,
}

impl LinkServer {
    /// The link of the Coral Instance `coral_instance_id`, which admits
    /// `credentials` and writes lease rows in `database`.
    #[must_use]
    pub fn new(
        credentials: Credentials,
        database: Arc<Database>,
        coral_instance_id: String,
    ) -> Self {
        Self {
            credentials,
            streams: PullStreams::default(),
            database,
            coral_instance_id,
            timing: Timing::default(),
        }
    }

    /// The pull streams this link holds.
    #[must_use]
    pub fn streams(&self) -> PullStreams {
        self.streams.clone()
    }

    /// Serves the link over plaintext HTTP/2 on `listener` until `shutdown`
    /// completes. Shutdown ends each pull stream cleanly, after its compiles
    /// in flight, as the connection age does.
    ///
    /// # Errors
    ///
    /// Returns the listener's failure.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), tonic::transport::Error> {
        let (close, closing) = watch::channel(false);
        let connection_age = self.timing.connection_age;
        let service = PullService {
            link: Arc::new(self),
            closing,
        };
        Server::builder()
            .max_connection_age(connection_age)
            .http2_keepalive_interval(Some(KEEPALIVE))
            .http2_keepalive_timeout(Some(KEEPALIVE))
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .add_service(
                LinkServiceServer::new(service)
                    .max_decoding_message_size(MESSAGE_MAX_BYTES)
                    .max_encoding_message_size(MESSAGE_MAX_BYTES),
            )
            .serve_with_incoming_shutdown(
                TcpIncoming::from(listener).with_nodelay(Some(true)),
                async move {
                    shutdown.await;
                    close.send_replace(true);
                },
            )
            .await
    }
}

#[derive(Clone)]
struct PullService {
    link: Arc<LinkServer>,
    closing: watch::Receiver<bool>,
}

#[tonic::async_trait]
impl LinkService for PullService {
    type PullStream = UnboundedReceiverStream<Result<PullResponse, Status>>;

    async fn pull(
        &self,
        request: Request<Streaming<PullRequest>>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        let admitted = self.link.credentials.admit(authorization).ok_or_else(|| {
            Status::unauthenticated("the Run Executor credential is not accepted")
        })?;
        let mut inbound = request.into_inner();
        // A Run Executor sends Hello with the call; one that sends none
        // within a keepalive holds nothing open, shutdown included.
        let first = timeout(KEEPALIVE, inbound.message()).await.ok();
        let Some(pull_request::Message::Hello(hello)) = first
            .transpose()
            .map_err(|status| undecodable(&status).unwrap_or(status))?
            .flatten()
            .and_then(|request| request.message)
        else {
            return Err(Status::invalid_argument("the pull stream opens with Hello"));
        };
        if hello.protocol_level.abs_diff(PROTOCOL_LEVEL) > 1 {
            tracing::warn!(
                coral.run_executor.protocol_level = safe::number(PROTOCOL_LEVEL),
                coral.run_executor.peer_protocol_level = safe::number(hello.protocol_level),
                "the Run Executor's protocol level is more than one apart; refusing its pull stream"
            );
            return Err(Status::failed_precondition(
                "the protocol levels are more than one apart",
            ));
        }
        let run_executor_id = uuid::Uuid::try_parse(&hello.run_executor_id)
            .map_err(|_invalid| Status::invalid_argument("the Run Executor id is not a UUID"))?
            .to_string();
        let opened = Instant::now();
        let kind = admitted.0;
        self.renew(&run_executor_id, kind).await.map_err(|error| {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "Run Executor lease write failed; refusing its pull stream"
            );
            Status::unavailable("the Run Executor lease is not written")
        })?;
        let (outbound, responses) = mpsc::unbounded_channel();
        let welcome = response(pull_response::Message::Welcome(Welcome {
            protocol_level: PROTOCOL_LEVEL,
            coral_instance_id: self.link.coral_instance_id.clone(),
        }));
        // The receiver is still here, so the send lands.
        drop(outbound.send(Ok(welcome)));
        let stream =
            self.link
                .streams
                .insert(run_executor_id.clone(), admitted, hello.free, outbound);
        tokio::spawn(
            self.clone()
                .hold(stream, inbound, opened, run_executor_id, kind),
        );
        Ok(Response::new(UnboundedReceiverStream::new(responses)))
    }
}

impl PullService {
    /// Holds `stream` until it ends: applies its messages, renews its lease,
    /// and ends it cleanly at its age or shutdown once its compiles in
    /// flight are answered or due. A protocol violation ends it with
    /// `INVALID_ARGUMENT`. Nothing is written once it ends.
    async fn hold(
        self,
        stream: StreamId,
        mut inbound: Streaming<PullRequest>,
        opened: Instant,
        run_executor_id: String,
        kind: Kind,
    ) {
        let streams = &self.link.streams;
        let timing = self.link.timing;
        let mut renewals = interval_at(opened + timing.renewal, timing.renewal);
        renewals.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // The renewal in flight, polled beside the stream, so a slow write
        // holds back no message.
        let mut renewal = None;
        // The connection age, then the end of the drain.
        let deadline = sleep_until(opened + timing.connection_age);
        tokio::pin!(deadline);
        let mut closing = self.closing.clone();
        let mut draining_until = None;
        let end = loop {
            if let Some(until) = draining_until
                && (!streams.compiles_in_flight(stream) || Instant::now() >= until)
            {
                break Ok(());
            }
            tokio::select! {
                message = inbound.message() => match message {
                    Ok(Some(request)) => {
                        if let Err(status) = streams.receive(stream, request) {
                            break Err(status);
                        }
                    }
                    // A message that does not decode is a violation; any
                    // other error is the connection ending.
                    Err(status) => match undecodable(&status) {
                        Some(violation) => break Err(violation),
                        None => break Ok(()),
                    },
                    // The Run Executor's side ended.
                    Ok(None) => break Ok(()),
                },
                _ = renewals.tick(), if renewal.is_none() => {
                    renewal = Some(Box::pin(self.renew(&run_executor_id, kind)));
                }
                written = async {
                    match renewal.as_mut() {
                        Some(renewal) => renewal.await,
                        None => pending().await,
                    }
                } => {
                    renewal = None;
                    renewed(written);
                }
                () = &mut deadline => {
                    if draining_until.is_none() {
                        draining_until = Some(drain(streams, stream, deadline.as_mut()));
                    }
                }
                // Its only change is to closing; a dropped sender closes too.
                _ = closing.changed(), if draining_until.is_none() => {
                    draining_until = Some(drain(streams, stream, deadline.as_mut()));
                }
            }
        };
        // A renewal in flight lands before the stream goes; none starts after.
        if let Some(renewal) = renewal {
            renewed(renewal.await);
        }
        streams.end(stream, end.err());
    }

    async fn renew(&self, run_executor_id: &str, kind: Kind) -> Result<(), coral_db::Error> {
        let lease_until = (SystemTime::now() + self.link.timing.lease)
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|since| i64::try_from(since.as_nanos()).ok())
            .unwrap_or(i64::MAX);
        lease_repository::renew(&self.link.database, run_executor_id, kind, lease_until).await
    }
}

/// Stops sends on `stream` and moves `deadline` to when its compiles in
/// flight are due, which it returns.
fn drain(streams: &PullStreams, stream: StreamId, deadline: Pin<&mut Sleep>) -> Instant {
    let until = streams.close(stream);
    deadline.reset(until);
    until
}

/// The violation that ends a pull stream when `status`, an inbound error,
/// says a message did not decode: tonic answers `INTERNAL` for bytes that are
/// not a `PullRequest` and `OUT_OF_RANGE` for one over the size limit.
fn undecodable(status: &Status) -> Option<Status> {
    matches!(status.code(), Code::Internal | Code::OutOfRange)
        .then(|| Status::invalid_argument("the message is not a PullRequest"))
}

fn renewed(written: Result<(), coral_db::Error>) {
    if let Err(error) = written {
        tracing::warn!(
            error.type = safe::kind(error.code()),
            "Run Executor lease renewal failed; the next one retries"
        );
    }
}

pub(crate) fn response(message: pull_response::Message) -> PullResponse {
    PullResponse {
        message: Some(message),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
pub(crate) mod tests;
