//! A scripted Coral that serves the pull stream and the Run streams, for
//! tests of the Run Executor's side of the link, and a compiler client that
//! counts calls.
//! Each stream it holds sends compiles through a [`PullStreamCompiler`] over
//! [`PullStreams`], as Coral's listener does.

#![allow(
    clippy::missing_panics_doc,
    reason = "test support fails the test that uses it"
)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::TcpIncoming;
use tonic::{Code, Request, Response, Status, Streaming};

mod counting;
pub use counting::{CompilerCalls, CountingCompiler};

use crate::pull::dialed_admission;
use crate::wire::v1::link_service_server::{LinkService, LinkServiceServer};
use crate::wire::v1::{
    Hello, Open, PullRequest, PullResponse, RunPackage, RunRequest, RunResponse, Taken, TakenRun,
    Welcome, pull_request, pull_response, run_request, run_response,
};
use crate::{
    MESSAGE_MAX_BYTES, PROTOCOL_LEVEL, PullStreamCompiler, PullStreams, RunExecutorConfig,
    RunLimits, Token, compile_wire,
};

/// How the scripted Coral answers one pull stream.
#[derive(Clone, Copy, Debug)]
pub enum Answer {
    /// Welcome at this protocol level, then hold the stream open until
    /// [`ScriptedCoral::release`]: it carries [`ScriptedCoral::take`] and
    /// reports each `Room`.
    Welcome(u32),
    /// Welcome, then end the stream cleanly: the move (the go-away).
    Move,
    /// End the stream cleanly before any message.
    Close,
    /// End the call with this status before any message.
    Refuse(Code),
}

/// One pull stream a Run Executor opened.
#[derive(Debug)]
pub struct PullCall {
    /// When it arrived.
    pub at: Instant,
    /// The Run Executor's end of the connection it rode.
    pub peer: Option<SocketAddr>,
    /// Its `authorization` metadata.
    pub authorization: Option<String>,
    /// Its first message, unless the call was refused.
    pub hello: Option<Hello>,
    /// Sends compiles down the stream while it is held.
    pub compiler: Arc<PullStreamCompiler>,
    /// The room the Run Executor reports on it, in order.
    pub rooms: mpsc::UnboundedReceiver<u32>,
}

/// A Coral that answers pull streams from a script, and refuses each one
/// after the script with `UNAVAILABLE`.
pub struct ScriptedCoral {
    address: String,
    calls: mpsc::UnboundedReceiver<PullCall>,
    release: watch::Sender<()>,
    takers: Takers,
    runs: mpsc::UnboundedReceiver<RunCall>,
    server: tokio::task::JoinHandle<()>,
}

/// One Run stream a Run Executor opened, which the test answers.
pub struct RunCall {
    /// Its first message.
    pub open: Open,
    inbound: Streaming<RunRequest>,
    outbound: mpsc::Sender<Result<RunResponse, Status>>,
}

impl RunCall {
    /// Sends `message` on the stream.
    pub async fn send(&self, message: run_response::Message) {
        let response = RunResponse {
            message: Some(message),
        };
        self.outbound
            .send(Ok(response))
            .await
            .expect("the Run stream is open");
    }

    /// The Run Executor's next message within 10 s, or `None` once its side
    /// of the stream ends.
    pub async fn next(&mut self) -> Option<run_request::Message> {
        tokio::time::timeout(Duration::from_secs(10), self.inbound.message())
            .await
            .expect("the Run Executor sends or closes its side")
            .ok()
            .flatten()
            .and_then(|request| request.message)
    }
}

/// The token [`start_run_executor`] dials with.
pub const RUN_EXECUTOR_TOKEN: &str = "test-run-executor-token";

/// The config of a Run Executor that dials `coral` with room for
/// `max_runs_held` Runs within `run_limits`.
#[must_use]
pub fn run_executor_config(
    coral: &ScriptedCoral,
    max_runs_held: u32,
    run_limits: RunLimits,
) -> RunExecutorConfig {
    RunExecutorConfig {
        coral_address: coral
            .address()
            .parse()
            .expect("the scripted Coral's address"),
        token: Token::new(RUN_EXECUTOR_TOKEN.to_owned()).expect("a valid token"),
        max_runs_held,
        compiler_heap_bytes: coral_v8::IsolateLimits::defaults().compiler_heap_bytes(),
        run_limits,
    }
}

/// Starts a Run Executor that dials `coral` with room for `max_runs_held`
/// Runs within `run_limits`, and serves health on `health`. It retires when
/// the sender drops.
#[must_use]
pub fn start_run_executor(
    coral: &ScriptedCoral,
    max_runs_held: u32,
    run_limits: RunLimits,
    health: Option<TcpListener>,
) -> oneshot::Sender<()> {
    let config = run_executor_config(coral, max_runs_held, run_limits);
    let (retire, retired) = oneshot::channel::<()>();
    tokio::spawn(crate::run(config, health, async {
        drop(retired.await);
    }));
    retire
}

/// The package of `script` and `source_map` with the default value limits.
#[must_use]
pub fn package(script: &str, source_map: &str) -> RunPackage {
    RunPackage {
        script: script.to_owned(),
        source_map: source_map.to_owned(),
        sha256: compile_wire::package_sha256(script, source_map).to_vec(),
        value_limits: Some(coral_value::ValueLimits::default().into()),
        linked_snapshots: Vec::new(),
        protocol_level: PROTOCOL_LEVEL,
        traceparent: String::new(),
    }
}

impl ScriptedCoral {
    /// Serves `script` on a loopback port.
    pub async fn start(script: impl IntoIterator<Item = Answer>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let address = format!("http://{}", listener.local_addr().expect("local address"));
        let (calls_tx, calls) = mpsc::unbounded_channel();
        let (release, released) = watch::channel(());
        let takers = Takers::default();
        let (runs_tx, runs) = mpsc::unbounded_channel();
        let script = Script {
            answers: Mutex::new(script.into_iter().collect()),
            calls: calls_tx,
            released,
            takers: Arc::clone(&takers),
            runs: runs_tx,
        };
        let server = tokio::spawn(async move {
            let service = LinkServiceServer::new(script)
                .max_decoding_message_size(MESSAGE_MAX_BYTES)
                .max_encoding_message_size(MESSAGE_MAX_BYTES);
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpIncoming::from(listener))
                .await
                .expect("the scripted Coral serves");
        });
        Self {
            address,
            calls,
            release,
            takers,
            runs,
            server,
        }
    }

    /// Takes one Run for the Run Executor on every pull stream it holds.
    pub fn take(&self, run_id: &str, claim_id: &str) {
        let run = TakenRun {
            run_id: run_id.to_owned(),
            claim_id: claim_id.to_owned(),
        };
        self.takers.lock().expect("the takers").retain(|taker| {
            taker
                .send(Taken {
                    runs: vec![run.clone()],
                })
                .is_ok()
        });
    }

    /// The next Run stream a Run Executor opens, within 10 s.
    pub async fn next_run(&mut self) -> RunCall {
        tokio::time::timeout(Duration::from_secs(10), self.runs.recv())
            .await
            .expect("a Run stream opens")
            .expect("the scripted Coral serves")
    }

    /// Where it listens, as an `http://` URL.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// The next pull stream a Run Executor opens, within 10 s.
    pub async fn next_call(&mut self) -> PullCall {
        tokio::time::timeout(Duration::from_secs(10), self.calls.recv())
            .await
            .expect("a pull stream opens")
            .expect("the scripted Coral serves")
    }

    /// The next pull stream a Run Executor opened, if one has.
    pub fn try_next_call(&mut self) -> Option<PullCall> {
        self.calls.try_recv().ok()
    }

    /// Ends every held stream with `UNAVAILABLE`.
    pub fn release(&self) {
        self.release.send_replace(());
    }
}

impl Drop for ScriptedCoral {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// One queue of `Taken` per pull stream, which loses none.
type Takers = Arc<Mutex<Vec<mpsc::UnboundedSender<Taken>>>>;

struct Script {
    answers: Mutex<VecDeque<Answer>>,
    calls: mpsc::UnboundedSender<PullCall>,
    released: watch::Receiver<()>,
    takers: Takers,
    runs: mpsc::UnboundedSender<RunCall>,
}

#[tonic::async_trait]
impl LinkService for Script {
    type PullStream = Pin<Box<dyn Stream<Item = Result<PullResponse, Status>> + Send>>;
    type RunStream = Pin<Box<dyn Stream<Item = Result<RunResponse, Status>> + Send>>;

    async fn run(
        &self,
        request: Request<Streaming<RunRequest>>,
    ) -> Result<Response<Self::RunStream>, Status> {
        let mut inbound = request.into_inner();
        let Ok(Some(RunRequest {
            message: Some(run_request::Message::Open(open)),
        })) = inbound.message().await
        else {
            return Err(Status::invalid_argument("a Run stream opens with Open"));
        };
        let (outbound, stream) = mpsc::channel(16);
        drop(self.runs.send(RunCall {
            open,
            inbound,
            outbound,
        }));
        Ok(Response::new(Box::pin(ReceiverStream::new(stream))))
    }

    async fn pull(
        &self,
        request: Request<Streaming<PullRequest>>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let at = Instant::now();
        // Armed before the call is published, so a release after
        // `next_call` returns reaches this stream.
        let mut released = self.released.clone();
        released.mark_unchanged();
        let (queue, mut taken) = mpsc::unbounded_channel();
        // `take` drops the queue of a stream that ended.
        self.takers.lock().expect("the takers").push(queue);
        let peer = request.remote_addr();
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let answer = self
            .answers
            .lock()
            .expect("the script")
            .pop_front()
            .unwrap_or(Answer::Refuse(Code::Unavailable));
        let mut inbound = request.into_inner();
        let hello = match answer {
            Answer::Refuse(_) => None,
            Answer::Welcome(_) | Answer::Move | Answer::Close => match inbound.message().await {
                Ok(Some(PullRequest {
                    message: Some(pull_request::Message::Hello(hello)),
                })) => Some(hello),
                _ => None,
            },
        };
        let streams = PullStreams::default();
        let (to_send, mut sent) = mpsc::unbounded_channel();
        let free = hello.as_ref().map_or(0, |hello| hello.free);
        let held = streams.insert("scripted".to_owned(), dialed_admission(), free, to_send);
        let compiler = Arc::new(PullStreamCompiler::for_stream(streams.clone(), held));
        let (rooms, reported) = mpsc::unbounded_channel();
        drop(self.calls.send(PullCall {
            at,
            peer,
            authorization,
            hello,
            compiler: Arc::clone(&compiler),
            rooms: reported,
        }));
        let protocol_level = match answer {
            Answer::Refuse(code) => {
                streams.end(held, None);
                return Err(Status::new(code, "scripted refusal"));
            }
            Answer::Close => {
                streams.end(held, None);
                return Ok(Response::new(Box::pin(tokio_stream::empty())));
            }
            Answer::Welcome(level) => level,
            Answer::Move => PROTOCOL_LEVEL,
        };
        let welcome = crate::pull::response(pull_response::Message::Welcome(Welcome {
            protocol_level,
            coral_instance_id: "scripted-coral".to_owned(),
        }));
        let (outbound, stream) = mpsc::channel(1);
        tokio::spawn(async move {
            if outbound.send(Ok(welcome)).await.is_err() {
                return;
            }
            if let Answer::Welcome(_) = answer {
                loop {
                    tokio::select! {
                        _ = released.changed() => {
                            drop(outbound.send(Err(Status::unavailable("released"))).await);
                            break;
                        }
                        () = outbound.closed() => break,
                        Some(compile) = sent.recv() => {
                            if outbound.send(compile).await.is_err() {
                                break;
                            }
                        }
                        Some(taken) = taken.recv() => {
                            let taken = crate::pull::response(pull_response::Message::Taken(taken));
                            if outbound.send(Ok(taken)).await.is_err() {
                                break;
                            }
                        }
                        message = inbound.message() => match message {
                            Ok(Some(request)) => {
                                if let Some(pull_request::Message::Room(room)) = &request.message {
                                    // A test that reads no rooms drops their receiver.
                                    let _unread = rooms.send(room.free).is_err();
                                }
                                if let Err(violation) = streams.receive(held, request) {
                                    drop(outbound.send(Err(violation)).await);
                                    break;
                                }
                            }
                            _ => break,
                        },
                    }
                }
            }
            streams.end(held, None);
            // Holds the Run Executor's side open until Coral's side ends.
            drop(inbound);
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(stream))))
    }
}
