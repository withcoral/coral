//! A scripted Coral that serves the pull stream, for tests of the Run
//! Executor's side of the link.

#![allow(
    clippy::missing_panics_doc,
    reason = "test support fails the test that uses it"
)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::TcpIncoming;
use tonic::{Code, Request, Response, Status, Streaming};

use crate::PROTOCOL_LEVEL;
use crate::wire::v1::link_service_server::{LinkService, LinkServiceServer};
use crate::wire::v1::{Hello, PullRequest, PullResponse, Welcome, pull_request, pull_response};

/// How the scripted Coral answers one pull stream.
#[derive(Clone, Copy, Debug)]
pub enum Answer {
    /// Welcome at this protocol level, then hold the stream open until
    /// [`ScriptedCoral::release`].
    Welcome(u32),
    /// Welcome, then end the stream cleanly: the go-away.
    GoAway,
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
}

/// A Coral that answers pull streams from a script, and refuses each one
/// after the script with `UNAVAILABLE`.
pub struct ScriptedCoral {
    address: String,
    calls: mpsc::UnboundedReceiver<PullCall>,
    release: watch::Sender<()>,
    server: tokio::task::JoinHandle<()>,
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
        let script = Script {
            answers: Mutex::new(script.into_iter().collect()),
            calls: calls_tx,
            released,
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(LinkServiceServer::new(script))
                .serve_with_incoming(TcpIncoming::from(listener))
                .await
                .expect("the scripted Coral serves");
        });
        Self {
            address,
            calls,
            release,
            server,
        }
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

struct Script {
    answers: Mutex<VecDeque<Answer>>,
    calls: mpsc::UnboundedSender<PullCall>,
    released: watch::Receiver<()>,
}

#[tonic::async_trait]
impl LinkService for Script {
    type PullStream = Pin<Box<dyn Stream<Item = Result<PullResponse, Status>> + Send>>;

    async fn pull(
        &self,
        request: Request<Streaming<PullRequest>>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let at = Instant::now();
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
            Answer::Welcome(_) | Answer::GoAway | Answer::Close => match inbound.message().await {
                Ok(Some(PullRequest {
                    message: Some(pull_request::Message::Hello(hello)),
                })) => Some(hello),
                _ => None,
            },
        };
        drop(self.calls.send(PullCall {
            at,
            peer,
            authorization,
            hello,
        }));
        let protocol_level = match answer {
            Answer::Refuse(code) => return Err(Status::new(code, "scripted refusal")),
            Answer::Close => return Ok(Response::new(Box::pin(tokio_stream::empty()))),
            Answer::Welcome(level) => level,
            Answer::GoAway => PROTOCOL_LEVEL,
        };
        let welcome = PullResponse {
            message: Some(pull_response::Message::Welcome(Welcome {
                protocol_level,
                coral_instance_id: "scripted-coral".to_owned(),
            })),
        };
        let (outbound, stream) = mpsc::channel(1);
        let mut released = self.released.clone();
        released.mark_unchanged();
        tokio::spawn(async move {
            if outbound.send(Ok(welcome)).await.is_err() {
                return;
            }
            if let Answer::Welcome(_) = answer {
                tokio::select! {
                    _ = released.changed() => {
                        drop(outbound.send(Err(Status::unavailable("released"))).await);
                    }
                    () = outbound.closed() => {}
                }
            }
            // Holds the Run Executor's side open until Coral's side ends.
            drop(inbound);
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(stream))))
    }
}
