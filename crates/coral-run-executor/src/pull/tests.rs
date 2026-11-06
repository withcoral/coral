use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;

use coral_test_support::TestDatabase;
use hyper_util::rt::TokioIo;
use sea_query::ExprTrait as _;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::codegen::http::Uri;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};

use super::*;
use crate::wire::v1::{Hello, Room};

const TOKEN: &str = "an-operator-token-of-at-least-32-characters";
const ROTATED: &str = "the-next-operator-token-during-a-rotation";
const LOCAL: &str = "a-local-child-token-that-coral-wrote-to-its-stdin";
const INSTANCE: &str = "coral-instance";

use pull_request::Message as Up;
use pull_response::Message as Down;

struct Link {
    address: SocketAddr,
    credentials: Credentials,
    streams: PullStreams,
    database: TestDatabase,
    dials: Arc<AtomicUsize>,
    stop: Option<oneshot::Sender<()>>,
}

/// Serves the link with both operator tokens on a loopback port.
async fn start(timing: Timing) -> Link {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let credentials =
        Credentials::new([TOKEN, ROTATED].map(|token| Token::new(token.to_owned()).unwrap()));
    let streams = PullStreams::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = LinkServer {
        credentials: credentials.clone(),
        streams: streams.clone(),
        database: database.database(),
        coral_instance_id: INSTANCE.to_owned(),
        timing,
    };
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(server.serve(listener, async {
        drop(stopped.await);
    }));
    Link {
        address,
        credentials,
        streams,
        database,
        dials: Arc::default(),
        stop: Some(stop),
    }
}

/// A Run Executor's end of one pull stream, which sends `M`.
struct Pull<M = PullRequest> {
    requests: mpsc::Sender<M>,
    responses: Streaming<PullResponse>,
}

/// A message that encodes as a `PullRequest` with `hello`, and with
/// `undecodable` does not decode as one: its field 2 is a number where a
/// `PullRequest`'s is a `Room`.
#[derive(Clone, PartialEq, prost::Message)]
struct Wire {
    #[prost(message, optional, tag = "1")]
    hello: Option<Hello>,
    #[prost(uint64, optional, tag = "2")]
    undecodable: Option<u64>,
}

impl Pull {
    async fn send(&self, message: Up) {
        let request = PullRequest {
            message: Some(message),
        };
        self.requests.send(request).await.unwrap();
    }
}

impl<M> Pull<M> {
    async fn next(&mut self) -> Result<Option<Down>, Status> {
        let message = timeout(Duration::from_secs(5), self.responses.message());
        Ok(message
            .await
            .unwrap()?
            .and_then(|response| response.message))
    }
}

impl Link {
    /// One connection to the listener; each dial is counted.
    async fn connect(&self) -> Channel {
        let (address, dials) = (self.address, self.dials.clone());
        let connector = tower::service_fn(move |_: Uri| {
            dials.fetch_add(1, Ordering::SeqCst);
            async move { TcpStream::connect(address).await.map(TokioIo::new) }
        });
        Endpoint::from_static("http://run-executor.test")
            .connect_with_connector(connector)
            .await
            .unwrap()
    }

    /// Opens a pull stream on `channel` with the `authorization` header, whose
    /// first message is `first`.
    async fn open(
        &self,
        channel: &Channel,
        authorization: &str,
        first: Up,
    ) -> Result<Pull, Status> {
        let first = PullRequest {
            message: Some(first),
        };
        self.open_with(channel, authorization, first).await
    }

    /// Opens a pull stream on `channel` with the `authorization` header that
    /// sends `M`, the first being `first`.
    async fn open_with<M: prost::Message + 'static>(
        &self,
        channel: &Channel,
        authorization: &str,
        first: M,
    ) -> Result<Pull<M>, Status> {
        let (requests, outbound) = mpsc::channel(8);
        requests.send(first).await.unwrap();
        let mut request = Request::new(ReceiverStream::new(outbound));
        let authorization = authorization.parse().unwrap();
        request
            .metadata_mut()
            .insert("authorization", authorization);
        let mut grpc = tonic::client::Grpc::new(channel.clone());
        grpc.ready().await.unwrap();
        let path = PathAndQuery::from_static("/coral.run_executor.v1.LinkService/Pull");
        let codec = tonic_prost::ProstCodec::<M, PullResponse>::default();
        let responses = grpc.streaming(request, path, codec).await?.into_inner();
        Ok(Pull {
            requests,
            responses,
        })
    }

    /// Opens a pull stream that Coral welcomes.
    async fn welcomed(&self, channel: &Channel, hello: Hello) -> Pull {
        let mut pull = self
            .open(channel, &bearer(TOKEN), Up::Hello(hello))
            .await
            .unwrap();
        let welcome = Welcome {
            protocol_level: PROTOCOL_LEVEL,
            coral_instance_id: INSTANCE.to_owned(),
        };
        assert_eq!(pull.next().await.unwrap(), Some(Down::Welcome(welcome)));
        pull
    }

    /// The held stream of Run Executor `id` once `ready` holds for it.
    async fn held(&self, id: &str, ready: impl Fn(&HeldStream) -> bool) -> HeldStream {
        let found = async {
            loop {
                let held = self.streams.held().into_iter();
                if let Some(held) = held.filter(|held| held.run_executor_id == id).find(&ready) {
                    return held;
                }
                sleep(Duration::from_millis(10)).await;
            }
        };
        timeout(Duration::from_secs(5), found).await.unwrap()
    }

    /// Run Executor `id`'s lease row: its kind, and its lease in Unix
    /// nanoseconds.
    async fn lease(&self, id: &str) -> Option<(String, i64)> {
        let database = self.database.database();
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let select = sea_query::Query::select()
            .columns(["kind", "lease_until"].map(sea_query::Alias::new))
            .from(sea_query::Alias::new("run_executors"))
            .and_where(sea_query::Expr::col(sea_query::Alias::new("id")).eq(id))
            .to_owned();
        let row = transaction.fetch_optional(select).await.unwrap();
        transaction.commit().await.unwrap();
        row
    }
}

fn hello(protocol_level: u32, free: u32) -> Hello {
    Hello {
        run_executor_id: uuid::Uuid::new_v4().to_string(),
        protocol_level,
        free,
        limits: None,
    }
}

fn run(number: u32) -> TakenRun {
    TakenRun {
        run_id: format!("run-{number}"),
        claim_id: format!("claim-{number}"),
    }
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

fn code<T>(result: Result<T, Status>) -> Option<Code> {
    result.err().map(|status| status.code())
}

#[tokio::test]
async fn the_credential_admits_both_operator_tokens_and_a_live_local_child_by_kind() {
    let link = start(Timing::default()).await;
    let channel = link.connect().await;
    let local = link
        .credentials
        .admit_local(&Token::new(LOCAL.to_owned()).unwrap());
    // As long as a token may be.
    let long = "t".repeat(16 * 1024);
    let _long = link
        .credentials
        .admit_local(&Token::new(long.clone()).unwrap());
    let unknown = "an-unknown-token-of-at-least-32-characters";
    for (authorization, kind) in [
        (bearer(TOKEN), Some((Kind::Dialed, "dialed"))),
        // The scheme's name ignores case.
        (format!("bearer {ROTATED}"), Some((Kind::Dialed, "dialed"))),
        (bearer(LOCAL), Some((Kind::Local, "local"))),
        (bearer(&long), Some((Kind::Local, "local"))),
        (bearer(unknown), None),
        (format!("Basic {TOKEN}"), None),
    ] {
        let hello = hello(PROTOCOL_LEVEL, 1);
        let id = hello.run_executor_id.clone();
        let opened = link.open(&channel, &authorization, Up::Hello(hello)).await;
        if let Some((kind, column)) = kind {
            let _pull = opened.unwrap();
            let held = link.held(&id, |_| true).await;
            assert_eq!(held.kind, kind, "{authorization}");
            let written = link.lease(&id).await.map(|(kind, _)| kind);
            assert_eq!(written.as_deref(), Some(column), "{authorization}");
        } else {
            assert_eq!(code(opened), Some(Code::Unauthenticated), "{authorization}");
            assert_eq!(link.lease(&id).await, None);
        }
    }
    // The child exited, so its token admits nothing.
    drop(local);
    let opened = link
        .open(
            &channel,
            &bearer(LOCAL),
            Up::Hello(hello(PROTOCOL_LEVEL, 1)),
        )
        .await;
    assert_eq!(code(opened), Some(Code::Unauthenticated));
}

#[tokio::test]
async fn hello_is_welcomed_one_level_apart_and_refused_two_apart() {
    let link = start(Timing::default()).await;
    let channel = link.connect().await;
    for (level, welcomed) in [
        (PROTOCOL_LEVEL, true),
        (PROTOCOL_LEVEL + 1, true),
        (PROTOCOL_LEVEL - 1, true),
        (PROTOCOL_LEVEL + 2, false),
    ] {
        let hello = hello(level, 1);
        let id = hello.run_executor_id.clone();
        if welcomed {
            let _pull = link.welcomed(&channel, hello).await;
            link.held(&id, |_| true).await;
        } else {
            let opened = link.open(&channel, &bearer(TOKEN), Up::Hello(hello)).await;
            assert_eq!(code(opened), Some(Code::FailedPrecondition));
            assert_eq!(link.lease(&id).await, None);
            assert!(
                link.streams
                    .held()
                    .iter()
                    .all(|held| held.run_executor_id != id)
            );
        }
    }
}

#[tokio::test]
async fn sends_spend_the_room_and_a_report_replaces_it() {
    let link = start(Timing::default()).await;
    let channel = link.connect().await;
    let hello = hello(PROTOCOL_LEVEL, 3);
    let id = hello.run_executor_id.clone();
    let mut pull = link.welcomed(&channel, hello).await;
    let stream = link.held(&id, |_| true).await.stream;

    link.streams.take(stream, vec![run(1), run(2)]).unwrap();
    let taken = Taken {
        runs: vec![run(1), run(2)],
    };
    assert_eq!(pull.next().await.unwrap(), Some(Down::Taken(taken)));
    let compile = Compile {
        number: 0,
        time_left_ms: 2_000,
    };
    let answer = link.streams.compile(stream, compile).unwrap();
    let first = Compile {
        number: 1,
        time_left_ms: 2_000,
    };
    assert_eq!(pull.next().await.unwrap(), Some(Down::Compile(first)));
    // Three units sent: nothing more goes.
    assert_eq!(link.held(&id, |_| true).await.free, 0);
    assert_eq!(
        link.streams.take(stream, vec![run(3)]),
        Err(Refused::NoRoom)
    );
    assert_eq!(
        link.streams.compile(stream, compile).err(),
        Some(Refused::NoRoom)
    );

    pull.send(Up::Room(Room { free: 2 })).await;
    link.held(&id, |held| held.free == 2).await;
    pull.send(Up::Compiled(Compiled { number: 1 })).await;
    let answered = timeout(Duration::from_secs(5), answer).await.unwrap();
    assert_eq!(answered.unwrap(), Compiled { number: 1 });
    // An answer frees nothing; the next report does.
    link.streams.compile(stream, compile).unwrap();
    assert_eq!(link.held(&id, |_| true).await.free, 1);
    let second = Compile {
        number: 2,
        time_left_ms: 2_000,
    };
    assert_eq!(pull.next().await.unwrap(), Some(Down::Compile(second)));
}

#[tokio::test]
async fn the_lease_is_written_at_welcome_and_renewed_only_while_the_stream_is_open() {
    let renewal = Duration::from_millis(50);
    let link = start(Timing {
        renewal,
        ..Timing::default()
    })
    .await;
    let channel = link.connect().await;
    let hello = hello(PROTOCOL_LEVEL, 1);
    let id = hello.run_executor_id.clone();
    let before = SystemTime::now() + LEASE;
    let pull = link.welcomed(&channel, hello).await;

    let (kind, written) = link.lease(&id).await.unwrap();
    assert_eq!(kind, "dialed");
    let before = i64::try_from(before.duration_since(UNIX_EPOCH).unwrap().as_nanos()).unwrap();
    assert!(written >= before, "the lease lasts {LEASE:?} past Welcome");
    let renewed = async {
        while link.lease(&id).await.unwrap().1 <= written {
            sleep(renewal).await;
        }
    };
    timeout(Duration::from_secs(5), renewed).await.unwrap();

    drop(pull);
    let closed = async {
        while link
            .streams
            .held()
            .iter()
            .any(|held| held.run_executor_id == id)
        {
            sleep(Duration::from_millis(10)).await;
        }
    };
    timeout(Duration::from_secs(5), closed).await.unwrap();
    let last = link.lease(&id).await.unwrap();
    sleep(renewal * 4).await;
    assert_eq!(
        link.lease(&id).await,
        Some(last),
        "nothing renews a closed stream's lease"
    );
}

#[tokio::test]
async fn a_protocol_violation_ends_its_stream_and_not_the_connection() {
    let link = start(Timing::default()).await;
    let channel = link.connect().await;
    let bystander_hello = hello(PROTOCOL_LEVEL, 1);
    let bystander_id = bystander_hello.run_executor_id.clone();
    let mut bystander = link.welcomed(&channel, bystander_hello).await;

    for violation in [
        Some(Up::Hello(hello(PROTOCOL_LEVEL, 1))),
        None,
        Some(Up::Compiled(Compiled { number: 7 })),
    ] {
        let label = format!("{violation:?}");
        let mut pull = link.welcomed(&channel, hello(PROTOCOL_LEVEL, 1)).await;
        let request = PullRequest { message: violation };
        pull.requests.send(request).await.unwrap();
        assert_eq!(
            code(pull.next().await),
            Some(Code::InvalidArgument),
            "{label}"
        );
    }
    let opened = link
        .open(&channel, &bearer(TOKEN), Up::Room(Room { free: 1 }))
        .await;
    assert_eq!(code(opened), Some(Code::InvalidArgument));

    // A message that does not decode, first or after Hello.
    let undecodable = Wire {
        hello: None,
        undecodable: Some(1),
    };
    let opened = link
        .open_with(&channel, &bearer(TOKEN), undecodable.clone())
        .await;
    assert_eq!(code(opened), Some(Code::InvalidArgument));
    let hello = Wire {
        hello: Some(hello(PROTOCOL_LEVEL, 1)),
        undecodable: None,
    };
    let mut pull = link
        .open_with(&channel, &bearer(TOKEN), hello)
        .await
        .unwrap();
    assert!(matches!(pull.next().await, Ok(Some(Down::Welcome(_)))));
    pull.requests.send(undecodable).await.unwrap();
    assert_eq!(code(pull.next().await), Some(Code::InvalidArgument));

    let stream = link.held(&bystander_id, |_| true).await.stream;
    link.streams.take(stream, vec![run(1)]).unwrap();
    let taken = Taken { runs: vec![run(1)] };
    assert_eq!(bystander.next().await.unwrap(), Some(Down::Taken(taken)));
    assert_eq!(link.dials.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn at_the_connection_age_or_shutdown_the_stream_ends_cleanly_once_its_compiles_are_answered_or_due()
 {
    let connection_age = Duration::from_millis(300);
    // Whether the connection age, not shutdown, ends the stream, and whether
    // the Run Executor answers its compile.
    for (at_age, answered) in [(true, true), (false, true), (true, false)] {
        let label = format!("at age: {at_age}, answered: {answered}");
        let mut link = start(Timing {
            connection_age: if at_age {
                connection_age
            } else {
                CONNECTION_AGE
            },
            ..Timing::default()
        })
        .await;
        let channel = link.connect().await;
        let hello = hello(PROTOCOL_LEVEL, 2);
        let id = hello.run_executor_id.clone();
        let mut pull = link.welcomed(&channel, hello).await;
        let stream = link.held(&id, |_| true).await.stream;
        let compile = Compile {
            number: 0,
            time_left_ms: if answered { 10_000 } else { 600 },
        };
        let due = Instant::now() + Duration::from_millis(compile.time_left_ms.into());
        let answer = link.streams.compile(stream, compile).unwrap();
        let Some(Down::Compile(Compile { number, .. })) = pull.next().await.unwrap() else {
            panic!("a Compile");
        };

        if !at_age {
            drop(link.stop.take());
        }
        let closing = async {
            while !link.streams.held().is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        };
        timeout(Duration::from_secs(5), closing).await.unwrap();
        let refused = link.streams.take(stream, vec![run(1)]);
        assert_eq!(refused, Err(Refused::Closed), "{label}");
        if answered {
            pull.send(Up::Compiled(Compiled { number })).await;
        }
        let reply = timeout(Duration::from_secs(5), answer).await.unwrap();
        if answered {
            assert_eq!(reply.unwrap(), Compiled { number }, "{label}");
        } else {
            // The compile held the stream until its deadline, unanswered.
            assert!(reply.is_err(), "{label}");
            assert!(Instant::now() >= due, "{label}");
        }
        assert_eq!(
            pull.next().await.unwrap(),
            None,
            "a clean end, the move: {label}"
        );
        if at_age {
            // The GOAWAY closed the connection: the next stream dials anew.
            let _next = link
                .welcomed(&channel, self::hello(PROTOCOL_LEVEL, 1))
                .await;
            assert_eq!(link.dials.load(Ordering::SeqCst), 2);
        }
    }
}
