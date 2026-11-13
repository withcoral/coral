use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;

use coral_test_support::TestDatabase;
use hyper_util::rt::TokioIo;
use sea_query::ExprTrait as _;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::codegen::http::Uri;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};

use super::*;
use crate::CompileError;
use crate::compile_wire::{analyze_call, compile_call, failure, package_sha256};
use crate::wire::v1::compiled::Answer;
use crate::wire::v1::{self as wire, Hello, Room};
use coral_v8::AnalyzedSource;

const TOKEN: &str = "an-operator-token-of-at-least-32-characters";
const ROTATED: &str = "the-next-operator-token-during-a-rotation";
const LOCAL: &str = "a-local-child-token-that-coral-wrote-to-its-stdin";
const INSTANCE: &str = "coral-instance";

use pull_request::Message as Up;
use pull_response::Message as Down;

const TIME_LEFT_MS: u32 = 2_000;

/// The call each compile here sends.
fn call() -> Call {
    Call::Compile(wire::ProgramSource {
        source: "return 42;".to_owned(),
        linked_tools: Vec::new(),
        callable: None,
        builtin_tools: Vec::new(),
    })
}

/// An analysis of two sources.
fn analyze() -> Call {
    analyze_call(
        &["return 1;", "return 2;"].map(|source| AnalyzedSource::Program(source.to_owned())),
    )
}

/// An analysis answer with `found` results.
fn analyzed(number: u64, found: usize) -> Compiled {
    let sources = vec![wire::SourceReferences::default(); found];
    Compiled {
        number,
        answer: Some(Answer::Analyzed(wire::Analyzed { sources })),
    }
}

/// The `Compile` that carries [`call`] as `number`.
fn compile(number: u64, time_left_ms: u32) -> Compile {
    Compile {
        number,
        time_left_ms,
        call: Some(call()),
    }
}

fn package(script: &str, digest: [u8; 32]) -> Answer {
    Answer::Package(wire::Package {
        script: script.to_owned(),
        source_map: "{}".to_owned(),
        sha256: digest.to_vec(),
        diagnostics: Vec::new(),
    })
}

/// A sound answer to [`call`] sent as `number`.
pub(crate) fn compiled(number: u64) -> Compiled {
    Compiled {
        number,
        answer: Some(package("42", package_sha256("42", "{}"))),
    }
}

pub(crate) fn up(compiled: Compiled) -> PullRequest {
    PullRequest {
        message: Some(Up::Compiled(compiled)),
    }
}

/// What `answer` receives, or an error once its stream ends unanswered.
async fn answer_within(answer: oneshot::Receiver<Read>) -> Result<Read, oneshot::error::RecvError> {
    timeout(Duration::from_secs(5), answer)
        .await
        .expect("an answer or an end within 5 s")
}

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
    let answer = link.streams.compile(stream, TIME_LEFT_MS, call()).unwrap();
    let first = compile(1, TIME_LEFT_MS);
    assert_eq!(pull.next().await.unwrap(), Some(Down::Compile(first)));
    // Three units sent: nothing more goes.
    assert_eq!(link.held(&id, |_| true).await.free, 0);
    assert_eq!(
        link.streams.take(stream, vec![run(3)]),
        Err(Refused::NoRoom)
    );
    assert_eq!(
        link.streams.compile(stream, TIME_LEFT_MS, call()).err(),
        Some(Refused::NoRoom)
    );

    pull.send(Up::Room(Room { free: 2 })).await;
    link.held(&id, |held| held.free == 2).await;
    pull.send(Up::Compiled(compiled(1))).await;
    assert!(matches!(answer_within(answer).await, Ok(Read::Package(_))));
    // An answer frees nothing; the next report does.
    link.streams.compile(stream, TIME_LEFT_MS, call()).unwrap();
    assert_eq!(link.held(&id, |_| true).await.free, 1);
    let second = compile(2, TIME_LEFT_MS);
    assert_eq!(pull.next().await.unwrap(), Some(Down::Compile(second)));
}

#[test]
fn a_compile_past_the_link_bound_is_refused_alone() {
    let streams = PullStreams::default();
    let (outbound, mut sent) = mpsc::unbounded_channel();
    let stream = streams.insert("a-run-executor".to_owned(), dialed_admission(), 1, outbound);
    let too_large = compile_call(
        &"x".repeat(MESSAGE_MAX_BYTES),
        &[],
        &coral_v8::BuiltinTools::default(),
        None,
    );

    let refused = streams.compile(stream, TIME_LEFT_MS, too_large);
    assert_eq!(refused.err(), Some(Refused::TooLarge));
    // Nothing went, no room was spent, and no number was used.
    assert_eq!(
        sent.try_recv().err(),
        Some(mpsc::error::TryRecvError::Empty)
    );
    assert!(!streams.compiles_in_flight(stream));
    assert_eq!(streams.held().first().map(|held| held.free), Some(1));
    streams.compile(stream, TIME_LEFT_MS, call()).unwrap();
    let next = sent.try_recv().unwrap().unwrap();
    assert_eq!(next.message, Some(Down::Compile(compile(1, TIME_LEFT_MS))));
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
        Some(Up::Compiled(compiled(7))),
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
async fn at_the_age_or_shutdown_the_stream_ends_cleanly_once_its_compiles_are_answered_or_due() {
    let connection_age = Duration::from_millis(300);
    // Whether the connection age, not shutdown, ends the stream, and whether
    // the Run Executor answers its compile.
    for (at_age, answered) in [(true, true), (false, true), (true, false), (false, false)] {
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
        let time_left_ms = if answered { 10_000 } else { 600 };
        let due = Instant::now() + Duration::from_millis(time_left_ms.into());
        let answer = link.streams.compile(stream, time_left_ms, call()).unwrap();
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
            pull.send(Up::Compiled(compiled(number))).await;
        }
        let reply = answer_within(answer).await;
        if answered {
            assert!(matches!(reply, Ok(Read::Package(_))), "{label}");
        } else {
            // The compile held the stream until its deadline, unanswered.
            assert!(
                matches!(reply, Err(oneshot::error::RecvError { .. })),
                "{label}"
            );
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

#[test]
fn an_answer_that_breaks_the_protocol_changes_nothing() {
    // A call, its sound answer, and whether a read is the one it takes.
    type Fixture = (fn() -> Call, fn(u64) -> Compiled, fn(&Read) -> bool);
    const COMPILE: Fixture = (call, compiled, |read| matches!(read, Read::Package(_)));
    const ANALYSIS: Fixture = (
        analyze,
        |number| analyzed(number, 2),
        |read| matches!(read, Read::Analyzed(found) if found.len() == 2),
    );
    // A case: its name, the call, and the answer that breaks it.
    type Break = (&'static str, Fixture, fn(u64) -> Compiled);
    let breaks: [Break; 11] = [
        ("a number never sent", COMPILE, |number| {
            compiled(number + 1)
        }),
        ("an empty answer", COMPILE, |number| Compiled {
            number,
            answer: None,
        }),
        ("a digest that does not match", COMPILE, |number| Compiled {
            number,
            answer: Some(package("42", package_sha256("4", "2{}"))),
        }),
        ("a failure with no kind", COMPILE, |number| Compiled {
            number,
            answer: Some(Answer::Failure(wire::CompileFailure::default())),
        }),
        ("an analysis for a compile", COMPILE, |number| {
            analyzed(number, 1)
        }),
        ("an analysis failure for a compile", COMPILE, |number| {
            Compiled {
                number,
                answer: Some(Answer::AnalysisFailure(wire::AnalysisFailure {
                    index: 0,
                    failure: Some(failure(CompileError::TimedOut)),
                })),
            }
        }),
        ("a package for an analysis", ANALYSIS, |number| {
            compiled(number)
        }),
        ("an analysis that misses a source", ANALYSIS, |number| {
            analyzed(number, 1)
        }),
        ("an analysis with an extra result", ANALYSIS, |number| {
            analyzed(number, 3)
        }),
        (
            "an analysis that stops past its sources",
            ANALYSIS,
            |number| Compiled {
                number,
                answer: Some(Answer::AnalysisFailure(wire::AnalysisFailure {
                    index: 2,
                    failure: Some(failure(CompileError::TimedOut)),
                })),
            },
        ),
        ("a compile failure for an analysis", ANALYSIS, |number| {
            Compiled {
                number,
                answer: Some(Answer::Failure(failure(CompileError::TimedOut))),
            }
        }),
    ];
    for (name, (call, sound, takes), broken) in breaks {
        let streams = PullStreams::default();
        let (outbound, _sent) = mpsc::unbounded_channel();
        let stream = streams.insert("a-run-executor".to_owned(), dialed_admission(), 1, outbound);
        let mut answer = streams.compile(stream, TIME_LEFT_MS, call()).unwrap();
        let refused = streams.receive(stream, up(broken(1)));
        assert_eq!(
            refused.map_err(|status| status.code()),
            Err(Code::InvalidArgument),
            "{name}"
        );
        // The compile still waits, and takes its answer.
        assert!(streams.compiles_in_flight(stream), "{name}");
        assert!(answer.try_recv().is_err(), "{name}");
        streams.receive(stream, up(sound(1))).unwrap();
        let read = answer.try_recv().unwrap();
        assert!(takes(&read), "{name}: {read:?}");
        // A second answer answers no compile in flight.
        let again = streams.receive(stream, up(sound(1)));
        assert_eq!(
            again.map_err(|status| status.code()),
            Err(Code::InvalidArgument),
            "{name}"
        );
    }
}

#[tokio::test]
async fn a_message_up_to_the_link_bound_reaches_coral() {
    let link = start(Timing::default()).await;
    let channel = link.connect().await;
    let hello = hello(PROTOCOL_LEVEL, 1);
    let id = hello.run_executor_id.clone();
    let mut pull = link.welcomed(&channel, hello).await;
    let stream = link.held(&id, |_| true).await.stream;
    let answer = link.streams.compile(stream, TIME_LEFT_MS, call()).unwrap();
    let Some(Down::Compile(Compile { number, .. })) = pull.next().await.unwrap() else {
        panic!("a Compile");
    };

    // Twice tonic's default bound of 4 MiB.
    let rejection = wire::Rejection {
        kind: "compilation".to_owned(),
        message: "x".repeat(8 * 1024 * 1024),
    };
    let failure = wire::CompileFailure {
        failure: Some(wire::compile_failure::Failure::Rejected(rejection)),
    };
    let compiled = Compiled {
        number,
        answer: Some(Answer::Failure(failure)),
    };
    pull.send(Up::Compiled(compiled)).await;
    let reply = answer_within(answer).await;
    assert!(
        matches!(reply, Ok(Read::Failure(CompileError::Diagnostics(_)))),
        "{reply:?}"
    );
}

// Catches: a size check that rejects a `Compile` that fits, or passes one
// that does not, near the message bound.
#[test]
fn a_call_is_too_large_exactly_when_its_compile_passes_the_bound() {
    let encoded = |call: &Call| {
        let compile = pull_response::Message::Compile(Compile {
            number: u64::MAX,
            time_left_ms: u32::MAX,
            call: Some(call.clone()),
        });
        prost::Message::encoded_len(&response(compile))
    };
    let near = MESSAGE_MAX_BYTES - 64;
    for extra in 0..128 {
        let call = compile_call(
            &"x".repeat(near + extra),
            &[],
            &coral_v8::BuiltinTools::default(),
            None,
        );
        assert_eq!(
            too_large(&call),
            encoded(&call) > MESSAGE_MAX_BYTES,
            "{} bytes encoded",
            encoded(&call)
        );
    }
}
