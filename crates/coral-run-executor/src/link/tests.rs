use std::time::{Duration, Instant};

use coral_test_support::TEST_COMPILATION_TIMEOUT;
use coral_v8::{
    AnalyzedSource, AuthoredToolCompilation, BuiltinTools, CallableDeclaration, CompiledProgram,
    CoralDeclarations, LinkedToolSource, SourceReferences,
};
use tokio::sync::oneshot;
use tonic::Code;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

use super::*;
use crate::testing::{
    Answer, RUN_EXECUTOR_TOKEN, ScriptedCoral, run_executor_config, start_run_executor,
};
use crate::{AnalysisError, CompileError, PullStreamCompiler};

fn config(coral: &ScriptedCoral, compiler_heap_bytes: usize) -> RunExecutorConfig {
    RunExecutorConfig {
        compiler_heap_bytes,
        ..run_executor_config(coral, 8, RunLimits::default())
    }
}

/// Starts a Run Executor that holds eight Runs, until the sender drops.
fn start(coral: &ScriptedCoral, health: Option<TcpListener>) -> oneshot::Sender<()> {
    start_run_executor(coral, 8, RunLimits::default(), health)
}

#[tokio::test]
async fn hello_carries_the_token_and_a_move_reopens_on_a_fresh_connection() {
    let mut coral = ScriptedCoral::start([
        Answer::Move,
        Answer::Move,
        Answer::Move,
        Answer::Welcome(PROTOCOL_LEVEL),
    ])
    .await;
    let _retire = start(&coral, None);

    let first = coral.next_call().await;
    let hello = first.hello.clone().unwrap();
    uuid::Uuid::parse_str(&hello.run_executor_id).unwrap();
    let limits = Limits {
        max_run_duration_seconds: 900,
        fixed: None,
    };
    assert_eq!(
        (hello.protocol_level, hello.free, hello.limits),
        (1, 8, Some(limits))
    );
    let mut previous = None;
    // Three moves, then the fourth stream Coral holds.
    let mut calls = vec![first];
    for _ in 0..3 {
        calls.push(coral.next_call().await);
    }
    for call in calls {
        assert_eq!(
            call.authorization,
            Some(format!("Bearer {RUN_EXECUTOR_TOKEN}"))
        );
        assert_eq!(
            call.hello,
            Some(hello.clone()),
            "each reopen reports the room again"
        );
        assert_ne!(call.peer, previous, "each move dials a fresh connection");
        previous = call.peer;
    }
}

#[tokio::test]
async fn every_other_end_redials_with_the_backoff() {
    for answer in [
        Answer::Refuse(Code::Unavailable),
        Answer::Refuse(Code::Unauthenticated),
        Answer::Refuse(Code::FailedPrecondition),
        Answer::Welcome(PROTOCOL_LEVEL + 2),
        Answer::Close,
    ] {
        let mut coral = ScriptedCoral::start([answer; 4]).await;
        let _retire = start(&coral, None);
        let mut previous = coral.next_call().await.at;
        // Each wait is at least half its ceiling of 100, 200 and 400 ms.
        for floor in [50, 100, 200].map(Duration::from_millis) {
            let at = coral.next_call().await.at;
            assert!(at.duration_since(previous) >= floor, "{answer:?}");
            previous = at;
        }
    }
}

#[test]
fn the_backoff_doubles_to_five_seconds_skips_a_move_and_resets() {
    let mut backoff = Backoff::default();
    let ceilings = [100, 200, 400, 800, 1600, 3200, 5000, 5000].map(Duration::from_millis);
    for ceiling in ceilings {
        // A move reopens at once and leaves the next wait where it is.
        assert_eq!(backoff.after(&End::Moved), None);
        let delay = backoff.after(&End::Failed).unwrap();
        assert!(ceiling / 2 <= delay && delay <= ceiling, "{delay:?}");
    }
    backoff.reset();
    assert!(backoff.after(&End::Failed).unwrap() <= FIRST_BACKOFF);
}

#[test]
fn an_address_dials_only_http_with_a_host() {
    for (address, dials) in [
        ("http://coral:50052", true),
        ("http://127.0.0.1:50052", true),
        ("https://coral:50052", false),
        ("coral:50052", false),
        ("http:///path", false),
        ("not a url", false),
    ] {
        let parsed = address.parse::<CoralAddress>();
        assert_eq!(parsed.is_ok(), dials, "{address}");
        if let Err(error) = parsed {
            assert!(matches!(error, AddressError), "{address}");
        }
    }
}

#[test]
fn a_token_is_visible_ascii_up_to_16_kib() {
    for (token, accepted) in [
        ("x".repeat(16 * 1024), true),
        (String::new(), false),
        ("two words".to_owned(), false),
        ("line\nbreak".to_owned(), false),
        ("caf\u{e9}".to_owned(), false),
        ("x".repeat(16 * 1024 + 1), false),
    ] {
        let created = Token::new(token.clone());
        assert_eq!(created.is_ok(), accepted, "{token:.16}");
        if let Err(error) = created {
            assert!(matches!(error, TokenError), "{token:.16}");
        }
    }
}

#[tokio::test]
async fn readiness_serves_only_while_a_pull_stream_is_linked() {
    let mut coral = ScriptedCoral::start([
        Answer::Refuse(Code::Unavailable),
        Answer::Welcome(PROTOCOL_LEVEL),
    ])
    .await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health = format!("http://{}", listener.local_addr().unwrap());
    let _retire = start(&coral, Some(listener));
    let mut health = HealthClient::new(Endpoint::from_shared(health).unwrap().connect_lazy());

    coral.next_call().await;
    wait_for(&mut health, ServingStatus::NotServing).await;
    coral.next_call().await;
    wait_for(&mut health, ServingStatus::Serving).await;
    coral.release();
    wait_for(&mut health, ServingStatus::NotServing).await;
}

async fn wait_for(health: &mut HealthClient<tonic::transport::Channel>, status: ServingStatus) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let request = HealthCheckRequest {
            service: READINESS_SERVICE.to_owned(),
        };
        if let Ok(response) = health.check(request).await
            && response.into_inner().status == status as i32
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "readiness never reached {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

const PROGRAM: &str = "const team = await coral.private.team.lookup({});\n\
    const org = await coral.shared.team.org({});\n\
    return await coral.providers.github.repos.get({ owner: team, repo: org })\n\
    + await coral.providers.notion.search({});";
const LOOKUP: &str = "export default async function lookup() { return \"withcoral\"; }";
const ORG: &str = "export default async function org() { return \"coral\"; }";
const SAVED: &str = "export default async function repo(input: { owner: string }) {\n\
    return coral.providers.github.repos.get({ owner: input.owner, repo: 1 })\n\
    ?? coral.private.team.lookup({}) ?? coral.shared.team.org({})\n\
    ?? coral.providers.github[input.owner]({});\n}";
const NOT_DATA: &str = "export default async function run(input: { callback: () => void }) {\n\
    return coral.search({ query: \"repos\" });\n}";
const REPOS_GET: &str = "export default function (input: { \"owner\": string; \"repo\": string }): Promise<{ status: 200; data: { \"id\": number } }>;";

/// The Tool the callable checks below allow.
const REPOS_GET_PATH: &str = "coral.providers.github.repos.get";

/// What a call returned: its result's debug text, which carries every field,
/// and its error's code.
type Called = (String, Option<&'static str>);

/// A call: its name, the error code it ends with, and the call.
type NamedCall = (
    &'static str,
    Option<&'static str>,
    fn(&dyn CompilerClient) -> Called,
);

fn called<T: std::fmt::Debug>(result: &Result<T, CompileError>) -> Called {
    let code = result.as_ref().err().map(CompileError::code);
    (format!("{result:?}"), code)
}

fn analyzed<T: std::fmt::Debug>(result: &Result<T, AnalysisError>) -> Called {
    let code = result.as_ref().err().map(|stopped| stopped.error.code());
    (format!("{result:?}"), code)
}

/// One call of each kind, on success and on a typed error.
const CALLS: [NamedCall; 12] = [
    ("an analysis", None, |client| {
        let sources = [
            AnalyzedSource::Program(PROGRAM.to_owned()),
            AnalyzedSource::AuthoredTool(SAVED.to_owned()),
        ];
        analyzed(&client.analyze(TEST_COMPILATION_TIMEOUT, &sources))
    }),
    (
        "an invalid reference in the second source",
        Some("invalid_linked_tool_reference"),
        |client| {
            let sources = [
                AnalyzedSource::Program(PROGRAM.to_owned()),
                AnalyzedSource::Program("return await coral.private[name]();".to_owned()),
            ];
            analyzed(&client.analyze(TEST_COMPILATION_TIMEOUT, &sources))
        },
    ),
    ("a compile", None, |client| {
        let none = BuiltinTools::default();
        called(&client.compile(TEST_COMPILATION_TIMEOUT, "return 42;", &[], &none, None))
    }),
    ("a linked compile with Built-in Tools", None, |client| {
        let source = "return [await coral.private.team.lookup({}), \
            await coral.shared.team.org({}), await coral.search({ query: \"x\" })];";
        let linked = [lookup(), org()];
        let search = BuiltinTools::new(["search"]);
        called(&client.compile(TEST_COMPILATION_TIMEOUT, source, &linked, &search, None))
    }),
    ("a checked compile", None, |client| {
        let source = "return await coral.providers.github.repos.get({});";
        let callable = [REPOS_GET_PATH.to_owned()];
        let none = BuiltinTools::default();
        called(&client.compile(
            TEST_COMPILATION_TIMEOUT,
            source,
            &[],
            &none,
            Some(&callable),
        ))
    }),
    ("unknown Tool calls", Some("unknown_tool_calls"), |client| {
        let callable = [REPOS_GET_PATH.to_owned()];
        let linked = [lookup(), org()];
        let none = BuiltinTools::default();
        called(&client.compile(
            TEST_COMPILATION_TIMEOUT,
            PROGRAM,
            &linked,
            &none,
            Some(&callable),
        ))
    }),
    (
        "unknown Tool calls in a linked Tool",
        Some("unknown_tool_calls"),
        |client| {
            let callable = [REPOS_GET_PATH.to_owned()];
            let linked = [LinkedToolSource::shared(
                vec!["team".to_owned(), "org".to_owned()],
                "export default async function org() { \
                    return coral.providers.notion.search({}); }"
                    .to_owned(),
            )];
            let source = "return await coral.shared.team.org({});";
            let none = BuiltinTools::default();
            called(&client.compile(
                TEST_COMPILATION_TIMEOUT,
                source,
                &linked,
                &none,
                Some(&callable),
            ))
        },
    ),
    ("a rejected compile", Some("compilation"), |client| {
        let none = BuiltinTools::default();
        called(&client.compile(TEST_COMPILATION_TIMEOUT, "return (", &[], &none, None))
    }),
    ("an untyped prepare", None, |client| {
        let none = BuiltinTools::default();
        let prepared = client.prepare(TEST_COMPILATION_TIMEOUT, SAVED, &none, None);
        // The case carries every reference kind, so a field the wire drops
        // shows as a difference.
        let found = prepared.as_ref().map(|prepared| {
            let tools = prepared.tools();
            (
                prepared.references().len(),
                prepared.shared_references().len(),
                tools.called_paths(),
                tools.runtime_built_paths(),
            )
        });
        let expected = (
            1,
            1,
            vec![REPOS_GET_PATH.to_owned()],
            vec!["coral.providers.github".to_owned()],
        );
        assert_eq!(found.ok(), Some(expected));
        called(&prepared)
    }),
    (
        "a prepare that takes something other than data",
        None,
        |client| {
            let search = BuiltinTools::new(["search"]);
            let prepared = client.prepare(TEST_COMPILATION_TIMEOUT, NOT_DATA, &search, None);
            let not_data = prepared
                .as_ref()
                .map(|prepared| prepared.not_data().is_some());
            assert_eq!(not_data.ok(), Some(true));
            called(&prepared)
        },
    ),
    ("a typed prepare", None, |client| {
        let declaration = |path: &str, text: &str| {
            CallableDeclaration::new(
                path.split('.').map(str::to_owned).collect(),
                text.to_owned(),
            )
        };
        let declarations = CoralDeclarations::new(256 * 1024)
            .with_tools(vec![declaration("providers.github.repos.get", REPOS_GET)])
            .with_private_tools(vec![declaration(
                "team.lookup",
                "export default function lookup(): Promise<string>;",
            )])
            .with_shared_tools(vec![declaration(
                "team.org",
                "export default function org(): Promise<string>;",
            )]);
        let none = BuiltinTools::default();
        called(&client.prepare(TEST_COMPILATION_TIMEOUT, SAVED, &none, Some(&declarations)))
    }),
    (
        "a rejected prepare",
        Some("invalid_authored_tool"),
        |client| {
            let none = BuiltinTools::default();
            let source = "import x from \"y\";";
            called(&client.prepare(TEST_COMPILATION_TIMEOUT, source, &none, None))
        },
    ),
];

fn lookup() -> LinkedToolSource {
    LinkedToolSource::private(
        vec!["team".to_owned(), "lookup".to_owned()],
        LOOKUP.to_owned(),
    )
}

fn org() -> LinkedToolSource {
    LinkedToolSource::shared(vec!["team".to_owned(), "org".to_owned()], ORG.to_owned())
}

#[tokio::test]
async fn each_call_on_a_pull_stream_returns_what_the_in_process_client_returns() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let _retire = start(&coral, None);
    let mut call = coral.next_call().await;
    let remote = Arc::clone(&call.compiler);
    let results = tokio::task::spawn_blocking(move || {
        let local = InProcessCompiler::new(IsolateLimits::defaults()).unwrap();
        CALLS.map(|(name, code, call)| (name, code, call(remote.as_ref()), call(&local)))
    })
    .await
    .unwrap();
    for (name, code, remote, local) in &results {
        assert_eq!(remote, local, "{name}");
        assert_eq!(remote.1, *code, "{name}: {}", remote.0);
    }
    // Each answer frees the unit its compile held and reports the room.
    for _ in CALLS {
        assert_eq!(call.rooms.recv().await, Some(8));
    }
}

/// Catches: a Room after a Compiled that leaves out the Runs held, or one
/// after a freed Run that leaves out the compiles held.
#[tokio::test]
async fn a_held_run_and_a_compile_share_the_room() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let _retire = start(&coral, None);
    let mut call = coral.next_call().await;
    coral.take("run-1", "claim-1");
    let run = coral.next_run().await;
    let remote = Arc::clone(&call.compiler);
    let compiled = tokio::task::spawn_blocking(move || {
        let none = BuiltinTools::default();
        remote.compile(TEST_COMPILATION_TIMEOUT, "return 1;", &[], &none, None)
    })
    .await
    .unwrap();
    assert!(compiled.is_ok(), "{compiled:?}");
    assert_eq!(call.rooms.recv().await, Some(7), "the Run holds its unit");
    drop(run);
    assert_eq!(call.rooms.recv().await, Some(8), "the Run's unit frees");
}

/// The in-process compiler with a gate: each compile reports the time it
/// was given, then waits for a release.
struct Gated {
    compiler: InProcessCompiler,
    entered: mpsc::UnboundedSender<Duration>,
    released: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl CompilerClient for Gated {
    fn analyze(
        &self,
        time_left: Duration,
        sources: &[AnalyzedSource],
    ) -> Result<Vec<SourceReferences>, AnalysisError> {
        self.compiler.analyze(time_left, sources)
    }

    fn compile(
        &self,
        time_left: Duration,
        source: &str,
        linked_tools: &[LinkedToolSource],
        builtin_tools: &BuiltinTools,
        callable: Option<&[String]>,
    ) -> Result<CompiledProgram, CompileError> {
        let stopped = || CompileError::Unavailable(crate::Unavailable::Stopped);
        self.entered.send(time_left).map_err(|_gone| stopped())?;
        let released = self.released.lock().map_err(|_poisoned| stopped())?;
        released.recv().map_err(|_gone| stopped())?;
        self.compiler
            .compile(time_left, source, linked_tools, builtin_tools, callable)
    }

    fn prepare(
        &self,
        time_left: Duration,
        source: &str,
        builtin_tools: &BuiltinTools,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<AuthoredToolCompilation, CompileError> {
        self.compiler
            .prepare(time_left, source, builtin_tools, declarations)
    }
}

#[tokio::test]
async fn a_queued_compile_holds_its_room_and_its_time_runs_from_its_arrival() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let (entered, mut entries) = mpsc::unbounded_channel();
    let (release, released) = std::sync::mpsc::channel();
    let compiler =
        tokio::task::spawn_blocking(|| InProcessCompiler::new(IsolateLimits::defaults()))
            .await
            .unwrap()
            .unwrap();
    let gated = Gated {
        compiler,
        entered,
        released: std::sync::Mutex::new(released),
    };
    let heap = IsolateLimits::defaults().compiler_heap_bytes();
    let (linked, _linked) = watch::channel(false);
    let (on_queued, mut queued) = mpsc::unbounded_channel();
    let mut link = Link::new(config(&coral, heap), Arc::new(gated), linked);
    link.on_queued = Some(on_queued);
    tokio::spawn(async move { link.hold().await });
    let mut call = coral.next_call().await;
    let compile = |compiler: &Arc<PullStreamCompiler>, time_left| {
        let compiler = Arc::clone(compiler);
        std::thread::spawn(move || {
            compiler
                .compile(time_left, "return 42;", &[], &BuiltinTools::default(), None)
                .map(drop)
        })
    };
    let first = compile(&call.compiler, TEST_COMPILATION_TIMEOUT);
    entries.recv().await.unwrap();
    // A second compile arrives while the first holds the compiler thread.
    let short = Duration::from_millis(500);
    let second = compile(&call.compiler, short);
    for _compile in 0..2 {
        queued.recv().await.unwrap();
    }
    // Its time runs out while it waits.
    tokio::time::sleep(short).await;
    release.send(()).unwrap();
    assert_eq!(
        call.rooms.recv().await,
        Some(7),
        "the second holds its unit"
    );
    assert_eq!(entries.recv().await, Some(Duration::ZERO), "no time left");
    release.send(()).unwrap();
    assert_eq!(call.rooms.recv().await, Some(8));
    assert_eq!(first.join().unwrap(), Ok(()));
    // Its caller stopped waiting at its own deadline, before this answer.
    let _gave_up = second.join().unwrap();
}

#[tokio::test]
async fn a_compiler_that_cannot_start_ends_the_run_executor_before_any_dial() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    // A valid heap limit too small for the compiler to load in.
    let result = run(config(&coral, 8 * 1024 * 1024), None, pending()).await;
    assert!(
        matches!(
            result,
            Err(RunError::Compiler(coral_v8::Error::MemoryLimitExceeded))
        ),
        "{result:?}"
    );
    assert!(coral.try_next_call().is_none(), "no pull stream opened");
}
