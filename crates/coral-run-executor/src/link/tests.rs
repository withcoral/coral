use std::time::{Duration, Instant};

use coral_test_support::TEST_COMPILATION_TIMEOUT;
use coral_v8::{
    CallableDeclaration, CompiledProgram, CoralDeclarations, LinkedSavedFunctionSource,
    SavedFunctionCallPaths, SavedFunctionCompilation, SavedFunctionReference, ToolReferences,
    UnknownToolCall,
};
use tokio::sync::oneshot;
use tonic::Code;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

use super::*;
use crate::testing::{Answer, ScriptedCoral};
use crate::{CompileError, PullStreamCompiler};

const TOKEN: &str = "test-run-executor-token";

fn config(coral: &ScriptedCoral, compiler_heap_bytes: usize) -> RunExecutorConfig {
    RunExecutorConfig {
        coral_address: coral.address().parse().unwrap(),
        token: Token::new(TOKEN.to_owned()).unwrap(),
        max_runs_held: 8,
        compiler_heap_bytes,
    }
}

/// Starts a Run Executor that holds eight Runs, until the sender drops.
fn start(coral: &ScriptedCoral, health: Option<TcpListener>) -> oneshot::Sender<()> {
    let config = config(coral, IsolateLimits::defaults().compiler_heap_bytes());
    let (retire, retired) = oneshot::channel::<()>();
    tokio::spawn(run(config, health, async {
        drop(retired.await);
    }));
    retire
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
            call.authorization.as_deref(),
            Some("Bearer test-run-executor-token")
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

const PROGRAM: &str = "const team = await coral.functions.team.lookup({});\n\
    return await coral.providers.github.repos.get({ owner: team, repo: \"coral\" })\n\
    + await coral.providers.notion.search({});";
const LOOKUP: &str = "export default async function lookup() { return \"withcoral\"; }";
const SAVED: &str = "export default async function repo(input: { owner: string }) {\n\
    return coral.providers.github.repos.get({ owner: input.owner, repo: 1 })\n\
    ?? coral.functions.team.lookup({});\n}";
const REPOS_GET: &str = "export default function (input: { \"owner\": string; \"repo\": string }): Promise<{ status: 200; data: { \"id\": number } }>;";

/// A named call, which returns its result's debug text: it carries every
/// field.
type NamedCall = (&'static str, fn(&dyn CompilerClient) -> String);

/// One call of each kind, on success and on a typed error.
const CALLS: [NamedCall; 11] = [
    ("program references", |client| {
        format!(
            "{:?}",
            client.program_references(TEST_COMPILATION_TIMEOUT, PROGRAM)
        )
    }),
    ("an invalid reference", |client| {
        let source = "return await coral.functions[name]();";
        format!(
            "{:?}",
            client.program_references(TEST_COMPILATION_TIMEOUT, source)
        )
    }),
    ("unknown Tool calls", |client| {
        let named = client
            .program_references(TEST_COMPILATION_TIMEOUT, PROGRAM)
            .unwrap();
        let callable = ["coral.providers.github.repos.get".to_owned()];
        let unknown = client.unknown_tool_calls(TEST_COMPILATION_TIMEOUT, named.tools(), &callable);
        format!("{unknown:?}")
    }),
    ("dependency references", |client| {
        format!(
            "{:?}",
            client.dependency_references(TEST_COMPILATION_TIMEOUT, SAVED)
        )
    }),
    ("a compile", |client| {
        format!(
            "{:?}",
            client.compile(TEST_COMPILATION_TIMEOUT, "return 42;", &[])
        )
    }),
    ("a linked compile", |client| {
        let lookup = LinkedSavedFunctionSource::new(
            vec!["team".to_owned(), "lookup".to_owned()],
            LOOKUP.to_owned(),
        );
        let source = "return await coral.functions.team.lookup({});";
        format!(
            "{:?}",
            client.compile(TEST_COMPILATION_TIMEOUT, source, &[lookup])
        )
    }),
    ("a rejected compile", |client| {
        format!(
            "{:?}",
            client.compile(TEST_COMPILATION_TIMEOUT, "return (", &[])
        )
    }),
    ("call paths", |client| {
        format!("{:?}", client.call_paths(TEST_COMPILATION_TIMEOUT, SAVED))
    }),
    ("an untyped prepare", |client| {
        format!(
            "{:?}",
            client.prepare(TEST_COMPILATION_TIMEOUT, SAVED, None)
        )
    }),
    ("a typed prepare", |client| {
        let named = client.call_paths(TEST_COMPILATION_TIMEOUT, SAVED).unwrap();
        let declaration = |path: &str, text: &str| {
            CallableDeclaration::new(
                path.split('.').map(str::to_owned).collect(),
                text.to_owned(),
            )
        };
        let declarations = CoralDeclarations::new(256 * 1024)
            .with_tools(
                named.tools(),
                vec![declaration("providers.github.repos.get", REPOS_GET)],
            )
            .with_functions(vec![declaration(
                "team.lookup",
                "export default function lookup(): Promise<string>;",
            )]);
        let prepared = client.prepare(TEST_COMPILATION_TIMEOUT, SAVED, Some(&declarations));
        format!("{prepared:?}")
    }),
    ("a rejected prepare", |client| {
        format!(
            "{:?}",
            client.prepare(TEST_COMPILATION_TIMEOUT, "import x from \"y\";", None)
        )
    }),
];

#[tokio::test]
async fn each_call_on_a_pull_stream_returns_what_the_in_process_client_returns() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let _retire = start(&coral, None);
    let mut call = coral.next_call().await;
    let remote = Arc::clone(&call.compiler);
    let results = tokio::task::spawn_blocking(move || {
        let local = InProcessCompiler::new(IsolateLimits::defaults()).unwrap();
        CALLS.map(|(name, call)| (name, call(remote.as_ref()), call(&local)))
    })
    .await
    .unwrap();
    for (name, remote, local) in &results {
        assert_eq!(remote, local, "{name}");
    }
    assert!(
        results
            .iter()
            .any(|(_, result, _)| result.starts_with("Err(Diagnostics(Reference"))
    );
    // Each answer frees the unit its compile held and reports the room. The
    // eleven cases send thirteen calls: two cases call twice.
    let answers = 13;
    for _ in 0..answers {
        assert_eq!(call.rooms.recv().await, Some(8));
    }
}

/// The in-process compiler with a gate: each compile waits for a release.
struct Gated {
    compiler: InProcessCompiler,
    entered: mpsc::UnboundedSender<()>,
    released: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl CompilerClient for Gated {
    fn program_references(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<SavedFunctionCallPaths, CompileError> {
        self.compiler.program_references(time_left, source)
    }

    fn unknown_tool_calls(
        &self,
        time_left: Duration,
        tools: &ToolReferences,
        callable: &[String],
    ) -> Result<Vec<UnknownToolCall>, CompileError> {
        self.compiler.unknown_tool_calls(time_left, tools, callable)
    }

    fn dependency_references(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, CompileError> {
        self.compiler.dependency_references(time_left, source)
    }

    fn compile(
        &self,
        time_left: Duration,
        source: &str,
        saved_functions: &[LinkedSavedFunctionSource],
    ) -> Result<CompiledProgram, CompileError> {
        let stopped = || CompileError::Unavailable(crate::Unavailable::Stopped);
        self.entered.send(()).map_err(|_gone| stopped())?;
        let released = self.released.lock().map_err(|_poisoned| stopped())?;
        released.recv().map_err(|_gone| stopped())?;
        self.compiler.compile(time_left, source, saved_functions)
    }

    fn call_paths(
        &self,
        time_left: Duration,
        source: &str,
    ) -> Result<SavedFunctionCallPaths, CompileError> {
        self.compiler.call_paths(time_left, source)
    }

    fn prepare(
        &self,
        time_left: Duration,
        source: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<SavedFunctionCompilation, CompileError> {
        self.compiler.prepare(time_left, source, declarations)
    }
}

#[tokio::test]
async fn a_compile_holds_a_unit_of_room_until_it_is_answered() {
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
    let link = Link::new(config(&coral, heap), Arc::new(gated), linked);
    tokio::spawn(async move { link.hold().await });
    let mut call = coral.next_call().await;
    let compile = |compiler: &Arc<PullStreamCompiler>| {
        let compiler = Arc::clone(compiler);
        std::thread::spawn(move || {
            compiler
                .compile(TEST_COMPILATION_TIMEOUT, "return 42;", &[])
                .map(drop)
        })
    };
    let first = compile(&call.compiler);
    entries.recv().await.unwrap();
    // A second compile arrives while the first holds the compiler thread.
    let second = compile(&call.compiler);
    tokio::time::sleep(Duration::from_millis(250)).await;
    release.send(()).unwrap();
    assert_eq!(
        call.rooms.recv().await,
        Some(7),
        "the second holds its unit"
    );
    entries.recv().await.unwrap();
    release.send(()).unwrap();
    assert_eq!(call.rooms.recv().await, Some(8));
    for caller in [first, second] {
        assert_eq!(caller.join().unwrap(), Ok(()));
    }
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
