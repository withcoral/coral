#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test: only the Run Executor process and its pull stream are under test."
)]

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coral_run_executor::testing::{Answer, ScriptedCoral};
use coral_run_executor::{
    ChildCommand, Credentials, IsolateLimits, Kind, LinkServer, LocalChild, LocalRunExecutors,
    PROTOCOL_LEVEL, ProcessStarter, Token,
};
use coral_test_support::TestDatabase;
use tokio::io::AsyncWriteExt as _;
use tokio::process::{Child, Command};

/// The variables the Run Executor reads, which the test must not inherit.
const RUN_EXECUTOR_VARIABLES: [&str; 7] = [
    "CORAL_RUN_EXECUTOR_CORAL_ADDRESS",
    "CORAL_RUN_EXECUTOR_TOKEN",
    "CORAL_RUN_EXECUTOR_TOKEN_STDIN",
    "CORAL_RUN_EXECUTOR_MAX_RUNS_HELD",
    "CORAL_RUN_EXECUTOR_EXECUTION_HEAP_BYTES",
    "CORAL_RUN_EXECUTOR_COMPILER_HEAP_BYTES",
    "CORAL_RUN_EXECUTOR_HEALTH_LISTEN",
];

/// Starts the real binary against `coral`. The rest of the environment
/// stays: Windows needs `SystemRoot` to open a socket.
fn spawn(coral: &ScriptedCoral, configure: impl FnOnce(&mut Command)) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_coral"));
    for variable in RUN_EXECUTOR_VARIABLES {
        command.env_remove(variable);
    }
    command
        .args(["run-executor", "--coral-address", coral.address()])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true);
    configure(&mut command);
    command.spawn().expect("spawn the Run Executor")
}

#[tokio::test]
async fn a_local_child_says_hello_with_its_stdin_token_and_retires_when_stdin_closes() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let mut child = spawn(&coral, |command| {
        command.arg("--token-stdin");
    });
    let mut stdin = child.stdin.take().expect("the Run Executor's stdin");
    stdin
        .write_all(b"local-child-token\n")
        .await
        .expect("write the token");

    let call = coral.next_call().await;
    assert_eq!(
        call.authorization.as_deref(),
        Some("Bearer local-child-token")
    );
    let hello = call.hello.expect("the pull stream opens with Hello");
    assert_eq!((hello.protocol_level, hello.free), (PROTOCOL_LEVEL, 128));
    assert!(
        child.try_wait().expect("poll the Run Executor").is_none(),
        "the Run Executor runs while stdin is open"
    );

    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .expect("the Run Executor retires when stdin closes")
        .expect("wait for the Run Executor");
    assert!(status.success(), "{status}");
}

/// Catches: a local child that outlives the Coral that started it, holding
/// a Run no Coral will end.
#[tokio::test]
async fn a_local_child_exits_once_its_parent_is_gone() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let mut child = spawn(&coral, |command| {
        command.arg("--token-stdin");
    });
    let mut stdin = child.stdin.take().expect("the Run Executor's stdin");
    stdin
        .write_all(b"local-child-token\n")
        .await
        .expect("write the token");
    let mut call = coral.next_call().await;
    coral.take("run-1", "claim-1");
    let run = coral.next_run().await;

    // Coral's exit closes the child's stdin and its listener.
    drop(stdin);
    assert_eq!(call.rooms.recv().await, Some(0), "it retires");
    assert!(
        child.try_wait().expect("poll the Run Executor").is_none(),
        "it holds the Run while Coral answers"
    );
    coral.stop().await;
    run.fail(tonic::Status::unavailable("Coral is gone")).await;
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .expect("the Run Executor exits")
        .expect("wait for the Run Executor");
    assert!(!status.success(), "{status}");
}

/// Catches: a Run Executor that exits on SIGTERM while it holds a Run, or
/// one that never exits once that Run ends.
#[cfg(unix)]
#[tokio::test]
async fn after_sigterm_the_run_executor_exits_once_its_run_ends() {
    use coral_run_executor::testing::{ended, started};
    use coral_run_executor::wire::v1::{
        InvocationResult, invocation_result, run_request, run_response,
    };

    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let mut child = spawn(&coral, |command| {
        command.env("CORAL_RUN_EXECUTOR_TOKEN", "dialing-token");
    });
    let mut call = coral.next_call().await;
    let package = compiled("await coral.providers.tracker.issues.list({}); return 1;").await;
    let mut run = started(&mut coral, package, None).await;
    let number = |message: Option<run_request::Message>| match message {
        Some(run_request::Message::Request(request)) => request.number,
        other => panic!("expected a request, got {other:?}"),
    };
    assert_eq!(number(run.next().await), 1, "the Invoke");

    let pid = child.id().expect("the Run Executor's pid").to_string();
    let kill = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status();
    assert!(kill.expect("send SIGTERM").success());
    assert_eq!(call.rooms.recv().await, Some(0), "it retires");
    run.send(run_response::Message::InvocationResult(InvocationResult {
        number: 1,
        bytes: 2,
        outcome: Some(invocation_result::Outcome::ResultJson("[]".to_owned())),
    }))
    .await;
    assert_eq!(number(run.next().await), 2, "the Finish");
    assert!(
        child.try_wait().expect("poll the Run Executor").is_none(),
        "the Run Executor holds the Run until it ends"
    );
    run.send(ended()).await;
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .expect("the Run Executor exits once its Run ends")
        .expect("wait for the Run Executor");
    assert!(status.success(), "{status}");
}

#[tokio::test]
async fn a_dialing_run_executor_takes_its_token_from_the_environment() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let _child = spawn(&coral, |command| {
        command.env("CORAL_RUN_EXECUTOR_TOKEN", "dialing-token");
    });

    let call = coral.next_call().await;
    assert_eq!(call.authorization.as_deref(), Some("Bearer dialing-token"));
}

/// Catches: a Run heap limit that never reaches the Run Executor, so that
/// every Run gets the default 128 MiB, as a local child or as a dialing
/// Run Executor.
#[tokio::test]
async fn each_run_gets_the_heap_limit_its_run_executor_was_given() {
    use coral_run_executor::testing::started;
    use coral_run_executor::wire::v1::{Finish, FinishEnd, Request, finish, request, run_request};

    /// How the case starts its Run Executor.
    #[derive(Clone, Copy, Debug)]
    enum Start {
        Dialing,
        DialingWithEnvironment,
        LocalChildWithFlag,
    }
    // 16 to 32 MiB, by V8's element size, that stays reachable until the
    // Program returns: past an 8 MiB limit, and well within the default
    // 128 MiB.
    const PROGRAM: &str = "const chunks: number[][] = []; \
        for (let i = 0; i < 4; i++) chunks.push(new Array(1 << 20).fill(i)); \
        return chunks.length;";
    let low = IsolateLimits::new(8 << 20, IsolateLimits::defaults().compiler_heap_bytes())
        .expect("valid heap limits");
    let package = compiled(PROGRAM).await;
    let memory_limit = finish::Completion::End(FinishEnd::MemoryLimit.into());
    for (start, expected) in [
        (
            Start::Dialing,
            finish::Completion::ValueJson("4".to_owned()),
        ),
        (Start::DialingWithEnvironment, memory_limit.clone()),
        (Start::LocalChildWithFlag, memory_limit),
    ] {
        let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
        let dialing_token = |command: &mut Command| {
            command.env("CORAL_RUN_EXECUTOR_TOKEN", "dialing-token");
        };
        // Held until the case ends: each kills its Run Executor on drop.
        let _run_executor: Box<dyn std::any::Any> = match start {
            Start::Dialing => Box::new(spawn(&coral, dialing_token)),
            Start::DialingWithEnvironment => Box::new(spawn(&coral, |command| {
                dialing_token(command);
                command.env(
                    "CORAL_RUN_EXECUTOR_EXECUTION_HEAP_BYTES",
                    low.execution_heap_bytes().to_string(),
                );
            })),
            Start::LocalChildWithFlag => {
                let mut command = ChildCommand {
                    program: env!("CARGO_BIN_EXE_coral").into(),
                    coral_address: coral.address().to_owned(),
                    max_runs_held: 2,
                    isolate_limits: low,
                    environment: child_environment(),
                };
                let token = Token::new("local-child-token".to_owned()).unwrap();
                let child = command.start(&token).expect("start the local child");
                Box::new(SharedChild(Arc::new(Mutex::new(child))))
            }
        };
        let _call = coral.next_call().await;
        let mut run = started(&mut coral, package.clone(), None).await;
        let completion = match run.next().await {
            Some(run_request::Message::Request(Request {
                kind: Some(request::Kind::Finish(Finish { completion })),
                ..
            })) => completion,
            other => panic!("expected the Finish, got {other:?}"),
        };
        assert_eq!(completion, Some(expected), "{start:?}");
    }
}

/// `source` compiled as a Program with the default heap limits.
async fn compiled(source: &str) -> coral_run_executor::wire::v1::RunPackage {
    use coral_run_executor::{BuiltinTools, CompilerClient as _, InProcessCompiler};

    let compiler = InProcessCompiler::new(IsolateLimits::defaults()).expect("a compiler");
    let program = compiler
        .compile(
            coral_test_support::TEST_COMPILATION_TIMEOUT,
            source,
            &[],
            &BuiltinTools::default(),
            None,
        )
        .await
        .expect("the Program compiles");
    coral_run_executor::testing::package(program.javascript(), program.source_map())
}

/// Starts real children and keeps each one where the test reaches it.
struct SharedStarter {
    command: ChildCommand,
    children: Arc<Mutex<Vec<SharedChild>>>,
}

#[derive(Clone)]
struct SharedChild(Arc<Mutex<Box<dyn LocalChild>>>);

impl ProcessStarter for SharedStarter {
    fn start(&mut self, token: &Token) -> std::io::Result<Box<dyn LocalChild>> {
        let child = SharedChild(Arc::new(Mutex::new(self.command.start(token)?)));
        self.children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(child.clone());
        Ok(Box::new(child))
    }
}

/// A test that fails before the child retires leaves no process behind.
impl Drop for SharedChild {
    fn drop(&mut self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .kill();
    }
}

impl LocalChild for SharedChild {
    fn close_stdin(&mut self) {
        self.0.lock().expect("the child").close_stdin();
    }

    fn kill(&mut self) {
        self.0.lock().expect("the child").kill();
    }

    fn exited(&mut self) -> Option<Option<i32>> {
        self.0.lock().expect("the child").exited()
    }
}

/// A local child's whole environment: nothing, except what Windows needs
/// to open a socket.
#[expect(
    clippy::disallowed_methods,
    reason = "the test passes on what bootstrap would"
)]
fn child_environment() -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    std::env::var_os("SystemRoot")
        .filter(|_| cfg!(windows))
        .map(|root| ("SystemRoot".into(), root))
        .into_iter()
        .collect()
}

#[tokio::test]
async fn coral_starts_a_local_child_that_opens_its_pull_stream_and_exits_when_its_stdin_closes() {
    let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let credentials = Credentials::default();
    let link = LinkServer::new(
        credentials.clone(),
        database.database(),
        "coral-instance".to_owned(),
    );
    let streams = link.streams();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(link.serve(listener, std::future::pending()));
    let children = Arc::new(Mutex::new(Vec::new()));
    let starter = SharedStarter {
        command: ChildCommand {
            program: env!("CARGO_BIN_EXE_coral").into(),
            coral_address: format!("http://{address}"),
            max_runs_held: 3,
            isolate_limits: IsolateLimits::defaults(),
            environment: child_environment(),
        },
        children: children.clone(),
    };
    let supervisor = LocalRunExecutors::start(starter, 1, credentials, streams.clone(), None);

    supervisor.request_room();
    let opened = async {
        loop {
            if let [stream] = streams.held().as_slice() {
                return stream.clone();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let stream = tokio::time::timeout(Duration::from_secs(30), opened)
        .await
        .expect("the child opens its pull stream");
    assert_eq!((stream.kind, stream.free), (Kind::Local, 3));
    let child = children.lock().expect("the children").first().cloned();
    let mut child = child.expect("the supervisor started the child");
    assert_eq!(child.exited(), None, "the child runs while stdin is open");

    child.close_stdin();
    let exited = async {
        loop {
            if let Some(code) = child.exited() {
                return code;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let code = tokio::time::timeout(Duration::from_secs(30), exited)
        .await
        .expect("the child exits when its stdin closes");
    assert_eq!(code, Some(0));
}
