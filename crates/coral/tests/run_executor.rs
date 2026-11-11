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
    ChildCommand, Credentials, Kind, LinkServer, LocalChild, LocalRunExecutors, PROTOCOL_LEVEL,
    ProcessStarter, Token,
};
use coral_test_support::TestDatabase;
use tokio::io::AsyncWriteExt as _;
use tokio::process::{Child, Command};

/// The variables the Run Executor reads, which the test must not inherit.
const RUN_EXECUTOR_VARIABLES: [&str; 6] = [
    "CORAL_RUN_EXECUTOR_CORAL_ADDRESS",
    "CORAL_RUN_EXECUTOR_TOKEN",
    "CORAL_RUN_EXECUTOR_TOKEN_STDIN",
    "CORAL_RUN_EXECUTOR_MAX_RUNS_HELD",
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

#[tokio::test]
async fn a_dialing_run_executor_takes_its_token_from_the_environment() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let _child = spawn(&coral, |command| {
        command.env("CORAL_RUN_EXECUTOR_TOKEN", "dialing-token");
    });

    let call = coral.next_call().await;
    assert_eq!(call.authorization.as_deref(), Some("Bearer dialing-token"));
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
            compiler_heap_bytes: 512 << 20,
            environment: child_environment(),
        },
        children: children.clone(),
    };
    let supervisor = LocalRunExecutors::start(starter, 1, credentials, streams.clone());

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
