//! Program admission wired through the compile relay: a Run
//! Module compiles through its Coral Instance's link, and a compile that
//! finds no Run Executor with room there is served through the database.
//! The relay's own rules are tested in `coral-run-executor`.
use super::*;
use coral_run_executor::testing::{RUN_EXECUTOR_TOKEN, relayed_requests};
use coral_run_executor::{
    CompilerClient, Credentials, LinkServer, PullStreams, RunExecutorConfig, RunLimits, Token,
};

/// One Coral Instance's link on a loopback port, served until the test ends.
struct Link {
    compiler: Arc<dyn CompilerClient>,
    streams: PullStreams,
    address: std::net::SocketAddr,
}

async fn serve_link(fixture: &TestModules, instance: &str, tokens: Vec<Token>) -> Link {
    let link = LinkServer::new(
        Credentials::new(tokens),
        fixture.database.clone(),
        instance.to_owned(),
    );
    let compiler = Arc::new(link.compiler(None));
    let streams = link.streams();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let address = listener.local_addr().expect("the link's address");
    tokio::spawn(link.serve(listener, std::future::pending()));
    Link {
        compiler,
        streams,
        address,
    }
}

fn token() -> Token {
    Token::new(RUN_EXECUTOR_TOKEN.to_owned()).expect("a Run Executor token")
}

/// A Run Executor in this process that dials `link` and holds it.
fn dial(link: &Link) {
    let config = RunExecutorConfig {
        coral_address: format!("http://{}", link.address)
            .parse()
            .expect("the link's address"),
        token: token(),
        local_child: false,
        max_runs_held: 2,
        run_limits: RunLimits::default(),
    };
    tokio::spawn(coral_run_executor::run(
        config,
        None,
        std::future::pending(),
    ));
}

/// Waits until a Run Executor holds a pull stream on `link`.
async fn linked(link: &Link) {
    wait_until_async(async || !link.streams.held().is_empty()).await;
}

/// Waits until admission waits on the relay: the write of its request, a
/// probe or a compile, raises the change signal.
async fn relayed(fixture: &TestModules) {
    let mut changes = fixture
        .database
        .subscribe_changes()
        .expect("the change signal");
    let written = async {
        while relayed_requests(&fixture.database)
            .await
            .expect("the relayed requests")
            == 0
        {
            changes.changed().await.expect("the change signal");
        }
    };
    tokio::time::timeout(Duration::from_secs(30), written)
        .await
        .expect("a relayed compile");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_program_admitted_where_no_run_executor_is_linked_compiles_on_another_coral_instance() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let here = serve_link(&fixture, "coral-here", Vec::new()).await;
    let elsewhere = serve_link(&fixture, "coral-elsewhere", vec![token()]).await;
    dial(&elsewhere);
    linked(&elsewhere).await;
    let runs = fixture.live_runs_compiling_through(here.compiler.clone());
    let (workspace_id, principal) = fixture
        .user_scope("user:relayed-admission", "Relay a compile")
        .await;

    runs.prepare(&principal, &workspace_id, submission("return 42;"))
        .await
        .expect("the compile is served on the other Coral Instance");
    assert!(
        here.streams.held().is_empty(),
        "no Run Executor linked here"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_program_admitted_before_any_run_executor_links_compiles_once_one_dials_in() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let link = serve_link(&fixture, "coral-only", vec![token()]).await;
    let runs = fixture.live_runs_compiling_through(link.compiler.clone());
    let (workspace_id, principal) = fixture
        .user_scope("user:waiting-admission", "Wait for a Run Executor")
        .await;

    let prepared = tokio::spawn(async move {
        runs.prepare(&principal, &workspace_id, submission("return 42;"))
            .await
            .map(drop)
    });
    relayed(&fixture).await;
    dial(&link);
    prepared
        .await
        .expect("admission task")
        .expect("the compile is served once a Run Executor dials in");
}
