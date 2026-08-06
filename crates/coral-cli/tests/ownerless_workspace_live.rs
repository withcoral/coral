//! Live proof that a shared deployment serves around a workspace nobody owns,
//! and admits nobody to it — the host included.
//!
//! Ignored by default because it launches real `coral` processes and writes an
//! evidence document:
//!
//! ```text
//! CORAL_BREAK_GLASS_EVIDENCE_PATH=/tmp/break-glass-live.md \
//!   cargo test -p coral-cli --test ownerless_workspace_live -- --ignored --nocapture
//! ```
//!
//! Every process is pointed at a `TempDir` through `CORAL_CONFIG_DIR` with
//! `HOME` redirected beside it, so no run can reach the operator's own state.
//!
//! # What this proves, and what it cannot
//!
//! Repairing an ownerless workspace happens outside the shipped product, in
//! `cargo run -p xtask --features admin -- workspace-admin set-owner`, because
//! `[auth]` leaves no principal that could appoint an owner over an RPC. That
//! tool lives in a later commit than this test, so the appointment itself is
//! not exercised here and is not simulated either: writing the membership row
//! by hand would prove the test can write a row, not that the tool works.
//!
//! What is proved end to end with the shipped binaries is the state the tool
//! exists to repair, and the guarantee that survives it:
//!
//! - an ownerless workspace does not block startup — `coral server` binds and
//!   announces its listener with two of them present;
//! - those two workspaces really are there and really are unowned, which the
//!   concealment itself cannot show: creation is the one surface that answers
//!   from the row instead of from the membership, and it reports them as
//!   already existing;
//! - a signed-in person is concealed from an ownerless workspace, and reaches
//!   the workspace they do own on the same connection;
//! - the host, running ordinary `coral` commands against that same
//!   `[auth]`-configured state directory, is concealed from every workspace —
//!   the ownerless ones and the owned one alike.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the crate's dependency set and exercise only a subset of it."
)]

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use coral_api::v1::{
    CatalogItemKind, CreateWorkspaceRequest, ListCatalogRequest, ListWorkspacesRequest,
    PaginationRequest, Workspace,
};
use coral_client::BearerToken;
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
use tempfile::TempDir;
use tonic::{Code, Request};

/// Where to write the evidence document.
///
/// The variable keeps its old name so the invocation recorded alongside the
/// existing evidence still works. What it records is no longer a break-glass
/// procedure — only the lock-out that makes one necessary.
const EVIDENCE_PATH_ENV: &str = "CORAL_BREAK_GLASS_EVIDENCE_PATH";
/// The workspace every install seeds, and one of the two an upgrade strands.
const SEEDED_WORKSPACE: &str = "default";
/// A second stranded workspace, so nothing here can pass by special-casing the
/// seeded name.
const SHARED_WORKSPACE: &str = "analytics";
const PROCESS_TIMEOUT: Duration = Duration::from_mins(2);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Directory values and secrets the evidence document must never contain.
const PROVIDER_ISSUER: &str = "https://accounts.ownerless.test";
const PROVIDER_SUBJECT: &str = "ownerless-subject";
const CLIENT_SECRET: &str = "ownerless-client-secret";

/// Session-token settings, which have to agree with `[auth]` below.
const SESSION_ISSUER: &str = "https://auth.ownerless.test";
const SESSION_AUDIENCE: &str = "https://reef.ownerless.test";
const SESSION_CLIENT_ID: &str = "https://client.ownerless.test/client.json";
const SESSION_TTL: Duration = Duration::from_mins(5);

/// A pre-access-control workspace catalog, which the state migration carries
/// into the database with no memberships at all.
const BASE_CONFIG: &str = "[trace_history]\nenabled = false\n\n[credentials]\nstorage = \"file\"\n\n[workspaces.default]\n\n[workspaces.analytics]\n";

/// Isolated state directory, redirected home, and captured process logs.
struct Sandbox {
    _temp: TempDir,
    state: PathBuf,
    home: PathBuf,
    logs: PathBuf,
}

impl Sandbox {
    fn new() -> Self {
        let temp = TempDir::new().expect("temp dir");
        let (state, home, logs) = (
            temp.path().join("state"),
            temp.path().join("home"),
            temp.path().join("logs"),
        );
        for dir in [&state, &home, &logs] {
            fs::create_dir_all(dir).expect("create sandbox dir");
        }
        let signing_key =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key");
        fs::write(state.join("session.key"), signing_key.as_ref()).expect("session key");
        let sandbox = Self {
            _temp: temp,
            state,
            home,
            logs,
        };
        sandbox.write_config();
        sandbox
    }

    /// Writes a `config.toml` whose `[auth]` section is what makes the state
    /// directory shared — and therefore leaves it without a superuser.
    fn write_config(&self) {
        let config = format!(
            "{BASE_CONFIG}\n[auth]\nallowed_audiences = ['{SESSION_AUDIENCE}/']\n\n[auth.session]\nsigning_key_file = 'session.key'\n\n[auth.authorization_server]\nissuer = '{SESSION_ISSUER}'\n\n[auth.provider]\nissuer = '{PROVIDER_ISSUER}'\nclient_id = 'ownerless-client'\nclient_secret = '{CLIENT_SECRET}'\nredirect_uri = '{SESSION_ISSUER}/auth/oidc/callback'\n"
        );
        fs::write(self.state.join("config.toml"), config).expect("write config");
    }

    fn signing_key(&self) -> Vec<u8> {
        fs::read(self.state.join("session.key")).expect("read session key")
    }

    fn spawn(&self, label: &'static str, args: &[&str]) -> CoralProcess {
        let stdout_path = self.logs.join(format!("{label}.out"));
        let stderr_path = self.logs.join(format!("{label}.err"));
        let child = Command::new(env!("CARGO_BIN_EXE_coral"))
            .args(args)
            .env("CORAL_CONFIG_DIR", &self.state)
            .env("HOME", &self.home)
            .env_remove("CORAL_ENDPOINT")
            .env_remove("CORAL_WORKSPACE")
            .stdin(Stdio::null())
            .stdout(Stdio::from(File::create(&stdout_path).expect("stdout log")))
            .stderr(Stdio::from(File::create(&stderr_path).expect("stderr log")))
            .spawn()
            .expect("spawn coral");
        CoralProcess {
            label,
            child,
            stdout_path,
            stderr_path,
        }
    }

    /// Runs one host command to completion and returns its exit status.
    fn run(&self, label: &'static str, args: &[&str]) -> (ExitStatus, CoralProcess) {
        let mut process = self.spawn(label, args);
        let status = process.wait_for_exit();
        (status, process)
    }
}

/// A live `coral` process whose output is captured to files.
///
/// [`Drop`] kills it, so a panic anywhere in the protocol still leaves no
/// `coral` process behind.
struct CoralProcess {
    label: &'static str,
    child: Child,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl CoralProcess {
    /// Waits for a long-running process to announce its listener, and returns
    /// the endpoint it named. A process that exits first has failed to serve.
    fn announced_endpoint(&mut self, needle: &str) -> String {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            if let Some(line) = read_log(&self.stdout_path)
                .lines()
                .find(|line| line.contains(needle))
            {
                return line
                    .trim()
                    .rsplit_once(' ')
                    .map(|(_, endpoint)| endpoint.to_string())
                    .expect("announced endpoint");
            }
            if let Some(status) = self.tick(deadline, &format!("print {needle:?}")) {
                panic!("{} exited {status}\nstderr: {}", self.label, self.stderr());
            }
        }
    }

    fn wait_for_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            if let Some(status) = self.tick(deadline, "exit") {
                return status;
            }
        }
    }

    /// Reports the exit status if the process is done, and otherwise sleeps one
    /// interval, failing once the deadline passes.
    fn tick(&mut self, deadline: Instant, expectation: &str) -> Option<ExitStatus> {
        if let Some(status) = self.child.try_wait().expect("poll coral") {
            return Some(status);
        }
        assert!(
            Instant::now() < deadline,
            "{} did not {expectation}\nstderr: {}",
            self.label,
            self.stderr()
        );
        std::thread::sleep(POLL_INTERVAL);
        None
    }

    fn stdout(&self) -> String {
        read_log(&self.stdout_path)
    }

    fn stderr(&self) -> String {
        read_log(&self.stderr_path)
    }

    fn stop(&mut self) {
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

impl Drop for CoralProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

fn read_log(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

/// The evidence document named by `CORAL_BREAK_GLASS_EVIDENCE_PATH`.
///
/// It is rewritten after every step, so an interrupted run still leaves what it
/// completed, and every rewrite is checked against the directory values and
/// secrets that must never appear in it.
struct Evidence {
    path: PathBuf,
    started: Instant,
    steps: Vec<String>,
    forbidden: Vec<String>,
}

impl Evidence {
    #[expect(
        clippy::disallowed_methods,
        reason = "The ignored live proof is explicitly gated by this test-only variable."
    )]
    fn new() -> Self {
        let path = std::env::var(EVIDENCE_PATH_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| panic!("{EVIDENCE_PATH_ENV} must name the evidence file"));
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create evidence directory");
        }
        Self {
            path,
            started: Instant::now(),
            steps: Vec::new(),
            forbidden: vec![
                PROVIDER_ISSUER.to_string(),
                PROVIDER_SUBJECT.to_string(),
                CLIENT_SECRET.to_string(),
            ],
        }
    }

    /// Adds a value minted during the run to the redaction check.
    fn redact(&mut self, value: &str) {
        self.forbidden.push(value.to_string());
    }

    fn record(&mut self, command: &str, outcome: &str) {
        let unix = unix_seconds();
        let elapsed = self.started.elapsed().as_secs_f64();
        self.steps.push(format!(
            "| {unix} | +{elapsed:.1} | `{command}` | {outcome} |"
        ));
        self.flush();
    }

    fn flush(&self) {
        let written = unix_seconds();
        let mut document = format!(
            "# Ownerless-workspace live proof\n\n\
             Produced by the `coral-cli` test `live_ownerless_workspace_serves_and_admits_nobody`,\n\
             written {written}. Times are seconds since the Unix epoch (UTC), then\n\
             seconds elapsed since the run started. No issuer, subject, token, or user\n\
             identifier appears below: the test fails if one does.\n\n\
             | Unix time | Elapsed | Command or call | Result |\n\
             | --- | --- | --- | --- |\n"
        );
        for step in &self.steps {
            document.push_str(step);
            document.push('\n');
        }
        document.push_str(
            "\n## Not covered here\n\n\
             Appointing an owner is `cargo run -p xtask --features admin -- workspace-admin\n\
             set-owner`, which lands in a later commit than this test. The repair and the\n\
             recovery it unlocks are therefore absent above rather than simulated: writing\n\
             the membership row directly would prove only that the test can write a row.\n",
        );
        for secret in &self.forbidden {
            assert!(
                !document.contains(secret.as_str()),
                "evidence must not disclose directory values or secrets"
            );
        }
        fs::write(&self.path, document).expect("write evidence");
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_secs()
}

fn catalog_request(workspace: &str) -> ListCatalogRequest {
    ListCatalogRequest {
        workspace: Some(Workspace {
            name: workspace.to_string(),
        }),
        catalog_name: String::new(),
        schema_name: String::new(),
        kind: CatalogItemKind::Unspecified as i32,
        pagination: Some(PaginationRequest {
            limit: 1,
            offset: 0,
        }),
    }
}

/// Starts `coral server` against state that holds two ownerless workspaces.
///
/// The upgrade this reproduces used to be refused before any listener was
/// bound. It is served now: a workspace with no members conceals itself from
/// every caller, so there is nothing to refuse.
fn start_server(sandbox: &Sandbox, evidence: &mut Evidence) -> (CoralProcess, String) {
    let mut server = sandbox.spawn("server", &["server"]);
    let endpoint = server.announced_endpoint("Coral gRPC server listening on");
    assert!(endpoint.contains("127.0.0.1"), "{endpoint}");
    evidence.record(
        "coral server",
        &format!(
            "announced a loopback listener with `{SEEDED_WORKSPACE}` and `{SHARED_WORKSPACE}` ownerless; startup is no longer refused"
        ),
    );
    (server, endpoint)
}

/// Proves membership is the only key a signed-in person holds.
///
/// The same connection reaches the workspace they own and is told the ownerless
/// ones do not exist, so the concealment cannot be an authentication failure.
async fn prove_signed_in_person_sees_only_their_own(
    endpoint: &str,
    token: &str,
    personal_workspace: &str,
    evidence: &mut Evidence,
) {
    let client = coral_client::local::connect_with_loopback_bearer(
        endpoint,
        BearerToken::new(token).expect("bearer token"),
    )
    .await
    .expect("connect the signed-in person");

    let memberships = client
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("list the signed-in person's workspaces")
        .into_inner()
        .memberships
        .into_iter()
        .filter_map(|membership| membership.workspace)
        .map(|workspace| workspace.name)
        .collect::<Vec<_>>();
    assert_eq!(
        memberships,
        vec![personal_workspace.to_string()],
        "a signed-in person must see their own workspace and no other"
    );
    evidence.record(
        "coral.v1.WorkspaceService/ListWorkspaces (signed-in person)",
        "1 workspace — their own; neither ownerless workspace is listed",
    );

    client
        .catalog_client()
        .list_catalog(Request::new(catalog_request(personal_workspace)))
        .await
        .expect("read the catalog of the workspace they own");
    evidence.record(
        "coral.v1.CatalogService/ListCatalog (signed-in person, own workspace)",
        "OK; membership admits them to the workspace they own",
    );

    for workspace in [SEEDED_WORKSPACE, SHARED_WORKSPACE] {
        let denied = client
            .catalog_client()
            .list_catalog(Request::new(catalog_request(workspace)))
            .await
            .expect_err("an ownerless workspace must conceal itself");
        assert_eq!(denied.code(), Code::NotFound, "{workspace}: {denied}");
        evidence.record(
            &format!("coral.v1.CatalogService/ListCatalog (signed-in person, {workspace})"),
            "NotFound; an ownerless workspace is concealed from an authenticated caller",
        );

        // Concealment renders an existing workspace and an absent one
        // identically, so every NotFound above would also be satisfied by a
        // deployment that simply has no such workspace. Creation is the one
        // surface that answers from the row rather than from the membership,
        // and it refuses: the workspaces are really there, nobody owns them,
        // and being locked out of one is not a way to take it over.
        let conflict = client
            .workspace_client()
            .create_workspace(Request::new(CreateWorkspaceRequest {
                workspace: Some(Workspace {
                    name: workspace.to_string(),
                }),
            }))
            .await
            .expect_err("an existing workspace must not be re-creatable");
        assert_eq!(
            conflict.code(),
            Code::AlreadyExists,
            "{workspace}: {conflict}"
        );
        evidence.record(
            &format!("coral.v1.WorkspaceService/CreateWorkspace (signed-in person, {workspace})"),
            "AlreadyExists; the workspace exists, is owned by nobody, and cannot be claimed by re-creation",
        );
    }
}

/// Proves the host holds no privilege over an `[auth]`-configured state
/// directory, which is the behavior change an operator notices first.
///
/// `coral` subcommands start the app in-process and run as the built-in local
/// principal. That principal used to own everything. It now has the memberships
/// it was granted — none — so it is concealed from the ownerless workspaces and
/// from the owned one alike.
fn prove_host_commands_reach_nothing(
    sandbox: &Sandbox,
    personal_workspace: &str,
    evidence: &mut Evidence,
) {
    let (status, list) = sandbox.run("workspace-list", &["workspace", "list"]);
    assert!(
        status.success(),
        "a host command must still serve [auth]-configured state: {}",
        list.stderr()
    );
    let listed = list.stdout();
    assert!(
        listed.contains("No workspaces configured."),
        "the host must be a member of nothing: {listed}"
    );
    evidence.record(
        "coral workspace list",
        &format!("{status}; `No workspaces configured.` — the host belongs to none of the three"),
    );

    for (label, workspace, named, described) in [
        (
            "sql-seeded",
            SEEDED_WORKSPACE,
            SEEDED_WORKSPACE,
            "an ownerless workspace",
        ),
        (
            "sql-shared",
            SHARED_WORKSPACE,
            SHARED_WORKSPACE,
            "an ownerless workspace",
        ),
        (
            "sql-personal",
            personal_workspace,
            "<a workspace someone owns>",
            "a workspace a signed-in person owns",
        ),
    ] {
        let (status, query) = sandbox.run(label, &["--workspace", workspace, "sql", "select 1"]);
        let stderr = query.stderr();
        assert!(
            !status.success(),
            "the host must not query {described}: {stderr}"
        );
        assert!(
            stderr.contains(&format!("workspace '{workspace}' not found")),
            "the host must be told {described} does not exist: {stderr}"
        );
        evidence.record(
            &format!("coral --workspace {named} sql 'select 1'"),
            &format!("{status}; `workspace not found` — {described} is concealed from the host"),
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "launches real coral processes and needs CORAL_BREAK_GLASS_EVIDENCE_PATH"]
async fn live_ownerless_workspace_serves_and_admits_nobody() {
    let mut evidence = Evidence::new();
    let sandbox = Sandbox::new();
    evidence.record(
        "prepare an isolated state directory with [auth] and a pre-upgrade workspace catalog",
        &format!(
            "CORAL_CONFIG_DIR and HOME both point inside a TempDir; `{SEEDED_WORKSPACE}` and `{SHARED_WORKSPACE}` carry no memberships"
        ),
    );

    let user_id = coral_app::test_session_tokens::provision_test_login(
        &sandbox.state,
        PROVIDER_ISSUER,
        PROVIDER_SUBJECT,
        Some("Ownerless Live Operator"),
        "ownerless-workspace-live-attribution",
    )
    .await
    .expect("provision one signed-in person");
    evidence.redact(&user_id);
    let personal_workspace = format!("default-{user_id}");
    evidence.record(
        "sign one person in through the production login path",
        "provisioned with a personal default workspace they own (identifier withheld)",
    );

    let token = coral_app::test_session_tokens::issue_access_token(
        SESSION_ISSUER,
        &sandbox.signing_key(),
        SESSION_TTL,
        &user_id,
        SESSION_CLIENT_ID,
        SESSION_AUDIENCE,
    )
    .expect("mint a session token through the real issuer");
    evidence.redact(&token);

    let (mut server, endpoint) = start_server(&sandbox, &mut evidence);
    prove_signed_in_person_sees_only_their_own(
        &endpoint,
        &token,
        &personal_workspace,
        &mut evidence,
    )
    .await;
    prove_host_commands_reach_nothing(&sandbox, &personal_workspace, &mut evidence);
    server.stop();

    // Nothing above appointed an owner, so a restart meets the same state and
    // serves it again: the lock-out is stable, not a first-boot artifact.
    let (mut restarted, _) = start_server(&sandbox, &mut evidence);
    restarted.stop();
}
