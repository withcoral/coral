//! Live proof that shared mode serves explicit legacy ownerless workspaces
//! without granting them to an authenticated user or the host.
//!
//! Ignored by default because it launches real `coral` processes and writes a
//! redacted evidence document:
//!
//! ```text
//! CORAL_OWNERLESS_EVIDENCE_PATH=/tmp/ownerless-live.md \
//!   cargo test -p coral-cli --test ownerless_workspace_live -- --ignored --nocapture
//! ```
//!
//! Every process uses an isolated `CORAL_CONFIG_DIR` and home directory. The
//! proof initializes shared mode first, persists one synthetic test identity through
//! the production identity-only database transaction, and mints its session
//! token through Coral's test-only issuer.

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
    PaginationRequest, Workspace, WorkspaceRole,
};
use coral_client::{AppClient, BearerToken};
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
use tempfile::TempDir;
use tonic::{Code, Request};

const EVIDENCE_PATH_ENV: &str = "CORAL_OWNERLESS_EVIDENCE_PATH";
const LEGACY_ALPHA: &str = "legacy-alpha";
const LEGACY_BETA: &str = "legacy-beta";
const OWNED_WORKSPACE: &str = "explicitly-owned";
const PROCESS_TIMEOUT: Duration = Duration::from_mins(2);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

const PROVIDER_ISSUER: &str = "https://accounts.ownerless.test";
const PROVIDER_SUBJECT: &str = "ownerless-subject";
const CLIENT_SECRET: &str = "ownerless-client-secret";
const SESSION_ISSUER: &str = "https://auth.ownerless.test";
const SESSION_AUDIENCE: &str = "https://surface.ownerless.test";
const SESSION_CLIENT_ID: &str = "https://client.ownerless.test/client.json";
const SESSION_TTL: Duration = Duration::from_mins(5);

const BASE_CONFIG: &str = "[trace_history]\nenabled = false\n\n[credentials]\nstorage = \"file\"\n\n[server]\nbind_addr = \"127.0.0.1:0\"\n\n[workspaces.legacy-alpha]\n\n[workspaces.legacy-beta]\n";

struct Sandbox {
    temp: TempDir,
    state: PathBuf,
    home: PathBuf,
    logs: PathBuf,
}

impl Sandbox {
    fn new() -> Self {
        let temp = TempDir::new().expect("temp dir");
        let state = temp.path().join("state");
        let home = temp.path().join("home");
        let logs = temp.path().join("logs");
        for dir in [&state, &home, &logs] {
            fs::create_dir_all(dir).expect("create sandbox dir");
        }
        let signing_key =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key");
        fs::write(state.join("session.key"), signing_key.as_ref()).expect("session key");
        let sandbox = Self {
            temp,
            state,
            home,
            logs,
        };
        sandbox.write_config();
        sandbox
    }

    fn write_config(&self) {
        let config = format!(
            "{BASE_CONFIG}\n[auth]\nhttp_bind_addr = '127.0.0.1:0'\nallowed_audiences = ['{SESSION_AUDIENCE}/']\n\n[auth.session]\nsigning_key_file = 'session.key'\n\n[auth.authorization_server]\nissuer = '{SESSION_ISSUER}'\n\n[auth.provider]\nissuer = '{PROVIDER_ISSUER}'\nclient_id = 'ownerless-client'\nclient_secret = '{CLIENT_SECRET}'\nredirect_uri = '{SESSION_ISSUER}/auth/oidc/callback'\n"
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
            .env("USERPROFILE", &self.home)
            .env_remove("CORAL_ENDPOINT")
            .env_remove("CORAL_WORKSPACE")
            .stdin(Stdio::null())
            .stdout(Stdio::from(File::create(&stdout_path).expect("stdout log")))
            .stderr(Stdio::from(File::create(&stderr_path).expect("stderr log")))
            .spawn()
            .expect("spawn Coral");
        CoralProcess {
            label,
            child,
            stdout_path,
            stderr_path,
            stopped: false,
        }
    }

    fn run(&self, label: &'static str, args: &[&str]) -> (ExitStatus, CoralProcess) {
        let mut process = self.spawn(label, args);
        let status = process.wait_for_exit();
        (status, process)
    }

    fn redactions(&self) -> Vec<String> {
        [
            self.temp.path(),
            self.state.as_path(),
            self.home.as_path(),
            self.logs.as_path(),
        ]
        .into_iter()
        .map(|path| path.display().to_string())
        .chain([
            PROVIDER_ISSUER.to_string(),
            PROVIDER_SUBJECT.to_string(),
            SESSION_ISSUER.to_string(),
            CLIENT_SECRET.to_string(),
        ])
        .collect()
    }
}

struct CoralProcess {
    label: &'static str,
    child: Child,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    stopped: bool,
}

impl CoralProcess {
    fn announced_endpoint(&mut self) -> String {
        let needle = "Coral gRPC server listening on";
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            if let Some(line) = self.stdout().lines().find(|line| line.contains(needle)) {
                return line
                    .trim()
                    .rsplit_once(' ')
                    .map(|(_, endpoint)| endpoint.to_string())
                    .expect("announced endpoint");
            }
            if let Some(status) = self.tick(deadline, "announce its gRPC listener") {
                panic!("{} exited {status}\nstderr: {}", self.label, self.stderr());
            }
        }
    }

    fn wait_for_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            if let Some(status) = self.tick(deadline, "exit") {
                self.stopped = true;
                return status;
            }
        }
    }

    fn tick(&mut self, deadline: Instant, expectation: &str) -> Option<ExitStatus> {
        if let Some(status) = self.child.try_wait().expect("poll Coral") {
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
        if self.stopped {
            return;
        }
        if self.child.try_wait().expect("read Coral status").is_none() {
            #[cfg(unix)]
            assert!(
                Command::new("kill")
                    .args(["-TERM", &self.child.id().to_string()])
                    .status()
                    .expect("send SIGTERM to Coral")
                    .success(),
                "send SIGTERM to Coral"
            );
            #[cfg(not(unix))]
            self.child.kill().expect("stop Coral");
        }
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("read Coral status") {
                break status;
            }
            if Instant::now() >= deadline {
                self.child.kill().expect("kill unresponsive Coral");
                break self.child.wait().expect("wait for killed Coral");
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        self.stopped = true;
        #[cfg(unix)]
        assert!(status.success(), "Coral shutdown failed with {status}");
        #[cfg(not(unix))]
        drop(status);
    }
}

impl Drop for CoralProcess {
    fn drop(&mut self) {
        if !self.stopped {
            drop(self.child.kill());
            drop(self.child.wait());
        }
    }
}

fn read_log(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

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
    fn new(forbidden: Vec<String>) -> Self {
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
            forbidden,
        }
    }

    fn redact(&mut self, value: &str) {
        self.forbidden.push(value.to_string());
    }

    fn record(&mut self, command: &str, outcome: &str) {
        self.steps.push(format!(
            "| {} | +{:.1} | `{command}` | {outcome} |",
            unix_seconds(),
            self.started.elapsed().as_secs_f64()
        ));
        self.flush();
    }

    fn flush(&self) {
        let mut document = format!(
            "# Shared ownerless-workspace live proof\n\n\
             Produced by `ownerless_workspace_live` at {}. The proof uses real\n\
             Coral processes, the production identity-only database transaction,\n\
             and Coral's test-only session-token issuer. Identity identifiers, credentials,\n\
             and isolated directory paths are withheld.\n\n\
             | Unix time | Elapsed | Command or call | Result |\n\
             | --- | --- | --- | --- |\n",
            unix_seconds()
        );
        for step in &self.steps {
            document.push_str(step);
            document.push('\n');
        }
        for forbidden in &self.forbidden {
            assert!(
                !document.contains(forbidden),
                "evidence must not disclose a protected value"
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

fn workspace(name: &str) -> Workspace {
    Workspace {
        name: name.to_string(),
    }
}

fn catalog_request(name: &str) -> ListCatalogRequest {
    ListCatalogRequest {
        workspace: Some(workspace(name)),
        catalog_name: String::new(),
        schema_name: String::new(),
        kind: CatalogItemKind::Unspecified as i32,
        pagination: Some(PaginationRequest {
            limit: 1,
            offset: 0,
        }),
    }
}

fn start_server(
    sandbox: &Sandbox,
    label: &'static str,
    evidence: &mut Evidence,
) -> (CoralProcess, String) {
    let mut server = sandbox.spawn(label, &["server"]);
    let endpoint = server.announced_endpoint();
    assert!(endpoint.contains("127.0.0.1"), "{endpoint}");
    evidence.record(
        "coral server",
        "shared gRPC listener announced; two legacy workspaces are explicitly configured",
    );
    (server, endpoint)
}

async fn authenticated_client(endpoint: &str, token: &str) -> AppClient {
    coral_client::local::connect_with_loopback_bearer(
        endpoint,
        BearerToken::new(token).expect("bearer token"),
    )
    .await
    .expect("connect authenticated client")
}

async fn prove_authenticated_workspace_boundaries(
    endpoint: &str,
    token: &str,
    evidence: &mut Evidence,
) {
    let client = authenticated_client(endpoint, token).await;
    let initial = client
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("list initial memberships")
        .into_inner();
    assert!(
        initial.memberships.is_empty(),
        "identity persistence must not create a workspace or membership"
    );
    evidence.record(
        "coral.v1.WorkspaceService/ListWorkspaces (authenticated)",
        "0 memberships before explicit creation",
    );

    let created = client
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace(OWNED_WORKSPACE)),
        }))
        .await
        .expect("create explicitly owned workspace")
        .into_inner()
        .workspace
        .expect("created workspace");
    assert_eq!(created.name, OWNED_WORKSPACE);

    let memberships = client
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("list memberships after explicit creation")
        .into_inner()
        .memberships;
    let [membership] = memberships.as_slice() else {
        panic!("authenticated user must have exactly one membership")
    };
    assert_eq!(
        membership
            .workspace
            .as_ref()
            .map(|workspace| workspace.name.as_str()),
        Some(OWNED_WORKSPACE)
    );
    assert_eq!(membership.role, WorkspaceRole::Owner as i32);
    client
        .catalog_client()
        .list_catalog(Request::new(catalog_request(OWNED_WORKSPACE)))
        .await
        .expect("read explicitly owned workspace");
    evidence.record(
        "CreateWorkspace, ListWorkspaces, and ListCatalog (authenticated)",
        "explicit workspace created with Owner role and remained readable",
    );

    for name in [LEGACY_ALPHA, LEGACY_BETA] {
        let concealed = client
            .catalog_client()
            .list_catalog(Request::new(catalog_request(name)))
            .await
            .expect_err("ownerless workspace must be concealed");
        assert_eq!(concealed.code(), Code::NotFound, "{name}: {concealed}");
        let conflict = client
            .workspace_client()
            .create_workspace(Request::new(CreateWorkspaceRequest {
                workspace: Some(workspace(name)),
            }))
            .await
            .expect_err("existing ownerless workspace cannot be recreated");
        assert_eq!(conflict.code(), Code::AlreadyExists, "{name}: {conflict}");
        evidence.record(
            &format!("ListCatalog and CreateWorkspace (authenticated, {name})"),
            "NotFound for reads and AlreadyExists for creation",
        );
    }
}

fn assert_host_is_rejected(status: ExitStatus, process: &CoralProcess) {
    let stderr = process.stderr();
    assert!(!status.success(), "host command unexpectedly succeeded");
    assert!(
        stderr
            .to_ascii_lowercase()
            .contains("authentication required"),
        "host command must require authentication: {stderr}"
    );
}

fn prove_host_is_not_a_shared_principal(sandbox: &Sandbox, evidence: &mut Evidence) {
    let (list_status, list) = sandbox.run("host-list", &["workspace", "list"]);
    assert_host_is_rejected(list_status, &list);
    evidence.record(
        "coral workspace list",
        "Rejected with authentication required",
    );

    let (sql_status, sql) = sandbox.run(
        "host-sql",
        &["--workspace", LEGACY_ALPHA, "sql", "select 1"],
    );
    assert_host_is_rejected(sql_status, &sql);
    evidence.record(
        "coral --workspace legacy-alpha sql 'select 1'",
        "Rejected with authentication required",
    );
}

async fn prove_restart(endpoint: &str, token: &str, evidence: &mut Evidence) {
    let client = authenticated_client(endpoint, token).await;
    let memberships = client
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("list memberships after restart")
        .into_inner()
        .memberships;
    let [membership] = memberships.as_slice() else {
        panic!("authenticated user must retain exactly one membership")
    };
    assert_eq!(
        membership
            .workspace
            .as_ref()
            .map(|workspace| workspace.name.as_str()),
        Some(OWNED_WORKSPACE)
    );
    client
        .catalog_client()
        .list_catalog(Request::new(catalog_request(OWNED_WORKSPACE)))
        .await
        .expect("read owned workspace after restart");
    for name in [LEGACY_ALPHA, LEGACY_BETA] {
        let concealed = client
            .catalog_client()
            .list_catalog(Request::new(catalog_request(name)))
            .await
            .expect_err("ownerless workspace must stay concealed after restart");
        assert_eq!(concealed.code(), Code::NotFound, "{name}: {concealed}");
    }
    evidence.record(
        "restart, ListWorkspaces, and ListCatalog",
        "explicit ownership remained readable and ownerless state stayed concealed",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "launches real Coral processes and needs CORAL_OWNERLESS_EVIDENCE_PATH"]
async fn shared_ownerless_workspaces_remain_isolated_across_restart() {
    let sandbox = Sandbox::new();
    let mut evidence = Evidence::new(sandbox.redactions());
    evidence.record(
        "prepare isolated shared state with two explicit legacy workspaces",
        "config and home are temporary; both workspaces begin without memberships",
    );

    let (mut server, endpoint) = start_server(&sandbox, "server-first", &mut evidence);
    let identity = coral_app::test_session_tokens::persist_test_login_identity(
        &sandbox.state,
        PROVIDER_ISSUER,
        PROVIDER_SUBJECT,
        Some("Ownerless Live Operator"),
        "ownerless-live-attribution",
    )
    .await
    .expect("persist one synthetic test identity");
    assert!(
        !identity.local_ownership_migration_completed,
        "shared startup must not claim local ownership migration"
    );
    evidence.redact(&identity.user_id);
    evidence.record(
        "persist synthetic test identity through production identity-only persistence",
        "identity persisted; local ownership migration remained unclaimed",
    );

    let token = coral_app::test_session_tokens::issue_access_token(
        SESSION_ISSUER,
        &sandbox.signing_key(),
        SESSION_TTL,
        &identity.user_id,
        SESSION_CLIENT_ID,
        SESSION_AUDIENCE,
    )
    .expect("mint test session token");
    evidence.redact(&token);

    prove_authenticated_workspace_boundaries(&endpoint, &token, &mut evidence).await;
    prove_host_is_not_a_shared_principal(&sandbox, &mut evidence);
    server.stop();

    let (mut restarted, endpoint) = start_server(&sandbox, "server-restart", &mut evidence);
    prove_restart(&endpoint, &token, &mut evidence).await;
    restarted.stop();

    let same_identity = coral_app::test_session_tokens::persist_test_login_identity(
        &sandbox.state,
        PROVIDER_ISSUER,
        PROVIDER_SUBJECT,
        Some("Ownerless Live Operator"),
        "ownerless-live-attribution",
    )
    .await
    .expect("re-read identity state after restart");
    assert_eq!(same_identity.user_id, identity.user_id);
    assert!(
        !same_identity.local_ownership_migration_completed,
        "restarted shared mode must leave local ownership migration unclaimed"
    );
    evidence.record(
        "inspect identity and migration state after restart",
        "identity remained stable; local ownership migration remained unclaimed",
    );
}
