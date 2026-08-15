//! Public gRPC evidence that a trace is read by the owner of the workspace it
//! belongs to and by nobody else, and that this host's runtime features are
//! configured from the host rather than from any credential a shared deployment
//! issues.
//!
//! A trace carries the query text, arguments, and errors of whoever ran it, so
//! reading one is an owner's act rather than a member's. The request that names
//! no workspace is the widest read the service offers and so the widest
//! accidental-disclosure surface in the feature: it must fan out over the
//! workspaces the caller owns and nothing else — not one they merely belong to,
//! and not the host's own rows, which no workspace claims.
//!
//! Runtime features configure the machine this server runs on, so no workspace
//! role entitles a caller to them and a shared deployment has no superuser to
//! entrust them to. The single-user deployment's unrestricted access is asserted
//! here too: a change that locked the shared deployment down by locking everyone
//! down would pass a suite that only proved the denials.
//!
//! Host rows are planted rather than provoked. A `TraceSummary` carries no
//! workspace field, so without a row on record an empty answer would be
//! indistinguishable from a host that had recorded none.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_api::CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND;
use coral_api::v1::feature_service_client::FeatureServiceClient;
use coral_api::v1::trace_service_client::TraceServiceClient;
use coral_api::v1::{
    GetTraceRequest, ListFeaturesRequest, ListTracesRequest, SetFeatureRequest, TraceView,
    WorkspaceRole,
};
use coral_client::local::ServerBuilder;
use serde_json::json;
use tempfile::TempDir;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

#[path = "grpc/session_auth.rs"]
#[expect(
    dead_code,
    reason = "The session-auth fixture serves several integration binaries; this one uses the parts the shared harness needs."
)]
mod session_auth;

#[path = "grpc/harness.rs"]
#[expect(
    dead_code,
    reason = "The shared harness serves several integration binaries; this one exercises the shared-deployment half of it."
)]
mod harness;

use crate::harness::{
    SharedDeployment, add_member, concealed_refusal, create_workspace, named_workspace,
};

/// The widest page the service serves, so a listing is never short of rows for
/// a reason other than the scope under test.
const WHOLE_PAGE: i32 = 200;

/// A trace id nothing recorded, so a caller who reaches the store is told the
/// trace is missing and a caller who does not never gets that far.
const ABSENT_TRACE: &str = "probe-trace";

/// A feature key the registry itself rejects, for the same reason.
const UNKNOWN_FEATURE: &str = "nope";

/// The two services this binary exercises, dialed for one credential.
///
/// Neither is carried by `AppClient`, so both are built straight from a raw
/// channel; the credential rides in the same `authorization` metadata the
/// loopback client sends, which is what the deployment's principal provider
/// reads. A `None` credential is an anonymous call, which is what a single-user
/// deployment resolves to its built-in local principal.
struct Caller {
    traces: TraceServiceClient<Channel>,
    features: FeatureServiceClient<Channel>,
    credential: Option<String>,
}

impl Caller {
    async fn connect(endpoint_uri: &str, credential: Option<String>) -> Self {
        let channel = Channel::from_shared(endpoint_uri.to_string())
            .expect("a valid endpoint")
            .connect()
            .await
            .expect("connect a raw client");
        Self {
            traces: TraceServiceClient::new(channel.clone()),
            features: FeatureServiceClient::new(channel),
            credential,
        }
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(credential) = &self.credential {
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {credential}")
                    .parse()
                    .expect("a bearer credential is valid metadata"),
            );
        }
        request
    }

    /// Lists one page of traces, either for a named workspace or for whatever
    /// an unnamed request fans out over.
    async fn list_traces(&self, workspace: Option<&str>) -> Result<Vec<String>, Status> {
        self.traces
            .clone()
            .list_traces(self.request(ListTracesRequest {
                page_size: WHOLE_PAGE,
                page_token: String::new(),
                workspace: workspace.map(named_workspace),
                view: TraceView::Unspecified as i32,
            }))
            .await
            .map(|response| {
                response
                    .into_inner()
                    .traces
                    .into_iter()
                    .map(|summary| summary.trace_id)
                    .collect()
            })
    }

    async fn get_trace(&self, workspace: Option<&str>, trace_id: &str) -> Result<(), Status> {
        self.traces
            .clone()
            .get_trace(self.request(GetTraceRequest {
                trace_id: trace_id.to_string(),
                workspace: workspace.map(named_workspace),
                view: TraceView::Unspecified as i32,
            }))
            .await
            .map(|_| ())
    }

    async fn list_features(&self) -> Result<usize, Status> {
        self.features
            .clone()
            .list_features(self.request(ListFeaturesRequest {}))
            .await
            .map(|response| response.into_inner().features.len())
    }

    async fn set_feature(&self, key: &str) -> Result<(), Status> {
        self.features
            .clone()
            .set_feature(self.request(SetFeatureRequest {
                key: key.to_string(),
                enabled: true,
            }))
            .await
            .map(|_| ())
    }
}

async fn person(deployment: &SharedDeployment, user_id: &str) -> Caller {
    Caller::connect(deployment.endpoint_uri(), Some(format!("user:{user_id}"))).await
}

/// The session an MCP-audience token authenticates: the same person's id,
/// admitted as an agent rather than as themselves.
async fn agent(deployment: &SharedDeployment, user_id: &str) -> Caller {
    Caller::connect(deployment.endpoint_uri(), Some(format!("agent:{user_id}"))).await
}

/// The code a refusal answers with. A caller who may not ask must not be
/// answered at all, so being answered is the failure.
fn refusal<T>(result: Result<T, Status>) -> Code {
    let Err(status) = result else {
        panic!("a caller who may not ask must not be answered");
    };
    status.code()
}

fn holds(listed: &[String], trace_id: &str) -> bool {
    listed.iter().any(|found| found == trace_id)
}

/// The store every deployment in this process shares.
fn planted_store(deployment: &SharedDeployment) -> &Path {
    deployment
        .trace_store_dir()
        .expect("trace history is on by default, so the store is live")
}

/// Writes spans straight into the store the deployment reads, one file per call
/// so concurrent tests never overwrite each other's rows.
///
/// A `None` workspace is a host row: work this server did that no workspace
/// claims, and the only kind of row no request could be made to produce. The
/// rows are dated ahead of the clock deliberately — every deployment in this
/// process shares one store, and a row newer than anything real is on the first
/// page of any listing no matter what the rest of the suite exported.
fn plant_traces(dir: &Path, label: &str, rows: &[(&str, Option<&str>)]) {
    // The store creates its directory on the first span it exports, which a
    // deployment that has served no work yet has not reached.
    std::fs::create_dir_all(dir).expect("the trace store directory");
    let planted_at = unix_nanos_ahead(Duration::from_mins(1));
    let mut lines = String::new();
    for (trace_id, workspace) in rows {
        let attributes = workspace.map_or_else(
            || json!({ "status": "ok" }),
            |workspace| json!({ "workspace": workspace, "sql": "SELECT 1", "status": "ok" }),
        );
        lines.push_str(
            &json!({
                "trace_id": trace_id,
                "span_id": format!("{trace_id}-span"),
                "parent_span_id": null,
                "parent_span_is_remote": false,
                "name": "coral.query",
                "kind": "internal",
                "status": "ok",
                "status_message": null,
                "start_time_unix_nanos": planted_at,
                "end_time_unix_nanos": planted_at,
                "duration_nanos": 0,
                "attributes_json": attributes.to_string(),
                "events_json": "[]",
                "links_json": "[]",
                "resource_json": "{}",
                "scope_name": "test",
                "scope_version": null,
                "scope_schema_url": null,
                "scope_attributes_json": "{}",
                "trace_flags": 0,
                "trace_state": "",
                "is_remote": false
            })
            .to_string(),
        );
        lines.push('\n');
    }
    std::fs::write(dir.join(format!("spans-{label}.jsonl")), lines).expect("plant trace rows");
}

fn unix_nanos_ahead(ahead: Duration) -> i64 {
    let when = SystemTime::now()
        .checked_add(ahead)
        .expect("a representable timestamp");
    i64::try_from(
        when.duration_since(UNIX_EPOCH)
            .expect("a clock set after the epoch")
            .as_nanos(),
    )
    .expect("nanoseconds that fit a trace timestamp")
}

/// Reports only what a refused caller is told on each trace RPC: the surface
/// beside the code, the message with the workspace name they supplied
/// themselves factored out, and the structured reasons.
async fn refusals(caller: &Caller, name: &str) -> Vec<(&'static str, Code, String, Vec<String>)> {
    let mut refused = Vec::new();
    for (rpc, result) in [
        ("ListTraces", caller.list_traces(Some(name)).await.map(drop)),
        ("GetTrace", caller.get_trace(Some(name), ABSENT_TRACE).await),
    ] {
        let status = result.expect_err("a refused caller must not be answered by these RPCs");
        let (code, message, reasons) = concealed_refusal(&status, name);
        refused.push((rpc, code, message, reasons));
    }
    refused
}

/// An owner inspects the workspaces they own and only those. The fan-out is the
/// half that carries the risk: this caller owns one workspace and merely exists
/// alongside another, and a global request must answer with the first and say
/// nothing at all about the second or about the host's own rows.
#[tokio::test]
async fn an_owner_reads_the_traces_of_the_workspaces_they_own() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("tf-own-ada", "Ada").await;
    let bob = deployment.seed_user("tf-own-bob", "Bob").await;
    create_workspace(&deployment.as_person(&ada).await, "tf-own-mine")
        .await
        .expect("the creator makes their own workspace");
    create_workspace(&deployment.as_person(&bob).await, "tf-own-theirs")
        .await
        .expect("somebody else makes theirs");
    plant_traces(
        planted_store(&deployment),
        "tf-own",
        &[
            ("tf-own-mine-trace", Some("tf-own-mine")),
            ("tf-own-theirs-trace", Some("tf-own-theirs")),
            ("tf-own-host-trace", None),
        ],
    );
    let owner = person(&deployment, &ada).await;

    let named = owner
        .list_traces(Some("tf-own-mine"))
        .await
        .expect("an owner inspects their own workspace");
    assert!(holds(&named, "tf-own-mine-trace"), "{named:?}");
    owner
        .get_trace(Some("tf-own-mine"), "tf-own-mine-trace")
        .await
        .expect("an owner reads one of their own traces");

    let fanned_out = owner
        .list_traces(None)
        .await
        .expect("an owner is answered about the workspaces they own");
    assert!(
        holds(&fanned_out, "tf-own-mine-trace")
            && !holds(&fanned_out, "tf-own-theirs-trace")
            && !holds(&fanned_out, "tf-own-host-trace"),
        "the fan-out must reach the workspace the caller owns and nothing else: {fanned_out:?}",
    );

    // The control that keeps the exclusion from being vacuous: the row the
    // fan-out did not reach is on record and does reach the person who owns it.
    let theirs = person(&deployment, &bob)
        .await
        .list_traces(None)
        .await
        .expect("the other owner is answered about their own workspace");
    assert!(holds(&theirs, "tf-own-theirs-trace"), "{theirs:?}");

    // The same boundary one trace at a time: a trace outside what the caller
    // owns is reported absent rather than refused, so which workspace it does
    // belong to stays unlearnable.
    for trace_id in ["tf-own-theirs-trace", "tf-own-host-trace"] {
        assert_eq!(
            refusal(owner.get_trace(None, trace_id).await),
            Code::NotFound,
            "{trace_id} was reachable",
        );
    }
}

/// Membership is not enough and an agent credential is never enough. The member
/// is the case that matters: they read the workspace's data all day and still
/// may not read the trace of somebody else's query in it. The owner's own agent
/// is the other: their role would make the read theirs to perform and the
/// credential is refused anyway, so a prompt-injected agent cannot exfiltrate
/// the query text of everything the workspace has run.
#[tokio::test]
async fn neither_a_member_nor_any_agent_credential_inspects_traces() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("tf-deny-ada", "Ada").await;
    let bob = deployment.seed_user("tf-deny-bob", "Bob").await;
    let owner_app = deployment.as_person(&ada).await;
    create_workspace(&owner_app, "tf-deny")
        .await
        .expect("the creator makes their own workspace");
    add_member(&owner_app, "tf-deny", &bob, WorkspaceRole::Member)
        .await
        .expect("the owner grants membership");
    plant_traces(
        planted_store(&deployment),
        "tf-deny",
        &[("tf-deny-trace", Some("tf-deny"))],
    );
    let member = person(&deployment, &bob).await;
    let member_agent = agent(&deployment, &bob).await;
    let owner_agent = agent(&deployment, &ada).await;

    for (who, caller) in [
        ("a member", &member),
        ("a member's agent", &member_agent),
        ("the owner's own agent", &owner_agent),
    ] {
        assert_eq!(
            refusal(caller.list_traces(Some("tf-deny")).await),
            Code::PermissionDenied,
            "{who} listed the workspace's traces",
        );
        assert_eq!(
            refusal(caller.get_trace(Some("tf-deny"), "tf-deny-trace").await),
            Code::PermissionDenied,
            "{who} read one of the workspace's traces",
        );
    }

    // An unnamed request is the same boundary reached from the other side: an
    // agent is refused it outright, and a member owns nothing for it to reach.
    for (who, caller) in [
        ("a member's agent", &member_agent),
        ("the owner's own agent", &owner_agent),
    ] {
        assert_eq!(
            refusal(caller.list_traces(None).await),
            Code::PermissionDenied,
            "{who} fanned out",
        );
    }
    let members_fan_out = member
        .list_traces(None)
        .await
        .expect("a caller who owns nothing is answered with nothing");
    assert!(
        !holds(&members_fan_out, "tf-deny-trace"),
        "a member's fan-out reached a workspace they only belong to: {members_fan_out:?}",
    );
    assert_eq!(
        refusal(member.get_trace(None, "tf-deny-trace").await),
        Code::NotFound,
    );
}

/// A trace RPC may not answer a question its caller may not ask. A workspace a
/// non-member holds no membership in has to read exactly like a name nobody ever
/// created — and read as the *absent* workspace specifically, since a uniform
/// "denied" would agree with itself while still confirming the name exists.
#[tokio::test]
async fn a_non_members_trace_refusals_read_exactly_like_an_absent_workspace() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("tf-conceal-ada", "Ada").await;
    let bob = deployment.seed_user("tf-conceal-bob", "Bob").await;
    create_workspace(&deployment.as_person(&ada).await, "tf-conceal")
        .await
        .expect("the creator makes their own workspace");
    plant_traces(
        planted_store(&deployment),
        "tf-conceal",
        &[("tf-conceal-trace", Some("tf-conceal"))],
    );
    let outsider = person(&deployment, &bob).await;

    let existing = refusals(&outsider, "tf-conceal").await;
    assert_eq!(
        existing,
        refusals(&outsider, "tf-ghost").await,
        "an existing workspace must be indistinguishable from one that never existed",
    );
    assert!(
        existing
            .iter()
            .all(|(_, code, _, reasons)| *code == Code::NotFound
                && reasons.as_slice() == [CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND]),
        "both must read as the absent workspace, not as a denial that confirms one: {existing:?}",
    );
    let fanned_out = outsider
        .list_traces(None)
        .await
        .expect("a caller who owns nothing is answered with nothing");
    assert!(
        !holds(&fanned_out, "tf-conceal-trace"),
        "the concealed workspace's traces were reachable behind the concealment: {fanned_out:?}",
    );
}

/// Runtime features configure the machine this server runs on, so a shared
/// deployment has nobody to entrust them to: neither a person nor their agent
/// reaches either RPC, whatever workspaces they own.
///
/// The probe key is what makes each refusal an absence rather than an error
/// code: `nope` is a key the registry itself would reject, so a caller who
/// reached the registry would be told so.
#[tokio::test]
async fn no_shared_credential_configures_this_hosts_runtime_features() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("tf-features-ada", "Ada").await;
    create_workspace(&deployment.as_person(&ada).await, "tf-features")
        .await
        .expect("the creator makes their own workspace");
    let owner = person(&deployment, &ada).await;
    let owners_agent = agent(&deployment, &ada).await;

    for (who, caller) in [("an owner", &owner), ("their agent", &owners_agent)] {
        assert_eq!(
            refusal(caller.list_features().await),
            Code::PermissionDenied,
            "{who} listed this host's features",
        );
        assert_eq!(
            refusal(caller.set_feature(UNKNOWN_FEATURE).await),
            Code::PermissionDenied,
            "{who} configured this host's features",
        );
    }
}

/// The single-user deployment keeps the unrestricted access it has always had.
/// Its caller reads the host's own rows and a workspace it holds no membership
/// in, and reaches the feature registry — there is nobody there to conceal any
/// of it from, and locking the shared deployment down must not have locked this
/// one down with it.
#[tokio::test]
async fn the_implicit_owner_still_reads_host_rows_and_configures_features() {
    let temp = TempDir::new().expect("temp dir");
    // A builder with no principal provider is a single-user deployment: every
    // request arrives as the built-in local principal.
    let server = ServerBuilder::new()
        .with_config_dir(temp.path().join("coral-config"))
        .start()
        .await
        .expect("start a single-user server");
    let store = server
        .local_trace_store_dir()
        .expect("trace history is on by default, so the store is live")
        .to_path_buf();
    // The process installs one trace store, owned by whichever server started
    // first. When that is this one, its temp dir must outlive the deployment:
    // removing it would delete the store out from under a concurrent test.
    let _temp = if store.starts_with(temp.path()) {
        let _installed_store_root: PathBuf = temp.keep();
        None
    } else {
        Some(temp)
    };
    plant_traces(
        &store,
        "tf-local",
        &[
            ("tf-local-host-trace", None),
            ("tf-local-workspace-trace", Some("tf-local-unknown")),
        ],
    );
    let local = Caller::connect(server.endpoint_uri(), None).await;

    let listed = local
        .list_traces(None)
        .await
        .expect("a single-user deployment reads every trace this host recorded");
    for trace_id in ["tf-local-host-trace", "tf-local-workspace-trace"] {
        assert!(
            holds(&listed, trace_id),
            "{trace_id} was hidden: {listed:?}"
        );
        local
            .get_trace(None, trace_id)
            .await
            .unwrap_or_else(|status| panic!("the implicit owner reads {trace_id}: {status}"));
    }

    assert!(
        local
            .list_features()
            .await
            .expect("the implicit owner inspects this host's features")
            > 0,
    );
    assert_eq!(
        refusal(local.set_feature(UNKNOWN_FEATURE).await),
        Code::InvalidArgument,
        "the implicit owner must be stopped by the request, never by the gate",
    );
}
