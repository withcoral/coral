//! Public gRPC evidence that a source's configuration reaches only the people
//! who manage the workspace holding it.
//!
//! A source response is not a list of names: it carries the variables, the
//! secret keys, and the credential-setup metadata that configure a connection.
//! Reading one is managing the workspace rather than reading its contents, so
//! all nine source RPCs are an owner's act. A member is refused outright — the
//! redacted projection that would let them see a source without its setup
//! deliberately does not exist — and a non-member is told only what they would
//! be told about a workspace nobody ever created.
//!
//! Every refusal is proved to be an absence rather than an error code. Each
//! request sent here is one the source work itself has an answer for, so the
//! owner sending it is told about the request while the refused callers are
//! told nothing about it; and what the workspace holds afterwards — its
//! sources, its tasks, its recorded queries, its attributed spans — is read
//! back to show the refusals moved none of it.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use coral_api::CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND;
use coral_api::v1::{
    CreateBundledSourceRequest, CreateBundledSourceWithOAuthRequest, DeleteSourceRequest,
    DiscoverSourcesRequest, ExecuteSqlRequest, GetSourceInfoRequest, GetSourceRequest,
    ImportSourceRequest, ListSourcesRequest, OAuthCredentialRetrieval, StartTaskRequest,
    TaskAttribution, ValidateSourceRequest, WorkspaceRole, import_source_response,
};
use coral_client::AppClient;
use tempfile::TempDir;
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
    SharedDeployment, add_member, concealed_refusal, create_workspace, execute_sql,
    fixture_manifest_with_required_filter_yaml, fixture_manifest_yaml, invalid_manifest_yaml,
    named_workspace,
};

/// A source name that is neither installed nor bundled, so a caller who
/// reaches the source work is stopped by the work rather than by the gate.
const ABSENT_SOURCE: &str = "probe";

/// The name the fixture manifest installs under.
const FIXTURE_SOURCE: &str = "local_messages";

/// The name the second fixture manifest would install under, so its absence
/// afterwards is an observation about the import that was refused.
const REFUSED_IMPORT_SOURCE: &str = "filtered_messages";

async fn discover_sources(client: &AppClient, name: &str) -> Result<(), Status> {
    client
        .source_client()
        .discover_sources(Request::new(DiscoverSourcesRequest {
            workspace: Some(named_workspace(name)),
        }))
        .await
        .map(|_| ())
}

async fn list_sources(client: &AppClient, name: &str) -> Result<Vec<String>, Status> {
    client
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(named_workspace(name)),
        }))
        .await
        .map(|response| {
            response
                .into_inner()
                .sources
                .into_iter()
                .map(|source| source.name)
                .collect()
        })
}

async fn get_source(client: &AppClient, name: &str, source: &str) -> Result<(), Status> {
    client
        .source_client()
        .get_source(Request::new(GetSourceRequest {
            workspace: Some(named_workspace(name)),
            name: source.to_string(),
        }))
        .await
        .map(|_| ())
}

async fn get_source_info(client: &AppClient, name: &str, source: &str) -> Result<(), Status> {
    client
        .source_client()
        .get_source_info(Request::new(GetSourceInfoRequest {
            workspace: Some(named_workspace(name)),
            name: source.to_string(),
        }))
        .await
        .map(|_| ())
}

async fn create_bundled_source(client: &AppClient, name: &str, source: &str) -> Result<(), Status> {
    client
        .source_client()
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(named_workspace(name)),
            name: source.to_string(),
            variables: Vec::new(),
            secrets: Vec::new(),
        }))
        .await
        .map(|_| ())
}

/// Asks for a bundled install whose OAuth half is missing the method index the
/// credential conversion requires. Reaching that conversion is credential work,
/// so a refusal that arrives instead proves it was never reached.
async fn create_bundled_source_with_oauth(
    client: &AppClient,
    name: &str,
    source: &str,
) -> Result<(), Status> {
    client
        .source_client()
        .create_bundled_source_with_o_auth(Request::new(CreateBundledSourceWithOAuthRequest {
            workspace: Some(named_workspace(name)),
            name: source.to_string(),
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: vec![OAuthCredentialRetrieval {
                input_key: "API_TOKEN".to_string(),
                method_index: None,
                credential_inputs: Vec::new(),
            }],
        }))
        .await
        .map(|_| ())
}

/// Imports `manifest_yaml` and returns the name it installed under.
///
/// The call itself is what a refused caller is answered with: a status can only
/// come back where a stream did not, so a refusal here is also the proof that
/// no import stream was ever handed over.
async fn import_source(
    client: &AppClient,
    name: &str,
    manifest_yaml: String,
) -> Result<String, Status> {
    let mut stream = client
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(named_workspace(name)),
            manifest_yaml,
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
        }))
        .await?
        .into_inner();
    stream
        .message()
        .await?
        .and_then(|response| match response.event {
            Some(import_source_response::Event::Source(source)) => Some(source.name),
            _ => None,
        })
        .ok_or_else(|| Status::internal("the import stream carried no installed source"))
}

async fn delete_source(client: &AppClient, name: &str, source: &str) -> Result<(), Status> {
    client
        .source_client()
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(named_workspace(name)),
            name: source.to_string(),
        }))
        .await
        .map(|_| ())
}

async fn validate_source(client: &AppClient, name: &str, source: &str) -> Result<(), Status> {
    client
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(named_workspace(name)),
            name: source.to_string(),
        }))
        .await
        .map(|_| ())
}

async fn start_task(client: &AppClient, name: &str) -> Result<String, Status> {
    client
        .task_client()
        .start_task(Request::new(StartTaskRequest {
            workspace: Some(named_workspace(name)),
            intent: "prove source isolation".to_string(),
        }))
        .await
        .map(|response| response.into_inner().task.expect("started task").task_id)
}

/// Runs one statement under `task_id`, which is what makes the server record
/// the query against that task rather than only run it.
async fn execute_sql_in_task(
    client: &AppClient,
    name: &str,
    task_id: &str,
    sql: &str,
) -> Result<(), Status> {
    client
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(named_workspace(name)),
            sql: sql.to_string(),
            guide_read_context: None,
            task_attribution: Some(TaskAttribution {
                task_id: task_id.to_string(),
                intent: "prove source isolation".to_string(),
            }),
        }))
        .await
        .map(|_| ())
}

/// Calls all nine source RPCs against `name` and reports what each answered.
///
/// Every request here is one the source work itself has an answer for — a
/// source name that is neither installed nor bundled, a manifest that does not
/// parse, an OAuth retrieval with no method index — so the caller who is let
/// through is told what is wrong with the request, and the caller who is not
/// never gets that far.
async fn every_source_rpc(
    client: &AppClient,
    name: &str,
) -> Vec<(&'static str, Result<(), Status>)> {
    vec![
        ("DiscoverSources", discover_sources(client, name).await),
        ("ListSources", list_sources(client, name).await.map(|_| ())),
        ("GetSource", get_source(client, name, ABSENT_SOURCE).await),
        (
            "GetSourceInfo",
            get_source_info(client, name, ABSENT_SOURCE).await,
        ),
        (
            "CreateBundledSource",
            create_bundled_source(client, name, ABSENT_SOURCE).await,
        ),
        (
            "CreateBundledSourceWithOAuth",
            create_bundled_source_with_oauth(client, name, ABSENT_SOURCE).await,
        ),
        (
            "ImportSource",
            import_source(client, name, invalid_manifest_yaml())
                .await
                .map(|_| ()),
        ),
        (
            "DeleteSource",
            delete_source(client, name, ABSENT_SOURCE).await,
        ),
        (
            "ValidateSource",
            validate_source(client, name, ABSENT_SOURCE).await,
        ),
    ]
}

/// Reports only what a refused caller is told on each source RPC: the surface
/// beside the code, the message with the workspace name they supplied
/// themselves factored out, and the structured reasons.
async fn source_refusals(
    client: &AppClient,
    name: &str,
) -> Vec<(&'static str, Code, String, Vec<String>)> {
    let mut refusals = Vec::new();
    for (rpc, result) in every_source_rpc(client, name).await {
        let status = result.expect_err("a refused caller must not be answered by a source RPC");
        let (code, message, reasons) = concealed_refusal(&status, name);
        refusals.push((rpc, code, message, reasons));
    }
    refusals
}

/// The owner of a workspace reaches every source RPC in it: the seven that have
/// something to say about an installed source say it, and the two bundled
/// installs are stopped by the catalog they looked in rather than by the gate.
#[tokio::test]
async fn an_owner_reaches_every_source_rpc_in_their_own_workspace() {
    let deployment = SharedDeployment::start().await;
    let fixtures = TempDir::new().expect("fixture data dir");
    let ada = deployment.seed_user("source-own-ada", "Ada").await;
    let owner = deployment.as_person(&ada).await;
    create_workspace(&owner, "source-own")
        .await
        .expect("the creator makes their own workspace");

    assert_eq!(
        import_source(&owner, "source-own", fixture_manifest_yaml(fixtures.path()))
            .await
            .expect("the owner imports a source"),
        FIXTURE_SOURCE,
    );
    discover_sources(&owner, "source-own")
        .await
        .expect("the owner discovers bundled sources");
    assert_eq!(
        list_sources(&owner, "source-own")
            .await
            .expect("the owner lists their sources"),
        vec![FIXTURE_SOURCE.to_string()],
    );
    get_source(&owner, "source-own", FIXTURE_SOURCE)
        .await
        .expect("the owner reads the source's configuration");
    get_source_info(&owner, "source-own", FIXTURE_SOURCE)
        .await
        .expect("the owner reads the source's setup metadata");
    validate_source(&owner, "source-own", FIXTURE_SOURCE)
        .await
        .expect("the owner revalidates the source");

    for (rpc, result) in every_source_rpc(&owner, "source-own").await {
        if let Err(status) = result {
            let (code, message, reasons) = concealed_refusal(&status, "source-own");
            assert!(
                code != Code::PermissionDenied
                    && reasons.as_slice() != [CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND],
                "the owner must be stopped by the request, never by the gate: {rpc} {code} {message}",
            );
        }
    }

    delete_source(&owner, "source-own", FIXTURE_SOURCE)
        .await
        .expect("the owner deletes the source");
    assert_eq!(
        list_sources(&owner, "source-own")
            .await
            .expect("the owner lists their sources"),
        Vec::<String>::new(),
    );
}

/// Membership opens the workspace's contents and nothing about how they are
/// configured. The member's own read is the control: they are plainly inside
/// the workspace, and it is the source family specifically that stays shut.
#[tokio::test]
async fn a_member_is_refused_every_source_rpc() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("source-member-ada", "Ada").await;
    let bob = deployment.seed_user("source-member-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let member = deployment.as_person(&bob).await;
    create_workspace(&owner, "source-member")
        .await
        .expect("the creator makes their own workspace");
    add_member(&owner, "source-member", &bob, WorkspaceRole::Member)
        .await
        .expect("the owner grants membership");
    execute_sql(&member, "source-member", "select 1")
        .await
        .expect("a member reads the workspace's contents");

    for (rpc, code, message, _) in source_refusals(&member, "source-member").await {
        assert_eq!(
            code,
            Code::PermissionDenied,
            "a member reads and changes no source configuration: {rpc} {message}",
        );
    }
    // The agent session behind the same person is bounded by that person, so it
    // gains nothing the member did not have.
    for (rpc, code, message, _) in
        source_refusals(&deployment.as_agent(&bob).await, "source-member").await
    {
        assert_eq!(
            code,
            Code::PermissionDenied,
            "an agent session inherits the member's refusal: {rpc} {message}",
        );
    }
}

/// The source surfaces must not answer questions their caller may not ask. A
/// workspace a non-member holds no membership in has to read exactly like a
/// name nobody ever created — and read as the *absent* workspace specifically,
/// since a uniform "denied" would agree with itself while still confirming the
/// name exists.
#[tokio::test]
async fn a_non_members_source_refusals_read_exactly_like_an_absent_workspace() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("source-conceal-ada", "Ada").await;
    let bob = deployment.seed_user("source-conceal-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let outsider = deployment.as_person(&bob).await;
    create_workspace(&owner, "source-conceal")
        .await
        .expect("the creator makes their own workspace");

    let existing = source_refusals(&outsider, "source-conceal").await;
    assert_eq!(
        existing,
        source_refusals(&outsider, "source-ghost").await,
        "an existing workspace must be indistinguishable from one that never existed",
    );
    assert!(
        existing
            .iter()
            .all(|(_, code, _, reasons)| *code == Code::NotFound
                && reasons.as_slice() == [CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND]),
        "both must read as the absent workspace, not as a denial that confirms one: {existing:?}",
    );
}

/// A refused source request must leave the workspace exactly as it found it:
/// the installed source still installed and nothing installed beside it, and no
/// task, recorded query, or attributed span to show it was ever asked for.
///
/// The attributed probe is the one that matters most. A gate placed after the
/// work would only betray itself when the denied statement names a task that
/// genuinely exists in the target workspace, so one refusal here carries the
/// real task id the owner minted first. The owner's own statement under that
/// same task afterwards is the control: it moves every counter the refusals
/// left where they were, so the sameness is an observation about the refusals
/// rather than about an observer that sees nothing.
#[tokio::test]
async fn refused_source_requests_leave_the_workspace_untouched() {
    let deployment = SharedDeployment::start().await;
    let fixtures = TempDir::new().expect("fixture data dir");
    let ada = deployment.seed_user("source-record-ada", "Ada").await;
    let bob = deployment.seed_user("source-record-bob", "Bob").await;
    let carol = deployment.seed_user("source-record-carol", "Carol").await;
    let owner = deployment.as_person(&ada).await;
    let member = deployment.as_person(&bob).await;
    let outsider = deployment.as_person(&carol).await;
    create_workspace(&owner, "source-record")
        .await
        .expect("the creator makes their own workspace");
    add_member(&owner, "source-record", &bob, WorkspaceRole::Member)
        .await
        .expect("the owner grants membership");
    import_source(
        &owner,
        "source-record",
        fixture_manifest_yaml(fixtures.path()),
    )
    .await
    .expect("the owner imports a source");
    let task_id = start_task(&owner, "source-record")
        .await
        .expect("the owner opens a task");
    let before = deployment.workspace_work("source-record").await;
    assert!(
        before.tasks > 0,
        "the attributed refusal below only probes anything if that task really is on this workspace's record: {before:?}",
    );

    for (rpc, result) in [
        (
            "DeleteSource",
            delete_source(&member, "source-record", FIXTURE_SOURCE).await,
        ),
        (
            "ImportSource",
            import_source(
                &member,
                "source-record",
                fixture_manifest_with_required_filter_yaml(),
            )
            .await
            .map(|_| ()),
        ),
        (
            "CreateBundledSourceWithOAuth",
            create_bundled_source_with_oauth(&member, "source-record", ABSENT_SOURCE).await,
        ),
    ] {
        assert_eq!(
            result
                .expect_err("a member must not change source configuration")
                .code(),
            Code::PermissionDenied,
            "the member reached {rpc}",
        );
    }
    assert_eq!(
        execute_sql_in_task(&outsider, "source-record", &task_id, "select 1")
            .await
            .expect_err("a non-member must not run a statement under this workspace's task")
            .code(),
        Code::NotFound,
    );

    assert_eq!(
        deployment.workspace_work("source-record").await,
        before,
        "refused work must add no task row, no recorded query, and no attributed span — not even when the statement names a task that really is in this workspace",
    );
    assert_eq!(
        list_sources(&owner, "source-record")
            .await
            .expect("the owner lists their sources"),
        vec![FIXTURE_SOURCE.to_string()],
        "the refused delete removed nothing and the refused import installed nothing",
    );
    assert!(
        get_source(&outsider, "source-record", REFUSED_IMPORT_SOURCE)
            .await
            .is_err(),
        "the manifest a refused import carried must not have been installed",
    );

    execute_sql_in_task(&owner, "source-record", &task_id, "select 1")
        .await
        .expect("the owner runs a statement under their own task");
    let recorded = deployment.workspace_work("source-record").await;
    assert!(
        recorded.queries > before.queries && recorded.attributed_spans > before.attributed_spans,
        "permitted work must move the counters the refusals left where they were: {recorded:?} after {before:?}",
    );
}
