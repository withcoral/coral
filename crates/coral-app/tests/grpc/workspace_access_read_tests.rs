use coral_api::CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND;
use coral_api::v1::{
    EndTaskRequest, ExecuteSqlRequest, ExplainSqlRequest, ListCatalogRequest, PaginationRequest,
    StartTaskRequest, SubmitFeedbackRequest, TaskAttribution, TaskStatus, WorkspaceRole,
};
use coral_client::AppClient;
use tonic::{Code, Request, Status};

use crate::harness::{
    SharedDeployment, WorkspaceWork, add_member, concealed_refusal, create_workspace, execute_sql,
    membership_rows, named_workspace, remove_member,
};

/// A task id that was never minted anywhere, so `EndTask` cannot be answered
/// from a real row: the refusal it draws has to come from the workspace rule.
const UNMINTED_TASK_ID: &str = "00000000-0000-4000-8000-000000000000";

/// SQL the query path itself rejects. A caller who reached the planner would
/// be told so, which is what makes a concealing refusal on `ExplainSql` an
/// absence rather than an error code.
const UNPLANNABLE_SQL: &str = "this is not sql";

async fn list_catalog(client: &AppClient, name: &str) -> Result<usize, Status> {
    client
        .catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(named_workspace(name)),
            catalog_name: String::new(),
            schema_name: String::new(),
            kind: 0,
            pagination: Some(PaginationRequest {
                limit: 0,
                offset: 0,
            }),
        }))
        .await
        .map(|response| response.into_inner().items.len())
}

async fn explain_sql(client: &AppClient, name: &str, sql: &str) -> Result<(), Status> {
    client
        .query_client()
        .explain_sql(Request::new(ExplainSqlRequest {
            workspace: Some(named_workspace(name)),
            sql: sql.to_string(),
        }))
        .await
        .map(|_| ())
}

async fn start_task(client: &AppClient, name: &str) -> Result<String, Status> {
    client
        .task_client()
        .start_task(Request::new(StartTaskRequest {
            workspace: Some(named_workspace(name)),
            intent: "prove read isolation".to_string(),
        }))
        .await
        .map(|response| response.into_inner().task.expect("started task").task_id)
}

async fn end_task(client: &AppClient, name: &str, task_id: &str) -> Result<(), Status> {
    client
        .task_client()
        .end_task(Request::new(EndTaskRequest {
            workspace: Some(named_workspace(name)),
            task_id: task_id.to_string(),
            task_status: TaskStatus::Success.into(),
        }))
        .await
        .map(|_| ())
}

async fn submit_feedback(client: &AppClient, name: &str) -> Result<(), Status> {
    client
        .feedback_client()
        .submit_feedback(Request::new(SubmitFeedbackRequest {
            workspace: Some(named_workspace(name)),
            trying_to_do: "read the workspace".to_string(),
            tried: "one statement".to_string(),
            stuck: "nowhere".to_string(),
        }))
        .await
        .map(|_| ())
}

/// Runs one statement under `task_id`, which is what makes the server record
/// the query against the task rather than only run it.
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
                intent: "prove read isolation".to_string(),
            }),
        }))
        .await
        .map(|_| ())
}

/// Probes one workspace name across every classified read RPC and reports the
/// surface beside what that surface told the caller. Two names that agree here
/// are indistinguishable to that caller.
async fn read_refusals(
    client: &AppClient,
    name: &str,
) -> Vec<(&'static str, Code, String, Vec<String>)> {
    let probes = [
        (
            "execute_sql",
            execute_sql(client, name, "select 1")
                .await
                .expect_err("a non-member must not query the workspace"),
        ),
        (
            "explain_sql",
            explain_sql(client, name, UNPLANNABLE_SQL)
                .await
                .expect_err("a non-member must not plan against the workspace"),
        ),
        (
            "list_catalog",
            list_catalog(client, name)
                .await
                .expect_err("a non-member must not browse the catalog"),
        ),
        (
            "start_task",
            start_task(client, name)
                .await
                .expect_err("a non-member must not open a task"),
        ),
        (
            "end_task",
            end_task(client, name, UNMINTED_TASK_ID)
                .await
                .expect_err("a non-member must not end a task"),
        ),
        (
            "submit_feedback",
            submit_feedback(client, name)
                .await
                .expect_err("a non-member must not file feedback"),
        ),
    ];
    probes
        .iter()
        .map(|(surface, status)| {
            let (code, message, reasons) = concealed_refusal(status, name);
            (*surface, code, message, reasons)
        })
        .collect()
}

/// Runs every classified read RPC as a caller who is expected to be allowed,
/// so a claim about the boundary rests on both of its sides.
async fn expect_full_read_access(client: &AppClient, name: &str) {
    execute_sql(client, name, "select 1")
        .await
        .expect("a member queries the workspace");
    explain_sql(client, name, "select 1")
        .await
        .expect("a member plans a statement");
    list_catalog(client, name)
        .await
        .expect("a member browses the catalog");
    let task_id = start_task(client, name)
        .await
        .expect("a member opens a task");
    end_task(client, name, &task_id)
        .await
        .expect("a member ends the task they opened");
    submit_feedback(client, name)
        .await
        .expect("a member files feedback");
}

/// The harness has to hold before any isolation claim can rest on it: two
/// people who are distinct to the server, a workspace only one of them created,
/// a refusal that leaves nothing on that workspace's record, and membership as
/// the only thing that changes either answer.
#[tokio::test]
async fn workspace_access_read_harness_seats_two_people_around_one_workspace() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("read-ada", "Ada").await;
    let bob = deployment.seed_user("read-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let agent = deployment.as_agent("agent-read-harness").await;
    let outsider = deployment.as_person(&bob).await;
    let created = create_workspace(&owner, "read-harness")
        .await
        .expect("the creator makes their own workspace")
        .workspace
        .expect("create workspace response");
    assert_eq!(created.name, "read-harness");

    assert_eq!(
        execute_sql(&outsider, "read-harness", "select 1")
            .await
            .expect_err("a non-member must not read the workspace")
            .code(),
        Code::NotFound,
    );
    assert_eq!(
        deployment.workspace_work("read-harness").await,
        WorkspaceWork::default(),
        "a refused read must leave behind no task, no recorded query, and no attributed span",
    );

    // An agent is its own principal, so it holds no membership here and the
    // workspace is concealed from it exactly as it is from any other outsider.
    assert_eq!(
        execute_sql(&agent, "read-harness", "select 1")
            .await
            .expect_err("an agent holds no membership of its own")
            .code(),
        Code::NotFound,
    );
    add_member(&owner, "read-harness", &bob, WorkspaceRole::Member)
        .await
        .expect("the owner grants membership");
    execute_sql(&outsider, "read-harness", "select 1")
        .await
        .expect("membership is what opens the read");

    remove_member(&owner, "read-harness", &bob)
        .await
        .expect("the owner revokes membership");
    assert_eq!(
        execute_sql(&outsider, "read-harness", "select 1")
            .await
            .expect_err("revocation must apply to the next request")
            .code(),
        Code::NotFound,
    );
    // The permitted reads are what prove the observer above can see anything at
    // all: without this, an emptiness assertion would pass on a blind observer.
    assert!(
        deployment
            .workspace_work("read-harness")
            .await
            .attributed_spans
            > 0,
        "permitted reads must leave the workspace attribution the refused one did not",
    );
}

/// Two people who each created a workspace share a deployment but not a view of
/// it: the listing shows each of them one membership, and every classified read
/// RPC stops at the workspace they do not belong to.
#[tokio::test]
async fn two_creators_reach_only_the_workspace_each_of_them_made() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("read-alpha-ada", "Ada").await;
    let bob = deployment.seed_user("read-beta-bob", "Bob").await;
    let ada_client = deployment.as_person(&ada).await;
    let bob_client = deployment.as_person(&bob).await;
    create_workspace(&ada_client, "read-alpha")
        .await
        .expect("Ada makes her own workspace");
    create_workspace(&bob_client, "read-beta")
        .await
        .expect("Bob makes his own workspace");

    assert_eq!(
        membership_rows(&ada_client).await,
        vec![("read-alpha".to_string(), WorkspaceRole::Owner)],
        "creating one workspace must not reveal anybody else's",
    );
    assert_eq!(
        membership_rows(&bob_client).await,
        vec![("read-beta".to_string(), WorkspaceRole::Owner)],
    );

    for (surface, code, _, _) in read_refusals(&ada_client, "read-beta").await {
        assert_eq!(code, Code::NotFound, "Ada reached Bob's {surface}");
    }
    for (surface, code, _, _) in read_refusals(&bob_client, "read-alpha").await {
        assert_eq!(code, Code::NotFound, "Bob reached Ada's {surface}");
    }
    expect_full_read_access(&ada_client, "read-alpha").await;
    expect_full_read_access(&bob_client, "read-beta").await;
}

/// The read surfaces must not answer questions their caller may not ask. A
/// workspace they hold no membership in has to read exactly like a name nobody
/// ever created — and read as the *absent* workspace specifically, since a
/// uniform "denied" would agree with itself while still confirming the name.
#[tokio::test]
async fn an_inaccessible_workspace_reads_exactly_like_an_absent_one() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("read-conceal-ada", "Ada").await;
    let bob = deployment.seed_user("read-conceal-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let outsider = deployment.as_person(&bob).await;
    create_workspace(&owner, "read-conceal")
        .await
        .expect("the creator makes their own workspace");

    let existing = read_refusals(&outsider, "read-conceal").await;
    assert_eq!(
        existing,
        read_refusals(&outsider, "read-ghost").await,
        "an existing workspace must be indistinguishable from one that never existed",
    );
    assert!(
        existing
            .iter()
            .all(|(_, code, _, reasons)| *code == Code::NotFound
                && reasons.as_slice() == [CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND]),
        "both must read as the absent workspace, not as a denial that confirms one: {existing:?}",
    );
    // A probe that changed the workspace would make the two names agree only
    // until the owner looked.
    assert_eq!(
        deployment.workspace_work("read-conceal").await,
        WorkspaceWork::default(),
    );

    // `end_task` carries its share of the vector only if its refusal came from
    // the workspace rule rather than from the task id being unknown. The owner
    // asking the same impossible question is what separates the two: they are
    // told about the task, and told it without a Coral reason attached.
    let (code, message, reasons) = concealed_refusal(
        &end_task(&owner, "read-conceal", UNMINTED_TASK_ID)
            .await
            .expect_err("the task id was never minted"),
        "read-conceal",
    );
    assert_eq!(code, Code::NotFound);
    assert!(
        reasons.is_empty() && message.contains(UNMINTED_TASK_ID),
        "a member's unknown task must not read as the absent workspace: {message} {reasons:?}",
    );
}

/// A refused read must leave the workspace with no record that it was asked
/// for. The owner's own reads afterwards are the control: they move every
/// counter the refusals left at zero, so the emptiness is an observation about
/// the refusals rather than about an observer that sees nothing.
#[tokio::test]
async fn refused_reads_leave_the_workspace_no_record_of_them() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("read-record-ada", "Ada").await;
    let bob = deployment.seed_user("read-record-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let outsider = deployment.as_person(&bob).await;
    create_workspace(&owner, "read-record")
        .await
        .expect("the creator makes their own workspace");

    let refused = read_refusals(&outsider, "read-record").await;
    assert!(
        refused
            .iter()
            .all(|(_, code, _, _)| *code == Code::NotFound),
        "every read surface must refuse before it does anything: {refused:?}",
    );
    assert_eq!(
        deployment.workspace_work("read-record").await,
        WorkspaceWork::default(),
        "refused work must create no task row, no recorded query, and no attributed span",
    );

    let task_id = start_task(&owner, "read-record")
        .await
        .expect("the owner opens a task");
    execute_sql_in_task(&owner, "read-record", &task_id, "select 1")
        .await
        .expect("the owner runs a statement under it");
    let recorded = deployment.workspace_work("read-record").await;
    assert!(
        recorded.tasks > 0 && recorded.queries > 0 && recorded.attributed_spans > 0,
        "permitted work must move every counter the refusals left at zero: {recorded:?}",
    );
}

/// Membership is the whole of the read boundary: adding a person opens every
/// classified read RPC to them, and revoking it closes the very next request on
/// each of those same RPCs.
#[tokio::test]
async fn membership_opens_every_read_surface_and_revocation_closes_it() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("read-grant-ada", "Ada").await;
    let bob = deployment.seed_user("read-grant-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let member = deployment.as_person(&bob).await;
    create_workspace(&owner, "read-grant")
        .await
        .expect("the creator makes their own workspace");
    for (surface, code, _, _) in read_refusals(&member, "read-grant").await {
        assert_eq!(code, Code::NotFound, "a non-member reached {surface}");
    }

    add_member(&owner, "read-grant", &bob, WorkspaceRole::Member)
        .await
        .expect("the owner grants membership");
    expect_full_read_access(&member, "read-grant").await;
    // Granting the person nothing for the agent: the two are separate
    // principals, so a membership written for one is not readable by the other.
    for (surface, code, _, _) in
        read_refusals(&deployment.as_agent("agent-read-grant").await, "read-grant").await
    {
        assert_eq!(code, Code::NotFound, "an agent reached {surface}");
    }

    remove_member(&owner, "read-grant", &bob)
        .await
        .expect("the owner revokes membership");
    let after_revocation = read_refusals(&member, "read-grant").await;
    assert!(
        after_revocation
            .iter()
            .all(|(_, code, _, reasons)| *code == Code::NotFound
                && reasons.as_slice() == [CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND]),
        "revocation must close every read surface on the next request: {after_revocation:?}",
    );
}
