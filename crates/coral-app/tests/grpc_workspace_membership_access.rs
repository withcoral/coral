//! Public gRPC evidence for the control plane of a workspace: who may reach
//! it, who may change that, and who may delete it.
//!
//! Membership is the access-control state itself, so reading the roster is as
//! much an owner's act as changing it — a member works inside a workspace
//! without learning who else holds a key to it. Deletion belongs to the same
//! family for the same reason: it is the workspace, not its contents.
//!
//! The membership contract is the one place where a refusal is not the
//! interesting half. Promotion, demotion, a second owner, a revocation, a
//! person the directory has never seen, and a grant somebody already holds each
//! have their own answer, and this binary pins all of them, because a contract
//! that only says "denied" correctly would still be free to garble everything
//! it allows.
//!
//! Two guarantees get their own tests. The owner floor — that no sequence of
//! permitted changes strands a workspace with nobody able to reach it — is
//! proved at the transaction layer against a barrier; here it is proved again
//! over the transport, where two owners changing themselves at the same time is
//! the only way to ask for it. And an agent credential is refused this whole
//! family even for a workspace the person behind it owns, so an MCP session
//! cannot manage what the person's own browser session manages.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use coral_api::CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND;
use coral_api::v1::{
    DeleteWorkspaceRequest, ListWorkspaceMembersRequest, WorkspaceMember, WorkspaceRole,
};
use coral_client::AppClient;
use tonic::{Code, Request, Status};

#[path = "grpc/harness.rs"]
#[expect(
    dead_code,
    reason = "The shared harness serves several integration binaries; this one exercises the shared-deployment half of it."
)]
mod harness;

use crate::harness::{
    SharedDeployment, add_member, concealed_refusal, create_workspace, execute_sql,
    membership_rows, named_workspace, remove_member,
};

/// A user id no seeded login carries, so a caller who reaches the directory is
/// told the person is unknown and a caller who does not never gets that far.
const ABSENT_USER: &str = "user-nobody";

async fn list_members(
    client: &AppClient,
    name: &str,
) -> Result<Vec<(String, WorkspaceRole)>, Status> {
    client
        .workspace_client()
        .list_workspace_members(Request::new(ListWorkspaceMembersRequest {
            workspace: Some(named_workspace(name)),
        }))
        .await
        .map(|response| {
            response
                .into_inner()
                .members
                .into_iter()
                .map(|member| (member.user_id, member.role.try_into().expect("listed role")))
                .collect()
        })
}

async fn delete_workspace(client: &AppClient, name: &str) -> Result<(), Status> {
    client
        .workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(named_workspace(name)),
        }))
        .await
        .map(|_| ())
}

/// Calls every control-plane RPC one workspace has and reports what each
/// answered.
///
/// The two membership changes name a person the directory has never seen, so a
/// caller who is let through is stopped by their own request rather than by the
/// gate — which is what makes a refusal here evidence about the gate.
async fn every_control_plane_rpc(
    client: &AppClient,
    name: &str,
) -> Vec<(&'static str, Result<(), Status>)> {
    vec![
        (
            "ListWorkspaceMembers",
            list_members(client, name).await.map(|_| ()),
        ),
        (
            "AddWorkspaceMember",
            add_member(client, name, ABSENT_USER, WorkspaceRole::Member)
                .await
                .map(|_| ()),
        ),
        (
            "RemoveWorkspaceMember",
            remove_member(client, name, ABSENT_USER).await.map(|_| ()),
        ),
        ("DeleteWorkspace", delete_workspace(client, name).await),
    ]
}

/// Reports only what a refused caller is told on each control-plane RPC: the
/// surface beside the code, the message with the workspace name they supplied
/// themselves factored out, and the structured reasons.
async fn control_plane_refusals(
    client: &AppClient,
    name: &str,
) -> Vec<(&'static str, Code, String, Vec<String>)> {
    let mut refusals = Vec::new();
    for (rpc, result) in every_control_plane_rpc(client, name).await {
        let status =
            result.expect_err("a refused caller must not be answered by a control-plane RPC");
        let (code, message, reasons) = concealed_refusal(&status, name);
        refusals.push((rpc, code, message, reasons));
    }
    refusals
}

/// Membership opens a workspace's contents and nothing about who may reach it.
/// The member's own query is the control: they are plainly inside the
/// workspace, and it is the control plane specifically that stays shut. The
/// owner's delete at the end is the other control — the RPC the member was
/// refused really does destroy the workspace when the right caller sends it.
#[tokio::test]
async fn a_member_is_refused_the_whole_control_plane_of_their_own_workspace() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("member-shut-ada", "Ada").await;
    let bob = deployment.seed_user("member-shut-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let member = deployment.as_person(&bob).await;
    create_workspace(&owner, "member-shut")
        .await
        .expect("the creator makes their own workspace");
    add_member(&owner, "member-shut", &bob, WorkspaceRole::Member)
        .await
        .expect("the owner grants membership");
    execute_sql(&member, "member-shut", "select 1")
        .await
        .expect("a member reads the workspace's contents");

    for (rpc, code, message, _) in control_plane_refusals(&member, "member-shut").await {
        assert_eq!(
            code,
            Code::PermissionDenied,
            "a member neither reads nor changes who may reach the workspace: {rpc} {message}",
        );
    }

    assert_eq!(
        list_members(&owner, "member-shut")
            .await
            .expect("the owner reads the roster"),
        vec![
            (ada.clone(), WorkspaceRole::Owner),
            (bob.clone(), WorkspaceRole::Member),
        ],
        "the refused changes moved nobody",
    );
    execute_sql(&member, "member-shut", "select 1")
        .await
        .expect("the refused delete destroyed nothing");

    delete_workspace(&owner, "member-shut")
        .await
        .expect("the owner deletes their own workspace");
    assert_eq!(
        execute_sql(&member, "member-shut", "select 1")
            .await
            .expect_err("a deleted workspace holds nothing to read")
            .code(),
        Code::NotFound,
    );
}

/// The control plane must not answer questions its caller may not ask. A
/// workspace a non-member holds no membership in has to read exactly like a
/// name nobody ever created — and read as the *absent* workspace specifically,
/// since a uniform "denied" would agree with itself while still confirming the
/// name exists.
#[tokio::test]
async fn a_non_members_control_plane_refusals_read_exactly_like_an_absent_workspace() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("member-conceal-ada", "Ada").await;
    let bob = deployment.seed_user("member-conceal-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let outsider = deployment.as_person(&bob).await;
    create_workspace(&owner, "member-conceal")
        .await
        .expect("the creator makes their own workspace");

    let existing = control_plane_refusals(&outsider, "member-conceal").await;
    assert_eq!(
        existing,
        control_plane_refusals(&outsider, "member-ghost").await,
        "an existing workspace must be indistinguishable from one that never existed",
    );
    assert!(
        existing
            .iter()
            .all(|(_, code, _, reasons)| *code == Code::NotFound
                && reasons.as_slice() == [CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND]),
        "both must read as the absent workspace, not as a denial that confirms one: {existing:?}",
    );
    assert_eq!(
        list_members(&owner, "member-conceal")
            .await
            .expect("the owner reads the roster"),
        vec![(ada, WorkspaceRole::Owner)],
        "an outsider's refused changes moved nobody",
    );
}

/// The two changes that are answered by a miss, and the one that is answered by
/// nothing at all.
///
/// A grant somebody already holds is a success that writes nothing, so a
/// retried invitation is indistinguishable from the first one. The other two
/// name somebody the workspace cannot act on, and each is reported as a miss on
/// that person rather than on the workspace: the caller already owns this
/// workspace, so there is nothing here to conceal from them, and saying so with
/// the workspace's own concealment reason would be a lie about which of the two
/// was not found.
#[tokio::test]
async fn a_repeated_grant_writes_nothing_and_an_unknown_person_is_a_miss_on_the_directory() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("member-miss-ada", "Ada").await;
    let bob = deployment.seed_user("member-miss-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    create_workspace(&owner, "member-miss")
        .await
        .expect("the creator makes their own workspace");

    assert_eq!(
        add_member(&owner, "member-miss", &ada, WorkspaceRole::Owner)
            .await
            .expect("repeating a grant somebody already holds succeeds")
            .member,
        Some(WorkspaceMember {
            user_id: ada.clone(),
            role: WorkspaceRole::Owner.into(),
            display_name: "Ada".to_string(),
        }),
    );

    let unknown = add_member(&owner, "member-miss", ABSENT_USER, WorkspaceRole::Member)
        .await
        .expect_err("nobody can be granted membership before their first sign-in");
    assert_eq!(
        (unknown.code(), unknown.message()),
        (Code::NotFound, &*format!("user '{ABSENT_USER}' not found")),
    );
    assert_eq!(
        concealed_refusal(&unknown, "member-miss").2,
        Vec::<String>::new(),
        "a directory miss must not carry the workspace's concealment reason",
    );
    assert_eq!(
        remove_member(&owner, "member-miss", &bob)
            .await
            .expect_err("revoking a membership nobody holds is the same miss")
            .code(),
        Code::NotFound,
    );

    assert_eq!(
        list_members(&owner, "member-miss")
            .await
            .expect("the owner reads the roster"),
        vec![(ada, WorkspaceRole::Owner)],
        "none of the three changed who may reach the workspace",
    );
}

/// Promotion, demotion, and the floor they both answer to.
///
/// The roster is read back rather than only the responses, because the failure
/// this guards against is a change that reports one thing and records another.
#[tokio::test]
async fn promotion_and_demotion_move_the_roster_and_stop_at_the_last_owner() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("member-contract-ada", "Ada").await;
    let bob = deployment.seed_user("member-contract-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    create_workspace(&owner, "member-contract")
        .await
        .expect("the creator makes their own workspace");

    // Granting and promoting are the same call, and each reports the role it
    // was asked for rather than the one the person came in with.
    for (step, role) in [
        ("grants membership", WorkspaceRole::Member),
        ("promotes a member", WorkspaceRole::Owner),
    ] {
        assert_eq!(
            add_member(&owner, "member-contract", &bob, role)
                .await
                .unwrap_or_else(|error| panic!("the owner {step}: {error}"))
                .member
                .expect("the changed membership")
                .role,
            i32::from(role),
        );
    }
    assert_eq!(
        list_members(&owner, "member-contract")
            .await
            .expect("the owner reads the roster"),
        vec![
            (ada.clone(), WorkspaceRole::Owner),
            (bob.clone(), WorkspaceRole::Owner),
        ],
        "the promotion recorded a second owner",
    );

    // Stepping an owner back down is the same call as promoting one, and it is
    // allowed here only because the co-owner above holds the floor.
    assert_eq!(
        add_member(&owner, "member-contract", &ada, WorkspaceRole::Member)
            .await
            .expect("an owner steps back down while a co-owner holds the floor")
            .member
            .expect("the changed membership")
            .role,
        i32::from(WorkspaceRole::Member),
    );

    let last_owner = deployment.as_person(&bob).await;
    for (change, result) in [
        (
            "demoted",
            add_member(&last_owner, "member-contract", &bob, WorkspaceRole::Member)
                .await
                .map(|_| ()),
        ),
        (
            "revoked",
            remove_member(&last_owner, "member-contract", &bob)
                .await
                .map(|_| ()),
        ),
    ] {
        let refused = result.expect_err("the last owner cannot strand the workspace");
        assert_eq!(
            (refused.code(), refused.message()),
            (
                Code::FailedPrecondition,
                "workspace 'member-contract' must retain at least one owner",
            ),
            "the last owner was {change}",
        );
    }

    remove_member(&last_owner, "member-contract", &ada)
        .await
        .expect("the owner revokes a plain member");
    assert_eq!(
        list_members(&last_owner, "member-contract")
            .await
            .expect("the owner reads the roster"),
        vec![(bob, WorkspaceRole::Owner)],
        "the two refusals left the floor where it was, and only the plain member's revocation moved anything",
    );
}

/// Membership is read per request, so a change lands on the caller's very next
/// call over the connection they already hold rather than at their next login.
///
/// Each step is a repeat of a call that had just succeeded, which is what makes
/// the difference attributable to the change rather than to the request.
#[tokio::test]
async fn a_revoked_callers_very_next_request_is_answered_by_the_change() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("member-revoke-ada", "Ada").await;
    let bob = deployment.seed_user("member-revoke-bob", "Bob").await;
    let owner = deployment.as_person(&ada).await;
    let bob_client = deployment.as_person(&bob).await;
    create_workspace(&owner, "member-revoke")
        .await
        .expect("the creator makes their own workspace");
    add_member(&owner, "member-revoke", &bob, WorkspaceRole::Owner)
        .await
        .expect("the owner grants a second ownership");
    list_members(&bob_client, "member-revoke")
        .await
        .expect("a co-owner reads the roster");

    add_member(&owner, "member-revoke", &bob, WorkspaceRole::Member)
        .await
        .expect("the owner steps their co-owner back down");
    assert_eq!(
        list_members(&bob_client, "member-revoke")
            .await
            .expect_err("the demoted co-owner's next request is answered as a member's")
            .code(),
        Code::PermissionDenied,
    );
    execute_sql(&bob_client, "member-revoke", "select 1")
        .await
        .expect("the demotion left the membership it stepped down to");

    remove_member(&owner, "member-revoke", &bob)
        .await
        .expect("the owner revokes the membership");
    let concealed = list_members(&bob_client, "member-revoke")
        .await
        .expect_err("the revoked member's next request is answered as an outsider's");
    assert_eq!(
        concealed_refusal(&concealed, "member-revoke"),
        (
            Code::NotFound,
            "workspace '<workspace>' not found".to_string(),
            vec![CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND.to_string()],
        ),
    );
    assert_eq!(
        execute_sql(&bob_client, "member-revoke", "select 1")
            .await
            .expect_err("the revocation closed the contents too")
            .code(),
        Code::NotFound,
    );
    assert!(
        membership_rows(&bob_client).await.is_empty(),
        "a revoked caller's own listing no longer names the workspace",
    );
}

/// The owner floor holds when two owners change themselves at the same time.
///
/// Either change is permitted on its own: the workspace has two owners, so
/// stepping one down and revoking the other each leave an owner behind. Only
/// together do they strand it, and only if the second one decides on an owner
/// count the first has already invalidated. Exactly one must therefore be
/// refused, and which one is not the test's business.
///
/// This is the transport-level half of a guarantee the transaction layer proves
/// against a barrier that pauses a mutation while it holds the workspace
/// parent. That barrier is crate-internal, so what is asked here is the real
/// question a client can ask: two requests in flight at once, on a runtime with
/// threads to run them, against a pool with connections to serve them.
///
/// The race was falsified before it was trusted: replacing the parent hold with
/// a read-only existence check fails this test on every run. The assertion that
/// catches it is the one on *why* the loser was refused, not the one on how
/// many changes were allowed — without the hold both writers read the same
/// owner count, and the second one is then stopped by the database rather than
/// by the floor. So an ownerless workspace never materializes on `SQLite`, and
/// a test that only counted the successes would pass over the broken code.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_owner_changes_cannot_strand_the_workspace() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("member-race-ada", "Ada").await;
    let bob = deployment.seed_user("member-race-bob", "Bob").await;
    let ada_client = deployment.as_person(&ada).await;
    let bob_client = deployment.as_person(&bob).await;
    create_workspace(&ada_client, "member-race")
        .await
        .expect("the creator makes their own workspace");
    add_member(&ada_client, "member-race", &bob, WorkspaceRole::Owner)
        .await
        .expect("the owner grants a second ownership");

    let (demotion, revocation) = tokio::join!(
        add_member(&ada_client, "member-race", &ada, WorkspaceRole::Member),
        remove_member(&bob_client, "member-race", &bob),
    );

    let refused = match (demotion, revocation) {
        (Ok(_), Err(refused)) | (Err(refused), Ok(_)) => refused,
        (Ok(_), Ok(_)) => panic!("both changes were allowed, leaving the workspace with no owner"),
        (Err(demotion), Err(revocation)) => {
            panic!("neither change was allowed: {demotion}, {revocation}")
        }
    };
    assert_eq!(
        (refused.code(), refused.message()),
        (
            Code::FailedPrecondition,
            "workspace 'member-race' must retain at least one owner",
        ),
        "the loser must be refused by the owner floor rather than by a lock or a conflict",
    );

    // Read back from each caller's own listing rather than from the roster: the
    // roster is an owner's read, and which of the two is still an owner is
    // exactly what is in question.
    let mut owners = Vec::new();
    for (client, who) in [(&ada_client, "Ada"), (&bob_client, "Bob")] {
        if membership_rows(client)
            .await
            .contains(&("member-race".to_string(), WorkspaceRole::Owner))
        {
            owners.push(who);
        }
    }
    assert_eq!(
        owners.len(),
        1,
        "exactly one owner must survive the race: {owners:?}",
    );
}

/// An agent credential carries the person behind it and none of their control
/// plane. The same user id owns this workspace through their own session, so
/// the refusals below are about the credential's kind rather than about who is
/// behind it — and the person's own calls afterwards say so.
///
/// The denial is uniform across workspaces because the rule refuses an agent
/// every workspace alike: it is settled before any membership is read, so it
/// reveals nothing about which workspaces exist.
#[tokio::test]
async fn an_agent_credential_is_refused_the_control_plane_of_its_own_persons_workspace() {
    let deployment = SharedDeployment::start().await;
    let ada = deployment.seed_user("member-agent-ada", "Ada").await;
    let person = deployment.as_person(&ada).await;
    let agent = deployment.as_agent(&ada).await;
    create_workspace(&person, "member-agent")
        .await
        .expect("the creator makes their own workspace");
    execute_sql(&agent, "member-agent", "select 1")
        .await
        .expect("the agent session reads what the person behind it may read");

    let refusals = control_plane_refusals(&agent, "member-agent").await;
    for (rpc, code, message, _) in &refusals {
        assert_eq!(
            *code,
            Code::PermissionDenied,
            "an agent credential manages no workspace, not even its own person's: {rpc} {message}",
        );
    }
    assert_eq!(
        refusals,
        control_plane_refusals(&agent, "member-agent-ghost").await,
        "the refusal must not tell an agent which workspaces exist",
    );
    assert_eq!(
        create_workspace(&agent, "member-agent-new")
            .await
            .expect_err("an agent credential cannot make itself an owner")
            .code(),
        Code::PermissionDenied,
    );

    assert_eq!(
        list_members(&person, "member-agent")
            .await
            .expect("the person's own session manages the workspace their agent could not"),
        vec![(ada, WorkspaceRole::Owner)],
        "nothing the agent asked for changed the roster",
    );
    assert_eq!(
        membership_rows(&person).await,
        vec![("member-agent".to_string(), WorkspaceRole::Owner)],
        "the workspace the agent was refused was never created",
    );
}
