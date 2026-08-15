use coral_api::v1::WorkspaceRole;
use tonic::Code;

use crate::harness::{
    SharedDeployment, WorkspaceWork, add_member, create_workspace, execute_sql, remove_member,
};

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
    let owner_agent = deployment.as_agent(&ada).await;
    let outsider = deployment.as_person(&bob).await;
    let created = create_workspace(&owner, "read-harness")
        .await
        .expect("the creator makes their own workspace");
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

    execute_sql(&owner_agent, "read-harness", "select 1")
        .await
        .expect("an agent session reads what the person behind it may read");
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
