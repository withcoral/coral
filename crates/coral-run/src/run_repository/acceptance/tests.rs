use coral_test_support::{TestDatabase, change, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, OrganizationRole, WorkspaceDisplayName, WorkspaceModule,
};

use sea_query::{Expr, ExprTrait as _, Query};

use super::insert_record;
use crate::RunId;
use crate::run_repository::ProgramRunRecords;
use crate::types::AcceptedRun;

/// A conflict on the identifier writes nothing and reports `false`; a fresh
/// identifier in the same transaction still inserts.
#[tokio::test]
async fn a_taken_run_identifier_is_reported_and_writes_nothing() {
    let fixture = TestDatabase::new(coral_db::migration_plan())
        .await
        .expect("test database");
    let database = fixture.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let user = provision_user(
        database.clone(),
        "user:run-identifier",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(user.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Identifiers").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let principal = AuthenticatedPrincipal::user(user).principal().clone();
    let run = |id: &str, accepted_at_unix_nanos: i64| AcceptedRun {
        id: RunId::parse(id).expect("identifier"),
        workspace_id: workspace.id().clone(),
        principal: principal.clone(),
        accepted_at_unix_nanos,
        deadline_at_unix_nanos: accepted_at_unix_nanos + 1_000,
        shutdown_grace_nanos: 0,
        value_limits: coral_value::ValueLimits::default(),
        claim_id: format!("claim-{accepted_at_unix_nanos}"),
        worker_claim_expires_at_unix_nanos: accepted_at_unix_nanos + 1_000,
        linked_snapshots: std::collections::HashMap::new(),
    };
    let taken = "height-word-database-library";

    let mut transaction = database.begin().await.expect("transaction");
    assert!(
        insert_record(&mut transaction, &run(taken, 10))
            .await
            .expect("first insert")
    );
    assert!(
        !insert_record(&mut transaction, &run(taken, 20))
            .await
            .expect("conflicting insert")
    );
    assert!(
        insert_record(&mut transaction, &run("random-big-business-store", 30))
            .await
            .expect("fresh insert after the conflict")
    );
    let statement = Query::select()
        .column(ProgramRunRecords::AcceptedAtUnixNanos)
        .from(ProgramRunRecords::Table)
        .and_where(Expr::col(ProgramRunRecords::Id).eq(taken))
        .to_owned();
    let (accepted_at_unix_nanos,): (i64,) = transaction
        .fetch_optional(statement)
        .await
        .expect("read the Run row")
        .expect("the first Run row");
    assert_eq!(accepted_at_unix_nanos, 10, "the conflict changed nothing");
    transaction.commit().await.expect("commit");
}
