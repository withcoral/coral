//! The relayed compile rows on both databases, at explicit times.

use std::time::Duration;

use coral_db::{Database, UnixNanos};
use coral_test_support::TestDatabase;

use super::{NewRequest, Status, Take, delete_past, request, status, take_oldest};

/// Writes a probe row `id` with the deadline `deadline`.
async fn write(database: &Database, id: &str, deadline: i64) {
    let mut transaction = database.begin().await.unwrap();
    let row = NewRequest {
        id,
        requester: "requester",
        call: None,
        created_at: UnixNanos::new(0),
        deadline: UnixNanos::new(deadline),
    };
    request(&mut transaction, row).await.unwrap();
    transaction.commit().await.unwrap();
}

async fn waiting(database: &Database, id: &str) -> Option<bool> {
    match status(database, id).await.unwrap() {
        Status::Waiting { taken } => Some(taken),
        Status::Answered { .. } | Status::Gone => None,
    }
}

// Catches: a take mark that raises no change signal, so a requester sees a
// take only on its next look, and gives up on a row a pickup is serving.
#[tokio::test]
async fn a_take_wakes_the_requester() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let mut changes = database.subscribe_changes().unwrap();
    write(&database, "row", i64::MAX).await;
    tokio::time::timeout(Duration::from_secs(5), changes.changed())
        .await
        .expect("the write raises the change signal")
        .unwrap();

    let mut transaction = database.begin().await.unwrap();
    let take = Take {
        taker: "server",
        deadline_after: UnixNanos::new(0),
        from: None,
    };
    let taken = take_oldest(&mut transaction, take).await.unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(taken.map(|taken| taken.id).as_deref(), Some("row"));
    tokio::time::timeout(Duration::from_secs(5), changes.changed())
        .await
        .expect("the take raises the change signal")
        .unwrap();
    assert_eq!(waiting(&database, "row").await, Some(true));
}

// Catches: a cleanup that deletes a row its requester may still wait for, or
// keeps one left over past its deadline.
#[tokio::test]
async fn a_row_left_past_its_deadline_is_deleted_and_reads_as_gone() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    write(&database, "left over", 1_000).await;
    write(&database, "due now", 2_000).await;
    write(&database, "waited for", 3_000).await;

    let mut transaction = database.begin().await.unwrap();
    delete_past(&mut transaction, UnixNanos::new(2_000))
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    for gone in ["left over", "due now"] {
        assert!(
            matches!(status(&database, gone).await.unwrap(), Status::Gone),
            "{gone}"
        );
    }
    assert_eq!(waiting(&database, "waited for").await, Some(false));
}
