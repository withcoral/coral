use std::fs;
use std::path::Path;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;

use rusqlite::{Connection, TransactionBehavior, params};

use super::super::governance::{
    ObservedValuesStoragePolicy, observed_fts_mergeable_segments_exist,
};
use super::super::sqlite_projection::{MAX_OBSERVED_QUEUE_JOB_ATTEMPTS, ObservedValuesDrainBudget};
use super::super::{
    ObservedValuesLiveScope, ObservedValuesLiveScopeLoadFailure, ObservedValuesRetrievalPolicy,
};
use super::{
    SqliteObservedValuesStore, clear_source_in_transaction, enqueue_if_current_in_transaction,
};
use crate::search::observed::sqlite_queue::{
    ObservedValuesEnqueueResult, ObservedValuesQueueJob, ObservedValuesSurfaceKind,
};
use crate::search::sqlite_store::{SqliteSearchError, SqliteSearchStore};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;
use tempfile::tempdir;

#[test]
fn queue_job_is_durable_across_store_reopen() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
    let result = store
        .enqueue_if_current(&workspace, &test_job(), epoch)
        .expect("enqueue");

    assert!(matches!(
        result,
        ObservedValuesEnqueueResult::Enqueued { .. }
    ));
    let reopened = SqliteObservedValuesStore::new(layout);
    assert_eq!(
        reopened
            .pending_queue_job_count(&workspace)
            .expect("queue count"),
        1
    );
}

#[test]
fn clear_workspace_does_not_need_source_manifests() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
    store
        .enqueue_if_current(&workspace, &test_job(), epoch)
        .expect("enqueue");

    store
        .clear_workspace_and_advance_epoch(&workspace)
        .expect("clear workspace");

    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("queue count"),
        0
    );
    assert_eq!(
        store
            .capture_epoch(&workspace, "github")
            .expect("epoch")
            .workspace_generation,
        1
    );
}

#[test]
fn stale_source_epoch_is_not_enqueued() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let stale_epoch = store.capture_epoch(&workspace, "github").expect("epoch");

    store
        .clear_source_and_advance_epoch(&workspace, "github")
        .expect("clear source");
    let result = store
        .enqueue_if_current(&workspace, &test_job(), stale_epoch)
        .expect("enqueue");

    assert_eq!(result, ObservedValuesEnqueueResult::StaleEpoch);
    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("queue count"),
        0
    );
}

#[test]
fn clear_transaction_committing_first_rejects_in_flight_observation() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let captured_epoch = store
        .capture_epoch(&workspace, "github")
        .expect("captured epoch");

    let search_store =
        SqliteSearchStore::open_workspace(&layout, &workspace).expect("search store");
    let mut connection = search_store.connect_for_test().expect("connection");
    let mut contending_connection = search_store.connect_for_test().expect("contender");
    contending_connection
        .busy_timeout(Duration::ZERO)
        .expect("disable contender wait");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .expect("clear transaction");
    clear_source_in_transaction(&transaction, &workspace, "github")
        .expect("clear source in transaction");

    let (contended_tx, contended_rx) = sync_channel(0);
    let (retry_tx, retry_rx) = sync_channel(0);
    let worker = thread::spawn({
        let store = store.clone();
        let workspace = workspace.clone();
        move || {
            contended_tx
                .send(immediate_transaction_is_locked(&mut contending_connection))
                .expect("report lock contention");
            retry_rx.recv().expect("retry after clear commit");
            store.enqueue_if_current(&workspace, &test_job(), captured_epoch)
        }
    });
    assert!(
        contended_rx.recv().expect("lock contention result"),
        "enqueue must contend with the open clear transaction"
    );
    transaction.commit().expect("commit clear");
    retry_tx.send(()).expect("resume enqueue");

    let result = worker.join().expect("enqueue worker").expect("enqueue");
    assert_eq!(result, ObservedValuesEnqueueResult::StaleEpoch);
    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("queue count"),
        0
    );
}

#[test]
fn enqueue_transaction_committing_first_is_removed_by_clear() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let captured_epoch = store
        .capture_epoch(&workspace, "github")
        .expect("captured epoch");

    let search_store =
        SqliteSearchStore::open_workspace(&layout, &workspace).expect("search store");
    let mut connection = search_store.connect_for_test().expect("connection");
    let mut contending_connection = search_store.connect_for_test().expect("contender");
    contending_connection
        .busy_timeout(Duration::ZERO)
        .expect("disable contender wait");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .expect("enqueue transaction");
    let result = enqueue_if_current_in_transaction(
        &transaction,
        &workspace,
        &test_job(),
        captured_epoch,
        ObservedValuesStoragePolicy::default(),
    )
    .expect("enqueue in transaction");
    assert!(matches!(
        result,
        ObservedValuesEnqueueResult::Enqueued { .. }
    ));

    let (contended_tx, contended_rx) = sync_channel(0);
    let (retry_tx, retry_rx) = sync_channel(0);
    let worker = thread::spawn({
        let store = store.clone();
        let workspace = workspace.clone();
        move || {
            contended_tx
                .send(immediate_transaction_is_locked(&mut contending_connection))
                .expect("report lock contention");
            retry_rx.recv().expect("retry after enqueue commit");
            store.clear_source_and_advance_epoch(&workspace, "github")
        }
    });
    assert!(
        contended_rx.recv().expect("lock contention result"),
        "clear must contend with the open enqueue transaction"
    );
    transaction.commit().expect("commit enqueue");
    retry_tx.send(()).expect("resume clear");
    worker.join().expect("clear worker").expect("clear source");

    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("queue count"),
        0
    );
}

#[test]
fn workspace_clear_invalidates_captured_source_epoch() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let captured_epoch = store
        .capture_epoch(&workspace, "github")
        .expect("captured epoch");

    store
        .clear_workspace_and_advance_epoch(&workspace)
        .expect("clear workspace");
    let result = store
        .enqueue_if_current(&workspace, &test_job(), captured_epoch)
        .expect("enqueue");

    assert_eq!(result, ObservedValuesEnqueueResult::StaleEpoch);
}

fn immediate_transaction_is_locked(connection: &mut rusqlite::Connection) -> bool {
    match connection.transaction_with_behavior(TransactionBehavior::Immediate) {
        Ok(transaction) => {
            drop(transaction);
            false
        }
        Err(error) => SqliteSearchError::from(error).is_lock_contention(),
    }
}

#[test]
fn capture_epochs_for_sources_reads_all_sources_with_one_store_open() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);

    store
        .clear_source_and_advance_epoch(&workspace, "github")
        .expect("clear github");
    store
        .clear_source_and_advance_epoch(&workspace, "slack")
        .expect("clear slack");
    store
        .clear_source_and_advance_epoch(&workspace, "slack")
        .expect("clear slack again");

    let epochs = store
        .capture_epochs_for_sources(&workspace, ["github", "slack", "notion"])
        .expect("epochs");

    assert_eq!(
        epochs
            .get("github")
            .expect("github epoch")
            .source_generation,
        1
    );
    assert_eq!(
        epochs.get("slack").expect("slack epoch").source_generation,
        2
    );
    assert_eq!(
        epochs
            .get("notion")
            .expect("notion epoch")
            .source_generation,
        0
    );
}

#[test]
fn pending_queue_jobs_are_deduplicated_by_scope() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
    store
        .enqueue_if_current(&workspace, &test_job_with("scope", "issues", "Bug"), epoch)
        .expect("first enqueue");
    store
        .enqueue_if_current(&workspace, &test_job_with("scope", "issues", "Fix"), epoch)
        .expect("second enqueue");

    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("queue count"),
        1
    );
    assert_eq!(
        store.queue_payloads(&workspace).expect("payloads"),
        [payload_json("Fix")]
    );
}

#[test]
fn enqueue_respects_workspace_pending_queue_cap() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with("scope-1", "issues", "One"),
            epoch,
        )
        .expect("first enqueue");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with("scope-2", "issues", "Two"),
            epoch,
        )
        .expect("second enqueue");
    let result = store
        .enqueue_if_current(
            &workspace,
            &test_job_with("scope-3", "issues", "Three"),
            epoch,
        )
        .expect("third enqueue");

    assert_eq!(result, ObservedValuesEnqueueResult::QueueFull);
    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("queue count"),
        2
    );
}

#[test]
fn enqueue_applies_workspace_storage_backpressure() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            max_storage_bytes: 1,
            wal_headroom_bytes: 0,
            ..ObservedValuesStoragePolicy::default()
        },
    );
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");

    let result = store
        .enqueue_if_current(&workspace, &test_job(), generation)
        .expect("enqueue result");

    assert_eq!(result, ObservedValuesEnqueueResult::StorageLimitReached);
    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("queue count"),
        0
    );
}

#[test]
fn drain_queue_projects_observed_values_into_searchable_fts() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with("scope", "issues", "Payment outage"),
            generation,
        )
        .expect("enqueue");

    let result = store
        .drain_queue(&workspace, drain_budget())
        .expect("drain queue");

    assert_eq!(result.queue_jobs_processed, 1);
    assert_eq!(result.canonical_rows_upserted, 1);
    assert_eq!(result.fts_rows_written, 1);
    assert_eq!(result.remaining_queue_depth, 0);
    let hits = store
        .search(
            &workspace,
            &[String::from("payment")],
            10,
            &test_policy(&[("scope", "issues")]),
        )
        .expect("search observed values");
    assert_eq!(hits.value_count, 1);
    assert_eq!(hits.hits.len(), 1);
    let hit = hits.hits.first().expect("observed hit");
    assert_eq!(hit.source_name, "github");
    assert_eq!(hit.surface_name, "issues");
    assert_eq!(hit.column_name, "title");
    assert_eq!(hit.display_value, "Payment outage");
    assert_eq!(hit.observation_count, 1);
}

#[test]
fn storage_pressure_drops_best_effort_jobs_without_projecting() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = ordinary_store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    for (scope, value) in [("scope-1", "One"), ("scope-2", "Two")] {
        ordinary_store
            .enqueue_if_current(
                &workspace,
                &test_job_with(scope, "issues", value),
                generation,
            )
            .expect("enqueue");
    }
    let pressure_store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            max_storage_bytes: 1,
            wal_headroom_bytes: 0,
            stale_after_days: u32::MAX,
            maintenance_batch_rows: 0,
        },
    );

    let result = pressure_store
        .drain_queue(&workspace, drain_budget())
        .expect("pressure-limited drain");

    assert_eq!(result.queue_jobs_processed, 0);
    assert_eq!(result.storage_jobs_dropped, 2);
    assert_eq!(result.remaining_queue_depth, 0);
    assert!(!result.budget_exhausted);
    assert!(result.storage_limit_reached);
    assert_eq!(
        pressure_store
            .projected_value_count(&workspace)
            .expect("projected value count"),
        0
    );
}

#[test]
fn projection_crossing_live_page_limit_is_rolled_back_and_dropped() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = ordinary_store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    let job = bulk_test_job("large-scope", "large", 500);
    ordinary_store
        .enqueue_if_current(&workspace, &job, generation)
        .expect("enqueue large job");
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let page_size: i64 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("page size");
    let page_count: i64 = connection
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .expect("page count");
    let freelist_count: i64 = connection
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .expect("freelist count");
    let live_bytes = u64::try_from(page_count.saturating_sub(freelist_count))
        .expect("non-negative live pages")
        .saturating_mul(u64::try_from(page_size).expect("positive page size"));
    drop(connection);
    drop(backing);
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            max_storage_bytes: live_bytes
                .saturating_add(u64::try_from(page_size).expect("positive page size")),
            wal_headroom_bytes: 0,
            stale_after_days: u32::MAX,
            maintenance_batch_rows: 0,
        },
    );

    let result = governed_store
        .drain_queue(&workspace, drain_budget())
        .expect("storage-guarded drain");

    assert_eq!(result.queue_jobs_processed, 0);
    assert_eq!(result.storage_jobs_dropped, 1);
    assert_eq!(result.remaining_queue_depth, 0);
    assert_eq!(
        governed_store
            .projected_value_count(&workspace)
            .expect("projected value count"),
        0
    );
}

#[test]
fn boundary_crossing_projection_evicts_oldest_values_and_keeps_fresh_job() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = ordinary_store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    ordinary_store
        .enqueue_if_current(
            &workspace,
            &bulk_test_job("old-scope", "old", 160),
            generation,
        )
        .expect("enqueue old values");
    ordinary_store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(5)),
        )
        .expect("project old values");
    ordinary_store
        .enqueue_if_current(
            &workspace,
            &bulk_test_job("fresh-scope", "fresh", 160),
            generation,
        )
        .expect("enqueue fresh values");
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let live_bytes = live_database_bytes_for_test(&connection);
    let page_size: i64 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("page size");
    drop(connection);
    drop(backing);
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout.clone(),
        ObservedValuesStoragePolicy {
            max_storage_bytes: live_bytes
                .saturating_add(u64::try_from(page_size).expect("positive page size")),
            wal_headroom_bytes: 0,
            stale_after_days: u32::MAX,
            maintenance_batch_rows: 256,
        },
    );

    let result = governed_store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(1, Duration::from_secs(10)),
        )
        .expect("storage-reclaiming drain");

    assert_eq!(result.queue_jobs_processed, 1);
    assert_eq!(result.storage_jobs_dropped, 0);
    assert!(result.evicted_rows > 0);
    assert_eq!(result.remaining_queue_depth, 0);
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let fresh_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1 AND source_scope_id = 'fresh-scope'",
            params![workspace.as_str()],
            |row| row.get(0),
        )
        .expect("fresh projected rows");
    assert_eq!(fresh_rows, 160);
}

#[test]
fn boundary_crossing_projection_respects_eviction_cap_and_keeps_job_queued() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = ordinary_store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    ordinary_store
        .enqueue_if_current(
            &workspace,
            &bulk_test_job("old-scope", "old", 160),
            generation,
        )
        .expect("enqueue old values");
    ordinary_store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(5)),
        )
        .expect("project old values");
    ordinary_store
        .enqueue_if_current(
            &workspace,
            &bulk_test_job("fresh-scope", "fresh", 160),
            generation,
        )
        .expect("enqueue fresh values");
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let live_bytes = live_database_bytes_for_test(&connection);
    let page_size: i64 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("page size");
    drop(connection);
    drop(backing);
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout.clone(),
        ObservedValuesStoragePolicy {
            max_storage_bytes: live_bytes
                .saturating_add(u64::try_from(page_size).expect("positive page size")),
            wal_headroom_bytes: 0,
            stale_after_days: u32::MAX,
            maintenance_batch_rows: 1,
        },
    );

    let result = governed_store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(20, Duration::from_secs(10)),
        )
        .expect("storage-reclaiming drain");

    assert_eq!(result.queue_jobs_processed, 0);
    assert_eq!(result.storage_jobs_dropped, 0);
    assert_eq!(result.evicted_rows, 1);
    assert_eq!(result.remaining_queue_depth, 1);
    assert!(result.budget_exhausted);
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let fresh_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1 AND source_scope_id = 'fresh-scope'",
            params![workspace.as_str()],
            |row| row.get(0),
        )
        .expect("fresh projected rows");
    assert_eq!(fresh_rows, 0);
}

#[test]
fn upgraded_v2_fts_tombstones_are_compacted_without_a_queued_job() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let database_path = layout.search_sqlite_file(&workspace);
    seed_v2_fts_tombstones(&database_path);

    let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = ordinary_store
        .capture_epoch(&workspace, "github")
        .expect("upgrade v2 database and capture generation");
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let secure_delete: i64 = connection
        .query_row(
            "SELECT v FROM observed_values_fts_config WHERE k = 'secure-delete'",
            [],
            |row| row.get(0),
        )
        .expect("secure-delete setting");
    assert_eq!(secure_delete, 1);
    assert!(
        observed_fts_mergeable_segments_exist(&connection).expect("mergeable legacy FTS segments")
    );
    let live_bytes_before = live_database_bytes_for_test(&connection);
    let page_size: i64 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("page size");
    drop(connection);
    drop(backing);
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            max_storage_bytes: live_bytes_before
                .saturating_sub(u64::try_from(page_size).expect("positive page size")),
            wal_headroom_bytes: 0,
            stale_after_days: u32::MAX,
            maintenance_batch_rows: 256,
        },
    );
    assert_eq!(
        governed_store
            .pending_queue_job_count(&workspace)
            .expect("empty upgraded queue"),
        0
    );
    let mut final_result = None;

    for _ in 0..32 {
        let result = governed_store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(1, Duration::from_secs(5)),
            )
            .expect("maintain upgraded tombstone-heavy database");
        assert_eq!(result.queue_jobs_processed, 0);
        assert_eq!(result.storage_jobs_dropped, 0);
        assert_eq!(result.remaining_queue_depth, 0);
        let complete = !result.storage_limit_reached;
        final_result = Some(result);
        if complete {
            break;
        }
    }

    let final_result = final_result.expect("at least one drain result");
    assert_eq!(final_result.remaining_queue_depth, 0);
    assert!(!final_result.storage_limit_reached);
    assert!(matches!(
        governed_store
            .enqueue_if_current(&workspace, &test_job(), generation)
            .expect("enqueue fresh observation after tombstone maintenance"),
        ObservedValuesEnqueueResult::Enqueued { .. }
    ));
    let projection = governed_store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(1, Duration::from_secs(5)),
        )
        .expect("project fresh observation after tombstone maintenance");
    assert_eq!(projection.queue_jobs_processed, 1);
    assert_eq!(projection.storage_jobs_dropped, 0);
    assert_eq!(projection.remaining_queue_depth, 0);
    assert_eq!(
        governed_store
            .projected_value_count(&workspace)
            .expect("projected fresh value count"),
        1
    );
}

#[test]
fn upgraded_v2_fts_tombstones_are_compacted_during_enqueue() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    seed_v2_fts_tombstones(&layout.search_sqlite_file(&workspace));
    let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = ordinary_store
        .capture_epoch(&workspace, "github")
        .expect("upgrade v2 database and capture generation");
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let live_bytes = live_database_bytes_for_test(&connection);
    let page_size: i64 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("page size");
    drop(connection);
    drop(backing);
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            max_storage_bytes: live_bytes
                .saturating_sub(u64::try_from(page_size).expect("positive page size")),
            wal_headroom_bytes: 0,
            stale_after_days: u32::MAX,
            maintenance_batch_rows: 256,
        },
    );

    assert!(matches!(
        governed_store
            .enqueue_if_current(&workspace, &test_job(), generation)
            .expect("enqueue after bounded tombstone compaction"),
        ObservedValuesEnqueueResult::Enqueued { .. }
    ));
    assert_eq!(
        governed_store
            .pending_queue_job_count(&workspace)
            .expect("queued fresh observation"),
        1
    );
}

#[test]
fn ordinary_drain_purges_stale_observed_rows() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let initial_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = initial_store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    initial_store
        .enqueue_if_current(&workspace, &test_job(), generation)
        .expect("enqueue");
    initial_store
        .drain_queue(&workspace, drain_budget())
        .expect("initial drain");
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "UPDATE observed_values SET last_observed_at = '2020-01-01T00:00:00.000Z' WHERE workspace = ?1",
            params![workspace.as_str()],
        )
        .expect("age observed row");
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            stale_after_days: 30,
            ..ObservedValuesStoragePolicy::default()
        },
    );

    let result = governed_store
        .drain_queue(&workspace, drain_budget())
        .expect("maintenance drain");

    assert_eq!(result.stale_rows_purged, 1);
    assert_eq!(
        governed_store
            .projected_value_count(&workspace)
            .expect("projected value count"),
        0
    );
}

#[test]
fn zero_soft_budget_reports_unfinished_stale_row_governance() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let initial_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = initial_store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    initial_store
        .enqueue_if_current(&workspace, &test_job(), generation)
        .expect("enqueue");
    initial_store
        .drain_queue(&workspace, drain_budget())
        .expect("initial drain");
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "UPDATE observed_values SET last_observed_at = '2020-01-01T00:00:00.000Z' WHERE workspace = ?1",
            params![workspace.as_str()],
        )
        .expect("age observed row");
    drop(connection);
    drop(backing);
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            stale_after_days: 30,
            maintenance_batch_rows: 0,
            ..ObservedValuesStoragePolicy::default()
        },
    );

    let result = governed_store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::ZERO),
        )
        .expect("zero-budget governance drain");

    assert!(result.budget_exhausted);
    assert_eq!(result.stale_rows_purged, 0);
    assert_eq!(
        governed_store
            .projected_value_count(&workspace)
            .expect("projected value count"),
        1
    );
}

#[test]
fn zero_soft_budget_without_governance_work_is_not_exhausted() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);

    let result = store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::ZERO),
        )
        .expect("zero-budget empty drain");

    assert!(!result.budget_exhausted);
    assert_eq!(result.remaining_queue_depth, 0);
    assert!(!result.storage_limit_reached);
}

#[test]
fn zero_soft_budget_with_catalog_only_storage_pressure_is_not_exhausted() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            max_storage_bytes: 1,
            wal_headroom_bytes: 0,
            ..ObservedValuesStoragePolicy::default()
        },
    );

    let result = store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::ZERO),
        )
        .expect("zero-budget catalog-only drain");

    assert!(result.storage_limit_reached);
    assert!(!result.budget_exhausted);
    assert_eq!(result.evicted_rows, 0);
    assert_eq!(
        store
            .projected_value_count(&workspace)
            .expect("projected value count"),
        0
    );
}

#[test]
fn zero_soft_budget_reports_unfinished_storage_governance() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let initial_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = initial_store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    initial_store
        .enqueue_if_current(&workspace, &test_job(), generation)
        .expect("enqueue");
    initial_store
        .drain_queue(&workspace, drain_budget())
        .expect("initial drain");
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            max_storage_bytes: 1,
            wal_headroom_bytes: 0,
            stale_after_days: u32::MAX,
            maintenance_batch_rows: 0,
        },
    );

    let result = governed_store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::ZERO),
        )
        .expect("zero-budget governance drain");

    assert!(result.budget_exhausted);
    assert!(result.storage_limit_reached);
    assert_eq!(result.evicted_rows, 0);
    assert_eq!(
        governed_store
            .projected_value_count(&workspace)
            .expect("projected value count"),
        1
    );
}

#[test]
fn ordinary_drain_bounds_eviction_when_workspace_is_over_limit() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let initial_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = initial_store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    initial_store
        .enqueue_if_current(&workspace, &test_job(), generation)
        .expect("enqueue");
    initial_store
        .drain_queue(&workspace, drain_budget())
        .expect("initial drain");
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout,
        ObservedValuesStoragePolicy {
            max_storage_bytes: 1,
            wal_headroom_bytes: 0,
            stale_after_days: u32::MAX,
            maintenance_batch_rows: 1,
        },
    );

    let result = governed_store
        .drain_queue(
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_mins(1)),
        )
        .expect("governance drain");

    assert_eq!(result.evicted_rows, 1);
    assert!(result.storage_limit_reached);
    assert!(!result.budget_exhausted);
    assert_eq!(
        governed_store
            .projected_value_count(&workspace)
            .expect("projected value count"),
        0
    );
}

#[test]
fn eviction_preserves_same_value_key_owned_by_another_source() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let initial_store = SqliteObservedValuesStore::new(layout.clone());
    for owner_source_name in ["owner-a", "owner-b"] {
        let generation = initial_store
            .capture_epoch(&workspace, owner_source_name)
            .expect("generation");
        initial_store
            .enqueue_if_current(
                &workspace,
                &test_job_with_owner(owner_source_name),
                generation,
            )
            .expect("enqueue owner observation");
    }
    initial_store
        .drain_queue(&workspace, drain_budget())
        .expect("initial drain");
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "
            UPDATE observed_values
            SET last_observed_at = CASE owner_source_name
                WHEN 'owner-a' THEN '2020-01-01T00:00:00.000Z'
                ELSE '2021-01-01T00:00:00.000Z'
            END
            WHERE workspace = ?1
            ",
            params![workspace.as_str()],
        )
        .expect("order observed rows for eviction");
    drop(connection);
    drop(backing);
    let governed_store = SqliteObservedValuesStore::with_policy(
        layout.clone(),
        ObservedValuesStoragePolicy {
            max_storage_bytes: 1,
            wal_headroom_bytes: 0,
            stale_after_days: u32::MAX,
            maintenance_batch_rows: 1,
        },
    );

    let result = governed_store
        .drain_queue(&workspace, drain_budget())
        .expect("governance drain");

    assert_eq!(result.evicted_rows, 1);
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    for table_name in ["observed_values", "observed_values_fts"] {
        assert_eq!(
            projected_owner_names(&connection, table_name, &workspace),
            ["owner-b"],
            "eviction should remove one exact {table_name} identity"
        );
    }
}

#[test]
fn search_finds_short_observed_values_without_trigram_match() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with("scope", "issues", "OK"),
            generation,
        )
        .expect("enqueue");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain queue");

    let hits = store
        .search(
            &workspace,
            &[String::from("ok")],
            10,
            &test_policy(&[("scope", "issues")]),
        )
        .expect("short search observed values");

    assert_eq!(hits.value_count, 1);
    assert_eq!(hits.hits.len(), 1);
    let hit = hits.hits.first().expect("observed hit");
    assert_eq!(hit.display_value, "OK");
}

#[test]
fn search_filters_observed_values_by_live_source_scope() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with("live-scope", "issues", "Payment outage"),
            generation,
        )
        .expect("enqueue live");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with("old-scope", "issues", "Payment backlog"),
            generation,
        )
        .expect("enqueue stale scope");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain queue");

    let hits = store
        .search(
            &workspace,
            &[String::from("payment")],
            10,
            &test_policy(&[("live-scope", "issues")]),
        )
        .expect("search observed values");

    assert_eq!(hits.value_count, 1);
    assert_eq!(hits.hits.len(), 1);
    let hit = hits.hits.first().expect("observed hit");
    assert_eq!(hit.display_value, "Payment outage");
}

#[test]
fn search_filters_values_stale_by_last_observed_at_without_purging() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with("scope", "issues", "Fresh payment"),
            generation,
        )
        .expect("enqueue fresh");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with("old-scope", "issues", "Ancient payment"),
            generation,
        )
        .expect("enqueue old");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain queue");
    mark_observed_value_stale_for_test(&layout, &workspace, "ancient-payment");

    let hits = store
        .search(
            &workspace,
            &[String::from("payment")],
            10,
            &test_policy(&[("scope", "issues"), ("old-scope", "issues")]),
        )
        .expect("search observed values");

    assert_eq!(hits.value_count, 1);
    assert_eq!(hits.hits.len(), 1);
    let hit = hits.hits.first().expect("observed hit");
    assert_eq!(hit.display_value, "Fresh payment");
    assert_eq!(canonical_value_count_for_test(&layout, &workspace), 2);
}

#[test]
fn drain_queue_keeps_failed_payload_for_retry() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    let mut job = test_job();
    job.payload_json = "{not-json".to_string();
    store
        .enqueue_if_current(&workspace, &job, generation)
        .expect("enqueue");

    let result = store
        .drain_queue(&workspace, drain_budget())
        .expect("drain queue");

    assert_eq!(result.failed_jobs, 1);
    assert_eq!(result.remaining_queue_depth, 1);
    let attempts = store
        .queue_attempts_and_errors(&workspace)
        .expect("attempts");
    assert_eq!(attempts.len(), 1);
    let attempt = attempts.first().expect("failed attempt");
    assert_eq!(attempt.0, 1);
    assert!(
        attempt.1.contains("expected ident") || attempt.1.contains("key"),
        "parse error should be recorded, got: {}",
        attempt.1
    );
}

#[test]
fn drain_queue_dead_letters_failed_payload_after_retry_cap() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    let mut job = test_job_with("poison", "issues", "Poison");
    job.payload_json = "{not-json".to_string();
    store
        .enqueue_if_current(&workspace, &job, generation)
        .expect("enqueue poison");

    for _ in 0..MAX_OBSERVED_QUEUE_JOB_ATTEMPTS {
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");
    }

    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("active queue count"),
        0
    );
    let attempts = store
        .queue_attempts_and_errors(&workspace)
        .expect("attempts");
    assert_eq!(attempts.len(), 1);
    let attempt = attempts.first().expect("failed attempt");
    assert_eq!(attempt.0, MAX_OBSERVED_QUEUE_JOB_ATTEMPTS);
    assert!(attempt.1.contains("expected ident") || attempt.1.contains("key"));

    assert!(matches!(
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with("scope-1", "issues", "One"),
                generation,
            )
            .expect("first active enqueue"),
        ObservedValuesEnqueueResult::Enqueued { .. }
    ));
    assert!(matches!(
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with("scope-2", "issues", "Two"),
                generation,
            )
            .expect("second active enqueue"),
        ObservedValuesEnqueueResult::Enqueued { .. }
    ));
    let result = store
        .enqueue_if_current(&workspace, &job, generation)
        .expect("revive dead-lettered poison job");

    assert_eq!(result, ObservedValuesEnqueueResult::QueueFull);
    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("active queue count"),
        2
    );
    let attempts = store
        .queue_attempts_and_errors(&workspace)
        .expect("attempts after rejected revive");
    assert!(
        attempts
            .iter()
            .any(|attempt| attempt.0 == MAX_OBSERVED_QUEUE_JOB_ATTEMPTS),
        "dead-lettered job should not be reset when active queue is full"
    );
}

#[test]
fn drain_queue_deletes_stale_generation_jobs() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    store
        .enqueue_if_current(&workspace, &test_job(), generation)
        .expect("enqueue");
    advance_source_epoch_for_test(&layout, &workspace, "github");

    let result = store
        .drain_queue(&workspace, drain_budget())
        .expect("drain queue");

    assert_eq!(result.stale_jobs_skipped, 1);
    assert_eq!(result.remaining_queue_depth, 0);
    assert_eq!(
        store
            .pending_queue_job_count(&workspace)
            .expect("queue count"),
        0
    );
}

#[test]
fn rebuild_fts_recreates_observed_search_index_from_canonical_rows() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with("scope", "issues", "Invoice timeout"),
            generation,
        )
        .expect("enqueue");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain queue");
    clear_observed_fts_for_test(&layout, &workspace);
    assert!(
        store
            .search(
                &workspace,
                &[String::from("invoice")],
                10,
                &test_policy(&[("scope", "issues")]),
            )
            .expect("search without fts")
            .hits
            .is_empty()
    );

    let result = store
        .rebuild_fts(&workspace, &test_policy(&[("scope", "issues")]))
        .expect("rebuild fts");

    assert_eq!(result.canonical_rows_scanned, 1);
    assert_eq!(result.fts_rows_rebuilt, 1);
    assert_eq!(
        store
            .search(
                &workspace,
                &[String::from("invoice")],
                10,
                &test_policy(&[("scope", "issues")]),
            )
            .expect("search rebuilt fts")
            .hits
            .len(),
        1
    );
}

#[test]
fn rebuild_fts_purges_non_live_canonical_rows() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    for (scope, value) in [
        ("live-scope", "Payment current"),
        ("removed-scope", "Payment removed"),
    ] {
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with(scope, "issues", value),
                generation,
            )
            .expect("enqueue");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");
    }

    let result = store
        .rebuild_fts(&workspace, &test_policy(&[("live-scope", "issues")]))
        .expect("rebuild fts");

    assert_eq!(result.canonical_rows_scanned, 1);
    assert_eq!(result.fts_rows_rebuilt, 1);
    assert_eq!(canonical_value_count_for_test(&layout, &workspace), 1);
    let hits = store
        .search(
            &workspace,
            &[String::from("payment")],
            10,
            &test_policy(&[("live-scope", "issues")]),
        )
        .expect("search observed values");
    assert_eq!(hits.hits.len(), 1);
    let hit = hits.hits.first().expect("observed hit");
    assert_eq!(hit.display_value, "Payment current");
}

#[test]
fn rebuild_fts_preserves_rows_for_sources_with_live_scope_load_failures() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let github_generation = store
        .capture_epoch(&workspace, "github")
        .expect("github generation");
    let jira_generation = store
        .capture_epoch(&workspace, "jira")
        .expect("jira generation");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_for_owner("github", "live-scope", "issues", "Payment current"),
            github_generation,
        )
        .expect("enqueue github");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain github");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_for_owner("github", "removed-scope", "issues", "Payment removed"),
            github_generation,
        )
        .expect("enqueue removed github scope");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain removed github scope");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_for_owner("jira", "unknown-scope", "issues", "Payment blocked"),
            jira_generation,
        )
        .expect("enqueue jira");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain jira");

    let result = store
        .rebuild_fts(
            &workspace,
            &test_policy_with_failed_sources(&[("live-scope", "issues")], &["jira"]),
        )
        .expect("rebuild fts");

    assert_eq!(result.canonical_rows_scanned, 1);
    assert_eq!(result.fts_rows_rebuilt, 1);
    assert_eq!(canonical_value_count_for_test(&layout, &workspace), 2);
    assert_eq!(fts_value_count_for_test(&layout, &workspace), 2);

    let failed_policy = test_policy_with_failed_sources(&[("live-scope", "issues")], &["jira"]);
    let blocked_while_failed = store
        .search(&workspace, &[String::from("blocked")], 10, &failed_policy)
        .expect("search with failed Jira policy");
    assert!(blocked_while_failed.hits.is_empty());

    let recovered_policy = ObservedValuesRetrievalPolicy::new(
        vec![
            test_live_scope_for_owner("github", "live-scope", "issues"),
            test_live_scope_for_owner("jira", "unknown-scope", "issues"),
        ],
        365,
    );
    let recovered_hits = store
        .search(
            &workspace,
            &[String::from("blocked")],
            10,
            &recovered_policy,
        )
        .expect("search after Jira policy recovery");
    assert_eq!(recovered_hits.hits.len(), 1);
    assert_eq!(
        recovered_hits
            .hits
            .first()
            .expect("recovered Jira hit")
            .display_value,
        "Payment blocked"
    );
}

#[test]
fn rebuild_fts_skips_stale_purge_when_too_many_rows_would_be_deleted() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    for index in 0..10 {
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with("scope", "issues", &format!("Payment ancient {index}")),
                generation,
            )
            .expect("enqueue");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");
    }
    mark_all_observed_values_stale_for_test(&layout, &workspace);

    let result = store
        .rebuild_fts(&workspace, &test_policy(&[("scope", "issues")]))
        .expect("rebuild fts");

    assert_eq!(result.canonical_rows_scanned, 0);
    assert_eq!(result.fts_rows_rebuilt, 0);
    assert_eq!(canonical_value_count_for_test(&layout, &workspace), 10);
}

#[test]
fn search_may_return_more_than_limit_before_provider_diversification() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout);
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    for (scope, surface, value) in [
        ("scope-1", "issues", "Payment alpha"),
        ("scope-2", "issues", "Payment beta"),
        ("scope-3", "issues", "Payment gamma"),
        ("scope-4", "pulls", "OK"),
    ] {
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with(scope, surface, value),
                generation,
            )
            .expect("enqueue");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");
    }

    let hits = store
        .search(
            &workspace,
            &[String::from("payment"), String::from("ok")],
            3,
            &test_policy(&[
                ("scope-1", "issues"),
                ("scope-2", "issues"),
                ("scope-3", "issues"),
                ("scope-4", "pulls"),
            ]),
        )
        .expect("search observed values");

    assert_eq!(hits.value_count, 4);
    assert!(hits.retrieval_limited);
    assert!(
        hits.hits.len() > 3,
        "store should leave provider diversification with the full candidate fan-in: {:?}",
        hits.hits
    );
}

fn test_job() -> ObservedValuesQueueJob {
    test_job_with("scope", "issues", "Bug")
}

fn test_job_with(
    source_scope_id: &str,
    surface_name: &str,
    display_value: &str,
) -> ObservedValuesQueueJob {
    test_job_for_owner("github", source_scope_id, surface_name, display_value)
}

fn test_job_for_owner(
    owner_source_name: &str,
    source_scope_id: &str,
    surface_name: &str,
    display_value: &str,
) -> ObservedValuesQueueJob {
    ObservedValuesQueueJob {
        owner_source_name: owner_source_name.to_string(),
        source_name: owner_source_name.to_string(),
        source_scope_id: source_scope_id.to_string(),
        surface_kind: ObservedValuesSurfaceKind::Table,
        surface_name: surface_name.to_string(),
        payload_json: payload_json(display_value),
    }
}

fn test_job_with_owner(owner_source_name: &str) -> ObservedValuesQueueJob {
    ObservedValuesQueueJob {
        owner_source_name: owner_source_name.to_string(),
        source_name: "shared_query_schema".to_string(),
        source_scope_id: "shared-scope".to_string(),
        surface_kind: ObservedValuesSurfaceKind::Table,
        surface_name: "issues".to_string(),
        payload_json: payload_json("Shared value"),
    }
}

fn bulk_test_job(
    source_scope_id: &str,
    value_prefix: &str,
    value_count: usize,
) -> ObservedValuesQueueJob {
    let large_value = "x".repeat(512);
    let values = (0..value_count)
        .map(|index| {
            format!(
                r#"{{"column_name":"title","display_value":"{large_value}-{index}","search_text":"{large_value}-{index}","value_key":"{value_prefix}-{index}"}}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut job = test_job_with(source_scope_id, "issues", "unused");
    job.payload_json = format!(r#"{{"values":[{values}]}}"#);
    job
}

fn seed_v2_fts_tombstones(database_path: &Path) {
    fs::create_dir_all(database_path.parent().expect("search database parent"))
        .expect("create search database parent");
    let connection = Connection::open(database_path).expect("raw v2 connection");
    connection
        .execute_batch(include_str!("../../migrations/0001_catalog_search.sql"))
        .expect("v1 search schema");
    connection
        .execute_batch(include_str!("../../migrations/0002_observed_values.sql"))
        .expect("v2 observed-values schema");
    connection
        .execute(
            "
            INSERT INTO search_meta (key, value)
            VALUES ('schema_version', '2')
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ",
            [],
        )
        .expect("record v2 schema version");
    connection
        .pragma_update(None, "user_version", 2)
        .expect("record v2 user version");
    connection
        .execute_batch(
            "
            WITH RECURSIVE rows(id) AS (
                VALUES(1)
                UNION ALL
                SELECT id + 1 FROM rows WHERE id < 2048
            )
            INSERT INTO observed_values (
                workspace,
                owner_source_name,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                column_name,
                value_key,
                display_value,
                search_text,
                first_observed_at,
                last_observed_at,
                observation_count,
                source_generation,
                workspace_generation
            )
            SELECT
                'default', 'github', 'github', 'legacy', 'table', 'issues', 'title',
                printf('legacy-%d', id), hex(randomblob(256)), hex(randomblob(256)),
                '2020-01-01T00:00:00.000Z', '2020-01-01T00:00:00.000Z', 1, 0, 0
            FROM rows;

            INSERT INTO observed_values_fts (
                workspace,
                owner_source_name,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                column_name,
                value_key,
                display_value,
                search_text
            )
            SELECT
                workspace,
                owner_source_name,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                column_name,
                value_key,
                display_value,
                search_text
            FROM observed_values;

            DELETE FROM observed_values;
            DELETE FROM observed_values_fts;
            ",
        )
        .expect("create v2 FTS tombstones");
    assert!(observed_fts_mergeable_segments_exist(&connection).expect("mergeable v2 FTS segments"));
}

fn live_database_bytes_for_test(connection: &rusqlite::Connection) -> u64 {
    let page_size: i64 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("page size");
    let page_count: i64 = connection
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .expect("page count");
    let freelist_count: i64 = connection
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .expect("freelist count");
    u64::try_from(page_count.saturating_sub(freelist_count))
        .expect("non-negative live pages")
        .saturating_mul(u64::try_from(page_size).expect("positive page size"))
}

fn payload_json(display_value: &str) -> String {
    let value_key = display_value.to_ascii_lowercase().replace(' ', "-");
    format!(
        r#"{{"values":[{{"column_name":"title","display_value":"{display_value}","search_text":"{}","value_key":"{value_key}"}}]}}"#,
        display_value.to_ascii_lowercase()
    )
}

fn test_policy(scopes: &[(&str, &str)]) -> ObservedValuesRetrievalPolicy {
    ObservedValuesRetrievalPolicy::new(test_live_scopes(scopes), 365)
}

fn test_policy_with_failed_sources(
    scopes: &[(&str, &str)],
    failed_sources: &[&str],
) -> ObservedValuesRetrievalPolicy {
    ObservedValuesRetrievalPolicy::with_load_failures(
        test_live_scopes(scopes),
        failed_sources
            .iter()
            .map(|owner_source_name| ObservedValuesLiveScopeLoadFailure {
                owner_source_name: (*owner_source_name).to_string(),
                message: "failed to load".to_string(),
            })
            .collect(),
        365,
    )
}

fn test_live_scopes(scopes: &[(&str, &str)]) -> Vec<ObservedValuesLiveScope> {
    scopes
        .iter()
        .map(|(scope, surface)| test_live_scope_for_owner("github", scope, surface))
        .collect()
}

fn test_live_scope_for_owner(
    owner_source_name: &str,
    source_scope_id: &str,
    surface_name: &str,
) -> ObservedValuesLiveScope {
    ObservedValuesLiveScope {
        owner_source_name: owner_source_name.to_string(),
        source_name: owner_source_name.to_string(),
        source_scope_id: source_scope_id.to_string(),
        surface_kind: ObservedValuesSurfaceKind::Table,
        surface_name: surface_name.to_string(),
    }
}

fn drain_budget() -> ObservedValuesDrainBudget {
    ObservedValuesDrainBudget::new(10, Duration::from_secs(1))
}

fn projected_owner_names(
    connection: &rusqlite::Connection,
    table_name: &str,
    workspace: &WorkspaceName,
) -> Vec<String> {
    let sql = format!(
        "SELECT owner_source_name FROM {table_name} WHERE workspace = ?1 ORDER BY owner_source_name"
    );
    let mut statement = connection.prepare(&sql).expect("owner query");
    let rows = statement
        .query_map(params![workspace.as_str()], |row| row.get(0))
        .expect("query owner rows");
    rows.collect::<Result<Vec<_>, _>>()
        .expect("collect owner rows")
}

fn advance_source_epoch_for_test(
    layout: &AppStateLayout,
    workspace: &WorkspaceName,
    source_name: &str,
) {
    let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "
            INSERT INTO observed_source_generations (
                workspace,
                source_name,
                generation,
                updated_at
            )
            VALUES (?1, ?2, 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            ON CONFLICT(workspace, source_name) DO UPDATE SET
                generation = generation + 1,
                updated_at = excluded.updated_at
            ",
            params![workspace.as_str(), source_name],
        )
        .expect("increment source generation");
}

fn clear_observed_fts_for_test(layout: &AppStateLayout, workspace: &WorkspaceName) {
    let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "DELETE FROM observed_values_fts WHERE workspace = ?1",
            params![workspace.as_str()],
        )
        .expect("clear fts");
}

fn mark_observed_value_stale_for_test(
    layout: &AppStateLayout,
    workspace: &WorkspaceName,
    value_key: &str,
) {
    let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "
            UPDATE observed_values
            SET last_observed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-366 days')
            WHERE workspace = ?1 AND value_key = ?2
            ",
            params![workspace.as_str(), value_key],
        )
        .expect("mark stale value");
}

fn mark_all_observed_values_stale_for_test(layout: &AppStateLayout, workspace: &WorkspaceName) {
    let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "
            UPDATE observed_values
            SET last_observed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-366 days')
            WHERE workspace = ?1
            ",
            params![workspace.as_str()],
        )
        .expect("mark stale values");
}

fn canonical_value_count_for_test(layout: &AppStateLayout, workspace: &WorkspaceName) -> i64 {
    let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .query_row(
            "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1",
            params![workspace.as_str()],
            |row| row.get(0),
        )
        .expect("canonical count")
}

fn fts_value_count_for_test(layout: &AppStateLayout, workspace: &WorkspaceName) -> i64 {
    let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .query_row(
            "SELECT COUNT(*) FROM observed_values_fts WHERE workspace = ?1",
            params![workspace.as_str()],
            |row| row.get(0),
        )
        .expect("FTS count")
}
