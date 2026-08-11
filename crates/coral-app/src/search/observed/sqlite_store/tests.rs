use std::cell::Cell;
use std::path::Path;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;

use rusqlite::types::Value;
use rusqlite::{Connection, TransactionBehavior, params};

use super::super::governance::{
    ObservedValuesStoragePolicy, observed_fts_mergeable_segments_exist,
};
use super::super::sqlite_projection::{
    MAX_OBSERVED_QUEUE_JOB_ATTEMPTS, ObservedFtsRebuildPhase, ObservedValuesDrainBudget,
};
use super::super::{
    ObservedValuesLiveScope, ObservedValuesLiveScopeLoadFailure, ObservedValuesRetrievalPolicy,
};
use super::{
    SqliteObservedValuesStore, clear_observed_source_in_transaction,
    enqueue_if_current_in_transaction,
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
    clear_observed_source_in_transaction(&transaction, &workspace, "github")
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
fn clear_source_removes_only_that_installed_source() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let jobs = independent_source_clear_jobs();
    seed_projected_and_pending_jobs(&layout, &workspace, &store, &jobs);

    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    assert_projected_source_names(
        &connection,
        &workspace,
        &["github_mcp_v4", "github_v4", "jira_v4"],
    );
    drop(connection);

    let result = store
        .clear_source_and_advance_epoch(&workspace, "github_v4")
        .expect("clear github_v4");

    // `github_v4` and `github_mcp_v4` are two independent sources, not two
    // components of one: clearing the REST source must not touch the MCP one.
    assert_eq!(result.values, 1);
    assert_eq!(result.fts_rows, 1);
    assert_eq!(result.queue_jobs, 1);
    let connection = backing.connect_for_test().expect("reconnect");
    assert_projected_source_names(&connection, &workspace, &["github_mcp_v4", "jira_v4"]);
    assert_eq!(
        store
            .capture_epoch(&workspace, "github_v4")
            .expect("github_v4 epoch after clear")
            .source_generation,
        1
    );
    for untouched in ["github_mcp_v4", "jira_v4"] {
        assert_eq!(
            store
                .capture_epoch(&workspace, untouched)
                .expect("untouched epoch after clear")
                .source_generation,
            0,
            "clearing github_v4 must not advance the {untouched} epoch"
        );
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
fn fts_tombstones_are_compacted_without_a_queued_job() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let database_path = layout.search_sqlite_file(&workspace);
    seed_fts_tombstones(&database_path);

    let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = ordinary_store
        .capture_epoch(&workspace, "github")
        .expect("capture generation");
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
    assert!(observed_fts_mergeable_segments_exist(&connection).expect("mergeable FTS segments"));
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
fn fts_tombstones_are_compacted_during_enqueue() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    seed_fts_tombstones(&layout.search_sqlite_file(&workspace));
    let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
    let generation = ordinary_store
        .capture_epoch(&workspace, "github")
        .expect("capture generation");
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
    for source_name in ["source-a", "source-b"] {
        let generation = initial_store
            .capture_epoch(&workspace, source_name)
            .expect("generation");
        initial_store
            .enqueue_if_current(
                &workspace,
                &test_job_for_shared_value(source_name),
                generation,
            )
            .expect("enqueue source observation");
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
            SET last_observed_at = CASE source_name
                WHEN 'source-a' THEN '2020-01-01T00:00:00.000Z'
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
            projected_source_names(&connection, table_name, &workspace),
            ["source-b"],
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
fn projection_realigns_a_legacy_same_key_fts_row_without_leaving_a_duplicate() {
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
            &test_job_with_stable_key("scope", "issues", "Legacy value", "stable-key"),
            generation,
        )
        .expect("enqueue legacy value");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("project legacy value");

    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let canonical_rowid: i64 = connection
        .query_row(
            "SELECT rowid FROM observed_values WHERE workspace = ?1 AND value_key = 'stable-key'",
            params![workspace.as_str()],
            |row| row.get(0),
        )
        .expect("canonical rowid");
    let legacy_rowid = canonical_rowid.saturating_add(100);
    connection
        .execute(
            "DELETE FROM observed_values_fts WHERE rowid = ?1",
            params![canonical_rowid],
        )
        .expect("remove aligned row");
    connection
        .execute(
            "
            INSERT INTO observed_values_fts (
                rowid, workspace, source_name, source_scope_id,
                surface_kind, surface_name, column_name, value_key, display_value, search_text
            )
            SELECT ?2, workspace, source_name, source_scope_id,
                   surface_kind, surface_name, column_name, value_key, display_value, search_text
            FROM observed_values
            WHERE rowid = ?1
            ",
            params![canonical_rowid, legacy_rowid],
        )
        .expect("move FTS row to a legacy rowid");
    drop(connection);

    store
        .enqueue_if_current(
            &workspace,
            &test_job_with_stable_key("scope", "issues", "Fresh value", "stable-key"),
            generation,
        )
        .expect("enqueue refreshed value");
    let drained = store
        .drain_queue(&workspace, drain_budget())
        .expect("project refreshed value");
    assert_eq!(drained.failed_jobs, 0);

    let connection = backing.connect_for_test().expect("connection");
    let rows: (i64, i64, String) = connection
        .query_row(
            "
            SELECT COUNT(*), MIN(f.rowid), MIN(f.display_value)
            FROM observed_values_fts f
            WHERE f.workspace = ?1 AND f.value_key = 'stable-key'
            ",
            params![workspace.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("aligned FTS row");
    assert_eq!(rows, (1, canonical_rowid, "Fresh value".to_string()));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the collision test verifies projection rollback, failed-source preservation, recovery, and retry as one state transition"
)]
fn projection_and_rebuild_preserve_an_unrelated_failed_source_rowid_collision() {
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
            &test_job_with_stable_key("scope", "issues", "Original value", "stable-key"),
            generation,
        )
        .expect("enqueue original value");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("project original value");

    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let canonical_rowid: i64 = connection
        .query_row(
            "SELECT rowid FROM observed_values WHERE workspace = ?1 AND value_key = 'stable-key'",
            params![workspace.as_str()],
            |row| row.get(0),
        )
        .expect("canonical rowid");
    connection
        .execute(
            "DELETE FROM observed_values_fts WHERE rowid = ?1",
            params![canonical_rowid],
        )
        .expect("remove aligned row");
    connection
        .execute(
            "
            INSERT INTO observed_values_fts (
                rowid, workspace, source_name, source_scope_id,
                surface_kind, surface_name, column_name, value_key, display_value, search_text
            ) VALUES (
                ?1, ?2, 'jira', 'failed-scope', 'table', 'issues', 'title',
                'failed-key', 'Failed source value', 'failed source value'
            )
            ",
            params![canonical_rowid, workspace.as_str()],
        )
        .expect("occupy the canonical rowid with a failed-source row");
    drop(connection);

    store
        .enqueue_if_current(
            &workspace,
            &test_job_with_stable_key("scope", "issues", "Fresh value", "stable-key"),
            generation,
        )
        .expect("enqueue refreshed value");
    let failed_projection = store
        .drain_queue(&workspace, drain_budget())
        .expect("drain collision");
    assert_eq!(failed_projection.failed_jobs, 1);

    let connection = backing.connect_for_test().expect("connection");
    let state: (String, String, String) = connection
        .query_row(
            "
            SELECT v.display_value, f.source_name, f.display_value
            FROM observed_values v
            JOIN observed_values_fts f ON f.rowid = v.rowid
            WHERE v.rowid = ?1
            ",
            params![canonical_rowid],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("collision state");
    assert_eq!(
        state,
        (
            "Original value".to_string(),
            "jira".to_string(),
            "Failed source value".to_string(),
        ),
        "the failed projection transaction must preserve both canonical and unrelated FTS content"
    );
    drop(connection);

    let failed_policy = test_policy_with_failed_sources(&[("scope", "issues")], &["jira"]);
    let blocked = store
        .rebuild_fts(&workspace, &failed_policy)
        .expect("rebuild while Jira is failed");
    assert_eq!(blocked.fts_rows_rebuilt, 0);
    let connection = backing.connect_for_test().expect("connection");
    let source_while_failed: String = connection
        .query_row(
            "SELECT source_name FROM observed_values_fts WHERE rowid = ?1",
            params![canonical_rowid],
            |row| row.get(0),
        )
        .expect("failed-source occupant");
    assert_eq!(source_while_failed, "jira");
    drop(connection);

    let recovered = store
        .rebuild_fts(&workspace, &test_policy(&[("scope", "issues")]))
        .expect("rebuild after Jira recovers");
    assert_eq!(recovered.fts_rows_rebuilt, 1);
    let retried = store
        .drain_queue(&workspace, drain_budget())
        .expect("retry projection after collision recovery");
    assert_eq!(retried.failed_jobs, 0);
    assert_eq!(retried.queue_jobs_processed, 1);

    let connection = backing.connect_for_test().expect("connection");
    let recovered_state: (i64, String, String) = connection
        .query_row(
            "
            SELECT COUNT(*), MIN(source_name), MIN(display_value)
            FROM observed_values_fts
            WHERE rowid = ?1
            ",
            params![canonical_rowid],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("recovered aligned row");
    assert_eq!(
        recovered_state,
        (1, "github".to_string(), "Fresh value".to_string())
    );
}

#[test]
fn rebuild_snapshot_read_does_not_reserve_the_workspace_writer() {
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
            &test_job_with_stable_key("scope-old", "issues", "Old row", "old-key"),
            generation,
        )
        .expect("enqueue initial row");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("project initial row");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with_stable_key("scope-new", "issues", "New row", "new-key"),
            generation,
        )
        .expect("enqueue concurrent row");

    let snapshot_hook_ran = Cell::new(false);
    let policy = test_policy(&[("scope-old", "issues"), ("scope-new", "issues")]);
    let result = store
        .rebuild_fts_with_limits_guard_and_hook(
            &workspace,
            &policy,
            1,
            usize::MAX,
            |_| Ok(false),
            |phase| {
                if phase == ObservedFtsRebuildPhase::SnapshotRead
                    && !snapshot_hook_ran.replace(true)
                {
                    let drained = store.drain_queue(&workspace, drain_budget())?;
                    assert_eq!(drained.queue_jobs_processed, 1);
                }
                Ok(())
            },
        )
        .expect("rebuild while a second connection commits");

    assert!(snapshot_hook_ran.get());
    assert_eq!(
        result.canonical_rows_scanned, 1,
        "all snapshot reads must retain the pre-writer view"
    );
    assert_eq!(result.fts_rows_rebuilt, 0);
    assert_eq!(canonical_value_count_for_test(&layout, &workspace), 2);
    assert_eq!(fts_value_count_for_test(&layout, &workspace), 2);
}

#[test]
fn rebuild_rechecks_canonical_and_fts_rows_after_the_bounded_scan() {
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
            &test_job_with_stable_key("scope", "issues", "Old value", "stable-key"),
            generation,
        )
        .expect("enqueue old value");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("project old value");
    clear_observed_fts_for_test(&layout, &workspace);
    store
        .enqueue_if_current(
            &workspace,
            &test_job_with_stable_key("scope", "issues", "Fresh value", "stable-key"),
            generation,
        )
        .expect("enqueue concurrent refresh");

    let hook_ran = Cell::new(false);
    let result = store
        .rebuild_fts_with_limits_guard_and_hook(
            &workspace,
            &test_policy(&[("scope", "issues")]),
            1,
            usize::MAX,
            |_| Ok(false),
            |phase| {
                if phase == ObservedFtsRebuildPhase::CanonicalReconciliation
                    && !hook_ran.replace(true)
                {
                    let drained = store.drain_queue(&workspace, drain_budget())?;
                    assert_eq!(drained.queue_jobs_processed, 1);
                }
                Ok(())
            },
        )
        .expect("rebuild around concurrent projection");

    assert!(hook_ran.get());
    assert_eq!(result.fts_rows_rebuilt, 0);
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let aligned: (i64, String, String) = connection
        .query_row(
            "
            SELECT COUNT(*), MIN(v.display_value), MIN(f.display_value)
            FROM observed_values v
            JOIN observed_values_fts f ON f.rowid = v.rowid
            WHERE v.workspace = ?1 AND v.value_key = 'stable-key'
            ",
            params![workspace.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("concurrently refreshed aligned row");
    assert_eq!(
        aligned,
        (1, "Fresh value".to_string(), "Fresh value".to_string())
    );
}

#[test]
fn rebuild_converges_a_healthy_legacy_rowid_collision_without_duplicates_or_loss() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    for (scope, display_value, value_key) in [
        ("scope-a", "Value A", "key-a"),
        ("scope-c", "Value C", "key-c"),
    ] {
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with_stable_key(scope, "issues", display_value, value_key),
                generation,
            )
            .expect("enqueue value");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("project value");
    }

    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let rowid_a: i64 = connection
        .query_row(
            "SELECT rowid FROM observed_values WHERE value_key = 'key-a'",
            [],
            |row| row.get(0),
        )
        .expect("A rowid");
    let rowid_c: i64 = connection
        .query_row(
            "SELECT rowid FROM observed_values WHERE value_key = 'key-c'",
            [],
            |row| row.get(0),
        )
        .expect("C rowid");
    assert!(
        rowid_a < rowid_c,
        "A must reconcile before its legacy target"
    );
    connection
        .execute(
            "DELETE FROM observed_values_fts WHERE rowid IN (?1, ?2)",
            params![rowid_a, rowid_c],
        )
        .expect("remove aligned rows");
    connection
        .execute(
            "
            INSERT INTO observed_values_fts (
                rowid, workspace, source_name, source_scope_id,
                surface_kind, surface_name, column_name, value_key, display_value, search_text
            )
            SELECT ?2, workspace, source_name, source_scope_id,
                   surface_kind, surface_name, column_name, value_key, display_value, search_text
            FROM observed_values
            WHERE rowid = ?1
            ",
            params![rowid_a, rowid_c],
        )
        .expect("place the only A FTS row at C's canonical rowid");
    drop(connection);

    let policy = test_policy(&[("scope-a", "issues"), ("scope-c", "issues")]);
    let result = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 1, usize::MAX, |_| Ok(false))
        .expect("resolve healthy rowid collision");
    assert_eq!(result.fts_rows_rebuilt, 2);

    let connection = backing.connect_for_test().expect("connection");
    let mut statement = connection
        .prepare(
            "
            SELECT f.value_key, f.rowid, COUNT(*)
            FROM observed_values_fts f
            WHERE f.workspace = ?1 AND f.value_key IN ('key-a', 'key-c')
            GROUP BY f.value_key, f.rowid
            ORDER BY f.value_key
            ",
        )
        .expect("prepare collision result");
    let rows = statement
        .query_map(params![workspace.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .expect("query collision result")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect collision result");
    assert_eq!(
        rows,
        [
            ("key-a".to_string(), rowid_a, 1),
            ("key-c".to_string(), rowid_c, 1),
        ]
    );
}

#[test]
fn rebuild_breaks_a_healthy_two_row_legacy_collision_cycle() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    for (scope, display_value, value_key) in [
        ("scope-a", "Value A", "key-a"),
        ("scope-b", "Value B", "key-b"),
    ] {
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with_stable_key(scope, "issues", display_value, value_key),
                generation,
            )
            .expect("enqueue value");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("project value");
    }

    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let rowid_a: i64 = connection
        .query_row(
            "SELECT rowid FROM observed_values WHERE value_key = 'key-a'",
            [],
            |row| row.get(0),
        )
        .expect("A rowid");
    let rowid_b: i64 = connection
        .query_row(
            "SELECT rowid FROM observed_values WHERE value_key = 'key-b'",
            [],
            |row| row.get(0),
        )
        .expect("B rowid");
    connection
        .execute(
            "DELETE FROM observed_values_fts WHERE rowid IN (?1, ?2)",
            params![rowid_a, rowid_b],
        )
        .expect("remove aligned rows");
    connection
        .execute(
            "
            INSERT INTO observed_values_fts (
                rowid, workspace, source_name, source_scope_id,
                surface_kind, surface_name, column_name, value_key, display_value, search_text
            )
            SELECT
                CASE value_key WHEN 'key-a' THEN ?2 ELSE ?1 END,
                workspace, source_name, source_scope_id,
                surface_kind, surface_name, column_name, value_key, display_value, search_text
            FROM observed_values
            WHERE rowid IN (?1, ?2)
            ",
            params![rowid_a, rowid_b],
        )
        .expect("swap the two legacy FTS rowids");
    let extra_legacy_rowid = rowid_b.saturating_add(100);
    connection
        .execute(
            "
            INSERT INTO observed_values_fts (
                rowid, workspace, source_name, source_scope_id,
                surface_kind, surface_name, column_name, value_key, display_value, search_text
            )
            SELECT ?2, workspace, source_name, source_scope_id,
                   surface_kind, surface_name, column_name, value_key, display_value, search_text
            FROM observed_values
            WHERE rowid = ?1
            ",
            params![rowid_a, extra_legacy_rowid],
        )
        .expect("add a second unaligned A row outside the collision cycle");
    drop(connection);

    let policy = test_policy(&[("scope-a", "issues"), ("scope-b", "issues")]);
    let result = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 1, usize::MAX, |_| Ok(false))
        .expect("break the rowid cycle and remove its deferred duplicate");
    assert_eq!(result.fts_rows_rebuilt, 2);

    let connection = backing.connect_for_test().expect("connection");
    let counts: (i64, i64) = connection
        .query_row(
            "
            SELECT
                COUNT(*),
                SUM(CASE WHEN f.rowid = v.rowid AND f.value_key = v.value_key THEN 1 ELSE 0 END)
            FROM observed_values_fts f
            JOIN observed_values v ON v.value_key = f.value_key
            WHERE f.workspace = ?1 AND f.value_key IN ('key-a', 'key-b')
            ",
            params![workspace.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("cycle repair counts");
    assert_eq!(counts, (2, 2));
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
#[expect(
    clippy::too_many_lines,
    reason = "the corruption matrix is clearer when its setup and final exact-row assertion remain together"
)]
fn rebuild_fts_reconciles_corrupt_rows_in_bounded_commits() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    for (scope, value) in [
        ("scope-missing", "Invoice missing"),
        ("scope-duplicate", "Invoice duplicate"),
        ("scope-stale", "Invoice current"),
        ("scope-removed", "Invoice removed"),
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

    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "DELETE FROM observed_values_fts WHERE workspace = ?1 AND value_key = 'invoice-missing'",
            params![workspace.as_str()],
        )
        .expect("remove one FTS row");
    connection
        .execute(
            "
            INSERT INTO observed_values_fts (
                workspace,
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
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                column_name,
                value_key,
                display_value,
                search_text
            FROM observed_values_fts
            WHERE workspace = ?1 AND value_key = 'invoice-duplicate'
            ",
            params![workspace.as_str()],
        )
        .expect("duplicate one FTS row");
    connection
        .execute(
            "
            UPDATE observed_values_fts
            SET display_value = 'Invoice obsolete',
                search_text = 'invoice obsolete'
            WHERE workspace = ?1 AND value_key = 'invoice-current'
            ",
            params![workspace.as_str()],
        )
        .expect("make one FTS row stale");
    connection
        .execute(
            "
            INSERT INTO observed_values_fts (
                workspace,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                column_name,
                value_key,
                display_value,
                search_text
            ) VALUES (?1, 'github', 'scope-orphan', 'table', 'issues', 'title',
                'invoice-orphan', 'Invoice orphan', 'invoice orphan')
            ",
            params![workspace.as_str()],
        )
        .expect("insert orphan FTS row");
    drop(connection);

    let commit_count = Cell::new(0_u32);
    let policy = test_policy(&[
        ("scope-missing", "issues"),
        ("scope-duplicate", "issues"),
        ("scope-stale", "issues"),
    ]);
    let result = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 2, usize::MAX, |_| {
            commit_count.set(commit_count.get().saturating_add(1));
            Ok(false)
        })
        .expect("reconcile FTS");

    assert_eq!(result.canonical_rows_scanned, 3);
    assert_eq!(result.fts_rows_rebuilt, 2);
    assert_eq!(canonical_value_count_for_test(&layout, &workspace), 3);
    assert_eq!(fts_value_count_for_test(&layout, &workspace), 3);

    let connection = backing.connect_for_test().expect("connection");
    let reconciled = observed_fts_rows_for_test(&connection, &workspace);
    assert_eq!(
        reconciled,
        [
            (
                "invoice-current".to_string(),
                "Invoice current".to_string(),
                "invoice current".to_string(),
            ),
            (
                "invoice-duplicate".to_string(),
                "Invoice duplicate".to_string(),
                "invoice duplicate".to_string(),
            ),
            (
                "invoice-missing".to_string(),
                "Invoice missing".to_string(),
                "invoice missing".to_string(),
            ),
        ]
    );
}

#[test]
fn rebuild_fts_bounds_each_commit_by_payload_bytes() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    let scopes = ["scope-0", "scope-1", "scope-2"];
    for (index, scope) in scopes.iter().enumerate() {
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with(scope, "issues", &format!("Payload value {index}")),
                generation,
            )
            .expect("enqueue");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");
    }
    clear_observed_fts_for_test(&layout, &workspace);

    let commit_count = Cell::new(0_u32);
    let policy = test_policy(&scopes.map(|scope| (scope, "issues")));
    let result = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 10, 100, |_| {
            commit_count.set(commit_count.get().saturating_add(1));
            Ok(false)
        })
        .expect("rebuild payload-bounded FTS batches");

    assert_eq!(result.fts_rows_rebuilt, 3);
    assert_eq!(
        commit_count.get(),
        3,
        "two canonical payloads exceed the 100-byte transaction budget"
    );
}

#[test]
fn rebuild_fts_rolls_back_guarded_batch_and_resumes_on_rerun() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    for (scope, value) in [
        ("scope-one", "Payment old one"),
        ("scope-two", "Payment old two"),
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
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "
            UPDATE observed_values
            SET display_value = REPLACE(display_value, 'old', 'fresh'),
                search_text = REPLACE(search_text, 'old', 'fresh')
            WHERE workspace = ?1
            ",
            params![workspace.as_str()],
        )
        .expect("change canonical payloads without refreshing FTS");
    drop(connection);

    let policy = test_policy(&[("scope-one", "issues"), ("scope-two", "issues")]);
    let guard_calls = Cell::new(0_u32);
    let error = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 1, usize::MAX, |connection| {
            guard_calls.set(guard_calls.get().saturating_add(1));
            let fresh_rows: i64 = connection.query_row(
                "SELECT COUNT(*) FROM observed_values_fts WHERE workspace = ?1 AND search_text LIKE '%fresh%'",
                params![workspace.as_str()],
                |row| row.get(0),
            )?;
            assert_eq!(fresh_rows, 1, "guard runs after the first batch is staged");
            Ok(true)
        })
        .expect_err("storage guard should abort the first batch");

    assert!(error.is_storage_exhaustion());
    assert_eq!(guard_calls.get(), 1);
    let old_hits = store
        .search(&workspace, &[String::from("old")], 10, &policy)
        .expect("search prior FTS projection after rollback");
    assert_eq!(
        old_hits.hits.len(),
        2,
        "rolled-back and untouched keys remain searchable"
    );
    assert!(
        store
            .search(&workspace, &[String::from("fresh")], 10, &policy)
            .expect("search rolled-back payload")
            .hits
            .is_empty()
    );

    let resumed_commits = Cell::new(0_u32);
    let resumed = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 1, usize::MAX, |_| {
            resumed_commits.set(resumed_commits.get().saturating_add(1));
            Ok(false)
        })
        .expect("resume rebuild");
    assert_eq!(resumed.fts_rows_rebuilt, 2);
    assert_eq!(resumed_commits.get(), 2);
    assert_eq!(
        store
            .search(&workspace, &[String::from("fresh")], 10, &policy)
            .expect("search rebuilt payloads")
            .hits
            .len(),
        2
    );
}

#[test]
fn rebuild_fts_stops_before_wal_batches_accumulate_behind_a_reader() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    let scopes = ["scope-one", "scope-two", "scope-three"];
    for (index, scope) in scopes.iter().enumerate() {
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with(scope, "issues", &format!("Pinned WAL {index}")),
                generation,
            )
            .expect("enqueue");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");
    }
    clear_observed_fts_for_test(&layout, &workspace);

    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let reader = backing.connect_for_test().expect("reader");
    let reader_started = Cell::new(false);
    let policy = test_policy(&scopes.map(|scope| (scope, "issues")));
    let error = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 1, usize::MAX, |_| {
            if !reader_started.replace(true) {
                reader.execute_batch("BEGIN").expect("begin reader");
                let _: i64 = reader
                    .query_row("SELECT COUNT(*) FROM observed_values_fts", [], |row| {
                        row.get(0)
                    })
                    .expect("pin reader snapshot");
            }
            Ok(false)
        })
        .expect_err("the second WAL batch must wait for the pinned reader");

    assert!(error.is_storage_exhaustion());
    assert!(
        error
            .to_string()
            .contains("cannot reclaim the prior WAL batch")
    );
    assert_eq!(fts_value_count_for_test(&layout, &workspace), 1);
    reader.execute_batch("ROLLBACK").expect("release reader");

    let resumed = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 1, usize::MAX, |_| Ok(false))
        .expect("resume after releasing reader");
    assert_eq!(resumed.fts_rows_rebuilt, 2);
    assert_eq!(fts_value_count_for_test(&layout, &workspace), 3);
}

#[test]
fn rebuild_fts_repairs_non_text_derived_keys_by_rowid() {
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
            &test_job_with("scope", "issues", "Typed value"),
            generation,
        )
        .expect("enqueue");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain queue");

    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "DELETE FROM observed_values_fts WHERE workspace = ?1",
            params![workspace.as_str()],
        )
        .expect("remove valid FTS row");
    connection
        .execute_batch(&format!(
            "
            INSERT INTO observed_values_fts VALUES (
                '{}', NULL, 'scope-null', 'table', 'issues', 'title',
                'null-key', 'Null key', 'null key'
            );
            INSERT INTO observed_values_fts VALUES (
                '{}', X'676974687562', 'scope-blob', 'table', 'issues', 'title',
                'blob-key', 'Blob key', 'blob key'
            );
            INSERT INTO observed_values_fts VALUES (
                '{}', 7, 'scope-integer', 'table', 'issues', 'title',
                'integer-key', 'Integer key', 'integer key'
            );
            ",
            workspace.as_str(),
            workspace.as_str(),
            workspace.as_str(),
        ))
        .expect("insert malformed derived rows");
    drop(connection);

    let result = store
        .rebuild_fts(&workspace, &test_policy(&[("scope", "issues")]))
        .expect("repair malformed FTS rows");

    assert_eq!(result.fts_rows_rebuilt, 1);
    assert_eq!(fts_value_count_for_test(&layout, &workspace), 1);
    let connection = backing.connect_for_test().expect("connection");
    let non_text_keys: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM observed_values_fts WHERE typeof(source_name) <> 'text'",
            [],
            |row| row.get(0),
        )
        .expect("non-text key count");
    assert_eq!(non_text_keys, 0);
}

#[test]
fn rebuild_fts_keyset_covers_the_entire_sqlite_rowid_domain() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    let rows = [
        (i64::MIN, "scope-min", "key-min", "Minimum rowid"),
        (-1, "scope-negative", "key-negative", "Negative rowid"),
        (0, "scope-zero", "key-zero", "Zero rowid"),
        (i64::MAX, "scope-max", "key-max", "Maximum rowid"),
    ];
    let malformed_source_names = [
        Value::Null,
        Value::Blob(b"github".to_vec()),
        Value::Integer(7),
        Value::Real(7.0),
    ];
    for ((rowid, scope, value_key, display_value), malformed_source_name) in
        rows.iter().zip(&malformed_source_names)
    {
        connection
            .execute(
                "
                INSERT INTO observed_values (
                    rowid, workspace, source_name, source_scope_id,
                    surface_kind, surface_name, column_name, value_key, display_value,
                    search_text, first_observed_at, last_observed_at, observation_count,
                    source_generation, workspace_generation
                ) VALUES (
                    ?1, ?2, 'github', ?3, 'table', 'issues', 'title', ?4, ?5,
                    ?6, '2020-01-01T00:00:00.000Z', '9999-01-01T00:00:00.000Z', 1, 0, 0
                )
                ",
                params![
                    rowid,
                    workspace.as_str(),
                    scope,
                    value_key,
                    display_value,
                    display_value.to_ascii_lowercase(),
                ],
            )
            .expect("insert canonical extreme-rowid row");
        connection
            .execute(
                "
                INSERT INTO observed_values_fts (
                    rowid, workspace, source_name, source_scope_id,
                    surface_kind, surface_name, column_name, value_key, display_value, search_text
                ) VALUES (
                    ?1, ?2, ?3, ?4, 'table', 'issues', 'title', ?5, ?6, ?7
                )
                ",
                params![
                    rowid,
                    workspace.as_str(),
                    malformed_source_name,
                    scope,
                    value_key,
                    display_value,
                    display_value.to_ascii_lowercase(),
                ],
            )
            .expect("insert malformed extreme-rowid FTS row");
    }
    drop(connection);

    let policy = ObservedValuesRetrievalPolicy::new(
        rows.iter()
            .map(|(_, scope, _, _)| test_live_scope_for_source("github", scope, "issues"))
            .collect(),
        365,
    );
    let result = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 1, usize::MAX, |_| Ok(false))
        .expect("rebuild every SQLite rowid range");
    assert_eq!(result.fts_rows_rebuilt, 4);

    let connection = backing.connect_for_test().expect("connection");
    let mut statement = connection
        .prepare(
            "
            SELECT f.rowid
            FROM observed_values_fts f
            JOIN observed_values v ON v.rowid = f.rowid
            WHERE f.workspace = ?1
              AND f.source_name = v.source_name
              AND f.value_key = v.value_key
              AND f.display_value = v.display_value
            ORDER BY f.rowid
            ",
        )
        .expect("prepare aligned rowid query");
    let aligned_rowids = statement
        .query_map(params![workspace.as_str()], |row| row.get(0))
        .expect("query aligned rowids")
        .collect::<Result<Vec<i64>, _>>()
        .expect("collect aligned rowids");
    assert_eq!(aligned_rowids, [i64::MIN, -1, 0, i64::MAX]);
}

#[test]
fn rebuild_fts_allows_non_growing_cleanup_above_the_storage_limit() {
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
            &test_job_with("removed-scope", "issues", "Cleanup value"),
            generation,
        )
        .expect("enqueue");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain queue");
    clear_observed_fts_for_test(&layout, &workspace);

    let guard_calls = Cell::new(0_u32);
    let result = store
        .rebuild_fts_with_limits_and_guard(&workspace, &test_policy(&[]), 1, usize::MAX, |_| {
            guard_calls.set(guard_calls.get().saturating_add(1));
            Ok(true)
        })
        .expect("non-growing cleanup should proceed above the threshold");

    assert_eq!(result.fts_rows_rebuilt, 0);
    assert_eq!(guard_calls.get(), 0);
    assert_eq!(canonical_value_count_for_test(&layout, &workspace), 0);
}

#[test]
fn rebuild_fts_reuses_one_retention_cutoff_across_batches() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::default();
    let store = SqliteObservedValuesStore::new(layout.clone());
    let generation = store
        .capture_epoch(&workspace, "github")
        .expect("generation");
    let scopes = ["scope-one", "scope-two"];
    for (index, scope) in scopes.iter().enumerate() {
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with(scope, "issues", &format!("Boundary {index}")),
                generation,
            )
            .expect("enqueue");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");
    }
    let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "
            UPDATE observed_values
            SET last_observed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 day', '+0.5 seconds')
            WHERE workspace = ?1
            ",
            params![workspace.as_str()],
        )
        .expect("place rows just inside the retention boundary");
    drop(connection);
    clear_observed_fts_for_test(&layout, &workspace);

    let policy = ObservedValuesRetrievalPolicy::new(
        scopes
            .iter()
            .map(|scope| test_live_scope_for_source("github", scope, "issues"))
            .collect(),
        1,
    );
    let guard_calls = Cell::new(0_u32);
    let result = store
        .rebuild_fts_with_limits_and_guard(&workspace, &policy, 1, usize::MAX, |_| {
            if guard_calls.replace(guard_calls.get().saturating_add(1)) == 0 {
                thread::sleep(Duration::from_millis(750));
            }
            Ok(false)
        })
        .expect("rebuild with one retained cutoff");

    assert_eq!(result.fts_rows_rebuilt, 2);
    assert_eq!(canonical_value_count_for_test(&layout, &workspace), 2);
    assert_eq!(fts_value_count_for_test(&layout, &workspace), 2);
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
    assert_eq!(result.fts_rows_rebuilt, 0);
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
            &test_job_for_source("github", "live-scope", "issues", "Payment current"),
            github_generation,
        )
        .expect("enqueue github");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain github");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_for_source("github", "removed-scope", "issues", "Payment removed"),
            github_generation,
        )
        .expect("enqueue removed github scope");
    store
        .drain_queue(&workspace, drain_budget())
        .expect("drain removed github scope");
    store
        .enqueue_if_current(
            &workspace,
            &test_job_for_source("jira", "unknown-scope", "issues", "Payment blocked"),
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
    assert_eq!(result.fts_rows_rebuilt, 0);
    assert_eq!(canonical_value_count_for_test(&layout, &workspace), 2);
    assert_eq!(fts_value_count_for_test(&layout, &workspace), 2);

    let failed_policy = test_policy_with_failed_sources(&[("live-scope", "issues")], &["jira"]);
    let blocked_while_failed = store
        .search(&workspace, &[String::from("blocked")], 10, &failed_policy)
        .expect("search with failed Jira policy");
    assert!(blocked_while_failed.hits.is_empty());

    let recovered_policy = ObservedValuesRetrievalPolicy::new(
        vec![
            test_live_scope_for_source("github", "live-scope", "issues"),
            test_live_scope_for_source("jira", "unknown-scope", "issues"),
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
    test_job_for_source("github", source_scope_id, surface_name, display_value)
}

fn test_job_with_stable_key(
    source_scope_id: &str,
    surface_name: &str,
    display_value: &str,
    value_key: &str,
) -> ObservedValuesQueueJob {
    let mut job = test_job_with(source_scope_id, surface_name, display_value);
    job.payload_json = format!(
        r#"{{"values":[{{"column_name":"title","display_value":"{display_value}","search_text":"{}","value_key":"{value_key}"}}]}}"#,
        display_value.to_ascii_lowercase()
    );
    job
}

fn test_job_for_source(
    source_name: &str,
    source_scope_id: &str,
    surface_name: &str,
    display_value: &str,
) -> ObservedValuesQueueJob {
    ObservedValuesQueueJob {
        source_name: source_name.to_string(),
        source_scope_id: source_scope_id.to_string(),
        surface_kind: ObservedValuesSurfaceKind::Table,
        surface_name: surface_name.to_string(),
        payload_json: payload_json(display_value),
    }
}

/// Two independent sources observing the same value on the same surface name.
fn test_job_for_shared_value(source_name: &str) -> ObservedValuesQueueJob {
    ObservedValuesQueueJob {
        source_name: source_name.to_string(),
        source_scope_id: "shared-scope".to_string(),
        surface_kind: ObservedValuesSurfaceKind::Table,
        surface_name: "issues".to_string(),
        payload_json: payload_json("Shared value"),
    }
}

fn enqueue_test_jobs(
    store: &SqliteObservedValuesStore,
    workspace: &WorkspaceName,
    jobs: &[ObservedValuesQueueJob],
) {
    for job in jobs {
        let generation = store
            .capture_epoch(workspace, &job.source_name)
            .expect("generation");
        assert!(matches!(
            store
                .enqueue_if_current(workspace, job, generation)
                .expect("enqueue observation"),
            ObservedValuesEnqueueResult::Enqueued { .. }
        ));
    }
}

fn independent_source_clear_jobs() -> [ObservedValuesQueueJob; 3] {
    [
        test_job_for_source("github_v4", "rest-scope", "issues", "REST payment issue"),
        test_job_for_source("github_mcp_v4", "mcp-scope", "pulls", "MCP payment issue"),
        test_job_for_source("jira_v4", "jira-scope", "issues", "Jira payment issue"),
    ]
}

fn seed_projected_and_pending_jobs(
    layout: &AppStateLayout,
    workspace: &WorkspaceName,
    store: &SqliteObservedValuesStore,
    jobs: &[ObservedValuesQueueJob; 3],
) {
    enqueue_test_jobs(store, workspace, &jobs[..2]);
    store
        .drain_queue(workspace, drain_budget())
        .expect("project github observations");
    enqueue_test_jobs(store, workspace, &jobs[2..]);
    store
        .drain_queue(workspace, drain_budget())
        .expect("project jira observation");

    enqueue_test_jobs(store, workspace, &jobs[..2]);
    // Unit tests lower the queue cap to two, so seed the other owner directly.
    insert_queue_job_for_test(layout, workspace, store, &jobs[2]);
}

fn insert_queue_job_for_test(
    layout: &AppStateLayout,
    workspace: &WorkspaceName,
    store: &SqliteObservedValuesStore,
    job: &ObservedValuesQueueJob,
) {
    let generation = store
        .capture_epoch(workspace, &job.source_name)
        .expect("generation");
    let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
    let connection = backing.connect_for_test().expect("connection");
    connection
        .execute(
            "
            INSERT INTO observed_queue_jobs (
                workspace,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                workspace_generation,
                source_generation,
                payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ",
            params![
                workspace.as_str(),
                &job.source_name,
                &job.source_scope_id,
                job.surface_kind.as_str(),
                &job.surface_name,
                generation.workspace_generation,
                generation.source_generation,
                &job.payload_json,
            ],
        )
        .expect("seed pending queue job");
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

/// Leaves the FTS index full of deleted-but-unmerged rows on the current
/// schema. The version-5 migration rebuilds FTS from the canonical table, so
/// tombstones that matter now are the ones ordinary deletes and evictions
/// produce after the upgrade -- which is what these tests exercise.
fn seed_fts_tombstones(database_path: &Path) {
    SqliteSearchStore::open(database_path, WorkspaceName::default())
        .expect("create current-schema database");
    let connection = Connection::open(database_path).expect("raw connection");
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
                'default', 'github', 'legacy', 'table', 'issues', 'title',
                printf('legacy-%d', id), hex(randomblob(256)), hex(randomblob(256)),
                '2020-01-01T00:00:00.000Z', '2020-01-01T00:00:00.000Z', 1, 0, 0
            FROM rows;

            INSERT INTO observed_values_fts (
                workspace,
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
        .expect("create FTS tombstones");
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
            .map(|source_name| ObservedValuesLiveScopeLoadFailure {
                source_name: (*source_name).to_string(),
                message: "failed to load".to_string(),
            })
            .collect(),
        365,
    )
}

fn test_live_scopes(scopes: &[(&str, &str)]) -> Vec<ObservedValuesLiveScope> {
    scopes
        .iter()
        .map(|(scope, surface)| test_live_scope_for_source("github", scope, surface))
        .collect()
}

fn test_live_scope_for_source(
    source_name: &str,
    source_scope_id: &str,
    surface_name: &str,
) -> ObservedValuesLiveScope {
    ObservedValuesLiveScope {
        source_name: source_name.to_string(),
        catalog_name: None,
        schema_name: source_name.to_string(),
        source_scope_id: source_scope_id.to_string(),
        surface_kind: ObservedValuesSurfaceKind::Table,
        surface_name: surface_name.to_string(),
    }
}

fn drain_budget() -> ObservedValuesDrainBudget {
    ObservedValuesDrainBudget::new(10, Duration::from_secs(1))
}

fn projected_source_names(
    connection: &rusqlite::Connection,
    table_name: &str,
    workspace: &WorkspaceName,
) -> Vec<String> {
    let sql =
        format!("SELECT source_name FROM {table_name} WHERE workspace = ?1 ORDER BY source_name");
    let mut statement = connection.prepare(&sql).expect("source query");
    let rows = statement
        .query_map(params![workspace.as_str()], |row| row.get(0))
        .expect("query source rows");
    rows.collect::<Result<Vec<_>, _>>()
        .expect("collect source rows")
}

fn assert_projected_source_names(
    connection: &rusqlite::Connection,
    workspace: &WorkspaceName,
    expected: &[&str],
) {
    for table_name in [
        "observed_values",
        "observed_values_fts",
        "observed_queue_jobs",
    ] {
        assert_eq!(
            projected_source_names(connection, table_name, workspace),
            expected,
            "unexpected sources in {table_name}"
        );
    }
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

fn observed_fts_rows_for_test(
    connection: &rusqlite::Connection,
    workspace: &WorkspaceName,
) -> Vec<(String, String, String)> {
    let mut statement = connection
        .prepare(
            "
            SELECT value_key, display_value, search_text
            FROM observed_values_fts
            WHERE workspace = ?1
            ORDER BY value_key
            ",
        )
        .expect("prepare FTS row query");
    statement
        .query_map(params![workspace.as_str()], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("query FTS rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect FTS rows")
}
