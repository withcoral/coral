//! Transactional persistence for workspace-scoped Search response history.

use std::collections::VecDeque;
use std::fmt;

use super::clock::TraceSearchResponseRetentionBounds;
use super::repositories::trace_search_responses::TraceSearchResponseRow;
use super::{CoralDb, DbError, DbRepos};

const TRACE_SEARCH_RESPONSE_PRUNE_BATCH_ROWS: u64 = 32;

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum TraceSearchResponseOutcome {
    Response(Vec<u8>),
    TooLarge { bytes: i64 },
}

impl fmt::Debug for TraceSearchResponseOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response(response_proto) => formatter
                .debug_struct("Response")
                .field("bytes", &response_proto.len())
                .finish(),
            Self::TooLarge { bytes } => formatter
                .debug_struct("TooLarge")
                .field("bytes", bytes)
                .finish(),
        }
    }
}

impl TryFrom<TraceSearchResponseRow> for TraceSearchResponseOutcome {
    type Error = DbError;

    fn try_from(row: TraceSearchResponseRow) -> Result<Self, Self::Error> {
        match (row.response_proto, row.oversized_bytes) {
            (Some(response_proto), None) => Ok(Self::Response(response_proto)),
            (None, Some(bytes)) => Ok(Self::TooLarge { bytes }),
            (Some(_), Some(_)) | (None, None) => Err(DbError::CorruptData(
                "trace_search_responses row violates its outcome constraint".to_string(),
            )),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct TraceSearchResponseCapture {
    pub(crate) workspace_id: String,
    pub(crate) trace_id: String,
    pub(crate) search_span_id: String,
    pub(crate) recorded_at_unix_nanos: i64,
    pub(crate) outcome: TraceSearchResponseOutcome,
}

impl fmt::Debug for TraceSearchResponseCapture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TraceSearchResponseCapture")
            .field("workspace_id", &self.workspace_id)
            .field("trace_id", &self.trace_id)
            .field("search_span_id", &self.search_span_id)
            .field("recorded_at_unix_nanos", &self.recorded_at_unix_nanos)
            .field("outcome", &self.outcome)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TraceSearchResponseInsertResult {
    Inserted,
    WorkspaceNotFound,
    AlreadyExists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceSearchResponsePruneBatchResult {
    WorkspaceMissing,
    Pruned(u64),
}

#[cfg(test)]
struct TraceSearchResponseMutationBarrier {
    workspace_held: tokio::sync::Barrier,
    release_mutation: tokio::sync::Barrier,
}

#[cfg(test)]
impl TraceSearchResponseMutationBarrier {
    fn new() -> Self {
        Self {
            workspace_held: tokio::sync::Barrier::new(2),
            release_mutation: tokio::sync::Barrier::new(2),
        }
    }

    async fn pause_after_workspace_hold(&self) {
        self.workspace_held.wait().await;
        self.release_mutation.wait().await;
    }

    async fn wait_until_workspace_held(&self) {
        self.workspace_held.wait().await;
    }

    async fn release_mutation(&self) {
        self.release_mutation.wait().await;
    }
}

impl CoralDb {
    pub(crate) async fn insert_trace_search_response(
        &self,
        capture: TraceSearchResponseCapture,
    ) -> Result<TraceSearchResponseInsertResult, DbError> {
        self.insert_trace_search_response_inner(
            capture,
            #[cfg(test)]
            None,
        )
        .await
    }

    async fn insert_trace_search_response_inner(
        &self,
        capture: TraceSearchResponseCapture,
        #[cfg(test)] mutation_barrier: Option<&TraceSearchResponseMutationBarrier>,
    ) -> Result<TraceSearchResponseInsertResult, DbError> {
        let TraceSearchResponseCapture {
            workspace_id,
            trace_id,
            search_span_id,
            recorded_at_unix_nanos,
            outcome,
        } = capture;
        let (response_proto, oversized_bytes) = match outcome {
            TraceSearchResponseOutcome::Response(response_proto) => (Some(response_proto), None),
            TraceSearchResponseOutcome::TooLarge { bytes } => (None, Some(bytes)),
        };
        let mut tx = self.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(&workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(TraceSearchResponseInsertResult::WorkspaceNotFound);
        }
        #[cfg(test)]
        if let Some(mutation_barrier) = mutation_barrier {
            mutation_barrier.pause_after_workspace_hold().await;
        }
        let inserted = tx
            .trace_search_responses()
            .insert_first_write_wins(
                &workspace_id,
                &trace_id,
                &search_span_id,
                recorded_at_unix_nanos,
                response_proto,
                oversized_bytes,
            )
            .await?;
        if inserted {
            tx.commit().await?;
            Ok(TraceSearchResponseInsertResult::Inserted)
        } else {
            tx.rollback().await?;
            Ok(TraceSearchResponseInsertResult::AlreadyExists)
        }
    }

    #[cfg(test)]
    async fn insert_trace_search_response_with_mutation_barrier(
        &self,
        capture: TraceSearchResponseCapture,
        mutation_barrier: &TraceSearchResponseMutationBarrier,
    ) -> Result<TraceSearchResponseInsertResult, DbError> {
        self.insert_trace_search_response_inner(capture, Some(mutation_barrier))
            .await
    }

    pub(crate) async fn get_trace_search_response(
        &self,
        workspace_id: &str,
        trace_id: &str,
        search_span_id: &str,
        retention_bounds: TraceSearchResponseRetentionBounds,
    ) -> Result<Option<TraceSearchResponseOutcome>, DbError> {
        let mut session = self;
        let row = session
            .trace_search_responses()
            .get(workspace_id, trace_id, search_span_id, retention_bounds)
            .await?;
        row.map(TraceSearchResponseOutcome::try_from).transpose()
    }

    pub(crate) async fn prune_trace_search_responses_outside_retention(
        &self,
        retention_bounds: TraceSearchResponseRetentionBounds,
    ) -> Result<u64, DbError> {
        let mut after_workspace_id = None;
        let mut candidate_scan_complete = false;
        let mut pending_workspace_ids = VecDeque::new();
        let mut deleted_rows = 0;

        loop {
            if !candidate_scan_complete {
                let next_workspace_id = {
                    let mut session = self;
                    session
                        .trace_search_responses()
                        .next_out_of_retention_workspace_id(
                            retention_bounds,
                            after_workspace_id.as_deref(),
                        )
                        .await?
                };
                if let Some(workspace_id) = next_workspace_id {
                    after_workspace_id = Some(workspace_id.clone());
                    pending_workspace_ids.push_back(workspace_id);
                } else {
                    candidate_scan_complete = true;
                }
            }

            let Some(workspace_id) = pending_workspace_ids.pop_front() else {
                break;
            };
            match self
                .prune_trace_search_responses_batch_inner(
                    &workspace_id,
                    retention_bounds,
                    #[cfg(test)]
                    None,
                )
                .await?
            {
                TraceSearchResponsePruneBatchResult::WorkspaceMissing => {}
                TraceSearchResponsePruneBatchResult::Pruned(deleted) => {
                    deleted_rows += deleted;
                    if deleted == TRACE_SEARCH_RESPONSE_PRUNE_BATCH_ROWS {
                        pending_workspace_ids.push_back(workspace_id);
                    }
                }
            }
        }

        Ok(deleted_rows)
    }

    async fn prune_trace_search_responses_batch_inner(
        &self,
        workspace_id: &str,
        retention_bounds: TraceSearchResponseRetentionBounds,
        #[cfg(test)] mutation_barrier: Option<&TraceSearchResponseMutationBarrier>,
    ) -> Result<TraceSearchResponsePruneBatchResult, DbError> {
        let mut tx = self.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(TraceSearchResponsePruneBatchResult::WorkspaceMissing);
        }
        #[cfg(test)]
        if let Some(mutation_barrier) = mutation_barrier {
            mutation_barrier.pause_after_workspace_hold().await;
        }
        let deleted = tx
            .trace_search_responses()
            .delete_out_of_retention_batch(
                workspace_id,
                retention_bounds,
                TRACE_SEARCH_RESPONSE_PRUNE_BATCH_ROWS,
            )
            .await?;
        if deleted == 0 {
            tx.rollback().await?;
        } else {
            tx.commit().await?;
        }
        Ok(TraceSearchResponsePruneBatchResult::Pruned(deleted))
    }

    #[cfg(test)]
    async fn prune_trace_search_responses_batch_with_mutation_barrier(
        &self,
        workspace_id: &str,
        retention_bounds: TraceSearchResponseRetentionBounds,
        mutation_barrier: &TraceSearchResponseMutationBarrier,
    ) -> Result<TraceSearchResponsePruneBatchResult, DbError> {
        self.prune_trace_search_responses_batch_inner(
            workspace_id,
            retention_bounds,
            Some(mutation_barrier),
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn drop_trace_search_responses_for_test(&self) -> Result<(), DbError> {
        use super::backend::CoralDbBackend;

        match &self.backend {
            CoralDbBackend::Sqlite(db) => {
                sqlx::query("DROP TABLE trace_search_responses")
                    .execute(&db.pool)
                    .await?;
            }
            CoralDbBackend::Postgres(db) => {
                sqlx::query("DROP TABLE trace_search_responses")
                    .execute(&db.pool)
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::tempdir;

    use super::{
        TraceSearchResponseCapture, TraceSearchResponseInsertResult,
        TraceSearchResponseMutationBarrier, TraceSearchResponseOutcome,
        TraceSearchResponsePruneBatchResult,
    };
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::clock::TraceSearchResponseRetentionBounds;
    use crate::state::db::repositories::trace_search_responses::TraceSearchResponseRow;
    use crate::state::db::{CoralDb, DatabaseConfig, DbError, DbRepos, ResolvedDatabaseConfig};

    #[tokio::test]
    async fn trace_search_response_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_trace_search_response_repository_round_trip(&db, "sqlite_trace_search_response")
            .await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn trace_search_response_repository_round_trips_against_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        assert_trace_search_response_repository_round_trip(
            &db,
            &format!("postgres_trace_search_response_{suffix}"),
        )
        .await;
    }

    #[tokio::test]
    async fn trace_search_response_pruning_batches_workspace_mutations() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;
        let workspace_id = "sqlite_prune_batches";
        let second_workspace_id = "sqlite_prune_second";
        ensure_workspace(&db, workspace_id, 1).await;
        ensure_workspace(&db, second_workspace_id, 1).await;
        for index in 0..17 {
            insert_response(
                &db,
                workspace_id,
                &format!("old-{index:02}"),
                i64::from(index),
            )
            .await;
        }
        for index in 0..16 {
            insert_response(
                &db,
                workspace_id,
                &format!("future-{index:02}"),
                67 + i64::from(index),
            )
            .await;
        }
        insert_response(&db, workspace_id, "oldest-boundary", 33).await;
        insert_response(&db, workspace_id, "newest-boundary", 66).await;
        insert_response(&db, second_workspace_id, "second-future", 67).await;
        let retention_bounds = TraceSearchResponseRetentionBounds {
            oldest_inclusive_unix_nanos: 33,
            newest_inclusive_unix_nanos: 66,
        };

        assert_eq!(
            db.prune_trace_search_responses_outside_retention(retention_bounds)
                .await
                .expect("prune out-of-retention response batches"),
            34
        );
        assert!(
            db.get_trace_search_response(workspace_id, "old-16", "prune-span", retention_bounds)
                .await
                .expect("read old response after pruning multiple batches")
                .is_none()
        );
        assert!(
            db.get_trace_search_response(
                second_workspace_id,
                "second-future",
                "prune-span",
                retention_bounds,
            )
            .await
            .expect("read future response from second pruned workspace")
            .is_none()
        );
        for trace_id in ["oldest-boundary", "newest-boundary"] {
            assert!(
                db.get_trace_search_response(
                    workspace_id,
                    trace_id,
                    "prune-span",
                    retention_bounds,
                )
                .await
                .expect("read inclusive retention boundary")
                .is_some()
            );
        }
    }

    #[test]
    fn trace_search_response_debug_output_redacts_payloads() {
        let payload = b"private-search-response".to_vec();
        let raw_payload_debug = format!("{payload:?}");
        let outcome = TraceSearchResponseOutcome::Response(payload);
        let capture = capture("workspace", "trace", "span", 1, outcome.clone());

        for debug_output in [format!("{capture:?}"), format!("{outcome:?}")] {
            assert!(!debug_output.contains("private-search-response"));
            assert!(!debug_output.contains(&raw_payload_debug));
            assert!(debug_output.contains("bytes: 23"));
        }
    }

    #[test]
    fn trace_search_response_outcome_rejects_inconsistent_rows() {
        for row in [
            TraceSearchResponseRow {
                response_proto: None,
                oversized_bytes: None,
            },
            TraceSearchResponseRow {
                response_proto: Some(vec![7]),
                oversized_bytes: Some(7),
            },
        ] {
            let error = TraceSearchResponseOutcome::try_from(row)
                .expect_err("reject inconsistent stored outcome");
            assert!(matches!(error, DbError::CorruptData(_)));
        }
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to verify prune/delete parent locking on Postgres"]
    async fn trace_search_response_repository_round_trips_against_postgres_with_prune_delete_locking()
     {
        let Some(postgres_url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres {
            url: postgres_url.clone(),
        })
        .await
        .expect("open postgres");
        db.migrate().await.expect("migrate postgres");
        let lock_limited_db = open_lock_limited_postgres(&postgres_url).await;
        let suffix = uuid::Uuid::new_v4().simple().to_string();

        assert_prune_holds_workspace_before_deletion(
            &db,
            &lock_limited_db,
            &format!("postgres_prune_delete_{suffix}"),
        )
        .await;
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    #[expect(
        clippy::too_many_lines,
        reason = "The shared SQLite/Postgres contract verifies opaque payloads, first-write-wins identity, retention, constraints, cascade, and deletion races together."
    )]
    async fn assert_trace_search_response_repository_round_trip(db: &CoralDb, workspace_id: &str) {
        let second_workspace_id = format!("{workspace_id}_second");
        let missing_workspace_id = format!("{workspace_id}_missing");
        ensure_workspace(db, workspace_id, 1).await;
        ensure_workspace(db, &second_workspace_id, 2).await;

        assert_eq!(
            db.insert_trace_search_response(capture(
                &missing_workspace_id,
                "missing-trace",
                "missing-span",
                3,
                TraceSearchResponseOutcome::Response(vec![1]),
            ))
            .await
            .expect("reject missing workspace"),
            TraceSearchResponseInsertResult::WorkspaceNotFound
        );

        let trace_id = format!("{workspace_id}_shared-trace");
        let search_span_id = format!("{workspace_id}_shared-search-span");
        assert_eq!(
            db.insert_trace_search_response(capture(
                workspace_id,
                &trace_id,
                &search_span_id,
                10,
                TraceSearchResponseOutcome::Response(Vec::new()),
            ))
            .await
            .expect("insert empty protobuf payload"),
            TraceSearchResponseInsertResult::Inserted
        );
        assert_eq!(
            db.get_trace_search_response(
                workspace_id,
                &trace_id,
                &search_span_id,
                retention_bounds_from(10),
            )
            .await
            .expect("read empty protobuf payload"),
            Some(TraceSearchResponseOutcome::Response(Vec::new()))
        );
        assert_eq!(
            db.get_trace_search_response(
                workspace_id,
                &trace_id,
                &search_span_id,
                retention_bounds_from(11),
            )
            .await
            .expect("apply read retention cutoff"),
            None
        );

        assert_eq!(
            db.insert_trace_search_response(capture(
                workspace_id,
                &trace_id,
                &search_span_id,
                11,
                TraceSearchResponseOutcome::TooLarge { bytes: 1_048_577 },
            ))
            .await
            .expect("keep first response"),
            TraceSearchResponseInsertResult::AlreadyExists
        );
        assert_eq!(
            db.get_trace_search_response(
                workspace_id,
                &trace_id,
                &search_span_id,
                retention_bounds_from(i64::MIN),
            )
            .await
            .expect("read first response after duplicate"),
            Some(TraceSearchResponseOutcome::Response(Vec::new()))
        );

        let collision_error = db
            .insert_trace_search_response(capture(
                &second_workspace_id,
                &trace_id,
                &search_span_id,
                12,
                TraceSearchResponseOutcome::TooLarge { bytes: 1_048_577 },
            ))
            .await
            .expect_err("reject the same Search span identity in another workspace");
        assert!(collision_error.is_unique_violation());

        let second_trace_id = format!("{workspace_id}_second-workspace-trace");
        let second_search_span_id = format!("{workspace_id}_second-workspace-search-span");
        assert_eq!(
            db.insert_trace_search_response(capture(
                &second_workspace_id,
                &second_trace_id,
                &second_search_span_id,
                12,
                TraceSearchResponseOutcome::TooLarge { bytes: 1_048_577 },
            ))
            .await
            .expect("insert distinct Search identity in another workspace"),
            TraceSearchResponseInsertResult::Inserted
        );
        assert_eq!(
            db.get_trace_search_response(
                &second_workspace_id,
                &second_trace_id,
                &second_search_span_id,
                retention_bounds_from(i64::MIN),
            )
            .await
            .expect("read workspace-isolated oversized response"),
            Some(TraceSearchResponseOutcome::TooLarge { bytes: 1_048_577 })
        );

        for (suffix, response_proto, oversized_bytes) in
            [("neither", None, None), ("both", Some(vec![7]), Some(7))]
        {
            let mut tx = db.begin().await.expect("begin invalid outcome transaction");
            tx.trace_search_responses()
                .insert_first_write_wins(
                    workspace_id,
                    &format!("invalid-{suffix}"),
                    "invalid-span",
                    13,
                    response_proto,
                    oversized_bytes,
                )
                .await
                .expect_err("database must enforce exactly one stored outcome");
            tx.rollback()
                .await
                .expect("roll back invalid outcome transaction");
        }

        let retention_workspace_id = format!("{workspace_id}_retention");
        let retention_span_id = format!("{workspace_id}_retention-span");
        ensure_workspace(db, &retention_workspace_id, 20).await;
        let expired_trace_id = format!("{workspace_id}_expired");
        let retained_trace_id = format!("{workspace_id}_retained");
        for (trace_id, recorded_at_unix_nanos) in [
            (expired_trace_id.as_str(), i64::MIN),
            (retained_trace_id.as_str(), i64::MIN + 1),
        ] {
            assert_eq!(
                db.insert_trace_search_response(capture(
                    &retention_workspace_id,
                    trace_id,
                    &retention_span_id,
                    recorded_at_unix_nanos,
                    TraceSearchResponseOutcome::Response(vec![2]),
                ))
                .await
                .expect("insert retention response"),
                TraceSearchResponseInsertResult::Inserted
            );
        }
        assert_eq!(
            db.prune_trace_search_responses_batch_inner(
                &retention_workspace_id,
                retention_bounds_from(i64::MIN + 1),
                None,
            )
            .await
            .expect("prune expired responses"),
            TraceSearchResponsePruneBatchResult::Pruned(1)
        );
        assert!(
            db.get_trace_search_response(
                &retention_workspace_id,
                &expired_trace_id,
                &retention_span_id,
                retention_bounds_from(i64::MIN),
            )
            .await
            .expect("read pruned response")
            .is_none()
        );
        assert!(
            db.get_trace_search_response(
                &retention_workspace_id,
                &retained_trace_id,
                &retention_span_id,
                retention_bounds_from(i64::MIN + 1),
            )
            .await
            .expect("read retained response")
            .is_some()
        );

        let cascade_workspace_id = format!("{workspace_id}_cascade");
        let cascade_trace_id = format!("{workspace_id}_cascade-trace");
        let cascade_span_id = format!("{workspace_id}_cascade-span");
        ensure_workspace(db, &cascade_workspace_id, 30).await;
        db.insert_trace_search_response(capture(
            &cascade_workspace_id,
            &cascade_trace_id,
            &cascade_span_id,
            31,
            TraceSearchResponseOutcome::Response(vec![3]),
        ))
        .await
        .expect("insert cascade response");
        db.begin_workspace_deletion(&cascade_workspace_id)
            .await
            .expect("begin cascade workspace deletion")
            .expect("cascade workspace exists")
            .commit()
            .await
            .expect("commit cascade workspace deletion");
        assert!(
            db.get_trace_search_response(
                &cascade_workspace_id,
                &cascade_trace_id,
                &cascade_span_id,
                retention_bounds_from(i64::MIN),
            )
            .await
            .expect("read cascaded response")
            .is_none()
        );

        assert_capture_serializes_with_workspace_delete(db, workspace_id).await;
        assert_workspace_delete_serializes_before_capture(db, workspace_id).await;

        for workspace_id in [
            workspace_id,
            second_workspace_id.as_str(),
            &retention_workspace_id,
        ] {
            delete_workspace(db, workspace_id).await;
        }
    }

    async fn assert_capture_serializes_with_workspace_delete(db: &CoralDb, prefix: &str) {
        let workspace_id = format!("{prefix}_capture_delete");
        let trace_id = format!("{workspace_id}_race-trace");
        let span_id = format!("{workspace_id}_race-span");
        ensure_workspace(db, &workspace_id, 40).await;
        let mutation_barrier = TraceSearchResponseMutationBarrier::new();
        let (capture_result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                db.insert_trace_search_response_with_mutation_barrier(
                    capture(
                        &workspace_id,
                        &trace_id,
                        &span_id,
                        41,
                        TraceSearchResponseOutcome::Response(vec![4]),
                    ),
                    &mutation_barrier,
                ),
                delete_workspace_after_capture_holds(db, &workspace_id, &mutation_barrier),
            )
        })
        .await
        .expect("capture/delete race should finish");
        assert_eq!(
            capture_result.expect("capture response racing workspace deletion"),
            TraceSearchResponseInsertResult::Inserted
        );
        assert!(
            db.get_trace_search_response(
                &workspace_id,
                &trace_id,
                &span_id,
                retention_bounds_from(i64::MIN),
            )
            .await
            .expect("read response after workspace deletion")
            .is_none(),
            "the deletion that follows the parent hold must cascade the captured response"
        );
    }

    async fn assert_workspace_delete_serializes_before_capture(db: &CoralDb, prefix: &str) {
        let workspace_id = format!("{prefix}_delete_capture");
        let trace_id = format!("{workspace_id}_delete-first-trace");
        let span_id = format!("{workspace_id}_delete-first-span");
        ensure_workspace(db, &workspace_id, 42).await;
        let deletion = db
            .begin_workspace_deletion(&workspace_id)
            .await
            .expect("begin workspace deletion before capture")
            .expect("workspace exists before deletion-first race");
        let capture = db.insert_trace_search_response(capture(
            &workspace_id,
            &trace_id,
            &span_id,
            43,
            TraceSearchResponseOutcome::Response(vec![5]),
        ));
        tokio::pin!(capture);

        assert!(
            tokio::time::timeout(Duration::from_millis(250), capture.as_mut())
                .await
                .is_err(),
            "response capture must wait while workspace deletion holds the parent"
        );
        deletion
            .commit()
            .await
            .expect("commit deletion before capture resumes");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), capture)
                .await
                .expect("capture should finish after workspace deletion")
                .expect("capture after workspace deletion"),
            TraceSearchResponseInsertResult::WorkspaceNotFound
        );
        assert!(
            db.get_trace_search_response(
                &workspace_id,
                &trace_id,
                &span_id,
                retention_bounds_from(i64::MIN),
            )
            .await
            .expect("read response after deletion-first race")
            .is_none(),
            "capture must not recreate a child after workspace deletion"
        );
    }

    async fn delete_workspace_after_capture_holds(
        db: &CoralDb,
        workspace_id: &str,
        mutation_barrier: &TraceSearchResponseMutationBarrier,
    ) {
        mutation_barrier.wait_until_workspace_held().await;
        let deletion = async {
            db.begin_workspace_deletion(workspace_id)
                .await
                .expect("begin concurrent workspace deletion")
                .expect("workspace exists before concurrent deletion")
                .commit()
                .await
                .expect("commit concurrent workspace deletion");
        };
        tokio::pin!(deletion);
        assert!(
            tokio::time::timeout(Duration::from_millis(250), deletion.as_mut())
                .await
                .is_err(),
            "workspace deletion must wait while response capture holds the parent"
        );
        mutation_barrier.release_mutation().await;
        deletion.await;
    }

    async fn assert_prune_holds_workspace_before_deletion(
        db: &CoralDb,
        lock_limited_db: &CoralDb,
        workspace_id: &str,
    ) {
        ensure_workspace(db, workspace_id, 50).await;
        insert_response(db, workspace_id, &format!("{workspace_id}_prune-first"), 1).await;
        let mutation_barrier = TraceSearchResponseMutationBarrier::new();
        let (prune_result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                db.prune_trace_search_responses_batch_with_mutation_barrier(
                    workspace_id,
                    retention_bounds_from(2),
                    &mutation_barrier,
                ),
                assert_workspace_deletion_hits_lock_timeout(
                    lock_limited_db,
                    workspace_id,
                    &mutation_barrier,
                ),
            )
        })
        .await
        .expect("prune/delete serialization should finish");
        assert_eq!(
            prune_result.expect("prune response before workspace deletion"),
            TraceSearchResponsePruneBatchResult::Pruned(1)
        );
        delete_workspace(db, workspace_id).await;
    }

    async fn assert_workspace_deletion_hits_lock_timeout(
        db: &CoralDb,
        workspace_id: &str,
        mutation_barrier: &TraceSearchResponseMutationBarrier,
    ) {
        mutation_barrier.wait_until_workspace_held().await;
        let deletion_error = match db.begin_workspace_deletion(workspace_id).await {
            Err(error) => error,
            Ok(Some(deletion)) => {
                deletion
                    .rollback()
                    .await
                    .expect("roll back unexpectedly acquired deletion");
                mutation_barrier.release_mutation().await;
                panic!("workspace deletion unexpectedly acquired the held parent");
            }
            Ok(None) => {
                mutation_barrier.release_mutation().await;
                panic!("workspace disappeared while response pruning held its parent");
            }
        };
        mutation_barrier.release_mutation().await;
        assert_postgres_lock_timeout(deletion_error);
    }

    fn assert_postgres_lock_timeout(error: DbError) {
        let DbError::Sqlx(sqlx::Error::Database(error)) = error else {
            panic!("expected Postgres lock timeout, got {error:?}");
        };
        assert_eq!(error.code().as_deref(), Some("55P03"));
    }

    async fn open_lock_limited_postgres(postgres_url: &str) -> CoralDb {
        let mut url = url::Url::parse(postgres_url).expect("parse Postgres test URL");
        url.query_pairs_mut()
            .append_pair("options[lock_timeout]", "100ms");
        CoralDb::open(ResolvedDatabaseConfig::Postgres {
            url: url.to_string(),
        })
        .await
        .expect("open lock-limited Postgres")
    }

    async fn insert_response(
        db: &CoralDb,
        workspace_id: &str,
        trace_id: &str,
        recorded_at_unix_nanos: i64,
    ) {
        assert_eq!(
            db.insert_trace_search_response(capture(
                workspace_id,
                trace_id,
                "prune-span",
                recorded_at_unix_nanos,
                TraceSearchResponseOutcome::Response(vec![1]),
            ))
            .await
            .expect("insert response for pruning"),
            TraceSearchResponseInsertResult::Inserted
        );
    }

    fn capture(
        workspace_id: &str,
        trace_id: &str,
        search_span_id: &str,
        recorded_at_unix_nanos: i64,
        outcome: TraceSearchResponseOutcome,
    ) -> TraceSearchResponseCapture {
        TraceSearchResponseCapture {
            workspace_id: workspace_id.to_string(),
            trace_id: trace_id.to_string(),
            search_span_id: search_span_id.to_string(),
            recorded_at_unix_nanos,
            outcome,
        }
    }

    fn retention_bounds_from(
        oldest_inclusive_unix_nanos: i64,
    ) -> TraceSearchResponseRetentionBounds {
        TraceSearchResponseRetentionBounds {
            oldest_inclusive_unix_nanos,
            newest_inclusive_unix_nanos: i64::MAX,
        }
    }

    async fn ensure_workspace(db: &CoralDb, workspace_id: &str, created_at_unix_nanos: i64) {
        let mut tx = db.begin().await.expect("begin workspace transaction");
        tx.workspaces()
            .ensure(workspace_id, created_at_unix_nanos)
            .await
            .expect("ensure workspace");
        tx.commit().await.expect("commit workspace transaction");
    }

    async fn delete_workspace(db: &CoralDb, workspace_id: &str) {
        let Some(deletion) = db
            .begin_workspace_deletion(workspace_id)
            .await
            .expect("begin workspace cleanup")
        else {
            return;
        };
        deletion.commit().await.expect("commit workspace cleanup");
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
