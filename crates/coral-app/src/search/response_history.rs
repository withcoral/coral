//! Bounded, best-effort retention of successful public Search responses.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use coral_api::v1::SearchResponse;
use prost::Message as _;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};
use tokio::task::JoinHandle;

use crate::search::result::SearchExecutionIdentity;
use crate::state::db::{
    CoralDb, TraceSearchResponseCapture, TraceSearchResponseInsertResult,
    TraceSearchResponseOutcome, now_unix_nanos_i64, trace_search_response_retention_bounds,
};
use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceLifecycleRevision, WorkspaceName};

pub(crate) const SEARCH_RESPONSE_HISTORY_MAX_BYTES: usize = 1024 * 1024;
const SEARCH_RESPONSE_HISTORY_SLOW_WRITE: Duration = Duration::from_millis(500);
const SEARCH_RESPONSE_HISTORY_SLOW_PRUNE: Duration = Duration::from_secs(1);
const SEARCH_RESPONSE_HISTORY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const SEARCH_RESPONSE_HISTORY_PRUNE_INTERVAL: Duration = Duration::from_hours(1);
const SEARCH_RESPONSE_HISTORY_PRUNE_RETRY_INTERVAL: Duration = Duration::from_mins(1);
const SEARCH_RESPONSE_HISTORY_QUEUE_CAPACITY: usize = 16;

#[derive(Clone)]
pub(crate) struct SearchResponseHistory {
    enabled: bool,
    sender: Sender<SearchResponseHistoryWork>,
    warnings: Arc<SearchResponseHistoryWarnings>,
}

#[derive(Default)]
struct SearchResponseHistoryWarnings {
    invalid_clock: AtomicBool,
    missing_identity: AtomicBool,
    queue_full: AtomicBool,
    worker_closed: AtomicBool,
}

pub(crate) struct SearchResponseHistoryWorker {
    join_handle: Option<JoinHandle<()>>,
}

impl Drop for SearchResponseHistoryWorker {
    fn drop(&mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort();
        }
    }
}

impl SearchResponseHistoryWorker {
    pub(crate) async fn shutdown(mut self) {
        let Some(mut join_handle) = self.join_handle.take() else {
            return;
        };
        match tokio::time::timeout(SEARCH_RESPONSE_HISTORY_SHUTDOWN_TIMEOUT, &mut join_handle).await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    error = %error,
                    "Search response history background writer stopped unexpectedly"
                );
            }
            Err(_elapsed) => {
                tracing::warn!(
                    timeout_ms = SEARCH_RESPONSE_HISTORY_SHUTDOWN_TIMEOUT.as_millis(),
                    "aborting Search response history background writer during shutdown"
                );
                join_handle.abort();
                drop(join_handle.await);
            }
        }
    }
}

enum SearchResponseHistoryWork {
    Capture(QueuedSearchResponseCapture),
    #[cfg(test)]
    Flush(tokio::sync::oneshot::Sender<()>),
    #[cfg(test)]
    Pause {
        started: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    },
}

struct QueuedSearchResponseCapture {
    workspace_name: WorkspaceName,
    workspace_lifecycle_revision: WorkspaceLifecycleRevision,
    row: TraceSearchResponseCapture,
}

struct SearchResponseHistoryWorkerState {
    db: Arc<CoralDb>,
    lifecycle_lock: WorkspaceLifecycleLock,
    retention: Duration,
    next_prune_at: Option<Instant>,
    slow_write_active: bool,
    write_failure_active: bool,
}

impl SearchResponseHistory {
    pub(crate) fn start(
        db: Arc<CoralDb>,
        lifecycle_lock: WorkspaceLifecycleLock,
        enabled: bool,
        retention: Duration,
    ) -> (Self, SearchResponseHistoryWorker) {
        let (sender, receiver) = mpsc::channel(SEARCH_RESPONSE_HISTORY_QUEUE_CAPACITY);
        let worker = tokio::spawn(
            SearchResponseHistoryWorkerState {
                db,
                lifecycle_lock,
                retention,
                next_prune_at: None,
                slow_write_active: false,
                write_failure_active: false,
            }
            .run(receiver),
        );
        (
            Self {
                enabled,
                sender,
                warnings: Arc::new(SearchResponseHistoryWarnings::default()),
            },
            SearchResponseHistoryWorker {
                join_handle: Some(worker),
            },
        )
    }

    pub(crate) fn capture(
        &self,
        workspace_name: &WorkspaceName,
        workspace_lifecycle_revision: WorkspaceLifecycleRevision,
        identity: Option<&SearchExecutionIdentity>,
        response: &SearchResponse,
    ) -> bool {
        if !self.enabled {
            return false;
        }
        let Some(identity) = identity else {
            if !self.warnings.missing_identity.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "Search response history is enabled, but the operation span had no valid trace identity"
                );
            }
            return false;
        };
        let Ok(recorded_at_unix_nanos) = now_unix_nanos_i64() else {
            if !self.warnings.invalid_clock.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "Search response history is enabled, but the system clock cannot produce a durable timestamp"
                );
            }
            return false;
        };

        let permit = match self.sender.clone().try_reserve_owned() {
            Ok(permit) => {
                self.warnings.queue_full.store(false, Ordering::Relaxed);
                permit
            }
            Err(TrySendError::Full(_sender)) => {
                if !self.warnings.queue_full.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        queue_capacity = SEARCH_RESPONSE_HISTORY_QUEUE_CAPACITY,
                        "dropping Search response history because its background queue is full"
                    );
                }
                return false;
            }
            Err(TrySendError::Closed(_sender)) => {
                if !self.warnings.worker_closed.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "dropping Search response history because its background writer is unavailable"
                    );
                }
                return false;
            }
        };

        let encoded_len = response.encoded_len();
        let outcome = if encoded_len <= SEARCH_RESPONSE_HISTORY_MAX_BYTES {
            TraceSearchResponseOutcome::Response(response.encode_to_vec())
        } else {
            TraceSearchResponseOutcome::TooLarge {
                bytes: i64::try_from(encoded_len).unwrap_or(i64::MAX),
            }
        };
        permit.send(SearchResponseHistoryWork::Capture(
            QueuedSearchResponseCapture {
                workspace_name: workspace_name.clone(),
                workspace_lifecycle_revision,
                row: TraceSearchResponseCapture {
                    workspace_id: workspace_name.to_string(),
                    trace_id: identity.trace_id.clone(),
                    search_span_id: identity.span_id.clone(),
                    recorded_at_unix_nanos,
                    outcome,
                },
            },
        ));
        true
    }

    #[cfg(test)]
    async fn flush(&self) {
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(SearchResponseHistoryWork::Flush(finished_tx))
            .await
            .expect("Search response history worker remains available");
        finished_rx
            .await
            .expect("Search response history flush completes");
    }

    #[cfg(test)]
    async fn pause_worker(&self) -> tokio::sync::oneshot::Sender<()> {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(SearchResponseHistoryWork::Pause {
                started: started_tx,
                release: release_rx,
            })
            .await
            .expect("Search response history worker remains available");
        started_rx
            .await
            .expect("Search response history worker reaches pause");
        release_tx
    }
}

impl SearchResponseHistoryWorkerState {
    async fn run(mut self, mut receiver: Receiver<SearchResponseHistoryWork>) {
        self.prune_now().await;
        while let Some(work) = receiver.recv().await {
            match work {
                SearchResponseHistoryWork::Capture(capture) => {
                    self.retain(capture).await;
                    self.prune_if_due().await;
                }
                #[cfg(test)]
                SearchResponseHistoryWork::Flush(finished) => {
                    let _finished = finished.send(());
                }
                #[cfg(test)]
                SearchResponseHistoryWork::Pause { started, release } => {
                    let _started = started.send(());
                    let _released = release.await;
                }
            }
        }
    }

    async fn retain(&mut self, queued: QueuedSearchResponseCapture) {
        let Some(lifecycle_lease) = self
            .lifecycle_lock
            .read_lease_if_unchanged(queued.workspace_lifecycle_revision, &queued.workspace_name)
            .await
        else {
            tracing::debug!(
                workspace = %queued.workspace_name,
                "discarded stale Search response history capture"
            );
            return;
        };
        let capture = queued.row;
        let workspace_id = capture.workspace_id.clone();
        let trace_id = capture.trace_id.clone();
        let search_span_id = capture.search_span_id.clone();
        let insert = self.db.insert_trace_search_response(capture);
        tokio::pin!(insert);
        let slow_warning = tokio::time::sleep(SEARCH_RESPONSE_HISTORY_SLOW_WRITE);
        tokio::pin!(slow_warning);
        let result = tokio::select! {
            result = &mut insert => {
                self.slow_write_active = false;
                result
            }
            () = &mut slow_warning => {
                if !self.slow_write_active {
                    tracing::warn!(
                        workspace_id,
                        trace_id,
                        search_span_id,
                        slow_after_ms = SEARCH_RESPONSE_HISTORY_SLOW_WRITE.as_millis(),
                        "Search response history database write is slow"
                    );
                    self.slow_write_active = true;
                }
                insert.await
            }
        };
        drop(lifecycle_lease);
        match result {
            Ok(
                TraceSearchResponseInsertResult::Inserted
                | TraceSearchResponseInsertResult::AlreadyExists,
            ) => self.write_failure_active = false,
            Ok(TraceSearchResponseInsertResult::WorkspaceNotFound) => {
                self.write_failure_active = false;
                tracing::debug!(
                    workspace_id,
                    trace_id,
                    search_span_id,
                    "Search response history skipped because its workspace no longer exists"
                );
            }
            Err(error) => {
                if !self.write_failure_active {
                    tracing::warn!(
                        error = ?error,
                        workspace_id,
                        trace_id,
                        search_span_id,
                        "failed to retain Search response history"
                    );
                    self.write_failure_active = true;
                }
            }
        }
    }

    async fn prune_if_due(&mut self) {
        if self
            .next_prune_at
            .is_some_and(|next_prune_at| Instant::now() < next_prune_at)
        {
            return;
        }
        self.prune_now().await;
    }

    async fn prune_now(&mut self) {
        let succeeded = prune_search_response_history(&self.db, self.retention).await;
        self.next_prune_at = Instant::now().checked_add(prune_retry_after(succeeded));
    }
}

const fn prune_retry_after(succeeded: bool) -> Duration {
    if succeeded {
        SEARCH_RESPONSE_HISTORY_PRUNE_INTERVAL
    } else {
        SEARCH_RESPONSE_HISTORY_PRUNE_RETRY_INTERVAL
    }
}

async fn prune_search_response_history(db: &CoralDb, retention: Duration) -> bool {
    let Ok(now_unix_nanos) = now_unix_nanos_i64() else {
        tracing::warn!(
            "Search response history pruning skipped because the system clock was invalid"
        );
        return false;
    };
    let retention_bounds = trace_search_response_retention_bounds(now_unix_nanos, retention);
    let prune = db.prune_trace_search_responses_outside_retention(retention_bounds);
    tokio::pin!(prune);
    let slow_warning = tokio::time::sleep(SEARCH_RESPONSE_HISTORY_SLOW_PRUNE);
    tokio::pin!(slow_warning);
    let result = tokio::select! {
        result = &mut prune => result,
        () = &mut slow_warning => {
            tracing::warn!(
                slow_after_ms = SEARCH_RESPONSE_HISTORY_SLOW_PRUNE.as_millis(),
                "Search response history pruning is slow"
            );
            prune.await
        }
    };
    match result {
        Ok(_deleted) => true,
        Err(error) => {
            tracing::warn!(error = ?error, "failed to prune Search response history outside retention");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use coral_api::v1::search_result::Shape;
    use coral_api::v1::{
        SearchField, SearchFieldValues, SearchFunctionShape, SearchProvider,
        SearchProviderCoverage, SearchProviderState, SearchProviderStatus, SearchResponse,
        SearchResult, SearchResultTruncation, SearchSurfaceRef, SearchTableShape,
    };
    use prost::Message as _;
    use tempfile::tempdir;

    use super::{
        SEARCH_RESPONSE_HISTORY_MAX_BYTES, SEARCH_RESPONSE_HISTORY_PRUNE_INTERVAL,
        SEARCH_RESPONSE_HISTORY_PRUNE_RETRY_INTERVAL, SEARCH_RESPONSE_HISTORY_QUEUE_CAPACITY,
        SearchResponseHistory, SearchResponseHistoryWorker, prune_retry_after,
    };
    use crate::search::result::SearchExecutionIdentity;
    use crate::state::AppStateLayout;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, TraceSearchResponseCapture,
        TraceSearchResponseInsertResult, TraceSearchResponseOutcome,
        TraceSearchResponseRetentionBounds,
    };
    use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceName};

    #[tokio::test]
    async fn one_mebibyte_boundary_is_measured_before_encoding_and_stored_complete() {
        let (_temp, db) = open_sqlite().await;
        ensure_workspace(&db, "alpha").await;
        let (history, worker, lifecycle) = start_history(&db, true, test_retention());
        history.flush().await;
        let mut response = empty_response();
        resize_note_to_encoded_len(&mut response, SEARCH_RESPONSE_HISTORY_MAX_BYTES);
        assert_eq!(response.encoded_len(), SEARCH_RESPONSE_HISTORY_MAX_BYTES);
        let boundary_identity = identity("trace-boundary", "span-boundary");
        assert!(capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&boundary_identity),
            &response,
        ));
        history.flush().await;
        let outcome = get_record(&db, &boundary_identity)
            .await
            .expect("captured boundary response");
        let TraceSearchResponseOutcome::Response(response_proto) = outcome else {
            panic!("boundary response must be stored complete");
        };
        assert_eq!(response_proto.len(), SEARCH_RESPONSE_HISTORY_MAX_BYTES);

        response
            .truncation
            .as_mut()
            .expect("truncation")
            .note
            .push('x');
        assert!(response.encoded_len() > SEARCH_RESPONSE_HISTORY_MAX_BYTES);
        shutdown_history(history, worker).await;
    }

    #[tokio::test]
    async fn startup_pruning_runs_independently_of_capture_enablement() {
        let (_temp, db) = open_sqlite().await;
        ensure_workspace(&db, "alpha").await;
        let expired_identity = identity("trace-startup-expired", "span-startup-expired");
        insert_raw_response(&db, &expired_identity, 1).await;
        let future_identity = identity("trace-startup-future", "span-startup-future");
        insert_raw_response(&db, &future_identity, i64::MAX).await;

        let (history, worker, lifecycle) = start_history(&db, false, Duration::ZERO);
        history.flush().await;

        assert_eq!(get_record(&db, &expired_identity).await, None);
        assert_eq!(get_record(&db, &future_identity).await, None);
        let disabled_identity = identity("trace-disabled-after-prune", "span-disabled-after-prune");
        assert!(!capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&disabled_identity),
            &empty_response(),
        ));
        history.flush().await;
        assert_eq!(get_record(&db, &disabled_identity).await, None);
        shutdown_history(history, worker).await;
    }

    #[tokio::test]
    async fn capture_prunes_opportunistically_at_most_once_per_hour() {
        let (_temp, db) = open_sqlite().await;
        ensure_workspace(&db, "alpha").await;
        let first_expired = identity("trace-first-expired", "span-first-expired");
        insert_raw_response(&db, &first_expired, 1).await;
        let (history, worker, lifecycle) = start_history(&db, true, Duration::from_hours(1));
        history.flush().await;
        assert_eq!(get_record(&db, &first_expired).await, None);

        let second_expired = identity("trace-second-expired", "span-second-expired");
        insert_raw_response(&db, &second_expired, 1).await;
        let second_fresh = identity("trace-second-fresh", "span-second-fresh");
        assert!(capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&second_fresh),
            &empty_response(),
        ));
        history.flush().await;
        assert!(get_record(&db, &second_expired).await.is_some());
        shutdown_history(history, worker).await;
    }

    #[test]
    fn prune_failures_retry_before_the_normal_interval() {
        assert_eq!(prune_retry_after(false), Duration::from_mins(1));
        assert_eq!(prune_retry_after(true), Duration::from_hours(1));
        assert_eq!(
            SEARCH_RESPONSE_HISTORY_PRUNE_RETRY_INTERVAL,
            Duration::from_mins(1)
        );
        assert_eq!(
            SEARCH_RESPONSE_HISTORY_PRUNE_INTERVAL,
            Duration::from_hours(1)
        );
    }

    #[tokio::test]
    async fn complete_grouped_response_and_zero_result_response_round_trip() {
        let (_temp, db) = open_sqlite().await;
        ensure_workspace(&db, "alpha").await;
        let (history, worker, lifecycle) = start_history(&db, true, test_retention());
        history.flush().await;
        let response = grouped_response();
        let complete_identity = identity("trace-complete", "span-complete");
        assert!(capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&complete_identity),
            &response,
        ));
        history.flush().await;

        let outcome = get_record(&db, &complete_identity)
            .await
            .expect("captured response");
        let TraceSearchResponseOutcome::Response(encoded) = outcome else {
            panic!("complete response must store its protobuf");
        };
        assert_eq!(
            SearchResponse::decode(encoded.as_slice()).expect("decode retained response"),
            response
        );

        let zero_identity = identity("trace-zero", "span-zero");
        let zero_response = empty_response();
        assert!(capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&zero_identity),
            &zero_response,
        ));
        history.flush().await;
        let zero_outcome = get_record(&db, &zero_identity)
            .await
            .expect("captured zero-result response");
        let TraceSearchResponseOutcome::Response(zero_response_proto) = zero_outcome else {
            panic!("zero-result response must store its protobuf");
        };
        assert_eq!(
            SearchResponse::decode(zero_response_proto.as_slice())
                .expect("decode zero-result response"),
            zero_response
        );
        shutdown_history(history, worker).await;
    }

    #[tokio::test]
    async fn disabled_capture_is_absent() {
        let (_temp, db) = open_sqlite().await;
        ensure_workspace(&db, "alpha").await;

        let disabled_identity = identity("trace-disabled", "span-disabled");
        let (history, worker, lifecycle) = start_history(&db, false, test_retention());
        history.flush().await;
        assert!(!capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&disabled_identity),
            &empty_response(),
        ));
        assert_eq!(get_record(&db, &disabled_identity).await, None);
        shutdown_history(history, worker).await;
    }

    #[tokio::test]
    async fn oversized_response_stores_only_its_original_encoded_size() {
        let (_temp, db) = open_sqlite().await;
        ensure_workspace(&db, "alpha").await;
        let (history, worker, lifecycle) = start_history(&db, true, test_retention());
        history.flush().await;
        let identity = identity("trace-large", "span-large");
        let mut response = empty_response();
        resize_note_to_encoded_len(&mut response, SEARCH_RESPONSE_HISTORY_MAX_BYTES + 1);
        let encoded_len = response.encoded_len();

        assert!(capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&identity),
            &response,
        ));
        history.flush().await;

        assert_eq!(
            get_record(&db, &identity).await,
            Some(TraceSearchResponseOutcome::TooLarge {
                bytes: i64::try_from(encoded_len).expect("encoded size fits i64"),
            })
        );
        shutdown_history(history, worker).await;
    }

    #[tokio::test]
    async fn full_queue_drops_capture_without_waiting_for_the_worker() {
        let (_temp, db) = open_sqlite().await;
        ensure_workspace(&db, "alpha").await;
        let (history, worker, lifecycle) = start_history(&db, true, test_retention());
        history.flush().await;
        let release = history.pause_worker().await;

        for index in 0..SEARCH_RESPONSE_HISTORY_QUEUE_CAPACITY {
            let queued_identity = identity(
                &format!("trace-queued-{index}"),
                &format!("span-queued-{index}"),
            );
            assert!(capture(
                &history,
                &lifecycle,
                "alpha",
                Some(&queued_identity),
                &empty_response(),
            ));
        }
        let dropped_identity = identity("trace-dropped", "span-dropped");
        assert!(!capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&dropped_identity),
            &empty_response(),
        ));

        release.send(()).expect("release history worker");
        history.flush().await;
        assert_eq!(get_record(&db, &dropped_identity).await, None);
        shutdown_history(history, worker).await;
    }

    #[tokio::test]
    async fn queued_capture_for_recreated_workspace_is_discarded() {
        let (_temp, db) = open_sqlite().await;
        let workspace = WorkspaceName::parse("alpha").expect("workspace name");
        ensure_workspace(&db, workspace.as_str()).await;
        let lifecycle = WorkspaceLifecycleLock::default();
        let (history, worker) = SearchResponseHistory::start(
            Arc::clone(&db),
            lifecycle.clone(),
            true,
            test_retention(),
        );
        history.flush().await;
        let original_revision = lifecycle
            .revision_if_active(&workspace)
            .expect("workspace starts active");
        let release = history.pause_worker().await;
        let stale_identity = identity("trace-stale", "span-stale");
        assert!(history.capture(
            &workspace,
            original_revision,
            Some(&stale_identity),
            &empty_response(),
        ));

        tokio::time::timeout(Duration::from_secs(5), async {
            let deletion_marker = lifecycle
                .mark_workspace_deleting(&workspace)
                .await
                .expect("mark workspace deleting");
            db.begin_workspace_deletion(workspace.as_str())
                .await
                .expect("begin workspace deletion")
                .expect("workspace exists")
                .commit()
                .await
                .expect("commit workspace deletion");
            drop(deletion_marker);
            {
                let _lifecycle_guard = lifecycle.lock_async().await;
                ensure_workspace(&db, workspace.as_str()).await;
            }

            release.send(()).expect("release history worker");
            history.flush().await;
            assert_eq!(get_record(&db, &stale_identity).await, None);
        })
        .await
        .expect("workspace delete and recreate completes without a lifecycle deadlock");
        shutdown_history(history, worker).await;
    }

    #[tokio::test]
    async fn database_contention_does_not_block_capture_and_the_worker_eventually_persists() {
        let (_temp, db) = open_sqlite().await;
        ensure_workspace(&db, "alpha").await;
        let (history, worker, lifecycle) = start_history(&db, true, test_retention());
        history.flush().await;
        let mut blocker = db.begin().await.expect("begin blocking transaction");
        assert!(
            blocker
                .workspaces()
                .hold_for_child_mutation("alpha")
                .await
                .expect("hold workspace writer")
        );

        let captured_identity = identity("trace-contended", "span-contended");
        assert!(capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&captured_identity),
            &empty_response(),
        ));
        assert_eq!(get_record(&db, &captured_identity).await, None);

        blocker.commit().await.expect("release workspace writer");
        history.flush().await;
        assert!(get_record(&db, &captured_identity).await.is_some());
        shutdown_history(history, worker).await;
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_capture() {
        let (_temp, db) = open_sqlite().await;
        ensure_workspace(&db, "alpha").await;
        let (history, worker, lifecycle) = start_history(&db, true, test_retention());
        let captured_identity = identity("trace-shutdown", "span-shutdown");
        assert!(capture(
            &history,
            &lifecycle,
            "alpha",
            Some(&captured_identity),
            &empty_response(),
        ));

        drop(history);
        worker.shutdown().await;

        assert!(get_record(&db, &captured_identity).await.is_some());
    }

    fn empty_response() -> SearchResponse {
        SearchResponse {
            results: Vec::new(),
            provider_statuses: Vec::new(),
            truncation: Some(SearchResultTruncation {
                truncated: false,
                returned_count: 0,
                max_results: 10,
                note: String::new(),
            }),
        }
    }

    fn grouped_response() -> SearchResponse {
        SearchResponse {
            results: vec![
                SearchResult {
                    surface: Some(SearchSurfaceRef {
                        schema_name: "github".to_string(),
                        name: "repositories".to_string(),
                        catalog_name: String::new(),
                    }),
                    description: "Repository metadata".to_string(),
                    guide: "Filter by owner and name.".to_string(),
                    shape: Some(Shape::Table(SearchTableShape {
                        fields: vec![
                            SearchField {
                                name: "owner".to_string(),
                                data_type: "Utf8".to_string(),
                                required: true,
                            },
                            SearchField {
                                name: "description".to_string(),
                                data_type: "Utf8".to_string(),
                                required: false,
                            },
                        ],
                    })),
                    matching_values: vec![SearchFieldValues {
                        field: "owner".to_string(),
                        values: vec!["withcoral".to_string()],
                    }],
                    omitted_matching_field_count: 2,
                    providers: vec![
                        SearchProvider::CatalogMetadata as i32,
                        SearchProvider::ObservedValues as i32,
                    ],
                },
                SearchResult {
                    surface: Some(SearchSurfaceRef {
                        schema_name: "github".to_string(),
                        name: "search_issues".to_string(),
                        catalog_name: "remote-data".to_string(),
                    }),
                    description: "Search repository issues".to_string(),
                    guide: "Supply the query argument.".to_string(),
                    shape: Some(Shape::Function(SearchFunctionShape {
                        arguments: vec![SearchField {
                            name: "query".to_string(),
                            data_type: "Utf8".to_string(),
                            required: true,
                        }],
                        returns: vec![SearchField {
                            name: "title".to_string(),
                            data_type: "Utf8".to_string(),
                            required: false,
                        }],
                    })),
                    matching_values: Vec::new(),
                    omitted_matching_field_count: 0,
                    providers: vec![SearchProvider::CatalogMetadata as i32],
                },
            ],
            provider_statuses: vec![
                SearchProviderStatus {
                    provider: SearchProvider::CatalogMetadata as i32,
                    state: SearchProviderState::ResultsFound as i32,
                    note: "catalog returned candidates".to_string(),
                    coverage: Some(SearchProviderCoverage {
                        eligible_units: 8,
                        searched_units: 8,
                        failed_units: 0,
                        returned_count: 2,
                        has_more: false,
                        budget_exhausted: false,
                        timed_out: false,
                        stale_index: false,
                    }),
                },
                SearchProviderStatus {
                    provider: SearchProvider::ObservedValues as i32,
                    state: SearchProviderState::ResultsFound as i32,
                    note: "observed values contributed evidence".to_string(),
                    coverage: Some(SearchProviderCoverage {
                        eligible_units: 4,
                        searched_units: 4,
                        failed_units: 0,
                        returned_count: 1,
                        has_more: true,
                        budget_exhausted: false,
                        timed_out: false,
                        stale_index: false,
                    }),
                },
            ],
            truncation: Some(SearchResultTruncation {
                truncated: true,
                returned_count: 2,
                max_results: 2,
                note: "More results were available.".to_string(),
            }),
        }
    }

    fn identity(trace_id: &str, span_id: &str) -> SearchExecutionIdentity {
        SearchExecutionIdentity {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
        }
    }

    fn test_retention() -> Duration {
        Duration::from_hours(7 * 24)
    }

    fn start_history(
        db: &Arc<CoralDb>,
        enabled: bool,
        retention: Duration,
    ) -> (
        SearchResponseHistory,
        SearchResponseHistoryWorker,
        WorkspaceLifecycleLock,
    ) {
        let lifecycle = WorkspaceLifecycleLock::default();
        let (history, worker) =
            SearchResponseHistory::start(Arc::clone(db), lifecycle.clone(), enabled, retention);
        (history, worker, lifecycle)
    }

    fn capture(
        history: &SearchResponseHistory,
        lifecycle: &WorkspaceLifecycleLock,
        workspace_id: &str,
        identity: Option<&SearchExecutionIdentity>,
        response: &SearchResponse,
    ) -> bool {
        let workspace = WorkspaceName::parse(workspace_id).expect("workspace name");
        let revision = lifecycle
            .revision_if_active(&workspace)
            .expect("workspace is active");
        history.capture(&workspace, revision, identity, response)
    }

    async fn shutdown_history(history: SearchResponseHistory, worker: SearchResponseHistoryWorker) {
        drop(history);
        worker.shutdown().await;
    }

    async fn open_sqlite() -> (tempfile::TempDir, Arc<CoralDb>) {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default test database must be SQLite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open SQLite"),
        );
        db.migrate().await.expect("migrate SQLite");
        (temp, db)
    }

    async fn ensure_workspace(db: &CoralDb, workspace_id: &str) {
        let mut tx = db.begin().await.expect("begin workspace transaction");
        tx.workspaces()
            .ensure(workspace_id, 1)
            .await
            .expect("ensure workspace");
        tx.commit().await.expect("commit workspace");
    }

    async fn get_record(
        db: &CoralDb,
        identity: &SearchExecutionIdentity,
    ) -> Option<TraceSearchResponseOutcome> {
        db.get_trace_search_response(
            "alpha",
            &identity.trace_id,
            &identity.span_id,
            TraceSearchResponseRetentionBounds {
                oldest_inclusive_unix_nanos: i64::MIN,
                newest_inclusive_unix_nanos: i64::MAX,
            },
        )
        .await
        .expect("read Search response history")
    }

    async fn insert_raw_response(
        db: &CoralDb,
        identity: &SearchExecutionIdentity,
        recorded_at_unix_nanos: i64,
    ) {
        assert_eq!(
            db.insert_trace_search_response(TraceSearchResponseCapture {
                workspace_id: "alpha".to_string(),
                trace_id: identity.trace_id.clone(),
                search_span_id: identity.span_id.clone(),
                recorded_at_unix_nanos,
                outcome: TraceSearchResponseOutcome::Response(Vec::new()),
            })
            .await
            .expect("insert raw Search response"),
            TraceSearchResponseInsertResult::Inserted
        );
    }

    fn resize_note_to_encoded_len(response: &mut SearchResponse, target: usize) {
        for _attempt in 0..16 {
            let current = response.encoded_len();
            if current == target {
                return;
            }
            let note = &mut response.truncation.as_mut().expect("truncation").note;
            if current < target {
                note.extend(std::iter::repeat_n('x', target - current));
            } else {
                note.truncate(
                    note.len()
                        .checked_sub(current - target)
                        .expect("fixture note can absorb protobuf length overhead"),
                );
            }
        }
        panic!("protobuf fixture cannot reach encoded length {target}");
    }
}
