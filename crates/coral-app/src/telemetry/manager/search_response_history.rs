//! Read-side coordination for retained Search responses.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::{RetainedSearchResponse, TraceListView};
use crate::state::db::{
    CoralDb, DbError, TraceSearchResponseOutcome, now_unix_nanos_i64,
    trace_search_response_retention_bounds,
};
use crate::telemetry::SEARCH_SPAN_NAME;
use crate::telemetry::local_store::{
    StoredTraceInvocationKind, StoredTraceOperationKind, TraceDetailRecord, TraceSpanRecord,
    attributes_match_workspace,
};

const READ_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, Clone)]
pub(super) struct SearchResponseHistoryReader {
    db: Arc<CoralDb>,
    retention: Duration,
    warnings: Arc<ReadWarnings>,
}

#[derive(Debug, Default)]
struct ReadWarnings {
    invalid_clock: AtomicBool,
    read_failure: AtomicBool,
}

#[derive(Debug)]
enum ReadError {
    Database(DbError),
    TimedOut,
}

impl SearchResponseHistoryReader {
    pub(super) fn new(db: Arc<CoralDb>, retention: Duration) -> Self {
        Self {
            db,
            retention,
            warnings: Arc::new(ReadWarnings::default()),
        }
    }

    pub(super) async fn read(
        &self,
        trace: &TraceDetailRecord,
        view: TraceListView,
        workspace: Option<&str>,
    ) -> Option<RetainedSearchResponse> {
        let workspace = workspace?;
        let search_span = selected_search_execution(trace, view, workspace)?;
        let now_unix_nanos = match now_unix_nanos_i64() {
            Ok(now) => {
                self.warnings.invalid_clock.store(false, Ordering::Relaxed);
                now
            }
            Err(error) => {
                if !self.warnings.invalid_clock.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        error = ?error,
                        "failed to apply Search response history retention"
                    );
                }
                return None;
            }
        };
        let retention_bounds =
            trace_search_response_retention_bounds(now_unix_nanos, self.retention);
        let outcome = match bounded_read(
            self.db.get_trace_search_response(
                workspace,
                &trace.summary.trace_id,
                &search_span.span_id,
                retention_bounds,
            ),
            READ_TIMEOUT,
        )
        .await
        {
            Ok(outcome) => {
                self.warnings.read_failure.store(false, Ordering::Relaxed);
                outcome?
            }
            Err(read_error) => {
                if !self.warnings.read_failure.swap(true, Ordering::Relaxed) {
                    match read_error {
                        ReadError::Database(error) => {
                            tracing::warn!(
                                error = ?error,
                                workspace,
                                trace_id = %trace.summary.trace_id,
                                search_span_id = %search_span.span_id,
                                "failed to read Search response history"
                            );
                        }
                        ReadError::TimedOut => {
                            tracing::warn!(
                                timeout = ?READ_TIMEOUT,
                                workspace,
                                trace_id = %trace.summary.trace_id,
                                search_span_id = %search_span.span_id,
                                "timed out reading Search response history"
                            );
                        }
                    }
                }
                return None;
            }
        };
        Some(retained_response(outcome))
    }
}

async fn bounded_read<F>(
    read: F,
    timeout: Duration,
) -> Result<Option<TraceSearchResponseOutcome>, ReadError>
where
    F: Future<Output = Result<Option<TraceSearchResponseOutcome>, DbError>>,
{
    tokio::time::timeout(timeout, read)
        .await
        .map_err(|_elapsed| ReadError::TimedOut)?
        .map_err(ReadError::Database)
}

fn retained_response(outcome: TraceSearchResponseOutcome) -> RetainedSearchResponse {
    match outcome {
        TraceSearchResponseOutcome::Response(response) => {
            RetainedSearchResponse::Response(response)
        }
        TraceSearchResponseOutcome::TooLarge { bytes: _ } => RetainedSearchResponse::TooLarge,
    }
}

fn selected_search_execution<'a>(
    trace: &'a TraceDetailRecord,
    view: TraceListView,
    workspace: &str,
) -> Option<&'a TraceSpanRecord> {
    if view != TraceListView::QueryStream
        || trace.summary.operation_kind != StoredTraceOperationKind::Search
    {
        return None;
    }

    let selected = match trace.summary.invocation_kind {
        StoredTraceInvocationKind::Direct => trace.spans.iter().find(|span| {
            span.span_id == trace.summary.root_span_id && span.name == SEARCH_SPAN_NAME
        }),
        StoredTraceInvocationKind::Mcp => nearest_reachable_search_descendant(trace),
        StoredTraceInvocationKind::Unspecified => None,
    }?;
    attributes_match_workspace(&selected.attributes_json, workspace).then_some(selected)
}

fn nearest_reachable_search_descendant(trace: &TraceDetailRecord) -> Option<&TraceSpanRecord> {
    // Query Stream attributes descendants of an explicit MCP entry to that root. Detail records do
    // not retain projector owner IDs, so recover the Search execution through the same parent edges.
    let spans_by_id = trace
        .spans
        .iter()
        .map(|span| (span.span_id.as_str(), span))
        .collect::<HashMap<_, _>>();
    trace
        .spans
        .iter()
        .filter(|span| span.name == SEARCH_SPAN_NAME)
        .filter_map(|span| {
            descendant_depth(span, &trace.summary.root_span_id, &spans_by_id)
                .map(|depth| (depth, span))
        })
        .min_by(|(left_depth, left), (right_depth, right)| {
            left_depth
                .cmp(right_depth)
                .then_with(|| left.start_time_unix_nanos.cmp(&right.start_time_unix_nanos))
                .then_with(|| left.span_id.cmp(&right.span_id))
        })
        .map(|(_depth, span)| span)
}

fn descendant_depth(
    span: &TraceSpanRecord,
    root_span_id: &str,
    spans_by_id: &HashMap<&str, &TraceSpanRecord>,
) -> Option<usize> {
    if span.span_id == root_span_id {
        return Some(0);
    }
    let mut depth = 0usize;
    let mut current = span;
    let mut visited = HashSet::new();
    while visited.insert(current.span_id.as_str()) {
        let parent_span_id = current.parent_span_id.as_deref()?;
        depth = depth.saturating_add(1);
        if parent_span_id == root_span_id {
            return Some(depth);
        }
        current = spans_by_id.get(parent_span_id)?;
    }
    None
}

#[cfg(test)]
mod tests;
