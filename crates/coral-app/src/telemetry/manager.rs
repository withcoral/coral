//! App-level orchestration for local trace inspection.

use std::path::PathBuf;
use std::time::Duration;

use super::local_store::{
    OwnedWorkspaceScope, TraceDetailRecord, TraceStore, TraceStoreError, TraceSummaryRecord,
};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TraceListView {
    All,
    QueryStream,
}

/// Trace visibility the caller has already been authorized for.
///
/// The service decides this before any read reaches the store: a named
/// workspace is authorized for `Manage`, the local principal reads everything,
/// and a federated caller without a named workspace sees only the workspaces
/// they own.
#[derive(Debug)]
pub(crate) enum TraceAccessScope {
    /// Every trace, including host-level rows with no workspace attribution.
    Unrestricted,
    /// One workspace the caller is authorized to manage.
    Workspace(WorkspaceName),
    /// Only the workspaces the caller owns.
    Owned(OwnedWorkspaceScope),
}

#[derive(Debug)]
pub(crate) struct ListTracesQuery {
    pub(crate) view: TraceListView,
    pub(crate) scope: TraceAccessScope,
    pub(crate) page_size: usize,
    pub(crate) offset: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TraceListPage {
    pub(crate) traces: Vec<TraceSummaryRecord>,
    pub(crate) next_offset: Option<usize>,
}

#[derive(Debug)]
pub(crate) struct GetTraceQuery {
    pub(crate) trace_id: String,
    pub(crate) scope: TraceAccessScope,
    pub(crate) view: TraceListView,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TraceManagerError {
    #[error("trace '{trace_id}' not found")]
    NotFound { trace_id: String },
    /// The query-stream view has no owner-scoped read yet, so an owner-scoped
    /// caller must name the workspace instead of reading across all of them.
    #[error("query-stream traces require an explicit workspace")]
    OwnedScopeUnsupported,
    #[error(transparent)]
    Store(TraceStoreError),
}

impl From<TraceStoreError> for TraceManagerError {
    fn from(error: TraceStoreError) -> Self {
        match error {
            TraceStoreError::NotFound(trace_id) => Self::NotFound { trace_id },
            error => Self::Store(error),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TraceManager {
    traces: TraceStore,
}

impl TraceManager {
    pub(crate) fn new(trace_store_dir: PathBuf, retention: Duration) -> Self {
        Self {
            traces: TraceStore::with_retention(trace_store_dir, retention),
        }
    }

    pub(crate) async fn list_traces(
        &self,
        query: ListTracesQuery,
    ) -> Result<TraceListPage, TraceManagerError> {
        let ListTracesQuery {
            view,
            scope,
            page_size,
            offset,
        } = query;
        let fetch_limit = page_size.saturating_add(1);
        let mut traces = match (view, scope) {
            (TraceListView::All, TraceAccessScope::Unrestricted) => {
                self.traces
                    .list_traces_unrestricted(fetch_limit, offset)
                    .await
            }
            (TraceListView::All, TraceAccessScope::Workspace(workspace)) => {
                self.traces
                    .list_traces_for_workspace(fetch_limit, offset, workspace.as_str().to_string())
                    .await
            }
            (TraceListView::All, TraceAccessScope::Owned(owned)) => {
                self.traces
                    .list_traces_for_owned_workspaces(fetch_limit, offset, owned)
                    .await
            }
            (TraceListView::QueryStream, TraceAccessScope::Unrestricted) => {
                self.traces
                    .list_query_stream(fetch_limit, offset, None)
                    .await
            }
            (TraceListView::QueryStream, TraceAccessScope::Workspace(workspace)) => {
                self.traces
                    .list_query_stream(fetch_limit, offset, Some(workspace.as_str().to_string()))
                    .await
            }
            (TraceListView::QueryStream, TraceAccessScope::Owned(_owned)) => {
                return Err(TraceManagerError::OwnedScopeUnsupported);
            }
        }?;
        let next_offset = (traces.len() > page_size).then(|| offset.saturating_add(page_size));
        if next_offset.is_some() {
            traces.truncate(page_size);
        }
        Ok(TraceListPage {
            traces,
            next_offset,
        })
    }

    pub(crate) async fn get_trace(
        &self,
        query: GetTraceQuery,
    ) -> Result<TraceDetailRecord, TraceManagerError> {
        let GetTraceQuery {
            trace_id,
            scope,
            view,
        } = query;
        match (view, scope) {
            (TraceListView::All, TraceAccessScope::Unrestricted) => {
                self.traces.get_trace_unrestricted(trace_id).await
            }
            (TraceListView::All, TraceAccessScope::Workspace(workspace)) => {
                self.traces
                    .get_trace_for_workspace(trace_id, workspace.as_str().to_string())
                    .await
            }
            (TraceListView::All, TraceAccessScope::Owned(owned)) => {
                self.traces
                    .get_trace_for_owned_workspaces(trace_id, owned)
                    .await
            }
            (TraceListView::QueryStream, TraceAccessScope::Unrestricted) => {
                self.traces.get_query_stream_trace(trace_id, None).await
            }
            (TraceListView::QueryStream, TraceAccessScope::Workspace(workspace)) => {
                self.traces
                    .get_query_stream_trace(trace_id, Some(workspace.as_str().to_string()))
                    .await
            }
            (TraceListView::QueryStream, TraceAccessScope::Owned(_owned)) => {
                return Err(TraceManagerError::OwnedScopeUnsupported);
            }
        }
        .map_err(TraceManagerError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tempfile::TempDir;

    use super::{
        GetTraceQuery, ListTracesQuery, TraceAccessScope, TraceListView, TraceManager,
        TraceManagerError,
    };
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn manager_scopes_and_paginates_all_trace_lists() {
        assert_manager_scopes_and_paginates_trace_lists(TraceListView::All).await;
    }

    #[tokio::test]
    async fn manager_scopes_and_paginates_query_stream_trace_lists() {
        assert_manager_scopes_and_paginates_trace_lists(TraceListView::QueryStream).await;
    }

    async fn assert_manager_scopes_and_paginates_trace_lists(view: TraceListView) {
        let (_temp, manager) = trace_manager_fixture();
        let alpha = WorkspaceName::parse("alpha").expect("alpha workspace");

        let first_page = manager
            .list_traces(ListTracesQuery {
                view,
                scope: TraceAccessScope::Workspace(alpha.clone()),
                page_size: 1,
                offset: 0,
            })
            .await
            .expect("first trace page");
        assert_eq!(first_page.traces.len(), 1);
        assert_eq!(
            first_page.traces.first().expect("first trace").trace_id,
            "alpha-new"
        );
        assert_eq!(first_page.next_offset, Some(1));

        let second_page = manager
            .list_traces(ListTracesQuery {
                view,
                scope: TraceAccessScope::Workspace(alpha),
                page_size: 1,
                offset: 1,
            })
            .await
            .expect("second trace page");
        assert_eq!(second_page.traces.len(), 1);
        assert_eq!(
            second_page.traces.first().expect("second trace").trace_id,
            "alpha-old"
        );
        assert_eq!(second_page.next_offset, None);
    }

    #[tokio::test]
    async fn manager_applies_workspace_scope_to_trace_detail() {
        let (_temp, manager) = trace_manager_fixture();

        let detail = manager
            .get_trace(GetTraceQuery {
                trace_id: "beta".to_string(),
                scope: TraceAccessScope::Unrestricted,
                view: TraceListView::All,
            })
            .await
            .expect("unscoped beta trace");
        assert_eq!(detail.summary.trace_id, "beta");

        let error = manager
            .get_trace(GetTraceQuery {
                trace_id: "beta".to_string(),
                scope: TraceAccessScope::Workspace(
                    WorkspaceName::parse("alpha").expect("alpha workspace"),
                ),
                view: TraceListView::All,
            })
            .await
            .expect_err("beta trace must not match alpha workspace");
        assert!(matches!(
            error,
            TraceManagerError::NotFound { trace_id } if trace_id == "beta"
        ));
    }

    fn trace_manager_fixture() -> (TempDir, TraceManager) {
        let temp = TempDir::new().expect("temp dir");
        let trace_store = temp.path().join("trace-store");
        std::fs::create_dir_all(&trace_store).expect("trace store dir");
        let records = [
            trace_record("alpha-old", "alpha", 10, 20),
            trace_record("alpha-new", "alpha", 30, 40),
            trace_record("beta", "beta", 50, 60),
        ];
        let lines = records
            .into_iter()
            .map(|record| record.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(trace_store.join("spans-test.jsonl"), format!("{lines}\n"))
            .expect("write trace records");
        (temp, TraceManager::new(trace_store, Duration::from_mins(1)))
    }

    fn trace_record(
        trace_id: &str,
        workspace: &str,
        start_time_unix_nanos: i64,
        end_time_unix_nanos: i64,
    ) -> serde_json::Value {
        json!({
            "trace_id": trace_id,
            "span_id": format!("{trace_id}-span"),
            "parent_span_id": null,
            "parent_span_is_remote": false,
            "name": "coral.query",
            "kind": "internal",
            "status": "ok",
            "status_message": null,
            "start_time_unix_nanos": start_time_unix_nanos,
            "end_time_unix_nanos": end_time_unix_nanos,
            "duration_nanos": end_time_unix_nanos - start_time_unix_nanos,
            "attributes_json": json!({
                "coral.stream.entry": true,
                "coral.stream.kind": "query",
                "coral.stream.name": trace_id,
                "workspace": workspace,
                "sql": format!("SELECT '{trace_id}'"),
                "status": "ok",
            }).to_string(),
            "events_json": "[]",
            "links_json": "[]",
            "resource_json": "{}",
            "scope_name": "test",
            "scope_version": null,
            "scope_schema_url": null,
            "scope_attributes_json": "{}",
            "trace_flags": 0,
            "trace_state": "",
            "is_remote": false
        })
    }
}
