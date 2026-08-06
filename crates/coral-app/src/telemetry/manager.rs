//! App-level orchestration for local trace inspection.

use std::path::PathBuf;
use std::time::Duration;

use super::local_store::{TraceDetailRecord, TraceStore, TraceStoreError, TraceSummaryRecord};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TraceListView {
    All,
    QueryStream,
}

#[derive(Debug)]
pub(crate) struct ListTracesQuery {
    pub(crate) view: TraceListView,
    pub(crate) workspace: Option<WorkspaceName>,
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
    pub(crate) workspace: Option<WorkspaceName>,
    pub(crate) view: TraceListView,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TraceManagerError {
    #[error("trace '{trace_id}' not found")]
    NotFound { trace_id: String },
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
            workspace,
            page_size,
            offset,
        } = query;
        let workspace = workspace.map(|workspace| workspace.as_str().to_string());
        let fetch_limit = page_size.saturating_add(1);
        let mut traces = match view {
            TraceListView::All => match workspace {
                Some(workspace) => {
                    self.traces
                        .list_traces_for_workspace(fetch_limit, offset, workspace)
                        .await
                }
                None => self.traces.list_traces(fetch_limit, offset).await,
            },
            TraceListView::QueryStream => {
                self.traces
                    .list_query_stream(fetch_limit, offset, workspace)
                    .await
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
            workspace,
            view,
        } = query;
        let workspace = workspace.map(|workspace| workspace.as_str().to_string());
        match view {
            TraceListView::All => match workspace {
                Some(workspace) => {
                    self.traces
                        .get_trace_for_workspace(trace_id, workspace)
                        .await
                }
                None => self.traces.get_trace(trace_id).await,
            },
            TraceListView::QueryStream => {
                self.traces
                    .get_query_stream_trace(trace_id, workspace)
                    .await
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

    use super::{GetTraceQuery, ListTracesQuery, TraceListView, TraceManager, TraceManagerError};
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
                workspace: Some(alpha.clone()),
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
                workspace: Some(alpha),
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
                workspace: None,
                view: TraceListView::All,
            })
            .await
            .expect("unscoped beta trace");
        assert_eq!(detail.summary.trace_id, "beta");

        let error = manager
            .get_trace(GetTraceQuery {
                trace_id: "beta".to_string(),
                workspace: Some(WorkspaceName::parse("alpha").expect("alpha workspace")),
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
