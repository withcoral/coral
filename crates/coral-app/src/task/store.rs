//! Per-workspace, append-only task lifecycle store.

#![allow(
    dead_code,
    reason = "The task stack introduces stores before service slices consume them."
)]

use std::path::Path;

use coral_api::CORAL_TASK_INTENT_MAX_CHARS;
use serde::{Deserialize, Serialize};

use super::id::TaskId;
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

const MAX_TASK_BYTES_PER_WORKSPACE: u64 = 256 * 1024 * 1024;

/// Final task status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskStatus {
    /// Task completed successfully.
    Completed,
    /// Task failed.
    Failed,
}

/// A task start event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskStart {
    pub(crate) id: TaskId,
    pub(crate) workspace: WorkspaceName,
    pub(crate) intent: String,
}

/// A task end event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskEnd {
    pub(crate) id: TaskId,
    pub(crate) workspace: WorkspaceName,
    pub(crate) status: TaskStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum PersistedTaskEvent {
    Start {
        task_id: String,
        workspace: String,
        intent: String,
    },
    End {
        task_id: String,
        workspace: String,
        task_status: TaskStatus,
    },
}

impl PersistedTaskEvent {
    fn from_start(start: &TaskStart, intent: &str) -> Self {
        Self::Start {
            task_id: start.id.to_string(),
            workspace: start.workspace.as_str().to_string(),
            intent: intent.to_string(),
        }
    }

    fn from_end(end: &TaskEnd) -> Self {
        Self::End {
            task_id: end.id.to_string(),
            workspace: end.workspace.as_str().to_string(),
            task_status: end.status,
        }
    }
}

/// Errors from the task lifecycle store.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskStoreError {
    /// Filesystem error reading or writing the store.
    #[error("task store io: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization error.
    #[error("task store serialization: {0}")]
    Serde(#[from] serde_json::Error),
    /// The task intent is empty or exceeds the maximum length.
    #[error("task intent must be non-empty and at most {max} characters")]
    InvalidIntent {
        /// The configured maximum intent length, in characters.
        max: usize,
    },
}

/// Append-only, per-workspace JSONL task lifecycle store, bounded by a byte
/// ceiling with oldest-out eviction.
#[derive(Clone)]
pub(crate) struct TaskStore {
    layout: AppStateLayout,
    max_bytes: u64,
}

impl TaskStore {
    /// Creates a store that persists under `layout`.
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self {
            layout,
            max_bytes: MAX_TASK_BYTES_PER_WORKSPACE,
        }
    }

    /// Overrides the per-workspace byte ceiling for tests.
    #[cfg(test)]
    pub(crate) fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Persists a task start event.
    pub(crate) fn start_task(&self, start: &TaskStart) -> Result<(), TaskStoreError> {
        let intent = start.intent.trim();
        if intent.is_empty() || intent.chars().count() > CORAL_TASK_INTENT_MAX_CHARS {
            return Err(TaskStoreError::InvalidIntent {
                max: CORAL_TASK_INTENT_MAX_CHARS,
            });
        }
        self.append_event(
            &start.workspace,
            PersistedTaskEvent::from_start(start, intent),
        )
    }

    /// Persists a task end event.
    pub(crate) fn end_task(&self, end: &TaskEnd) -> Result<(), TaskStoreError> {
        self.append_event(&end.workspace, PersistedTaskEvent::from_end(end))
    }

    /// Returns whether a task has been started for this workspace.
    pub(crate) fn contains_started_task(
        &self,
        workspace: &WorkspaceName,
        task_id: &TaskId,
    ) -> Result<bool, TaskStoreError> {
        let _lock = FileLock::shared(self.layout.state_lock())?;
        let path = self.layout.task_events_file(workspace);
        let records = read_all_records(&path)?;
        let task_id = task_id.to_string();
        Ok(records.iter().any(|record| match record {
            PersistedTaskEvent::Start {
                task_id: started_task_id,
                ..
            } => started_task_id == &task_id,
            PersistedTaskEvent::End { .. } => false,
        }))
    }

    fn append_event(
        &self,
        workspace: &WorkspaceName,
        event: PersistedTaskEvent,
    ) -> Result<(), TaskStoreError> {
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let path = self.layout.task_events_file(workspace);
        let records = read_all_records(&path)?;
        append_within_budget(&path, records, event, self.max_bytes)
    }
}

fn read_all_records(path: &Path) -> Result<Vec<PersistedTaskEvent>, TaskStoreError> {
    let contents = match std::fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    Ok(contents
        .split(|&byte| byte == b'\n')
        .filter_map(|raw_line| {
            let Ok(line) = std::str::from_utf8(raw_line) else {
                tracing::warn!("skipping task record with invalid UTF-8");
                return None;
            };
            if line.trim().is_empty() {
                return None;
            }
            match serde_json::from_str::<PersistedTaskEvent>(line) {
                Ok(record) => Some(record),
                Err(error) => {
                    tracing::warn!(%error, "skipping unparsable task record");
                    None
                }
            }
        })
        .collect())
}

fn append_within_budget(
    path: &Path,
    existing: Vec<PersistedTaskEvent>,
    record: PersistedTaskEvent,
    max_bytes: u64,
) -> Result<(), TaskStoreError> {
    let mut kept = existing;
    kept.push(record);
    let encoded: Vec<Vec<u8>> = kept
        .iter()
        .map(serde_json::to_vec)
        .collect::<Result<_, _>>()?;
    let record_count = encoded.len();
    let mut total: u64 = encoded.iter().map(|line| line.len() as u64 + 1).sum();
    let mut dropped = 0;
    for line in &encoded {
        if total <= max_bytes || dropped + 1 >= record_count {
            break;
        }
        total -= line.len() as u64 + 1;
        dropped += 1;
    }
    let mut bytes = Vec::new();
    for line in encoded.iter().skip(dropped) {
        bytes.extend_from_slice(line);
        bytes.push(b'\n');
    }
    if let Some(parent) = path.parent() {
        storage_fs::ensure_dir(parent)?;
    }
    storage_fs::write_atomic(path, &bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "JSON record assertions intentionally fail loudly in tests"
    )]

    use std::fs;

    use serde_json::Value;
    use tempfile::TempDir;

    use super::{PersistedTaskEvent, TaskEnd, TaskStart, TaskStatus, TaskStore, TaskStoreError};
    use crate::state::AppStateLayout;
    use crate::task::id::TaskId;
    use crate::workspaces::WorkspaceName;

    const TASK_ID_1: &str = "550e8400-e29b-41d4-a716-446655440000";
    const TASK_ID_2: &str = "650e8400-e29b-41d4-a716-446655440000";

    fn layout() -> (TempDir, AppStateLayout) {
        let dir = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(dir.path().join("coral-config")))
            .expect("layout should resolve");
        (dir, layout)
    }

    fn start(workspace: &WorkspaceName, id: &str, intent: &str) -> TaskStart {
        TaskStart {
            id: TaskId::parse(id).expect("valid task id"),
            workspace: workspace.clone(),
            intent: intent.to_string(),
        }
    }

    fn end(workspace: &WorkspaceName, id: &str, status: TaskStatus) -> TaskEnd {
        TaskEnd {
            id: TaskId::parse(id).expect("valid task id"),
            workspace: workspace.clone(),
            status,
        }
    }

    #[test]
    fn start_task_persists_task() {
        let (_dir, layout) = layout();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let store = TaskStore::new(layout.clone());

        store
            .start_task(&start(&workspace, TASK_ID_1, "Find renewal risk"))
            .expect("start task");

        let raw = fs::read_to_string(layout.task_events_file(&workspace)).expect("task file");
        let record: Value = serde_json::from_str(raw.trim()).expect("task JSONL should parse");
        assert_eq!(record["event"], "start");
        assert_eq!(record["task_id"], TASK_ID_1);
        assert_eq!(record["workspace"], "acme");
        assert_eq!(record["intent"], "Find renewal risk");
    }

    #[test]
    fn contains_started_task_matches_start_events_only() {
        let (_dir, layout) = layout();
        let workspace = WorkspaceName::default();
        let store = TaskStore::new(layout);

        store
            .end_task(&end(&workspace, TASK_ID_1, TaskStatus::Failed))
            .expect("end task");
        assert!(
            !store
                .contains_started_task(
                    &workspace,
                    &TaskId::parse(TASK_ID_1).expect("valid task id")
                )
                .expect("contains task")
        );

        store
            .start_task(&start(&workspace, TASK_ID_1, "Find renewal risk"))
            .expect("start task");
        assert!(
            store
                .contains_started_task(
                    &workspace,
                    &TaskId::parse(TASK_ID_1).expect("valid task id")
                )
                .expect("contains task")
        );
    }

    #[test]
    fn end_task_preserves_existing_start_event() {
        let (_dir, layout) = layout();
        let workspace = WorkspaceName::default();
        let store = TaskStore::new(layout.clone());

        store
            .start_task(&start(&workspace, TASK_ID_1, "Find renewal risk"))
            .expect("start task");
        let raw_after_start =
            fs::read_to_string(layout.task_events_file(&workspace)).expect("task file");
        assert!(
            raw_after_start.contains("Find renewal risk"),
            "got after start: {raw_after_start}"
        );
        let _parsed: PersistedTaskEvent =
            serde_json::from_str(raw_after_start.trim()).expect("start event should parse");
        store
            .end_task(&end(&workspace, TASK_ID_1, TaskStatus::Completed))
            .expect("end task");

        let raw = fs::read_to_string(layout.task_events_file(&workspace)).expect("task file");
        assert!(raw.contains("Find renewal risk"), "got: {raw}");
        assert!(raw.contains("completed"), "got: {raw}");
    }

    #[test]
    fn validates_intent() {
        let (_dir, layout) = layout();
        let workspace = WorkspaceName::default();
        let store = TaskStore::new(layout);

        let blank_start = store
            .start_task(&start(&workspace, TASK_ID_1, " "))
            .expect_err("blank intent");
        assert!(matches!(blank_start, TaskStoreError::InvalidIntent { .. }));
    }

    #[test]
    fn task_events_evict_oldest_records_within_budget() {
        let (_dir, layout) = layout();
        let workspace = WorkspaceName::default();
        let store = TaskStore::new(layout.clone()).with_max_bytes(1);

        store
            .end_task(&end(&workspace, TASK_ID_1, TaskStatus::Completed))
            .expect("first event");
        store
            .end_task(&end(&workspace, TASK_ID_2, TaskStatus::Failed))
            .expect("second event");

        let raw = fs::read_to_string(layout.task_events_file(&workspace)).expect("task file");
        assert!(!raw.contains(TASK_ID_1));
        assert!(raw.contains(TASK_ID_2));
    }
}
