//! Durable observed-values queue records.

use serde::{Deserialize, Serialize};

/// Snapshot of the workspace-wide and source-local invalidation counters.
///
/// A queued observation is accepted only while both counters still match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedValuesEpoch {
    pub(crate) workspace_generation: i64,
    pub(crate) source_generation: i64,
}

impl ObservedValuesEpoch {
    pub(crate) const ZERO: Self = Self {
        workspace_generation: 0,
        source_generation: 0,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ObservedValuesSurfaceKind {
    Table,
    Function,
}

impl ObservedValuesSurfaceKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Function => "function",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "table" => Some(Self::Table),
            "function" => Some(Self::Function),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ObservedValuesQueuePayload {
    pub(crate) values: Vec<ObservedValueCandidate>,
}

impl ObservedValuesQueuePayload {
    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ObservedValueCandidate {
    pub(crate) column_name: String,
    pub(crate) display_value: String,
    pub(crate) search_text: String,
    pub(crate) value_key: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ObservedValuesQueueJob {
    /// Canonical installed source that owns lifecycle clears and generations.
    pub(crate) owner_source_name: String,
    /// Runtime component schema used in SQL and search results.
    pub(crate) source_name: String,
    pub(crate) source_scope_id: String,
    pub(crate) surface_kind: ObservedValuesSurfaceKind,
    pub(crate) surface_name: String,
    pub(crate) payload_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObservedValuesEnqueueResult {
    Enqueued { job_id: i64 },
    StaleEpoch,
    QueueFull,
    StorageLimitReached,
}
