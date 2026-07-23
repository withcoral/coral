//! Transport-free attribution for a query's originating context.

use crate::task::id::TaskId;

/// Request-scoped attribution threaded from the gRPC service edge into the query
/// manager, so transport concerns (gRPC metadata, OpenTelemetry baggage) stay
/// out of the manager and off the deeper query path.
///
/// It carries the optional originating task; the manager stamps `task.id` on
/// the `coral.query` span so trace consumers can join a task's queries to the
/// intent registered by `TaskService.StartTask`. Intent itself is never carried
/// here; only the opaque id is propagated.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryAttribution {
    /// The active task whose intent this query served; `None` for an untagged
    /// query.
    pub(crate) task_id: Option<TaskId>,
}

impl QueryAttribution {
    pub(crate) fn new(task_id: Option<TaskId>) -> Self {
        Self { task_id }
    }
}
