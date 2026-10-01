//! Transport-free attribution for a query's originating context.

use crate::task::id::TaskId;

/// Request-scoped attribution threaded from the gRPC service edge into the query
/// manager, so transport concerns (gRPC metadata, OpenTelemetry baggage) stay
/// out of the manager and off the deeper query path.
///
/// It carries the optional originating task and ordinary tool intent. The
/// manager uses the pair for durable SQL activity and stamps `task.id` on the
/// `coral.query` span.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryAttribution {
    /// The active task whose intent this query served; `None` for an untagged
    /// query.
    pub(crate) task_id: Option<TaskId>,
    /// The intent supplied for this ordinary tool call, when present.
    pub(crate) tool_intent: Option<String>,
}

impl QueryAttribution {
    pub(crate) fn new(task_id: Option<TaskId>) -> Self {
        Self {
            task_id,
            tool_intent: None,
        }
    }

    pub(crate) fn with_tool_intent(mut self, tool_intent: Option<&str>) -> Self {
        self.tool_intent = tool_intent.map(ToString::to_string);
        self
    }
}
