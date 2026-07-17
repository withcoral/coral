//! Validated task identifier.

#![allow(
    dead_code,
    reason = "The task stack introduces ids before service and store slices consume them."
)]

use std::fmt;

use crate::bootstrap::AppError;

/// App-owned identity for one validated task id.
///
/// Task ids are UUIDs minted by `TaskService.StartTask` and round-trip as the
/// `coral-task-id` gRPC metadata value on subsequent Coral calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TaskId(uuid::Uuid);

impl TaskId {
    /// Mint a new server-owned task id.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Parse and validate a task id from a client or persistence boundary.
    pub(crate) fn parse(id: &str) -> Result<Self, AppError> {
        uuid::Uuid::parse_str(id)
            .map(Self)
            .map_err(|_err| AppError::InvalidInput("task id must be a UUID".to_string()))
    }

    /// Borrow the validated UUID for typed boundaries.
    #[must_use]
    pub(crate) fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }

    /// Lower the task id into the validated UUID.
    #[must_use]
    pub(crate) fn into_uuid(self) -> uuid::Uuid {
        self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::TaskId;

    #[test]
    fn accepts_uuid() {
        let task_id =
            TaskId::parse("550e8400-e29b-41d4-a716-446655440000").expect("task id is valid");
        assert_eq!(task_id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn canonicalizes_simple_uuid() {
        let task_id = TaskId::parse("550e8400e29b41d4a716446655440000").expect("task id is valid");
        assert_eq!(task_id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn rejects_malformed_ids() {
        TaskId::parse("").expect_err("empty id must be rejected");
        TaskId::parse("   ").expect_err("whitespace id must be rejected");
        TaskId::parse("task_550e8400-e29b-41d4-a716-446655440000")
            .expect_err("prefixed id must be rejected");
        TaskId::parse("not-a-uuid").expect_err("malformed id must be rejected");
    }
}
