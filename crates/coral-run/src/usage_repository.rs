//! Run-owned counts for anonymous usage data. Each read joins the caller's
//! snapshot and returns counts alone: never a Program, a message or a
//! result. coral-run keeps the storage encoding these counts decode.

use coral_db::Transaction;

use crate::Error;

/// Runs that ended in a window, by how they ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunEndings {
    /// Runs that succeeded.
    pub succeeded: u64,
    /// Runs that failed. An interrupted Run is not one of them.
    pub failed: u64,
    /// Runs that their deadline ended.
    pub timed_out: u64,
    /// Runs that were canceled.
    pub canceled: u64,
    /// Runs whose started execution was lost.
    pub interrupted: u64,
    /// Failed Runs whose Program let a Coral `ToolError` escape.
    pub tool_error: u64,
    /// Failed Runs that used up their CPU budget.
    pub cpu_budget: u64,
}

/// Counts the Runs that ended in `[start, end)` unix nanoseconds, across
/// every Workspace. The caller authorizes the read.
///
/// # Errors
///
/// Returns a database failure, or corrupt data for a Run row Coral did not
/// write.
pub async fn run_endings(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<RunEndings, Error> {
    crate::run_repository::run_endings(tx, start, end).await
}
