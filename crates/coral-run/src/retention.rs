//! Transaction-compatible execution-history retention facets, not a cleanup scheduler.
//! Retention holds and deletes terminal Runs only; a nonterminal Run's rows are its
//! Run Owner's.
use crate::{Error, RunId, RunState, run_repository};
use coral_db::Transaction;
use coral_workspace::{Principal, WorkspaceId};

/// Scope-bound retained Run evidence: a terminal Run's read while holding its
/// row, a nonterminal Run's read without a hold.
pub struct RunRetentionEvidence {
    principal: Principal,
    state: RunState,
    last_activity_at_unix_nanos: i64,
    terminal_at_unix_nanos: Option<i64>,
}
impl RunRetentionEvidence {
    /// Returns the Invoking Principal, for exact association validation.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }
    /// Nonterminal Runs protect their complete retention unit.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
    /// Qualifying execution and human-action activity, never observation or worker heartbeats.
    #[must_use]
    pub const fn last_activity_at_unix_nanos(&self) -> i64 {
        self.last_activity_at_unix_nanos
    }
    /// Standalone history uses terminal completion, not Task inactivity.
    #[must_use]
    pub const fn terminal_at_unix_nanos(&self) -> Option<i64> {
        self.terminal_at_unix_nanos
    }
}

/// Returns owning evidence for retention eligibility, holding the Run only
/// when it is terminal. A nonterminal Run is read without a hold, since its
/// row is its Run Owner's alone, and protects its retention unit.
/// # Errors
/// Returns persistence failure; absence remains explicitly unavailable.
pub async fn hold_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<Option<RunRetentionEvidence>, Error> {
    let held = run_repository::hold_terminal_run(transaction, workspace, run).await?;
    let row = match held {
        Some(held) => held,
        None => match run_repository::find_exact_in_transaction(transaction, workspace, run).await?
        {
            // It ended between the hold and the read, for good: hold it now.
            Some(ended) if ended.state.is_terminal() => {
                let Some(held) =
                    run_repository::hold_terminal_run(transaction, workspace, run).await?
                else {
                    return Ok(None);
                };
                held
            }
            Some(nonterminal) => nonterminal,
            None => return Ok(None),
        },
    };
    let activity = run_repository::retention_activity(transaction, workspace, run).await?;
    Ok(Some(RunRetentionEvidence {
        principal: row.principal,
        state: row.state,
        last_activity_at_unix_nanos: activity
            .max(row.accepted_at_unix_nanos)
            .max(row.execution_started_at_unix_nanos.unwrap_or(i64::MIN))
            .max(row.terminal_at_unix_nanos.unwrap_or(i64::MIN)),
        terminal_at_unix_nanos: row.terminal_at_unix_nanos,
    }))
}

/// Deletes terminal execution-dependent history in the caller's transaction.
/// The coordinator must select and hold the complete retention unit first. This
/// facet applies no independent expiry clock and never removes shared Account connection
/// workflows, current configuration, or Administrative Lifecycle Events.
/// # Errors
/// Rejects an active Run or mismatched attribution; persistence failure requires rollback.
pub async fn delete_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    principal: &Principal,
) -> Result<(), Error> {
    let evidence = hold_in_transaction(transaction, workspace, run)
        .await?
        .ok_or(Error::RunNotFound)?;
    if !evidence.is_terminal() || evidence.principal() != principal {
        return Err(Error::RunNotFound);
    }
    run_repository::delete_retained_history(transaction, workspace, run).await
}
