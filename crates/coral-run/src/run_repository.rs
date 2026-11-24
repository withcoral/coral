//! Run record persistence. Children own the package, take, Run Ledger,
//! termination, Event, and retention statements; callers reach all of them
//! through this module.

#[cfg(test)]
use coral_db::Database;
use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalKind, WorkspaceId};
use sea_query::{Condition, Expr, ExprTrait as _, Iden, Query};

use crate::lifecycle::RunRecord;
use crate::{Error, RunId};

mod acceptance;
mod events;
mod ledger;
mod retention;
mod run_row;
mod stream;
mod sweep;
mod take;
mod termination;
mod usage;

pub(crate) use acceptance::insert_acceptance;
use events::last_event_sequence;
pub(crate) use events::{
    NewEvent, PendingEvent, PendingEventKind, event_page_in_transaction, output_page_in_transaction,
};
pub(crate) use ledger::{Chunk, ChunkEvent, commit_chunk};
pub(crate) use retention::{delete_retained_history, hold_terminal_run, retention_activity};
use run_row::RunRow;
pub(crate) use run_row::UndecodableRun;
pub(crate) use stream::{RunHead, find_taken, heads, linked_tools, reopen_taken, start_taken};
pub(crate) use sweep::{
    HeldRun, elect_sweeper, held_by_run_executor, names_run_executor, past_deadline, put_back,
    unstarted_takes,
};
pub(crate) use take::{Claimed, Taker, any_waiting, take_oldest};
pub(crate) use termination::{finish_run, finish_taken_run, interrupt_run};
pub(crate) use usage::{first_success_before, private_tool_calls, run_endings, shared_tool_calls};

#[derive(Iden, Clone, Copy)]
enum ProgramRunRecords {
    Table,
    Id,
    WorkspaceId,
    PrincipalId,
    PrincipalKind,
    State,
    AcceptedAtUnixNanos,
    DeadlineAtUnixNanos,
    WorkerClaimId,
    WorkerClaimExpiresAtUnixNanos,
    HolderInstanceId,
    TraceParent,
    ExecutionClaimId,
    ExecutionStartedAtUnixNanos,
    TerminationKind,
    TerminationStartedAtUnixNanos,
    TerminationDeadlineAtUnixNanos,
    ShutdownGraceNanos,
    CancellationRequestedAtUnixNanos,
    CancellationPrincipalId,
    CancellationPrincipalKind,
    TerminalAtUnixNanos,
    FailureCode,
    FailureMessage,
    FailureErrorJson,
    FinalValueJson,
    ValueLimitsJson,
    NextEventSequence,
    AccumulatedOutputBytes,
    ChunkSequence,
    LastCommitAtUnixNanos,
    TakenAtUnixNanos,
    RunExecutorId,
    RunExecutorKind,
    CommittedRequestNumber,
    TimeLimitNanos,
}

/// Reads the Run row in one statement, outside any transaction and without
/// holding it: the reread a loop makes when a commit got no answer.
pub(crate) async fn find_exact(
    database: &coral_db::Database,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<Option<RunRecord>, Error> {
    let row: Option<RunRow> = database
        .fetch_optional(exact_run_statement(workspace_id, run_id))
        .await?;
    row.map(RunRow::decode).transpose()
}

pub(crate) async fn find_exact_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<Option<RunRecord>, Error> {
    let row: Option<RunRow> = transaction
        .fetch_optional(exact_run_statement(workspace_id, run_id))
        .await?;
    row.map(RunRow::decode).transpose()
}

pub(crate) async fn find_visible_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    principal: &Principal,
    include_workspace_agents: bool,
) -> Result<Option<RunRecord>, Error> {
    let statement = base_run_select()
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run_id.to_string()))
        .cond_where(visibility_condition(principal, include_workspace_agents))
        .to_owned();
    let row: Option<RunRow> = transaction.fetch_optional(statement).await?;
    row.map(RunRow::decode).transpose()
}

/// The previous release's Event counter, which this release only advances.
#[cfg(test)]
pub(crate) async fn next_event_sequence(
    database: &Database,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<i64, Error> {
    let (counter,): (i64,) = database
        .fetch_optional(
            Query::select()
                .column(ProgramRunRecords::NextEventSequence)
                .from(ProgramRunRecords::Table)
                .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ProgramRunRecords::Id).eq(run_id.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::RunNotFound)?;
    Ok(counter)
}

/// The time of the Run's last chunk commit, `None` before its first.
#[cfg(test)]
pub(crate) async fn last_commit_at(
    database: &Database,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<Option<i64>, Error> {
    let (committed_at,): (Option<i64>,) = database
        .fetch_optional(
            Query::select()
                .column(ProgramRunRecords::LastCommitAtUnixNanos)
                .from(ProgramRunRecords::Table)
                .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ProgramRunRecords::Id).eq(run_id.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::RunNotFound)?;
    Ok(committed_at)
}

#[cfg(test)]
pub(crate) async fn count_runs(database: &Database) -> Result<u64, Error> {
    let statement = Query::select()
        .expr(Expr::col(ProgramRunRecords::Id).count())
        .from(ProgramRunRecords::Table)
        .to_owned();
    let (count,): (i64,) = database
        .fetch_optional(statement)
        .await?
        .ok_or_else(|| Error::CorruptData("Run count query returned no row".to_string()))?;
    u64::try_from(count).map_err(|_error| Error::CorruptData("Run count is negative".to_string()))
}

fn exact_run_statement(workspace_id: &WorkspaceId, run_id: &RunId) -> sea_query::SelectStatement {
    base_run_select()
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run_id.to_string()))
        .to_owned()
}

/// The columns `RunRow` decodes, shared by Run reads and retention's hold.
const RUN_COLUMNS: [ProgramRunRecords; 31] = [
    ProgramRunRecords::Id,
    ProgramRunRecords::WorkspaceId,
    ProgramRunRecords::PrincipalId,
    ProgramRunRecords::PrincipalKind,
    ProgramRunRecords::State,
    ProgramRunRecords::AcceptedAtUnixNanos,
    ProgramRunRecords::DeadlineAtUnixNanos,
    ProgramRunRecords::WorkerClaimId,
    ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
    ProgramRunRecords::HolderInstanceId,
    ProgramRunRecords::TraceParent,
    ProgramRunRecords::ExecutionClaimId,
    ProgramRunRecords::ExecutionStartedAtUnixNanos,
    ProgramRunRecords::TerminationKind,
    ProgramRunRecords::TerminationStartedAtUnixNanos,
    ProgramRunRecords::TerminationDeadlineAtUnixNanos,
    ProgramRunRecords::CancellationRequestedAtUnixNanos,
    ProgramRunRecords::CancellationPrincipalId,
    ProgramRunRecords::CancellationPrincipalKind,
    ProgramRunRecords::TerminalAtUnixNanos,
    ProgramRunRecords::FailureCode,
    ProgramRunRecords::FailureMessage,
    ProgramRunRecords::FailureErrorJson,
    ProgramRunRecords::FinalValueJson,
    ProgramRunRecords::ValueLimitsJson,
    ProgramRunRecords::AccumulatedOutputBytes,
    ProgramRunRecords::ChunkSequence,
    ProgramRunRecords::TimeLimitNanos,
    ProgramRunRecords::RunExecutorKind,
    ProgramRunRecords::CommittedRequestNumber,
    ProgramRunRecords::LastCommitAtUnixNanos,
];

/// Columns a Run read computes beside the row, named as `RunRow` decodes them.
#[derive(Iden)]
enum ComputedRunColumns {
    LastEventSequence,
}

/// The Run row with its last Event sequence, which the observation index
/// answers from its end; Event numbering is data on the Event rows.
fn base_run_select() -> sea_query::SelectStatement {
    Query::select()
        .columns(RUN_COLUMNS)
        .expr_as(last_event_sequence(), ComputedRunColumns::LastEventSequence)
        .from(ProgramRunRecords::Table)
        .to_owned()
}

/// A running Run that no worker claim fences: one a Run Executor waits for,
/// has taken or holds. An older release's Run Owner's Run holds a worker
/// claim until it ends.
/// The state is a literal, so each plan can use an index that `state` leads.
fn running_without_worker_claim() -> Condition {
    Condition::all()
        .add(Expr::col(ProgramRunRecords::State).eq(Expr::cust("'running'")))
        .add(Expr::col(ProgramRunRecords::WorkerClaimId).is_null())
}

/// [`running_without_worker_claim`] for the Run row a query names `alias`.
pub(crate) fn running_without_worker_claim_as(alias: &str) -> Condition {
    let alias = sea_query::Alias::new(alias);
    Condition::all()
        .add(Expr::col((alias.clone(), ProgramRunRecords::State)).eq(Expr::cust("'running'")))
        .add(Expr::col((alias, ProgramRunRecords::WorkerClaimId)).is_null())
}

fn visibility_condition(principal: &Principal, include_workspace_agents: bool) -> Condition {
    let own_runs = Condition::all()
        .add(Expr::col(ProgramRunRecords::PrincipalId).eq(principal.id().as_str()))
        .add(Expr::col(ProgramRunRecords::PrincipalKind).eq(principal.kind().as_storage_str()));
    let condition = Condition::any().add(own_runs);
    if include_workspace_agents {
        condition.add(
            Expr::col(ProgramRunRecords::PrincipalKind).eq(PrincipalKind::Agent.as_storage_str()),
        )
    } else {
        condition
    }
}

fn principal_kind_from_storage(value: &str) -> Result<PrincipalKind, Error> {
    PrincipalKind::from_storage_str(value).map_err(|_error| {
        Error::CorruptData(format!("Run has unrecognized Principal kind '{value}'"))
    })
}
