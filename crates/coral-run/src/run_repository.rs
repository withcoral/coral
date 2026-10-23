//! Run record persistence. Children own the package, claim, Run Ledger,
//! termination, Event, and retention statements; callers reach all of them
//! through this module.

#[cfg(test)]
use coral_db::Database;
use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalKind, WorkspaceId};
use sea_query::{Condition, Expr, ExprTrait as _, Iden, Order, Query};

use crate::lifecycle::RunRecord;
use crate::{Error, RunId, RunState};

mod acceptance;
mod claims;
mod events;
mod ledger;
mod retention;
mod run_row;
mod termination;

pub(crate) use acceptance::{insert_acceptance, load_package_in_transaction};
#[cfg(test)]
pub(crate) use claims::claim_for_test;
pub(crate) use claims::start_execution;
use events::last_event_sequence;
pub(crate) use events::{
    NewEvent, OwnedEvent, OwnedEventKind, event_page_in_transaction, output_page_in_transaction,
};
pub(crate) use ledger::{Chunk, ChunkEvent, commit_chunk};
pub(crate) use retention::{delete_retained_history, hold_terminal_run, retention_activity};
use run_row::RunRow;
pub(crate) use termination::{
    finish_run, finish_termination, finish_unstarted_run, interrupt_run, request_termination,
};

#[derive(Iden)]
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
}

/// Reads the Run row in one statement, outside any transaction and without
/// holding it: the reread a Run Owner makes when a commit got no answer.
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

/// Every `running` Run no process owns at `now_unix_nanos`, as the
/// expired-lease sweep reads them in one snapshot, in acceptance order: its
/// lease lapsed, or it has no claim and was accepted more than `lease_nanos`
/// ago. An earlier release claimed a moment after acceptance and released
/// the claim on an interruption, so a fresh Run with no claim belongs to the
/// process that accepted it and is not abandoned.
pub(crate) async fn lapsed_leases(
    transaction: &mut Transaction<'_>,
    now_unix_nanos: i64,
    lease_nanos: i64,
) -> Result<Vec<RunRecord>, Error> {
    let lapsed = Condition::any()
        .add(Expr::col(ProgramRunRecords::WorkerClaimExpiresAtUnixNanos).lte(now_unix_nanos))
        .add(
            Condition::all()
                .add(Expr::col(ProgramRunRecords::WorkerClaimExpiresAtUnixNanos).is_null())
                .add(
                    Expr::col(ProgramRunRecords::AcceptedAtUnixNanos)
                        .lt(now_unix_nanos.saturating_sub(lease_nanos)),
                ),
        );
    let rows: Vec<RunRow> = transaction
        .fetch_all(
            base_run_select()
                .and_where(
                    Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()),
                )
                .cond_where(lapsed)
                .order_by(ProgramRunRecords::AcceptedAtUnixNanos, Order::Asc)
                .order_by(ProgramRunRecords::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(RunRow::decode).collect()
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
const RUN_COLUMNS: [ProgramRunRecords; 26] = [
    ProgramRunRecords::Id,
    ProgramRunRecords::WorkspaceId,
    ProgramRunRecords::PrincipalId,
    ProgramRunRecords::PrincipalKind,
    ProgramRunRecords::State,
    ProgramRunRecords::AcceptedAtUnixNanos,
    ProgramRunRecords::DeadlineAtUnixNanos,
    ProgramRunRecords::WorkerClaimId,
    ProgramRunRecords::WorkerClaimExpiresAtUnixNanos,
    ProgramRunRecords::ExecutionClaimId,
    ProgramRunRecords::ExecutionStartedAtUnixNanos,
    ProgramRunRecords::TerminationKind,
    ProgramRunRecords::TerminationStartedAtUnixNanos,
    ProgramRunRecords::TerminationDeadlineAtUnixNanos,
    ProgramRunRecords::ShutdownGraceNanos,
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
