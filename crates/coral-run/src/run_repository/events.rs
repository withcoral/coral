//! Sequenced Run Events, their pages, and the Run counters they advance.

use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;

use super::{ProgramRunRecords, principal_kind_from_storage};
use crate::lifecycle::RunRecord;
use crate::{
    Error, InvocationId, InvocationResultKind, PROGRAM_LOG_MESSAGE_MAX_BYTES, ProgramLogLevel,
    ProgramValue, RunEvent, RunEventKind, RunId, RunState,
};

#[derive(Iden)]
enum ProgramRunEvents {
    Table,
    WorkspaceId,
    RunId,
    Sequence,
    EventType,
    OccurredAtUnixNanos,
    ActorPrincipalId,
    ActorPrincipalKind,
    TerminalState,
    InvocationId,
    ResultKind,
    ProgramValueJson,
    LogLevel,
    LogMessage,
    OutputBytes,
}

#[derive(Debug, FromRow)]
struct EventRow {
    sequence: i64,
    event_type: String,
    occurred_at_unix_nanos: i64,
    actor_principal_id: Option<String>,
    actor_principal_kind: Option<String>,
    terminal_state: Option<String>,
    invocation_id: Option<String>,
    result_kind: Option<String>,
    program_value_json: Option<String>,
    log_level: Option<String>,
    log_message: Option<String>,
}

impl EventRow {
    fn decode(self, limits: coral_value::ValueLimits) -> Result<RunEvent, Error> {
        let sequence = u64::try_from(self.sequence)
            .ok()
            .filter(|sequence| *sequence > 0)
            .ok_or_else(|| Error::CorruptData("Run Event has invalid sequence".to_string()))?;
        let missing =
            |field| Error::CorruptData(format!("{} Event has no {field}", self.event_type));
        // The table CHECK constrains unrelated columns to NULL. Decode and
        // validate only the payload owned by this Event kind.
        let kind = match self.event_type.as_str() {
            "run.started" => RunEventKind::Started,
            "cancellation.requested" => {
                let id = self
                    .actor_principal_id
                    .ok_or_else(|| missing("Principal"))?;
                let kind = self
                    .actor_principal_kind
                    .as_deref()
                    .ok_or_else(|| missing("Principal kind"))?;
                let id = PrincipalId::parse(&id).map_err(|_error| {
                    Error::CorruptData("cancellation Event has invalid Principal".to_string())
                })?;
                RunEventKind::CancellationRequested {
                    principal: Principal::from_parts(id, principal_kind_from_storage(kind)?),
                }
            }
            "run.ended" => {
                let state = self
                    .terminal_state
                    .as_deref()
                    .ok_or_else(|| missing("terminal state"))
                    .and_then(RunState::from_storage_str)?;
                if !state.is_terminal() {
                    return Err(Error::CorruptData(
                        "Run ended Event has nonterminal state".to_string(),
                    ));
                }
                RunEventKind::Ended { state }
            }
            "invocation.started" | "invocation.ended" => {
                let id = self
                    .invocation_id
                    .ok_or_else(|| missing("Invocation identity"))?;
                let invocation_id = InvocationId::parse(&id).map_err(|_error| {
                    Error::CorruptData(
                        "Invocation Event has invalid Invocation identity".to_string(),
                    )
                })?;
                if self.event_type == "invocation.started" {
                    RunEventKind::InvocationStarted { invocation_id }
                } else {
                    let result_kind = self
                        .result_kind
                        .as_deref()
                        .ok_or_else(|| missing("result kind"))
                        .and_then(InvocationResultKind::from_storage_str)?;
                    RunEventKind::InvocationEnded {
                        invocation_id,
                        result_kind,
                    }
                }
            }
            "program.emitted" => {
                let encoded = self
                    .program_value_json
                    .ok_or_else(|| missing("Program Value"))?;
                RunEventKind::ProgramEmitted {
                    value: ProgramValue::from_persisted_json(encoded, limits)?,
                }
            }
            "program.logged" => {
                let level = self
                    .log_level
                    .as_deref()
                    .ok_or_else(|| missing("log level"))
                    .and_then(ProgramLogLevel::from_storage_str)?;
                let message = self.log_message.ok_or_else(|| missing("log message"))?;
                if message.len() > PROGRAM_LOG_MESSAGE_MAX_BYTES {
                    return Err(Error::CorruptData(
                        "Program Log exceeds its stored limit".to_string(),
                    ));
                }
                RunEventKind::ProgramLogged { level, message }
            }
            _ => {
                return Err(Error::CorruptData(format!(
                    "Run Event has invalid kind '{}'",
                    self.event_type
                )));
            }
        };
        Ok(RunEvent::new(sequence, kind, self.occurred_at_unix_nanos))
    }
}

pub(crate) enum NewEvent<'a> {
    Started,
    CancellationRequested(&'a Principal),
    Ended(RunState, usize),
    InvocationStarted(&'a InvocationId),
    InvocationEnded(&'a InvocationId, InvocationResultKind),
    Emitted(&'a ProgramValue),
    Logged(ProgramLogLevel, &'a str),
}

pub(crate) async fn insert_event(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    sequence: u64,
    event: NewEvent<'_>,
    occurred_at_unix_nanos: i64,
) -> Result<(), Error> {
    use ProgramRunEvents as Events;

    let output_bytes = match &event {
        NewEvent::Emitted(value) => value.encoded_json().len(),
        NewEvent::Logged(_, message) => message.len(),
        NewEvent::Ended(_, bytes) => *bytes,
        _ => 0,
    };
    let output_bytes = i64::try_from(output_bytes)
        .map_err(|_overflow| Error::CorruptData("Event byte count overflow".into()))?;
    let (event_type, mut fields) = match event {
        NewEvent::Started => ("run.started", Vec::new()),
        NewEvent::CancellationRequested(principal) => (
            "cancellation.requested",
            vec![
                (Events::ActorPrincipalId, Expr::val(principal.id().as_str())),
                (
                    Events::ActorPrincipalKind,
                    Expr::val(principal.kind().as_storage_str()),
                ),
            ],
        ),
        NewEvent::Ended(state, _) => (
            "run.ended",
            vec![(Events::TerminalState, Expr::val(state.as_storage_str()))],
        ),
        NewEvent::InvocationStarted(id) => (
            "invocation.started",
            vec![(Events::InvocationId, Expr::val(id.to_string()))],
        ),
        NewEvent::InvocationEnded(id, kind) => (
            "invocation.ended",
            vec![
                (Events::InvocationId, Expr::val(id.to_string())),
                (Events::ResultKind, Expr::val(kind.as_storage_str())),
            ],
        ),
        NewEvent::Emitted(value) => (
            "program.emitted",
            vec![(Events::ProgramValueJson, Expr::val(value.encoded_json()))],
        ),
        NewEvent::Logged(level, message) => (
            "program.logged",
            vec![
                (Events::LogLevel, Expr::val(level.as_storage_str())),
                (Events::LogMessage, Expr::val(message)),
            ],
        ),
    };
    let sequence = i64::try_from(sequence)
        .map_err(|_error| Error::CorruptData("Run Event sequence overflowed".to_string()))?;
    fields.extend([
        (Events::OutputBytes, Expr::val(output_bytes)),
        (Events::WorkspaceId, Expr::val(workspace_id.to_string())),
        (Events::RunId, Expr::val(run_id.to_string())),
        (Events::Sequence, Expr::val(sequence)),
        (Events::EventType, Expr::val(event_type)),
        (
            Events::OccurredAtUnixNanos,
            Expr::val(occurred_at_unix_nanos),
        ),
    ]);
    let (columns, values): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
    let statement = Query::insert()
        .into_table(Events::Table)
        .columns(columns)
        .values_panic(values)
        .to_owned();
    transaction.execute_change(statement).await?;
    Ok(())
}

/// Called only after the current Invocation's guarded result write succeeds,
/// inside the same Run-held transaction. Dispatched results may settle during termination.
pub(crate) async fn record_invocation_output(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    additional: usize,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(ProgramRunRecords::Table)
        .value(
            ProgramRunRecords::AccumulatedOutputBytes,
            stored_output_total(run, additional)?,
        )
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run.id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).eq(claim_id))
        .and_where(
            Expr::col(ProgramRunRecords::AccumulatedOutputBytes).eq(stored_output_total(run, 0)?),
        )
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::InvocationPersistence(
            "Program Run lost Invocation output accounting".into(),
        ));
    }
    Ok(())
}

fn stored_output_total(run: &RunRecord, additional: usize) -> Result<i64, Error> {
    run.accumulated_output_bytes
        .checked_add(additional)
        .and_then(|total| i64::try_from(total).ok())
        .ok_or_else(|| Error::CorruptData("Run output bytes overflowed".into()))
}

pub(crate) async fn advance_event_sequence(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    claim_id: &str,
    increment: u64,
    additional_output_bytes: usize,
) -> Result<(), Error> {
    let current = i64::try_from(run.next_event_sequence)
        .map_err(|_error| Error::CorruptData("Run Event sequence overflowed".to_string()))?;
    let next = run
        .next_event_sequence
        .checked_add(increment)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| Error::CorruptData("Run Event sequence overflowed".to_string()))?;
    let statement = Query::update()
        .table(ProgramRunRecords::Table)
        .value(ProgramRunRecords::NextEventSequence, next)
        .value(
            ProgramRunRecords::AccumulatedOutputBytes,
            stored_output_total(run, additional_output_bytes)?,
        )
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run.id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::TerminationKind).is_null())
        .and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).eq(claim_id))
        .and_where(Expr::col(ProgramRunRecords::WorkerClaimId).eq(claim_id))
        .and_where(Expr::col(ProgramRunRecords::NextEventSequence).eq(current))
        .and_where(
            Expr::col(ProgramRunRecords::AccumulatedOutputBytes).eq(stored_output_total(run, 0)?),
        )
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::RunEventPersistence(
            "Program Run no longer accepts Event evidence".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn advance_event_sequence_after_dispatch(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    claim_id: &str,
    current_sequence: u64,
    increment: u64,
) -> Result<(), Error> {
    advance_terminal_event_sequence(
        transaction,
        workspace_id,
        run_id,
        current_sequence,
        increment,
        Some(claim_id),
    )
    .await
}

pub(crate) async fn advance_event_sequence_during_termination(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    current_sequence: u64,
    increment: u64,
) -> Result<(), Error> {
    advance_terminal_event_sequence(
        transaction,
        workspace_id,
        run_id,
        current_sequence,
        increment,
        None,
    )
    .await
}

async fn advance_terminal_event_sequence(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    current_sequence: u64,
    increment: u64,
    dispatched_claim_id: Option<&str>,
) -> Result<(), Error> {
    let current = i64::try_from(current_sequence)
        .map_err(|_error| Error::CorruptData("Run Event sequence overflowed".to_string()))?;
    let next = current_sequence
        .checked_add(increment)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| Error::CorruptData("Run Event sequence overflowed".to_string()))?;
    let mut statement = Query::update();
    statement
        .table(ProgramRunRecords::Table)
        .value(ProgramRunRecords::NextEventSequence, next)
        .and_where(Expr::col(ProgramRunRecords::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::Id).eq(run_id.to_string()))
        .and_where(Expr::col(ProgramRunRecords::State).eq(RunState::Running.as_storage_str()))
        .and_where(Expr::col(ProgramRunRecords::NextEventSequence).eq(current));
    if let Some(claim_id) = dispatched_claim_id {
        statement.and_where(Expr::col(ProgramRunRecords::ExecutionClaimId).eq(claim_id));
    } else {
        statement.and_where(Expr::col(ProgramRunRecords::TerminationKind).is_not_null());
    }
    if transaction.execute(statement).await? != 1 {
        return Err(Error::RunEventPersistence(
            "Program Run no longer accepts terminal Event evidence".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn event_page_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    after_sequence: u64,
    limit: u16,
    limits: coral_value::ValueLimits,
) -> Result<(Vec<RunEvent>, bool), Error> {
    event_page(
        transaction,
        workspace_id,
        run_id,
        after_sequence,
        limit,
        limits,
        false,
    )
    .await
}

pub(crate) async fn output_page_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    after_sequence: u64,
    limit: u16,
    limits: coral_value::ValueLimits,
) -> Result<(Vec<RunEvent>, bool), Error> {
    event_page(
        transaction,
        workspace_id,
        run_id,
        after_sequence,
        limit,
        limits,
        true,
    )
    .await
}

async fn event_page(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    after_sequence: u64,
    limit: u16,
    limits: coral_value::ValueLimits,
    authored_only: bool,
) -> Result<(Vec<RunEvent>, bool), Error> {
    let after_sequence = i64::try_from(after_sequence)
        .map_err(|_error| Error::InvalidInput("Run cursor is invalid".to_string()))?;
    let mut statement = Query::select()
        .columns([
            ProgramRunEvents::Sequence,
            ProgramRunEvents::EventType,
            ProgramRunEvents::OccurredAtUnixNanos,
            ProgramRunEvents::ActorPrincipalId,
            ProgramRunEvents::ActorPrincipalKind,
            ProgramRunEvents::TerminalState,
            ProgramRunEvents::InvocationId,
            ProgramRunEvents::ResultKind,
            ProgramRunEvents::ProgramValueJson,
            ProgramRunEvents::LogLevel,
            ProgramRunEvents::LogMessage,
        ])
        .from(ProgramRunEvents::Table)
        .and_where(Expr::col(ProgramRunEvents::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(ProgramRunEvents::RunId).eq(run_id.to_string()))
        .and_where(Expr::col(ProgramRunEvents::Sequence).gt(after_sequence))
        .order_by(ProgramRunEvents::Sequence, Order::Asc)
        .limit(u64::from(limit) + 1)
        .to_owned();
    if authored_only {
        // Fixed persistence encoding matches the partial index on both databases.
        // Hidden lifecycle payloads are never loaded to produce an output page.
        statement.and_where(Expr::cust(
            "event_type IN ('program.emitted', 'program.logged')",
        ));
    }
    let rows: Vec<EventRow> = transaction.fetch_all(statement).await?;
    let mut events = rows
        .into_iter()
        .map(|row| row.decode(limits))
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = events.len() > usize::from(limit);
    events.truncate(usize::from(limit));
    Ok((events, has_more))
}
