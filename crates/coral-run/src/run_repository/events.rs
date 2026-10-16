//! Sequenced Run Events and their pages.

use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;

use super::principal_kind_from_storage;
use crate::{
    Error, InvocationId, InvocationResultKind, PROGRAM_LOG_MESSAGE_MAX_BYTES, ProgramLogLevel,
    ProgramValue, RunEvent, RunEventKind, RunId, RunState,
};

#[derive(Iden, Clone, Copy, PartialEq, Eq)]
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

/// Appends `events` after the Run's last Event, in multi-row statements of at
/// most [`EVENT_INSERT_ROWS`] rows each. Only [`super::commit_chunk`] and
/// acceptance's `run.started` write Events.
///
/// Each statement numbers its rows from `MAX(sequence)` read in that same
/// statement. The caller holds the Run row, and a statement that starts after
/// the hold sees every Event the previous holder committed; a value read
/// beside the hold itself would not, since Postgres gives a blocked `UPDATE`
/// its snapshot before the wait.
pub(super) async fn insert_events(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    events: &[super::ChunkEvent<'_>],
) -> Result<(), Error> {
    use sea_query::{Alias, ValueTuple};

    let rows = Alias::new("chunk");
    let last = Alias::new("last");
    let column = |index: usize| Expr::col((rows.clone(), Alias::new(format!("column{index}"))));
    for batch in events.chunks(EVENT_INSERT_ROWS) {
        let tuples = batch
            .iter()
            .enumerate()
            .map(|(offset, event)| {
                let offset = i64::try_from(offset + 1).map_err(|_overflow| {
                    Error::CorruptData("Run Event sequence overflowed".to_string())
                })?;
                event_row(
                    workspace_id,
                    run_id,
                    offset,
                    &event.event,
                    event.occurred_at_unix_nanos,
                )
                .map(|values| ValueTuple::Many(values.to_vec()))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let last_sequence = Query::select()
            .expr_as(
                Expr::expr(Expr::col(ProgramRunEvents::Sequence).max()).if_null(0_i64),
                Alias::new("sequence"),
            )
            .from(ProgramRunEvents::Table)
            .and_where(Expr::col(ProgramRunEvents::WorkspaceId).eq(workspace_id.to_string()))
            .and_where(Expr::col(ProgramRunEvents::RunId).eq(run_id.to_string()))
            .to_owned();
        let mut select = Query::select();
        for (index, event_column) in EVENT_COLUMNS.iter().enumerate() {
            if *event_column == ProgramRunEvents::Sequence {
                select
                    .expr(Expr::col((last.clone(), Alias::new("sequence"))).add(column(index + 1)));
            } else {
                select.expr(column(index + 1));
            }
        }
        select
            .from_values(tuples, rows.clone())
            .from_subquery(last_sequence, last.clone());
        let statement = Query::insert()
            .into_table(ProgramRunEvents::Table)
            .columns(EVENT_COLUMNS)
            .select_from(select)
            .map_err(|error| Error::CorruptData(format!("Run Event insert is malformed: {error}")))?
            .to_owned();
        transaction.execute_change(statement).await?;
    }
    Ok(())
}

/// Rows per Event insert statement: the ADR's chunk cap, so a capped chunk is
/// one statement and a larger one stays under both backends' bind limits.
const EVENT_INSERT_ROWS: usize = 1000;

/// The Event columns, in the order the insert and its value rows use. The
/// `Sequence` entry carries the row's offset within its statement.
const EVENT_COLUMNS: [ProgramRunEvents; 14] = [
    ProgramRunEvents::WorkspaceId,
    ProgramRunEvents::RunId,
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
    ProgramRunEvents::OutputBytes,
];

/// The columns one Event kind owns. The table CHECK keeps the others NULL.
#[derive(Default)]
struct EventPayload<'a> {
    actor: Option<&'a Principal>,
    terminal_state: Option<&'static str>,
    invocation: Option<&'a InvocationId>,
    result_kind: Option<&'static str>,
    value: Option<&'a str>,
    log: Option<(&'static str, &'a str)>,
}

/// One Event row in [`EVENT_COLUMNS`] order, with `offset` in the sequence slot.
fn event_row(
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    offset: i64,
    event: &NewEvent<'_>,
    occurred_at_unix_nanos: i64,
) -> Result<[sea_query::Value; 14], Error> {
    let output_bytes = match event {
        NewEvent::Emitted(value) => value.encoded_json().len(),
        NewEvent::Logged(_, message) => message.len(),
        NewEvent::Ended(_, bytes) => *bytes,
        _ => 0,
    };
    let output_bytes = i64::try_from(output_bytes)
        .map_err(|_overflow| Error::CorruptData("Event byte count overflow".into()))?;
    let (event_type, payload) = match event {
        NewEvent::Started => ("run.started", EventPayload::default()),
        NewEvent::CancellationRequested(principal) => (
            "cancellation.requested",
            EventPayload {
                actor: Some(principal),
                ..EventPayload::default()
            },
        ),
        NewEvent::Ended(state, _) => (
            "run.ended",
            EventPayload {
                terminal_state: Some(state.as_storage_str()),
                ..EventPayload::default()
            },
        ),
        NewEvent::InvocationStarted(id) => (
            "invocation.started",
            EventPayload {
                invocation: Some(id),
                ..EventPayload::default()
            },
        ),
        NewEvent::InvocationEnded(id, kind) => (
            "invocation.ended",
            EventPayload {
                invocation: Some(id),
                result_kind: Some(kind.as_storage_str()),
                ..EventPayload::default()
            },
        ),
        NewEvent::Emitted(value) => (
            "program.emitted",
            EventPayload {
                value: Some(value.encoded_json()),
                ..EventPayload::default()
            },
        ),
        NewEvent::Logged(level, message) => (
            "program.logged",
            EventPayload {
                log: Some((level.as_storage_str(), message)),
                ..EventPayload::default()
            },
        ),
    };
    let text = |value: Option<&str>| sea_query::Value::from(value.map(str::to_string));
    Ok([
        workspace_id.to_string().into(),
        run_id.to_string().into(),
        offset.into(),
        event_type.into(),
        occurred_at_unix_nanos.into(),
        text(payload.actor.map(|principal| principal.id().as_str())),
        text(
            payload
                .actor
                .map(|principal| principal.kind().as_storage_str()),
        ),
        text(payload.terminal_state),
        text(payload.invocation.map(ToString::to_string).as_deref()),
        text(payload.result_kind),
        text(payload.value),
        text(payload.log.map(|(level, _)| level)),
        text(payload.log.map(|(_, message)| message)),
        output_bytes.into(),
    ])
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
