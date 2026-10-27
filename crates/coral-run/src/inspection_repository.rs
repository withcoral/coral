use crate::{Error, InvocationDetail, InvocationId, RequiredActionRequest, RunId, SourceSnapshot};
use coral_db::{Transaction, column};
use coral_workspace::WorkspaceId;
use sea_query::{Alias, Expr, ExprTrait as _, Order, Query};

pub(crate) async fn approval_labels(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    invocation: &InvocationId,
) -> Result<(Option<String>, Option<String>), Error> {
    let mut select = Query::select();
    select
        .from(Alias::new("tool_invocations"))
        .join_as(
            sea_query::JoinType::InnerJoin,
            Alias::new("program_run_records"),
            Alias::new("r"),
            Expr::col(column("r", "workspace_id"))
                .eq(Expr::col(column("tool_invocations", "workspace_id")))
                .and(
                    Expr::col(column("r", "id"))
                        .eq(Expr::col(column("tool_invocations", "run_id"))),
                ),
        )
        .expr(coral_provider::history_repository::display_label(
            column("tool_invocations", "workspace_id"),
            column("tool_invocations", "provider_id"),
        ))
        .expr(coral_workspace::history_repository::agent_label(
            column("r", "workspace_id"),
            column("r", "principal_id"),
        ));
    select
        .and_where(Expr::col(column("tool_invocations", "workspace_id")).eq(workspace.to_string()))
        .and_where(Expr::col(column("tool_invocations", "id")).eq(invocation.to_string()));
    tx.fetch_optional(select).await?.ok_or(Error::RunNotFound)
}

fn scoped(
    table: &str,
    workspace: &WorkspaceId,
    id_field: &str,
    id: String,
) -> sea_query::SelectStatement {
    Query::select()
        .from(Alias::new(table))
        .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.to_string()))
        .and_where(Expr::col(Alias::new(id_field)).eq(id))
        .to_owned()
}
/// Exact retained association only: current waiter attachment may already be cleared.
pub(crate) async fn has_account_connection(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    request: coral_provider::account::ConnectionRequestId,
) -> Result<bool, Error> {
    let mut select = scoped("tool_invocations", workspace, "run_id", run.to_string());
    select
        .expr(Expr::val(1_i64))
        .and_where(
            Expr::col(Alias::new("retained_account_connection_request_id")).eq(request.to_string()),
        )
        .limit(1);
    Ok(tx.fetch_optional::<(i64,)>(select).await?.is_some())
}

pub(crate) async fn sources(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
) -> Result<(String, Vec<SourceSnapshot>), Error> {
    let mut source = scoped("program_run_packages", workspace, "run_id", run.to_string());
    source.column(Alias::new("submitted_source"));
    let (source,) = tx
        .fetch_optional::<(String,)>(source)
        .await?
        .ok_or_else(|| Error::CorruptData("missing accepted source".into()))?;
    let mut snapshots = scoped(
        "program_run_saved_function_snapshots",
        workspace,
        "run_id",
        run.to_string(),
    );
    snapshots
        .columns([
            Alias::new("saved_function_id"),
            Alias::new("path"),
            Alias::new("source"),
        ])
        .order_by(Alias::new("path"), Order::Asc)
        .limit(crate::LINKED_SAVED_FUNCTION_MAX_COUNT as u64 + 1);
    let rows: Vec<(String, String, String)> = tx.fetch_all(snapshots).await?;
    if rows.len() > crate::LINKED_SAVED_FUNCTION_MAX_COUNT {
        return Err(Error::CorruptData(
            "accepted linked source exceeds its admission bound".into(),
        ));
    }
    Ok((
        source,
        rows.into_iter()
            .map(|(saved_function_id, path, source)| SourceSnapshot {
                saved_function_id,
                path,
                source,
            })
            .collect(),
    ))
}
pub(crate) async fn invocation_run(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    id: &InvocationId,
) -> Result<Option<RunId>, Error> {
    let mut select = scoped("tool_invocations", workspace, "id", id.to_string());
    select.column(Alias::new("run_id"));
    tx.fetch_optional::<(String,)>(select)
        .await?
        .map(|(id,)| RunId::parse(&id))
        .transpose()
}
#[derive(sqlx::FromRow)]
struct InvocationEvidence {
    arguments_json: String,
    result_json: Option<String>,
    failure_code: Option<String>,
    failure_message: Option<String>,
    retained_account_connection_request_id: Option<String>,
}
pub(crate) async fn invocation(
    tx: &mut Transaction<'_>,
    run: &crate::lifecycle::RunRecord,
    id: &InvocationId,
    now: i64,
) -> Result<InvocationDetail, Error> {
    let summary =
        crate::history_repository::exact_invocation(tx, &run.workspace_id, &run.id, id, now)
            .await?;
    let mut select = scoped("tool_invocations", &run.workspace_id, "id", id.to_string());
    select
        .and_where(Expr::col(Alias::new("run_id")).eq(run.id.to_string()))
        .columns([
            Alias::new("arguments_json"),
            Alias::new("result_json"),
            Alias::new("failure_code"),
            Alias::new("failure_message"),
            Alias::new("retained_account_connection_request_id"),
        ]);
    let row: InvocationEvidence = tx.fetch_optional(select).await?.ok_or(Error::RunNotFound)?;
    let mut requests = Vec::new();
    if let Some(id) = row.retained_account_connection_request_id {
        requests.push(RequiredActionRequest::AccountConnection(
            coral_provider::account::ConnectionRequestId::parse(&id)?,
        ));
    }
    if let Some(approval) =
        crate::approval_repository::find_for_run_invocation(tx, &run.workspace_id, &run.id, id)
            .await?
    {
        requests.push(RequiredActionRequest::InvocationApproval(approval.id));
    }
    let outcome = match summary.outcome {
        None => None,
        Some(crate::InvocationResultKind::UpstreamOutcome) => {
            Some(crate::InvocationOutcome::UpstreamOutcome(decode(
                row.result_json
                    .as_deref()
                    .ok_or_else(|| Error::CorruptData("missing upstream result".into()))?,
            )?))
        }
        Some(crate::InvocationResultKind::CoralFailure) => {
            Some(crate::InvocationOutcome::CoralFailure {
                code: row
                    .failure_code
                    .ok_or_else(|| Error::CorruptData("missing Coral Failure code".into()))?,
                message: row
                    .failure_message
                    .ok_or_else(|| Error::CorruptData("missing Coral Failure message".into()))?,
            })
        }
        Some(crate::InvocationResultKind::RunTerminated) => {
            Some(crate::InvocationOutcome::RunTerminated)
        }
        Some(crate::InvocationResultKind::Indeterminate) => {
            Some(crate::InvocationOutcome::Indeterminate)
        }
    };
    Ok(InvocationDetail {
        summary,
        arguments: decode(&row.arguments_json)?,
        outcome,
        requests,
    })
}
pub(crate) async fn entry_start(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    window: &crate::RunEntryWindow,
) -> Result<u64, Error> {
    use crate::RunEntryWindow as W;
    let mut query = scoped("program_run_events", workspace, "run_id", run.to_string());
    query.column(Alias::new("sequence"));
    match window {
        W::Viewport { .. } => return Err(Error::InvalidInput("unresolved entry viewport".into())),
        W::Start => return Ok(0),
        W::After(sequence) => return Ok(*sequence),
        W::Event(sequence) => {
            query.and_where(
                Expr::col(Alias::new("sequence")).eq(i64::try_from(*sequence)
                    .map_err(|_overflow| Error::InvalidInput("invalid Event sequence".into()))?),
            );
        }
        W::Invocation(id) => {
            query
                .and_where(Expr::col(Alias::new("invocation_id")).eq(id.to_string()))
                .and_where(Expr::col(Alias::new("event_type")).eq("invocation.started"));
        }
        W::Final => {
            query.and_where(Expr::col(Alias::new("event_type")).eq("run.ended"));
        }
    }
    let (sequence,): (i64,) = tx.fetch_optional(query).await?.ok_or(Error::RunNotFound)?;
    u64::try_from(sequence - 1)
        .map_err(|_invalid| Error::CorruptData("invalid Event sequence".into()))
}
pub(crate) async fn event_context_bytes(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    sequence: u64,
) -> Result<i64, Error> {
    let mut query = scoped("program_run_events", workspace, "run_id", run.to_string());
    query.column(Alias::new("output_bytes")).and_where(
        Expr::col(Alias::new("sequence")).eq(i64::try_from(sequence)
            .map_err(|_overflow| Error::InvalidInput("invalid Event sequence".into()))?),
    );
    let (bytes,) = tx.fetch_optional(query).await?.ok_or(Error::RunNotFound)?;
    Ok(bytes)
}

pub(crate) struct EntrySummary {
    pub sequence: u64,
    pub kind: String,
    pub occurred_at: i64,
    pub invocation: Option<InvocationId>,
    pub context_impact_bytes: i64,
    pub source_location: Option<crate::SourceLocation>,
}
#[derive(sqlx::FromRow)]
struct EntryRow {
    sequence: i64,
    event_type: String,
    occurred_at_unix_nanos: i64,
    invocation_id: Option<String>,
    output_bytes: i64,
    #[sqlx(flatten)]
    source_location: crate::source_location_repository::StoredSourceLocation,
}
fn entry_view(row: EntryRow, exact_event: Option<i64>) -> Result<EntrySummary, Error> {
    let EntryRow {
        sequence,
        event_type: kind,
        occurred_at_unix_nanos: occurred_at,
        invocation_id: invocation,
        output_bytes: context_impact_bytes,
        source_location,
    } = row;
    Ok(EntrySummary {
        source_location: source_location.decode()?,
        sequence: u64::try_from(sequence)
            .map_err(|_overflow| Error::CorruptData("invalid Event sequence".into()))?,
        invocation: if kind == "invocation.started" && exact_event != Some(sequence) {
            Some(InvocationId::parse(&invocation.ok_or_else(|| {
                Error::CorruptData("missing Invocation reference".into())
            })?)?)
        } else {
            None
        },
        kind,
        occurred_at,
        context_impact_bytes,
    })
}
pub(crate) struct EntryPage {
    pub rows: Vec<EntrySummary>,
    pub total: i64,
    pub offset: u64,
    pub anchor_unavailable: bool,
    pub after: u64,
}
pub(crate) async fn entry_summaries(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    window: &crate::RunEntryWindow,
    limit: u16,
) -> Result<EntryPage, Error> {
    use crate::RunEntryWindow as W;
    let resolved;
    let lookup = match window {
        W::Viewport { anchor, .. } => {
            resolved = anchor.as_ref().map_or(W::Start, W::from);
            &resolved
        }
        other => other,
    };
    let start = entry_start(tx, workspace, run, lookup).await;
    let anchor_unavailable = matches!(
        window,
        W::Viewport {
            anchor: Some(_),
            ..
        }
    ) && matches!(start, Err(Error::RunNotFound));
    let after_sequence = if anchor_unavailable { 0 } else { start? };
    let include_anchor = matches!(lookup, W::Event(_)) && !anchor_unavailable;
    let after = i64::try_from(after_sequence)
        .map_err(|_overflow| Error::InvalidInput("invalid Event cursor".into()))?;
    let mut query = scoped("program_run_events", workspace, "run_id", run.to_string());
    query
        .columns([
            Alias::new("sequence"),
            Alias::new("event_type"),
            Alias::new("occurred_at_unix_nanos"),
            Alias::new("invocation_id"),
            Alias::new("output_bytes"),
        ])
        .columns(crate::source_location_repository::COLUMNS.map(Alias::new))
        .and_where(
            Expr::col(Alias::new("event_type"))
                .is_not_in(["invocation.ended", "run.started", "run.ended"])
                .or(Expr::col(Alias::new("event_type"))
                    .eq("run.ended")
                    .and(Expr::col(Alias::new("output_bytes")).gt(0_i64)))
                .or(Expr::val(include_anchor)
                    .and(Expr::col(Alias::new("sequence")).eq(after.saturating_add(1)))),
        );
    let mut count = query.clone();
    count
        .clear_selects()
        .expr(Expr::col(Alias::new("sequence")).count());
    let (total,) = tx
        .fetch_optional::<(i64,)>(count)
        .await?
        .ok_or_else(|| Error::CorruptData("missing entry count".into()))?;
    let mut prefix = query.clone();
    prefix
        .clear_selects()
        .expr(Expr::col(Alias::new("sequence")).count())
        .and_where(Expr::col(Alias::new("sequence")).lte(after));
    let (position,) = tx
        .fetch_optional::<(i64,)>(prefix)
        .await?
        .ok_or_else(|| Error::CorruptData("missing entry position".into()))?;
    let position = u64::try_from(position)
        .map_err(|_overflow| Error::CorruptData("invalid entry position".into()))?;
    let offset = match window {
        W::Viewport {
            offset,
            anchor,
            preceding,
        } => {
            let requested = if anchor.is_some() && !anchor_unavailable {
                position.saturating_sub(u64::from(*preceding))
            } else {
                *offset
            };
            let end = u64::try_from(total)
                .map_err(|_overflow| Error::CorruptData("invalid entry count".into()))?;
            if requested >= end {
                end.saturating_sub(u64::from(limit))
            } else {
                requested
            }
        }
        _ => position,
    };
    query
        .offset(offset)
        .order_by(Alias::new("sequence"), Order::Asc)
        .limit(u64::from(limit) + 1);
    let rows: Vec<EntryRow> = tx.fetch_all(query).await?;
    let exact_event = include_anchor.then_some(after.saturating_add(1));
    let rows = rows
        .into_iter()
        .map(|row| entry_view(row, exact_event))
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(EntryPage {
        rows,
        total,
        offset,
        anchor_unavailable,
        after: after_sequence,
    })
}
fn decode(value: &str) -> Result<serde_json::Value, Error> {
    serde_json::from_str(value)
        .map_err(|_invalid| Error::CorruptData("invalid retained Invocation value".into()))
}
