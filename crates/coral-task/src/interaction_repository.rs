use crate::{
    AttemptEvidence, AttemptFailure, AttemptKind, AttemptOutcome, AttributedCommand, Error,
    IntentQuery, InteractionDetail, InteractionId, InteractionSummary, TaskId, TaskIdentity,
    TaskIntent,
};
use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Condition, Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;
use std::collections::BTreeMap;

/// Bounded submission attribution for already-selected Runs. Task reauthorizes these
/// rows independently without loading Task feedback or interaction evidence.
pub(super) async fn action_run_submissions(
    tx: &mut Transaction<'_>,
    authority: &coral_workspace::HistoryAuthority,
    runs: &[&coral_run::RunSummary],
) -> Result<BTreeMap<(WorkspaceId, coral_run::RunId), (TaskIdentity, TaskIntent)>, Error> {
    use sea_query::IntoColumnRef as _;
    if runs.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut selected = Condition::any();
    for run in runs {
        selected = selected.add(
            Expr::col(TaskInteractions::WorkspaceId)
                .eq(run.workspace.to_string())
                .and(Expr::col(TaskInteractions::RunId).eq(run.id.to_string())),
        );
    }
    let mut query = Query::select();
    query
        .from(TaskInteractions::Table)
        .columns([
            TaskInteractions::WorkspaceId,
            TaskInteractions::RunId,
            TaskInteractions::TaskId,
            TaskInteractions::Intent,
        ])
        .cond_where(selected);
    coral_workspace::history_repository::restrict(
        &mut query,
        authority,
        TaskInteractions::WorkspaceId.into_column_ref(),
        TaskInteractions::PrincipalId.into_column_ref(),
        TaskInteractions::PrincipalKind.into_column_ref(),
    );
    let rows: Vec<(String, String, String, String)> = tx.fetch_all(query).await?;
    rows.into_iter()
        .map(|(workspace, run, task, intent)| {
            let workspace = WorkspaceId::parse(&workspace)?;
            Ok((
                (workspace.clone(), coral_run::RunId::parse(&run)?),
                (
                    TaskIdentity {
                        workspace,
                        task: TaskId::parse(&task)?,
                    },
                    TaskIntent::parse(&intent)?,
                ),
            ))
        })
        .collect()
}

#[derive(Iden)]
enum TaskInteractions {
    Table,
    Id,
    WorkspaceId,
    TaskId,
    PrincipalId,
    PrincipalKind,
    Kind,
    Intent,
    StartedAtUnixNanos,
    CompletedAtUnixNanos,
    Outcome,
    EvidenceJson,
    ResponseBytes,
    RunId,
    FunctionId,
}

#[derive(FromRow)]
struct SummaryRow {
    response_bytes: Option<i64>,
    workspace_id: String,
    principal_id: String,
    principal_kind: String,
    id: String,
    kind: String,
    intent: String,
    started_at_unix_nanos: i64,
    completed_at_unix_nanos: Option<i64>,
    outcome: String,
    run_id: Option<String>,
}
impl SummaryRow {
    fn decode(self) -> Result<InteractionSummary, Error> {
        let kind = decode_kind(&self.kind)?;
        let outcome = match self.outcome.as_str() {
            "unknown" => AttemptOutcome::Unknown,
            "succeeded" => AttemptOutcome::Succeeded,
            "rejected" => AttemptOutcome::Rejected,
            _ => return Err(Error::CorruptData("invalid attempt outcome".into())),
        };
        Ok(InteractionSummary {
            context_impact_bytes: self.response_bytes,
            workspace: WorkspaceId::parse(&self.workspace_id)?,
            principal: coral_workspace::Principal::from_parts(
                coral_workspace::PrincipalId::parse(&self.principal_id)?,
                coral_workspace::PrincipalKind::from_storage_str(&self.principal_kind)?,
            ),
            id: InteractionId::parse(&self.id)?,
            kind,
            intent: TaskIntent::parse(&self.intent)?,
            outcome,
            started_at_unix_nanos: self.started_at_unix_nanos,
            completed_at_unix_nanos: self.completed_at_unix_nanos,
            run: self
                .run_id
                .as_deref()
                .map(coral_run::RunId::parse)
                .transpose()?,
        })
    }
}

pub(super) async fn insert(
    transaction: &mut Transaction<'_>,
    call: &AttributedCommand,
    id: &InteractionId,
    kind: AttemptKind,
    now: i64,
) -> Result<(), Error> {
    transaction
        .execute_change(
            Query::insert()
                .into_table(TaskInteractions::Table)
                .columns([
                    TaskInteractions::Id,
                    TaskInteractions::WorkspaceId,
                    TaskInteractions::TaskId,
                    TaskInteractions::PrincipalId,
                    TaskInteractions::PrincipalKind,
                    TaskInteractions::Kind,
                    TaskInteractions::Intent,
                    TaskInteractions::StartedAtUnixNanos,
                    TaskInteractions::Outcome,
                ])
                .values_panic([
                    Expr::val(id.to_string()),
                    Expr::val(call.workspace_id.to_string()),
                    Expr::val(call.task_id.to_string()),
                    Expr::val(call.actor.principal().id().as_str()),
                    Expr::val(call.actor.principal().kind().as_storage_str()),
                    Expr::val(encode_kind(kind)),
                    Expr::val(call.intent.as_str()),
                    Expr::val(now),
                    Expr::val("unknown"),
                ])
                .to_owned(),
        )
        .await?;
    Ok(())
}

pub(super) async fn complete(
    transaction: &mut Transaction<'_>,
    call: &AttributedCommand,
    id: &InteractionId,
    evidence: &AttemptEvidence,
    now: i64,
) -> Result<bool, Error> {
    let (run_id, function_id) = match evidence {
        AttemptEvidence::Run(id) => (Some(id.to_string()), None),
        AttemptEvidence::SavedFunction(id) => (None, Some(id.to_string())),
        _ => (None, None),
    };
    let encoded = encode(evidence)?;
    let response_bytes = match evidence {
        AttemptEvidence::Discovery { response, .. } => Some(
            i64::try_from(serde_json::to_vec(response).map_err(json_error)?.len())
                .map_err(|_overflow| Error::SnapshotTooLarge)?,
        ),
        _ => None,
    };
    let changed = transaction
        .execute_change(
            Query::update()
                .table(TaskInteractions::Table)
                .values([
                    (
                        TaskInteractions::Outcome,
                        Expr::val(match evidence.outcome() {
                            AttemptOutcome::Unknown => "unknown",
                            AttemptOutcome::Succeeded => "succeeded",
                            AttemptOutcome::Rejected => "rejected",
                        }),
                    ),
                    (TaskInteractions::CompletedAtUnixNanos, Expr::val(now)),
                    (TaskInteractions::EvidenceJson, Expr::val(encoded)),
                    (TaskInteractions::ResponseBytes, Expr::val(response_bytes)),
                    (TaskInteractions::RunId, Expr::val(run_id)),
                    (TaskInteractions::FunctionId, Expr::val(function_id)),
                ])
                .cond_where(scope(&call.workspace_id, &call.task_id))
                .and_where(Expr::col(TaskInteractions::Id).eq(id.to_string()))
                .and_where(
                    Expr::col(TaskInteractions::PrincipalId).eq(call
                        .actor
                        .principal()
                        .id()
                        .as_str()),
                )
                .and_where(
                    Expr::col(TaskInteractions::PrincipalKind).eq(call
                        .actor
                        .principal()
                        .kind()
                        .as_storage_str()),
                )
                .and_where(Expr::col(TaskInteractions::Outcome).eq("unknown"))
                .and_where(Expr::col(TaskInteractions::StartedAtUnixNanos).lte(now))
                .to_owned(),
        )
        .await?;
    Ok(changed == 1)
}

pub(super) async fn count(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
    matching: Option<&IntentQuery>,
) -> Result<i64, Error> {
    let children = children(transaction.system(), workspace, task, matching);
    let (total,) = transaction
        .fetch_optional::<(i64,)>(
            Query::select()
                .expr(Expr::col(TaskInteractions::Id).count())
                .from(TaskInteractions::Table)
                .cond_where(children)
                .to_owned(),
        )
        .await?
        .ok_or_else(|| Error::CorruptData("missing interaction count".into()))?;
    Ok(total)
}

/// The anchor's ordinal among the children `matching` keeps; none for an anchor it
/// leaves out.
pub(super) async fn position(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
    id: &InteractionId,
    matching: Option<&IntentQuery>,
) -> Result<Option<u64>, Error> {
    let children = children(tx.system(), workspace, task, matching);
    let anchor: Option<SummaryRow> = tx
        .fetch_optional(
            summary_select()
                .cond_where(children.clone())
                .and_where(Expr::col(TaskInteractions::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    let Some(row) = anchor.map(SummaryRow::decode).transpose()? else {
        return Ok(None);
    };
    let (position,) = tx
        .fetch_optional::<(i64,)>(
            Query::select()
                .expr(Expr::col(TaskInteractions::Id).count())
                .from(TaskInteractions::Table)
                .cond_where(children)
                .and_where(
                    Expr::tuple([
                        Expr::col(TaskInteractions::StartedAtUnixNanos),
                        Expr::col(TaskInteractions::Id),
                    ])
                    .lt(Expr::tuple([
                        Expr::val(row.started_at_unix_nanos),
                        Expr::val(id.to_string()),
                    ])),
                )
                .to_owned(),
        )
        .await?
        .ok_or_else(|| Error::CorruptData("missing interaction position".into()))?;
    Ok(Some(u64::try_from(position).map_err(|_overflow| {
        Error::CorruptData("invalid interaction position".into())
    })?))
}

pub(super) struct ActivityPage {
    pub rows: Vec<InteractionSummary>,
    pub total: i64,
    pub offset: u64,
    pub anchor_unavailable: bool,
}
pub(super) async fn activity_page(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
    window: &crate::InteractionWindow,
    limit: u16,
    matching: Option<&IntentQuery>,
) -> Result<ActivityPage, Error> {
    use crate::InteractionWindow as W;
    let total = count(tx, workspace, task, matching).await?;
    if let W::Viewport {
        offset,
        anchor,
        preceding,
    } = window
    {
        let at = if let Some(id) = anchor {
            position(tx, workspace, task, id, matching).await?
        } else {
            None
        };
        let end = u64::try_from(total)
            .map_err(|_overflow| Error::CorruptData("invalid interaction count".into()))?;
        let requested = at.map_or(*offset, |index| index.saturating_sub(u64::from(*preceding)));
        let offset = if requested >= end {
            end.saturating_sub(u64::from(limit))
        } else {
            requested
        };
        let rows: Vec<SummaryRow> = tx
            .fetch_all(
                summary_select()
                    .cond_where(children(tx.system(), workspace, task, matching))
                    .order_by(TaskInteractions::StartedAtUnixNanos, Order::Asc)
                    .order_by(TaskInteractions::Id, Order::Asc)
                    .offset(offset)
                    .limit(u64::from(limit) + 1)
                    .to_owned(),
            )
            .await?;
        return Ok(ActivityPage {
            rows: rows
                .into_iter()
                .map(SummaryRow::decode)
                .collect::<Result<_, _>>()?,
            total,
            offset,
            anchor_unavailable: anchor.is_some() && at.is_none(),
        });
    }
    let (after, at) = match window {
        W::Start => (None, None),
        W::After(id) => (Some(id), None),
        W::At(id) => (None, Some(id)),
        W::Run(_) | W::Viewport { .. } => {
            return Err(Error::InvalidInput("unresolved interaction window".into()));
        }
    };
    let rows = page(tx, workspace, task, after, at, limit + 1, matching).await?;
    let offset = if let Some(first) = rows.first() {
        position(tx, workspace, task, &first.id, matching)
            .await?
            .unwrap_or(0)
    } else {
        0
    };
    Ok(ActivityPage {
        rows,
        total,
        offset,
        anchor_unavailable: false,
    })
}

pub(super) async fn list(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
    after: Option<&InteractionId>,
    limit: u16,
) -> Result<Vec<InteractionSummary>, Error> {
    page(transaction, workspace, task, after, None, limit, None).await
}
pub(super) async fn page(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
    after: Option<&InteractionId>,
    at: Option<&InteractionId>,
    limit: u16,
    matching: Option<&IntentQuery>,
) -> Result<Vec<InteractionSummary>, Error> {
    let mut query = summary_select();
    query.cond_where(children(transaction.system(), workspace, task, matching));
    if let Some(after) = at.or(after) {
        let row = summary(transaction, workspace, task, after)
            .await?
            .ok_or(Error::InteractionNotFound)?;
        query.cond_where(
            Condition::any()
                .add(Expr::col(TaskInteractions::StartedAtUnixNanos).gt(row.started_at_unix_nanos))
                .add(
                    Condition::all()
                        .add(
                            Expr::col(TaskInteractions::StartedAtUnixNanos)
                                .eq(row.started_at_unix_nanos),
                        )
                        .add(if at.is_some() {
                            Expr::col(TaskInteractions::Id).gte(after.to_string())
                        } else {
                            Expr::col(TaskInteractions::Id).gt(after.to_string())
                        }),
                ),
        );
    }
    query
        .order_by(TaskInteractions::StartedAtUnixNanos, Order::Asc)
        .order_by(TaskInteractions::Id, Order::Asc)
        .limit(u64::from(limit));
    let rows: Vec<SummaryRow> = transaction.fetch_all(query).await?;
    rows.into_iter().map(SummaryRow::decode).collect()
}

pub(super) async fn summary(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
    id: &InteractionId,
) -> Result<Option<InteractionSummary>, Error> {
    let row: Option<SummaryRow> = transaction
        .fetch_optional(
            summary_select()
                .cond_where(scope(workspace, task))
                .and_where(Expr::col(TaskInteractions::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(SummaryRow::decode).transpose()
}

pub(super) async fn detail(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
    id: &InteractionId,
) -> Result<InteractionDetail, Error> {
    let summary = summary(transaction, workspace, task, id)
        .await?
        .ok_or(Error::InteractionNotFound)?;
    let row: Option<(Option<String>,)> = transaction
        .fetch_optional(
            Query::select()
                .column(TaskInteractions::EvidenceJson)
                .from(TaskInteractions::Table)
                .cond_where(scope(workspace, task))
                .and_where(Expr::col(TaskInteractions::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    let evidence = row
        .ok_or(Error::InteractionNotFound)?
        .0
        .map(|json| decode(&json))
        .transpose()?;
    Ok(InteractionDetail { summary, evidence })
}

pub(super) async fn associated_work(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    task: &TaskId,
) -> Result<Vec<coral_run::RunId>, Error> {
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(TaskInteractions::RunId)
                .from(TaskInteractions::Table)
                .cond_where(scope(workspace, task))
                .and_where(Expr::col(TaskInteractions::RunId).is_not_null())
                .order_by(TaskInteractions::RunId, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| coral_run::RunId::parse(&id).map_err(Into::into))
        .collect()
}

pub(super) async fn run_interaction(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &coral_run::RunId,
) -> Result<Option<(TaskId, InteractionId)>, Error> {
    let row: Option<(String, String)> = transaction
        .fetch_optional(
            Query::select()
                .columns([TaskInteractions::TaskId, TaskInteractions::Id])
                .from(TaskInteractions::Table)
                .and_where(Expr::col(TaskInteractions::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(TaskInteractions::RunId).eq(run.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(|(task, interaction)| Ok((TaskId::parse(&task)?, InteractionId::parse(&interaction)?)))
        .transpose()
}
pub(super) async fn run_intent(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &coral_run::RunId,
) -> Result<Option<(TaskId, TaskIntent)>, Error> {
    let row: Option<(String, String)> = transaction
        .fetch_optional(
            Query::select()
                .columns([TaskInteractions::TaskId, TaskInteractions::Intent])
                .from(TaskInteractions::Table)
                .and_where(Expr::col(TaskInteractions::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(TaskInteractions::RunId).eq(run.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(|(task, intent)| Ok((TaskId::parse(&task)?, TaskIntent::parse(&intent)?)))
        .transpose()
}
pub(super) async fn run_task(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &coral_run::RunId,
) -> Result<Option<TaskId>, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(TaskInteractions::TaskId)
                .from(TaskInteractions::Table)
                .and_where(Expr::col(TaskInteractions::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(TaskInteractions::RunId).eq(run.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(|row| TaskId::parse(&row.0)).transpose()
}

fn summary_select() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            TaskInteractions::WorkspaceId,
            TaskInteractions::PrincipalId,
            TaskInteractions::PrincipalKind,
            TaskInteractions::Id,
            TaskInteractions::Kind,
            TaskInteractions::Intent,
            TaskInteractions::StartedAtUnixNanos,
            TaskInteractions::CompletedAtUnixNanos,
            TaskInteractions::Outcome,
            TaskInteractions::ResponseBytes,
            TaskInteractions::RunId,
        ])
        .from(TaskInteractions::Table)
        .to_owned()
}
fn scope(workspace: &WorkspaceId, task: &TaskId) -> Condition {
    Condition::all()
        .add(Expr::col(TaskInteractions::WorkspaceId).eq(workspace.to_string()))
        .add(Expr::col(TaskInteractions::TaskId).eq(task.to_string()))
}
/// The Task's children, or with `matching`, only its Run submissions whose
/// intent contains the query.
fn children(
    system: coral_db::DatabaseSystem,
    workspace: &WorkspaceId,
    task: &TaskId,
    matching: Option<&IntentQuery>,
) -> Condition {
    let scope = scope(workspace, task);
    match matching {
        None => scope,
        Some(query) => scope
            .add(Expr::col(TaskInteractions::RunId).is_not_null())
            .add(crate::repository::activity::intent_contains(
                system,
                "task_interactions",
                query,
            )),
    }
}

pub(crate) fn encode_kind(kind: AttemptKind) -> &'static str {
    match kind {
        AttemptKind::Search => "search",
        AttemptKind::Describe => "describe",
        AttemptKind::Save => "save",
        AttemptKind::Delete => "delete",
        AttemptKind::Submit => "submit",
    }
}

fn decode_kind(kind: &str) -> Result<AttemptKind, Error> {
    match kind {
        "search" => Ok(AttemptKind::Search),
        "describe" => Ok(AttemptKind::Describe),
        "save" => Ok(AttemptKind::Save),
        "delete" => Ok(AttemptKind::Delete),
        "submit" => Ok(AttemptKind::Submit),
        _ => Err(Error::CorruptData("invalid attempt kind".into())),
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredFailure {
    code: String,
    message: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StoredEvidence {
    Discovery {
        context: StoredDiscoveryContext,
        response: serde_json::Value,
    },
    Run {
        id: String,
    },
    SavedFunction {
        id: String,
    },
    Rejected {
        failure: StoredFailure,
    },
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StoredDiscoveryContext {
    Search {
        query: Option<String>,
        provider: Option<String>,
        limit: u16,
        cursor: Option<String>,
    },
    Describe {
        call_path: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        types: Vec<String>,
    },
}
impl From<&crate::DiscoveryContext> for StoredDiscoveryContext {
    fn from(context: &crate::DiscoveryContext) -> Self {
        match context {
            crate::DiscoveryContext::Search {
                query,
                provider,
                limit,
                cursor,
            } => Self::Search {
                query: query.clone(),
                provider: provider.clone(),
                limit: *limit,
                cursor: cursor.clone(),
            },
            crate::DiscoveryContext::Describe { call_path, types } => Self::Describe {
                call_path: call_path.clone(),
                types: types.clone(),
            },
        }
    }
}
impl From<StoredDiscoveryContext> for crate::DiscoveryContext {
    fn from(context: StoredDiscoveryContext) -> Self {
        match context {
            StoredDiscoveryContext::Search {
                query,
                provider,
                limit,
                cursor,
            } => Self::Search {
                query,
                provider,
                limit,
                cursor,
            },
            StoredDiscoveryContext::Describe { call_path, types } => {
                Self::Describe { call_path, types }
            }
        }
    }
}
fn encode(evidence: &AttemptEvidence) -> Result<String, Error> {
    let stored = match evidence {
        AttemptEvidence::Discovery { context, response } => StoredEvidence::Discovery {
            context: context.into(),
            response: response.clone(),
        },
        AttemptEvidence::Run(id) => StoredEvidence::Run { id: id.to_string() },
        AttemptEvidence::SavedFunction(id) => StoredEvidence::SavedFunction { id: id.to_string() },
        AttemptEvidence::Rejected(failure) => StoredEvidence::Rejected {
            failure: StoredFailure {
                code: failure.code.clone(),
                message: failure.message.clone(),
            },
        },
    };
    let encoded = serde_json::to_string(&stored).map_err(json_error)?;
    // Search bounds the complete response; allow the separately bounded request context
    // and record envelope without imposing a smaller historical-response limit.
    if encoded.len() > coral_search::SEARCH_PAGE_MAX_BYTES + 64 * 1024 {
        return Err(Error::SnapshotTooLarge);
    }
    Ok(encoded)
}
fn decode(encoded: &str) -> Result<AttemptEvidence, Error> {
    Ok(match serde_json::from_str(encoded).map_err(json_error)? {
        StoredEvidence::Discovery { context, response } => AttemptEvidence::Discovery {
            context: context.into(),
            response,
        },
        StoredEvidence::Run { id } => AttemptEvidence::Run(coral_run::RunId::parse(&id)?),
        StoredEvidence::SavedFunction { id } => {
            AttemptEvidence::SavedFunction(coral_functions::SavedFunctionId::parse(&id)?)
        }
        StoredEvidence::Rejected { failure } => AttemptEvidence::Rejected(AttemptFailure {
            code: failure.code,
            message: failure.message,
        }),
    })
}
#[cfg(test)]
pub(crate) async fn inject_fault(
    transaction: &mut Transaction<'_>,
    deferred: bool,
) -> Result<(), Error> {
    use sea_query::Alias;
    transaction
        .execute(
            Query::insert()
                .into_table(Alias::new("task_test_fault_child"))
                .columns([Alias::new("id"), Alias::new("parent_id")])
                .values_panic([Expr::val(if deferred { 1 } else { -1 }), Expr::val(1)])
                .to_owned(),
        )
        .await?;
    Ok(())
}

fn json_error(_: serde_json::Error) -> Error {
    Error::CorruptData("invalid Task evidence encoding".into())
}
