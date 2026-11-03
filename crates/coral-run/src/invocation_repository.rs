use crate::lifecycle::RunRecord;
use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Iden, IntoIden, Order, Query, SelectStatement, SimpleExpr};
use sqlx::FromRow;

use crate::invocation::{AdmittedInvocation, InvocationCompletion};
use crate::invocation_lifecycle::{InvocationState, UnfinishedInvocation};
use crate::{CoralFailure, Error, InvocationId, InvocationResultKind, RunId};

#[derive(Iden)]
enum ToolInvocations {
    Table,
    Id,
    WorkspaceId,
    RunId,
    ProviderId,
    ProviderAccountSettingVersion,
    CredentialRoute,
    ProviderAccountId,
    AccountAuthorizationVersion,
    AccountConnectionRequestId,
    RetainedAccountConnectionRequestId,
    AccountWaitStartedAtUnixNanos,
    ProviderGenerationId,
    ToolKey,
    CallPath,
    ArgumentsJson,
    State,
    AdmittedAtUnixNanos,
    DispatchStartedAtUnixNanos,
    EndedAtUnixNanos,
    ResultKind,
    ResultJson,
    ResultBytes,
    FailureCode,
    FailureMessage,
}

impl InvocationState {
    const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Dispatching => "dispatching",
            Self::Ended => "ended",
        }
    }

    fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "pending" => Ok(Self::Pending),
            "dispatching" => Ok(Self::Dispatching),
            "ended" => Ok(Self::Ended),
            _ => Err(Error::CorruptData(
                "Invocation has invalid nonterminal state".to_string(),
            )),
        }
    }
}

/// Inserts the admitted Invocation's row; its `invocation.started` rides the
/// same Run Ledger chunk.
pub(super) async fn insert_admission(
    transaction: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
) -> Result<(), Error> {
    let (location_columns, location_values): (Vec<_>, Vec<_>) =
        crate::source_location_repository::values(admitted.source_location.as_ref())
            .into_iter()
            .unzip();
    let statement = Query::insert()
        .into_table(ToolInvocations::Table)
        .columns(
            [
                ToolInvocations::Id,
                ToolInvocations::WorkspaceId,
                ToolInvocations::RunId,
                ToolInvocations::ProviderId,
                ToolInvocations::ProviderAccountSettingVersion,
                ToolInvocations::CredentialRoute,
                ToolInvocations::ProviderGenerationId,
                ToolInvocations::ToolKey,
                ToolInvocations::CallPath,
                ToolInvocations::ArgumentsJson,
                ToolInvocations::State,
                ToolInvocations::AdmittedAtUnixNanos,
            ]
            .into_iter()
            .map(IntoIden::into_iden)
            .chain(location_columns.into_iter().map(IntoIden::into_iden)),
        )
        .values_panic(
            [
                Expr::val(admitted.id.to_string()),
                Expr::val(admitted.workspace_id.to_string()),
                Expr::val(admitted.run_id.to_string()),
                Expr::val(admitted.target.provider_access().id().to_string()),
                Expr::val(admitted.target.provider_access().version()),
                Expr::val(
                    admitted
                        .target
                        .provider_access()
                        .credential_route()
                        .as_storage_str(),
                ),
                Expr::val(admitted.target.provider_generation_id().to_string()),
                Expr::val(admitted.target.tool().key().as_str()),
                Expr::val(&admitted.call_path),
                Expr::val(&admitted.arguments_json),
                Expr::val(InvocationState::Pending.as_storage_str()),
                Expr::val(admitted.admitted_at_unix_nanos),
            ]
            .into_iter()
            .chain(location_values),
        )
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) async fn connection_requests_for_run(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
) -> Result<Vec<coral_provider::account::ConnectionRequestId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .distinct()
                .column(ToolInvocations::AccountConnectionRequestId)
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(run.workspace_id.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run.id.to_string()))
                .and_where(
                    Expr::col(ToolInvocations::State).eq(InvocationState::Pending.as_storage_str()),
                )
                .and_where(Expr::col(ToolInvocations::AccountConnectionRequestId).is_not_null())
                .order_by(ToolInvocations::AccountConnectionRequestId, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| Ok(coral_provider::account::ConnectionRequestId::parse(&id)?))
        .collect()
}

/// Latest account-connection activity for a Run, selected as `at`.
pub(super) fn connection_activity(workspace: &WorkspaceId, run: &RunId) -> SelectStatement {
    use sea_query::{Alias, IntoColumnRef as _};
    let column = |name: &str| (Alias::new("waiting"), Alias::new(name)).into_column_ref();
    let activity = coral_provider::account::history_repository::connection_completion(
        column("workspace_id"),
        column("retained_account_connection_request_id"),
        column("account_wait_started_at_unix_nanos"),
        column("ended_at_unix_nanos"),
    );
    Query::select()
        .expr_as(Expr::expr(activity).max(), Alias::new("at"))
        .from_as(ToolInvocations::Table, Alias::new("waiting"))
        .and_where(Expr::col(column("workspace_id")).eq(workspace.to_string()))
        .and_where(Expr::col(column("run_id")).eq(run.to_string()))
        .to_owned()
}

pub(super) async fn attach_connection(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    invocation: &InvocationId,
    request: coral_provider::account::ConnectionRequestId,
    now: i64,
) -> Result<(), Error> {
    let changed = tx
        .execute_change(
            Query::update()
                .table(ToolInvocations::Table)
                .values([
                    (
                        ToolInvocations::AccountWaitStartedAtUnixNanos,
                        Expr::val(now),
                    ),
                    (
                        ToolInvocations::AccountConnectionRequestId,
                        Expr::val(request.to_string()),
                    ),
                    (
                        ToolInvocations::RetainedAccountConnectionRequestId,
                        Expr::val(request.to_string()),
                    ),
                ])
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ToolInvocations::Id).eq(invocation.to_string()))
                .and_where(
                    Expr::col(ToolInvocations::State).eq(InvocationState::Pending.as_storage_str()),
                )
                .and_where(Expr::col(ToolInvocations::AccountConnectionRequestId).is_null())
                .to_owned(),
        )
        .await?;
    if changed != 1 {
        return Err(Error::ProgramHostTerminated);
    }
    Ok(())
}

pub(super) async fn retain_credentials(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    invocation: &InvocationId,
    evidence: &coral_provider::account::CredentialEvidence,
) -> Result<(), Error> {
    let changed = tx
        .execute_change(
            Query::update()
                .table(ToolInvocations::Table)
                .values([
                    (
                        ToolInvocations::ProviderAccountId,
                        Expr::val(evidence.account_id.to_string()),
                    ),
                    (
                        ToolInvocations::AccountAuthorizationVersion,
                        Expr::val(&evidence.authorization_version),
                    ),
                    (
                        ToolInvocations::AccountConnectionRequestId,
                        Expr::val(Option::<String>::None),
                    ),
                ])
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ToolInvocations::Id).eq(invocation.to_string()))
                .and_where(
                    Expr::col(ToolInvocations::State).eq(InvocationState::Pending.as_storage_str()),
                )
                .and_where(Expr::col(ToolInvocations::AccountAuthorizationVersion).is_null())
                .to_owned(),
        )
        .await?;
    if changed != 1 {
        return Err(Error::ProgramHostTerminated);
    }
    Ok(())
}

#[cfg(test)]
pub(super) async fn has_dispatch_start(
    database: &coral_db::Database,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    invocation_id: &InvocationId,
) -> Result<bool, Error> {
    let row: Option<(Option<i64>,)> = database
        .fetch_optional(
            Query::select()
                .column(ToolInvocations::DispatchStartedAtUnixNanos)
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run_id.to_string()))
                .and_where(Expr::col(ToolInvocations::Id).eq(invocation_id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(|(started,)| started.is_some()).ok_or_else(|| {
        Error::InvocationPersistence(
            "admitted Invocation disappeared while observing its enforcement point".into(),
        )
    })
}

/// Records the dispatch start of every Invocation in `invocation_ids` that
/// is still `pending` with no start, in one statement, and returns the ones
/// it moved to `dispatching`. A row another writer ended meanwhile is left
/// as it is and is not returned.
pub(super) async fn mark_dispatch_started_batch(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    invocation_ids: &[InvocationId],
    dispatch_started_at_unix_nanos: i64,
) -> Result<Vec<InvocationId>, Error> {
    if invocation_ids.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<String> = invocation_ids.iter().map(ToString::to_string).collect();
    let id_in = || Expr::col(ToolInvocations::Id).is_in(ids.iter().map(String::as_str));
    let statement = Query::update()
        .table(ToolInvocations::Table)
        .values([
            (
                ToolInvocations::State,
                Expr::val(InvocationState::Dispatching.as_storage_str()),
            ),
            (
                ToolInvocations::DispatchStartedAtUnixNanos,
                Expr::val(dispatch_started_at_unix_nanos),
            ),
        ])
        .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(ToolInvocations::RunId).eq(run_id.to_string()))
        .and_where(id_in())
        .and_where(Expr::col(ToolInvocations::State).eq(InvocationState::Pending.as_storage_str()))
        .and_where(Expr::col(ToolInvocations::DispatchStartedAtUnixNanos).is_null())
        .to_owned();
    let changed = transaction.execute_change(statement).await?;
    if changed == invocation_ids.len() as u64 {
        return Ok(invocation_ids.to_vec());
    }
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(ToolInvocations::Id)
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run_id.to_string()))
                .and_where(id_in())
                .and_where(
                    Expr::col(ToolInvocations::State)
                        .eq(InvocationState::Dispatching.as_storage_str()),
                )
                .and_where(
                    Expr::col(ToolInvocations::DispatchStartedAtUnixNanos)
                        .eq(dispatch_started_at_unix_nanos),
                )
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(|(id,)| invocation_id(&id)).collect()
}

fn invocation_id(value: &str) -> Result<InvocationId, Error> {
    InvocationId::parse(value)
        .map_err(|_error| Error::CorruptData("Invocation has invalid identity".into()))
}

pub(super) async fn finish(
    transaction: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
    required_state: InvocationState,
    completion: &InvocationCompletion,
    ended_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let (result_json, failure_code, failure_message) = match completion {
        InvocationCompletion::Upstream { result_json } => (Some(result_json.as_str()), None, None),
        InvocationCompletion::Failure(failure) => (
            None,
            Some(failure.code()),
            Some(failure.message().into_owned()),
        ),
    };
    let result_bytes = i64::try_from(result_json.map_or(0, str::len))
        .map_err(|_overflow| Error::CorruptData("Invocation byte count overflow".into()))?;
    let statement = Query::update()
        .table(ToolInvocations::Table)
        .values([
            (
                ToolInvocations::State,
                Expr::val(InvocationState::Ended.as_storage_str()),
            ),
            (
                ToolInvocations::AccountConnectionRequestId,
                Expr::val(Option::<String>::None),
            ),
            (
                ToolInvocations::EndedAtUnixNanos,
                Expr::val(ended_at_unix_nanos),
            ),
            (
                ToolInvocations::ResultKind,
                Expr::val(completion.result_kind().as_storage_str()),
            ),
            (ToolInvocations::ResultJson, Expr::val(result_json)),
            (ToolInvocations::ResultBytes, Expr::val(result_bytes)),
            (ToolInvocations::FailureCode, Expr::val(failure_code)),
            (ToolInvocations::FailureMessage, Expr::val(failure_message)),
        ])
        .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(admitted.workspace_id.to_string()))
        .and_where(Expr::col(ToolInvocations::Id).eq(admitted.id.to_string()))
        .and_where(Expr::col(ToolInvocations::State).eq(required_state.as_storage_str()))
        .and_where(Expr::col(ToolInvocations::RunId).eq(admitted.run_id.to_string()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(super) async fn end_pending_with_failure(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &InvocationId,
    failure: crate::CoralFailure,
    now: i64,
) -> Result<bool, Error> {
    let changed = tx
        .execute(
            Query::update()
                .table(ToolInvocations::Table)
                .values([
                    (
                        ToolInvocations::State,
                        Expr::val(InvocationState::Ended.as_storage_str()),
                    ),
                    (ToolInvocations::EndedAtUnixNanos, Expr::val(now)),
                    (
                        ToolInvocations::ResultKind,
                        Expr::val(InvocationResultKind::CoralFailure.as_storage_str()),
                    ),
                    (ToolInvocations::FailureCode, Expr::val(failure.code())),
                    (
                        ToolInvocations::FailureMessage,
                        Expr::val(failure.message()),
                    ),
                ])
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(run.workspace_id.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run.id.to_string()))
                .and_where(Expr::col(ToolInvocations::Id).eq(invocation.to_string()))
                .and_where(
                    Expr::col(ToolInvocations::State).eq(InvocationState::Pending.as_storage_str()),
                )
                .to_owned(),
        )
        .await?;
    Ok(changed == 1)
}

pub(super) struct ApprovalTarget {
    pub generation: coral_catalog::ProviderGenerationId,
    pub tool: coral_catalog::ToolKey,
    pub provider_id: coral_provider::ProviderId,
    pub provider_account_setting_version: i64,
    pub credential_route: coral_provider::access::CredentialRoute,
    pub provider_account_id: Option<coral_provider::account::ProviderAccountId>,
    pub call_path: String,
    pub arguments: serde_json::Value,
}

pub(super) async fn approval_target(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    invocation: &InvocationId,
) -> Result<ApprovalTarget, Error> {
    #[derive(FromRow)]
    struct Row {
        provider_generation_id: String,
        tool_key: String,
        provider_id: String,
        provider_account_setting_version: i64,
        credential_route: String,
        provider_account_id: Option<String>,
        call_path: String,
        arguments_json: String,
    }
    let row: Row = tx
        .fetch_optional(
            Query::select()
                .columns([
                    ToolInvocations::ProviderGenerationId,
                    ToolInvocations::ToolKey,
                    ToolInvocations::ProviderId,
                    ToolInvocations::ProviderAccountSettingVersion,
                    ToolInvocations::CredentialRoute,
                    ToolInvocations::ProviderAccountId,
                    ToolInvocations::CallPath,
                    ToolInvocations::ArgumentsJson,
                ])
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run.to_string()))
                .and_where(Expr::col(ToolInvocations::Id).eq(invocation.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::ApprovalNotFound)?;
    Ok(ApprovalTarget {
        generation: coral_catalog::ProviderGenerationId::parse(&row.provider_generation_id)
            .map_err(coral_provider::Error::from)?,
        tool: coral_catalog::ToolKey::parse(&row.tool_key).map_err(coral_provider::Error::from)?,
        provider_id: coral_provider::ProviderId::parse(&row.provider_id)?,
        provider_account_setting_version: row.provider_account_setting_version,
        credential_route: coral_provider::access::CredentialRoute::from_storage_str(
            &row.credential_route,
        )?,
        provider_account_id: row
            .provider_account_id
            .map(|id| coral_provider::account::ProviderAccountId::parse(&id))
            .transpose()?,
        call_path: row.call_path,
        arguments: serde_json::from_str(&row.arguments_json).map_err(|error| {
            Error::CorruptData(format!("Invocation has invalid arguments JSON: {error}"))
        })?,
    })
}

pub(super) async fn completed_failure(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    invocation: &InvocationId,
) -> Result<Option<CoralFailure>, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(ToolInvocations::FailureCode)
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ToolInvocations::Id).eq(invocation.to_string()))
                .and_where(
                    Expr::col(ToolInvocations::State).eq(InvocationState::Ended.as_storage_str()),
                )
                .and_where(
                    Expr::col(ToolInvocations::ResultKind)
                        .eq(InvocationResultKind::CoralFailure.as_storage_str()),
                )
                .to_owned(),
        )
        .await?;
    row.map(|(code,)| {
        CoralFailure::from_code(&code)
            .ok_or_else(|| Error::CorruptData("Invocation has invalid failure code".into()))
    })
    .transpose()
}

/// The latest Tool Results the Run's Program received, in call order, and how
/// many it received that were not read. Of the latest `limit`, newest first,
/// it reads each whose retained bytes fit what `max_bytes` has left, so a
/// large result never costs more than the budget to parse.
pub(super) async fn received_results(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    limit: u64,
    max_bytes: u64,
) -> Result<(Vec<crate::tool_result_shape::ReceivedResult>, u64), Error> {
    let received = Query::select()
        .from(ToolInvocations::Table)
        .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace.to_string()))
        .and_where(Expr::col(ToolInvocations::RunId).eq(run.to_string()))
        .and_where(
            Expr::col(ToolInvocations::ResultKind)
                .eq(InvocationResultKind::UpstreamOutcome.as_storage_str()),
        )
        .to_owned();
    let (total,): (i64,) = transaction
        .fetch_optional(
            received
                .clone()
                .expr(Expr::col(ToolInvocations::Id).count())
                .to_owned(),
        )
        .await?
        .ok_or_else(|| Error::CorruptData("missing Tool Result count".into()))?;
    let total = u64::try_from(total)
        .map_err(|_negative| Error::CorruptData("invalid Tool Result count".into()))?;
    let latest: Vec<(String, i64)> = transaction
        .fetch_all(
            received
                .clone()
                .columns([ToolInvocations::Id, ToolInvocations::ResultBytes])
                .order_by(ToolInvocations::AdmittedAtUnixNanos, Order::Desc)
                .order_by(ToolInvocations::Id, Order::Desc)
                .limit(limit)
                .to_owned(),
        )
        .await?;
    let mut left = max_bytes;
    let mut chosen = Vec::new();
    for (id, bytes) in latest {
        let bytes = u64::try_from(bytes)
            .map_err(|_negative| Error::CorruptData("invalid Tool Result size".into()))?;
        if let Some(rest) = left.checked_sub(bytes) {
            left = rest;
            chosen.push(id);
        }
    }
    let unread = total.saturating_sub(chosen.len() as u64);
    if chosen.is_empty() {
        return Ok((Vec::new(), unread));
    }
    let rows: Vec<(String, Option<String>)> = transaction
        .fetch_all(
            received
                .clone()
                .columns([ToolInvocations::CallPath, ToolInvocations::ResultJson])
                .and_where(Expr::col(ToolInvocations::Id).is_in(chosen.iter().map(String::as_str)))
                .order_by(ToolInvocations::AdmittedAtUnixNanos, Order::Asc)
                .order_by(ToolInvocations::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    let received = rows
        .into_iter()
        .map(|(call_path, result_json)| {
            let tool_result = serde_json::from_str(
                result_json
                    .as_deref()
                    .ok_or_else(|| Error::CorruptData("missing upstream result".into()))?,
            )
            .map_err(|_invalid| Error::CorruptData("invalid retained Tool Result".into()))?;
            Ok(crate::tool_result_shape::ReceivedResult {
                call_path,
                tool_result,
            })
        })
        .collect::<Result<_, Error>>()?;
    Ok((received, unread))
}

/// The arguments the Invocation was admitted with.
pub(super) async fn arguments(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    invocation: &InvocationId,
) -> Result<serde_json::Value, Error> {
    let (arguments,): (String,) = transaction
        .fetch_optional(
            Query::select()
                .column(ToolInvocations::ArgumentsJson)
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run.to_string()))
                .and_where(Expr::col(ToolInvocations::Id).eq(invocation.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or_else(|| Error::CorruptData("Invocation is missing".into()))?;
    serde_json::from_str(&arguments).map_err(|error| {
        Error::CorruptData(format!("Invocation has invalid arguments JSON: {error}"))
    })
}

pub(super) async fn nonterminal_for_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
) -> Result<Vec<UnfinishedInvocation>, Error> {
    let rows: Vec<NonterminalInvocationRow> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    ToolInvocations::Id,
                    ToolInvocations::State,
                    ToolInvocations::ProviderGenerationId,
                    ToolInvocations::ToolKey,
                ])
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(run.workspace_id.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run.id.to_string()))
                .and_where(
                    Expr::col(ToolInvocations::State).ne(InvocationState::Ended.as_storage_str()),
                )
                .order_by(ToolInvocations::AdmittedAtUnixNanos, Order::Asc)
                .order_by(ToolInvocations::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(UnfinishedInvocation {
                id: invocation_id(&row.id)?,
                state: InvocationState::from_storage_str(&row.state)?,
                generation: coral_catalog::ProviderGenerationId::parse(&row.provider_generation_id)
                    .map_err(coral_provider::Error::from)?,
                tool: coral_catalog::ToolKey::parse(&row.tool_key)
                    .map_err(coral_provider::Error::from)?,
            })
        })
        .collect()
}

pub(super) async fn end_for_termination(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &UnfinishedInvocation,
    result_kind: InvocationResultKind,
    result_json: Option<&str>,
    ended_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let result_bytes = i64::try_from(result_json.map_or(0, str::len))
        .map_err(|_overflow| Error::CorruptData("Invocation byte count overflow".into()))?;
    let statement = Query::update()
        .table(ToolInvocations::Table)
        .values([
            (
                ToolInvocations::State,
                Expr::val(InvocationState::Ended.as_storage_str()),
            ),
            (
                ToolInvocations::AccountConnectionRequestId,
                Expr::val(Option::<String>::None),
            ),
            (
                ToolInvocations::EndedAtUnixNanos,
                Expr::val(ended_at_unix_nanos),
            ),
            (
                ToolInvocations::ResultKind,
                Expr::val(result_kind.as_storage_str()),
            ),
            (ToolInvocations::ResultJson, Expr::val(result_json)),
            (ToolInvocations::ResultBytes, Expr::val(result_bytes)),
        ])
        .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(ToolInvocations::Id).eq(invocation.id.to_string()))
        .and_where(Expr::col(ToolInvocations::State).eq(invocation.state.as_storage_str()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

#[derive(Debug, FromRow)]
struct NonterminalInvocationRow {
    id: String,
    state: String,
    provider_generation_id: String,
    tool_key: String,
}

pub(super) async fn has_nonterminal_for_run(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<bool, Error> {
    has_for_run(
        transaction,
        workspace_id,
        run_id,
        Expr::col(ToolInvocations::State).ne(InvocationState::Ended.as_storage_str()),
    )
    .await
}

/// Reports whether an upstream dispatch started and has not ended. Only these
/// Invocations can still record a result once Run termination is requested.
pub(super) async fn has_dispatching_for_run(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<bool, Error> {
    has_for_run(
        transaction,
        workspace_id,
        run_id,
        Expr::col(ToolInvocations::State).eq(InvocationState::Dispatching.as_storage_str()),
    )
    .await
}

async fn has_for_run(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
    state: SimpleExpr,
) -> Result<bool, Error> {
    let row: Option<(i32,)> = transaction
        .fetch_optional(
            Query::select()
                .expr(Expr::val(1_i32))
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run_id.to_string()))
                .and_where(state)
                .limit(1)
                .to_owned(),
        )
        .await?;
    Ok(row.is_some())
}
