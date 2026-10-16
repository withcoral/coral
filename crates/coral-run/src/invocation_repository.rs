use crate::lifecycle::RunRecord;
use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query, SelectStatement, SimpleExpr};
use sqlx::FromRow;

use crate::invocation::{AdmittedInvocation, InvocationCompletion};
use crate::invocation_lifecycle::{InvocationState, UnfinishedInvocation};
use crate::run_repository::{Chunk, NewEvent};
use crate::{CoralFailure, Error, InvocationId, InvocationResultKind, RunId, run_repository};

#[derive(Iden)]
enum OperationInvocations {
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
    OperationKey,
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

/// Inserts the admitted Invocation and commits `invocation.started` as one
/// chunk under the worker's claim.
pub(super) async fn insert_admission(
    transaction: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
    run: &RunRecord,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(OperationInvocations::Table)
        .columns([
            OperationInvocations::Id,
            OperationInvocations::WorkspaceId,
            OperationInvocations::RunId,
            OperationInvocations::ProviderId,
            OperationInvocations::ProviderAccountSettingVersion,
            OperationInvocations::CredentialRoute,
            OperationInvocations::ProviderGenerationId,
            OperationInvocations::OperationKey,
            OperationInvocations::CallPath,
            OperationInvocations::ArgumentsJson,
            OperationInvocations::State,
            OperationInvocations::AdmittedAtUnixNanos,
        ])
        .values_panic([
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
            Expr::val(admitted.target.operation().key().as_str()),
            Expr::val(&admitted.call_path),
            Expr::val(&admitted.arguments_json),
            Expr::val(InvocationState::Pending.as_storage_str()),
            Expr::val(admitted.admitted_at_unix_nanos),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    run_repository::commit_chunk(
        transaction,
        Chunk::new(run, Some(&admitted.claim_id)).event(
            NewEvent::InvocationStarted(&admitted.id),
            admitted.admitted_at_unix_nanos,
        ),
    )
    .await
}

pub(super) async fn connection_requests_for_run(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
) -> Result<Vec<coral_provider::account::ConnectionRequestId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .distinct()
                .column(OperationInvocations::AccountConnectionRequestId)
                .from(OperationInvocations::Table)
                .and_where(
                    Expr::col(OperationInvocations::WorkspaceId).eq(run.workspace_id.to_string()),
                )
                .and_where(Expr::col(OperationInvocations::RunId).eq(run.id.to_string()))
                .and_where(
                    Expr::col(OperationInvocations::State)
                        .eq(InvocationState::Pending.as_storage_str()),
                )
                .and_where(
                    Expr::col(OperationInvocations::AccountConnectionRequestId).is_not_null(),
                )
                .order_by(OperationInvocations::AccountConnectionRequestId, Order::Asc)
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
        .from_as(OperationInvocations::Table, Alias::new("waiting"))
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
                .table(OperationInvocations::Table)
                .values([
                    (
                        OperationInvocations::AccountWaitStartedAtUnixNanos,
                        Expr::val(now),
                    ),
                    (
                        OperationInvocations::AccountConnectionRequestId,
                        Expr::val(request.to_string()),
                    ),
                    (
                        OperationInvocations::RetainedAccountConnectionRequestId,
                        Expr::val(request.to_string()),
                    ),
                ])
                .and_where(Expr::col(OperationInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(OperationInvocations::Id).eq(invocation.to_string()))
                .and_where(
                    Expr::col(OperationInvocations::State)
                        .eq(InvocationState::Pending.as_storage_str()),
                )
                .and_where(Expr::col(OperationInvocations::AccountConnectionRequestId).is_null())
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
                .table(OperationInvocations::Table)
                .values([
                    (
                        OperationInvocations::ProviderAccountId,
                        Expr::val(evidence.account_id.to_string()),
                    ),
                    (
                        OperationInvocations::AccountAuthorizationVersion,
                        Expr::val(&evidence.authorization_version),
                    ),
                    (
                        OperationInvocations::AccountConnectionRequestId,
                        Expr::val(Option::<String>::None),
                    ),
                ])
                .and_where(Expr::col(OperationInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(OperationInvocations::Id).eq(invocation.to_string()))
                .and_where(
                    Expr::col(OperationInvocations::State)
                        .eq(InvocationState::Pending.as_storage_str()),
                )
                .and_where(Expr::col(OperationInvocations::AccountAuthorizationVersion).is_null())
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
                .column(OperationInvocations::DispatchStartedAtUnixNanos)
                .from(OperationInvocations::Table)
                .and_where(
                    Expr::col(OperationInvocations::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(OperationInvocations::RunId).eq(run_id.to_string()))
                .and_where(Expr::col(OperationInvocations::Id).eq(invocation_id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(|(started,)| started.is_some()).ok_or_else(|| {
        Error::InvocationPersistence(
            "admitted Invocation disappeared while observing its fence".into(),
        )
    })
}

pub(super) async fn mark_dispatch_started(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    invocation_id: &InvocationId,
    dispatch_started_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(OperationInvocations::Table)
        .values([
            (
                OperationInvocations::State,
                Expr::val(InvocationState::Dispatching.as_storage_str()),
            ),
            (
                OperationInvocations::DispatchStartedAtUnixNanos,
                Expr::val(dispatch_started_at_unix_nanos),
            ),
        ])
        .and_where(Expr::col(OperationInvocations::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(OperationInvocations::Id).eq(invocation_id.to_string()))
        .and_where(
            Expr::col(OperationInvocations::State).eq(InvocationState::Pending.as_storage_str()),
        )
        .and_where(Expr::col(OperationInvocations::DispatchStartedAtUnixNanos).is_null())
        .to_owned();
    Ok(transaction.execute_change(statement).await? == 1)
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
        InvocationCompletion::Failure(failure) => {
            (None, Some(failure.code()), Some(failure.message()))
        }
    };
    let result_bytes = i64::try_from(result_json.map_or(0, str::len))
        .map_err(|_overflow| Error::CorruptData("Invocation byte count overflow".into()))?;
    let statement = Query::update()
        .table(OperationInvocations::Table)
        .values([
            (
                OperationInvocations::State,
                Expr::val(InvocationState::Ended.as_storage_str()),
            ),
            (
                OperationInvocations::AccountConnectionRequestId,
                Expr::val(Option::<String>::None),
            ),
            (
                OperationInvocations::EndedAtUnixNanos,
                Expr::val(ended_at_unix_nanos),
            ),
            (
                OperationInvocations::ResultKind,
                Expr::val(completion.result_kind().as_storage_str()),
            ),
            (OperationInvocations::ResultJson, Expr::val(result_json)),
            (OperationInvocations::ResultBytes, Expr::val(result_bytes)),
            (OperationInvocations::FailureCode, Expr::val(failure_code)),
            (
                OperationInvocations::FailureMessage,
                Expr::val(failure_message),
            ),
        ])
        .and_where(
            Expr::col(OperationInvocations::WorkspaceId).eq(admitted.workspace_id.to_string()),
        )
        .and_where(Expr::col(OperationInvocations::Id).eq(admitted.id.to_string()))
        .and_where(Expr::col(OperationInvocations::State).eq(required_state.as_storage_str()))
        .and_where(Expr::col(OperationInvocations::RunId).eq(admitted.run_id.to_string()))
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
                .table(OperationInvocations::Table)
                .values([
                    (
                        OperationInvocations::State,
                        Expr::val(InvocationState::Ended.as_storage_str()),
                    ),
                    (OperationInvocations::EndedAtUnixNanos, Expr::val(now)),
                    (
                        OperationInvocations::ResultKind,
                        Expr::val(InvocationResultKind::CoralFailure.as_storage_str()),
                    ),
                    (OperationInvocations::FailureCode, Expr::val(failure.code())),
                    (
                        OperationInvocations::FailureMessage,
                        Expr::val(failure.message()),
                    ),
                ])
                .and_where(
                    Expr::col(OperationInvocations::WorkspaceId).eq(run.workspace_id.to_string()),
                )
                .and_where(Expr::col(OperationInvocations::RunId).eq(run.id.to_string()))
                .and_where(Expr::col(OperationInvocations::Id).eq(invocation.to_string()))
                .and_where(
                    Expr::col(OperationInvocations::State)
                        .eq(InvocationState::Pending.as_storage_str()),
                )
                .to_owned(),
        )
        .await?;
    Ok(changed == 1)
}

pub(super) struct ApprovalTarget {
    pub generation: coral_catalog::ProviderGenerationId,
    pub operation: coral_catalog::OperationKey,
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
        operation_key: String,
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
                    OperationInvocations::ProviderGenerationId,
                    OperationInvocations::OperationKey,
                    OperationInvocations::ProviderId,
                    OperationInvocations::ProviderAccountSettingVersion,
                    OperationInvocations::CredentialRoute,
                    OperationInvocations::ProviderAccountId,
                    OperationInvocations::CallPath,
                    OperationInvocations::ArgumentsJson,
                ])
                .from(OperationInvocations::Table)
                .and_where(Expr::col(OperationInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(OperationInvocations::RunId).eq(run.to_string()))
                .and_where(Expr::col(OperationInvocations::Id).eq(invocation.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::ApprovalNotFound)?;
    Ok(ApprovalTarget {
        generation: coral_catalog::ProviderGenerationId::parse(&row.provider_generation_id)
            .map_err(coral_provider::Error::from)?,
        operation: coral_catalog::OperationKey::parse(&row.operation_key)
            .map_err(coral_provider::Error::from)?,
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
                .column(OperationInvocations::FailureCode)
                .from(OperationInvocations::Table)
                .and_where(Expr::col(OperationInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(OperationInvocations::Id).eq(invocation.to_string()))
                .and_where(
                    Expr::col(OperationInvocations::State)
                        .eq(InvocationState::Ended.as_storage_str()),
                )
                .and_where(
                    Expr::col(OperationInvocations::ResultKind)
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

pub(super) async fn nonterminal_for_run(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
) -> Result<Vec<UnfinishedInvocation>, Error> {
    let rows: Vec<NonterminalInvocationRow> = transaction
        .fetch_all(
            Query::select()
                .columns([OperationInvocations::Id, OperationInvocations::State])
                .from(OperationInvocations::Table)
                .and_where(
                    Expr::col(OperationInvocations::WorkspaceId).eq(run.workspace_id.to_string()),
                )
                .and_where(Expr::col(OperationInvocations::RunId).eq(run.id.to_string()))
                .and_where(
                    Expr::col(OperationInvocations::State)
                        .ne(InvocationState::Ended.as_storage_str()),
                )
                .order_by(OperationInvocations::AdmittedAtUnixNanos, Order::Asc)
                .order_by(OperationInvocations::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(UnfinishedInvocation {
                id: InvocationId::parse(&row.id).map_err(|_error| {
                    Error::CorruptData("Invocation has invalid identity".into())
                })?,
                state: InvocationState::from_storage_str(&row.state)?,
            })
        })
        .collect()
}

pub(super) async fn end_for_termination(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &UnfinishedInvocation,
    result_kind: InvocationResultKind,
    ended_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(OperationInvocations::Table)
        .values([
            (
                OperationInvocations::State,
                Expr::val(InvocationState::Ended.as_storage_str()),
            ),
            (
                OperationInvocations::AccountConnectionRequestId,
                Expr::val(Option::<String>::None),
            ),
            (
                OperationInvocations::EndedAtUnixNanos,
                Expr::val(ended_at_unix_nanos),
            ),
            (
                OperationInvocations::ResultKind,
                Expr::val(result_kind.as_storage_str()),
            ),
        ])
        .and_where(Expr::col(OperationInvocations::WorkspaceId).eq(run.workspace_id.to_string()))
        .and_where(Expr::col(OperationInvocations::Id).eq(invocation.id.to_string()))
        .and_where(Expr::col(OperationInvocations::State).eq(invocation.state.as_storage_str()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

#[derive(Debug, FromRow)]
struct NonterminalInvocationRow {
    id: String,
    state: String,
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
        Expr::col(OperationInvocations::State).ne(InvocationState::Ended.as_storage_str()),
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
        Expr::col(OperationInvocations::State).eq(InvocationState::Dispatching.as_storage_str()),
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
                .from(OperationInvocations::Table)
                .and_where(
                    Expr::col(OperationInvocations::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(Expr::col(OperationInvocations::RunId).eq(run_id.to_string()))
                .and_where(state)
                .limit(1)
                .to_owned(),
        )
        .await?;
    Ok(row.is_some())
}
