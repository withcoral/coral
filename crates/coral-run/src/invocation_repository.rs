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
    ParentInvocationId,
    DeclaredDefaultAccessState,
    RequestNumber,
    UpstreamRejection,
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
                ToolInvocations::ParentInvocationId,
                ToolInvocations::DeclaredDefaultAccessState,
                ToolInvocations::RequestNumber,
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
                Expr::val(
                    admitted
                        .parent_invocation_id
                        .as_ref()
                        .map(ToString::to_string),
                ),
                Expr::val(
                    authored_tool_default(admitted.target.tool())
                        .map(coral_catalog::ToolAccessState::as_storage_str),
                ),
                Expr::val(
                    admitted
                        .request_number
                        .map(i64::try_from)
                        .transpose()
                        .map_err(|_error| {
                            Error::CorruptData("Run request number overflowed".to_string())
                        })?,
                ),
            ]
            .into_iter()
            .chain(location_values),
        )
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

/// The default of the revision the Run linked, for a Private or Shared
/// Tool: its author declared a Private Tool's, and its publishing Owner
/// chose a Shared Tool's. `None` for every other Tool, whose catalog keeps
/// its facts.
fn authored_tool_default(
    tool: &coral_catalog::MaterializedTool,
) -> Option<coral_catalog::ToolAccessState> {
    matches!(
        tool.execution_binding(),
        coral_catalog::ExecutionBinding::Code(_)
    )
    .then(|| tool.policy_facts().declared_default())
    .flatten()
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

// Ending a row adds it to Postgres's `idx_tool_invocations_ended_outcomes`, which
// stores the Call Path. An unusually long Call Path that does not compress can
// exceed the index's row limit. This write then fails and can leave the Run
// running (#2933). No fixed length marks this, because the whole row counts.
pub(super) async fn finish(
    transaction: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
    required_state: InvocationState,
    completion: &InvocationCompletion,
    ended_at_unix_nanos: i64,
) -> Result<bool, Error> {
    let (result_json, failure_code, failure_message, rejection) = match completion {
        InvocationCompletion::Upstream {
            result_json,
            rejection,
        } => (
            Some(result_json.as_str()),
            None,
            None,
            rejection.map(crate::error::UpstreamRejection::code),
        ),
        InvocationCompletion::Failure(failure) => (
            None,
            Some(failure.code()),
            Some(failure.message().into_owned()),
            None,
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
            (ToolInvocations::UpstreamRejection, Expr::val(rejection)),
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
    /// The default of the revision the Run linked, for a Private or Shared
    /// Tool; `None` for any other Tool.
    pub declared_default: Option<coral_catalog::ToolAccessState>,
    pub tool: coral_catalog::ToolKey,
    pub provider_id: coral_provider::ProviderId,
    pub provider_account_setting_version: i64,
    pub credential_route: coral_provider::access::CredentialRoute,
    pub provider_account_id: Option<coral_provider::account::ProviderAccountId>,
    pub call_path: String,
    pub arguments: serde_json::Value,
}

/// The Invocation columns an [`ApprovalTarget`] is read from.
const APPROVAL_TARGET_COLUMNS: [ToolInvocations; 9] = [
    ToolInvocations::ProviderGenerationId,
    ToolInvocations::ToolKey,
    ToolInvocations::ProviderId,
    ToolInvocations::ProviderAccountSettingVersion,
    ToolInvocations::CredentialRoute,
    ToolInvocations::ProviderAccountId,
    ToolInvocations::CallPath,
    ToolInvocations::ArgumentsJson,
    ToolInvocations::DeclaredDefaultAccessState,
];

/// Joins to `select` the Invocation that matches `on`, aliased `i`, and
/// selects the columns an [`ApprovalTargetRow`] decodes.
pub(super) fn join_approval_target(select: &mut SelectStatement, on: SimpleExpr) {
    use sea_query::Alias;
    select
        .join_as(
            sea_query::JoinType::InnerJoin,
            ToolInvocations::Table,
            Alias::new("i"),
            on,
        )
        .columns(APPROVAL_TARGET_COLUMNS.map(|column| (Alias::new("i"), column)));
}

/// One Invocation's [`APPROVAL_TARGET_COLUMNS`], before decoding.
#[derive(FromRow)]
pub(super) struct ApprovalTargetRow {
    provider_generation_id: String,
    tool_key: String,
    provider_id: String,
    provider_account_setting_version: i64,
    credential_route: String,
    provider_account_id: Option<String>,
    call_path: String,
    arguments_json: String,
    declared_default_access_state: Option<String>,
}

impl ApprovalTargetRow {
    pub(super) fn decode(self) -> Result<ApprovalTarget, Error> {
        let declared_default = self
            .declared_default_access_state
            .map(|state| {
                coral_catalog::ToolAccessState::from_storage_str(&state).ok_or_else(|| {
                    Error::CorruptData("Invocation has an invalid declared default".to_owned())
                })
            })
            .transpose()?;
        Ok(ApprovalTarget {
            generation: coral_catalog::ProviderGenerationId::parse(&self.provider_generation_id)
                .map_err(coral_provider::Error::from)?,
            declared_default,
            tool: coral_catalog::ToolKey::parse(&self.tool_key)
                .map_err(coral_provider::Error::from)?,
            provider_id: coral_provider::ProviderId::parse(&self.provider_id)?,
            provider_account_setting_version: self.provider_account_setting_version,
            credential_route: coral_provider::access::CredentialRoute::from_storage_str(
                &self.credential_route,
            )?,
            provider_account_id: self
                .provider_account_id
                .map(|id| coral_provider::account::ProviderAccountId::parse(&id))
                .transpose()?,
            call_path: self.call_path,
            arguments: serde_json::from_str(&self.arguments_json).map_err(|error| {
                Error::CorruptData(format!("Invocation has invalid arguments JSON: {error}"))
            })?,
        })
    }
}

pub(super) async fn approval_target(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    run: &RunId,
    invocation: &InvocationId,
) -> Result<ApprovalTarget, Error> {
    let row: ApprovalTargetRow = tx
        .fetch_optional(
            Query::select()
                .columns(APPROVAL_TARGET_COLUMNS)
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run.to_string()))
                .and_where(Expr::col(ToolInvocations::Id).eq(invocation.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::ApprovalNotFound)?;
    row.decode()
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

/// The Generations whose Tools the run's dispatches that started and have
/// not ended called, each once.
pub(super) async fn dispatching_generations_for_run(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<Vec<coral_catalog::ProviderGenerationId>, Error> {
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .distinct()
                .column(ToolInvocations::ProviderGenerationId)
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run_id.to_string()))
                .and_where(
                    Expr::col(ToolInvocations::State)
                        .eq(InvocationState::Dispatching.as_storage_str()),
                )
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(generation,)| {
            coral_catalog::ProviderGenerationId::parse(&generation)
                .map_err(|error| coral_provider::Error::from(error).into())
        })
        .collect()
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

/// Counts the Tool Invocations that ended in `[start, end)`, in every
/// Workspace, by result kind.
pub(crate) async fn ended_by_result_kind(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<Vec<(InvocationResultKind, u64)>, Error> {
    let rows: Vec<(Option<String>, i64)> = tx
        .fetch_all(
            Query::select()
                .column(ToolInvocations::ResultKind)
                .expr(Expr::col(ToolInvocations::Id).count())
                .from(ToolInvocations::Table)
                .and_where(
                    Expr::col(ToolInvocations::State).eq(InvocationState::Ended.as_storage_str()),
                )
                .and_where(Expr::col(ToolInvocations::EndedAtUnixNanos).gte(start))
                .and_where(Expr::col(ToolInvocations::EndedAtUnixNanos).lt(end))
                .group_by_col(ToolInvocations::ResultKind)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(kind, invocations)| {
            let kind = kind.ok_or_else(|| {
                Error::CorruptData("ended Invocation has no result kind".to_string())
            })?;
            Ok((
                InvocationResultKind::from_storage_str(&kind)?,
                u64::try_from(invocations).unwrap_or_default(),
            ))
        })
        .collect()
}

/// How an ended Invocation ended, as its row recorded it: what a Run stream
/// that moved answers its call with again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecordedOutcome {
    /// A declared success, as native JSON.
    Returned(String),
    /// An Upstream Outcome that rejects the call.
    Rejected {
        rejection: crate::error::UpstreamRejection,
        outcome_json: String,
    },
    /// A Coral Failure, with the message it recorded.
    Failed {
        failure: CoralFailure,
        message: String,
    },
    /// Dispatch started, and no complete result won.
    Indeterminate,
    /// The Run's termination ended it.
    RunTerminated,
}

impl RecordedOutcome {
    /// The outcome an ended row of `kind` records with these columns.
    pub(crate) fn of(
        kind: InvocationResultKind,
        result_json: Option<String>,
        failure_code: Option<&str>,
        failure_message: Option<String>,
        upstream_rejection: Option<&str>,
    ) -> Result<Self, Error> {
        let corrupt = |what: &str| Error::CorruptData(format!("Invocation has {what}"));
        Ok(match kind {
            InvocationResultKind::UpstreamOutcome => {
                let json = result_json.ok_or_else(|| corrupt("no Tool Result"))?;
                match upstream_rejection {
                    None => Self::Returned(json),
                    Some(code) => Self::Rejected {
                        rejection: crate::error::UpstreamRejection::from_code(code)
                            .ok_or_else(|| corrupt("an invalid upstream rejection"))?,
                        outcome_json: json,
                    },
                }
            }
            InvocationResultKind::CoralFailure => Self::Failed {
                failure: failure_code
                    .and_then(CoralFailure::from_code)
                    .ok_or_else(|| corrupt("an invalid failure code"))?,
                message: failure_message.unwrap_or_default(),
            },
            InvocationResultKind::Indeterminate => Self::Indeterminate,
            InvocationResultKind::RunTerminated => Self::RunTerminated,
        })
    }

    /// `failure`, with its own message.
    pub(crate) fn failed(failure: CoralFailure) -> Self {
        Self::Failed {
            failure,
            message: failure.message().to_owned(),
        }
    }
}

/// An Invocation a Run stream request opened, as a reopened stream rebuilds
/// it.
#[derive(Debug, Clone)]
pub(crate) struct StreamInvocation {
    pub(crate) id: InvocationId,
    pub(crate) request_number: u64,
    pub(crate) state: InvocationState,
    pub(crate) provider_id: coral_provider::ProviderId,
    pub(crate) credential_route: coral_provider::access::CredentialRoute,
    pub(crate) provider_version: i64,
    pub(crate) generation: coral_catalog::ProviderGenerationId,
    pub(crate) tool: coral_catalog::ToolKey,
    pub(crate) call_path: String,
    pub(crate) arguments_json: String,
    pub(crate) parent: Option<InvocationId>,
    pub(crate) admitted_at_unix_nanos: i64,
    /// `None` while it is nonterminal.
    pub(crate) outcome: Option<RecordedOutcome>,
}

#[derive(FromRow)]
struct StreamInvocationRow {
    id: String,
    request_number: i64,
    state: String,
    provider_id: String,
    credential_route: String,
    provider_account_setting_version: i64,
    provider_generation_id: String,
    tool_key: String,
    call_path: String,
    arguments_json: String,
    parent_invocation_id: Option<String>,
    admitted_at_unix_nanos: i64,
    result_kind: Option<String>,
    result_json: Option<String>,
    failure_code: Option<String>,
    failure_message: Option<String>,
    upstream_rejection: Option<String>,
}

/// The Invocations of `run` that Run stream requests opened and that are
/// nonterminal or that one of `requests` opened, in request order.
pub(crate) async fn for_run_stream(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    requests: &[u64],
) -> Result<Vec<StreamInvocation>, Error> {
    let requests = requests
        .iter()
        .map(|number| i64::try_from(*number))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_error| Error::CorruptData("Run request number overflowed".to_string()))?;
    let rows: Vec<StreamInvocationRow> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    ToolInvocations::Id,
                    ToolInvocations::RequestNumber,
                    ToolInvocations::State,
                    ToolInvocations::ProviderId,
                    ToolInvocations::CredentialRoute,
                    ToolInvocations::ProviderAccountSettingVersion,
                    ToolInvocations::ProviderGenerationId,
                    ToolInvocations::ToolKey,
                    ToolInvocations::CallPath,
                    ToolInvocations::ArgumentsJson,
                    ToolInvocations::ParentInvocationId,
                    ToolInvocations::AdmittedAtUnixNanos,
                    ToolInvocations::ResultKind,
                    ToolInvocations::ResultJson,
                    ToolInvocations::FailureCode,
                    ToolInvocations::FailureMessage,
                    ToolInvocations::UpstreamRejection,
                ])
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(run.workspace_id.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run.id.to_string()))
                .and_where(Expr::col(ToolInvocations::RequestNumber).is_not_null())
                .and_where(
                    Expr::col(ToolInvocations::State)
                        .ne(InvocationState::Ended.as_storage_str())
                        .or(Expr::col(ToolInvocations::RequestNumber).is_in(requests)),
                )
                // A body's call is numbered after its Perform, whatever the
                // clocks of the Coral Instances that admitted them.
                .order_by(ToolInvocations::RequestNumber, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(StreamInvocationRow::decode).collect()
}

/// The request numbers of `run`'s ended Invocations whose call path is one
/// of `performs`: the Performs that ended.
pub(crate) async fn ended_performs<'a>(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    performs: impl ExactSizeIterator<Item = &'a String>,
) -> Result<Vec<u64>, Error> {
    if performs.len() == 0 {
        return Ok(Vec::new());
    }
    let numbers: Vec<(i64,)> = transaction
        .fetch_all(
            Query::select()
                .column(ToolInvocations::RequestNumber)
                .from(ToolInvocations::Table)
                .and_where(Expr::col(ToolInvocations::WorkspaceId).eq(run.workspace_id.to_string()))
                .and_where(Expr::col(ToolInvocations::RunId).eq(run.id.to_string()))
                .and_where(Expr::col(ToolInvocations::RequestNumber).is_not_null())
                .and_where(
                    Expr::col(ToolInvocations::State).eq(InvocationState::Ended.as_storage_str()),
                )
                .and_where(Expr::col(ToolInvocations::CallPath).is_in(performs))
                .to_owned(),
        )
        .await?;
    numbers
        .into_iter()
        .map(|(number,)| request_number(number))
        .collect()
}

fn request_number(stored: i64) -> Result<u64, Error> {
    u64::try_from(stored).map_err(|_negative| {
        Error::CorruptData("Invocation has an invalid request number".to_string())
    })
}

impl StreamInvocationRow {
    fn decode(self) -> Result<StreamInvocation, Error> {
        let corrupt = |what: &str| Error::CorruptData(format!("Invocation has {what}"));
        let state = InvocationState::from_storage_str(&self.state)?;
        let outcome = self
            .result_kind
            .as_deref()
            .map(|kind| {
                RecordedOutcome::of(
                    InvocationResultKind::from_storage_str(kind)?,
                    self.result_json,
                    self.failure_code.as_deref(),
                    self.failure_message,
                    self.upstream_rejection.as_deref(),
                )
            })
            .transpose()?;
        if outcome.is_some() != matches!(state, InvocationState::Ended) {
            return Err(corrupt("a result that contradicts its state"));
        }
        Ok(StreamInvocation {
            id: invocation_id(&self.id)?,
            request_number: request_number(self.request_number)?,
            state,
            provider_id: coral_provider::ProviderId::parse(&self.provider_id)
                .map_err(|_invalid| corrupt("an invalid Provider id"))?,
            credential_route: coral_provider::access::CredentialRoute::from_storage_str(
                &self.credential_route,
            )
            .map_err(|_invalid| corrupt("an invalid credential route"))?,
            provider_version: self.provider_account_setting_version,
            generation: coral_catalog::ProviderGenerationId::parse(&self.provider_generation_id)
                .map_err(|_invalid| corrupt("an invalid Provider Generation id"))?,
            tool: coral_catalog::ToolKey::parse(&self.tool_key)
                .map_err(|_invalid| corrupt("an invalid Tool key"))?,
            call_path: self.call_path,
            arguments_json: self.arguments_json,
            parent: self
                .parent_invocation_id
                .as_deref()
                .map(invocation_id)
                .transpose()?,
            admitted_at_unix_nanos: self.admitted_at_unix_nanos,
            outcome,
        })
    }
}
