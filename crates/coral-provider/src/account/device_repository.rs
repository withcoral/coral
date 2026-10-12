//! Durable device polling schedules and exclusive worker leases. No credential plaintext lives here.
use crate::Error;
use coral_db::Transaction;
use coral_workspace::{ChangeContext, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Query};

#[derive(Iden)]
enum Jobs {
    #[iden = "provider_device_jobs"]
    Table,
    Id,
    WorkspaceId,
    ActorUserId,
    TraceId,
    SetupId,
    IntervalSeconds,
    NextPollAt,
    LeaseOwner,
    LeaseToken,
    LeaseUntil,
}

#[derive(sqlx::FromRow)]
struct JobRow {
    pub id: String,
    pub workspace_id: String,
    pub actor_user_id: String,
    pub trace_id: String,
    pub setup_id: Option<String>,
    pub interval_seconds: i64,
    pub next_poll_at: i64,
}

pub(super) enum Target {
    Setup(crate::ProviderSetupId),
    Account(super::AuthorizationId),
}
pub(super) struct Job {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub actor_user_id: coral_workspace::UserId,
    pub trace_id: coral_telemetry::TraceId,
    pub target: Target,
    pub interval_seconds: u64,
    pub next_poll_at: i64,
}
impl JobRow {
    fn decode(self) -> Result<Job, Error> {
        let target = match self.setup_id {
            Some(setup) => Target::Setup(crate::ProviderSetupId::parse(&setup)?),
            None => Target::Account(super::AuthorizationId::parse(&self.id)?),
        };
        Ok(Job {
            id: self.id,
            workspace_id: WorkspaceId::parse(&self.workspace_id)?,
            actor_user_id: coral_workspace::UserId::parse(&self.actor_user_id)?,
            trace_id: coral_telemetry::TraceId::parse(&self.trace_id)
                .map_err(|_invalid| Error::AccountInput)?,
            target,
            interval_seconds: u64::try_from(self.interval_seconds)
                .map_err(|_invalid| Error::AccountInput)?,
            next_poll_at: self.next_poll_at,
        })
    }
}

fn columns() -> [Jobs; 7] {
    [
        Jobs::Id,
        Jobs::WorkspaceId,
        Jobs::ActorUserId,
        Jobs::TraceId,
        Jobs::SetupId,
        Jobs::IntervalSeconds,
        Jobs::NextPollAt,
    ]
}
fn scope(owner: &WorkspaceId, id: &str) -> sea_query::SimpleExpr {
    Expr::col(Jobs::WorkspaceId)
        .eq(owner.to_string())
        .and(Expr::col(Jobs::Id).eq(id))
}

pub(super) async fn insert(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    owner: &WorkspaceId,
    id: &str,
    setup: Option<&crate::ProviderSetupId>,
) -> Result<(), Error> {
    tx.execute(
        Query::insert()
            .into_table(Jobs::Table)
            .columns([
                Jobs::Id,
                Jobs::WorkspaceId,
                Jobs::ActorUserId,
                Jobs::TraceId,
                Jobs::SetupId,
                Jobs::IntervalSeconds,
                Jobs::NextPollAt,
                Jobs::LeaseUntil,
            ])
            .values_panic([
                id.into(),
                owner.to_string().into(),
                context.actor().user_id().to_string().into(),
                context.trace_id().as_str().to_owned().into(),
                setup.map(ToString::to_string).into(),
                5_i64.into(),
                context.occurred_at_unix_nanos().into(),
                0_i64.into(),
            ])
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn recoverable(tx: &mut Transaction<'_>, now: i64) -> Result<Vec<Job>, Error> {
    let rows: Vec<JobRow> = tx
        .fetch_all(
            Query::select()
                .columns(columns())
                .from(Jobs::Table)
                .and_where(Expr::col(Jobs::LeaseUntil).lte(now))
                .limit(100)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(JobRow::decode).collect()
}

pub(super) async fn claim(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &str,
    worker: &str,
    token: &str,
    now: i64,
    until: i64,
) -> Result<Option<Job>, Error> {
    let changed = tx
        .execute(
            Query::update()
                .table(Jobs::Table)
                .value(Jobs::LeaseOwner, worker)
                .value(Jobs::LeaseToken, token)
                .value(Jobs::LeaseUntil, until)
                .and_where(scope(owner, id))
                .and_where(Expr::col(Jobs::LeaseUntil).lte(now))
                .to_owned(),
        )
        .await?;
    if changed == 0 {
        return Ok(None);
    }
    let row: Option<JobRow> = tx
        .fetch_optional(
            Query::select()
                .columns(columns())
                .from(Jobs::Table)
                .and_where(scope(owner, id))
                .to_owned(),
        )
        .await?;
    row.map(JobRow::decode).transpose()
}

pub(super) async fn renew(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &str,
    token: &str,
    now: i64,
    until: i64,
) -> Result<(), Error> {
    let changed = tx
        .execute(
            Query::update()
                .table(Jobs::Table)
                .value(Jobs::LeaseUntil, until)
                .and_where(scope(owner, id))
                .and_where(Expr::col(Jobs::LeaseToken).eq(token))
                .and_where(Expr::col(Jobs::LeaseUntil).gt(now))
                .to_owned(),
        )
        .await?;
    if changed == 0 {
        return Err(Error::AccountInput);
    }
    Ok(())
}

pub(super) async fn schedule(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &str,
    token: &str,
    interval: u64,
    next: i64,
) -> Result<(), Error> {
    let interval = i64::try_from(interval).map_err(|_invalid| Error::AccountInput)?;
    let changed = tx
        .execute(
            Query::update()
                .table(Jobs::Table)
                .value(Jobs::IntervalSeconds, interval)
                .value(Jobs::NextPollAt, next)
                .and_where(scope(owner, id))
                .and_where(Expr::col(Jobs::LeaseToken).eq(token))
                .to_owned(),
        )
        .await?;
    if changed == 0 {
        return Err(Error::AccountInput);
    }
    Ok(())
}

pub(super) async fn finish(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &str,
    token: &str,
) -> Result<(), Error> {
    tx.execute(
        Query::delete()
            .from_table(Jobs::Table)
            .and_where(scope(owner, id))
            .and_where(Expr::col(Jobs::LeaseToken).eq(token))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn release(tx: &mut Transaction<'_>, worker: &str) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(Jobs::Table)
            .value(Jobs::LeaseUntil, 0_i64)
            .value(Jobs::LeaseOwner, Option::<String>::None)
            .and_where(Expr::col(Jobs::LeaseOwner).eq(worker))
            .to_owned(),
    )
    .await?;
    Ok(())
}
