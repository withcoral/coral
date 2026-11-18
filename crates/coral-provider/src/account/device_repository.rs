//! Durable device polling schedules and exclusive worker leases. No credential plaintext lives here.
use crate::Error;
use coral_db::Transaction;
use coral_workspace::{ChangeContext, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};

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

/// The key a job row is stored under, as written. Recovery moves a row by it
/// even when the row's identity does not decode.
pub(super) struct StoredKey {
    workspace_id: String,
    id: String,
}
impl StoredKey {
    /// The Workspace the row names, when that much of it decodes.
    pub(super) fn workspace(&self) -> Option<WorkspaceId> {
        WorkspaceId::parse(&self.workspace_id).ok()
    }
}

/// One job whose lease has ended: its stored key, and the job when its row
/// decodes.
pub(super) struct Recoverable {
    pub key: StoredKey,
    pub job: Result<Job, Error>,
}

impl JobRow {
    fn key(&self) -> StoredKey {
        StoredKey {
            workspace_id: self.workspace_id.clone(),
            id: self.id.clone(),
        }
    }

    /// Every value was valid when Coral stored it, so one that no longer
    /// parses is corrupt data, not the caller's input.
    fn decode(self) -> Result<Job, Error> {
        let corrupt =
            |field: &'static str| Error::CorruptData(format!("stored device job {field}"));
        let target = match self.setup_id {
            Some(setup) => Target::Setup(
                crate::ProviderSetupId::parse(&setup)
                    .map_err(|_invalid| corrupt("Setup id is not valid"))?,
            ),
            None => Target::Account(
                super::AuthorizationId::parse(&self.id)
                    .map_err(|_invalid| corrupt("id is not valid"))?,
            ),
        };
        Ok(Job {
            id: self.id,
            workspace_id: WorkspaceId::parse(&self.workspace_id)
                .map_err(|_invalid| corrupt("Workspace id is not valid"))?,
            actor_user_id: coral_workspace::UserId::parse(&self.actor_user_id)
                .map_err(|_invalid| corrupt("User id is not valid"))?,
            trace_id: coral_telemetry::TraceId::parse(&self.trace_id)
                .map_err(|_invalid| corrupt("trace id is not valid"))?,
            target,
            interval_seconds: u64::try_from(self.interval_seconds)
                .map_err(|_invalid| corrupt("interval is not valid"))?,
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

/// The most jobs one recovery pass reads.
pub(crate) const RECOVERY_PAGE: u64 = 100;

/// The jobs whose lease has ended, the longest-ended first, one page at most.
/// Each row decodes on its own, so a corrupt row fails only itself.
pub(super) async fn recoverable(
    tx: &mut Transaction<'_>,
    now: i64,
) -> Result<Vec<Recoverable>, Error> {
    let rows: Vec<JobRow> = tx
        .fetch_all(
            Query::select()
                .columns(columns())
                .from(Jobs::Table)
                .and_where(Expr::col(Jobs::LeaseUntil).lte(now))
                .order_by(Jobs::LeaseUntil, Order::Asc)
                .order_by(Jobs::Id, Order::Asc)
                .limit(RECOVERY_PAGE)
                .to_owned(),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| Recoverable {
            key: row.key(),
            job: row.decode(),
        })
        .collect())
}

/// Moves a job that recovery could not resume behind the other recoverable
/// jobs: its lease now ends at `until`. Only a lease that has ended moves, so
/// a worker that claimed the job since `recoverable` read it keeps its lease.
pub(super) async fn defer(
    tx: &mut Transaction<'_>,
    key: &StoredKey,
    now: i64,
    until: i64,
) -> Result<(), Error> {
    // Not `scope()`: the key is the stored text, since a corrupt row's
    // Workspace id may not parse.
    tx.execute(
        Query::update()
            .table(Jobs::Table)
            .value(Jobs::LeaseUntil, until)
            .and_where(Expr::col(Jobs::WorkspaceId).eq(key.workspace_id.as_str()))
            .and_where(Expr::col(Jobs::Id).eq(key.id.as_str()))
            .and_where(Expr::col(Jobs::LeaseUntil).lte(now))
            .to_owned(),
    )
    .await?;
    Ok(())
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
        return Err(Error::DeviceLeaseLost);
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
    let interval = i64::try_from(interval)
        .map_err(|_invalid| Error::Internal(crate::InternalFault::Overflow))?;
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
        return Err(Error::DeviceLeaseLost);
    }
    Ok(())
}

/// The timing that each of the Workspace's jobs stores. A test asserts the
/// schedule a worker stored, in place of a wait for the poll that the
/// schedule sets.
#[cfg(test)]
pub(crate) async fn stored_timings(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
) -> Result<Vec<super::device_schedule::Timing>, Error> {
    #[derive(sqlx::FromRow)]
    struct TimingRow {
        interval_seconds: i64,
        next_poll_at: i64,
    }
    let rows: Vec<TimingRow> = tx
        .fetch_all(
            Query::select()
                .columns([Jobs::IntervalSeconds, Jobs::NextPollAt])
                .from(Jobs::Table)
                .and_where(Expr::col(Jobs::WorkspaceId).eq(owner.to_string()))
                .order_by(Jobs::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            let seconds =
                u64::try_from(row.interval_seconds).map_err(|_invalid| Error::AccountInput)?;
            Ok(super::device_schedule::Timing {
                interval: std::time::Duration::from_secs(seconds),
                next_poll_at: coral_db::UnixNanos::new(row.next_poll_at),
            })
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::{Error, JobRow, insert, renew, schedule};

    // Catches: a stored job that does not decode read as invalid input from
    // a caller, which the row never was.
    #[test]
    fn a_stored_job_that_does_not_decode_is_corrupt_data() {
        type Break = fn(&mut JobRow);
        let row = || JobRow {
            id: "00000000-0000-4000-8000-000000000000".into(),
            workspace_id: "0199f7a2-6a6e-7c3d-9a51-2f4e8b7d1c06".into(),
            actor_user_id: "user:device".into(),
            trace_id: coral_telemetry::TraceId::current_or_new()
                .as_str()
                .to_owned(),
            setup_id: None,
            interval_seconds: 5,
            next_poll_at: 0,
        };
        row().decode().expect("a valid row decodes");
        let corrupt: [(&str, Break); 6] = [
            ("id", |row| row.id = "not-an-id".into()),
            ("setup id", |row| row.setup_id = Some("not-an-id".into())),
            ("workspace id", |row| row.workspace_id = "not-an-id".into()),
            ("user id", |row| row.actor_user_id = String::new()),
            ("trace id", |row| row.trace_id = "corrupt".into()),
            ("interval", |row| row.interval_seconds = -1),
        ];
        for (field, break_it) in corrupt {
            let mut row = row();
            break_it(&mut row);
            assert!(
                matches!(row.decode(), Err(Error::CorruptData(_))),
                "{field}"
            );
        }
    }
    use crate::tests::{change_context, organization_user, test_modules};
    use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

    // Catches: a worker whose lease lapsed, or went to another worker,
    // reading that as invalid input, and a refused renewal or schedule that
    // still moves the job.
    #[tokio::test]
    async fn a_lease_this_worker_does_not_hold_is_lost_and_changes_nothing() {
        let (db, workspaces, _providers) = test_modules().await;
        let context = change_context(
            organization_user(db.database(), "user:device-lease", OrganizationRole::Owner).await,
            10,
            'a',
        );
        let workspace = workspaces
            .create(&context, WorkspaceDisplayName::parse("Device").unwrap())
            .await
            .unwrap();
        let owner = workspace.id();
        let database = db.database();
        let mut tx = database.begin().await.unwrap();
        insert(&mut tx, &context, owner, "job", None).await.unwrap();
        tx.commit().await.unwrap();
        // The job as the worker with the token `held` left it.
        let held = "UPDATE provider_device_jobs SET lease_token = 'held', lease_until = 100 \
                    WHERE id = 'job'";
        assert_eq!(db.execute_fixture_sql(held).await.unwrap(), 1);
        let unchanged = "UPDATE provider_device_jobs SET lease_token = 'held' \
                         WHERE id = 'job' AND lease_until = 100 AND interval_seconds = 5 \
                         AND next_poll_at = 10";

        let mut tx = database.begin().await.unwrap();
        assert!(matches!(
            renew(&mut tx, owner, "job", "other", 50, 200).await,
            Err(Error::DeviceLeaseLost)
        ));
        // The right token after the lease lapsed.
        assert!(matches!(
            renew(&mut tx, owner, "job", "held", 150, 300).await,
            Err(Error::DeviceLeaseLost)
        ));
        assert!(matches!(
            schedule(&mut tx, owner, "job", "other", 9, 99).await,
            Err(Error::DeviceLeaseLost)
        ));
        tx.commit().await.unwrap();
        assert_eq!(db.execute_fixture_sql(unchanged).await.unwrap(), 1);

        // An interval that the column cannot hold is a fault in Coral.
        let mut tx = database.begin().await.unwrap();
        assert!(matches!(
            schedule(&mut tx, owner, "job", "held", u64::MAX, 99).await,
            Err(Error::Internal(crate::InternalFault::Overflow))
        ));
        tx.rollback().await.unwrap();
        assert_eq!(db.execute_fixture_sql(unchanged).await.unwrap(), 1);

        let mut tx = database.begin().await.unwrap();
        renew(&mut tx, owner, "job", "held", 50, 200).await.unwrap();
        schedule(&mut tx, owner, "job", "held", 9, 99)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(db.execute_fixture_sql(unchanged).await.unwrap(), 0);
    }
}
