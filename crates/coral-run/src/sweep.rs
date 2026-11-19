//! The Run sweeper (ADR 0027): ends what no Run stream decided in time.
//!
//! A Run that a Run Executor holds has no Run Owner while no stream is open
//! for it, and a loop that holds its stream may hang. One sweeper at a time,
//! elected by a lease row, covers both. Each pass, bounded and oldest first,
//! it ends the Runs past their deadline, expires the Approval Requests past
//! theirs, puts back the taken Runs that never started, and releases the Run
//! Executors whose lease lapsed: their started Runs end, their unstarted ones
//! wait again, and their row goes.
//!
//! Every write reads its Run again in its own transaction and is predicated
//! on the claim and the head it shows, so a loop that writes first wins, and
//! two sweepers write nothing twice. A Run Executor's Runs are released only
//! under a hold of its row at the lease the pass read: a Run Executor that
//! renewed it since is back, and its Runs are its own. The sweeper never
//! touches a Run that a Run Owner holds: a worker claim fences each one.

use std::ops::AddAssign;
use std::time::Duration;

use coral_db::{DatabaseSystem, Transaction, UnixNanos};
use coral_run_executor::leases::{self, RunExecutorLease};
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;

use crate::approval::{self, ApprovalId, DecideWork};
use crate::lifecycle::RunRecord;
use crate::owner::ChunkWork as _;
use crate::run_repository::{self, Chunk, HeldRun, UndecodableRun};
use crate::termination::commit_end;
use crate::types::HeldRunId;
use crate::{Error, RunId, RunModule, RunState};

/// How often bootstrap runs a pass.
const SWEEP_INTERVAL: Duration = Duration::from_secs(15);

/// How long one election holds: three passes, so another Coral Instance
/// takes over once the holder missed two.
const SWEEPER_LEASE: Duration = Duration::from_secs(45);

/// How long a taken Run waits for its start before it goes back to waiting.
const START_CONFIRMATION: Duration = Duration::from_secs(10);

/// How many Runs, Approval Requests and Run Executors of each kind one pass
/// writes; the rest wait for the next pass.
const PASS_LIMIT: u64 = 100;

/// What one sweep pass wrote.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    /// Runs ended: past their deadline, or started by a Run Executor that is
    /// gone.
    pub ended: usize,
    /// Approval Requests expired.
    pub expired: usize,
    /// Taken Runs that never started, back to waiting.
    pub put_back: usize,
    /// Run Executor rows deleted.
    pub released: usize,
}

impl Swept {
    /// The Runs released by ending them or putting them back.
    fn released_runs(self) -> u64 {
        u64::try_from(self.ended + self.put_back).unwrap_or(u64::MAX)
    }
}

impl AddAssign for Swept {
    fn add_assign(&mut self, other: Self) {
        self.ended += other.ended;
        self.expired += other.expired;
        self.put_back += other.put_back;
        self.released += other.released;
    }
}

/// Which pass runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pass {
    /// Every [`SWEEP_INTERVAL`].
    Periodic,
    /// Once, before any listener serves.
    Boot,
}

/// What releasing one of a Run Executor's Runs wrote.
enum Released {
    Ended,
    PutBack,
    /// Nothing: the Run moved on since the read.
    Skipped,
    /// Nothing: the Run Executor renewed its lease since the read.
    Renewed,
}

impl RunModule {
    /// How often bootstrap runs [`Self::sweep_runs`].
    #[must_use]
    pub const fn run_sweep_interval(&self) -> Duration {
        SWEEP_INTERVAL
    }

    /// One pass of the Run sweeper: elects this Coral Instance, or renews
    /// its election, and sweeps. `None` when another Coral Instance holds
    /// the election, or this one is shutting down.
    ///
    /// A write that fails is logged and the pass goes on; the next pass reads
    /// that Run again.
    ///
    /// # Errors
    ///
    /// Returns a clock failure, or a database failure of the election or of
    /// a read; the next pass tries again.
    pub async fn sweep_runs(&self) -> Result<Option<Swept>, Error> {
        self.sweep(Pass::Periodic, PASS_LIMIT).await
    }

    /// The boot pass, before any listener serves. On `SQLite` it takes the
    /// election whatever its lease, since one Coral process holds the
    /// database, and first releases every local child's row: this process
    /// is every local child's parent, so a child of an earlier one is gone,
    /// and none of its own has said hello yet. A dialed Run Executor's Runs
    /// wait for its lease. On Postgres it is a plain pass.
    ///
    /// # Errors
    ///
    /// As [`Self::sweep_runs`].
    pub async fn sweep_runs_at_boot(&self) -> Result<Option<Swept>, Error> {
        self.sweep(Pass::Boot, PASS_LIMIT).await
    }

    /// Releases Run Executor `run_executor_id` at once, whatever its lease:
    /// the write a Coral Instance runs when a local child of its own exits.
    /// Its started Runs end, its unstarted ones wait again, and its row
    /// goes. Nothing when it has no row, or this Coral Instance is shutting
    /// down.
    ///
    /// # Errors
    ///
    /// Returns a clock failure or a database failure of a read.
    pub async fn release_run_executor(&self, run_executor_id: &str) -> Result<Swept, Error> {
        if self.admission.check().is_err() {
            return Ok(Swept::default());
        }
        let now = self.now()?;
        match leases::find(&*self.database, run_executor_id).await? {
            Some(lease) => self.release(&lease, now, PASS_LIMIT).await,
            None => Ok(Swept::default()),
        }
    }

    /// A pass that writes at most `limit` of each kind.
    pub(crate) async fn sweep(&self, pass: Pass, limit: u64) -> Result<Option<Swept>, Error> {
        // A process shutting down leaves Runs to the next one.
        if self.admission.check().is_err() {
            return Ok(None);
        }
        let now = UnixNanos::new(self.now()?);
        let sqlite_boot = pass == Pass::Boot && self.database.system() == DatabaseSystem::Sqlite;
        if !run_repository::elect_sweeper(
            &self.database,
            &self.instance,
            now.saturating_add(SWEEPER_LEASE),
            (!sqlite_boot).then_some(now),
        )
        .await?
        {
            return Ok(None);
        }
        let mut swept = Swept::default();
        if sqlite_boot {
            let local = leases::local(&*self.database, limit).await?;
            swept += self.release_each(&local, now.get(), limit).await;
        }
        swept.ended += self.end_past_deadline(now, limit).await?;
        swept.expired += self.expire_past_deadline(now, limit).await?;
        swept.put_back += self.put_back_unstarted(now, limit).await?;
        let lapsed = leases::lapsed(&*self.database, now, limit).await?;
        swept += self.release_each(&lapsed, now.get(), limit).await;
        Ok(Some(swept))
    }

    async fn end_past_deadline(&self, now: UnixNanos, limit: u64) -> Result<usize, Error> {
        let mut ended = 0;
        for held in run_repository::past_deadline(&*self.database, now, limit).await? {
            let Some(held) = decoded(held) else { continue };
            ended += tally(self.end_on_deadline(&held, now.get()).await, &held.run_id);
        }
        Ok(ended)
    }

    /// Ends the Run with its recorded termination, else timed out, when its
    /// deadline still passed as the transaction reads it.
    async fn end_on_deadline(&self, held: &HeldRun, now: i64) -> Result<bool, Error> {
        let mut transaction = self.database.begin().await?;
        match unowned(&mut transaction, &held.workspace_id, &held.run_id).await? {
            Some(run) if run.deadline_passed(now) => end(transaction, &run, now).await,
            _ => Ok(false),
        }
    }

    async fn expire_past_deadline(&self, now: UnixNanos, limit: u64) -> Result<usize, Error> {
        let mut expired = 0;
        for request in approval::undecided_on_run_executors(&*self.database, now, limit).await? {
            let Some((workspace_id, run_id, approval)) = decoded(request) else {
                continue;
            };
            let written = self
                .expire(&workspace_id, &run_id, &approval, now.get())
                .await;
            expired += tally(written, &run_id);
        }
        Ok(expired)
    }

    /// Expires `approval` in one chunk under the Run's claim and head, when
    /// it is still undecided past its deadline and the Run records no
    /// termination: a recorded one closes it in the Run's end.
    async fn expire(
        &self,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        approval: &ApprovalId,
        now: i64,
    ) -> Result<bool, Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) = unowned(&mut transaction, workspace_id, run_id).await? else {
            return Ok(false);
        };
        if run.termination.is_some()
            || !approval::undecided_past(&mut transaction, &run, approval, now).await?
        {
            return Ok(false);
        }
        let now = now.max(run.accepted_at_unix_nanos);
        let events = DecideWork::expiry(approval.clone())
            .run(&mut transaction, &run, now)
            .await?;
        let chunk = Chunk::new(&run, run.execution_claim_id.as_deref(), now).owned_events(&events);
        if unless_moved(run_repository::commit_chunk(&mut transaction, chunk).await)?.is_none() {
            return Ok(false);
        }
        transaction.commit().await?;
        Ok(true)
    }

    async fn put_back_unstarted(&self, now: UnixNanos, limit: u64) -> Result<usize, Error> {
        let taken_before = now
            .checked_sub(START_CONFIRMATION)
            .unwrap_or(UnixNanos::new(i64::MIN));
        let mut put_back = 0;
        for held in run_repository::unstarted_takes(&*self.database, taken_before, limit).await? {
            let Some(held) = decoded(held) else { continue };
            let written = match self.database.begin().await {
                Ok(transaction) => put_back_in(transaction, &held).await,
                Err(error) => Err(error.into()),
            };
            put_back += tally(written, &held.run_id);
        }
        Ok(put_back)
    }

    /// Releases each of `leases`, until the Runs they released this pass
    /// reach `limit`; a failure is logged, and the next pass reads that Run
    /// Executor again.
    async fn release_each(&self, leases: &[RunExecutorLease], now: i64, limit: u64) -> Swept {
        let mut swept = Swept::default();
        for lease in leases {
            let left = limit.saturating_sub(swept.released_runs());
            if left == 0 {
                break;
            }
            match self.release(lease, now, left).await {
                Ok(released) => swept += released,
                Err(error) => tracing::warn!(
                    coral.run_executor.id = run_executor_id(lease.id()).as_ref().map(safe::id),
                    error.type = safe::kind(error.code()),
                    "the Run sweeper did not release a Run Executor; the next pass retries it"
                ),
            }
        }
        swept
    }

    /// Ends up to `limit` of `lease`'s started Runs and puts its unstarted
    /// ones back, each under a hold of its row at the lease read, then
    /// deletes the row once no running Run names it. A renewal since the
    /// read stops it.
    pub(crate) async fn release(
        &self,
        lease: &RunExecutorLease,
        now: i64,
        limit: u64,
    ) -> Result<Swept, Error> {
        let held = run_repository::held_by_run_executor(&*self.database, lease.id(), limit).await?;
        let mut swept = Swept::default();
        for run in held {
            let Some(run) = decoded(run) else { continue };
            match self.release_run(lease, &run, now).await {
                Ok(Released::Ended) => swept.ended += 1,
                Ok(Released::PutBack) => swept.put_back += 1,
                Ok(Released::Skipped) => {}
                Ok(Released::Renewed) => return Ok(swept),
                Err(error) => skip_failed(&run.run_id, &error),
            }
        }
        let mut transaction = self.database.begin().await?;
        if leases::hold(&mut transaction, lease).await?
            && !run_repository::names_run_executor(&mut transaction, lease.id()).await?
            && leases::delete(&mut transaction, lease).await?
        {
            swept.released += 1;
        }
        transaction.commit().await?;
        Ok(swept)
    }

    /// Ends one of `lease`'s Runs, or puts it back when it never started,
    /// while the Run still holds the take's claim read.
    async fn release_run(
        &self,
        lease: &RunExecutorLease,
        held: &HeldRun,
        now: i64,
    ) -> Result<Released, Error> {
        let mut transaction = self.database.begin().await?;
        if !leases::hold(&mut transaction, lease).await? {
            return Ok(Released::Renewed);
        }
        // A take mints a new claim: the same one means the same take, so the
        // Run still names this Run Executor.
        let run = match unowned(&mut transaction, &held.workspace_id, &held.run_id).await? {
            Some(run) if run.execution_claim_id.as_deref() == Some(held.claim.as_str()) => run,
            _ => return Ok(Released::Skipped),
        };
        let written = if run.execution_started() {
            end(transaction, &run, now)
                .await?
                .then_some(Released::Ended)
        } else {
            put_back_in(transaction, held)
                .await?
                .then_some(Released::PutBack)
        };
        Ok(written.unwrap_or(Released::Skipped))
    }
}

/// The Run as `transaction` reads it, while it is running with no worker
/// claim: no Run Owner fences it.
async fn unowned(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    run_id: &RunId,
) -> Result<Option<RunRecord>, Error> {
    Ok(
        run_repository::find_exact_in_transaction(transaction, workspace_id, run_id)
            .await?
            .filter(|run| run.state == RunState::Running && run.worker_claim_id.is_none()),
    )
}

/// The one chunk that ends `run`, read in `transaction`: its recorded
/// termination, else timed out past its deadline, else interrupted.
/// `false` when another writer moved the Run on first.
async fn end(transaction: Transaction<'_>, run: &RunRecord, now: i64) -> Result<bool, Error> {
    let now = now.max(run.accepted_at_unix_nanos);
    Ok(unless_moved(commit_end(transaction, run, None, None, now).await)?.is_some())
}

/// Puts `held` back in `transaction` and commits, logging the Run Executor
/// it was taken for.
async fn put_back_in(mut transaction: Transaction<'_>, held: &HeldRun) -> Result<bool, Error> {
    if !run_repository::put_back(&mut transaction, held).await? {
        return Ok(false);
    }
    transaction.commit().await?;
    tracing::info!(
        coral.run.id = safe::id(&HeldRunId::of_held_row(held.run_id.clone())),
        coral.run_executor.id = held
            .run_executor_id
            .as_deref()
            .and_then(run_executor_id)
            .as_ref()
            .map(safe::id),
        "a taken Run never started; it waits again"
    );
    Ok(true)
}

/// `None` when the write found its Run moved on: another writer took it,
/// moved its head or ended it first. It wrote nothing.
fn unless_moved<T>(written: Result<T, Error>) -> Result<Option<T>, Error> {
    match written {
        Ok(written) => Ok(Some(written)),
        Err(Error::ClaimMismatch | Error::HeadMismatch | Error::RunEnded) => Ok(None),
        Err(error) => Err(error),
    }
}

/// One for a write that landed; a failed one is logged.
fn tally(written: Result<bool, Error>, run_id: &RunId) -> usize {
    match written {
        Ok(landed) => usize::from(landed),
        Err(error) => {
            skip_failed(run_id, &error);
            0
        }
    }
}

/// The decoded candidate, or `None` after logging one whose stored row
/// does not decode; the next pass reads it again.
fn decoded<T>(candidate: Result<T, UndecodableRun>) -> Option<T> {
    candidate
        .map_err(|undecodable| {
            tracing::warn!(
                coral.run.id = undecodable.id.as_ref().map(safe::id),
                error.type = safe::kind(undecodable.error.code()),
                "the Run sweeper skips a Run whose stored row does not decode"
            );
        })
        .ok()
}

fn skip_failed(run_id: &RunId, error: &Error) {
    tracing::warn!(
        coral.run.id = safe::id(&HeldRunId::of_held_row(run_id.clone())),
        error.type = safe::kind(error.code()),
        "a Run sweeper write failed; the next pass retries it"
    );
}

/// The Run Executor's id as a field value: it declares its own id, so only
/// a UUID is logged.
fn run_executor_id(id: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::try_parse(id).ok()
}
