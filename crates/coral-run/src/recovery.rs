//! The expired-lease sweep (ADR 0018, Recovery): the worker lease is
//! ownership, so a `running` Run whose lease lapsed with no owner in this
//! process, or one with no claim accepted more than one lease ago, has no
//! process that owns it. The sweep recreates its Run Owner with the mailbox
//! already queued (every timer that passed, in deadline order, then the end)
//! and that owner writes one chunk: the expiries, then the Run's end with
//! every nonterminal Invocation ended, the Run taken over with a new claim.
//! Coral never resumes a Run.
//!
//! A Run with a live lease belongs to another process (a rolling update's
//! other process) and is never touched; a local owner whose lease lapsed is
//! never swept either: it continues while its commits find its claim, and a
//! takeover by another process reaches it as a claim mismatch.

use std::collections::HashMap;
use std::time::Duration;

use coral_workspace::WorkspaceId;
use uuid::Uuid;

use crate::approval::ApprovalId;
use crate::lifecycle::{RequestedTermination, RunRecord, RunTerminationKind};
use crate::owner::{self, Message, OwnerHandle, OwnerStart, RecoveryEnd, TakenClaim, Timer};
use crate::termination::end_in_one_chunk;
use crate::types::AcceptedRun;
use crate::{Error, RunId, RunModule, approval, run_repository};

/// How many recovered owners write their chunk at once in one process.
const RECOVERY_WRITES_AT_ONCE: usize = 4;

/// The permits [`RunModule`]'s recovered owners write under.
pub(crate) fn write_permits() -> std::sync::Arc<tokio::sync::Semaphore> {
    std::sync::Arc::new(tokio::sync::Semaphore::new(RECOVERY_WRITES_AT_ONCE))
}

impl RunModule {
    /// How often bootstrap runs [`Self::sweep_expired_leases`]: every half
    /// lease, so a Run whose process died ends within one lease and one
    /// sweep.
    #[must_use]
    pub fn sweep_interval(&self) -> Duration {
        self.execution_policy.sweep_interval()
    }

    /// One pass of the expired-lease sweep: reads, in one snapshot ordered by
    /// acceptance, every `running` Run with no owner in this process whose
    /// lease lapsed, or with no claim and accepted more than one lease ago,
    /// and recreates its Run Owner. Returns once those owners exist, with how
    /// many it recreated; each ends its Run in one chunk of its own, and a
    /// request that reaches one before that chunk waits behind it.
    ///
    /// Bootstrap runs the first pass before any listener binds, then on a
    /// fixed cadence of [`Self::sweep_interval`].
    ///
    /// # Errors
    ///
    /// Returns a clock or persistence failure of the snapshot read; the
    /// next pass reads again. Once the process is shutting down a pass
    /// recreates nothing.
    pub async fn sweep_expired_leases(&self) -> Result<usize, Error> {
        // A process shutting down leaves Runs to the next one.
        if self.admission.check().is_err() {
            return Ok(0);
        }
        let now = self.now()?;
        let mut transaction = self.database.begin_read_snapshot().await?;
        let lapsed = run_repository::lapsed_leases(
            &mut transaction,
            now,
            self.execution_policy.claim_lease_nanos(),
        )
        .await?;
        let mut recoveries = Vec::new();
        for run in lapsed {
            if self.owners.contains(&run.id) {
                continue;
            }
            let timers = passed_timers(&mut transaction, &run, now).await?;
            recoveries.push((run, timers));
        }
        transaction.commit().await?;
        Ok(recoveries
            .into_iter()
            .map(|(run, timers)| self.schedule_recovery(&run, timers))
            .filter(|recreated| *recreated)
            .count())
    }

    /// Recreates the owner of `run`, with a new claim, its mailbox queued
    /// with `timers` and then the end: finish the termination the row
    /// records, else interrupt. `false` when an owner for the Run became live
    /// here meanwhile, or the process began shutting down. The owner is registered before its task starts, so a
    /// request that arrives first waits in its mailbox behind the queue.
    fn schedule_recovery(&self, run: &RunRecord, timers: Vec<Timer>) -> bool {
        if self.admission.check().is_err() {
            return false;
        }
        let claim_id = Uuid::new_v4().hyphenated().to_string();
        let (handle, mailbox) = OwnerHandle::recovering(
            run.id.clone(),
            claim_id.clone(),
            TakenClaim::of(run.held_claim()),
        );
        for timer in timers {
            handle.send(Message::Timer(timer));
        }
        handle.send(Message::Recover(if run.termination.is_some() {
            RecoveryEnd::FinishRecorded
        } else {
            RecoveryEnd::Interrupt
        }));
        let Some(registration) = self.owners.register_recovered(handle.clone()) else {
            return false;
        };
        tracing::info!(
            run_id = %run.id,
            claim_id = %claim_id,
            "expired-lease sweep recreates the Run Owner of a Run no process owns"
        );
        // The Run as its owner needs it; a recovered owner starts no Program,
        // so it reads no lease or linked snapshot from here.
        let accepted = AcceptedRun {
            id: run.id.clone(),
            workspace_id: run.workspace_id.clone(),
            principal: run.principal.clone(),
            accepted_at_unix_nanos: run.accepted_at_unix_nanos,
            deadline_at_unix_nanos: run.deadline_at_unix_nanos,
            shutdown_grace_nanos: run.shutdown_grace_nanos,
            value_limits: run.value_limits,
            claim_id,
            worker_claim_expires_at_unix_nanos: run
                .worker_claim_expires_at_unix_nanos
                .unwrap_or(run.accepted_at_unix_nanos),
            linked_snapshots: HashMap::new(),
        };
        let run_id = run.id.clone();
        let owner_task = tokio::spawn(owner::host(
            self.clone(),
            accepted,
            OwnerStart::Recovery,
            handle,
            mailbox,
            registration,
        ));
        // A recovered owner that ends without retiring wrote nothing it
        // cannot write again: the next sweep recreates it.
        tokio::spawn(async move {
            if let Err(ended) = owner_task.await {
                tracing::error!(
                    run_id = %run_id,
                    ended = %ended,
                    "recovered Run Owner ended without retiring; the next sweep recovers its Run"
                );
            }
        });
        true
    }

    /// A recovered owner's one chunk ([`end_in_one_chunk`]): takes the Run
    /// over from `taken`, the claim its row held when the sweep found its
    /// lease lapsed, with `takeover_claim_id`, expires the `expired` Approval
    /// Requests, and ends the Run with the termination its row records, else
    /// `intent`, else interrupted. The row is read without a hold; the write
    /// is predicated on the claim and the head the row showed.
    ///
    /// # Errors
    ///
    /// [`Error::ClaimMismatch`] when the row holds another claim, or its
    /// lease is live again (the process that holds it renewed it, so the Run
    /// is that process's); [`Error::RunEnded`] when the Run is terminal: by
    /// another writer, or by an earlier attempt of this write on a Run that
    /// never started, whose row keeps no claim to recognize it by;
    /// [`Error::HeadMismatch`] when another writer moved the head or ended an
    /// Invocation between the read and the write. None of them writes
    /// anything.
    pub(crate) async fn record_recovery(&self, recovery: &RecoveryWrite<'_>) -> Result<(), Error> {
        #[cfg(test)]
        self.execution_observer.before_recovery_write().await;
        let mut transaction = self.database.begin().await?;
        let run = run_repository::find_exact_in_transaction(
            &mut transaction,
            recovery.workspace_id,
            recovery.run_id,
        )
        .await?
        .ok_or(Error::ClaimMismatch)?;
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        if run.state.is_terminal() {
            // An earlier attempt of this write may have landed without an
            // answer: the row then carries the claim it took over with.
            return if run.execution_claim_id.as_deref() == Some(recovery.takeover_claim_id) {
                Ok(())
            } else {
                Err(Error::RunEnded)
            };
        }
        // The lease is ownership: a Run whose holder renewed its lease since
        // the sweep read it is that process's again.
        if run.held_claim() != recovery.taken
            || !run.lease_lapsed(now, self.execution_policy.claim_lease_nanos())
        {
            return Err(Error::ClaimMismatch);
        }
        // A termination the row records stands: it was applied first.
        let termination = match (run.termination, recovery.intent) {
            (Some(recorded), _) => recorded,
            (None, Some(intent)) => intent.termination,
            (None, None) => {
                let kind = if now >= run.deadline_at_unix_nanos {
                    RunTerminationKind::TimedOut
                } else {
                    RunTerminationKind::Interrupted
                };
                run.request_termination(kind, now)?.termination
            }
        };
        end_in_one_chunk(
            &mut transaction,
            &run,
            &termination,
            recovery.expired,
            Some(recovery.takeover_claim_id),
            now.max(termination.started_at_unix_nanos),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

/// What a recovered owner's one chunk writes, as its `apply` decided it.
pub(crate) struct RecoveryWrite<'a> {
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) run_id: &'a RunId,
    /// The claim the Run row held when the sweep read it.
    pub(crate) taken: Option<&'a str>,
    pub(crate) takeover_claim_id: &'a str,
    pub(crate) expired: &'a [ApprovalId],
    pub(crate) intent: Option<&'a RequestedTermination>,
}

/// The Run's timers that passed by `now_unix_nanos`, in deadline order: each
/// Approval Request's still undecided, and the Run deadline. A termination
/// the row records was applied at its start: only an Approval Request whose
/// deadline came before it still expires, and the deadline no longer
/// applies. An Approval Request and the Run deadline at the same instant
/// expire the request first.
async fn passed_timers(
    transaction: &mut coral_db::Transaction<'_>,
    run: &RunRecord,
    now_unix_nanos: i64,
) -> Result<Vec<Timer>, Error> {
    let applies_before_termination = |deadline: i64| {
        run.termination
            .is_none_or(|termination| deadline < termination.started_at_unix_nanos)
    };
    let mut timers: Vec<(i64, bool, Timer)> =
        approval::undecided_by(transaction, run, now_unix_nanos)
            .await?
            .into_iter()
            .filter(|(_, deadline)| applies_before_termination(*deadline))
            .map(|(approval, deadline)| (deadline, false, Timer::Approval(approval)))
            .collect();
    if run.termination.is_none() && run.deadline_at_unix_nanos <= now_unix_nanos {
        timers.push((run.deadline_at_unix_nanos, true, Timer::RunDeadline));
    }
    timers.sort_by_key(|(deadline, run_deadline, _)| (*deadline, *run_deadline));
    Ok(timers.into_iter().map(|(_, _, timer)| timer).collect())
}
