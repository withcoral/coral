//! Human activity invalidation, independent of the transport and reading window.
//! Notifications carry no content or authority. Callers must reread authorized
//! projections, suppress unchanged views, and resnapshot after reconnecting.
use crate::{Error, RunModule};
use coral_workspace::AuthenticatedPrincipal;
use std::time::Duration;

/// How often an observer rechecks authority, whether or not activity changes.
const ACTIVITY_HEARTBEAT: Duration = Duration::from_secs(5);

/// Why [`ActivityObservation::next`] returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityWake {
    /// A committed change, or the expiry of the invoking authority behind
    /// an action. Reread authorized activity. A Run's or a request's
    /// deadline wakes nothing by itself: its owner's record of the timeout
    /// or the expiry is a committed change.
    Changed,
    /// Authority still holds and nothing changed. Do not reread activity.
    Heartbeat,
}

/// One human-only, coalescing activity observer. Dropping it detaches observation;
/// it cannot cancel execution, decide actions, or extend retention.
pub struct ActivityObservation {
    runs: RunModule,
    actor: AuthenticatedPrincipal,
    changes: coral_db::ChangeSubscription,
    deadline: Option<i64>,
    heartbeat_at: tokio::time::Instant,
}

impl RunModule {
    /// Subscribes before the caller's initial snapshot so concurrent commits cannot
    /// be lost. The caller reads activity once now, then after each `Changed` wake.
    /// # Errors
    /// Rejects Agents, unavailable notification infrastructure, and lost User authority.
    pub async fn observe_activity(
        &self,
        actor: &AuthenticatedPrincipal,
    ) -> Result<ActivityObservation, Error> {
        let changes = self.database.subscribe_changes()?;
        let mut observer = ActivityObservation {
            runs: self.clone(),
            actor: actor.clone(),
            changes,
            deadline: None,
            heartbeat_at: tokio::time::Instant::now() + ACTIVITY_HEARTBEAT,
        };
        observer.reschedule().await?;
        Ok(observer)
    }
}

impl ActivityObservation {
    /// Rechecks authority. The authority lives only as long as `tx`.
    async fn authorize(
        &self,
        tx: &mut coral_db::Transaction<'_>,
    ) -> Result<coral_workspace::HistoryAuthority, Error> {
        self.changes.check()?;
        Ok(self
            .runs
            .workspaces
            .action_history_authority_in_transaction(tx, &self.actor, None)
            .await?)
    }

    async fn heartbeat(&mut self) -> Result<(), Error> {
        let mut tx = self.runs.database.begin_read_snapshot().await?;
        self.authorize(&mut tx).await?;
        tx.commit().await?;
        self.heartbeat_at = tokio::time::Instant::now() + ACTIVITY_HEARTBEAT;
        Ok(())
    }

    /// Rechecks authority and finds the next time-dependent visibility
    /// change: the expiry of an invoking authority behind a current action.
    async fn reschedule(&mut self) -> Result<(), Error> {
        let mut tx = self.runs.database.begin_read_snapshot().await?;
        let authority = self.authorize(&mut tx).await?;
        let now = self.runs.now()?;
        self.deadline =
            crate::required_actions_repository::next_authority_deadline(&mut tx, &authority, now)
                .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Waits for a change or an authority expiry, or for the next authorized
    /// liveness check. Expiries include actions outside the caller's loaded
    /// window.
    /// # Errors
    /// Fails closed on authority or notification loss; reconnect with a new snapshot.
    pub async fn next(&mut self) -> Result<ActivityWake, Error> {
        let heartbeat = self
            .heartbeat_at
            .saturating_duration_since(tokio::time::Instant::now());
        // Constant unrelated changes must not starve liveness or reveal their cadence.
        if heartbeat.is_zero() {
            self.heartbeat().await?;
            return Ok(ActivityWake::Heartbeat);
        }
        let until_deadline = self
            .deadline
            .map(|deadline| {
                self.runs.now().map(|now| {
                    Duration::from_nanos(u64::try_from(deadline.saturating_sub(now)).unwrap_or(0))
                })
            })
            .transpose()?
            .filter(|duration| *duration <= heartbeat);
        let changed = tokio::select! {
            result = self.changes.changed() => { result?; true }
            () = tokio::time::sleep(until_deadline.unwrap_or(heartbeat)) => until_deadline.is_some(),
        };
        if changed {
            self.reschedule().await?;
            Ok(ActivityWake::Changed)
        } else {
            self.heartbeat().await?;
            Ok(ActivityWake::Heartbeat)
        }
    }
}
