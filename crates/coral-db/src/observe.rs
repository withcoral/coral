//! Bounded waiting on committed state for a condition the domain decides.

use std::time::Duration;

use tokio::time::Instant;

use crate::{ChangeSubscription, Database};

/// Minimum spacing between one waiter's rereads.
const MIN_REREAD_GAP: Duration = Duration::from_millis(50);
/// Reread cadence for a waiter whose change notifications are unavailable.
const FALLBACK_POLL: Duration = Duration::from_millis(500);

/// One bounded wait for committed changes. The domain reads a snapshot, decides
/// whether it is ready, and otherwise calls [`ChangeWait::pause`] before rereading:
///
/// ```ignore
/// let mut wait = database.wait_for_changes(timeout);
/// loop {
///     let snapshot = read().await?;
///     if ready(&snapshot) || !wait.pause(None).await {
///         return Ok(snapshot);
///     }
/// }
/// ```
pub struct ChangeWait {
    deadline: Instant,
    changes: Option<ChangeSubscription>,
}

impl Database {
    /// Starts a wait of at most `wait`. Create it before the first read, so a
    /// commit during that read still wakes the wait. A wake-up is only a hint:
    /// the database stays authoritative, so unavailable notifications degrade
    /// to slow polling instead of failing the wait.
    #[must_use]
    pub fn wait_for_changes(&self, wait: Duration) -> ChangeWait {
        ChangeWait {
            deadline: Instant::now() + wait,
            changes: if wait.is_zero() {
                None
            } else {
                self.subscribe_changes().ok()
            },
        }
    }
}

impl ChangeWait {
    /// Waits for a committed change, `wake_at`, or the deadline, then spaces
    /// rereads so busy writers coalesce. Returns false once the deadline has
    /// passed: the caller returns its last snapshot instead of rereading.
    pub async fn pause(&mut self, wake_at: Option<Instant>) -> bool {
        let read_at = Instant::now();
        if read_at >= self.deadline {
            return false;
        }
        let wake = wake_at.map_or(self.deadline, |wake_at| wake_at.min(self.deadline));
        match &mut self.changes {
            Some(subscription) => tokio::select! {
                result = subscription.changed() => if result.is_err() { self.changes = None },
                () = tokio::time::sleep_until(wake) => {}
            },
            None => tokio::time::sleep_until(wake.min(read_at + FALLBACK_POLL)).await,
        }
        tokio::time::sleep_until(self.deadline.min(read_at + MIN_REREAD_GAP)).await;
        true
    }
}
