//! The Run queue pass: reads the Runs waiting, the oldest wait and the Runs
//! held from the database, for the gauges that report them (ADR 0027,
//! Telemetry).
//!
//! Every Coral Instance runs the pass on the same database, across every
//! Workspace, so a backend takes the maximum of what they report, not the
//! sum.
//! No election and no lease: a read takes no lock, and each Coral Instance
//! keeps reporting while the others stop.

use std::time::Duration;

use coral_db::{Clock, Database};
use coral_telemetry::metric;

use crate::{Error, RunModule, run_repository};

/// How often bootstrap runs [`RunModule::read_queue`].
pub const RUN_QUEUE_INTERVAL: Duration = Duration::from_secs(15);

impl RunModule {
    /// Reads the Run queue in one read snapshot and reports it on
    /// `coral.run.waiting`, `coral.run.waiting.age` and `coral.run.held`
    /// until the next pass. The age reads `clock`, the instance's one
    /// clock, once. A failed read reports nothing, so the queue shows as
    /// unknown, not empty. With no metric catalog it reads nothing.
    ///
    /// # Errors
    ///
    /// Returns a database failure of the read.
    pub async fn read_queue(&self, clock: &dyn Clock) -> Result<(), Error> {
        if !metric::enabled() {
            return Ok(());
        }
        let read = read(&self.database, clock).await;
        metric::run_queue_read(read.as_ref().ok().copied());
        read.map(drop)
    }
}

/// The Run queue as one read snapshot of `database` finds it, aged by
/// `clock`.
pub(crate) async fn read(
    database: &Database,
    clock: &dyn Clock,
) -> Result<metric::RunQueue, Error> {
    let mut tx = database.begin_read_snapshot().await?;
    // Once per pass, as the snapshot opens.
    let now = clock.now();
    let counts = run_repository::queue_counts(&mut tx).await?;
    tx.rollback().await?;
    Ok(metric::RunQueue {
        waiting: counts.waiting,
        // An acceptance after `now` by another Coral Instance's clock
        // has waited for no time.
        waiting_age: counts.oldest_accepted.map_or(Duration::ZERO, |oldest| {
            now.saturating_duration_since(oldest)
        }),
        held: counts.held,
    })
}
