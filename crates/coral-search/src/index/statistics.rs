//! The recount of each Workspace's Postgres ranking statistics, off the
//! write path.
//!
//! An index write upserts its own rows and counts no statistics. Once it
//! commits, it marks its Workspace, and one recount follows the first mark by
//! [`RECOUNT_DELAY`]. Every write that marks the Workspace in that time shares
//! the recount. That recount always counts: a write's stamps alone may not
//! show the change when another host's clock runs ahead. The reconcile pass
//! counts each Workspace whose watermark no longer matches its rows, so a
//! recount that failed, or a process that stopped before it, leaves the
//! statistics stale until the pass.
//!
//! Document frequency and mean length a few seconds stale barely move a BM25
//! score. Candidate selection reads the text vectors, not the statistics, so
//! a new row matches its words at once. Until the recount, a word that only
//! new rows hold scores as a word no row holds, and no near spelling or
//! compact variant reaches it.

use std::collections::BTreeSet;
use std::sync::PoisonError;
use std::time::Duration;

use coral_db::DatabaseSystem;
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;
use tokio::sync::Notify;
use tracing::field::Empty;
use tracing::instrument::WithSubscriber as _;

use super::{InFlightWork, SearchIndex, WriteTrigger, in_write_span};
use crate::indexer::{self, IndexError, Recount};

/// How long after the first change of a Workspace's rows its recount runs.
/// A recount reads every row of the Workspace, so a burst of writes, such as a
/// reconcile pass or a run of saves, should pay for one.
pub(super) const RECOUNT_DELAY: Duration = Duration::from_secs(2);

/// The Workspaces whose recount is due, shared by every clone of one
/// [`SearchIndex`].
#[derive(Default)]
pub(super) struct PendingRecounts {
    pending: std::sync::Mutex<Pending>,
    /// Ends the wait of a scheduled recount early, for [`SearchIndex::settled`].
    flush: Notify,
}

#[derive(Default)]
struct Pending {
    workspaces: BTreeSet<WorkspaceId>,
    /// Whether a task waits to recount `workspaces`.
    scheduled: bool,
}

impl PendingRecounts {
    /// Ends the wait of the scheduled recount, or of the next one when none is
    /// scheduled yet.
    pub(super) fn flush(&self) {
        self.flush.notify_one();
    }
}

impl SearchIndex {
    /// Marks the Workspace's rows changed by a committed write, and schedules
    /// its recount unless one is scheduled already. Postgres only: `SQLite`'s
    /// FTS5 keeps its own statistics. A write of a reconcile pass schedules
    /// nothing: the pass's statistics step recounts it. The recount counts as
    /// in flight from before it is scheduled, so [`Self::settled`] never sees
    /// a scheduled recount it does not wait for.
    pub(super) fn rows_changed(&self, workspace_id: &WorkspaceId) {
        if !self.schedules_recounts || self.database.system() != DatabaseSystem::Postgres {
            return;
        }
        self.in_flight.send_modify(|work| *work += 1);
        let in_flight = InFlightWork(self.in_flight.clone());
        // No code panics while holding this lock, so a poisoned one still
        // holds a valid set.
        let schedule = {
            let mut pending = self
                .recounts
                .pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            pending.workspaces.insert(workspace_id.clone());
            !std::mem::replace(&mut pending.scheduled, true)
        };
        if !schedule {
            // The scheduled recount holds a count of its own.
            return;
        }
        let index = self.clone();
        let _recount = tokio::spawn(
            async move {
                let _in_flight = in_flight;
                tokio::select! {
                    () = tokio::time::sleep(index.recount_delay) => {}
                    () = index.recounts.flush.notified() => {}
                }
                // A write that marks a Workspace from here on schedules the
                // next recount.
                let workspaces = {
                    let mut pending = index
                        .recounts
                        .pending
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    pending.scheduled = false;
                    std::mem::take(&mut pending.workspaces)
                };
                for workspace_id in &workspaces {
                    // A failure is logged; the reconcile pass finds the
                    // stale watermark.
                    drop(
                        index
                            .recount(workspace_id, WriteTrigger::RowsChanged, Recount::Always)
                            .await,
                    );
                }
            }
            .with_current_subscriber(),
        );
    }

    /// Recounts one Workspace's statistics in a transaction of its own, as
    /// `when` decides, in a root span `coral.search.refresh_statistics`, the
    /// name earlier releases gave it. A failure is logged and returned.
    /// Returns the statistics rows written or deleted, `None` when the
    /// statistics were current.
    pub(super) async fn recount(
        &self,
        workspace_id: &WorkspaceId,
        trigger: WriteTrigger,
        when: Recount,
    ) -> Result<Option<u64>, IndexError> {
        let span = tracing::info_span!(
            parent: None,
            "coral.search.refresh_statistics",
            coral.workspace.id = safe::id(&workspace_id),
            coral.search.rows_written = Empty,
            coral.search.skipped = Empty,
            coral.search.write.trigger = Empty,
            error.type = Empty,
        );
        let recounted = in_write_span(
            span.clone(),
            trigger,
            self.within_limit(async {
                let mut transaction = self.database.begin().await?;
                let rows =
                    indexer::recount_statistics(&mut transaction, workspace_id, when).await?;
                transaction.commit().await?;
                Ok(rows)
            }),
        )
        .await;
        match &recounted {
            Ok(Some(rows)) => {
                span.record("coral.search.rows_written", safe::number(*rows));
            }
            Ok(None) => {
                span.record("coral.search.skipped", safe::kind("statistics_current"));
            }
            Err(error) => {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    coral.workspace.id = safe::id(&workspace_id),
                    error.type = safe::kind(error.code()),
                    "failed to recount a Workspace's search statistics"
                );
            }
        }
        recounted
    }
}
