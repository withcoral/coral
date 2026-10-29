//! The reconcile task of [`SearchIndex`]: its schedule, one pass, and the
//! pass's steps, as [`SearchIndex::reconcile`] lists them.

use std::future::Future;

use coral_provider::{GenerationEntries, ProviderId};
use coral_workspace::WorkspaceId;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::Instrument as _;
use tracing::field::Empty;
use tracing::instrument::WithSubscriber as _;

use super::{SearchIndex, WriteTrigger, function_span, generation_span, in_write_span, skipped};
use crate::indexer::functions::SavedFunctionIndexer;
use crate::indexer::tools::ToolIndexer;
use crate::indexer::{self, EntityIndexer, IndexError};

/// A debug-level span for one step of a pass, ready for [`step`] to mark it
/// failed.
macro_rules! step_span {
    ($name:literal $(, $($field:tt)*)?) => {
        tracing::debug_span!(
            $name,
            $($($field)*)?
            coral.status = Empty,
            error.type = Empty,
        )
    };
}

impl SearchIndex {
    /// Spawns the reconcile task: one pass at once, then one pass at the start
    /// of each interval. A pass that runs past the next start is followed by
    /// the next pass at once. The task runs until the caller aborts it; an
    /// abort during a pass rolls back the pass's open transaction, and later
    /// passes finish the work.
    ///
    /// # Panics
    ///
    /// Panics when the reconcile interval is zero.
    #[must_use = "the caller aborts the reconcile task at shutdown"]
    pub fn spawn_reconcile_task(&self) -> JoinHandle<()> {
        let index = self.clone();
        let mut starts = tokio::time::interval(self.reconcile_interval);
        starts.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::spawn(
            async move {
                loop {
                    starts.tick().await;
                    index.reconcile().await;
                }
            }
            .with_current_subscriber(),
        )
    }

    /// Runs one reconcile pass under the in-process mutex, so it converges
    /// the index on what the owners hold without a notification:
    ///
    /// 1. Indexes every Provider whose active Generation has no `complete`
    ///    state row, one under another Provider display name, or a `failed`
    ///    one whose retry is due, from the stored Generation.
    /// 2. Indexes every Saved Function whose `failed` state row's retry is
    ///    due, and every one with no `failed` state row whose row is missing
    ///    or whose fingerprint differs from its definition revision. A
    ///    failure of the current revision not yet due waits, whatever the
    ///    row holds.
    /// 3. Removes the rows and state rows of every Generation that is no
    ///    Provider's active Generation. Best effort. Saved Function rows never
    ///    outlive their function.
    /// 4. On Postgres, recounts the statistics of every Workspace whose rows
    ///    changed after its last count, not only those this pass wrote: a
    ///    Saved Function delete cascades to its row without an index write,
    ///    and step 3 removes rows without a count.
    ///
    /// Removal runs first, in a transaction of its own that reads the active
    /// Generations again; a failed removal is logged and the pass goes on.
    /// Each Generation and each function is written in a transaction of its
    /// own; a failure is logged and recorded like a notified write's, and the
    /// pass goes on. Passes in two processes write the same idempotent rows.
    ///
    /// The retry module's rule decides when a failure is due. A failure that
    /// repeats until something changes waits for a newer Coral version, or,
    /// for a Saved Function, for another revision, so it does not hold the
    /// mutex pass after pass.
    ///
    /// The pass runs in a root span, `coral.search.reconcile`, and each step
    /// in a child span, all at debug level: almost every pass finds nothing
    /// to do, so the default trace filter exports none of them. Each write
    /// the pass makes runs in an info root span of its own, which the
    /// default filter exports, linked to its step's span when that is traced:
    /// `coral.search.index_generation` and `coral.search.index_function`, as
    /// a notified write's, and `coral.search.refresh_statistics`.
    /// A failed write or step marks its span and the pass's span as failed.
    /// The pass's span records what it indexed and what failed, and the
    /// statistics step's span the Workspaces it checked and recounted and
    /// the statistics rows it wrote. A pass that finds no change writes no
    /// row.
    pub async fn reconcile(&self) {
        let _write = self.writes.lock().await;
        let pass = tracing::debug_span!(
            parent: None,
            "coral.search.reconcile",
            coral.search.reconcile.generations_indexed = Empty,
            coral.search.reconcile.generations_failed = Empty,
            coral.search.reconcile.functions_indexed = Empty,
            coral.search.reconcile.functions_failed = Empty,
            coral.search.reconcile.workspaces_checked = Empty,
            coral.search.reconcile.workspaces_recounted = Empty,
            coral.search.reconcile.rows_written = Empty,
            coral.status = Empty,
            error.type = Empty,
        );
        let work = self.run_pass().instrument(pass.clone()).await;
        if let Some(statistics) = work.statistics {
            statistics.record(&pass);
        }
        work.generations.record(&pass, "generations");
        work.functions.record(&pass, "functions");
        if work.failed() {
            coral_telemetry::record_failure(&pass, "reconcile_failed");
        }
    }

    /// Runs the steps of one pass, each in its own span. Its statistics are
    /// `None` when the pass cannot start or cannot read the statistics
    /// watermarks: no span then records counts the pass did not take.
    async fn run_pass(&self) -> PassWork {
        let mut work = PassWork::default();
        let Some(now) = crate::unix_now_nanos() else {
            // TODO: an error monitor for failed index writes is still to be set up.
            tracing::error!(
                error.type = IndexError::Clock.code(),
                "failed to start a search index reconcile pass"
            );
            work.step_failed = true;
            return work;
        };
        work.step_failed |= step(
            step_span!("coral.search.reconcile.remove_superseded"),
            self.remove_superseded_generations(),
        )
        .await
        .is_err();
        match step(
            step_span!("coral.search.reconcile.generations"),
            self.reconcile_generations(now),
        )
        .await
        {
            Ok(generations) => work.generations = generations,
            Err(()) => work.step_failed = true,
        }
        match step(
            step_span!("coral.search.reconcile.functions"),
            self.reconcile_functions(now),
        )
        .await
        {
            Ok(functions) => work.functions = functions,
            Err(()) => work.step_failed = true,
        }
        let span = step_span!(
            "coral.search.reconcile.statistics",
            coral.search.reconcile.workspaces_checked = Empty,
            coral.search.reconcile.workspaces_recounted = Empty,
            coral.search.reconcile.rows_written = Empty,
        );
        match step(span.clone(), self.refresh_stale_workspaces()).await {
            Ok(statistics) => {
                statistics.record(&span);
                work.statistics = Some(statistics);
            }
            Err(()) => work.step_failed = true,
        }
        work
    }

    /// Step 3 of a pass, run first: removes the rows and state rows of every
    /// Generation that is no Provider's active Generation. Best effort.
    async fn remove_superseded_generations(&self) -> Result<(), ()> {
        let removed = self
            .within_limit(async {
                let mut transaction = self.database.begin().await?;
                let active =
                    coral_provider::active_generations_in_transaction(&mut transaction).await?;
                ToolIndexer
                    .remove_superseded(&mut transaction, &active)
                    .await?;
                transaction.commit().await?;
                Ok(())
            })
            .await;
        if let Err(error) = removed {
            // TODO: an error monitor for failed index writes is still to be set up.
            tracing::error!(
                error.type = error.code(),
                "failed to remove superseded Provider Generations from the search index"
            );
            return Err(());
        }
        Ok(())
    }

    /// Step 1 of a pass: indexes each active Generation the index lacks,
    /// holds under an old name, or holds as failed with a retry due at
    /// `now_unix_nanos`.
    async fn reconcile_generations(&self, now_unix_nanos: i64) -> Result<ItemWork, ()> {
        let stale = self
            .within_limit(async {
                let mut transaction = self.database.begin_read_snapshot().await?;
                let expected =
                    coral_provider::active_generations_in_transaction(&mut transaction).await?;
                let stale = ToolIndexer
                    .reconcile(&mut transaction, &expected, now_unix_nanos)
                    .await?
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                transaction.rollback().await?;
                Ok(stale)
            })
            .await;
        let stale = match stale {
            Ok(stale) => stale,
            Err(error) => {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    error.type = error.code(),
                    "failed to reconcile the indexed Provider Generations"
                );
                return Err(());
            }
        };
        let mut work = ItemWork::default();
        for generation in &stale {
            let span = generation_span(
                generation.workspace_id(),
                generation.provider_id(),
                Some(generation.generation_id()),
            );
            let indexed = in_write_span(span, WriteTrigger::Reconcile, async {
                match self
                    .prepare_stored_generation(generation.workspace_id(), generation.provider_id())
                    .await
                {
                    Ok(Some(entries)) => self.write_generation(&entries).await.map(|()| true),
                    // The Provider was deleted after the pass read it; its rows
                    // cascaded with its Generations.
                    Ok(None) => {
                        skipped("provider_deleted");
                        Ok(false)
                    }
                    Err(error) => {
                        self.fail_generation(generation.into(), &error).await;
                        Err(error)
                    }
                }
            })
            .await;
            work.count(&indexed);
        }
        Ok(work)
    }

    /// Prepares the entries of the Provider's current active Generation from
    /// what the catalog stores. `None` when the Provider is gone.
    pub(super) async fn prepare_stored_generation(
        &self,
        workspace_id: &WorkspaceId,
        provider_id: &ProviderId,
    ) -> Result<Option<GenerationEntries>, IndexError> {
        self.within_limit(async {
            let mut transaction = self.database.begin_read_snapshot().await?;
            let entries = coral_provider::prepare_active_generation_entries_in_transaction(
                &mut transaction,
                workspace_id,
                provider_id,
            )
            .await?;
            transaction.rollback().await?;
            Ok(entries)
        })
        .await
    }

    /// Step 2 of a pass: indexes each current Saved Function whose failure
    /// has a retry due at `now_unix_nanos` or another revision, and each
    /// without a failure that the index lacks or holds at another revision.
    /// A failure of the current revision not yet due waits.
    async fn reconcile_functions(&self, now_unix_nanos: i64) -> Result<ItemWork, ()> {
        let stale = self
            .within_limit(async {
                let mut transaction = self.database.begin_read_snapshot().await?;
                let expected =
                    coral_functions::all_saved_function_entries_in_transaction(&mut transaction)
                        .await?;
                let stale = SavedFunctionIndexer
                    .reconcile(&mut transaction, &expected, now_unix_nanos)
                    .await?
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                transaction.rollback().await?;
                Ok(stale)
            })
            .await;
        match stale {
            Ok(stale) => {
                let mut work = ItemWork::default();
                for entry in &stale {
                    let indexed = in_write_span(
                        function_span(entry),
                        WriteTrigger::Reconcile,
                        self.write_function(entry),
                    )
                    .await
                    .map(|()| true);
                    work.count(&indexed);
                }
                Ok(work)
            }
            Err(error) => {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    error.type = error.code(),
                    "failed to reconcile the indexed Saved Functions"
                );
                Err(())
            }
        }
    }

    /// Step 4 of a pass: on Postgres, recounts the statistics of each
    /// Workspace whose watermark no longer matches its rows, one Workspace per
    /// transaction under its lock. Fails when the watermarks cannot be read.
    async fn refresh_stale_workspaces(&self) -> Result<StatisticsWork, ()> {
        let check = self
            .within_limit(async {
                let mut transaction = self.database.begin_read_snapshot().await?;
                let check = indexer::check_statistics(&mut transaction).await?;
                transaction.rollback().await?;
                Ok(check)
            })
            .await;
        let check = match check {
            Ok(check) => check,
            Err(error) => {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    error.type = error.code(),
                    "failed to check the Workspaces' search statistics"
                );
                return Err(());
            }
        };
        let mut work = StatisticsWork {
            workspaces_checked: u64::try_from(check.workspaces_checked).unwrap_or(u64::MAX),
            ..StatisticsWork::default()
        };
        for workspace_id in &check.stale {
            let span = tracing::info_span!(
                parent: None,
                "coral.search.refresh_statistics",
                coral.workspace.id = %workspace_id,
                coral.search.rows_written = Empty,
                coral.search.write.trigger = Empty,
                coral.status = Empty,
                error.type = Empty,
            );
            let refreshed = in_write_span(
                span.clone(),
                WriteTrigger::Reconcile,
                self.within_limit(async {
                    let mut transaction = self.database.begin().await?;
                    let rows = indexer::refresh_statistics(&mut transaction, workspace_id).await?;
                    transaction.commit().await?;
                    Ok(rows)
                }),
            )
            .await;
            match refreshed {
                Ok(rows) => {
                    span.record("coral.search.rows_written", rows);
                    work.workspaces_recounted += 1;
                    work.rows_written += rows;
                }
                Err(error) => {
                    // TODO: an error monitor for failed index writes is still to be set up.
                    tracing::error!(
                        coral.workspace.id = %workspace_id,
                        error.type = error.code(),
                        "failed to refresh a Workspace's search statistics"
                    );
                    work.failed = true;
                }
            }
        }
        Ok(work)
    }
}

/// What the statistics step of a pass did.
#[derive(Debug, Default, Clone, Copy)]
struct StatisticsWork {
    /// The Workspaces whose watermark the step compared.
    workspaces_checked: u64,
    /// The Workspaces whose statistics the step counted again.
    workspaces_recounted: u64,
    /// The `search_terms` and `search_stats` rows the step wrote or deleted.
    rows_written: u64,
    /// Whether a Workspace's recount failed.
    failed: bool,
}

impl StatisticsWork {
    fn record(self, span: &tracing::Span) {
        span.record(
            "coral.search.reconcile.workspaces_checked",
            self.workspaces_checked,
        );
        span.record(
            "coral.search.reconcile.workspaces_recounted",
            self.workspaces_recounted,
        );
        span.record("coral.search.reconcile.rows_written", self.rows_written);
    }
}

/// The items one step of a pass tried to index: Generations or Saved
/// Functions it found unindexed or due for a retry.
#[derive(Debug, Default, Clone, Copy)]
struct ItemWork {
    /// The items it indexed.
    indexed: u64,
    /// The items whose indexing failed and was recorded for a retry.
    failed: u64,
}

impl ItemWork {
    /// Records the counts on the pass's span under `items`, `generations` or
    /// `functions`.
    fn record(self, pass: &tracing::Span, items: &str) {
        pass.record(
            format!("coral.search.reconcile.{items}_indexed").as_str(),
            self.indexed,
        );
        pass.record(
            format!("coral.search.reconcile.{items}_failed").as_str(),
            self.failed,
        );
    }

    /// Counts one item's attempt, `Ok(false)` when the item was gone.
    fn count(&mut self, attempt: &Result<bool, IndexError>) {
        match attempt {
            Ok(true) => self.indexed += 1,
            Ok(false) => {}
            Err(_) => self.failed += 1,
        }
    }
}

/// What one pass did.
#[derive(Debug, Default, Clone, Copy)]
struct PassWork {
    generations: ItemWork,
    functions: ItemWork,
    /// The statistics step's work, when it could read the watermarks.
    statistics: Option<StatisticsWork>,
    /// Whether a step failed before it reached its items.
    step_failed: bool,
}

impl PassWork {
    /// Whether any step or any item failed.
    fn failed(&self) -> bool {
        self.step_failed
            || self.generations.failed > 0
            || self.functions.failed > 0
            || self.statistics.is_some_and(|statistics| statistics.failed)
    }
}

/// Runs one step of a pass in `span`, and marks the span failed when the
/// step fails.
async fn step<T>(span: tracing::Span, work: impl Future<Output = Result<T, ()>>) -> Result<T, ()> {
    let output = work.instrument(span.clone()).await;
    if output.is_err() {
        coral_telemetry::record_failure(&span, "reconcile_failed");
    }
    output
}
