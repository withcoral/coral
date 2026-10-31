//! The reconcile task of [`SearchIndex`]: its schedule, one pass, and the
//! pass's steps, as [`SearchIndex::reconcile`] lists them.

use std::future::Future;

use coral_functions::SavedFunctionEntry;
use coral_provider::{GenerationEntries, ProviderId};
use coral_telemetry::safe;
use coral_workspace::WorkspaceId;
use tokio::task::{Id, JoinHandle, JoinSet};
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
            error.type = Empty,
        )
    };
}

/// The two lanes share shutdown, not failure. A panic in telemetry must not
/// retire the indexing worker, and a retired worker is restarted with backoff.
#[derive(Clone, Copy)]
enum BackgroundTask {
    Observer,
    Reconcile,
}
impl BackgroundTask {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Observer => "observer",
            Self::Reconcile => "reconcile",
        }
    }
}
const RESTART_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

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
        assert!(
            !self.reconcile_interval.is_zero(),
            "reconcile interval must be positive"
        );
        let index = self.clone();
        let supervise = async move {
            let mut tasks = JoinSet::new();
            let mut observer = index.spawn_background_task(&mut tasks, BackgroundTask::Observer);
            let mut reconcile = index.spawn_background_task(&mut tasks, BackgroundTask::Reconcile);
            while let Some(result) = tasks.join_next_with_id().await {
                let (id, reason) = match result {
                    Ok((id, ())) => (id, "background_task_ended"),
                    Err(error) => (
                        error.id(),
                        if error.is_panic() {
                            "background_task_panicked"
                        } else {
                            "background_task_canceled"
                        },
                    ),
                };
                let role = if id == observer {
                    BackgroundTask::Observer
                } else {
                    BackgroundTask::Reconcile
                };
                debug_assert!(id == observer || id == reconcile);
                tracing::error!(coral.search.background.task = safe::kind(role.as_str()), error.type = safe::kind(reason), "search background task stopped; restarting");
                tokio::time::sleep(RESTART_DELAY).await;
                let restarted = index.spawn_background_task(&mut tasks, role);
                match role {
                    BackgroundTask::Observer => observer = restarted,
                    BackgroundTask::Reconcile => reconcile = restarted,
                }
            }
            // Dropping the set aborts both children when the caller shuts down.
        };
        tokio::spawn(supervise.with_current_subscriber())
    }

    fn spawn_background_task(&self, tasks: &mut JoinSet<()>, role: BackgroundTask) -> Id {
        let index = self.clone();
        tasks
            .spawn(
                async move {
                    match role {
                        BackgroundTask::Observer => index.observe_backlog_loop().await,
                        BackgroundTask::Reconcile => {
                            let mut starts = tokio::time::interval(index.reconcile_interval);
                            starts.set_missed_tick_behavior(MissedTickBehavior::Delay);
                            loop {
                                starts.tick().await;
                                index.reconcile().await;
                            }
                        }
                    }
                }
                .with_current_subscriber(),
            )
            .id()
    }

    /// Runs one reconcile pass, which converges the index on what the owners
    /// hold without a notification:
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
    /// Removal runs first. The pass reads what each step must do from a read
    /// snapshot, so a pass that finds no change never takes the writer, which
    /// every other write in the process would wait behind.
    ///
    /// Each removal, Generation, function and recount is written in a
    /// transaction of its own, under the in-process mutex, which the pass
    /// holds for that one item. A pass under load waits for the writer once
    /// per item, and holding the mutex across those waits would hold back
    /// every notified write. A notified write or a change that lands between
    /// two items may index, replace or delete what the pass read as stale, so
    /// the pass reads each Generation and function again under the mutex and
    /// skips what changed. A failed removal is logged and the pass goes on
    /// without further removals; a failed write is logged and
    /// recorded like a notified write's, and the pass goes on. Passes in two
    /// processes write the same idempotent rows.
    ///
    /// The retry module's rule decides when a failure is due. A failure that
    /// repeats until something changes waits for a newer Coral version, or,
    /// for a Saved Function, for another revision, so a pass does not spend
    /// its turn at the writer on it pass after pass.
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
            error.type = Empty,
        );
        let work = self.run_pass().instrument(pass.clone()).await;
        if let Some(statistics) = work.statistics {
            statistics.record(&pass);
        }
        pass.record(
            "coral.search.reconcile.generations_indexed",
            safe::number(work.generations.indexed),
        );
        pass.record(
            "coral.search.reconcile.generations_failed",
            safe::number(work.generations.failed),
        );
        pass.record(
            "coral.search.reconcile.functions_indexed",
            safe::number(work.functions.indexed),
        );
        pass.record(
            "coral.search.reconcile.functions_failed",
            safe::number(work.functions.failed),
        );
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
                error.type = safe::kind(IndexError::Clock.code()),
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
        let removed = async {
            let superseded = self
                .within_limit(async {
                    let mut transaction = self.database.begin_read_snapshot().await?;
                    let active =
                        coral_provider::active_generations_in_transaction(&mut transaction).await?;
                    let superseded = ToolIndexer.superseded(&mut transaction, &active).await?;
                    transaction.rollback().await?;
                    Ok(superseded)
                })
                .await?;
            for removal in &superseded {
                let _write = self.writes.lock().await;
                self.erase(&ToolIndexer, removal).await?;
            }
            Ok::<_, IndexError>(())
        }
        .await;
        if let Err(error) = removed {
            // TODO: an error monitor for failed index writes is still to be set up.
            tracing::error!(
                error.type = safe::kind(error.code()),
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
                    error.type = safe::kind(error.code()),
                    "failed to reconcile the indexed Provider Generations"
                );
                return Err(());
            }
        };
        let mut work = ItemWork::default();
        for generation in &stale {
            let _write = self.writes.lock().await;
            if !self.still_stale(generation, now_unix_nanos).await {
                continue;
            }
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

    /// Whether `generation`, which the pass read as stale, still is. A failed
    /// read answers `true`, so the write goes ahead: it indexes what the
    /// catalog holds now.
    async fn still_stale(
        &self,
        generation: &coral_provider::ActiveGeneration,
        now_unix_nanos: i64,
    ) -> bool {
        let needed = self
            .within_limit(async {
                let mut transaction = self.database.begin_read_snapshot().await?;
                let needed = ToolIndexer
                    .still_needs_indexing(&mut transaction, generation, now_unix_nanos)
                    .await?;
                transaction.rollback().await?;
                Ok(needed)
            })
            .await;
        !matches!(needed, Ok(false))
    }

    /// Whether `entry`, which the pass read as stale, still is. A failed read
    /// is logged and answers `false`: `entry` may be an older copy, and the
    /// next pass tries again.
    async fn function_still_stale(&self, entry: &SavedFunctionEntry, now_unix_nanos: i64) -> bool {
        let needed = self
            .within_limit(async {
                let mut transaction = self.database.begin_read_snapshot().await?;
                let needed = SavedFunctionIndexer
                    .still_needs_indexing(&mut transaction, entry, now_unix_nanos)
                    .await?;
                transaction.rollback().await?;
                Ok(needed)
            })
            .await;
        needed.unwrap_or_else(|error: IndexError| {
            // TODO: an error monitor for failed index writes is still to be set up.
            tracing::error!(
                coral.workspace.id = safe::id(&entry.workspace_id()),
                coral.saved_function.id = safe::id(&entry.id()),
                error.type = safe::kind(error.code()),
                "failed to read a Saved Function again before indexing it"
            );
            false
        })
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
                    let _write = self.writes.lock().await;
                    if !self.function_still_stale(entry, now_unix_nanos).await {
                        continue;
                    }
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
                    error.type = safe::kind(error.code()),
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
                    error.type = safe::kind(error.code()),
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
            let _write = self.writes.lock().await;
            let span = tracing::info_span!(
                parent: None,
                "coral.search.refresh_statistics",
                coral.workspace.id = safe::id(&workspace_id),
                coral.search.rows_written = Empty,
                coral.search.write.trigger = Empty,
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
                    span.record("coral.search.rows_written", safe::number(rows));
                    work.workspaces_recounted += 1;
                    work.rows_written += rows;
                }
                Err(error) => {
                    // TODO: an error monitor for failed index writes is still to be set up.
                    tracing::error!(
                        coral.workspace.id = safe::id(&workspace_id),
                        error.type = safe::kind(error.code()),
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
            safe::number(self.workspaces_checked),
        );
        span.record(
            "coral.search.reconcile.workspaces_recounted",
            safe::number(self.workspaces_recounted),
        );
        span.record(
            "coral.search.reconcile.rows_written",
            safe::number(self.rows_written),
        );
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
