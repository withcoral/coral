//! [`SearchIndex`]: the object the committing workflows notify after their
//! commits, so Universal Search writes its own index in the background, and
//! the reconcile task that converges what a notification missed.

use std::future::Future;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use coral_catalog::ProviderGenerationId;
use coral_db::{Database, Transaction};
use coral_functions::SavedFunctionEntry;
use coral_provider::{GenerationEntries, ProviderId};
use coral_telemetry::{metric, safe};
use coral_workspace::WorkspaceId;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tracing::Instrument as _;
use tracing::field::Empty;
use tracing::instrument::WithSubscriber as _;

use crate::indexer::functions::{self, SavedFunctionIndexer};
use crate::indexer::tools::{self, AttemptedGeneration, ToolIndexer, ToolRemoval};
use crate::indexer::{self, EntityIndexer, IndexError};

mod reconcile;
mod telemetry;

/// How long one index write may run before it is rolled back and logged as
/// failed. The limit frees the mutex: a write stalled on a database lock would
/// otherwise hold back every later write in the process. An entry's write
/// starts its limit once it holds its Workspace's lock.
const WRITE_LIMIT: Duration = Duration::from_mins(2);

/// How long an entry's write may wait to begin and to take its Workspace's
/// lock, behind another writer, before it is rolled back and logged as failed.
const WRITER_WAIT_LIMIT: Duration = Duration::from_mins(1);

/// Universal Search's write side.
///
/// Every notify method spawns the write and returns at once. It never fails
/// the caller: a failed write is logged, a failed Generation or Saved Function
/// attempt leaves a `failed` state row, and the reconcile task converges what a notification
/// missed. Each notified write is the root of a trace of its own, linked to
/// the span that notified it, such as the request that saved a Saved
/// Function, and a failed write marks its span. Notified writes run in the order of the notifications, one
/// in-process mutex serializes every write, a reconcile pass's one at a time,
/// and a write that runs past its limit is rolled back and counts as failed.
/// Cloning is cheap; every clone shares the database handle, the order, the
/// mutex, and the reconcile interval.
#[derive(Clone)]
pub struct SearchIndex {
    database: Arc<Database>,
    writes: Arc<Mutex<()>>,
    write_limit: Duration,
    writer_wait_limit: Duration,
    reconcile_interval: Duration,
    /// The last notified write; the next one waits for it.
    last_write: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    in_flight: Arc<watch::Sender<usize>>,
    telemetry: Arc<telemetry::IndexTelemetry>,
    #[cfg(test)]
    row_build_pause: Option<Arc<RowBuildPause>>,
    /// A test-only index whose notifications write nothing.
    #[cfg(any(test, feature = "test-fixtures"))]
    inert: bool,
    /// A test-only pause in each write once it holds its Workspace's lock.
    #[cfg(any(test, feature = "test-fixtures"))]
    stall_after_lock: Option<Duration>,
    /// A test-only pause in each write after it builds its rows, before it
    /// begins its transaction.
    #[cfg(any(test, feature = "test-fixtures"))]
    stall_before_write: Option<Duration>,
}

// A timing-only hook: exercise synchronous row work without building an
// artificial large catalog or tying the test to its machine's throughput.
#[cfg(test)]
struct RowBuildPause {
    entered: Arc<tokio::sync::Notify>,
    release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}
#[cfg(test)]
pub(crate) struct RowBuildHold {
    entered: Arc<tokio::sync::Notify>,
    release: std::sync::mpsc::Sender<()>,
}
#[cfg(test)]
impl RowBuildHold {
    pub(crate) async fn entered(&self) {
        self.entered.notified().await;
    }
}
#[cfg(test)]
impl Drop for RowBuildHold {
    fn drop(&mut self) {
        let _sent = self.release.send(());
    }
}

impl SearchIndex {
    #[cfg(test)]
    pub(crate) fn hold_row_building(&mut self) -> RowBuildHold {
        let entered = Arc::new(tokio::sync::Notify::new());
        let (release, receiver) = std::sync::mpsc::channel();
        self.row_build_pause = Some(Arc::new(RowBuildPause {
            entered: entered.clone(),
            release: std::sync::Mutex::new(Some(receiver)),
        }));
        RowBuildHold { entered, release }
    }

    /// Builds the write side over migrated domain persistence. The reconcile task
    /// runs a pass every `reconcile_interval`, and a pass retries a failed
    /// Generation or Saved Function by the retry module's rule, whose waits
    /// start at that interval.
    #[must_use]
    pub fn new(database: Arc<Database>, reconcile_interval: Duration) -> Self {
        Self {
            database,
            writes: Arc::new(Mutex::new(())),
            write_limit: WRITE_LIMIT,
            writer_wait_limit: WRITER_WAIT_LIMIT,
            reconcile_interval,
            last_write: Arc::new(std::sync::Mutex::new(None)),
            in_flight: Arc::new(watch::Sender::new(0)),
            telemetry: Arc::new(telemetry::IndexTelemetry::default()),
            #[cfg(test)]
            row_build_pause: None,
            #[cfg(any(test, feature = "test-fixtures"))]
            inert: false,
            #[cfg(any(test, feature = "test-fixtures"))]
            stall_after_lock: None,
            #[cfg(any(test, feature = "test-fixtures"))]
            stall_before_write: None,
        }
    }

    /// Builds the no-op stand-in for tests that need no index: every
    /// notification returns without a write, and [`Self::settled`] returns at
    /// once. The `test-fixtures` feature gates it, so production always has the
    /// real index.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[must_use]
    pub fn inert(database: Arc<Database>) -> Self {
        Self {
            inert: true,
            // No pass runs on an inert index; the interval is never read.
            ..Self::new(database, Duration::from_mins(5))
        }
    }

    /// A Generation became its Provider's active Generation: prepares its
    /// entries from what the catalog stores, then writes the Generation's rows
    /// and its `complete` state row in one transaction. The committing
    /// workflow sends ids alone and never waits for the preparation (ADR
    /// 0017). A failed preparation or write leaves a `failed` state row, and
    /// the reconcile task retries the Generation when its retry rule is due.
    ///
    /// A Generation that another publication replaced before the write runs
    /// is skipped: the replacing publication's notification indexes its own.
    /// So is one a reconcile pass already indexed.
    pub fn index_generation(
        &self,
        workspace_id: WorkspaceId,
        provider_id: ProviderId,
        generation_id: ProviderGenerationId,
    ) {
        self.index_stored_generation(workspace_id, provider_id, Some(generation_id));
    }

    /// A Provider was renamed: prepares its active Generation's entries from
    /// what the catalog stores and rewrites that Generation's rows under the
    /// new name. The rename's response does not wait for the rebuild. A
    /// failed rebuild is logged, and the reconcile task re-indexes a Provider
    /// whose name differs from the indexed one.
    pub fn index_active_generation(&self, workspace_id: WorkspaceId, provider_id: ProviderId) {
        self.index_stored_generation(workspace_id, provider_id, None);
    }

    /// Prepares the Provider's active Generation from what the catalog stores
    /// and writes it, when it is still `expected`, if one is named.
    fn index_stored_generation(
        &self,
        workspace_id: WorkspaceId,
        provider_id: ProviderId,
        expected: Option<ProviderGenerationId>,
    ) {
        let span = generation_span(&workspace_id, &provider_id, expected.as_ref());
        let index = self.clone();
        self.spawn(span, async move {
            let _write = index.writes.lock().await;
            if index
                .holds_active_generation(&workspace_id, &provider_id, expected.as_ref())
                .await
            {
                skipped("already_indexed");
                return Ok(());
            }
            match index
                .prepare_stored_generation(&workspace_id, &provider_id)
                .await
            {
                Ok(Some(entries))
                    if expected
                        .as_ref()
                        .is_none_or(|expected| expected == entries.generation_id()) =>
                {
                    index.write_generation(&entries).await
                }
                // A later publication replaced the Generation; its own
                // notification follows.
                Ok(Some(_)) => {
                    skipped("generation_replaced");
                    Ok(())
                }
                // The Provider was deleted; its deletion's removal follows.
                Ok(None) => {
                    skipped("provider_deleted");
                    Ok(())
                }
                Err(error) => {
                    index
                        .fail_preparation(&workspace_id, &provider_id, expected.as_ref(), &error)
                        .await;
                    Err(error)
                }
            }
        });
    }

    /// Whether the index already holds the Provider's active Generation, when
    /// it is still `expected`, if one is named. Under load a pass reaches a
    /// Generation before its notification does, and rewriting it would spend
    /// the preparation and a turn at the writer on nothing. A failed read
    /// answers `false`: the write that follows reports the failure.
    async fn holds_active_generation(
        &self,
        workspace_id: &WorkspaceId,
        provider_id: &ProviderId,
        expected: Option<&ProviderGenerationId>,
    ) -> bool {
        let held = self
            .within_limit(async {
                let mut transaction = self.database.begin_read_snapshot().await?;
                let active = coral_provider::workspace_active_generations_in_transaction(
                    &mut transaction,
                    workspace_id,
                    Some(provider_id),
                )
                .await?;
                let Some(active) = active.first() else {
                    return Ok(false);
                };
                if expected.is_some_and(|expected| expected != active.generation_id()) {
                    return Ok(false);
                }
                let held = ToolIndexer.holds_current(&mut transaction, active).await?;
                transaction.rollback().await?;
                Ok(held)
            })
            .await;
        matches!(held, Ok(true))
    }

    /// Logs a failed preparation and records a `failed` state row for the
    /// Provider's active Generation, as a reconcile pass does, so the
    /// Provider does not read as pending until the next pass. Records
    /// nothing when the Provider is gone or `expected` was replaced.
    async fn fail_preparation(
        &self,
        workspace_id: &WorkspaceId,
        provider_id: &ProviderId,
        expected: Option<&ProviderGenerationId>,
        error: &IndexError,
    ) {
        match self.active_generation(workspace_id, provider_id).await {
            Ok(Some(active))
                if expected.is_none_or(|expected| expected == active.generation_id()) =>
            {
                self.fail_generation((&active).into(), error).await;
            }
            Ok(_) => {}
            Err(_) => {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    coral.workspace.id = safe::id(&workspace_id),
                    coral.provider.id = safe::id(&provider_id),
                    coral.search.generation.id = expected.map(safe::id),
                    error.type = safe::kind(error.code()),
                    "failed to prepare a Provider Generation for the search index"
                );
            }
        }
    }

    /// The Provider's active Generation, or `None` when the Provider is gone.
    async fn active_generation(
        &self,
        workspace_id: &WorkspaceId,
        provider_id: &ProviderId,
    ) -> Result<Option<coral_provider::ActiveGeneration>, IndexError> {
        self.within_limit(async {
            let mut transaction = self.database.begin_read_snapshot().await?;
            let active = coral_provider::workspace_active_generations_in_transaction(
                &mut transaction,
                workspace_id,
                Some(provider_id),
            )
            .await?;
            transaction.rollback().await?;
            Ok(active.into_iter().next())
        })
        .await
    }

    /// A Generation stopped being active: removes its rows and state row.
    /// Best effort; the reconcile task removes what this misses.
    pub fn remove_generation(&self, generation_id: ProviderGenerationId) {
        let span = tracing::info_span!(
            parent: None,
            "coral.search.remove_generation",
            coral.search.generation.id = safe::id(&generation_id),
            coral.search.write.trigger = Empty,
            error.type = Empty,
        );
        let index = self.clone();
        self.spawn(span, async move {
            let _write = index.writes.lock().await;
            let removal = ToolRemoval::Generation(generation_id.clone());
            let removed = index.erase(&ToolIndexer, &removal).await;
            if let Err(error) = &removed {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    coral.search.generation.id = safe::id(&generation_id),
                    error.type = safe::kind(error.code()),
                    "failed to remove a Provider Generation from the search index"
                );
            }
            removed
        });
    }

    /// A Provider was deleted: removes the rows and state rows of all its
    /// Generations.
    pub fn remove_provider(&self, workspace_id: WorkspaceId, provider_id: ProviderId) {
        let span = tracing::info_span!(
            parent: None,
            "coral.search.remove_provider",
            coral.workspace.id = safe::id(&workspace_id),
            coral.provider.id = safe::id(&provider_id),
            coral.search.write.trigger = Empty,
            error.type = Empty,
        );
        let index = self.clone();
        self.spawn(span, async move {
            let _write = index.writes.lock().await;
            let removal = ToolRemoval::Provider {
                workspace_id: workspace_id.clone(),
                provider_id: provider_id.clone(),
            };
            let removed = index.erase(&ToolIndexer, &removal).await;
            if let Err(error) = &removed {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    coral.workspace.id = safe::id(&workspace_id),
                    coral.provider.id = safe::id(&provider_id),
                    error.type = safe::kind(error.code()),
                    "failed to remove a Provider from the search index"
                );
            }
            removed
        });
    }

    /// A Saved Function was saved or saved over: upserts its row and removes
    /// the `failed` state row an earlier attempt left. A failed attempt rolls
    /// back, removes the function's row, and leaves a `failed` state row.
    /// Removal needs no notification: the row cascades with the function.
    pub fn index_function(&self, entry: SavedFunctionEntry) {
        let span = function_span(&entry);
        let index = self.clone();
        self.spawn(span, async move {
            let _write = index.writes.lock().await;
            index.write_function(&entry).await
        });
    }

    /// Waits until notified writes and committed visibility observations finish.
    ///
    /// A caller that must observe the index after a notification, a test
    /// above all, awaits this first.
    pub async fn settled(&self) {
        let mut in_flight = self.in_flight.subscribe();
        // This index holds the sender, so the channel stays open while this
        // waits and the wait cannot fail.
        drop(in_flight.wait_for(|writes| *writes == 0).await);
    }

    /// Runs one write in the background after the previously notified one,
    /// counted until it finishes. The write logs to the caller's subscriber
    /// and runs in `span`, the root of a trace of its own that links to the
    /// caller's span. A failed write marks `span` failed; it has already
    /// logged and recorded its error.
    ///
    /// Tasks reach the mutex in no fixed order, so each write first awaits its
    /// predecessor: a rename's rewrite and a later removal of the same
    /// Generation then land in the order they were notified.
    fn spawn(
        &self,
        span: tracing::Span,
        write: impl Future<Output = Result<(), IndexError>> + Send + 'static,
    ) {
        #[cfg(any(test, feature = "test-fixtures"))]
        if self.inert {
            return;
        }
        // The write outlives the notifier's span, so it runs in a root span
        // of its own and links back.
        coral_telemetry::link_to_current(&span);
        self.in_flight.send_modify(|writes| *writes += 1);
        let in_flight = InFlightWork(self.in_flight.clone());
        // No code panics while holding this lock, so a poisoned one still
        // holds a valid handle.
        let mut last_write = self
            .last_write
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let previous = last_write.take();
        *last_write = Some(tokio::spawn(
            async move {
                let _in_flight = in_flight;
                if let Some(previous) = previous {
                    // A predecessor that panicked has finished all the same.
                    drop(previous.await);
                }
                drop(in_write_span(span, WriteTrigger::Notified, write).await);
            }
            .with_current_subscriber(),
        ));
    }

    /// Replaces both default limits of each index write, the two-minute
    /// write and the one-minute wait for the writer, with `limit`. A test can
    /// shorten them rather than wait out the real ones; a search-quality eval
    /// can lengthen them to index a Provider too large for the default.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[must_use]
    pub const fn with_write_limit(mut self, limit: Duration) -> Self {
        self.write_limit = limit;
        self.writer_wait_limit = limit;
        self
    }

    /// Pauses each later write for `stall` once it holds its Workspace's
    /// lock, so a test can overrun the write limit after the lock.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[must_use]
    pub const fn with_stall_after_lock(mut self, stall: Duration) -> Self {
        self.stall_after_lock = Some(stall);
        self
    }

    /// Pauses each later write for `stall` once it has built its rows,
    /// before it begins its transaction, so a test can stand in for a slow
    /// row build.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[must_use]
    pub const fn with_stall_before_write(mut self, stall: Duration) -> Self {
        self.stall_before_write = Some(stall);
        self
    }

    /// Writes one Generation's entries, then logs each search warning their
    /// preparation left. A failure is logged and recorded, then returned.
    /// The caller holds the mutex. Records the Generation on its write span,
    /// which named none, or an older one, before the entries were prepared.
    pub(crate) async fn write_generation(
        &self,
        entries: &GenerationEntries,
    ) -> Result<(), IndexError> {
        tracing::Span::current().record(
            "coral.search.generation.id",
            safe::id(entries.generation_id()),
        );
        let rows = match self.write(&ToolIndexer, entries).await {
            Ok(rows) => rows,
            Err(error) => {
                self.fail_generation(entries.into(), &error).await;
                return Err(error);
            }
        };
        for (_, warning) in rows.warnings() {
            tracing::warn!(
                coral.workspace.id = safe::id(&entries.workspace_id()),
                coral.provider.id = safe::id(&entries.provider_id()),
                coral.search.generation.id = safe::id(&entries.generation_id()),
                coral.search.warning.code = safe::id(warning.diagnostic_code()),
                "a Tool was indexed with a catalog warning"
            );
        }
        Ok(())
    }

    /// Writes one Saved Function's entry. A failure is logged and recorded,
    /// then returned. The caller holds the mutex.
    async fn write_function(&self, entry: &SavedFunctionEntry) -> Result<(), IndexError> {
        let Err(error) = self.write(&SavedFunctionIndexer, entry).await else {
            return Ok(());
        };
        // TODO: an error monitor for failed index writes is still to be set up.
        tracing::error!(
            coral.workspace.id = safe::id(&entry.workspace_id()),
            coral.saved_function.id = safe::id(&entry.id()),
            error.type = safe::kind(error.code()),
            "failed to index a Saved Function"
        );
        if let Err(record_error) = self.record_failed_function(entry, &error).await {
            tracing::error!(
                coral.workspace.id = safe::id(&entry.workspace_id()),
                coral.saved_function.id = safe::id(&entry.id()),
                error.type = safe::kind(record_error.code()),
                "failed to record a failed Saved Function index attempt"
            );
        }
        Err(error)
    }

    /// Logs a failed Generation attempt and records its `failed` state row.
    async fn fail_generation(&self, generation: AttemptedGeneration<'_>, error: &IndexError) {
        // TODO: an error monitor for failed index writes is still to be set up.
        tracing::error!(
            coral.workspace.id = safe::id(&generation.workspace_id()),
            coral.provider.id = safe::id(&generation.provider_id()),
            coral.search.generation.id = safe::id(&generation.generation_id()),
            error.type = safe::kind(error.code()),
            "failed to index a Provider Generation"
        );
        if let Err(record_error) = self.record_failed_generation(generation, error).await {
            tracing::error!(
                coral.workspace.id = safe::id(&generation.workspace_id()),
                coral.provider.id = safe::id(&generation.provider_id()),
                coral.search.generation.id = safe::id(&generation.generation_id()),
                error.type = safe::kind(record_error.code()),
                "failed to record a failed Provider Generation index attempt"
            );
        }
    }

    /// Runs one transaction's work within the write limit. A write that
    /// overruns is dropped, and dropping its transaction rolls it back.
    async fn within_limit<T>(
        &self,
        write: impl Future<Output = Result<T, IndexError>>,
    ) -> Result<T, IndexError> {
        tokio::time::timeout(self.write_limit, write)
            .await
            .map_err(|_elapsed| IndexError::TimedOut(self.write_limit))?
    }

    /// Writes one entry under its Workspace's lock. It builds the entry's
    /// rows before it begins: on `SQLite` the write holds the one writer of
    /// the process, which every other write waits for, so the text work must
    /// not hold it. The wait to begin and to take the lock has a limit of its
    /// own, so a write that waited long for another writer still has its
    /// whole write limit. A write that overruns
    /// its wait fails as [`IndexError::WriterBusy`], which may pass on a
    /// later try. A write that overruns after it takes the lock fails as
    /// [`IndexError::Overran`], which repeats until something changes. A
    /// write that took the lock records how long it waited and wrote on its
    /// write span. Returns the rows it wrote.
    async fn write<'e, I: EntityIndexer + 'e>(
        &self,
        indexer: &I,
        entry: &'e I::Entry,
    ) -> Result<I::Rows<'e>, IndexError> {
        let workspace_id = indexer.workspace_id(entry);
        #[cfg(test)]
        if let Some(pause) = &self.row_build_pause {
            let receiver = pause.release.lock().unwrap().take();
            if let Some(receiver) = receiver {
                pause.entered.notify_one();
                let _released = receiver.recv_timeout(Duration::from_secs(5));
            }
        }
        let rows = indexer.rows(entry)?;
        #[cfg(any(test, feature = "test-fixtures"))]
        if let Some(stall) = self.stall_before_write {
            tokio::time::sleep(stall).await;
        }
        let started = Instant::now();
        let mut transaction = tokio::time::timeout(self.writer_wait_limit, async {
            let mut transaction = self.database.begin().await?;
            if let Err(error) = indexer::lock_workspace(&mut transaction, workspace_id).await {
                rollback(transaction).await;
                return Err(error);
            }
            Ok(transaction)
        })
        .await
        .map_err(|_elapsed| IndexError::WriterBusy(self.writer_wait_limit))??;
        let locked = Instant::now();
        // Borrowed, so the rows outlive the `async move` write and return.
        let written_rows = &rows;
        let written = tokio::time::timeout(self.write_limit, async move {
            #[cfg(any(test, feature = "test-fixtures"))]
            if let Some(stall) = self.stall_after_lock {
                tokio::time::sleep(stall).await;
            }
            let effect = match indexer.index(&mut transaction, entry, written_rows).await {
                Ok(effect) => effect,
                Err(error) => {
                    rollback(transaction).await;
                    return Err(error);
                }
            };
            transaction.commit().await?;
            Ok((effect, crate::unix_now_nanos()))
        })
        .await
        .unwrap_or_else(|_elapsed| Err(IndexError::Overran(self.write_limit)));
        let span = tracing::Span::current();
        span.record(
            "coral.search.writer_wait_ms",
            safe::number(millis(locked - started)),
        );
        span.record(
            "coral.search.write_ms",
            safe::number(millis(locked.elapsed())),
        );
        let (effect, committed_at) = written?;
        self.observe_committed_visibility(workspace_id.clone(), effect, committed_at);
        Ok(rows)
    }

    /// Verification is best effort and independently scheduled. It neither
    /// holds the write mutex nor becomes a predecessor of the next write.
    fn observe_committed_visibility(
        &self,
        workspace: WorkspaceId,
        effect: indexer::IndexEffect,
        committed_at: Option<i64>,
    ) {
        if effect.catalog().is_none() || committed_at.is_none() || !metric::enabled() {
            return;
        }
        let index = self.clone();
        self.in_flight.send_modify(|work| *work += 1);
        let work = InFlightWork(self.in_flight.clone());
        let span = tracing::info_span!(parent: None, "coral.search.verify_visibility");
        coral_telemetry::link_to_current(&span);
        let observe = async move {
            let _work = work;
            let Some(target) = effect.catalog() else {
                return;
            };
            let check = async {
                let mut tx = index.database.begin_read_snapshot().await?;
                let active = coral_provider::workspace_active_generations_in_transaction(
                    &mut tx,
                    &workspace,
                    Some(&target.provider_id),
                )
                .await?;
                tx.rollback().await?;
                Ok::<_, IndexError>(
                    active
                        .iter()
                        .any(|active| active.generation_id() == &target.generation_id),
                )
            };
            match tokio::time::timeout(Duration::from_secs(1), check).await {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => return,
                Ok(Err(error)) => {
                    tracing::warn!(error.type = safe::kind(error.code()), "could not verify committed catalog visibility; sample omitted");
                    return;
                }
                Err(_elapsed) => {
                    tracing::warn!(error.type = "timed_out", "committed catalog visibility check timed out; sample omitted");
                    return;
                }
            }
            match effect {
                indexer::IndexEffect::FirstVisible {
                    published_at_unix_nanos,
                    path,
                    ..
                } => {
                    if let Some(elapsed) = elapsed_at(published_at_unix_nanos, committed_at) {
                        metric::publication_visible(path, elapsed);
                    }
                }
                indexer::IndexEffect::Recovered {
                    unavailable_since_unix_nanos,
                    ..
                } => {
                    if let Some(elapsed) = elapsed_at(unavailable_since_unix_nanos, committed_at) {
                        metric::index_recovered(elapsed);
                    }
                }
                indexer::IndexEffect::Updated => {}
            }
        };
        let _observation = tokio::spawn(observe.instrument(span).with_current_subscriber());
    }

    async fn erase<I: EntityIndexer>(
        &self,
        indexer: &I,
        removal: &I::Removal,
    ) -> Result<(), IndexError> {
        self.within_limit(async {
            let mut transaction = self.database.begin().await?;
            if let Err(error) = indexer.remove(&mut transaction, removal).await {
                rollback(transaction).await;
                return Err(error);
            }
            transaction.commit().await?;
            Ok(())
        })
        .await
    }

    /// Begins a write, waiting again while the writer is busy, in this
    /// process or another. A write that overran its limit keeps the writer
    /// until its statement ends, and the failure it leaves must still be
    /// recorded. The caller's limit bounds the wait.
    async fn begin_when_writer_free(&self) -> Result<Transaction<'_>, IndexError> {
        loop {
            match self.database.begin().await {
                Err(error) if error.is_serialization_conflict() => {}
                begun => return Ok(begun?),
            }
        }
    }

    async fn record_failed_generation(
        &self,
        generation: AttemptedGeneration<'_>,
        error: &IndexError,
    ) -> Result<(), IndexError> {
        self.within_limit(async {
            let mut transaction = self.begin_when_writer_free().await?;
            tools::record_failed_attempt(
                &mut transaction,
                generation,
                error,
                self.reconcile_interval,
            )
            .await?;
            transaction.commit().await?;
            Ok(())
        })
        .await
    }
    async fn record_failed_function(
        &self,
        entry: &SavedFunctionEntry,
        error: &IndexError,
    ) -> Result<(), IndexError> {
        self.within_limit(async {
            let mut transaction = self.begin_when_writer_free().await?;
            functions::record_failed_attempt(
                &mut transaction,
                entry,
                error,
                self.reconcile_interval,
            )
            .await?;
            transaction.commit().await?;
            Ok(())
        })
        .await
    }
}

/// The span of one Provider Generation's index write, notified or made by a
/// reconcile pass. The write records the Generation it prepared when none was
/// named, how long it waited for the writer and wrote, or why it wrote
/// nothing.
fn generation_span(
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
    generation_id: Option<&ProviderGenerationId>,
) -> tracing::Span {
    tracing::info_span!(
        parent: None,
        "coral.search.index_generation",
        coral.workspace.id = safe::id(&workspace_id),
        coral.provider.id = safe::id(&provider_id),
        coral.search.generation.id = generation_id.map(safe::id),
        coral.search.skipped = Empty,
        coral.search.writer_wait_ms = Empty,
        coral.search.write_ms = Empty,
        coral.search.write.trigger = Empty,
        error.type = Empty,
    )
}

/// The span of one Saved Function's index write, notified or made by a
/// reconcile pass. The write records how long it waited for the writer and
/// wrote.
fn function_span(entry: &SavedFunctionEntry) -> tracing::Span {
    tracing::info_span!(
        parent: None,
        "coral.search.index_function",
        coral.workspace.id = safe::id(&entry.workspace_id()),
        coral.saved_function.id = safe::id(&entry.id()),
        coral.search.writer_wait_ms = Empty,
        coral.search.write_ms = Empty,
        coral.search.write.trigger = Empty,
        error.type = Empty,
    )
}

/// Records on the current write span why the write wrote nothing.
fn skipped(reason: &'static str) {
    tracing::Span::current().record("coral.search.skipped", safe::kind(reason));
}

/// What started an index write, recorded on its span, so a trace query can
/// tell the writes a reconcile pass had to repair from those a change
/// notified.
#[derive(Debug, Clone, Copy)]
enum WriteTrigger {
    /// A committing workflow's notification.
    Notified,
    /// A reconcile pass.
    Reconcile,
}

impl WriteTrigger {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Notified => "notified",
            Self::Reconcile => "reconcile",
        }
    }
}

/// Runs one write in `span`, a root span, records its `trigger`, links it to
/// the current span, such as a reconcile pass's, and marks it with the
/// error's kind when the write fails. The write is boxed, so a pass's
/// future holds one pointer for it rather than a second copy.
fn in_write_span<T>(
    span: tracing::Span,
    trigger: WriteTrigger,
    write: impl Future<Output = Result<T, IndexError>>,
) -> impl Future<Output = Result<T, IndexError>> {
    span.record("coral.search.write.trigger", safe::kind(trigger.as_str()));
    coral_telemetry::link_to_current(&span);
    let write = Box::pin(write.instrument(span.clone()));
    async move {
        let written = write.await;
        if let Err(error) = &written {
            coral_telemetry::record_failure(&span, error.code());
        }
        written
    }
}

/// Rolls back a failed write. A failed rollback is logged on its own, so the
/// caller still reports the write's error, the cause.
async fn rollback(transaction: Transaction<'_>) {
    if let Err(error) = transaction.rollback().await {
        tracing::error!(
            error.type = safe::kind(error.code()),
            "failed to roll back a search index write"
        );
    }
}

/// A duration in whole milliseconds, for a span field.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Counts one write or visibility observation as in flight until dropped, so a write that
/// panics still stops counting.
struct InFlightWork(Arc<watch::Sender<usize>>);

impl Drop for InFlightWork {
    fn drop(&mut self) {
        self.0.send_modify(|writes| *writes -= 1);
    }
}

fn elapsed_at(since_unix_nanos: i64, committed_at: Option<i64>) -> Option<Duration> {
    let elapsed = committed_at?.checked_sub(since_unix_nanos)?;
    Some(Duration::from_nanos(u64::try_from(elapsed).ok()?))
}
