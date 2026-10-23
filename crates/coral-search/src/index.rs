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
use coral_workspace::WorkspaceId;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tracing::instrument::WithSubscriber as _;

use crate::indexer::functions::{self, SavedFunctionIndexer};
use crate::indexer::operations::{self, AttemptedGeneration, OperationIndexer, OperationRemoval};
use crate::indexer::{self, EntityIndexer, IndexError};

mod reconcile;

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
/// missed. Notified writes run in the order of the notifications, one
/// in-process mutex serializes every write and reconcile pass, and a write
/// that runs past its limit is rolled back and counts as failed. Cloning is
/// cheap; every clone shares the database handle, the order, the mutex, and
/// the reconcile interval.
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

impl SearchIndex {
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
    /// 0017). Writing an indexed Generation again rewrites its rows. A failed
    /// preparation or write leaves a `failed` state row, and the reconcile
    /// task retries the Generation when its retry rule is due.
    ///
    /// A Generation that another publication replaced before the write runs
    /// is skipped: the replacing publication's notification indexes its own.
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
        let index = self.clone();
        self.spawn(async move {
            let _write = index.writes.lock().await;
            match index
                .prepare_stored_generation(&workspace_id, &provider_id)
                .await
            {
                Ok(Some(entries))
                    if expected
                        .as_ref()
                        .is_none_or(|expected| expected == entries.generation_id()) =>
                {
                    index.write_generation(&entries).await;
                }
                // The Provider was deleted, or a later publication replaced
                // the Generation; that change's own notification follows.
                Ok(_) => {}
                Err(error) => {
                    index
                        .fail_preparation(&workspace_id, &provider_id, expected.as_ref(), &error)
                        .await;
                }
            }
        });
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
            Err(read_error) => {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    workspace_id = %workspace_id,
                    provider_id = %provider_id,
                    generation_id = expected.map(tracing::field::display),
                    error = %error,
                    read_error = %read_error,
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
        let index = self.clone();
        self.spawn(async move {
            let _write = index.writes.lock().await;
            let removal = OperationRemoval::Generation(generation_id.clone());
            if let Err(error) = index.erase(&OperationIndexer, &removal).await {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    generation_id = %generation_id,
                    error = %error,
                    "failed to remove a Provider Generation from the search index"
                );
            }
        });
    }

    /// A Provider was deleted: removes the rows and state rows of all its
    /// Generations.
    pub fn remove_provider(&self, workspace_id: WorkspaceId, provider_id: ProviderId) {
        let index = self.clone();
        self.spawn(async move {
            let _write = index.writes.lock().await;
            let removal = OperationRemoval::Provider {
                workspace_id: workspace_id.clone(),
                provider_id: provider_id.clone(),
            };
            if let Err(error) = index.erase(&OperationIndexer, &removal).await {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    workspace_id = %workspace_id,
                    provider_id = %provider_id,
                    error = %error,
                    "failed to remove a Provider from the search index"
                );
            }
        });
    }

    /// A Saved Function was saved or saved over: upserts its row and removes
    /// the `failed` state row an earlier attempt left. A failed attempt rolls
    /// back, removes the function's row, and leaves a `failed` state row.
    /// Removal needs no notification: the row cascades with the function.
    pub fn index_function(&self, entry: SavedFunctionEntry) {
        let index = self.clone();
        self.spawn(async move {
            let _write = index.writes.lock().await;
            index.write_function(&entry).await;
        });
    }

    /// Waits until no spawned write is running or waiting to run.
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
    /// counted until it finishes. The write logs to the caller's subscriber.
    ///
    /// Tasks reach the mutex in no fixed order, so each write first awaits its
    /// predecessor: a rename's rewrite and a later removal of the same
    /// Generation then land in the order they were notified.
    fn spawn(&self, write: impl Future<Output = ()> + Send + 'static) {
        #[cfg(any(test, feature = "test-fixtures"))]
        if self.inert {
            return;
        }
        self.in_flight.send_modify(|writes| *writes += 1);
        let in_flight = InFlightWrite(self.in_flight.clone());
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
                write.await;
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
    /// preparation left. A failure is logged and recorded. The caller holds
    /// the mutex.
    pub(crate) async fn write_generation(&self, entries: &GenerationEntries) {
        let rows = match self.write(&OperationIndexer, entries).await {
            Ok(rows) => rows,
            Err(error) => {
                self.fail_generation(entries.into(), &error).await;
                return;
            }
        };
        for (operation_key, warning) in rows.warnings() {
            tracing::warn!(
                workspace_id = %entries.workspace_id(),
                provider_id = %entries.provider_id(),
                generation_id = %entries.generation_id(),
                operation_key = %operation_key.as_str(),
                code = %warning.code(),
                "{}",
                warning.message()
            );
        }
    }

    /// Writes one Saved Function's entry. A failure is logged and recorded.
    /// The caller holds the mutex.
    async fn write_function(&self, entry: &SavedFunctionEntry) {
        let Err(error) = self.write(&SavedFunctionIndexer, entry).await else {
            return;
        };
        // TODO: an error monitor for failed index writes is still to be set up.
        tracing::error!(
            workspace_id = %entry.workspace_id(),
            principal_id = %entry.principal().id(),
            saved_function_id = %entry.id(),
            error = %error,
            "failed to index a Saved Function"
        );
        if let Err(record_error) = self.record_failed_function(entry, &error).await {
            tracing::error!(
                workspace_id = %entry.workspace_id(),
                principal_id = %entry.principal().id(),
                saved_function_id = %entry.id(),
                error = %record_error,
                "failed to record a failed Saved Function index attempt"
            );
        }
    }

    /// Logs a failed Generation attempt and records its `failed` state row.
    async fn fail_generation(&self, generation: AttemptedGeneration<'_>, error: &IndexError) {
        // TODO: an error monitor for failed index writes is still to be set up.
        tracing::error!(
            workspace_id = %generation.workspace_id(),
            provider_id = %generation.provider_id(),
            generation_id = %generation.generation_id(),
            error = %error,
            "failed to index a Provider Generation"
        );
        if let Err(record_error) = self.record_failed_generation(generation, error).await {
            tracing::error!(
                workspace_id = %generation.workspace_id(),
                provider_id = %generation.provider_id(),
                generation_id = %generation.generation_id(),
                error = %record_error,
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
    /// write that took the lock logs how long it waited and wrote. Returns
    /// the rows it wrote.
    async fn write<'e, I: EntityIndexer + 'e>(
        &self,
        indexer: &I,
        entry: &'e I::Entry,
    ) -> Result<I::Rows<'e>, IndexError> {
        let workspace_id = indexer.workspace_id(entry);
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
            if let Err(error) = indexer.index(&mut transaction, entry, written_rows).await {
                rollback(transaction).await;
                return Err(error);
            }
            transaction.commit().await?;
            Ok(())
        })
        .await
        .unwrap_or_else(|_elapsed| Err(IndexError::Overran(self.write_limit)));
        tracing::info!(
            workspace_id = %workspace_id,
            writer_wait_ms = millis(locked - started),
            write_ms = millis(locked.elapsed()),
            succeeded = written.is_ok(),
            "ended a search index write"
        );
        written.map(|()| rows)
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
            operations::record_failed_attempt(
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

/// Rolls back a failed write. A failed rollback is logged on its own, so the
/// caller still reports the write's error, the cause.
async fn rollback(transaction: Transaction<'_>) {
    if let Err(error) = transaction.rollback().await {
        tracing::error!(error = %error, "failed to roll back a search index write");
    }
}

/// A duration in whole milliseconds, for a log field.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Counts one spawned write as in flight until dropped, so a write that
/// panics still stops counting.
struct InFlightWrite(Arc<watch::Sender<usize>>);

impl Drop for InFlightWrite {
    fn drop(&mut self) {
        self.0.send_modify(|writes| *writes -= 1);
    }
}
