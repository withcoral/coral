//! [`SearchIndex`]: the object the committing workflows notify after their
//! commits, so Universal Search writes its own index in the background, and
//! the reconcile task that converges what a notification missed.

use std::future::Future;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

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
use crate::indexer::{EntityIndexer, IndexError};

mod reconcile;

/// How long one index write may run before it is rolled back and logged as
/// failed. The limit frees the mutex: a write stalled on a database lock would
/// otherwise hold back every later write in the process.
const WRITE_LIMIT: Duration = Duration::from_mins(1);

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
    reconcile_interval: Duration,
    /// The last notified write; the next one waits for it.
    last_write: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    in_flight: Arc<watch::Sender<usize>>,
    /// A test-only index whose notifications write nothing.
    #[cfg(any(test, feature = "test-fixtures"))]
    inert: bool,
}

impl SearchIndex {
    /// Builds the write side over initialized domain persistence whose search
    /// storage [`crate::prepare_storage`] has prepared. The reconcile task
    /// runs a pass every `reconcile_interval`, and a pass retries a failed
    /// Generation once its failure is older than that.
    #[must_use]
    pub fn new(database: Arc<Database>, reconcile_interval: Duration) -> Self {
        Self {
            database,
            writes: Arc::new(Mutex::new(())),
            write_limit: WRITE_LIMIT,
            reconcile_interval,
            last_write: Arc::new(std::sync::Mutex::new(None)),
            in_flight: Arc::new(watch::Sender::new(0)),
            #[cfg(any(test, feature = "test-fixtures"))]
            inert: false,
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

    /// A Generation became its Provider's active Generation: writes the
    /// Generation's rows and its `complete` state row in one transaction.
    /// Writing an indexed Generation again rewrites its rows. A failed
    /// attempt rolls back and leaves a `failed` state row.
    pub fn index_generation(&self, entries: Arc<GenerationEntries>) {
        let index = self.clone();
        self.spawn(async move {
            let _write = index.writes.lock().await;
            index.write_generation(&entries).await;
        });
    }

    /// A Provider was renamed: prepares its active Generation's entries from
    /// what the catalog stores and rewrites that Generation's rows under the
    /// new name. The rename's response does not wait for the rebuild. A
    /// failed rebuild is logged, and the reconcile task re-indexes a Provider
    /// whose name differs from the indexed one.
    pub fn index_active_generation(&self, workspace_id: WorkspaceId, provider_id: ProviderId) {
        let index = self.clone();
        self.spawn(async move {
            let _write = index.writes.lock().await;
            match index
                .prepare_stored_generation(&workspace_id, &provider_id)
                .await
            {
                Ok(Some(entries)) => index.write_generation(&entries).await,
                // The Provider was deleted after the rename; its removal
                // notification removes its rows.
                Ok(None) => {}
                Err(error) => {
                    // TODO: an error monitor for failed index writes is still to be set up.
                    tracing::error!(
                        workspace_id = %workspace_id,
                        provider_id = %provider_id,
                        error = %error,
                        "failed to prepare a renamed Provider's Generation for the search index"
                    );
                }
            }
        });
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

    /// Replaces the default one-minute limit on each index write. A test can
    /// shorten it rather than wait out the real one; a search-quality eval can
    /// lengthen it to index a Provider too large for the default.
    #[must_use]
    pub const fn with_write_limit(mut self, limit: Duration) -> Self {
        self.write_limit = limit;
        self
    }

    /// Writes one Generation's entries. A failure is logged and recorded.
    /// The caller holds the mutex.
    async fn write_generation(&self, entries: &GenerationEntries) {
        if let Err(error) = self.write(&OperationIndexer, entries).await {
            self.fail_generation(entries.into(), &error).await;
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

    async fn write<I: EntityIndexer>(
        &self,
        indexer: &I,
        entry: &I::Entry,
    ) -> Result<(), IndexError> {
        self.within_limit(async {
            let mut transaction = self.database.begin().await?;
            if let Err(error) = indexer.index(&mut transaction, entry).await {
                rollback(transaction).await;
                return Err(error);
            }
            transaction.commit().await?;
            Ok(())
        })
        .await
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

    async fn record_failed_generation(
        &self,
        generation: AttemptedGeneration<'_>,
        error: &IndexError,
    ) -> Result<(), IndexError> {
        self.within_limit(async {
            let mut transaction = self.database.begin().await?;
            operations::record_failed_attempt(&mut transaction, generation, error).await?;
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
            let mut transaction = self.database.begin().await?;
            functions::record_failed_attempt(&mut transaction, entry, error).await?;
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

/// Counts one spawned write as in flight until dropped, so a write that
/// panics still stops counting.
struct InFlightWrite(Arc<watch::Sender<usize>>);

impl Drop for InFlightWrite {
    fn drop(&mut self) {
        self.0.send_modify(|writes| *writes -= 1);
    }
}
