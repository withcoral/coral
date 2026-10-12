//! The reconcile task of [`SearchIndex`]: its schedule, one pass, and the
//! pass's steps, as [`SearchIndex::reconcile`] lists them.

use coral_provider::{GenerationEntries, ProviderId};
use coral_workspace::WorkspaceId;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::instrument::WithSubscriber as _;

use super::SearchIndex;
use crate::indexer::functions::SavedFunctionIndexer;
use crate::indexer::operations::OperationIndexer;
use crate::indexer::{self, EntityIndexer, IndexError};

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
    ///    one older than the reconcile interval, from the stored Generation.
    /// 2. Indexes every Saved Function whose row is missing, whose fingerprint
    ///    differs from its definition revision, or that has a `failed` state
    ///    row.
    /// 3. Removes the rows and state rows of every Generation that is no
    ///    Provider's active Generation. Best effort. Saved Function rows never
    ///    outlive their function.
    /// 4. On Postgres, rewrites the statistics of every Workspace, not only
    ///    those this pass wrote: two processes that write one Workspace at
    ///    once can each count without the other's rows, and nothing else
    ///    corrects that until the Workspace's next write.
    ///
    /// Removal runs first, in a transaction of its own that reads the active
    /// Generations again; a failed removal is logged and the pass goes on.
    /// Each Generation and each function is written in a transaction of its
    /// own; a failure is logged and recorded like a notified write's, and the
    /// pass goes on. Passes in two processes write the same idempotent rows.
    pub async fn reconcile(&self) {
        let _write = self.writes.lock().await;
        let Some(now) = crate::unix_now_nanos() else {
            // TODO: an error monitor for failed index writes is still to be set up.
            tracing::error!(error = %IndexError::Clock, "failed to start a search index reconcile pass");
            return;
        };
        let interval = i64::try_from(self.reconcile_interval.as_nanos()).unwrap_or(i64::MAX);
        let retry_failed_from = now.saturating_sub(interval);
        self.remove_superseded_generations().await;
        self.reconcile_generations(retry_failed_from).await;
        self.reconcile_functions(retry_failed_from).await;
        self.refresh_every_workspace().await;
    }

    /// Step 3 of a pass, run first: removes the rows and state rows of every
    /// Generation that is no Provider's active Generation. Best effort.
    async fn remove_superseded_generations(&self) {
        let removed = self
            .within_limit(async {
                let mut transaction = self.database.begin().await?;
                let active =
                    coral_provider::active_generations_in_transaction(&mut transaction).await?;
                OperationIndexer
                    .remove_superseded(&mut transaction, &active)
                    .await?;
                transaction.commit().await?;
                Ok(())
            })
            .await;
        if let Err(error) = removed {
            // TODO: an error monitor for failed index writes is still to be set up.
            tracing::error!(
                error = %error,
                "failed to remove superseded Provider Generations from the search index"
            );
        }
    }

    /// Step 1 of a pass: indexes each active Generation the index lacks,
    /// holds under an old name, or holds as failed before
    /// `retry_failed_from`.
    async fn reconcile_generations(&self, retry_failed_from: i64) {
        let stale = self
            .within_limit(async {
                let mut transaction = self.database.begin_read_snapshot().await?;
                let expected =
                    coral_provider::active_generations_in_transaction(&mut transaction).await?;
                let stale = OperationIndexer
                    .reconcile(&mut transaction, &expected, retry_failed_from)
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
                tracing::error!(error = %error, "failed to reconcile the indexed Provider Generations");
                return;
            }
        };
        for generation in &stale {
            match self
                .prepare_stored_generation(generation.workspace_id(), generation.provider_id())
                .await
            {
                Ok(Some(entries)) => self.write_generation(&entries).await,
                // The Provider was deleted after the pass read it; its rows
                // cascaded with its Generations.
                Ok(None) => {}
                Err(error) => self.fail_generation(generation.into(), &error).await,
            }
        }
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

    /// Step 2 of a pass: indexes each current Saved Function the index lacks,
    /// holds at another revision, or holds as failed.
    async fn reconcile_functions(&self, retry_failed_from: i64) {
        let stale = self
            .within_limit(async {
                let mut transaction = self.database.begin_read_snapshot().await?;
                let expected =
                    coral_functions::all_saved_function_entries_in_transaction(&mut transaction)
                        .await?;
                let stale = SavedFunctionIndexer
                    .reconcile(&mut transaction, &expected, retry_failed_from)
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
                for entry in &stale {
                    self.write_function(entry).await;
                }
            }
            Err(error) => {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(error = %error, "failed to reconcile the indexed Saved Functions");
            }
        }
    }

    /// Step 4 of a pass: on Postgres, rewrites every Workspace's statistics,
    /// one Workspace per transaction.
    async fn refresh_every_workspace(&self) {
        let workspaces = self
            .within_limit(async {
                let mut transaction = self.database.begin_read_snapshot().await?;
                let workspaces = indexer::workspaces_with_statistics(&mut transaction).await?;
                transaction.rollback().await?;
                Ok(workspaces)
            })
            .await;
        let workspaces = match workspaces {
            Ok(workspaces) => workspaces,
            Err(error) => {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(error = %error, "failed to list the Workspaces to refresh search statistics for");
                return;
            }
        };
        for workspace_id in &workspaces {
            let refreshed = self
                .within_limit(async {
                    let mut transaction = self.database.begin().await?;
                    indexer::refresh_statistics(&mut transaction, workspace_id).await?;
                    transaction.commit().await?;
                    Ok(())
                })
                .await;
            if let Err(error) = refreshed {
                // TODO: an error monitor for failed index writes is still to be set up.
                tracing::error!(
                    workspace_id = %workspace_id,
                    error = %error,
                    "failed to refresh a Workspace's search statistics"
                );
            }
        }
    }
}
