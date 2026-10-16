//! Each Provider's Search Index State, read apart from readiness: the state is
//! information only and never changes what a caller may invoke (ADR 0017).
use std::collections::{BTreeMap, HashMap};

use coral_catalog::ProviderGenerationId;
use coral_db::Transaction;
use coral_provider::ActiveGeneration;
use coral_workspace::{AuthenticatedUser, WorkspaceAction, WorkspaceId};

use crate::{Error, ProviderId, SearchIndexState, UniversalSearchModule, indexer};

impl UniversalSearchModule {
    /// Reports whether search has indexed each Provider's active Generation,
    /// or only the one Provider the filter names. A superseded Generation's
    /// state is never a Provider's. A failed read is logged and reports no
    /// states, so it never fails the inspection that asked.
    /// # Errors
    /// Requires Workspace inspection authority; archived Workspaces remain inspectable.
    pub async fn inspect_search_index_states(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider_filter: Option<&ProviderId>,
    ) -> Result<BTreeMap<ProviderId, SearchIndexState>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.workspaces
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        let read = read_states(&mut tx, workspace, provider_filter).await;
        tx.rollback().await?;
        Ok(read.unwrap_or_else(|error| {
            tracing::warn!(
                workspace_id = %workspace,
                error = %error,
                "failed to read the Providers' search index states"
            );
            BTreeMap::new()
        }))
    }
}

async fn read_states(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
) -> Result<BTreeMap<ProviderId, SearchIndexState>, Error> {
    let generations =
        coral_provider::workspace_active_generations_in_transaction(tx, workspace, provider_filter)
            .await?;
    let outcomes = indexer::generation_outcomes_in_workspace(tx, workspace).await?;
    Ok(provider_states(generations, &outcomes))
}

/// A Generation no attempt has reached is pending.
fn provider_states(
    generations: Vec<ActiveGeneration>,
    outcomes: &HashMap<ProviderGenerationId, SearchIndexState>,
) -> BTreeMap<ProviderId, SearchIndexState> {
    generations
        .into_iter()
        .map(|active| {
            let state = outcomes
                .get(active.generation_id())
                .copied()
                .unwrap_or(SearchIndexState::Pending);
            (active.provider_id().clone(), state)
        })
        .collect()
}
