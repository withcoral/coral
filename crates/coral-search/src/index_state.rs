//! Each Provider's Search Index State, read apart from readiness: the state is
//! information only and never changes what a caller may invoke (ADR 0017).
use std::collections::{BTreeMap, HashMap};

use coral_catalog::ProviderGenerationId;
use coral_db::Transaction;
use coral_provider::ActiveGeneration;
use coral_workspace::{AuthenticatedUser, WorkspaceAction, WorkspaceId};

use crate::indexer::{self, IndexError};
use crate::{Error, ProviderId, SearchIndexState, UniversalSearchModule};

/// Why a state read stopped: the caller lacks authority, or the read failed.
enum StateReadError {
    Authority(Error),
    Read(IndexError),
}

impl UniversalSearchModule {
    /// Reports whether search has indexed each Provider's active Generation,
    /// or only the one Provider the filter names. A superseded Generation's
    /// state is never a Provider's. Any failure but authority is logged and
    /// reports no states, so it never fails the inspection that asked.
    /// # Errors
    /// Requires Workspace inspection authority; archived Workspaces remain inspectable.
    pub async fn inspect_search_index_states(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider_filter: Option<&ProviderId>,
    ) -> Result<BTreeMap<ProviderId, SearchIndexState>, Error> {
        match self
            .read_search_index_states(actor, workspace, provider_filter)
            .await
        {
            Ok(states) => Ok(states),
            Err(StateReadError::Authority(error)) => Err(error),
            Err(StateReadError::Read(error)) => {
                tracing::warn!(
                    coral.workspace.id = %workspace,
                    error.type = coral_telemetry::error_type(&error),
                    "failed to read the Providers' search index states"
                );
                Ok(BTreeMap::new())
            }
        }
    }

    async fn read_search_index_states(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider_filter: Option<&ProviderId>,
    ) -> Result<BTreeMap<ProviderId, SearchIndexState>, StateReadError> {
        let mut tx = self
            .database
            .begin_read_snapshot()
            .await
            .map_err(|error| StateReadError::Read(error.into()))?;
        self.workspaces
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await
            .map_err(|error| StateReadError::Authority(error.into()))?;
        let read = read_states(&mut tx, workspace, provider_filter).await;
        tx.rollback()
            .await
            .map_err(|error| StateReadError::Read(error.into()))?;
        read.map_err(StateReadError::Read)
    }
}

async fn read_states(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
) -> Result<BTreeMap<ProviderId, SearchIndexState>, IndexError> {
    let generations =
        coral_provider::workspace_active_generations_in_transaction(tx, workspace, provider_filter)
            .await?;
    let outcomes =
        indexer::generation_outcomes_in_workspace(tx, workspace, provider_filter).await?;
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
