//! The Sub-agent model workflow owns the transaction that changes the
//! Organization's model together with the rules that turn `coral.agent` off.
//!
//! The settings module writes the model. Policy turns the Tool off in every
//! Workspace whenever the model's id changes, so no Workspace keeps a consent
//! to another destination. After the commit, each Workspace's Built-in catalog
//! gains or loses the Sub-agent's Tools; one that fails waits for the next
//! update, and its Turns stop all the same.

use coral_agent_model::{
    AgentModel, AgentModels, ApiKeyChange, EnteredSettings, Error, TestFailure, TestKey,
};
use coral_credentials::Secret;
use coral_db::{Transaction, UnixNanos};
use coral_telemetry::safe;
use coral_workspace::{AuthenticatedUser, ChangeContext, IfMatch, WorkspaceId};

use crate::ProviderWorkflow;

/// Reads and changes the model that the Organization's Sub-agents use.
#[derive(Clone)]
pub struct SubAgentModelWorkflow {
    models: AgentModels,
    providers: ProviderWorkflow,
}

impl SubAgentModelWorkflow {
    /// Composes the workflow over the settings module and the Provider
    /// workflow, whose database its transactions open on.
    #[must_use]
    pub const fn new(models: AgentModels, providers: ProviderWorkflow) -> Self {
        Self { models, providers }
    }

    /// The model, or `None` while the Organization has none.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`AgentModels::read`].
    pub async fn read(&self, actor: &AuthenticatedUser) -> Result<Option<AgentModel>, Error> {
        self.models.read(actor).await
    }

    /// The model, for a Workspace Owner to see what they agree to.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`AgentModels::read_for_workspace`].
    pub async fn read_for_workspace(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<AgentModel, Error> {
        self.models.read_for_workspace(actor, workspace_id).await
    }

    /// Asks a model for one reply with the settings as entered.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`AgentModels::test`].
    pub async fn test(
        &self,
        actor: &AuthenticatedUser,
        entered: &EnteredSettings,
        key: TestKey,
    ) -> Result<Result<(), TestFailure>, Error> {
        self.models.test(actor, entered, key).await
    }

    /// Sets the model of an Organization that has none. Each Workspace then
    /// gains `coral.agent`, turned off.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`AgentModels::set_in_transaction`], and changes
    /// nothing then.
    pub async fn set(
        &self,
        context: &ChangeContext,
        entered: &EnteredSettings,
        api_key: Option<Secret>,
    ) -> Result<AgentModel, Error> {
        let mut tx = self.providers.database().begin().await?;
        let set = self
            .models
            .set_in_transaction(&mut tx, context, entered, api_key);
        let set = set.await?;
        // A rule can be left from a model that was removed.
        turn_off(&mut tx, context).await?;
        tx.commit().await?;
        self.update_catalogs(context.occurred_at()).await;
        Ok(set)
    }

    /// Changes the model that the caller read at `if_match`. A new base URL
    /// turns `coral.agent` off in every Workspace.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`AgentModels::change_in_transaction`], and
    /// changes nothing then.
    pub async fn change(
        &self,
        context: &ChangeContext,
        if_match: &IfMatch,
        entered: &EnteredSettings,
        api_key: ApiKeyChange,
    ) -> Result<AgentModel, Error> {
        let mut tx = self.providers.database().begin().await?;
        let models = &self.models;
        let changed = models.change_in_transaction(&mut tx, context, if_match, entered, api_key);
        let changed = changed.await?;
        if changed.moved {
            turn_off(&mut tx, context).await?;
        }
        tx.commit().await?;
        Ok(changed.model)
    }

    /// Removes the model that the caller read at `if_match`. Each Workspace
    /// then loses the Sub-agent's Tools.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`AgentModels::remove_in_transaction`], and
    /// changes nothing then.
    pub async fn remove(&self, context: &ChangeContext, if_match: &IfMatch) -> Result<(), Error> {
        let mut tx = self.providers.database().begin().await?;
        let removed = self
            .models
            .remove_in_transaction(&mut tx, context, if_match);
        removed.await?;
        turn_off(&mut tx, context).await?;
        tx.commit().await?;
        self.update_catalogs(context.occurred_at()).await;
        Ok(())
    }

    /// Gives each Workspace the catalog that the committed model calls for.
    /// The change stands when it fails.
    async fn update_catalogs(&self, at: UnixNanos) {
        if let Err(error) = self.providers.update_builtin_catalogs(at).await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "failed to read the Built-in catalogs after a change of the Sub-agent's model"
            );
        }
    }
}

async fn turn_off(tx: &mut Transaction<'_>, context: &ChangeContext) -> Result<(), Error> {
    let off = coral_policy::turn_sub_agent_off_in_transaction(tx, context.occurred_at()).await;
    off.map_err(|error| match error.into_database_error() {
        Ok(error) => Error::Database(error),
        Err(_other) => Error::CorruptData("a Tool rule is not what Coral writes"),
    })
}
