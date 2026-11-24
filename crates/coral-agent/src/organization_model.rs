//! The model that the Organization set, and whether a Workspace may use it.

use std::sync::Arc;

use coral_agent_model::{ActiveModel, AgentModels, ModelProtocol};
use coral_db::{Database, RowLock};
use coral_workspace::WorkspaceId;
use url::Url;
use uuid::Uuid;

use crate::model::{ModelClient, Protocol};
use crate::sub_agents::{ModelRefusal, ModelSource, TurnModel};

/// Looks the model up where an Organization Owner set it. A Workspace uses it
/// while its rule on `coral.agent` is on, which is its Owner's consent to the
/// model that is set.
pub(crate) struct OrganizationModel {
    pub(crate) database: Arc<Database>,
    pub(crate) models: AgentModels,
    /// Sends under the operator's destination policy.
    pub(crate) http: coral_outbound::Client,
}

#[async_trait::async_trait]
impl ModelSource for OrganizationModel {
    async fn current(&self, workspace_id: &WorkspaceId) -> Result<TurnModel, ModelRefusal> {
        let mut snapshot = self.database.begin_read_snapshot().await?;
        let on = coral_policy::sub_agent_is_on_in_transaction(&mut snapshot, workspace_id).await?;
        let active = self.models.active_in_transaction(&mut snapshot).await?;
        snapshot.commit().await?;
        let active = active.ok_or(ModelRefusal::NotSet)?;
        if !on {
            return Err(ModelRefusal::TurnedOff);
        }
        Ok(TurnModel {
            id: active.id,
            client: Arc::new(self.client(active)?),
        })
    }

    async fn allows(&self, workspace_id: &WorkspaceId, id: Uuid) -> Result<(), ModelRefusal> {
        let mut snapshot = self.database.begin_read_snapshot().await?;
        let on = coral_policy::sub_agent_is_on_in_transaction(&mut snapshot, workspace_id).await?;
        let current =
            coral_agent_model::current_id_in_transaction(&mut snapshot, RowLock::None).await?;
        snapshot.commit().await?;
        // A change of the base URL also turns the Tool off, so the model is
        // what the lead must read first.
        if current != Some(id) {
            return Err(ModelRefusal::Changed);
        }
        if on {
            Ok(())
        } else {
            Err(ModelRefusal::TurnedOff)
        }
    }
}

impl OrganizationModel {
    fn client(&self, active: ActiveModel) -> Result<ModelClient, ModelRefusal> {
        let settings = active.settings;
        let base_url = Url::parse(settings.base_url())
            .map_err(|_invalid| ModelRefusal::Unreadable("invalid_base_url"))?;
        Ok(ModelClient::new(
            self.http.clone(),
            match settings.protocol() {
                ModelProtocol::ChatCompletions => Protocol::ChatCompletions,
                ModelProtocol::OpenAiResponses => Protocol::OpenAiResponses,
                ModelProtocol::Anthropic => Protocol::Anthropic,
            },
            &base_url,
            settings.model().to_owned(),
            active.api_key,
            settings.context_window(),
            settings.request_args().clone(),
        ))
    }
}

impl From<coral_db::Error> for ModelRefusal {
    fn from(error: coral_db::Error) -> Self {
        Self::Unreadable(error.code())
    }
}

impl From<coral_policy::Error> for ModelRefusal {
    fn from(error: coral_policy::Error) -> Self {
        Self::Unreadable(
            error
                .database_error()
                .map_or("policy", coral_db::Error::code),
        )
    }
}

impl From<coral_agent_model::Error> for ModelRefusal {
    fn from(error: coral_agent_model::Error) -> Self {
        Self::Unreadable(error.code())
    }
}
