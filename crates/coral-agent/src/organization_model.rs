//! The model that the Organization set, and whether a Workspace may use it.

use std::sync::Arc;

use coral_agent_model::{AgentModels, ModelProtocol, ModelSettings, ModelTester, TestFailure};
use coral_credentials::Secret;
use coral_db::{Database, RowLock};
use coral_workspace::WorkspaceId;
use url::Url;
use uuid::Uuid;

use crate::model::{ModelClient, ModelError, Protocol};
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
            client: Arc::new(
                client(&self.http, &active.settings, active.api_key)
                    .map_err(|_invalid| ModelRefusal::Unreadable("invalid_base_url"))?,
            ),
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

/// Tests a model over a client of its own, which waits less than a Turn's.
pub(crate) struct Tester {
    pub(crate) http: coral_outbound::Client,
}

#[async_trait::async_trait]
impl ModelTester for Tester {
    async fn test(&self, settings: &ModelSettings, key: Option<Secret>) -> Result<(), TestFailure> {
        let client =
            client(&self.http, settings, key).map_err(|_invalid| TestFailure::Unreadable)?;
        client.probe().await.map_err(|error| match error {
            ModelError::Unavailable(None) => TestFailure::Unreachable,
            ModelError::Unavailable(Some(status))
            | ModelError::Refused(status)
            | ModelError::ContextTooLong(status) => TestFailure::Refused(status.as_u16()),
            ModelError::Forbidden => TestFailure::Forbidden,
            ModelError::Misconfigured => TestFailure::Unreadable,
        })
    }
}

fn client(
    http: &coral_outbound::Client,
    settings: &ModelSettings,
    api_key: Option<Secret>,
) -> Result<ModelClient, url::ParseError> {
    let base_url = Url::parse(settings.base_url())?;
    Ok(ModelClient::new(
        http.clone(),
        match settings.protocol() {
            ModelProtocol::ChatCompletions => Protocol::ChatCompletions,
            ModelProtocol::OpenAiResponses => Protocol::OpenAiResponses,
            ModelProtocol::Anthropic => Protocol::Anthropic,
        },
        &base_url,
        settings.model().to_owned(),
        api_key,
        settings.context_window(),
        settings.request_args().clone(),
    ))
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
