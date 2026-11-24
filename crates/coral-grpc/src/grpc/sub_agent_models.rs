use super::status::{self, invalid, safe_status};
use super::workspaces::{change, timestamp, workspace_id};
use super::{GrpcServices, management as wire, policies};
use coral_agent_model::{
    AgentModel, ApiKeyChange, EnteredSettings, Error, InvalidSetting, ModelProtocol, ModelSettings,
};
use coral_credentials::Secret;
use coral_workspace::IfMatch;
use tonic::{Code, Request, Response, Status};

fn settings(settings: &ModelSettings) -> wire::SubAgentModelSettings {
    wire::SubAgentModelSettings {
        protocol: match settings.protocol() {
            ModelProtocol::ChatCompletions => wire::SubAgentModelProtocol::ChatCompletions,
            ModelProtocol::OpenAiResponses => wire::SubAgentModelProtocol::OpenaiResponses,
            ModelProtocol::Anthropic => wire::SubAgentModelProtocol::Anthropic,
        }
        .into(),
        base_url: settings.base_url().to_owned(),
        model: settings.model().to_owned(),
        context_window: settings.context_window(),
        request_args_json: if settings.request_args().is_empty() {
            String::new()
        } else {
            serde_json::Value::Object(settings.request_args().clone()).to_string()
        },
    }
}

fn model(model: &AgentModel) -> wire::SubAgentModel {
    wire::SubAgentModel {
        id: model.id().to_string(),
        settings: Some(settings(model.settings())),
        has_api_key: model.has_api_key(),
        changed_by: format!("users/{}", model.changed_by()),
        change_time: Some(timestamp(model.changed_at())),
        etag: model.etag().to_string(),
    }
}

fn error(error: &Error) -> Status {
    let (code, reason) = match error {
        Error::PermissionDenied => return status::permission_denied(),
        Error::NotSet => return status::not_found(),
        Error::Invalid(setting) => (
            Code::InvalidArgument,
            match setting {
                InvalidSetting::BaseUrl => "INVALID_BASE_URL",
                InvalidSetting::Destination => "DESTINATION_NOT_PERMITTED",
                InvalidSetting::Model => "INVALID_MODEL",
                InvalidSetting::ApiKey => "INVALID_API_KEY",
                InvalidSetting::ContextWindow => "INVALID_CONTEXT_WINDOW",
                InvalidSetting::RequestArgs => "INVALID_REQUEST_ARGS",
                InvalidSetting::RequestArgName => "REQUEST_ARG_NOT_ALLOWED",
            },
        ),
        Error::AlreadySet => (Code::AlreadyExists, "SUB_AGENT_MODEL_ALREADY_SET"),
        Error::EtagMismatch => (Code::Aborted, "ETAG_MISMATCH"),
        Error::Sealing => return status::internal(),
        Error::Database(_) | Error::CorruptData(_) | Error::Unavailable => {
            return status::unavailable();
        }
    };
    safe_status(code, reason)
}

fn api_key(entered: String) -> Result<Secret, Status> {
    Secret::new(entered).map_err(|_invalid| error(&Error::Invalid(InvalidSetting::ApiKey)))
}

/// What the request entered. The module checks it once the caller is authorized.
fn entered(settings: Option<wire::SubAgentModelSettings>) -> Result<EnteredSettings, Status> {
    let settings = settings.ok_or_else(invalid)?;
    Ok(EnteredSettings {
        protocol: match settings.protocol() {
            wire::SubAgentModelProtocol::Unspecified => return Err(invalid()),
            wire::SubAgentModelProtocol::ChatCompletions => ModelProtocol::ChatCompletions,
            wire::SubAgentModelProtocol::OpenaiResponses => ModelProtocol::OpenAiResponses,
            wire::SubAgentModelProtocol::Anthropic => ModelProtocol::Anthropic,
        },
        base_url: settings.base_url,
        model: settings.model,
        context_window: settings.context_window,
        request_args_json: settings.request_args_json,
    })
}

#[tonic::async_trait]
impl wire::sub_agent_model_service_server::SubAgentModelService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_sub_agent_model")]
    async fn get_sub_agent_model(
        &self,
        request: Request<wire::GetSubAgentModelRequest>,
    ) -> Result<Response<wire::GetSubAgentModelResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let read = self.sub_agent_models.read(&actor).await;
        let read = read.map_err(|failure| error(&failure))?;
        Ok(Response::new(wire::GetSubAgentModelResponse {
            model: read.as_ref().map(model),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.create_sub_agent_model")]
    async fn create_sub_agent_model(
        &self,
        request: Request<wire::CreateSubAgentModelRequest>,
    ) -> Result<Response<wire::SubAgentModel>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let settings = entered(input.settings)?;
        let key = input.api_key.map(api_key).transpose()?;
        let set = self
            .sub_agent_models
            .set(&change(actor), &settings, key)
            .await;
        let set = set.map_err(|failure| error(&failure))?;
        Ok(Response::new(model(&set)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.update_sub_agent_model")]
    async fn update_sub_agent_model(
        &self,
        request: Request<wire::UpdateSubAgentModelRequest>,
    ) -> Result<Response<wire::SubAgentModel>, Status> {
        let actor = self.management_user(&request).await?;
        let input = request.into_inner();
        let if_match = IfMatch::parse(&input.etag).map_err(super::workspaces::error)?;
        let settings = entered(input.settings)?;
        let key = match input.api_key {
            None => ApiKeyChange::Keep,
            Some(entered) if entered.is_empty() => ApiKeyChange::Clear,
            Some(entered) => ApiKeyChange::Set(api_key(entered)?),
        };
        let context = change(actor);
        let changed = self
            .sub_agent_models
            .change(&context, &if_match, &settings, key);
        let changed = changed.await.map_err(|failure| error(&failure))?;
        Ok(Response::new(model(&changed)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.delete_sub_agent_model")]
    async fn delete_sub_agent_model(
        &self,
        request: Request<wire::DeleteSubAgentModelRequest>,
    ) -> Result<Response<()>, Status> {
        let actor = self.management_user(&request).await?;
        let if_match = IfMatch::parse(&request.get_ref().etag).map_err(super::workspaces::error)?;
        let removed = self
            .sub_agent_models
            .remove(&change(actor), &if_match)
            .await;
        removed.map_err(|failure| error(&failure))?;
        Ok(Response::new(()))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_workspace_sub_agent_model")]
    async fn get_workspace_sub_agent_model(
        &self,
        request: Request<wire::GetWorkspaceSubAgentModelRequest>,
    ) -> Result<Response<wire::WorkspaceSubAgentModel>, Status> {
        let actor = self.management_user(&request).await?;
        let workspace = workspace_id(&request.get_ref().workspace)?;
        let read = self
            .sub_agent_models
            .read_for_workspace(&actor, &workspace)
            .await
            .map_err(|failure| error(&failure))?;
        Ok(Response::new(wire::WorkspaceSubAgentModel {
            id: read.id().to_string(),
            settings: Some(settings(read.settings())),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.turn_on_sub_agent")]
    async fn turn_on_sub_agent(
        &self,
        request: Request<wire::TurnOnSubAgentRequest>,
    ) -> Result<Response<wire::TurnOnSubAgentResponse>, Status> {
        let context = change(self.management_user(&request).await?);
        let input = request.into_inner();
        let workspace = workspace_id(&input.workspace)?;
        let seen = input.model_id.parse().map_err(|_not_a_uuid| invalid())?;
        let state = policies::access(input.access_state)?;
        let decision = self
            .policies
            .turn_on_sub_agent(&context, &workspace, seen, state)
            .await
            .map_err(policies::error)?;
        Ok(Response::new(wire::TurnOnSubAgentResponse {
            access_state: policies::access_state(decision.access_state()).into(),
        }))
    }
}
