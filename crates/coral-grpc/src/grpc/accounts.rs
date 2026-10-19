//! Reef Account projection. Domains own authorization, transactions and lifecycle evidence.
use super::providers::{error, parent, resource, scope};
use super::workspaces::{change, invalid, timestamp};
use super::{GrpcServices, management as wire};
use coral_provider::account::{self as domain, AccountScope};
use coral_provider::{ManagementRequestId, ProviderId};
use coral_workspace::{AuthenticatedUser, PrincipalId, WorkspaceId};
use tonic::{Request, Response, Status};

fn account_id(id: &str) -> Result<domain::ProviderAccountId, Status> {
    domain::ProviderAccountId::parse(id).map_err(error)
}
fn authorization_name(owner: &WorkspaceId, id: domain::AuthorizationId) -> String {
    format!("{}/accountAuthorizations/{id}", parent(owner))
}
fn authorization(name: &str) -> Result<(WorkspaceId, domain::AuthorizationId), Status> {
    let (owner, id) = resource(name, "accountAuthorizations")?;
    Ok((owner, domain::AuthorizationId::parse(id).map_err(error)?))
}
fn authorization_view(
    owner: &WorkspaceId,
    value: domain::Authorization,
) -> wire::ProviderAccountAuthorization {
    use wire::provider_account_authorization::Target;
    let provider = format!("{}/providers/{}", parent(owner), value.provider_id);
    let target = match &value.scope {
        AccountScope::User => Target::UserProvider(provider),
        AccountScope::Shared => Target::SharedProvider(provider),
        AccountScope::Agent(agent) => Target::AgentAccount(wire::AgentAccountAuthorizationTarget {
            provider,
            agent: format!("{}/agents/{agent}", parent(owner)),
        }),
    };
    wire::ProviderAccountAuthorization {
        user_code: value.user_code.clone(),
        name: authorization_name(owner, value.id),
        target: Some(target),
        state: match value.state {
            domain::AuthorizationState::Pending => wire::SetupAuthorizationState::Pending,
            domain::AuthorizationState::Exchanging => wire::SetupAuthorizationState::Exchanging,
            domain::AuthorizationState::Succeeded => wire::SetupAuthorizationState::Succeeded,
            domain::AuthorizationState::Failed => wire::SetupAuthorizationState::Failed,
            domain::AuthorizationState::Canceled => wire::SetupAuthorizationState::Canceled,
            domain::AuthorizationState::Expired => wire::SetupAuthorizationState::Expired,
        }
        .into(),
        consent_navigation: value
            .authorization_url
            .clone()
            .map(super::providers::consent_navigation),
        consent_rejected: value.consent_rejected,
        account: value
            .account_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        expire_time: Some(timestamp(value.expires_at_unix_nanos)),
        failure_code: value.failure_code().unwrap_or_default().into(),
        automatic_refresh: value.automatic_refresh,
        granted_scope: value.granted_scope,
        credential_fields: super::providers::credential_fields(value.credential_fields),
    }
}
fn account_view(value: domain::AccountSummary) -> wire::ProviderAccount {
    wire::ProviderAccount {
        id: value.id.to_string(),
        state: match value.state {
            domain::AccountState::Ready => wire::ProviderAccountState::Ready,
            domain::AccountState::ReconnectRequired => {
                wire::ProviderAccountState::ReconnectRequired
            }
            domain::AccountState::Disconnected => wire::ProviderAccountState::Disconnected,
        }
        .into(),
        automatic_refresh: value.automatic_refresh,
        granted_scope: value.granted_scope,
        management_parent: parent(&value.management_scope),
        can_manage: value.can_manage,
        connected_time: value.connected_at_unix_nanos.map(timestamp),
        configured_by_user: value
            .configured_by_user_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
    }
}
fn account_inspection_view(value: domain::AccountView) -> wire::GetProviderAccountResponse {
    let method = super::providers::wire_method(value.contract.authentication().kind());
    let scopes = match value.contract.authentication() {
        coral_catalog::AuthenticationConfiguration::OAuthAuthorizationCode { scopes, .. } => {
            scopes.clone()
        }
        _ => vec![],
    };
    wire::GetProviderAccountResponse {
        account: value.account.map(account_view),
        credential_method: method.into(),
        credential_fields: super::providers::credential_fields(value.credential_fields),
        requested_scopes: scopes,
        can_connect: value.can_connect,
        can_reconnect: value.can_reconnect,
    }
}

fn agent_provider_view(
    value: coral_provider::access::AgentProviderInspection,
) -> wire::AgentProviderInspection {
    use coral_provider::access::AgentAccountUse;
    use wire::agent_provider_inspection::AccountUse;
    wire::AgentProviderInspection {
        provider: Some(super::providers::provider_view(
            &value.readiness.provider,
            &value.readiness.blockers,
            None,
        )),
        account_use: Some(match value.account {
            AgentAccountUse::None => AccountUse::NoAccount(()),
            AgentAccountUse::Shared(account) => {
                AccountUse::SharedAccount(account_inspection_view(account))
            }
            AgentAccountUse::Separate(account) => {
                AccountUse::SeparateAccount(account_inspection_view(account))
            }
        }),
    }
}

fn provider_resource(name: &str) -> Result<(WorkspaceId, ProviderId), Status> {
    let (workspace, id) = super::resource(name, "providers")?;
    Ok((workspace, ProviderId::parse(id).map_err(error)?))
}
fn agent(name: &str, workspace: &WorkspaceId) -> Result<Option<PrincipalId>, Status> {
    if name.is_empty() {
        return Ok(None);
    }
    let (ws, id) = super::resource(name, "agents")?;
    if &ws != workspace {
        return Err(invalid("Agent Workspace mismatch"));
    }
    Ok(Some(
        PrincipalId::parse(id).map_err(super::workspaces::error)?,
    ))
}
fn connection(name: &str) -> Result<(WorkspaceId, domain::ConnectionRequestId), Status> {
    let (ws, id) = super::resource(name, "accountConnections")?;
    Ok((ws, domain::ConnectionRequestId::parse(id).map_err(error)?))
}
fn connection_view(
    ws: &WorkspaceId,
    value: &domain::ConnectionRequest,
) -> wire::AccountConnectionRequest {
    wire::AccountConnectionRequest {
        name: format!("workspaces/{ws}/accountConnections/{}", value.id),
        provider: format!("workspaces/{ws}/providers/{}", value.provider_id),
        state: match value.state {
            domain::ConnectionState::Pending => wire::AccountConnectionState::Pending,
            domain::ConnectionState::Connected => wire::AccountConnectionState::Connected,
            domain::ConnectionState::Declined => wire::AccountConnectionState::Declined,
            domain::ConnectionState::Expired => wire::AccountConnectionState::Expired,
            domain::ConnectionState::RouteChanged => wire::AccountConnectionState::RouteChanged,
            domain::ConnectionState::AuthorizationChanged => {
                wire::AccountConnectionState::AuthorizationChanged
            }
        }
        .into(),
        expire_time: Some(timestamp(value.expires_at_unix_nanos)),
    }
}
fn provider_in_scope(owner: &WorkspaceId, name: &str) -> Result<(WorkspaceId, ProviderId), Status> {
    let (ws, id) = provider_resource(name)?;
    if owner != &ws {
        return Err(invalid("Provider scope mismatch"));
    }
    Ok((ws, id))
}

impl GrpcServices {
    async fn account_provider(
        &self,
        actor: &AuthenticatedUser,
        inspection: &WorkspaceId,
        name: &str,
    ) -> Result<ProviderId, Status> {
        let (owner, id) = resource(name, "providers")?;
        if &owner != inspection {
            return Err(error(coral_provider::Error::ProviderNotFound));
        }
        let id = ProviderId::parse(id).map_err(error)?;
        self.visible_provider(actor, inspection, &id).await?;
        Ok(id)
    }
}

#[tonic::async_trait]
impl wire::provider_account_service_server::ProviderAccountService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.list_agent_providers")]
    async fn list_agent_providers(
        &self,
        request: Request<wire::ListAgentProvidersRequest>,
    ) -> Result<Response<wire::ListAgentProvidersResponse>, Status> {
        let user = self.management_user(&request).await?;
        let req = request.into_inner();
        let (workspace, agent) = super::resource(&req.parent, "agents")?;
        let agent = PrincipalId::parse(agent).map_err(super::workspaces::error)?;
        let inspections = self
            .provider_readiness
            .inspect_agent_providers(&user, &workspace, &agent)
            .await
            .map_err(super::providers::readiness_error)?;
        Ok(Response::new(wire::ListAgentProvidersResponse {
            providers: inspections.into_iter().map(agent_provider_view).collect(),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.list_user_accounts")]
    async fn list_user_accounts(
        &self,
        request: Request<wire::ListUserAccountsRequest>,
    ) -> Result<Response<wire::ListUserAccountsResponse>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let owner = scope(&request.into_inner().parent)?;
        let accounts = self
            .accounts()?
            .list_user_accounts(&context, &owner)
            .await
            .map_err(error)?;
        Ok(Response::new(wire::ListUserAccountsResponse {
            accounts: accounts
                .into_iter()
                .map(|value| wire::UserAccountInspection {
                    provider: format!("workspaces/{owner}/providers/{}", value.provider.id()),
                    provider_display_name: value.provider.display_name().to_owned(),
                    account: Some(account_inspection_view(value.account)),
                    pending_connection: value
                        .pending_connection
                        .as_ref()
                        .map(|request| connection_view(&owner, request)),
                })
                .collect(),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_provider_account")]
    async fn get_provider_account(
        &self,
        request: Request<wire::GetProviderAccountRequest>,
    ) -> Result<Response<wire::GetProviderAccountResponse>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let owner = scope(&req.parent)?;
        let provider = self
            .account_provider(context.actor(), &owner, &req.provider)
            .await?;
        let filter = match wire::ProviderAccountScope::try_from(req.scope) {
            Ok(wire::ProviderAccountScope::User) if req.agent.is_empty() => AccountScope::User,
            Ok(wire::ProviderAccountScope::Shared) if req.agent.is_empty() => AccountScope::Shared,
            Ok(wire::ProviderAccountScope::Agent) => AccountScope::Agent(
                agent(&req.agent, &owner)?.ok_or_else(|| invalid("Agent required"))?,
            ),
            _ => return Err(invalid("invalid Account scope")),
        };
        let accounts = self
            .accounts()?
            .get_account(&context, &owner, &provider, &filter)
            .await
            .map_err(error)?;
        Ok(Response::new(account_inspection_view(accounts)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.start_account_authorization")]
    async fn start_provider_account_authorization(
        &self,
        request: Request<wire::StartProviderAccountAuthorizationRequest>,
    ) -> Result<Response<wire::ProviderAccountAuthorization>, Status> {
        use wire::start_provider_account_authorization_request::Target;
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let owner = scope(&req.parent)?;
        let request_id = ManagementRequestId::parse(&req.request_id).map_err(error)?;
        let client = domain::OAuthClient::from_provider(req.redirect_uri).map_err(error)?;
        let accounts = self.accounts()?;
        let value = match req
            .target
            .ok_or_else(|| invalid("authorization target required"))?
        {
            Target::SharedProvider(name) => {
                let provider = self
                    .account_provider(context.actor(), &owner, &name)
                    .await?;
                accounts
                    .start_authorization(&context, &owner, &provider, client, request_id)
                    .await
            }
            Target::Provider(name) => {
                let (ws, id) = provider_in_scope(&owner, &name)?;
                accounts
                    .start_user_authorization(&context, &ws, &id, request_id, Some(client))
                    .await
            }
            Target::AgentAccount(target) => {
                let (ws, id) = provider_in_scope(&owner, &target.provider)?;
                let agent = agent(&target.agent, &ws)?.ok_or_else(|| invalid("Agent required"))?;
                accounts
                    .start_agent_authorization(&context, &ws, &agent, &id, request_id, Some(client))
                    .await
            }
            Target::Reconnect(target) => {
                let provider = self
                    .account_provider(context.actor(), &owner, &target.provider)
                    .await?;
                accounts
                    .start_reconnect(
                        &context,
                        &owner,
                        &provider,
                        account_id(&target.account)?,
                        request_id,
                        Some(client),
                    )
                    .await
            }
        }
        .map_err(error)?;
        Ok(Response::new(authorization_view(&owner, value)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_account_authorization")]
    async fn get_provider_account_authorization(
        &self,
        request: Request<wire::GetProviderAccountAuthorizationRequest>,
    ) -> Result<Response<wire::ProviderAccountAuthorization>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let (owner, id) = authorization(&request.get_ref().name)?;
        Ok(Response::new(authorization_view(
            &owner,
            self.accounts()?
                .get_authorization(&context, &owner, id)
                .await
                .map_err(error)?,
        )))
    }

    #[tracing::instrument(skip_all, name = "coral.private.submit_account_secret")]
    async fn submit_provider_account_secret(
        &self,
        request: Request<wire::SubmitProviderAccountSecretRequest>,
    ) -> Result<Response<wire::ProviderAccountAuthorization>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let (owner, id) = authorization(&req.name)?;
        let value = self
            .accounts()?
            .submit_secret(
                &context,
                &owner,
                id,
                super::providers::credential_values(req.credential_values)?,
            )
            .await
            .map_err(error)?;
        Ok(Response::new(authorization_view(&owner, value)))
    }

    #[tracing::instrument(skip_all, name = "coral.private.complete_provider_setup_authorization")]
    async fn complete_provider_setup_authorization(
        &self,
        request: Request<wire::CompleteProviderSetupAuthorizationRequest>,
    ) -> Result<Response<wire::CompleteProviderSetupAuthorizationResponse>, Status> {
        use wire::complete_provider_setup_authorization_response::Workflow;
        let context = change(self.management_user(&request).await?)?;
        let completion = self
            .accounts()?
            .complete_callback(&context, &request.into_inner().query)
            .await
            .map_err(error)?;
        let workflow = match completion {
            domain::CallbackCompletion::Setup { owner, setup_id } => {
                Workflow::ProviderSetup(format!("{}/providerSetups/{setup_id}", parent(&owner)))
            }
            domain::CallbackCompletion::Account {
                owner,
                authorization_id,
            } => Workflow::AccountAuthorization(authorization_name(&owner, authorization_id)),
        };
        Ok(Response::new(
            wire::CompleteProviderSetupAuthorizationResponse {
                workflow: Some(workflow),
            },
        ))
    }

    #[tracing::instrument(skip_all, name = "coral.private.cancel_account_authorization")]
    async fn cancel_provider_account_authorization(
        &self,
        request: Request<wire::CancelProviderAccountAuthorizationRequest>,
    ) -> Result<Response<wire::ProviderAccountAuthorization>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let (owner, id) = authorization(&request.get_ref().name)?;
        Ok(Response::new(authorization_view(
            &owner,
            self.accounts()?
                .cancel(&context, &owner, id)
                .await
                .map_err(error)?,
        )))
    }

    #[tracing::instrument(skip_all, name = "coral.private.disconnect_provider_account")]
    async fn disconnect_provider_account(
        &self,
        request: Request<wire::DisconnectProviderAccountRequest>,
    ) -> Result<Response<()>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let owner = scope(&req.parent)?;
        let provider = self
            .account_provider(context.actor(), &owner, &req.provider)
            .await?;
        self.accounts()?
            .disconnect(&context, &owner, &provider, account_id(&req.account)?)
            .await
            .map_err(error)?;
        Ok(Response::new(()))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_account_connection")]
    async fn get_account_connection_request(
        &self,
        request: Request<wire::GetAccountConnectionRequestRequest>,
    ) -> Result<Response<wire::AccountConnectionRequest>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let (ws, id) = connection(&request.get_ref().name)?;
        let value = connection_view(
            &ws,
            &self
                .accounts()?
                .get_connection_request(&context, &ws, id)
                .await
                .map_err(error)?,
        );
        Ok(Response::new(value))
    }

    #[tracing::instrument(skip_all, name = "coral.private.resolve_account_connection")]
    async fn resolve_account_connection_request(
        &self,
        request: Request<wire::ResolveAccountConnectionRequestRequest>,
    ) -> Result<Response<wire::AccountConnectionRequest>, Status> {
        use wire::resolve_account_connection_request_request::Decision;
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let (ws, id) = connection(&req.name)?;
        let value = match req
            .decision
            .ok_or_else(|| invalid("connection decision required"))?
        {
            Decision::Decline(()) => self.accounts()?.decline_connection(&context, &ws, id).await,
        }
        .map_err(error)?;
        Ok(Response::new(connection_view(&ws, &value)))
    }
}

#[cfg(test)]
mod tests {
    use coral_workspace::WorkspaceId;
    use tonic::Code;

    const PARENT: &str = "00000000-0000-4000-8000-000000000001";
    const OTHER: &str = "00000000-0000-4000-8000-000000000002";
    const ID: &str = "00000000-0000-4000-8000-0000000000aa";

    /// A Provider or Agent named under another Workspace than the request's parent
    /// is refused before any lookup, even when the ID exists there.
    #[test]
    fn a_named_resource_must_belong_to_the_parent_workspace() {
        let parent = WorkspaceId::parse(PARENT).unwrap();
        let provider = |workspace: &str| format!("workspaces/{workspace}/providers/{ID}");
        let agent = |workspace: &str| format!("workspaces/{workspace}/agents/{ID}");
        let (owner, _provider) = super::provider_in_scope(&parent, &provider(PARENT)).unwrap();
        assert_eq!(owner, parent);
        assert!(super::agent(&agent(PARENT), &parent).unwrap().is_some());
        assert_eq!(
            super::provider_in_scope(&parent, &provider(OTHER))
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            super::agent(&agent(OTHER), &parent).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
}
