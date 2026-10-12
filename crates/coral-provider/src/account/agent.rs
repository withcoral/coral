//! Owner provisioning keeps the accountable User separate from the Agent owner.
//! Execution only selects existing evidence; it never starts this workflow.
use coral_workspace::WorkspaceId;

use super::{AccountModule, Authorization, OAuthClient};
use crate::Error;
use crate::ProviderId;
use coral_workspace::ChangeContext;
use coral_workspace::PrincipalId;

impl AccountModule {
    /// Starts an initial connection for one exact Agent and Provider.
    /// The provisioning User is not the Agent; connected Accounts require explicit reconnect.
    /// # Errors
    /// Requires a current Workspace Owner, an exact Workspace Agent and caller Provider.
    pub async fn start_agent_authorization(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        agent: &PrincipalId,
        target_provider: &ProviderId,
        request: crate::ManagementRequestId,
        oauth: Option<OAuthClient>,
    ) -> Result<Authorization, Error> {
        self.start(
            context,
            workspace,
            target_provider,
            request,
            oauth,
            &super::AuthorizationTarget::Agent {
                target_provider: target_provider.clone(),
                agent: agent.clone(),
            },
        )
        .await
    }
}
