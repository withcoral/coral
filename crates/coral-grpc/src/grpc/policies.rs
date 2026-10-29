//! Exact transport-safe Tool addresses, with atomic writes in the policy domain.
use super::workspaces::{change, invalid};
use super::{GrpcServices, management as wire};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use coral_policy::{ToolAccessState, ToolPolicyTarget};
use coral_provider::ProviderId;
use tonic::{Request, Response, Status};

pub(super) fn error(value: coral_policy::Error) -> Status {
    match value {
        coral_policy::Error::Workspace(inner) => super::workspaces::error(inner),
        coral_policy::Error::Provider(inner) => super::providers::error(inner),
        coral_policy::Error::InvalidBatch => invalid("invalid policy batch"),
        coral_policy::Error::ToolNotFound | coral_policy::Error::ToolPathsNotFound(_) => {
            super::workspaces::safe_status(tonic::Code::NotFound, "NOT_FOUND")
        }
        coral_policy::Error::CorruptData(_)
        | coral_policy::Error::Database(_)
        | coral_policy::Error::EventFields(_) => {
            super::workspaces::safe_status(tonic::Code::Unavailable, "UNAVAILABLE")
        }
    }
}
fn access(state: i32) -> Result<ToolAccessState, Status> {
    match wire::ToolAccessState::try_from(state) {
        Ok(wire::ToolAccessState::Enabled) => Ok(ToolAccessState::Enabled),
        Ok(wire::ToolAccessState::ApprovalRequired) => Ok(ToolAccessState::ApprovalRequired),
        Ok(wire::ToolAccessState::Disabled) => Ok(ToolAccessState::Disabled),
        _ => Err(invalid("invalid access state")),
    }
}
fn view(tool: String, decision: &coral_policy::ToolPolicyDecision) -> wire::ToolPolicy {
    wire::ToolPolicy {
        tool,
        access_state: match decision.access_state() {
            ToolAccessState::Enabled => wire::ToolAccessState::Enabled,
            ToolAccessState::ApprovalRequired => wire::ToolAccessState::ApprovalRequired,
            ToolAccessState::Disabled => wire::ToolAccessState::Disabled,
        }
        .into(),
        explicit: decision.is_explicit(),
    }
}
impl GrpcServices {
    async fn policy_targets(
        &self,
        actor: &coral_workspace::AuthenticatedUser,
        parent: &str,
        tools: &[String],
    ) -> Result<Vec<ToolPolicyTarget>, Status> {
        let mut distinct = std::collections::HashSet::new();
        if tools.is_empty() || tools.len() > 100 || tools.iter().any(|op| !distinct.insert(op)) {
            return Err(invalid("policy selection requires 1–100 distinct Tools"));
        }
        let (ws, id) = super::resource(parent, "providers")?;
        let id = ProviderId::parse(id).map_err(super::providers::error)?;
        let provider_access = self.provider_access(actor, &ws, &id).await?;
        let mut targets = Vec::with_capacity(tools.len());
        for tool in tools {
            let (provider_name, encoded) = tool
                .rsplit_once("/tools/")
                .ok_or_else(|| invalid("invalid Tool name"))?;
            let (owner, provider) = super::providers::resource(provider_name, "providers")?;
            let provider =
                coral_provider::ProviderId::parse(provider).map_err(super::providers::error)?;
            if &provider != provider_access.id() {
                return Err(invalid("Tool Provider mismatch"));
            }
            if owner != ws {
                return Err(invalid("Tool Workspace mismatch"));
            }
            let key = String::from_utf8(
                URL_SAFE_NO_PAD
                    .decode(encoded)
                    .map_err(|_invalid| invalid("invalid Tool name"))?,
            )
            .map_err(|_invalid| invalid("invalid Tool name"))?;
            if URL_SAFE_NO_PAD.encode(&key) != encoded {
                return Err(invalid("invalid Tool name"));
            }
            targets.push(ToolPolicyTarget::new(
                ws.clone(),
                id.clone(),
                coral_catalog::ToolKey::parse(&key)
                    .map_err(|_invalid| invalid("invalid Tool Key"))?,
            ));
        }
        Ok(targets)
    }
}
#[tonic::async_trait]
impl wire::tool_policy_service_server::ToolPolicyService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_tool_policies")]
    async fn get_tool_policies(
        &self,
        request: Request<wire::GetToolPoliciesRequest>,
    ) -> Result<Response<wire::GetToolPoliciesResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let req = request.into_inner();
        let targets = self.policy_targets(&actor, &req.parent, &req.tools).await?;
        let decisions = self
            .policies
            .decisions(&actor, &targets)
            .await
            .map_err(error)?;
        let policies = req
            .tools
            .into_iter()
            .zip(decisions)
            .map(|(name, decision)| view(name, &decision))
            .collect();
        Ok(Response::new(wire::GetToolPoliciesResponse { policies }))
    }
    #[tracing::instrument(skip_all, name = "coral.private.update_tool_policies")]
    async fn update_tool_policies(
        &self,
        request: Request<wire::UpdateToolPoliciesRequest>,
    ) -> Result<Response<wire::UpdateToolPoliciesResponse>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let names = req
            .policies
            .iter()
            .map(|p| p.tool.clone())
            .collect::<Vec<_>>();
        let targets = self
            .policy_targets(context.actor(), &req.parent, &names)
            .await?;
        let updates = targets
            .into_iter()
            .zip(req.policies)
            .map(|(target, policy)| Ok((target, access(policy.access_state)?)))
            .collect::<Result<Vec<_>, Status>>()?;
        let decisions = self
            .policies
            .set_access_states(&context, updates)
            .await
            .map_err(error)?;
        Ok(Response::new(wire::UpdateToolPoliciesResponse {
            policies: names
                .into_iter()
                .zip(decisions)
                .map(|(name, decision)| view(name, &decision))
                .collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use coral_policy::Error as E;
    use tonic::Code;

    /// Policy failures keep the codes and reasons of the owner they wrap. Only a
    /// storage failure is retryable.
    #[test]
    fn policy_errors_map_to_their_owners_codes_and_reasons() {
        let cases = [
            (E::InvalidBatch, Code::InvalidArgument, "INVALID_ARGUMENT"),
            (E::ToolNotFound, Code::NotFound, "NOT_FOUND"),
            (
                E::ToolPathsNotFound(vec!["repos.nope".to_owned()]),
                Code::NotFound,
                "NOT_FOUND",
            ),
            (
                E::Workspace(coral_workspace::Error::PermissionDenied("private detail")),
                Code::PermissionDenied,
                "PERMISSION_DENIED",
            ),
            (
                E::Workspace(coral_workspace::Error::WorkspaceArchived),
                Code::FailedPrecondition,
                "WORKSPACE_ARCHIVED",
            ),
            (
                E::Provider(coral_provider::Error::ProviderNotFound),
                Code::NotFound,
                "NOT_FOUND",
            ),
            (
                E::Provider(coral_provider::Error::EtagMismatch),
                Code::Aborted,
                "ETAG_MISMATCH",
            ),
            (
                E::CorruptData("private detail".into()),
                Code::Unavailable,
                "UNAVAILABLE",
            ),
        ];
        for (failure, code, reason) in cases {
            let label = format!("{failure:?}");
            let status = super::error(failure);
            assert_eq!((status.code(), status.message()), (code, reason), "{label}");
        }
    }
}
