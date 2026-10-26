//! Exact transport-safe Operation addresses, with atomic writes in the policy domain.
use super::workspaces::{change, invalid};
use super::{GrpcServices, management as wire};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use coral_policy::{OperationAccessState, OperationPolicyTarget};
use coral_provider::ProviderId;
use tonic::{Request, Response, Status};

pub(super) fn error(value: coral_policy::Error) -> Status {
    match value {
        coral_policy::Error::Workspace(inner) => super::workspaces::error(inner),
        coral_policy::Error::Provider(inner) => super::providers::error(inner),
        coral_policy::Error::InvalidBatch => invalid("invalid policy batch"),
        coral_policy::Error::OperationNotFound | coral_policy::Error::OperationPathsNotFound(_) => {
            super::workspaces::safe_status(tonic::Code::NotFound, "NOT_FOUND")
        }
        coral_policy::Error::CorruptData(_)
        | coral_policy::Error::Database(_)
        | coral_policy::Error::EventFields(_) => {
            super::workspaces::safe_status(tonic::Code::Unavailable, "UNAVAILABLE")
        }
    }
}
fn access(state: i32) -> Result<OperationAccessState, Status> {
    match wire::OperationAccessState::try_from(state) {
        Ok(wire::OperationAccessState::Enabled) => Ok(OperationAccessState::Enabled),
        Ok(wire::OperationAccessState::ApprovalRequired) => {
            Ok(OperationAccessState::ApprovalRequired)
        }
        Ok(wire::OperationAccessState::Disabled) => Ok(OperationAccessState::Disabled),
        _ => Err(invalid("invalid access state")),
    }
}
fn view(
    operation: String,
    decision: &coral_policy::OperationPolicyDecision,
) -> wire::OperationPolicy {
    wire::OperationPolicy {
        operation,
        access_state: match decision.access_state() {
            OperationAccessState::Enabled => wire::OperationAccessState::Enabled,
            OperationAccessState::ApprovalRequired => wire::OperationAccessState::ApprovalRequired,
            OperationAccessState::Disabled => wire::OperationAccessState::Disabled,
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
        operations: &[String],
    ) -> Result<Vec<OperationPolicyTarget>, Status> {
        let mut distinct = std::collections::HashSet::new();
        if operations.is_empty()
            || operations.len() > 100
            || operations.iter().any(|op| !distinct.insert(op))
        {
            return Err(invalid(
                "policy selection requires 1–100 distinct Operations",
            ));
        }
        let (ws, id) = super::resource(parent, "providers")?;
        let id = ProviderId::parse(id).map_err(super::providers::error)?;
        let provider_access = self.provider_access(actor, &ws, &id).await?;
        let mut targets = Vec::with_capacity(operations.len());
        for operation in operations {
            let (provider_name, encoded) = operation
                .rsplit_once("/operations/")
                .ok_or_else(|| invalid("invalid Operation name"))?;
            let (owner, provider) = super::providers::resource(provider_name, "providers")?;
            let provider =
                coral_provider::ProviderId::parse(provider).map_err(super::providers::error)?;
            if &provider != provider_access.id() {
                return Err(invalid("Operation Provider mismatch"));
            }
            if owner != ws {
                return Err(invalid("Operation Workspace mismatch"));
            }
            let key = String::from_utf8(
                URL_SAFE_NO_PAD
                    .decode(encoded)
                    .map_err(|_invalid| invalid("invalid Operation name"))?,
            )
            .map_err(|_invalid| invalid("invalid Operation name"))?;
            if URL_SAFE_NO_PAD.encode(&key) != encoded {
                return Err(invalid("invalid Operation name"));
            }
            targets.push(OperationPolicyTarget::new(
                ws.clone(),
                id.clone(),
                coral_catalog::OperationKey::parse(&key)
                    .map_err(|_invalid| invalid("invalid Operation Key"))?,
            ));
        }
        Ok(targets)
    }
}
#[tonic::async_trait]
impl wire::operation_policy_service_server::OperationPolicyService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_operation_policies")]
    async fn get_operation_policies(
        &self,
        request: Request<wire::GetOperationPoliciesRequest>,
    ) -> Result<Response<wire::GetOperationPoliciesResponse>, Status> {
        let actor = self.management_user(&request).await?;
        let req = request.into_inner();
        let targets = self
            .policy_targets(&actor, &req.parent, &req.operations)
            .await?;
        let decisions = self
            .policies
            .decisions(&actor, &targets)
            .await
            .map_err(error)?;
        let policies = req
            .operations
            .into_iter()
            .zip(decisions)
            .map(|(name, decision)| view(name, &decision))
            .collect();
        Ok(Response::new(wire::GetOperationPoliciesResponse {
            policies,
        }))
    }
    #[tracing::instrument(skip_all, name = "coral.private.update_operation_policies")]
    async fn update_operation_policies(
        &self,
        request: Request<wire::UpdateOperationPoliciesRequest>,
    ) -> Result<Response<wire::UpdateOperationPoliciesResponse>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let names = req
            .policies
            .iter()
            .map(|p| p.operation.clone())
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
        Ok(Response::new(wire::UpdateOperationPoliciesResponse {
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
            (E::OperationNotFound, Code::NotFound, "NOT_FOUND"),
            (
                E::OperationPathsNotFound(vec!["repos.nope".to_owned()]),
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
