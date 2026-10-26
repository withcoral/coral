//! Performs `coral.workspace.getPolicy`.

use coral_catalog::{GetPolicyArguments, GetPolicyResult};
use coral_provider::access::ProviderNamespace;

use super::BuiltinHost;
use crate::{CoralFailure, Error, Rejection};

/// `getPolicy`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct GetPolicy {
    namespace: ProviderNamespace,
}

impl GetPolicy {
    /// The call for arguments the contract accepted.
    pub(super) fn new(arguments: &GetPolicyArguments) -> Result<Self, Rejection> {
        // The contract checks the namespace with the same rule, so this does
        // not fail.
        let namespace = ProviderNamespace::parse(arguments.namespace())
            .map_err(|_error| Rejection::from(CoralFailure::InvalidInput))?;
        Ok(Self { namespace })
    }

    /// Reads the Provider's exact rules as the invoking Principal.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let rules = host
            .policies
            .provider_rules(
                host.actor,
                host.workspace_id,
                &self.namespace,
                host.run.now()?,
            )
            .await
            .map_err(|error| match error {
                coral_policy::Error::Provider(coral_provider::Error::ProviderNotFound) => {
                    Error::InvocationRejected(CoralFailure::ProviderNotFound.into())
                }
                coral_policy::Error::Workspace(error) if crate::fence::loses_authority(&error) => {
                    Error::InvocationRejected(CoralFailure::AuthorityLost.into())
                }
                other => other.into(),
            })?;
        Ok(GetPolicyResult {
            namespace: self.namespace.as_str().to_owned(),
            rules: rules
                .into_iter()
                .map(|(path, state)| (path, state.into()))
                .collect(),
        }
        .into_operation_result())
    }
}
