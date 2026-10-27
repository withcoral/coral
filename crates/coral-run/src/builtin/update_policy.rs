//! Performs `coral.workspace.updatePolicy`.

use std::collections::BTreeMap;

use coral_catalog::{UpdatePolicyArguments, UpdatePolicyResult};
use coral_policy::ToolAccessState;
use coral_provider::access::ProviderNamespace;

use super::BuiltinHost;
use crate::error::InputProblem;
use crate::{CoralFailure, Error, Rejection};

/// `updatePolicy`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct UpdatePolicy {
    namespace: ProviderNamespace,
    tools: BTreeMap<String, ToolAccessState>,
}

impl UpdatePolicy {
    /// The call for arguments the contract accepted.
    pub(super) fn new(arguments: &UpdatePolicyArguments) -> Result<Self, Rejection> {
        // The contract checks the namespace with the same rule, so this does
        // not fail.
        let namespace = ProviderNamespace::parse(arguments.namespace())
            .map_err(|_error| Rejection::from(CoralFailure::InvalidInput))?;
        Ok(Self {
            namespace,
            tools: arguments
                .tools()
                .iter()
                .map(|(path, state)| (path.clone(), (*state).into()))
                .collect(),
        })
    }

    /// Sets every sent state as the invoking Owner, or none.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        // Only a User reaches updatePolicy; an Agent changes no policy.
        let Some(context) = host.context else {
            return Err(Error::InvocationRejected(
                CoralFailure::AuthorityLost.into(),
            ));
        };
        let states = host
            .policies
            .set_provider_access_states(context, host.workspace_id, &self.namespace, &self.tools)
            .await
            .map_err(|error| match error {
                coral_policy::Error::Provider(coral_provider::Error::ProviderNotFound) => {
                    Error::InvocationRejected(CoralFailure::ProviderNotFound.into())
                }
                coral_policy::Error::ToolPathsNotFound(paths) => {
                    Error::InvocationRejected(Rejection::naming(
                        CoralFailure::ToolNotFound,
                        paths
                            .into_iter()
                            .map(|path| {
                                InputProblem::new(
                                    format!("tools.{path}"),
                                    format!("no Tool of {} has this path", self.namespace.as_str()),
                                )
                            })
                            .collect(),
                    ))
                }
                coral_policy::Error::Workspace(error) if crate::fence::loses_authority(&error) => {
                    Error::InvocationRejected(CoralFailure::AuthorityLost.into())
                }
                other => other.into(),
            })?;
        Ok(UpdatePolicyResult {
            namespace: self.namespace.as_str().to_owned(),
            tools: states
                .into_iter()
                .map(|(path, state)| (path, state.into()))
                .collect(),
        }
        .into_tool_result())
    }
}
