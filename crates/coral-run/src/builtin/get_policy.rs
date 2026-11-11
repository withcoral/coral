//! Performs `coral.getPolicy`.

use coral_catalog::{GetPolicyArguments, GetPolicyResult};

use super::BuiltinHost;
use crate::{CoralFailure, Error};

/// `getPolicy`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct GetPolicy {
    under: Option<String>,
}

impl GetPolicy {
    /// The call for arguments the contract accepted.
    pub(super) fn new(arguments: &GetPolicyArguments) -> Self {
        Self {
            under: arguments.under().map(ToOwned::to_owned),
        }
    }

    /// Reads the exact rules that apply to the Tools the invoking Principal
    /// can read, by complete Call Path.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let rules = host
            .policies
            .rules(
                host.actor,
                host.workspace_id,
                self.under.as_deref(),
                host.run.now()?,
            )
            .await
            .map_err(|error| match error {
                coral_policy::Error::Workspace(error)
                    if crate::enforcement::loses_authority(&error) =>
                {
                    Error::InvocationRejected(CoralFailure::AuthorityLost.into())
                }
                other => other.into(),
            })?;
        Ok(GetPolicyResult { rules }.into_tool_result())
    }
}
