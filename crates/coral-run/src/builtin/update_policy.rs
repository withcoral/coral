//! Performs `coral.updatePolicy`.

use std::collections::BTreeMap;

use coral_catalog::{UpdatePolicyArguments, UpdatePolicyResult};
use coral_policy::ToolAccessState;

use super::BuiltinHost;
use crate::error::InputProblem;
use crate::{CoralFailure, Error, Rejection};

/// `updatePolicy`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct UpdatePolicy {
    tools: BTreeMap<String, ToolAccessState>,
}

impl UpdatePolicy {
    /// The call for arguments the contract accepted.
    pub(super) fn new(arguments: &UpdatePolicyArguments) -> Self {
        Self {
            tools: arguments.tools().clone(),
        }
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
            .set_access_states_by_call_path(context, host.workspace_id, &self.tools)
            .await
            .map_err(|error| match error {
                coral_policy::Error::RefusedToolPaths { missing, builtin } => {
                    Error::InvocationRejected(refused_paths(missing, builtin))
                }
                coral_policy::Error::Workspace(error)
                    if crate::enforcement::loses_authority(&error) =>
                {
                    Error::InvocationRejected(CoralFailure::AuthorityLost.into())
                }
                other => other.into(),
            })?;
        Ok(UpdatePolicyResult { tools: states }.into_tool_result())
    }
}

/// The rejection that names every refused path, in path order, each with
/// its own reason. A path that names no Tool makes it `tool_not_found`;
/// only Built-in paths make it `invalid_input`.
fn refused_paths(missing: Vec<String>, builtin: Vec<String>) -> Rejection {
    let failure = if missing.is_empty() {
        CoralFailure::InvalidInput
    } else {
        CoralFailure::ToolNotFound
    };
    let mut problems = missing
        .into_iter()
        .map(|path| (path, "no Tool you can set has this Call Path"))
        .chain(
            builtin
                .into_iter()
                .map(|path| (path, "set a Built-in Tool's policy in the Coral UI")),
        )
        .collect::<Vec<_>>();
    problems.sort_unstable();
    Rejection::naming(
        failure,
        problems
            .into_iter()
            .map(|(path, reason)| InputProblem::new(format!("tools.{path}"), reason))
            .collect(),
    )
}
