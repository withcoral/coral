//! Performs `coral.agent` and `coral.cancelAgent` through [`SubAgentTools`],
//! which the Sub-agent's crate implements: coral-run cannot depend on it.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use coral_catalog::{AgentArguments, AgentResult, CancelAgentArguments};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use super::BuiltinHost;
use crate::{CoralFailure, Error, Rejection, RunId};

/// What `coral.agent` and `coral.cancelAgent` act through.
#[async_trait]
pub trait SubAgentTools: Send + Sync {
    /// Starts a Sub-agent in the Task of the calling Run, messages one or
    /// waits on one, then reads its Turn.
    async fn agent(
        &self,
        caller: SubAgentCaller<'_>,
        arguments: &AgentArguments,
    ) -> Result<AgentResult, SubAgentError>;

    /// Cancels a Sub-agent's Turn and its unfinished Runs, then reads the Turn.
    async fn cancel(
        &self,
        caller: SubAgentCaller<'_>,
        arguments: &CancelAgentArguments,
    ) -> Result<AgentResult, SubAgentError>;
}

/// The Run that calls a Sub-agent Tool, and the Principal it runs as.
pub struct SubAgentCaller<'a> {
    /// The invoking Principal.
    pub actor: &'a AuthenticatedPrincipal,
    /// The Run's Workspace.
    pub workspace_id: &'a WorkspaceId,
    /// The calling Run. Its Task is the Sub-agent's.
    pub run_id: &'a RunId,
    /// Completes when the Run is asked to stop, with why.
    pub stopped: Pin<Box<dyn Future<Output = RunStop> + Send + 'a>>,
}

/// Why the Run that calls a Sub-agent Tool was asked to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStop {
    /// Someone canceled the Run.
    Canceled,
    /// Its deadline, its end, or the shutdown of its Coral Instance.
    Ended,
}

/// Why a Sub-agent Tool did nothing.
#[derive(Debug)]
pub enum SubAgentError {
    /// The Program must correct the argument `field`, as `reason` says.
    InvalidInput {
        /// The argument's name.
        field: &'static str,
        /// What to correct. It holds no submitted value.
        reason: &'static str,
    },
    /// The call cannot run here, and the text tells the Program why.
    Unavailable(&'static str),
    /// A host defect, which stops the Run.
    Host(Error),
}

/// Where bootstrap puts the Sub-agent's Tools, once. The Sub-agent holds the
/// Run module that holds this, so the two live as long as the process.
pub(crate) type SubAgentSlot = Arc<OnceLock<Arc<dyn SubAgentTools>>>;

/// `coral.agent` or `coral.cancelAgent`, with the arguments the contract accepted.
#[derive(Clone)]
pub(crate) enum SubAgentCall {
    Agent(AgentArguments),
    Cancel(CancelAgentArguments),
}

impl SubAgentCall {
    /// Performs the call as the invoking Principal. A deployment whose
    /// Sub-agent is not wired fails it as `tool_unavailable`.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let unavailable = || Error::InvocationRejected(CoralFailure::ToolUnavailable.into());
        let tools = host.sub_agents.ok_or_else(unavailable)?;
        let caller = SubAgentCaller {
            actor: host.actor,
            workspace_id: host.workspace_id,
            run_id: host.run_id,
            stopped: Box::pin(async {
                host.run.stopped().await;
                if host.run.canceled() {
                    RunStop::Canceled
                } else {
                    RunStop::Ended
                }
            }),
        };
        let result = match self {
            Self::Agent(arguments) => tools.agent(caller, arguments).await,
            Self::Cancel(arguments) => tools.cancel(caller, arguments).await,
        };
        result.map(AgentResult::into_tool_result).map_err(|error| {
            Error::InvocationRejected(match error {
                SubAgentError::InvalidInput { field, reason } => {
                    Rejection::invalid_input(field, reason)
                }
                SubAgentError::Unavailable(reason) => Rejection::naming(
                    CoralFailure::ToolUnavailable,
                    vec![crate::error::InputProblem::new("", reason)],
                ),
                SubAgentError::Host(error) => return error,
            })
        })
    }
}
