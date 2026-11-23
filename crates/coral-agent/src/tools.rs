//! What a Program reaches through `coral.agent` and `coral.cancelAgent`: the
//! lead's Sub-agents, and each Turn as the lead reads it.

use std::sync::Arc;
use std::time::Duration;

use coral_catalog::{
    AgentArguments, AgentRequiredAction, AgentResult, AgentStep, CancelAgentArguments,
};
use coral_db::Database;
use coral_run::{
    HumanInterfaceUrl, RequiredActionRequest, RequiredActor, RunId, SubAgentCaller, SubAgentError,
    SubAgentTools,
};
use coral_task::TaskModule;
use coral_workspace::WorkspaceId;

use crate::repository;
use crate::run_tools::Outcome;
use crate::sub_agents::{Error, SubAgents, TurnPage, TurnState};

/// How long `coral.agent` waits for a Turn before it returns it as running.
const AGENT_WAIT: Duration = Duration::from_secs(600);

/// The Sub-agent as a Program of the lead calls it.
pub(crate) struct AgentTools {
    pub(crate) database: Arc<Database>,
    pub(crate) tasks: TaskModule,
    pub(crate) sub_agents: SubAgents,
    /// Where a required action's page is.
    pub(crate) human_interface: HumanInterfaceUrl,
}

#[async_trait::async_trait]
impl SubAgentTools for AgentTools {
    async fn agent(
        &self,
        caller: SubAgentCaller<'_>,
        arguments: &AgentArguments,
    ) -> Result<AgentResult, SubAgentError> {
        let (actor, workspace_id) = (caller.actor, caller.workspace_id);
        self.refuse_a_sub_agent(workspace_id, caller.run_id).await?;
        let sub_agents = &self.sub_agents;
        let (sub_agent_id, cursor) = match arguments {
            AgentArguments::Start { message } => {
                let task_id = self.tasks.run_task(workspace_id, caller.run_id).await;
                let task_id = task_id.map_err(Error::from)?;
                let task_id = task_id.ok_or(SubAgentError::Unavailable(NO_TASK))?;
                let started = sub_agents.start(actor, workspace_id, &task_id, message.clone());
                (started.await?, None)
            }
            AgentArguments::Continue {
                sub_agent_id,
                message: Some(message),
                ..
            } => {
                // The message can start a new Turn, which no earlier cursor names.
                let messaged = sub_agents.message(actor, workspace_id, sub_agent_id, message);
                messaged.await?;
                (sub_agent_id.clone(), None)
            }
            AgentArguments::Continue {
                sub_agent_id,
                cursor,
                ..
            } => (sub_agent_id.clone(), cursor.as_deref()),
        };
        let waited = sub_agents.wait(actor, workspace_id, &sub_agent_id, cursor, AGENT_WAIT);
        let page = tokio::select! {
            page = waited => page?,
            // The Turn goes on. The Run reads it as it stands.
            () = caller.stopped => {
                let page = sub_agents.page(actor, workspace_id, &sub_agent_id).await?;
                page.seen(cursor)?;
                page
            }
        };
        Ok(self.result(&sub_agent_id, &page))
    }

    async fn cancel(
        &self,
        caller: SubAgentCaller<'_>,
        arguments: &CancelAgentArguments,
    ) -> Result<AgentResult, SubAgentError> {
        let (actor, workspace_id) = (caller.actor, caller.workspace_id);
        self.refuse_a_sub_agent(workspace_id, caller.run_id).await?;
        let sub_agent_id = &arguments.sub_agent_id;
        let canceled = self.sub_agents.cancel(actor, workspace_id, sub_agent_id);
        Ok(self.result(sub_agent_id, &canceled.await?))
    }
}

const NO_TASK: &str = "this run is in no task";
const STARTED_BY_AGENT: &str = "an agent started this run, and an agent cannot call an agent";

impl AgentTools {
    /// Refuses a Run that a Sub-agent's `exec` started, from the record of it.
    async fn refuse_a_sub_agent(
        &self,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<(), SubAgentError> {
        let started = repository::started_by_sub_agent(&*self.database, workspace_id, run_id);
        match started.await {
            Ok(false) => Ok(()),
            Ok(true) => Err(SubAgentError::Unavailable(STARTED_BY_AGENT)),
            Err(error) => Err(SubAgentError::Host(error.into())),
        }
    }

    /// The Turn as the lead reads it. A running Turn whose Runs wait for a
    /// person is `needs_action`.
    fn result(&self, sub_agent_id: &str, page: &TurnPage) -> AgentResult {
        let (state, answer, failure) = match &page.state {
            TurnState::Running if page.required_actions.is_empty() => ("running", None, None),
            TurnState::Running => ("needs_action", None, None),
            TurnState::Completed { answer } => ("completed", Some(answer.clone()), None),
            TurnState::Failed { failure } => ("failed", None, Some(failure.clone())),
            TurnState::Canceled => ("canceled", None, None),
            TurnState::Interrupted => ("interrupted", None, None),
        };
        let steps = page.steps.iter().map(|step| AgentStep {
            intent: step.intent.clone(),
            run_id: step.run_id.as_ref().map(ToString::to_string),
        });
        // The fields of core's `required_actions`, and the Run's `run_id`.
        let actions = page.required_actions.iter().map(|waiting| {
            let action = &waiting.action;
            let (kind, request_id) = match &action.request {
                RequiredActionRequest::AccountConnection(id) => {
                    ("account_connection", id.to_string())
                }
                RequiredActionRequest::InvocationApproval(id) => {
                    ("invocation_approval", id.to_string())
                }
            };
            let deadline_at = chrono::DateTime::from_timestamp_nanos(action.deadline_at_unix_nanos);
            AgentRequiredAction {
                run_id: waiting.run_id.to_string(),
                kind,
                request_id,
                required_actor: match action.required_actor {
                    RequiredActor::InvokingUser => "invoking_user",
                    RequiredActor::WorkspaceOwner => "workspace_owner",
                },
                tool: action.call_path.clone(),
                provider: action.provider_label.clone(),
                deadline_at: deadline_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                url: self.human_interface.page_url(&action.action_path),
            }
        });
        AgentResult {
            sub_agent_id: sub_agent_id.to_owned(),
            state,
            answer,
            failure,
            required_actions: actions.collect(),
            outcome: page.outcome.map(|outcome| match outcome {
                Outcome::Success => "success",
                Outcome::Failure => "failure",
            }),
            steps: steps.collect(),
            next_cursor: page.cursor(),
        }
    }
}

/// The failure that a Program reads when a Sub-agent call did nothing. A
/// database failure is the host's, and stops the Run.
impl From<Error> for SubAgentError {
    fn from(error: Error) -> Self {
        let corrupt = || coral_run::Error::CorruptData("Sub-agent data".to_owned());
        match error {
            Error::SubAgentNotFound => Self::InvalidInput {
                field: "sub_agent_id",
                reason: "names no agent that you started",
            },
            Error::CursorOfAnotherTurn => Self::InvalidInput {
                field: "cursor",
                reason: "belongs to another turn of the agent",
            },
            Error::Task(error) => match error.into_database_error() {
                Ok(database) => Self::Host(database.into()),
                Err(_refused) => Self::Unavailable("you can no longer use the task of this run"),
            },
            Error::Run(error) => Self::Host(error),
            Error::Database(error) => Self::Host(error.into()),
            Error::IdentifierIssuance | Error::CorruptData => Self::Host(corrupt()),
        }
    }
}
