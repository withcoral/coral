//! `coral.agent` and `coral.cancelAgent` reach the Sub-agent that bootstrap
//! wires, over the real V8 engine. coral-agent tests the Sub-agent's rules.
use super::provider_setup::{AdminScope, final_value};
use super::*;
use crate::{SubAgentCaller, SubAgentError};
use coral_catalog::{AgentArguments, AgentResult, CancelAgentArguments};
use serde_json::json;

/// A Sub-agent that answers a brief with its message, names the calling Run
/// as the `sub_agent_id`, and refuses every other call.
struct Echo;

#[async_trait]
impl crate::SubAgentTools for Echo {
    async fn agent(
        &self,
        caller: SubAgentCaller<'_>,
        arguments: &AgentArguments,
    ) -> Result<AgentResult, SubAgentError> {
        let AgentArguments::Start { message } = arguments else {
            let (field, reason) = ("sub_agent_id", "names no agent of yours");
            return Err(SubAgentError::InvalidInput { field, reason });
        };
        Ok(AgentResult {
            sub_agent_id: caller.run_id.to_string(),
            state: "completed",
            answer: Some(message.clone()),
            failure: None,
            required_actions: Vec::new(),
            outcome: None,
            steps: Vec::new(),
            next_cursor: String::new(),
        })
    }

    async fn cancel(
        &self,
        _caller: SubAgentCaller<'_>,
        _arguments: &CancelAgentArguments,
    ) -> Result<AgentResult, SubAgentError> {
        Err(SubAgentError::Unavailable("an agent started this run"))
    }
}

// Catches: a Sub-agent Tool that runs where none is wired, a call that reaches
// the Sub-agent without its Run, and a refusal that the Program cannot read.
#[tokio::test]
async fn programs_reach_the_wired_sub_agent_and_read_its_refusals() {
    let scope = AdminScope::new("user:sub-agent").await;
    // A process whose deployment configures a Sub-agent updates the Workspace.
    let configured = ProviderModule::new(
        DestinationPolicy::default(),
        scope.fixture.database.clone(),
        scope.fixture.workspaces.clone(),
        coral_catalog::BuiltinCatalog::with_sub_agent().unwrap(),
        coral_catalog::SchemaGraphCache::with_default_budget(),
    );
    let [outdated] = &configured.outdated_builtin_generations().await.unwrap()[..] else {
        panic!("the Workspace pins the catalog without a Sub-agent");
    };
    let updated = configured.update_builtin_catalog(outdated, coral_db::UnixNanos::new(20));
    assert!(updated.await.unwrap().skip_search());
    let agent = "coral.agent({ message: 'Count' })";
    let unwired = ("tool_unavailable", "Tool is unavailable");
    let unknown = "Tool arguments are invalid at `sub_agent_id`: names no agent of yours";
    let nested = "Tool is unavailable: an agent started this run";

    for (wired, call, refusal) in [
        (false, agent, Some(unwired)),
        (true, agent, None),
        (
            true,
            "coral.agent({ sub_agent_id: 'a1' })",
            Some(("invalid_input", unknown)),
        ),
        (
            true,
            "coral.cancelAgent({ sub_agent_id: 'a1' })",
            Some(("tool_unavailable", nested)),
        ),
    ] {
        if wired {
            scope.fixture.runs.set_sub_agent_tools(Arc::new(Echo));
        }
        let program = format!(
            "try {{ return (await {call}).data; }} catch (error) {{ return [error.code, error.message]; }}"
        );
        let run = scope.start(&scope.owner, submission(&program)).await;
        let value = final_value(&scope.terminal(&scope.owner, &run).await);
        let expected = match refusal {
            Some((code, message)) => json!([code, message]),
            None => {
                json!({"sub_agent_id": run.to_string(), "state": "completed", "answer": "Count", "steps": [], "next_cursor": ""})
            }
        };
        assert_eq!(value, expected, "{call}");
    }
}
