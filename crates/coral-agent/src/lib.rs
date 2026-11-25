//! The Sub-agent (ADR 0040): a model that Coral runs to do data work for the
//! agent that calls it, the lead. A Program of the lead reaches it through
//! `coral.agent`, while the Organization has a model and the Workspace's Owner
//! turned the Tool on.

mod model;
mod organization_model;
mod prompt;
mod repository;
mod run_tools;
mod search;
mod sub_agents;
mod telemetry;
mod tools;

#[cfg(test)]
mod tests;

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use coral_app::{SubAgentDependencies, SubAgentToolsFactory};
use coral_run::SubAgentTools;

use organization_model::{OrganizationModel, Tester};
use sub_agents::SubAgents;
use tools::AgentTools;

/// Builds what `coral.agent` and `coral.cancelAgent` reach.
pub struct SubAgentFactory;

impl SubAgentToolsFactory for SubAgentFactory {
    fn build(
        &self,
        dependencies: SubAgentDependencies,
    ) -> Result<Arc<dyn SubAgentTools>, Box<dyn Error + Send + Sync>> {
        let models = OrganizationModel {
            database: dependencies.database.clone(),
            models: dependencies.agent_models,
            http: coral_outbound::Client::new(dependencies.outbound_policy, MODEL_CALL_TIMEOUT)?,
        };
        let tester = Tester {
            http: coral_outbound::Client::new(dependencies.outbound_policy, TEST_TIMEOUT)?,
        };
        models.models.set_tester(Arc::new(tester));
        let human_interface = dependencies.modules.human_interface().clone();
        let modules = dependencies.modules;
        let tasks = modules.tasks;
        let sub_agents = SubAgents::new(
            dependencies.database.clone(),
            tasks.clone(),
            modules.commands,
            modules.runs,
            modules.search,
            Arc::new(models),
            dependencies.clock,
            dependencies.stopping,
        );
        Ok(Arc::new(AgentTools {
            database: dependencies.database,
            tasks,
            sub_agents,
            human_interface,
        }))
    }
}

/// How long the model may take over one attempt at a reply.
const MODEL_CALL_TIMEOUT: Duration = Duration::from_secs(300);
/// How long a test waits for the model. A person waits for it on a page.
const TEST_TIMEOUT: Duration = Duration::from_secs(45);
