//! The Sub-agent a launching binary registers: what a Program reaches through
//! `coral.agent` and `coral.cancelAgent`.

use std::sync::Arc;

use coral_mcp::McpModules;
use coral_run::SubAgentTools;

/// Builds the Sub-agent's Tools, once, at MCP startup. The launching process
/// registers one in [`Launch`](super::Launch). A process that registers none
/// serves no Workspace the two Tools.
pub trait SubAgentToolsFactory: Send + Sync {
    /// Builds the Tools.
    ///
    /// # Errors
    /// Returns why the Sub-agent cannot start.
    fn build(
        &self,
        dependencies: SubAgentDependencies,
    ) -> Result<Arc<dyn SubAgentTools>, Box<dyn std::error::Error + Send + Sync>>;
}

/// What bootstrap hands a [`SubAgentToolsFactory`].
pub struct SubAgentDependencies {
    /// The database of this Coral Instance.
    pub database: Arc<coral_db::Database>,
    /// The model that the Organization's Sub-agents use.
    pub agent_models: coral_agent_model::AgentModels,
    /// The Task, Run and search modules that MCP serves over.
    pub modules: McpModules,
    /// The operator's destination policy, for `coral_outbound::Client`.
    pub outbound_policy: coral_outbound::DestinationPolicy,
    /// The one clock of this Coral Instance.
    pub clock: Arc<dyn coral_db::Clock>,
    /// True once this Coral Instance begins to shut down, before the Shutdown
    /// Drain. A dropped sender reads the same.
    pub stopping: tokio::sync::watch::Receiver<bool>,
}
