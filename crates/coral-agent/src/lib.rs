//! The Sub-agent (ADR 0040): a model that Coral runs to do data work for the
//! agent that calls it, the lead. The lead reaches it through one more MCP
//! tool, `agent`, while the config file has a `[sub_agent]` section.

mod lead_text;
mod model;
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

use coral_app::{McpInterfaceDependencies, McpInterfaceFactory};
use coral_mcp::McpInterface;
use coral_secret_store::{BuildEnvironment, SettingError, StoreSettings, env_var_name};
use serde::Deserialize;
use url::Url;

use model::{ModelClient, Protocol};
use sub_agents::SubAgents;
use tools::CoreWithAgent;

/// Builds core's MCP tools with the `agent` tool beside them, from the
/// `[sub_agent]` section of the config file.
pub struct SubAgentFactory;

impl McpInterfaceFactory for SubAgentFactory {
    fn section_name(&self) -> &'static str {
        "sub_agent"
    }

    fn build(
        &self,
        section: &toml::Table,
        dependencies: McpInterfaceDependencies,
    ) -> Result<Arc<dyn McpInterface>, Box<dyn Error + Send + Sync>> {
        let section: Section = StoreSettings::new(section.clone()).decode()?;
        let model = section.model_client(&dependencies)?;
        let core = dependencies.modules.core_interface();
        let human_interface = dependencies.modules.human_interface().clone();
        let modules = dependencies.modules;
        let tasks = modules.tasks;
        let sub_agents = SubAgents::new(
            dependencies.database,
            tasks.clone(),
            modules.commands,
            modules.runs,
            modules.search,
            model,
            dependencies.clock,
            dependencies.stopping,
        );
        Ok(Arc::new(CoreWithAgent::new(
            core,
            tasks,
            sub_agents,
            human_interface,
        )))
    }
}

/// The fewest tokens a model's context window may hold.
const CONTEXT_WINDOW_MIN: u32 = 48_000;
/// How long the model may take over one attempt at a reply.
const MODEL_CALL_TIMEOUT: Duration = Duration::from_secs(300);

/// The `[sub_agent]` section, as the operator wrote it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Section {
    #[serde(default)]
    protocol: Protocol,
    base_url: String,
    model: String,
    api_key_env: Option<String>,
    context_window: Option<u32>,
    reasoning_effort: Option<String>,
}

impl Section {
    /// Checks each setting and reads the token. It sends the model nothing.
    fn model_client(
        self,
        dependencies: &McpInterfaceDependencies,
    ) -> Result<ModelClient, SettingError> {
        let base_url = Url::parse(&self.base_url)
            .ok()
            .filter(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
            .filter(|url| url.query().is_none() && url.fragment().is_none())
            .ok_or_else(|| {
                let reason = "must be an absolute http or https URL without a query or fragment";
                SettingError::new("base_url", reason)
            })?;
        if self.model.trim().is_empty() {
            return Err(SettingError::new("model", "must not be empty"));
        }
        if self
            .context_window
            .is_some_and(|tokens| tokens < CONTEXT_WINDOW_MIN)
        {
            let reason = format!("must be at least {CONTEXT_WINDOW_MIN}");
            return Err(SettingError::new("context_window", reason));
        }
        if self
            .reasoning_effort
            .as_deref()
            .is_some_and(|effort| effort.trim().is_empty())
        {
            return Err(SettingError::new("reasoning_effort", "must not be empty"));
        }
        let policy = dependencies.outbound_policy;
        let environment = BuildEnvironment::new(
            policy,
            Arc::clone(&dependencies.env),
            Arc::clone(&dependencies.clock),
        );
        let api_key = match &self.api_key_env {
            Some(name) => {
                let name = env_var_name("api_key_env", name)?;
                Some(environment.secret("api_key_env", &name)?)
            }
            None => None,
        };
        let http = coral_outbound::Client::new(policy, MODEL_CALL_TIMEOUT)
            .map_err(|_error| SettingError::new("", "model client initialization failed"))?;
        let (window, effort) = (self.context_window, self.reasoning_effort);
        Ok(ModelClient::new(
            http,
            self.protocol,
            &base_url,
            self.model,
            api_key,
            window,
            effort,
        ))
    }
}
