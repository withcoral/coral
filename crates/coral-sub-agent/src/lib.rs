//! The Sub-agent (ADR 0040): a model that Coral runs to do data work for the
//! agent that calls it, the lead. The lead reaches it through one more MCP
//! tool, `agent`, while the config file has a `[sub_agent]` section.

pub mod model;

use std::error::Error;
use std::sync::Arc;

use coral_app::{McpInterfaceDependencies, McpInterfaceFactory};
use coral_mcp::McpInterface;
use coral_secret_store::{BuildEnvironment, SettingError, StoreSettings, env_var_name};
use serde::Deserialize;
use url::Url;

use model::ModelClient;

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
        section.model_client(&dependencies)?;
        Ok(dependencies.modules.core_interface())
    }
}

/// The fewest tokens a model's context window may hold.
const CONTEXT_WINDOW_MIN: u32 = 48_000;

/// The `[sub_agent]` section, as the operator wrote it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Section {
    base_url: String,
    model: String,
    api_key_env: Option<String>,
    context_window: Option<u32>,
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
        let policy = dependencies.outbound_policy;
        let environment = BuildEnvironment {
            policy,
            env: &*dependencies.env,
        };
        let api_key = match &self.api_key_env {
            Some(name) => {
                let name = env_var_name("api_key_env", name)?;
                Some(environment.secret("api_key_env", &name)?)
            }
            None => None,
        };
        Ok(ModelClient::new(policy, base_url, self.model, api_key))
    }
}
