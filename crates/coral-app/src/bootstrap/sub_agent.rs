//! The Sub-agent a launching binary registers: what a Program reaches through
//! `coral.agent` and `coral.cancelAgent`.

use std::sync::Arc;

use coral_mcp::McpModules;
use coral_run::SubAgentTools;

use super::runtime::McpStartError;
use crate::config_toml::{ConfigDecodeError, Document};

/// Builds the Sub-agent's Tools, once, at MCP startup. The launching process
/// registers one in [`Launch`](super::Launch).
pub trait SubAgentToolsFactory: Send + Sync {
    /// The name of the top-level section of the config file that this
    /// factory reads. The section turns the Sub-agent on: without it, the
    /// catalog lacks the two Tools and core never calls [`Self::build`].
    fn section_name(&self) -> &'static str;

    /// Builds the Tools from the section, undecoded.
    ///
    /// # Errors
    /// Returns why the Sub-agent cannot start, never a value the section holds.
    fn build(
        &self,
        section: &toml::Table,
        dependencies: SubAgentDependencies,
    ) -> Result<Arc<dyn SubAgentTools>, Box<dyn std::error::Error + Send + Sync>>;
}

/// What bootstrap hands a [`SubAgentToolsFactory`] beside its section.
pub struct SubAgentDependencies {
    /// The database of this Coral Instance.
    pub database: Arc<coral_db::Database>,
    /// The Task, Run and search modules that MCP serves over.
    pub modules: McpModules,
    /// The operator's destination policy, for `coral_outbound::Client`.
    pub outbound_policy: coral_outbound::DestinationPolicy,
    /// Reads one variable of the process environment, which bootstrap owns.
    pub env: coral_secret_store::EnvReader,
    /// The one clock of this Coral Instance.
    pub clock: Arc<dyn coral_db::Clock>,
    /// True once this Coral Instance begins to shut down, before the Shutdown
    /// Drain. A dropped sender reads the same.
    pub stopping: tokio::sync::watch::Receiver<bool>,
}

/// A registered factory with its section, which the config file has.
#[derive(Clone)]
pub(super) struct ConfiguredSubAgent {
    factory: Arc<dyn SubAgentToolsFactory>,
    section: toml::Table,
}

impl ConfiguredSubAgent {
    /// Takes the section that `factory` claims out of `document`. None when
    /// the file lacks it.
    pub(super) fn take(
        factory: &Arc<dyn SubAgentToolsFactory>,
        document: &mut Document<'_>,
    ) -> Result<Option<Self>, ConfigDecodeError> {
        let section = document.take_section(factory.section_name())?;
        Ok(section.map(|section| Self {
            factory: Arc::clone(factory),
            section,
        }))
    }

    /// Calls the factory with its section.
    pub(super) fn build(
        &self,
        dependencies: SubAgentDependencies,
    ) -> Result<Arc<dyn SubAgentTools>, McpStartError> {
        self.factory
            .build(&self.section, dependencies)
            .map_err(|error| McpStartError::SubAgent {
                section_name: self.factory.section_name(),
                error,
            })
    }
}
