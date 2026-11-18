//! The tool surface a launching binary registers for MCP to serve in place
//! of core's.

use std::sync::Arc;

use coral_mcp::{McpInterface, McpModules};

use super::runtime::McpStartError;
use crate::config_toml::{ConfigDecodeError, Document};

/// Builds the tool surface that MCP serves in place of core's, once, at MCP
/// startup. The launching process registers one in [`Launch`](super::Launch).
pub trait McpInterfaceFactory: Send + Sync {
    /// The name of the top-level section of the config file that this
    /// factory reads. The section turns the surface on: without it, core
    /// never calls [`Self::build`].
    fn section_name(&self) -> &'static str;

    /// Builds the surface from its section, undecoded. To keep core's tools,
    /// wrap [`McpModules::core_interface`].
    ///
    /// # Errors
    /// Returns why the surface cannot start, never a value the section holds.
    fn build(
        &self,
        section: &toml::Table,
        dependencies: McpInterfaceDependencies,
    ) -> Result<Arc<dyn McpInterface>, Box<dyn std::error::Error + Send + Sync>>;
}

/// What bootstrap hands an [`McpInterfaceFactory`] beside its section.
pub struct McpInterfaceDependencies {
    /// The database of this Coral Instance.
    pub database: Arc<coral_db::Database>,
    /// The Workspace, Task, Run and search modules that MCP serves over.
    pub modules: McpModules,
    /// The operator's destination policy, for `coral_outbound::Client`.
    pub outbound_policy: coral_outbound::DestinationPolicy,
    /// Reads one variable of the process environment, which bootstrap owns.
    pub env: coral_secret_store::EnvReader,
    /// The one clock of this Coral Instance.
    pub clock: Arc<dyn coral_db::Clock>,
    /// True once this Coral Instance stops its listeners, which is after the
    /// Shutdown Drain. A dropped sender reads the same.
    pub stopping: tokio::sync::watch::Receiver<bool>,
}

/// A registered factory with its section, which the config file has.
#[derive(Clone)]
pub(super) struct ConfiguredMcpInterface {
    factory: Arc<dyn McpInterfaceFactory>,
    section: toml::Table,
}

impl ConfiguredMcpInterface {
    /// Takes the section that `factory` claims out of `document`. None when
    /// the file lacks it.
    pub(super) fn take(
        factory: &Arc<dyn McpInterfaceFactory>,
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
        dependencies: McpInterfaceDependencies,
    ) -> Result<Arc<dyn McpInterface>, McpStartError> {
        self.factory
            .build(&self.section, dependencies)
            .map_err(|error| McpStartError::Interface {
                section_name: self.factory.section_name(),
                error,
            })
    }
}
