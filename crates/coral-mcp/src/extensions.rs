//! Static MCP extension composition and per-session tool context.

use std::{collections::BTreeSet, sync::Arc};

use coral_api::v1::Workspace;
use rmcp::handler::server::router::tool::ToolRouter;

use crate::{server::CoralToolset, surface::ToolName};

/// RMCP router used for tools supplied by an MCP extension provider.
pub type McpToolRouter = ToolRouter<McpToolContext>;

/// Session-bound context supplied to extension tool handlers.
#[derive(Clone)]
pub struct McpToolContext {
    core_tools: CoralToolset,
    workspace: Workspace,
}

impl McpToolContext {
    pub(crate) fn new(core_tools: CoralToolset, workspace: Workspace) -> Self {
        Self {
            core_tools,
            workspace,
        }
    }

    /// Core Coral tools bound to this authenticated MCP session.
    #[must_use]
    pub fn core_tools(&self) -> &CoralToolset {
        &self.core_tools
    }

    /// Workspace selected for this MCP session.
    #[must_use]
    pub fn workspace(&self) -> &Workspace {
        &self.workspace
    }
}

/// Supplies static MCP contributions when an MCP runtime starts.
pub trait McpExtensionsProvider: Send + Sync {
    /// Returns this provider's static MCP contributions.
    fn extensions(&self) -> McpExtensions;
}

/// Static tools and public tool-name projection supplied by one or more hosts.
#[derive(Clone, Debug, Default)]
pub struct McpExtensions(Arc<McpExtensionsInner>);

#[derive(Clone, Debug, Default)]
struct McpExtensionsInner {
    added_tools: McpToolRouter,
    retained_tool_names: Option<BTreeSet<String>>,
    duplicate_tool_names: BTreeSet<String>,
}

impl McpExtensions {
    /// Adds extension tool routes.
    #[must_use]
    pub fn add_tools(mut self, tools: McpToolRouter) -> Self {
        let extensions = Arc::make_mut(&mut self.0);
        for route in tools {
            let name = route.name().to_string();
            if extensions.added_tools.has_route(&name) {
                extensions.duplicate_tool_names.insert(name);
            } else {
                extensions.added_tools.add_route(route);
            }
        }
        self
    }

    /// Retains only the named tools in the public MCP surface.
    ///
    /// Repeated calls intersect their name sets. Core tool projections obtained
    /// through [`McpToolContext::core_tools`] are not affected.
    #[must_use]
    pub fn retain_tools(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let names = names.into_iter().map(Into::into).collect::<BTreeSet<_>>();
        let retained_tool_names = &mut Arc::make_mut(&mut self.0).retained_tool_names;
        match retained_tool_names {
            Some(retained) => retained.retain(|name| names.contains(name)),
            None => *retained_tool_names = Some(names),
        }
        self
    }

    /// Evaluates and deterministically merges static provider contributions.
    ///
    /// # Errors
    ///
    /// Returns an error when an extension uses a reserved core name or when
    /// more than one extension route has the same name.
    pub fn from_providers(
        providers: &[Arc<dyn McpExtensionsProvider>],
    ) -> Result<Self, McpExtensionsError> {
        let mut merged = Self::default();
        for provider in providers {
            let extensions = provider.extensions();
            Arc::make_mut(&mut merged.0)
                .duplicate_tool_names
                .extend(extensions.0.duplicate_tool_names.iter().cloned());
            merged = merged.add_tools(extensions.0.added_tools.clone());
            if let Some(names) = &extensions.0.retained_tool_names {
                merged = merged.retain_tools(names.clone());
            }
        }
        merged.finalize()?;
        Ok(merged)
    }

    pub(crate) fn added_tools(&self) -> &McpToolRouter {
        &self.0.added_tools
    }

    pub(crate) fn retained_tool_names(&self) -> Option<&BTreeSet<String>> {
        self.0.retained_tool_names.as_ref()
    }

    pub(crate) fn finalize(&mut self) -> Result<(), McpExtensionsError> {
        if let Some(name) = self.0.duplicate_tool_names.iter().next() {
            return Err(McpExtensionsError::DuplicateToolName(name.clone()));
        }
        if let Some(name) = self
            .0
            .added_tools
            .list_all()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .find(|name| name.parse::<ToolName>().is_ok())
        {
            return Err(McpExtensionsError::ReservedToolName(name));
        }
        let extensions = Arc::make_mut(&mut self.0);
        if let Some(retained) = &extensions.retained_tool_names {
            for tool in extensions.added_tools.list_all() {
                if !retained.contains(tool.name.as_ref()) {
                    extensions.added_tools.disable_route(tool.name);
                }
            }
        }
        Ok(())
    }
}

/// Invalid static MCP extension composition.
#[derive(Debug, thiserror::Error)]
pub enum McpExtensionsError {
    /// An extension tried to register a reserved core Coral tool name.
    #[error("MCP extension tool name '{0}' is reserved by Coral")]
    ReservedToolName(String),
    /// More than one extension tool used the same name.
    #[error("duplicate MCP extension tool name '{0}'")]
    DuplicateToolName(String),
}
