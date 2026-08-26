//! Static MCP surface composition and per-session tool context.

use std::{collections::BTreeSet, error::Error, sync::Arc};

use coral_api::v1::Workspace;
use rmcp::handler::server::router::tool::{ToolRoute, ToolRouter};

use crate::{server::CoralToolset, surface::ToolName};

/// RMCP route used for one host-supplied tool.
pub type McpToolRoute = ToolRoute<McpToolContext>;

pub(crate) type McpToolRouter = ToolRouter<McpToolContext>;

/// Error returned by a host while it builds its MCP surface.
pub type McpSurfaceProviderError = Box<dyn Error + Send + Sync + 'static>;

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

/// Builds one static MCP surface when an MCP runtime starts.
pub trait McpSurfaceProvider: Send + Sync {
    /// Returns the complete host-owned MCP surface contribution.
    ///
    /// # Errors
    ///
    /// Returns an error when host configuration cannot produce a surface.
    fn surface(&self) -> Result<McpSurface, McpSurfaceProviderError>;
}

/// Static public MCP tool surface and initialize instructions.
#[derive(Clone, Debug)]
pub struct McpSurface(Arc<McpSurfaceInner>);

#[derive(Clone, Debug)]
struct McpSurfaceInner {
    extension_tools: McpToolRouter,
    public_core_tool_names: Option<BTreeSet<String>>,
    initialize_instructions: InitializeInstructions,
}

#[derive(Clone, Debug)]
pub(crate) enum InitializeInstructions {
    CoralDefault,
    Replace(Option<String>),
}

impl Default for McpSurface {
    fn default() -> Self {
        Self(Arc::new(McpSurfaceInner {
            extension_tools: McpToolRouter::new(),
            public_core_tool_names: None,
            initialize_instructions: InitializeInstructions::CoralDefault,
        }))
    }
}

impl McpSurface {
    /// Keeps the complete OSS Coral surface and adds host tool routes.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate routes or a route that uses a reserved
    /// Coral tool name.
    pub fn extend(routes: impl IntoIterator<Item = McpToolRoute>) -> Result<Self, McpSurfaceError> {
        Self::build(routes, None, InitializeInstructions::CoralDefault)
    }

    /// Replaces the public surface with exact host routes and named core tools.
    ///
    /// `initialize_instructions` replaces Coral's default instructions. `None`
    /// omits initialize instructions.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate routes, a route that uses a reserved
    /// Coral tool name, or an unknown core tool name.
    pub fn replace(
        routes: impl IntoIterator<Item = McpToolRoute>,
        public_core_tool_names: impl IntoIterator<Item = impl Into<String>>,
        initialize_instructions: Option<String>,
    ) -> Result<Self, McpSurfaceError> {
        let public_core_tool_names = public_core_tool_names
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>();
        if let Some(name) = public_core_tool_names
            .iter()
            .find(|name| name.parse::<ToolName>().is_err())
        {
            return Err(McpSurfaceError::UnknownCoreToolName(name.clone()));
        }
        Self::build(
            routes,
            Some(public_core_tool_names),
            InitializeInstructions::Replace(initialize_instructions),
        )
    }

    fn build(
        routes: impl IntoIterator<Item = McpToolRoute>,
        public_core_tool_names: Option<BTreeSet<String>>,
        initialize_instructions: InitializeInstructions,
    ) -> Result<Self, McpSurfaceError> {
        let mut extension_tools = McpToolRouter::new();
        for route in routes {
            let name = route.name().to_string();
            if name.parse::<ToolName>().is_ok() {
                return Err(McpSurfaceError::ReservedToolName(name));
            }
            if extension_tools.has_route(&name) {
                return Err(McpSurfaceError::DuplicateToolName(name));
            }
            extension_tools.add_route(route);
        }
        Ok(Self(Arc::new(McpSurfaceInner {
            extension_tools,
            public_core_tool_names,
            initialize_instructions,
        })))
    }

    pub(crate) fn extension_tools(&self) -> &McpToolRouter {
        &self.0.extension_tools
    }

    pub(crate) fn public_core_tool_names(&self) -> Option<&BTreeSet<String>> {
        self.0.public_core_tool_names.as_ref()
    }

    pub(crate) fn initialize_instructions(&self) -> &InitializeInstructions {
        &self.0.initialize_instructions
    }

    pub(crate) fn validate(&self, feedback_enabled: bool) -> Result<(), McpSurfaceError> {
        if !feedback_enabled
            && self
                .0
                .public_core_tool_names
                .as_ref()
                .is_some_and(|names| names.contains(ToolName::Feedback.as_str()))
        {
            return Err(McpSurfaceError::UnavailableCoreToolName(
                ToolName::Feedback.as_str().to_string(),
            ));
        }
        Ok(())
    }
}

/// Invalid static MCP surface composition.
#[derive(Debug, thiserror::Error)]
pub enum McpSurfaceError {
    /// A host route tried to use a reserved core Coral tool name.
    #[error("MCP extension tool name '{0}' is reserved by Coral")]
    ReservedToolName(String),
    /// More than one host route used the same name.
    #[error("duplicate MCP extension tool name '{0}'")]
    DuplicateToolName(String),
    /// A replacement surface named a core tool that Coral does not define.
    #[error("unknown core MCP tool name '{0}'")]
    UnknownCoreToolName(String),
    /// A replacement surface selected a core tool that is not enabled.
    #[error("core MCP tool '{0}' is not available")]
    UnavailableCoreToolName(String),
}
