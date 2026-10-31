//! Workspace Streamable HTTP Adapter. Authentication fixes a canonical Principal
//! on every HTTP request; domain Modules own current authority and visibility.
//! Coral issues no MCP session, so any Coral Instance serves any request, and
//! client metadata never grants authority.

mod arguments;
mod authentication;
mod http;
mod instructions;
mod projection;
mod prompts;
mod server;
mod urls;

#[cfg(test)]
use {
    coral_catalog as _, coral_policy as _, coral_provider as _, coral_test_support as _,
    jsonschema as _, opentelemetry as _, opentelemetry_sdk as _, reqwest as _,
    tracing_subscriber as _,
};

use std::num::NonZeroUsize;

use coral_run::RunModule;
use coral_task::{TaskCommands, TaskModule};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceModule};

pub use authentication::{AuthenticationError, OauthProtectedResource, WorkspaceAuthenticator};
pub use coral_run::{HumanInterfaceUrl, HumanInterfaceUrlError};
pub use http::{
    McpHttpConfig, McpHttpError, ReadinessProbe, RunningMcpHttpServer, start_authenticated,
    start_local,
};
pub use server::SUPPORTED_PROTOCOL_VERSIONS;
pub use urls::{
    McpEndpoint, ORGANIZATION_MCP_ROUTE, WORKSPACE_MCP_ROUTE, parse_workspace_mcp_path,
    workspace_mcp_path,
};

/// Default target size of one encoded `exec`, `wait` or `cancel` result.
const RUN_UPDATE_PAGE_BYTES: NonZeroUsize =
    NonZeroUsize::new(64 << 20).expect("the default page target is positive");

/// Domain Modules required by the MCP Adapter, independent of authentication mode,
/// and the projection settings: the human interface where projections send
/// people to act, and the Run Update page target.
#[derive(Clone)]
pub struct McpModules {
    pub(crate) workspaces: WorkspaceModule,
    pub(crate) tasks: TaskModule,
    pub(crate) commands: TaskCommands,
    pub(crate) runs: RunModule,
    pub(crate) human_interface: HumanInterfaceUrl,
    pub(crate) run_update_page_bytes: NonZeroUsize,
}
impl McpModules {
    /// Receives initialized Modules, including Task commands assembled by bootstrap.
    #[must_use]
    pub const fn new(
        workspaces: WorkspaceModule,
        tasks: TaskModule,
        commands: TaskCommands,
        runs: RunModule,
        human_interface: HumanInterfaceUrl,
    ) -> Self {
        Self {
            workspaces,
            tasks,
            commands,
            runs,
            human_interface,
            run_update_page_bytes: RUN_UPDATE_PAGE_BYTES,
        }
    }

    /// Selects the target size of one encoded `exec`, `wait` or `cancel`
    /// result. Pages fill up to it; paging never splits a value, so a single
    /// larger output item or terminal outcome arrives whole on its own page.
    #[must_use]
    pub const fn with_run_update_page_bytes(mut self, bytes: NonZeroUsize) -> Self {
        self.run_update_page_bytes = bytes;
        self
    }
}

#[derive(Clone)]
pub(crate) struct McpRuntime {
    modules: McpModules,
    principal: AuthenticatedPrincipal,
}
