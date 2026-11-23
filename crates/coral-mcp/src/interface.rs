//! The tool surface the Adapter serves.

use coral_db::UnixNanos;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use rmcp::ErrorData;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, ProgressNotificationParam, ProgressToken, Tool,
};
use rmcp::service::{Peer, RoleServer};

use crate::projection::ToolFailure;

/// The tool surface MCP serves: its instructions, its tools and each tool call.
#[async_trait::async_trait]
pub trait McpInterface: Send + Sync {
    /// The server instructions `caller` reads before its first call.
    async fn instructions(&self, caller: &McpCaller) -> &str;

    /// The tools `caller` is served.
    async fn tools(&self, caller: &McpCaller) -> Result<Vec<Tool>, ErrorData>;

    /// Runs one tool call for `caller`.
    async fn call(
        &self,
        caller: &McpCaller,
        request: CallToolRequestParams,
        progress: Option<Progress>,
    ) -> Result<CallToolResponse, ToolCallError>;
}

/// Who one MCP request acts as. Only the Adapter builds one, after it
/// authenticated the Principal and checked its authority in the Workspace.
#[derive(Clone)]
pub struct McpCaller {
    principal: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    now: UnixNanos,
}

impl McpCaller {
    pub(crate) const fn new(
        principal: AuthenticatedPrincipal,
        workspace_id: WorkspaceId,
        now: UnixNanos,
    ) -> Self {
        Self {
            principal,
            workspace_id,
            now,
        }
    }

    /// A caller that no MCP request authenticated, for another crate's test
    /// of a tool surface.
    #[cfg(feature = "test-fixtures")]
    #[must_use]
    pub const fn fixture(
        principal: AuthenticatedPrincipal,
        workspace_id: WorkspaceId,
        now: UnixNanos,
    ) -> Self {
        Self::new(principal, workspace_id, now)
    }

    /// The authenticated Principal.
    #[must_use]
    pub const fn principal(&self) -> &AuthenticatedPrincipal {
        &self.principal
    }

    /// The Workspace the request acts in.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// The request's time: the instant its authentication read.
    #[must_use]
    pub const fn now(&self) -> UnixNanos {
        self.now
    }
}

/// Where one tool call reports progress to the client that asked for it.
pub struct Progress {
    pub(crate) token: ProgressToken,
    pub(crate) peer: Peer<RoleServer>,
}

impl Progress {
    /// Sends one progress message on this call's own response stream. A
    /// client that cannot take it loses nothing the Run Update does not carry.
    pub(crate) async fn report(self, message: String) {
        let sent = self
            .peer
            .notify_progress(ProgressNotificationParam::new(self.token, 1.0).with_message(message))
            .await;
        if sent.is_err() {
            tracing::debug!("progress notification not delivered");
        }
    }
}

/// Why a tool call has no result.
pub enum ToolCallError {
    /// The request was wrong. The client gets a JSON-RPC error.
    Protocol(ErrorData),
    /// Coral could not complete the call, and the client gets the failure as
    /// the tool's result. A surface that wraps core's passes core's on, and
    /// builds one for a tool of its own.
    Tool(ToolFailure),
}

impl From<ErrorData> for ToolCallError {
    fn from(error: ErrorData) -> Self {
        Self::Protocol(error)
    }
}
