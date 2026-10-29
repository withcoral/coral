//! Shared planning and result projection for the Invocation owner.

use std::sync::Arc;

use coral_catalog::{ExecutionBinding, MaterializedTool, SchemaGraph};
use serde_json::Value;

use crate::{Error, HttpOutcome, McpOutcome, McpResources, ReadyHttpDispatch, ReadyMcpDispatch};

/// One prepared request, consumed once by its protocol sender.
pub enum ReadyDispatch {
    /// One HTTP request and its declared protocol continuations.
    Http(Box<ReadyHttpDispatch>),
    /// One MCP tool call or resource read.
    Mcp(Box<ReadyMcpDispatch>),
}

impl ReadyDispatch {
    /// Plans from the retained catalog without reading raw provider descriptors.
    /// The Provider identifier is host-supplied provenance for resource handles,
    /// never a Program-supplied authority selector.
    ///
    /// # Errors
    /// Returns a protocol planning or invalid-input error before any send.
    pub fn plan(
        tool: &MaterializedTool,
        schemas: &SchemaGraph,
        arguments: &Value,
        provider: &str,
        resources: Arc<McpResources>,
        limits: coral_value::ValueLimits,
    ) -> Result<Self, Error> {
        match tool.execution_binding() {
            ExecutionBinding::Http(_) => ReadyHttpDispatch::plan(tool, schemas, arguments, limits)
                .map(|ready| Self::Http(Box::new(ready))),
            ExecutionBinding::Mcp(_) => {
                ReadyMcpDispatch::plan(tool, arguments, provider, resources, limits)
                    .map(|ready| Self::Mcp(Box::new(ready)))
            }
            // The Run domain performs built-in Tools itself and never plans
            // one. Planning one anyway settles as a safe Coral Failure.
            ExecutionBinding::Builtin(_) => Err(Error::ToolUnavailable),
        }
    }
}

/// Whether a complete upstream response is a declared success, or why it is not.
///
/// A Program receives only a [`OutcomeClass::Success`]; every other class
/// rejects the call with the complete outcome attached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeClass {
    /// A response the Tool declares as a success. Its body can still
    /// miss its schema, or arrive for a success that declares no content.
    Success,
    /// The upstream reported failure: a non-2xx HTTP status, or an MCP tool
    /// result with `isError: true`.
    ErrorResponse,
    /// A 2xx status or media type that the Tool does not declare as a success.
    UndeclaredResponse,
    /// A body Coral could not decode: a malformed `Content-Type`, an
    /// unsupported charset, or a malformed body.
    UndecodableResponse,
}

/// A complete upstream response and its [`OutcomeClass`].
pub enum UpstreamOutcome {
    /// Complete HTTP outcome, regardless of status.
    Http(HttpOutcome),
    /// Complete MCP outcome, regardless of `isError`.
    Mcp(McpOutcome),
}

impl UpstreamOutcome {
    /// Returns whether the Program receives this outcome or is rejected with it.
    #[must_use]
    pub fn class(&self) -> OutcomeClass {
        match self {
            Self::Http(outcome) => outcome.class(),
            Self::Mcp(outcome) => outcome.class(),
        }
    }

    /// Returns the HTTP status of an HTTP outcome.
    #[must_use]
    pub const fn http_status(&self) -> Option<u16> {
        match self {
            Self::Http(outcome) => Some(outcome.status()),
            Self::Mcp(_) => None,
        }
    }

    /// Returns the mandatory, protocol-specific `{data, metadata}` projection.
    #[must_use]
    pub fn tool_result(&self) -> Value {
        match self {
            Self::Http(outcome) => outcome.tool_result(),
            Self::Mcp(outcome) => outcome.tool_result().clone(),
        }
    }
}
