//! Shared planning and result projection for the Invocation owner.

use std::sync::Arc;

use coral_catalog::{ExecutionBinding, MaterializedOperation, SchemaGraph};
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
        operation: &MaterializedOperation,
        schemas: &SchemaGraph,
        arguments: &Value,
        provider: &str,
        resources: Arc<McpResources>,
        limits: coral_value::ValueLimits,
    ) -> Result<Self, Error> {
        match operation.execution_binding() {
            ExecutionBinding::Http(_) => {
                ReadyHttpDispatch::plan(operation, schemas, arguments, limits)
                    .map(|ready| Self::Http(Box::new(ready)))
            }
            ExecutionBinding::Mcp(_) => {
                ReadyMcpDispatch::plan(operation, arguments, provider, resources, limits)
                    .map(|ready| Self::Mcp(Box::new(ready)))
            }
            // Coral performs built-in Operations itself and sends nothing upstream.
            // Until it does, planning one settles as a safe Coral Failure.
            ExecutionBinding::Builtin(_) => Err(Error::OperationUnavailable),
        }
    }
}

/// A complete upstream response, without a Coral-defined success discriminator.
pub enum UpstreamOutcome {
    /// Complete HTTP outcome, regardless of status.
    Http(HttpOutcome),
    /// Complete MCP outcome, regardless of `isError`.
    Mcp(McpOutcome),
}

impl UpstreamOutcome {
    /// Returns the mandatory, protocol-specific `{data, metadata}` projection.
    #[must_use]
    pub fn operation_result(&self) -> Value {
        match self {
            Self::Http(outcome) => outcome.operation_result(),
            Self::Mcp(outcome) => outcome.operation_result().clone(),
        }
    }
}
