//! The tools the agent calls. The Adapter that serves the caller supplies them,
//! bound to the caller's Principal, so the agent uses exactly the tools, schemas
//! and results an outside agent sees and holds no authority of its own.

use async_trait::async_trait;
use serde_json::Value;

/// One tool the model may call.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolSpec {
    /// The tool name.
    pub name: String,
    /// What the tool does, as the model reads it.
    pub description: String,
    /// JSON Schema of the tool's input.
    pub input_schema: Value,
}

/// A Program Run that a tool call observed.
#[derive(Debug, Clone, PartialEq)]
pub struct RunObserved {
    /// The Run.
    pub run_id: String,
    /// Whether the Run ended with a Final Value.
    pub succeeded: bool,
    /// The Run's current required actions; empty when none.
    pub required_actions: Vec<Value>,
}

/// The result of one tool call.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolOutcome {
    /// The text the model reads, normally the tool's JSON result.
    pub text: String,
    /// Whether the call failed.
    pub is_error: bool,
    /// The Run the call started or observed, if any.
    pub run: Option<RunObserved>,
}

/// The tools available in one turn, already bound to the caller.
#[async_trait]
pub trait Toolbox: Send + Sync {
    /// Instructions that explain the tools, placed in the system prompt.
    fn instructions(&self) -> &str;

    /// The tools the model may call.
    fn tools(&self) -> Vec<ToolSpec>;

    /// Calls one tool. Failures come back as an error outcome, never a panic.
    async fn call(&self, name: &str, input: Value) -> ToolOutcome;
}
