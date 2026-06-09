#![expect(
    missing_docs,
    reason = "coral-code-mode is a copied integration crate whose public API is still being adapted"
)]
#![expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::default_trait_access,
    clippy::ignored_unit_patterns,
    clippy::let_underscore_must_use,
    clippy::manual_let_else,
    clippy::map_err_ignore,
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::needless_continue,
    clippy::needless_pass_by_value,
    clippy::redundant_else,
    clippy::single_match_else,
    clippy::too_many_lines,
    reason = "coral-code-mode preserves copied V8 runtime structure while the Coral bridge is reviewed separately"
)]

mod description;
mod input;
mod response;
#[cfg(feature = "code-mode")]
mod runtime;
#[cfg(feature = "code-mode")]
mod service;

#[cfg(test)]
use tokio as _;

pub use description::CODE_MODE_PRAGMA_PREFIX;
pub use description::CodeModeToolKind;
pub use description::ToolDefinition;
pub use description::ToolName;
pub use description::normalize_code_mode_identifier;
pub use description::parse_exec_source;
pub use input::normalize_nested_tool_input;
pub use input::wrap_exec_source;
pub use response::DEFAULT_IMAGE_DETAIL;
pub use response::FunctionCallOutputContentItem;
pub use response::ImageDetail;
#[cfg(feature = "code-mode")]
pub use runtime::CodeModeNestedToolCall;
#[cfg(feature = "code-mode")]
pub use runtime::DEFAULT_EXEC_YIELD_TIME_MS;
#[cfg(feature = "code-mode")]
pub use runtime::DEFAULT_MAX_NESTED_TOOL_CALLS_PER_CELL;
#[cfg(feature = "code-mode")]
pub use runtime::DEFAULT_MAX_OUTPUT_TOKENS_PER_EXEC_CALL;
#[cfg(feature = "code-mode")]
pub use runtime::DEFAULT_MAX_PARALLEL_TOOL_CALLS_PER_CELL;
#[cfg(feature = "code-mode")]
pub use runtime::DEFAULT_MAX_TOTAL_INVOCATIONS_PER_CELL;
#[cfg(feature = "code-mode")]
pub use runtime::DEFAULT_WAIT_YIELD_TIME_MS;
#[cfg(feature = "code-mode")]
pub use runtime::ExecuteRequest;
#[cfg(feature = "code-mode")]
pub use runtime::ExecuteToPendingOutcome;
#[cfg(feature = "code-mode")]
pub use runtime::RuntimeResponse;
#[cfg(feature = "code-mode")]
pub use runtime::WaitOutcome;
#[cfg(feature = "code-mode")]
pub use runtime::WaitRequest;
#[cfg(feature = "code-mode")]
pub use runtime::WaitToPendingOutcome;
#[cfg(feature = "code-mode")]
pub use runtime::WaitToPendingRequest;
#[cfg(feature = "code-mode")]
pub use service::CodeModeService;
#[cfg(feature = "code-mode")]
pub use service::CodeModeTurnHost;
#[cfg(feature = "code-mode")]
pub use service::CodeModeTurnWorker;

pub const PUBLIC_TOOL_NAME: &str = "exec";
pub const WAIT_TOOL_NAME: &str = "wait";
