//! The safe telemetry fields: the only keys a span, span event, link or log
//! record carries when it leaves the host.
//!
//! Coral's own fields are named `coral.*`, after the glossary; the standard
//! OpenTelemetry semantic-convention keys keep their standard names. Each
//! value is an opaque identifier, a bounded kind, a count or a duration,
//! never caller text, an error's `Display` text or a credential. A key not
//! listed here is dropped at export, so a field records only what its name
//! promises.
//!
//! A tracing macro takes a constant as a field name in braces:
//! `tracing::info_span!("coral.run.execute", { field::RUN_ID } = %id)`.

/// The Workspace the work belongs to.
pub const WORKSPACE_ID: &str = "coral.workspace.id";
/// The authenticated Principal. Only after authentication, and never as a
/// metric attribute.
pub const PRINCIPAL_ID: &str = "coral.principal.id";
/// Whether the Principal is a User or an Agent.
pub const PRINCIPAL_KIND: &str = "coral.principal.kind";
/// The OAuth client a request came through.
pub const CLIENT_ID: &str = "coral.client.id";
/// The Task the work belongs to.
pub const TASK_ID: &str = "coral.task.id";
/// The Program Run.
pub const RUN_ID: &str = "coral.run.id";
/// The Run's status.
pub const RUN_STATUS: &str = "coral.run.status";
/// The Run phase a span covers.
pub const RUN_PHASE: &str = "coral.run.phase";
/// The Tool Invocation.
pub const INVOCATION_ID: &str = "coral.invocation.id";
/// How the Tool Invocation ended.
pub const INVOCATION_RESULT_KIND: &str = "coral.invocation.result_kind";
/// The gate a Tool Invocation waits at: authority, identity or approval.
pub const INVOCATION_GATE_KIND: &str = "coral.invocation.gate.kind";
/// The public failure code of a Tool Invocation.
pub const INVOCATION_FAILURE_CODE: &str = "coral.invocation.failure.code";
/// Whether the Tool Invocation passed its Dispatch Fence.
pub const INVOCATION_DISPATCH_STARTED: &str = "coral.invocation.dispatch_started";
/// The Provider.
pub const PROVIDER_ID: &str = "coral.provider.id";
/// The Provider's protocol.
pub const PROVIDER_PROTOCOL: &str = "coral.provider.protocol";
/// The Tool Call Path.
pub const TOOL_CALL_PATH: &str = "coral.tool.call_path";
/// The Tool's effect: whether it reads or changes provider state.
pub const TOOL_EFFECT: &str = "coral.tool.effect";
/// The Saved Function.
pub const SAVED_FUNCTION_ID: &str = "coral.saved_function.id";
/// The public failure code of an MCP tool call.
pub const MCP_FAILURE_CODE: &str = "coral.mcp.failure.code";
/// The Provider Generation a search index write covers.
pub const SEARCH_GENERATION_ID: &str = "coral.search.generation.id";
/// What started a search index write.
pub const SEARCH_WRITE_TRIGGER: &str = "coral.search.write.trigger";
/// Why a search index write was skipped.
pub const SEARCH_SKIPPED: &str = "coral.search.skipped";
/// How long a search index write waited for the writer, in milliseconds.
pub const SEARCH_WRITER_WAIT_MS: &str = "coral.search.writer_wait_ms";
/// How long a search index write took, in milliseconds.
pub const SEARCH_WRITE_MS: &str = "coral.search.write_ms";
/// The stable code of a catalog warning a search index write met.
pub const SEARCH_WARNING_CODE: &str = "coral.search.warning.code";
/// How many rows a search statistics refresh wrote.
pub const SEARCH_ROWS_WRITTEN: &str = "coral.search.rows_written";

/// How many Provider Generations a reconcile pass indexed.
pub const SEARCH_RECONCILE_GENERATIONS_INDEXED: &str = "coral.search.reconcile.generations_indexed";
/// How many Provider Generations a reconcile pass failed to index.
pub const SEARCH_RECONCILE_GENERATIONS_FAILED: &str = "coral.search.reconcile.generations_failed";
/// How many Saved Functions a reconcile pass indexed.
pub const SEARCH_RECONCILE_FUNCTIONS_INDEXED: &str = "coral.search.reconcile.functions_indexed";
/// How many Saved Functions a reconcile pass failed to index.
pub const SEARCH_RECONCILE_FUNCTIONS_FAILED: &str = "coral.search.reconcile.functions_failed";
/// How many Workspaces' search statistics a reconcile pass checked.
pub const SEARCH_RECONCILE_WORKSPACES_CHECKED: &str = "coral.search.reconcile.workspaces_checked";
/// How many Workspaces' search statistics a reconcile pass recounted.
pub const SEARCH_RECONCILE_WORKSPACES_RECOUNTED: &str =
    "coral.search.reconcile.workspaces_recounted";
/// How many rows a reconcile pass wrote.
pub const SEARCH_RECONCILE_ROWS_WRITTEN: &str = "coral.search.reconcile.rows_written";
/// `ok` or `error`, beside the span status.
pub const STATUS: &str = "status";

/// Coral's own fields.
const CORAL: &[&str] = &[
    WORKSPACE_ID,
    PRINCIPAL_ID,
    PRINCIPAL_KIND,
    CLIENT_ID,
    TASK_ID,
    RUN_ID,
    RUN_STATUS,
    RUN_PHASE,
    INVOCATION_ID,
    INVOCATION_RESULT_KIND,
    INVOCATION_GATE_KIND,
    INVOCATION_FAILURE_CODE,
    INVOCATION_DISPATCH_STARTED,
    PROVIDER_ID,
    PROVIDER_PROTOCOL,
    TOOL_CALL_PATH,
    TOOL_EFFECT,
    SAVED_FUNCTION_ID,
    MCP_FAILURE_CODE,
    SEARCH_GENERATION_ID,
    SEARCH_WRITE_TRIGGER,
    SEARCH_SKIPPED,
    SEARCH_WRITER_WAIT_MS,
    SEARCH_WRITE_MS,
    SEARCH_ROWS_WRITTEN,
    SEARCH_WARNING_CODE,
    SEARCH_RECONCILE_GENERATIONS_INDEXED,
    SEARCH_RECONCILE_GENERATIONS_FAILED,
    SEARCH_RECONCILE_FUNCTIONS_INDEXED,
    SEARCH_RECONCILE_FUNCTIONS_FAILED,
    SEARCH_RECONCILE_WORKSPACES_CHECKED,
    SEARCH_RECONCILE_WORKSPACES_RECOUNTED,
    SEARCH_RECONCILE_ROWS_WRITTEN,
    STATUS,
];

/// The standard keys Coral allows. `exception.message` and
/// `exception.stacktrace` are not among them: they can hold an error's
/// `Display` text. `error.type` names the failure instead.
const STANDARD: &[&str] = &[
    "error.type",
    "http.request.method",
    "http.route",
    "http.response.status_code",
    "http.request.body.size",
    "http.response.body.size",
    "url.scheme",
    "url.template",
    "server.address",
    "server.port",
    "rpc.system.name",
    "rpc.method",
    "rpc.response.status_code",
    "mcp.method.name",
    "gen_ai.tool.name",
    "process.executable.name",
    "process.exit.code",
    "process.pid",
];

/// The keys the tracing bridge adds to every span and event: its target and
/// level, the code location, the thread, and the span's busy and idle time.
const BRIDGE: &[&str] = &[
    "target",
    "level",
    "code.file.path",
    "code.module.name",
    "code.line.number",
    "thread.id",
    "thread.name",
    "busy_ns",
    "idle_ns",
];

/// Whether `key` may leave the host. A `coral.local.*` key never does.
pub(crate) fn is_exported(key: &str) -> bool {
    !key.starts_with(crate::LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX)
        && [CORAL, STANDARD, BRIDGE]
            .into_iter()
            .any(|keys| keys.contains(&key))
}
