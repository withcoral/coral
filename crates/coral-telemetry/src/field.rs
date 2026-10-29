//! The safe telemetry fields: the only keys a span, span event, link or log
//! record carries when it leaves the host.
//!
//! Coral's own fields are named `coral.*`, after the glossary; the standard
//! OpenTelemetry semantic-convention keys keep their standard names. Each
//! value is an opaque identifier, a bounded kind, a count or a duration,
//! never caller text, an error's `Display` text or a credential. The export
//! filters drop every other key, and a source check fails on a span or event
//! that names one, so a field records only what its name promises.

/// Coral's own fields.
const CORAL: &[&str] = &[
    // `ok` or `error`, beside the span status.
    "coral.status",
    // The Workspace the work belongs to.
    "coral.workspace.id",
    // The authenticated Principal: only after authentication, and never as
    // a metric attribute.
    "coral.principal.id",
    // Whether the Principal is a User or an Agent.
    "coral.principal.kind",
    // The OAuth client a request came through.
    "coral.client.id",
    // The Task the work belongs to.
    "coral.task.id",
    // The Program Run, its status, and the phase a span covers.
    "coral.run.id",
    "coral.run.status",
    "coral.run.phase",
    // The execution claim a Run Owner holds, and the one that takes over a
    // Run whose owner failed.
    "coral.run.claim.id",
    "coral.run.takeover_claim.id",
    // The Run Owner write that did not land, and which attempt it was.
    "coral.run.write",
    "coral.run.write.attempt",
    // How long ago the Run Owner's last commit succeeded, in milliseconds.
    "coral.run.last_commit_age_ms",
    // How many Runs an expired-lease sweep recovered.
    "coral.run.recovered",
    // How many Runs were still live when the Shutdown Drain ran out, and how
    // long it lasted, in milliseconds.
    "coral.shutdown.live_runs",
    "coral.shutdown.drain_ms",
    // The Tool Invocation, how it ended, the gate it waits at (authority,
    // identity or approval), its public failure code, and whether it passed
    // its Dispatch Fence.
    "coral.invocation.id",
    "coral.invocation.result_kind",
    "coral.invocation.gate.kind",
    "coral.invocation.failure.code",
    "coral.invocation.dispatch_started",
    // The Provider and its protocol.
    "coral.provider.id",
    "coral.provider.protocol",
    // The Tool Call Path, and whether the Tool reads or changes provider
    // state.
    "coral.tool.call_path",
    "coral.tool.effect",
    // The Saved Function and its Call Path.
    "coral.saved_function.id",
    "coral.saved_function.call_path",
    // The public failure code of an MCP tool call.
    "coral.mcp.failure.code",
    // A search index write: the Provider Generation it covers, what started
    // it, why it was skipped, how long it waited for the writer and took, in
    // milliseconds, how many rows a statistics refresh wrote, and the code of
    // a catalog warning it met.
    "coral.search.generation.id",
    "coral.search.write.trigger",
    "coral.search.skipped",
    "coral.search.writer_wait_ms",
    "coral.search.write_ms",
    "coral.search.rows_written",
    "coral.search.warning.code",
    // The kind of Search Entity a Universal Search retriever ranks.
    "coral.search.retriever",
    // What one search index reconcile pass did.
    "coral.search.reconcile.generations_indexed",
    "coral.search.reconcile.generations_failed",
    "coral.search.reconcile.functions_indexed",
    "coral.search.reconcile.functions_failed",
    "coral.search.reconcile.workspaces_checked",
    "coral.search.reconcile.workspaces_recounted",
    "coral.search.reconcile.rows_written",
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
