//! One Run on a Run Executor: its Run stream, and its Program on a thread of
//! its own.

mod host;

use std::sync::Arc;
use std::time::Duration;

use coral_telemetry::safe;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::PROTOCOL_LEVEL;
use crate::link::{Backoff, Client, End, HeldRun, HeldRunStream, HeldUnit, compatible};
use crate::wire::v1::{
    Open, RunPackage, RunRequest, RunResponse, TakenRun, ValueLimits, run_request, run_response,
};
use host::{Resume, RunHost};

/// The Applied cap: while more output bytes than this are above the
/// committed watermark, the Program's next request waits. Emissions and Logs
/// count their output bytes; an Invoke counts none.
pub const APPLIED_CAP_BYTES: usize = 8 << 20;
/// The most of a Run's requests kept above the committed watermark for a
/// resend: while this many are, the Program's next request waits.
pub const HELD_CAP_REQUESTS: usize = 512;
/// Maximum UTF-8 byte length of one Program Log message.
pub const PROGRAM_LOG_MESSAGE_MAX_BYTES: usize = 64 * 1024;

/// What each Run on a Run Executor runs within.
#[derive(Clone, Copy, Debug)]
pub struct RunLimits {
    /// The heap limits of each Run's isolate.
    pub isolate: coral_v8::IsolateLimits,
    /// The Applied cap, [`APPLIED_CAP_BYTES`] outside tests.
    pub applied_cap_bytes: usize,
    /// The held cap, [`HELD_CAP_REQUESTS`] outside tests.
    pub held_cap: usize,
    /// How long a stopped Program's isolate has to go before the Run
    /// Executor retires: 5 s outside tests.
    pub stop_grace: Duration,
    /// Holds each stopped Program's thread before it sends `Finish` while a
    /// test holds the lock, as an isolate that ignores termination.
    #[cfg(test)]
    pub(crate) stall: Option<&'static std::sync::RwLock<()>>,
    /// Told the number of each request that parks on a cap, so a test can
    /// wait for it.
    #[cfg(test)]
    pub(crate) on_parked: Option<&'static mpsc::UnboundedSender<u64>>,
}

impl Default for RunLimits {
    fn default() -> Self {
        Self {
            isolate: coral_v8::IsolateLimits::defaults(),
            applied_cap_bytes: APPLIED_CAP_BYTES,
            held_cap: HELD_CAP_REQUESTS,
            stop_grace: Duration::from_secs(5),
            #[cfg(test)]
            stall: None,
            #[cfg(test)]
            on_parked: None,
        }
    }
}

impl From<coral_value::ValueLimits> for ValueLimits {
    fn from(limits: coral_value::ValueLimits) -> Self {
        let bytes = |bytes: usize| u64::try_from(bytes).unwrap_or(u64::MAX);
        Self {
            binary_bytes: bytes(limits.binary_bytes()),
            host_bytes: bytes(limits.host_bytes()),
            upstream_bytes: bytes(limits.upstream_bytes()),
            run_output_bytes: bytes(limits.run_output_bytes()),
        }
    }
}

/// The value limits a package carries; `None` for a limit of zero or one
/// past this platform's `usize`.
fn value_limits(limits: &ValueLimits) -> Option<coral_value::ValueLimits> {
    let bytes = |bytes: u64| usize::try_from(bytes).ok();
    coral_value::ValueLimits::new(
        bytes(limits.binary_bytes)?,
        bytes(limits.host_bytes)?,
        bytes(limits.upstream_bytes)?,
        bytes(limits.run_output_bytes)?,
    )
    .ok()
}

/// A Run's id, kept only when it has the form Coral mints Run ids in.
#[derive(Clone)]
struct RunId(Option<String>);

impl RunId {
    fn new(id: &str) -> Self {
        Self(coral_identifier::validate(id).ok().map(|()| id.to_owned()))
    }
}

impl std::fmt::Display for RunId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0.as_deref().unwrap_or("invalid"))
    }
}

/// Coral mints every Run id in a form `coral_identifier::validate` accepts,
/// and [`RunId::new`] keeps no text of another form.
impl safe::Identifier for RunId {}

/// A Run's Program, once its package loaded here.
struct Program {
    host: StopsOnDrop,
    /// Ends once the isolate is gone, with the Run's stream hold, or once
    /// it missed its stop.
    stopped: tokio::task::JoinHandle<Option<HeldRunStream>>,
    /// The loaded package's digest.
    sha256: Vec<u8>,
}

/// One Run Coral took for this Run Executor.
struct Run {
    id: RunId,
    open: Open,
    limits: RunLimits,
    /// The unit of room, until the Program's thread holds it.
    unit: Option<HeldUnit>,
    /// What a retiring Run Executor waits for, until the Program's stop is
    /// watched.
    stream: Option<HeldRunStream>,
    program: Option<Program>,
    /// Ends the Program at the deadline; aborted when the Run ends.
    deadline: Option<tokio::task::JoinHandle<()>>,
}

/// What follows one Run stream's end.
enum Reopen {
    /// Reopen after `end`. `answered` once Coral answered the `Open`.
    After { end: End, answered: bool },
    /// Coral said the Run ended, or the stream broke the protocol.
    Never,
}

/// Holds one Run Coral took for this Run Executor until Coral says it
/// ended, or its stream breaks the protocol. A lost stream reopens with the
/// same `Open` on the link's current connection, after the back-off, until
/// then: past the Run's deadline too, so its `Finish` reaches Coral.
pub(crate) async fn hold(
    mut connection: watch::Receiver<Option<Client>>,
    taken: TakenRun,
    limits: RunLimits,
    held: HeldRun,
) {
    let mut run = Run {
        id: RunId::new(&taken.run_id),
        open: Open {
            run_id: taken.run_id,
            claim_id: taken.claim_id,
            protocol_level: PROTOCOL_LEVEL,
        },
        limits,
        unit: Some(held.unit),
        stream: Some(held.stream),
        program: None,
        deadline: None,
    };
    let mut backoff = Backoff::default();
    loop {
        // The pull stream publishes each connection Coral welcomes it on.
        let client = match connection.wait_for(Option::is_some).await {
            Ok(current) => current.clone(),
            Err(_link_gone) => None,
        };
        let Some(client) = client else { break };
        match run.stream(client).await {
            Reopen::Never => break,
            Reopen::After { end, answered } => {
                if answered {
                    backoff.reset();
                }
                if let Some(delay) = backoff.after(&end) {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    if let Some(deadline) = run.deadline {
        deadline.abort();
    }
    // The unit frees once the isolate is gone.
    if let Some(Program { host, stopped, .. }) = run.program {
        drop(host);
        drop(stopped.await);
    }
}

/// Waits until the Program's thread has discarded its isolate, and gives
/// back the Run's `stream`. When it has not `grace` after the Program was
/// stopped, the Run Executor retires and no longer waits for this Run.
async fn watch_stop(
    mut stopping: watch::Receiver<bool>,
    exited: oneshot::Receiver<()>,
    grace: Duration,
    stream: HeldRunStream,
    run: RunId,
) -> Option<HeldRunStream> {
    let mut exited = std::pin::pin!(exited);
    tokio::select! {
        _ = &mut exited => return Some(stream),
        // An error: the host is gone, so the thread is too.
        _ = stopping.wait_for(|stopping| *stopping) => {}
    }
    if tokio::time::timeout(grace, &mut exited).await.is_ok() {
        return Some(stream);
    }
    tracing::warn!(
        coral.run.id = safe::id(&run),
        "a stopped Program's isolate is still running past its grace; retiring the Run Executor"
    );
    stream.abandon();
    None
}

impl Run {
    /// Opens one Run stream on `client` and holds it until it ends.
    async fn stream(&mut self, mut client: Client) -> Reopen {
        let lost = Reopen::After {
            end: End::Failed,
            answered: false,
        };
        let (outbound, requests) = mpsc::unbounded_channel();
        drop(outbound.send(RunRequest {
            message: Some(run_request::Message::Open(self.open.clone())),
        }));
        let Ok(response) = client.run(UnboundedReceiverStream::new(requests)).await else {
            return lost;
        };
        let mut inbound = response.into_inner();
        let first = match inbound.message().await {
            Ok(Some(RunResponse {
                message: Some(first),
            })) => first,
            Ok(Some(_)) => return Reopen::Never,
            // A clean end before Coral answers is no move.
            Ok(None) | Err(_) => return lost,
        };
        if !self.answer(first, outbound) {
            return Reopen::Never;
        }
        let Some(Program { host, .. }) = &self.program else {
            return Reopen::Never;
        };
        let end = loop {
            match inbound.message().await {
                Ok(Some(RunResponse {
                    message: Some(message),
                })) => match message {
                    run_response::Message::Started(started) => {
                        if host.started(&started) {
                            self.deadline = Some(end_at_deadline(&host.0, started.deadline_ms));
                        }
                    }
                    run_response::Message::Committed(committed) => {
                        host.committed(committed.request_number);
                    }
                    run_response::Message::InvocationResult(result) => {
                        if !host.invocation_result(result) {
                            return Reopen::Never;
                        }
                    }
                    run_response::Message::Terminate(_) => host.stop(),
                    // `Ended` frees the unit; a second package breaks the
                    // protocol.
                    run_response::Message::Ended(_) | run_response::Message::Package(_) => {
                        return Reopen::Never;
                    }
                },
                Ok(Some(_)) => return Reopen::Never,
                // Coral moved the stream.
                Ok(None) => break End::Moved,
                Err(_) => break End::Failed,
            }
        };
        host.detach();
        Reopen::After {
            end,
            answered: true,
        }
    }

    /// Acts on Coral's answer to `Open`. `false` when the Run ends here:
    /// Coral said it ended, or the answer breaks the protocol.
    fn answer(
        &mut self,
        first: run_response::Message,
        outbound: mpsc::UnboundedSender<RunRequest>,
    ) -> bool {
        match (first, &self.program) {
            (run_response::Message::Package(package), None) => {
                let (Some(unit), Some(stream)) = (self.unit.take(), self.stream.take()) else {
                    return false;
                };
                self.program = load(package, outbound, self.limits, unit, &self.id, stream);
                self.program.is_some()
            }
            // Loaded here before: the digest names the package, so the
            // load is skipped.
            (run_response::Message::Package(package), Some(program)) => {
                let resumed = compatible(package.protocol_level)
                    && package.sha256 == program.sha256
                    && program.host.resume(outbound, &Resume::Package);
                if !resumed {
                    tracing::warn!(
                        coral.run.id = safe::id(&self.id),
                        "a package answered a reopened Run stream that it cannot resume; closing it"
                    );
                }
                resumed
            }
            (run_response::Message::Committed(committed), Some(program)) => program
                .host
                .resume(outbound, &Resume::Committed(committed.request_number)),
            // `Ended`, a `Committed` for a Run that never loaded here, or a
            // message that answers no `Open`.
            _ => false,
        }
    }
}

/// Ends the Program at the Run's deadline, `deadline_ms` from `Started`.
fn end_at_deadline(host: &Arc<RunHost>, deadline_ms: u64) -> tokio::task::JoinHandle<()> {
    let host = Arc::downgrade(host);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(deadline_ms)).await;
        if let Some(host) = host.upgrade() {
            host.reach_deadline();
        }
    })
}

/// Stops the Program when dropped, so one whose stream task ends without
/// reaching its last line, even by a panic, never waits on the stream.
struct StopsOnDrop(Arc<RunHost>);

impl std::ops::Deref for StopsOnDrop {
    type Target = RunHost;

    fn deref(&self) -> &RunHost {
        &self.0
    }
}

impl Drop for StopsOnDrop {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// Checks `package` and starts its Program's thread, which loads it and
/// holds `unit` until its isolate is gone, and the task that watches its
/// stop with `stream`. `None` when the package breaks the protocol: the
/// stream closes, with no reopen.
fn load(
    package: RunPackage,
    outbound: mpsc::UnboundedSender<RunRequest>,
    limits: RunLimits,
    unit: HeldUnit,
    run: &RunId,
    stream: HeldRunStream,
) -> Option<Program> {
    if !compatible(package.protocol_level) {
        tracing::warn!(
            coral.run.id = safe::id(run),
            coral.run_executor.protocol_level = safe::number(PROTOCOL_LEVEL),
            coral.run_executor.peer_protocol_level = safe::number(package.protocol_level),
            "Coral's protocol level is more than one apart; closing the Run stream"
        );
        return None;
    }
    if package.sha256[..]
        != crate::compile_wire::package_sha256(&package.script, &package.source_map)
    {
        tracing::warn!(
            coral.run.id = safe::id(run),
            "the package's digest does not match it; closing the Run stream"
        );
        return None;
    }
    // A source map the isolate cannot read would end the Program before
    // `Loaded`.
    if coral_v8::validate_source_map(&package.source_map).is_err() {
        tracing::warn!(
            coral.run.id = safe::id(run),
            "the package's source map is invalid; closing the Run stream"
        );
        return None;
    }
    let Some(value_limits) = package.value_limits.as_ref().and_then(value_limits) else {
        tracing::warn!(
            coral.run.id = safe::id(run),
            "the package's value limits are missing or invalid; closing the Run stream"
        );
        return None;
    };
    let linked = package
        .linked_snapshots
        .into_iter()
        .map(|snapshot| ((snapshot.root(), snapshot.path), snapshot.tool_id))
        .collect();
    let trace_id = coral_telemetry::RunTrace::parse(&package.traceparent)
        .ok()
        .map(|trace| trace.trace_id().as_str().to_owned());
    let host = Arc::new(RunHost::new(
        outbound,
        value_limits,
        &limits,
        linked,
        trace_id,
    ));
    let sha256 = package.sha256;
    let program = coral_v8::CompiledProgram::restore(package.script, package.source_map);
    let Ok(exited) = host.spawn(program, limits, unit) else {
        tracing::warn!(
            coral.run.id = safe::id(run),
            "the Run's thread could not start; closing the Run stream"
        );
        return None;
    };
    let stopping = host.stopping.subscribe();
    let grace = limits.stop_grace;
    let stopped = tokio::spawn(watch_stop(stopping, exited, grace, stream, run.clone()));
    Some(Program {
        host: StopsOnDrop(host),
        stopped,
        sha256,
    })
}

#[cfg(test)]
mod tests;
