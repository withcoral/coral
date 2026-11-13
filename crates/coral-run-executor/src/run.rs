//! One Run on a Run Executor: its Run stream, and its Program on a thread of
//! its own.

mod host;

use std::sync::Arc;

use coral_telemetry::safe;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::PROTOCOL_LEVEL;
use crate::link::{Client, HeldRun, compatible};
use crate::wire::v1::{
    Open, RunPackage, RunRequest, RunResponse, TakenRun, ValueLimits, run_request, run_response,
};
use host::RunHost;

/// The Applied cap: while more output bytes than this are above the
/// committed watermark, the Program's next request waits. Emissions and Logs
/// count their output bytes; an Invoke counts none.
pub const APPLIED_CAP_BYTES: usize = 8 << 20;
/// Maximum UTF-8 byte length of one Program Log message.
pub const PROGRAM_LOG_MESSAGE_MAX_BYTES: usize = 64 * 1024;

/// What each Run on a Run Executor runs within.
#[derive(Clone, Copy, Debug)]
pub struct RunLimits {
    /// The heap limits of each Run's isolate.
    pub isolate: coral_v8::IsolateLimits,
    /// The Applied cap, [`APPLIED_CAP_BYTES`] outside tests.
    pub applied_cap_bytes: usize,
    /// Told the number of each request that parks on the Applied cap, so a
    /// test can wait for it.
    #[cfg(test)]
    pub(crate) on_parked: Option<&'static mpsc::UnboundedSender<u64>>,
}

impl Default for RunLimits {
    fn default() -> Self {
        Self {
            isolate: coral_v8::IsolateLimits::defaults(),
            applied_cap_bytes: APPLIED_CAP_BYTES,
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

/// Holds one Run Coral took for this Run Executor until its stream ends.
pub(crate) async fn hold(mut client: Client, taken: TakenRun, limits: RunLimits, unit: HeldRun) {
    let (outbound, requests) = mpsc::unbounded_channel();
    let open = Open {
        run_id: taken.run_id,
        claim_id: taken.claim_id,
        protocol_level: PROTOCOL_LEVEL,
    };
    drop(outbound.send(RunRequest {
        message: Some(run_request::Message::Open(open)),
    }));
    let Ok(response) = client.run(UnboundedReceiverStream::new(requests)).await else {
        return;
    };
    let mut inbound = response.into_inner();
    // Anything but a package, such as `Ended` or a `Committed` for a Run
    // never loaded here, closes the stream.
    let Ok(Some(RunResponse {
        message: Some(run_response::Message::Package(package)),
    })) = inbound.message().await
    else {
        return;
    };
    let Some((host, exited)) = load(package, outbound, limits, unit) else {
        return;
    };
    let host = StopsOnDrop(host);
    while let Ok(Some(RunResponse {
        message: Some(message),
    })) = inbound.message().await
    {
        match message {
            run_response::Message::Started(started) => host.started(&started),
            run_response::Message::Committed(committed) => {
                host.committed(committed.request_number);
            }
            run_response::Message::InvocationResult(result) => {
                if !host.invocation_result(result) {
                    break;
                }
            }
            run_response::Message::Terminate(_) => host.stop(),
            // `Ended` frees the unit; a second package breaks the protocol.
            run_response::Message::Ended(_) | run_response::Message::Package(_) => break,
        }
    }
    // A Program still running ends with its stream; #2898 reopens it instead.
    // The unit frees once its isolate is gone.
    drop(host);
    drop(exited.await);
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
/// holds `unit` until its isolate is gone. `None` when the package breaks
/// the protocol.
fn load(
    package: RunPackage,
    outbound: mpsc::UnboundedSender<RunRequest>,
    limits: RunLimits,
    unit: HeldRun,
) -> Option<(Arc<RunHost>, oneshot::Receiver<()>)> {
    if !compatible(package.protocol_level) {
        tracing::warn!(
            coral.run_executor.protocol_level = safe::number(PROTOCOL_LEVEL),
            coral.run_executor.peer_protocol_level = safe::number(package.protocol_level),
            "Coral's protocol level is more than one apart; closing the Run stream"
        );
        return None;
    }
    if package.sha256[..]
        != crate::compile_wire::package_sha256(&package.script, &package.source_map)
    {
        tracing::warn!("the package's digest does not match it; closing the Run stream");
        return None;
    }
    // A source map the isolate cannot read would end the Program before
    // `Loaded`.
    if coral_v8::validate_source_map(&package.source_map).is_err() {
        tracing::warn!("the package's source map is invalid; closing the Run stream");
        return None;
    }
    let Some(value_limits) = package.value_limits.as_ref().and_then(value_limits) else {
        tracing::warn!("the package's value limits are missing or invalid; closing the Run stream");
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
    let program = coral_v8::CompiledProgram::restore(package.script, package.source_map);
    let Ok(exited) = host.spawn(program, limits.isolate, unit) else {
        tracing::warn!("the Run's thread could not start; closing the Run stream");
        return None;
    };
    Some((host, exited))
}

#[cfg(test)]
mod tests;
