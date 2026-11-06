//! One Run on a Run Executor: its Run stream, and its Program on a thread of
//! its own.

mod host;

use std::sync::Arc;

use coral_telemetry::safe;
use sha2::{Digest as _, Sha256};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::PROTOCOL_LEVEL;
use crate::link::{Client, compatible};
use crate::wire::v1::{
    Open, Package, RunRequest, RunResponse, TakenRun, ValueLimits, run_request, run_response,
};
use host::RunHost;

/// The Applied cap: while the requests above the committed watermark weigh
/// more than this, the Program's next request waits. A request weighs its
/// encoded size, so a Run holds at most the cap plus one request.
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
}

impl Default for RunLimits {
    fn default() -> Self {
        Self {
            isolate: coral_v8::IsolateLimits::defaults(),
            applied_cap_bytes: APPLIED_CAP_BYTES,
        }
    }
}

/// The SHA-256 digest a [`Package`] carries: of the script's length as eight
/// big-endian bytes, the script, then the source map.
#[must_use]
pub fn package_digest(script: &str, source_map: &str) -> Vec<u8> {
    let length = u64::try_from(script.len()).unwrap_or(u64::MAX);
    Sha256::new()
        .chain_update(length.to_be_bytes())
        .chain_update(script)
        .chain_update(source_map)
        .finalize()
        .to_vec()
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
pub(crate) async fn hold(mut client: Client, taken: TakenRun, limits: RunLimits) {
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
    let Some((host, exited)) = load(package, outbound, limits) else {
        return;
    };
    while let Ok(Some(RunResponse {
        message: Some(message),
    })) = inbound.message().await
    {
        match message {
            run_response::Message::Started(started) => host.started(&started),
            run_response::Message::Committed(committed) => {
                host.committed(committed.request_number);
            }
            run_response::Message::ToolResult(result) => {
                if !host.tool_result(result) {
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
    host.stop();
    drop(exited.await);
}

/// Checks `package` and starts its Program's thread, which loads it. `None`
/// when the package breaks the protocol.
fn load(
    package: Package,
    outbound: mpsc::UnboundedSender<RunRequest>,
    limits: RunLimits,
) -> Option<(Arc<RunHost>, oneshot::Receiver<()>)> {
    if !compatible(package.protocol_level) {
        tracing::warn!(
            coral.run_executor.protocol_level = safe::number(PROTOCOL_LEVEL),
            coral.run_executor.peer_protocol_level = safe::number(package.protocol_level),
            "Coral's protocol level is more than one apart; closing the Run stream"
        );
        return None;
    }
    if package.sha256 != package_digest(&package.script, &package.source_map) {
        tracing::warn!("the package's digest does not match it; closing the Run stream");
        return None;
    }
    // A source map the isolate cannot read would end the Program before
    // `Loaded`.
    if coral_v8::validate_source_map(&package.source_map).is_err() {
        tracing::warn!("the package's source map is invalid; closing the Run stream");
        return None;
    }
    let value_limits = value_limits(package.value_limits.as_ref()?)?;
    let linked = package
        .linked_snapshots
        .into_iter()
        .map(|snapshot| (snapshot.path, snapshot.saved_function_id))
        .collect();
    let trace_id = coral_telemetry::RunTrace::parse(&package.traceparent)
        .ok()
        .map(|trace| trace.trace_id().as_str().to_owned());
    let host = Arc::new(RunHost::new(
        outbound,
        value_limits,
        limits.applied_cap_bytes,
        linked,
        trace_id,
    ));
    let program = coral_v8::CompiledProgram::restore(package.script, package.source_map);
    let Ok(exited) = host.spawn(program, limits.isolate) else {
        tracing::warn!("the Run's thread could not start; closing the Run stream");
        return None;
    };
    Some((host, exited))
}

#[cfg(test)]
mod tests;
