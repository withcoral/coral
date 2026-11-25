use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;

use coral_v8::{
    AnalyzedSource, AuthoredToolCompilation, BuiltinTools, CompiledProgram, CoralDeclarations,
    LinkedToolSource, SourceReferences,
};

use crate::compile_wire::{self as codec, ProtocolError, Read};
use crate::compiler::STOPPED;
use crate::pull::LineEnded;
use crate::relay::{CompileRelay, Probed};
use crate::time::millis;
use crate::wire::v1::compile::Call;
use crate::{
    AnalysisError, CompileError, CompilerClient, Diagnostics, LocalRunExecutors, PullStreams,
    Refused, StreamId, Unavailable,
};

/// How long [`CompilerClient::ready`] waits for a pull stream with compile
/// room before no compiler is available: no Run Executor is linked, or
/// every one's compiler stays busy. A Run Executor redials within 5 s.
pub(crate) const COMPILE_WAIT: Duration = Duration::from_secs(30);

/// The time left a call keeps when it waits for room, so that it reports
/// that no compiler took it before its caller's own deadline ends the wait.
const ROOM_WAIT_RESERVE: Duration = Duration::from_millis(100);

/// How often a waiting call asks the local children for room again, and
/// looks again whether this Coral Instance still holds a stream to wait on:
/// a call that joined the line as it ended hears nothing.
const ASK_AGAIN: Duration = Duration::from_secs(1);

/// Why a call too large to send is rejected. Only a typed save grows that
/// large: its Tool declarations total at most 1 MiB, but it carries one
/// declaration of up to 256 KiB for each Private or Shared Tool it calls.
const TOO_LARGE: &str = "The source and the declarations of the custom tools it calls are too large to compile; call fewer custom tools";

/// Coral's side of the compiles the pull streams carry: a
/// [`CompilerClient`] that sends each call down a pull stream as `Compile`
/// and waits for its `Compiled` within the call's time left.
///
/// [`PullStreams`] numbers each call, spends its unit of compile room, and
/// reads each answer back. A call whose stream ends first is routed once
/// more, since a compile changes nothing, while it has more time left than
/// `ROOM_WAIT_RESERVE`. It fails as a stopped compiler when no other stream
/// takes it then, or when that stream ends first or runs out of time too,
/// and as rejected when it is too large to send.
#[derive(Clone)]
pub struct PullStreamCompiler {
    streams: PullStreams,
    target: Target,
    /// The unit [`CompilerClient::ready`] held for this caller's first call.
    held: Option<Arc<HeldUnit>>,
}

/// One unit of a stream's room held for one caller until its first call
/// spends it. Dropped unspent, it frees the unit.
struct HeldUnit {
    streams: PullStreams,
    stream: Mutex<Option<StreamId>>,
}

impl HeldUnit {
    fn take(&self) -> Option<StreamId> {
        self.stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

impl Drop for HeldUnit {
    fn drop(&mut self) {
        if let Some(stream) = self.take() {
            self.streams.let_go(stream);
        }
    }
}

#[derive(Clone)]
enum Target {
    /// Every call goes to this stream, and fails as a busy compiler when it
    /// has no room. Only tests send to one stream alone.
    #[cfg(any(test, feature = "test-support"))]
    Stream(StreamId),
    /// Each call goes to the stream with the most room. When none has
    /// room, it waits in line for room here. Only while this Coral Instance
    /// holds no stream that takes sends does it go through `relay` to any
    /// Coral Instance with room. Both ask the local children for room.
    MostRoom {
        children: Option<LocalRunExecutors>,
        relay: Option<CompileRelay>,
    },
}

/// Where a call goes.
enum Route<'a> {
    /// A unit of this stream's compile room, held for the call.
    Held(StreamId),
    /// The one stream of a [`Target::Stream`], with no unit held.
    #[cfg(any(test, feature = "test-support"))]
    Stream(StreamId),
    Relay(&'a CompileRelay, Option<&'a LocalRunExecutors>),
}

/// Where a call went: to a stream, whose answer is to come, or through the
/// relay, which answered.
enum Routed {
    Stream(oneshot::Receiver<Read>),
    Relayed(Read),
}

impl fmt::Debug for PullStreamCompiler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stream: Option<&StreamId> = match &self.target {
            #[cfg(any(test, feature = "test-support"))]
            Target::Stream(stream) => Some(stream),
            Target::MostRoom { .. } => None,
        };
        formatter
            .debug_struct("PullStreamCompiler")
            .field("stream", &stream)
            .finish_non_exhaustive()
    }
}

impl PullStreamCompiler {
    /// The client that sends each call to the stream in `streams` with the
    /// most compile room, ties as [`PullStreams`] breaks them. A full set of
    /// Runs takes none of it. With no room, callers wait in line, oldest
    /// first: [`CompilerClient::ready`] up to `COMPILE_WAIT`, and a call
    /// within its time left, less `ROOM_WAIT_RESERVE`. Both ask `children`,
    /// when there are local children, for room. A call no stream took fails as
    /// [`Unavailable::NoCompiler`], or as [`Unavailable::Stopped`] once its
    /// stream ended first.
    #[must_use]
    pub const fn new(streams: PullStreams, children: Option<LocalRunExecutors>) -> Self {
        Self {
            streams,
            target: Target::MostRoom {
                children,
                relay: None,
            },
            held: None,
        }
    }

    /// [`Self::new`], relaying a call through the database while this Coral
    /// Instance holds no stream that takes sends. [`CompilerClient::ready`]
    /// then waits up to [`COMPILE_WAIT`] for room on any Coral Instance. A
    /// Coral Instance that holds a stream waits for its own room.
    pub(crate) const fn relayed(
        streams: PullStreams,
        children: Option<LocalRunExecutors>,
        relay: CompileRelay,
    ) -> Self {
        Self {
            streams,
            target: Target::MostRoom {
                children,
                relay: Some(relay),
            },
            held: None,
        }
    }

    /// The client for `stream`, one of `streams`, alone.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub(crate) const fn for_stream(streams: PullStreams, stream: StreamId) -> Self {
        Self {
            streams,
            target: Target::Stream(stream),
            held: None,
        }
    }

    /// A unit of compile room held on the stream with the most, now or once
    /// every caller in line before this one took one, or none at `until`.
    /// A caller whose unit or stream went before its call was answered
    /// waits `at_front` of the line. While this Coral Instance holds no
    /// stream that takes sends, the relay instead, when there is one: the
    /// line ends with the last such stream. The wait asks the local
    /// children for room every `ASK_AGAIN`, and holds no thread.
    async fn route(&self, until: Instant, at_front: bool) -> Option<Route<'_>> {
        let (children, relay) = match &self.target {
            #[cfg(any(test, feature = "test-support"))]
            Target::Stream(stream) => return Some(Route::Stream(*stream)),
            Target::MostRoom { children, relay } => (children.as_ref(), relay.as_ref()),
        };
        let mut waiting = None;
        loop {
            // A stream here frees its room within a compile; the relay costs
            // writes to the database for each call.
            if let Some(relay) = relay
                && !self.streams.has_open_stream()
            {
                return Some(Route::Relay(relay, children));
            }
            let turn = match &mut waiting {
                Some(turn) => turn,
                None => match self.streams.compile_turn(at_front) {
                    Ok(stream) => return Some(Route::Held(stream)),
                    Err(turn) => waiting.insert(turn),
                },
            };
            if let Some(children) = children {
                children.request_room();
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            match turn.handed(left.min(ASK_AGAIN)).await {
                Ok(Some(stream)) => return Some(Route::Held(stream)),
                Ok(None) => {}
                // Relay, or wait in a line that starts again.
                Err(LineEnded) => waiting = None,
            }
        }
    }

    /// Sends `call` and waits for its answer, which is no compile or
    /// prepare failure. The time left runs from `called`, when its caller
    /// called, so building and sending the call count.
    async fn call(
        &self,
        called: Instant,
        time_left: Duration,
        call: &Call,
    ) -> Result<Read, CompileError> {
        let until = called + time_left;
        let left = || until.saturating_duration_since(Instant::now());
        if left().is_zero() {
            return Err(CompileError::TimedOut);
        }
        // The source, not the compiler, passed the bound: rejected before
        // it waits for room, and nothing is sent.
        if crate::pull::too_large(call) {
            return Err(CompileError::Diagnostics(Diagnostics::Message {
                kind: "compilation",
                message: TOO_LARGE.to_owned(),
            }));
        }
        // The first call spends the unit `ready` held for it, unless its
        // stream is ending: then it looks for room again, at the front.
        let held_for_it = self.held.as_ref().and_then(|held| held.take());
        let mut on_held = held_for_it.and_then(|stream| {
            self.streams
                .compile_held(stream, millis(left()), call.clone())
                .ok()
        });
        let lost_its_unit = held_for_it.is_some() && on_held.is_none();
        let room_until = called + time_left.saturating_sub(ROOM_WAIT_RESERVE);
        // Whether a stream the call went to ended before it answered, so it
        // was routed once more.
        let mut routed_again = false;
        loop {
            let answer = match on_held.take() {
                Some(answer) => answer,
                None => match self
                    .send(call, until, room_until, lost_its_unit || routed_again)
                    .await
                {
                    Ok(Routed::Stream(answer)) => answer,
                    Ok(Routed::Relayed(read)) => return Ok(read),
                    // No other stream took it: it fails as the one that ended.
                    Err(CompileError::Unavailable(Unavailable::NoCompiler)) if routed_again => {
                        return Err(STOPPED);
                    }
                    Err(error) => return Err(error),
                },
            };
            match tokio::time::timeout(left(), answer).await {
                // Routed again, it had less than its time: no judgement of
                // the source.
                Err(_) | Ok(Ok(Read::Failure(CompileError::TimedOut))) if routed_again => {
                    return Err(STOPPED);
                }
                Err(_elapsed) => return Err(CompileError::TimedOut),
                // The stream ended first: it is routed once more. A call that
                // ends each stream it reaches ends no more than two.
                Ok(Err(_closed))
                    if !routed_again
                        && matches!(self.target, Target::MostRoom { .. })
                        && Instant::now() < room_until =>
                {
                    routed_again = true;
                }
                Ok(Err(_closed)) => return Err(STOPPED),
                Ok(Ok(Read::Failure(error))) => return Err(error),
                Ok(Ok(answer)) => return Ok(answer),
            }
        }
    }

    /// Sends `call` to the stream [`Self::route`] finds by `room_until`, or
    /// relays it and waits for the relay's answer, before `until`. A call no
    /// stream took fails as [`Unavailable::NoCompiler`].
    async fn send(
        &self,
        call: &Call,
        until: Instant,
        room_until: Instant,
        mut at_front: bool,
    ) -> Result<Routed, CompileError> {
        // A call no stream took was not judged: it fails as no compiler,
        // never as a compile timeout.
        let no_compiler = || CompileError::Unavailable(Unavailable::NoCompiler);
        loop {
            let route = self.route(room_until, at_front).await;
            let (stream, held) = match route.ok_or_else(no_compiler)? {
                // Untaken, it answers before the caller's own deadline, as a
                // wait for room here does.
                Route::Relay(relay, children) => {
                    return relay.call(call, children, until).await.map(Routed::Relayed);
                }
                Route::Held(stream) => (stream, true),
                #[cfg(any(test, feature = "test-support"))]
                Route::Stream(stream) => (stream, false),
            };
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                if held {
                    self.streams.let_go(stream);
                }
                return Err(no_compiler());
            }
            let sent = if held {
                self.streams
                    .compile_held(stream, millis(left), call.clone())
            } else {
                self.streams.compile(stream, millis(left), call.clone())
            };
            match sent {
                Ok(answer) => return Ok(Routed::Stream(answer)),
                // Its stream is ending: look again, at the front.
                Err(Refused::NoRoom | Refused::Closed)
                    if matches!(self.target, Target::MostRoom { .. }) =>
                {
                    at_front = true;
                }
                Err(Refused::NoRoom) => {
                    return Err(CompileError::Unavailable(Unavailable::Busy));
                }
                Err(Refused::Closed) => return Err(STOPPED),
                // Checked in `call`.
                Err(Refused::TooLarge) => return Err(ProtocolError.into()),
            }
        }
    }
}

impl PullStreamCompiler {
    /// A client whose first call spends the unit of `stream`'s compile room
    /// held for it.
    fn holding(&self, stream: StreamId) -> Arc<dyn CompilerClient> {
        let held = HeldUnit {
            streams: self.streams.clone(),
            stream: Mutex::new(Some(stream)),
        };
        Arc::new(Self {
            streams: self.streams.clone(),
            target: self.target.clone(),
            held: Some(Arc::new(held)),
        })
    }
}

#[tonic::async_trait]
impl CompilerClient for PullStreamCompiler {
    async fn ready(&self) -> Result<Option<Arc<dyn CompilerClient>>, CompileError> {
        if !matches!(self.target, Target::MostRoom { .. }) || self.held.is_some() {
            return Ok(None);
        }
        let no_compiler = || CompileError::Unavailable(Unavailable::NoCompiler);
        let until = Instant::now() + COMPILE_WAIT;
        loop {
            match self.route(until, false).await.ok_or_else(no_compiler)? {
                Route::Held(stream) => return Ok(Some(self.holding(stream))),
                // Only a `Target::Stream`, which `ready` leaves as it is.
                #[cfg(any(test, feature = "test-support"))]
                Route::Stream(_) => return Ok(None),
                // No room here: wait for room anywhere. Room that shows here,
                // as a local child that started, is held on the next look, or
                // waited for again when another caller took it; room
                // elsewhere relays the calls.
                Route::Relay(relay, children) => match relay.room(children, until).await {
                    Probed::Here => {}
                    Probed::Elsewhere => return Ok(None),
                    Probed::Nobody => return Err(no_compiler()),
                },
            }
        }
    }

    async fn analyze(
        &self,
        time_left: Duration,
        sources: &[AnalyzedSource],
    ) -> Result<Vec<SourceReferences>, AnalysisError> {
        // Nothing to analyze takes no room, as in process.
        if sources.is_empty() {
            return Ok(Vec::new());
        }
        let called = Instant::now();
        // A call that never reached a source stops at the first.
        let stopped = |error| AnalysisError { index: 0, error };
        let call = codec::analyze_call(sources);
        match self.call(called, time_left, &call).await.map_err(stopped)? {
            Read::Analyzed(analyzed) => Ok(analyzed),
            Read::AnalysisFailure(failure) => Err(failure),
            _ => Err(stopped(ProtocolError.into())),
        }
    }

    async fn compile(
        &self,
        time_left: Duration,
        source: &str,
        linked_tools: &[LinkedToolSource],
        builtin_tools: &BuiltinTools,
        callable: Option<&[String]>,
    ) -> Result<CompiledProgram, CompileError> {
        let called = Instant::now();
        let call = codec::compile_call(source, linked_tools, builtin_tools, callable);
        match self.call(called, time_left, &call).await? {
            Read::Package(program) => Ok(program),
            _ => Err(ProtocolError.into()),
        }
    }

    async fn prepare(
        &self,
        time_left: Duration,
        source: &str,
        builtin_tools: &BuiltinTools,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<AuthoredToolCompilation, CompileError> {
        let called = Instant::now();
        let call = codec::prepare_call(source, builtin_tools, declarations);
        match self.call(called, time_left, &call).await? {
            Read::Prepared(prepared) => Ok(prepared),
            _ => Err(ProtocolError.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use coral_test_support::TEST_COMPILATION_TIMEOUT;
    use tokio::sync::mpsc;

    use super::*;
    use crate::pull::dialed_admission;
    use crate::pull::tests::{compiled, up};
    use crate::wire::v1::compile_failure::Failure;
    use crate::wire::v1::compiled::Answer;
    use crate::wire::v1::{self as wire, Compiled, PullResponse, pull_response};

    /// What Coral's side does once the call's `Compile`, with this number,
    /// is sent on the stream.
    type Answering = fn(&PullStreams, StreamId, u64);

    /// A case: its name, the units of the stream's compile room held
    /// before the call, the call's time left, what answers it (none sends
    /// nothing), and what the call returns.
    type Case = (
        &'static str,
        u32,
        Duration,
        Option<Answering>,
        Result<&'static str, CompileError>,
    );

    #[tokio::test(start_paused = true)]
    async fn a_call_returns_its_answer_or_fails_as_its_stream_does() {
        let stopped = CompileError::Unavailable(Unavailable::Stopped);
        let cases: [Case; 5] = [
            (
                "an answer",
                0,
                TEST_COMPILATION_TIMEOUT,
                Some(|streams, stream, number| {
                    streams.receive(stream, up(compiled(number))).unwrap();
                }),
                Ok("42"),
            ),
            (
                "a failure",
                0,
                TEST_COMPILATION_TIMEOUT,
                Some(|streams, stream, number| {
                    let failure = wire::CompileFailure {
                        failure: Some(Failure::MemoryLimit(true)),
                    };
                    let answer = Some(Answer::Failure(failure));
                    streams
                        .receive(stream, up(Compiled { number, answer }))
                        .unwrap();
                }),
                Err(CompileError::MemoryLimit),
            ),
            (
                "no answer in time",
                0,
                Duration::from_millis(100),
                Some(|_, _, _| {}),
                Err(CompileError::TimedOut),
            ),
            (
                "the stream ends",
                0,
                TEST_COMPILATION_TIMEOUT,
                Some(|streams, stream, _| streams.end(stream, None)),
                Err(stopped.clone()),
            ),
            (
                "a busy compiler",
                crate::pull::COMPILE_ROOM,
                TEST_COMPILATION_TIMEOUT,
                None,
                Err(CompileError::Unavailable(Unavailable::Busy)),
            ),
        ];
        for (name, busy, time_left, answering, expected) in cases {
            let streams = PullStreams::default();
            let (outbound, mut sent) = mpsc::unbounded_channel();
            // No room for Runs: a compile needs none.
            let stream =
                streams.insert("a-run-executor".to_owned(), dialed_admission(), 0, outbound);
            for _unit in 0..busy {
                streams.hold_compile(stream).unwrap();
            }
            let caller = PullStreamCompiler::for_stream(streams.clone(), stream);
            let calling = tokio::spawn(async move {
                let builtin_tools = BuiltinTools::default();
                let program = caller
                    .compile(time_left, "return 42;", &[], &builtin_tools, None)
                    .await?;
                Ok::<_, CompileError>(program.javascript().to_owned())
            });
            if let Some(answering) = answering {
                let Some(Ok(PullResponse {
                    message: Some(pull_response::Message::Compile(compile)),
                })) = sent.recv().await
                else {
                    panic!("a Compile: {name}");
                };
                answering(&streams, stream, compile.number);
            }
            let returned = calling.await.unwrap();
            assert_eq!(
                returned.as_deref().map_err(Clone::clone),
                expected,
                "{name}"
            );
            if answering.is_none() {
                assert!(sent.try_recv().is_err(), "{name} sends nothing");
            }
        }
        let streams = PullStreams::default();
        let (outbound, _sent) = mpsc::unbounded_channel();
        let stream = streams.insert("a-run-executor".to_owned(), dialed_admission(), 1, outbound);
        streams.end(stream, None);
        let ended = PullStreamCompiler::for_stream(streams, stream);
        let program = AnalyzedSource::Program("return 1;".to_owned());
        let after = ended.analyze(TEST_COMPILATION_TIMEOUT, &[program]).await;
        assert_eq!(
            after.map(drop),
            Err(AnalysisError {
                index: 0,
                error: stopped
            }),
            "a call once the stream ended"
        );
    }

    #[tokio::test]
    async fn a_call_too_large_to_send_is_rejected() {
        let streams = PullStreams::default();
        let (outbound, _sent) = mpsc::unbounded_channel();
        let stream = streams.insert("a-run-executor".to_owned(), dialed_admission(), 1, outbound);
        let caller = PullStreamCompiler::for_stream(streams, stream);
        let source = "x".repeat(crate::MESSAGE_MAX_BYTES);
        let builtin_tools = BuiltinTools::default();
        let rejected = caller
            .compile(TEST_COMPILATION_TIMEOUT, &source, &[], &builtin_tools, None)
            .await;
        let rejection = CompileError::Diagnostics(Diagnostics::Message {
            kind: "compilation",
            message: TOO_LARGE.to_owned(),
        });
        assert_eq!(rejected.map(drop), Err(rejection));
    }
}

#[cfg(test)]
mod routing_tests;
