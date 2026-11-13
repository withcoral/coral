use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use coral_v8::{
    AnalyzedSource, AuthoredToolCompilation, BuiltinTools, CompiledProgram, CoralDeclarations,
    LinkedToolSource, SourceReferences,
};

use crate::compile_wire::{self as codec, ProtocolError, Read};
use crate::compiler::STOPPED;
use crate::wire::v1::compile::Call;
use crate::{
    AnalysisError, CompileError, CompilerClient, Diagnostics, LocalRunExecutors, PullStreams,
    Refused, StreamId, Unavailable,
};

/// How long [`CompilerClient::ready`] waits for a pull stream with room
/// before no compiler is available. A Run Executor redials within 5 s.
pub(crate) const COMPILE_WAIT: Duration = Duration::from_secs(30);

/// The time left a call keeps when it waits for room, so that it reports
/// that no compiler took it before its caller's own deadline ends the wait.
const ROOM_WAIT_RESERVE: Duration = Duration::from_millis(100);

/// How often a waiting call asks the local children for room again: a
/// request is spent once any room shows, even room another call takes.
const ASK_AGAIN: Duration = Duration::from_secs(1);

/// Why a call too large to send is rejected. Only a typed save grows that
/// large: its Tool declarations total at most 1 MiB, but it carries one
/// declaration of up to 256 KiB for each Private or Shared Tool it calls.
const TOO_LARGE: &str = "The source and the declarations of the custom tools it calls are too large to compile; call fewer custom tools";

/// Coral's side of the compiles the pull streams carry: a
/// [`CompilerClient`] that sends each call down a pull stream as `Compile`
/// and waits for its `Compiled` within the call's time left.
///
/// [`PullStreams`] numbers each call, spends its unit of room, and reads
/// each answer back. A call fails as a stopped compiler when its stream
/// ends first, and as rejected when it is too large to send.
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
    /// Each call goes to the stream with the most room, and waits for one
    /// when none has room, asking the local children for room.
    MostRoom { children: Option<LocalRunExecutors> },
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
    /// most room, ties at random. With no room, [`CompilerClient::ready`]
    /// waits up to [`COMPILE_WAIT`] and a call waits within its time left,
    /// less [`ROOM_WAIT_RESERVE`]; both ask `children`, when there are
    /// local children, for room. A call no stream took fails as
    /// [`Unavailable::NoCompiler`].
    #[must_use]
    pub const fn new(streams: PullStreams, children: Option<LocalRunExecutors>) -> Self {
        Self {
            streams,
            target: Target::MostRoom { children },
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

    /// The stream with the most room, once one has room, or none at
    /// `until`. Each look that finds none asks the local children for room
    /// again, then waits for room, holding no thread.
    async fn room(&self, until: Instant) -> Option<StreamId> {
        let children = match &self.target {
            #[cfg(any(test, feature = "test-support"))]
            Target::Stream(stream) => return Some(*stream),
            Target::MostRoom { children } => children.as_ref(),
        };
        loop {
            if let Some(stream) = self.streams.most_room() {
                return Some(stream);
            }
            if let Some(children) = children {
                children.request_room();
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            self.streams.room_signalled(left.min(ASK_AGAIN)).await;
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
        let millis = |left: Duration| u32::try_from(left.as_millis()).unwrap_or(u32::MAX);
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
        // stream is ending: then it looks for room as any call does.
        let on_held = self
            .held
            .as_ref()
            .and_then(|held| held.take())
            .and_then(|stream| {
                let sent = self
                    .streams
                    .compile_held(stream, millis(left()), call.clone());
                if sent.is_err() {
                    self.streams.let_go(stream);
                }
                sent.ok()
            });
        let answer = if let Some(answer) = on_held {
            answer
        } else {
            // A call no stream took was not judged: it fails as no compiler,
            // never as a compile timeout.
            let no_compiler = || CompileError::Unavailable(Unavailable::NoCompiler);
            let room_until = called + time_left.saturating_sub(ROOM_WAIT_RESERVE);
            loop {
                let stream = self.room(room_until).await.ok_or_else(no_compiler)?;
                let left = left();
                if left.is_zero() {
                    return Err(no_compiler());
                }
                match self.streams.compile(stream, millis(left), call.clone()) {
                    Ok(answer) => break answer,
                    // Another call took the room first: look again.
                    Err(Refused::NoRoom | Refused::Closed)
                        if matches!(self.target, Target::MostRoom { .. }) => {}
                    Err(Refused::NoRoom) => {
                        return Err(CompileError::Unavailable(Unavailable::Busy));
                    }
                    Err(Refused::Closed) => {
                        return Err(STOPPED);
                    }
                    // Checked above.
                    Err(Refused::TooLarge) => return Err(ProtocolError.into()),
                }
            }
        };
        match tokio::time::timeout(left(), answer).await {
            Err(_elapsed) => Err(CompileError::TimedOut),
            // The stream ended first.
            Ok(Err(_closed)) => Err(STOPPED),
            Ok(Ok(Read::Failure(error))) => Err(error),
            Ok(Ok(answer)) => Ok(answer),
        }
    }
}

#[tonic::async_trait]
impl CompilerClient for PullStreamCompiler {
    async fn ready(&self) -> Result<Option<Arc<dyn CompilerClient>>, CompileError> {
        if !matches!(self.target, Target::MostRoom { .. }) || self.held.is_some() {
            return Ok(None);
        }
        let until = Instant::now() + COMPILE_WAIT;
        loop {
            let stream = self
                .room(until)
                .await
                .ok_or(CompileError::Unavailable(Unavailable::NoCompiler))?;
            // Another caller took the room first: look again.
            if self.streams.hold(stream).is_ok() {
                let held = HeldUnit {
                    streams: self.streams.clone(),
                    stream: Mutex::new(Some(stream)),
                };
                return Ok(Some(Arc::new(Self {
                    streams: self.streams.clone(),
                    target: self.target.clone(),
                    held: Some(Arc::new(held)),
                })));
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

    /// A case: its name, the stream's room, the call's time left, what
    /// answers it (none sends nothing), and what the call returns.
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
                1,
                TEST_COMPILATION_TIMEOUT,
                Some(|streams, stream, number| {
                    streams.receive(stream, up(compiled(number))).unwrap();
                }),
                Ok("42"),
            ),
            (
                "a failure",
                1,
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
                1,
                Duration::from_millis(100),
                Some(|_, _, _| {}),
                Err(CompileError::TimedOut),
            ),
            (
                "the stream ends",
                1,
                TEST_COMPILATION_TIMEOUT,
                Some(|streams, stream, _| streams.end(stream, None)),
                Err(stopped.clone()),
            ),
            (
                "no room",
                0,
                TEST_COMPILATION_TIMEOUT,
                None,
                Err(CompileError::Unavailable(Unavailable::Busy)),
            ),
        ];
        for (name, free, time_left, answering, expected) in cases {
            let streams = PullStreams::default();
            let (outbound, mut sent) = mpsc::unbounded_channel();
            let stream = streams.insert(
                "a-run-executor".to_owned(),
                dialed_admission(),
                free,
                outbound,
            );
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
