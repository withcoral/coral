use std::fmt;
use std::sync::mpsc as sync_mpsc;
use std::time::{Duration, Instant};

use coral_v8::{
    AnalyzedSource, CompiledProgram, CoralDeclarations, LinkedSavedFunctionSource,
    SavedFunctionCompilation, SourceReferences,
};

use crate::compile_wire::{self as codec, ProtocolError, Read};
use crate::wire::v1::compile::Call;
use crate::{
    AnalysisError, CompileError, CompilerClient, Diagnostics, LocalRunExecutors, PullStreams,
    Refused, StreamId, Unavailable,
};

/// How long a call waits for a pull stream with room before it fails: the
/// Shutdown Drain's length. A Run Executor redials within 5 s.
pub(crate) const COMPILE_WAIT: Duration = Duration::from_secs(30);

/// How often a waiting call asks the local children for room again: a
/// request is spent once any room shows, even room another call takes.
const ASK_AGAIN: Duration = Duration::from_secs(1);

/// Why a call too large to send is rejected. Only a typed save grows that
/// large: its Tool declarations total at most 1 MiB, but it carries one
/// declaration of up to 256 KiB for each Saved Function it calls.
const TOO_LARGE: &str = "The source and the declarations of the Saved Functions it calls are too large to compile; call fewer Saved Functions";

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
}

#[derive(Clone)]
enum Target {
    /// Every call goes to this stream, and fails as a busy compiler when it
    /// has no room.
    Stream(StreamId),
    /// Each call goes to the stream with the most room, and waits for one
    /// when none has room, asking the local children for room.
    MostRoom {
        children: Option<LocalRunExecutors>,
        wait: Duration,
    },
}

impl fmt::Debug for PullStreamCompiler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stream = match &self.target {
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
    /// waits up to [`COMPILE_WAIT`] and a call waits within its time left;
    /// both ask `children`, when there are local children, for room.
    #[must_use]
    pub const fn new(streams: PullStreams, children: Option<LocalRunExecutors>) -> Self {
        Self::waiting(streams, children, COMPILE_WAIT)
    }

    /// [`Self::new`] with `wait` in place of [`COMPILE_WAIT`].
    #[must_use]
    pub(crate) const fn waiting(
        streams: PullStreams,
        children: Option<LocalRunExecutors>,
        wait: Duration,
    ) -> Self {
        Self {
            streams,
            target: Target::MostRoom { children, wait },
        }
    }

    /// The client for `stream`, one of `streams`, alone.
    #[must_use]
    pub(crate) const fn for_stream(streams: PullStreams, stream: StreamId) -> Self {
        Self {
            streams,
            target: Target::Stream(stream),
        }
    }

    /// The stream with the most room, once one has room, or none at
    /// `until`. Each look asks the local children for room again.
    fn room(&self, until: Instant) -> Option<StreamId> {
        let children = match &self.target {
            Target::Stream(stream) => return Some(*stream),
            Target::MostRoom { children, .. } => children,
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
            self.streams.wait_for_room(left.min(ASK_AGAIN));
        }
    }

    /// Sends `call` and waits for its answer, which is no compile or
    /// prepare failure. The time left runs from `called`, when its caller
    /// called, so building and sending the call count.
    fn call(
        &self,
        called: Instant,
        time_left: Duration,
        call: &Call,
    ) -> Result<Read, CompileError> {
        if time_left.saturating_sub(called.elapsed()).is_zero() {
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
        let left = || time_left.saturating_sub(called.elapsed());
        let answer = loop {
            let stream = self
                .room(called + time_left)
                .ok_or(CompileError::TimedOut)?;
            if left().is_zero() {
                return Err(CompileError::TimedOut);
            }
            let time_left_ms = u32::try_from(left().as_millis()).unwrap_or(u32::MAX);
            match self.streams.compile(stream, time_left_ms, call.clone()) {
                Ok(answer) => break answer,
                // Another call took the room first: look again.
                Err(Refused::NoRoom | Refused::Closed)
                    if matches!(self.target, Target::MostRoom { .. }) => {}
                Err(Refused::NoRoom) => return Err(CompileError::Unavailable(Unavailable::Busy)),
                Err(Refused::Closed) => {
                    return Err(CompileError::Unavailable(Unavailable::Stopped));
                }
                // Checked above.
                Err(Refused::TooLarge) => return Err(ProtocolError.into()),
            }
        };
        match answer.recv_timeout(left()) {
            Ok(Read::Failure(error)) => Err(error),
            Ok(answer) => Ok(answer),
            Err(sync_mpsc::RecvTimeoutError::Timeout) => Err(CompileError::TimedOut),
            Err(sync_mpsc::RecvTimeoutError::Disconnected) => {
                Err(CompileError::Unavailable(Unavailable::Stopped))
            }
        }
    }
}

impl CompilerClient for PullStreamCompiler {
    fn ready(&self) -> Result<(), CompileError> {
        let wait = match &self.target {
            Target::Stream(_) => return Ok(()),
            Target::MostRoom { wait, .. } => *wait,
        };
        self.room(Instant::now() + wait)
            .map(drop)
            .ok_or(CompileError::Unavailable(Unavailable::NoCompiler))
    }

    fn analyze(
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
        match self.call(called, time_left, &call).map_err(stopped)? {
            Read::Analyzed(analyzed) => Ok(analyzed),
            Read::AnalysisFailure(failure) => Err(failure),
            _ => Err(stopped(ProtocolError.into())),
        }
    }

    fn compile(
        &self,
        time_left: Duration,
        source: &str,
        saved_functions: &[LinkedSavedFunctionSource],
        callable: Option<&[String]>,
    ) -> Result<CompiledProgram, CompileError> {
        let called = Instant::now();
        let call = codec::compile_call(source, saved_functions, callable);
        match self.call(called, time_left, &call)? {
            Read::Package(program) => Ok(program),
            _ => Err(ProtocolError.into()),
        }
    }

    fn prepare(
        &self,
        time_left: Duration,
        source: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<SavedFunctionCompilation, CompileError> {
        let called = Instant::now();
        let call = codec::prepare_call(source, declarations);
        match self.call(called, time_left, &call)? {
            Read::Prepared(prepared) => Ok(prepared),
            _ => Err(ProtocolError.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

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

    #[test]
    fn a_call_returns_its_answer_or_fails_as_its_stream_does() {
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
            let calling = thread::spawn(move || {
                let program = caller.compile(time_left, "return 42;", &[], None)?;
                Ok(program.javascript().to_owned())
            });
            if let Some(answering) = answering {
                let Some(Ok(PullResponse {
                    message: Some(pull_response::Message::Compile(compile)),
                })) = sent.blocking_recv()
                else {
                    panic!("a Compile: {name}");
                };
                answering(&streams, stream, compile.number);
            }
            let returned = calling.join().unwrap();
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
        let after = ended.analyze(TEST_COMPILATION_TIMEOUT, &[program]);
        assert_eq!(
            after.map(drop),
            Err(AnalysisError {
                index: 0,
                error: stopped
            }),
            "a call once the stream ended"
        );
    }

    #[test]
    fn a_call_too_large_to_send_is_rejected() {
        let streams = PullStreams::default();
        let (outbound, _sent) = mpsc::unbounded_channel();
        let stream = streams.insert("a-run-executor".to_owned(), dialed_admission(), 1, outbound);
        let caller = PullStreamCompiler::for_stream(streams, stream);
        let source = "x".repeat(crate::MESSAGE_MAX_BYTES);
        let rejected = caller.compile(TEST_COMPILATION_TIMEOUT, &source, &[], None);
        let rejection = CompileError::Diagnostics(Diagnostics::Message {
            kind: "compilation",
            message: TOO_LARGE.to_owned(),
        });
        assert_eq!(rejected.map(drop), Err(rejection));
    }
}

#[cfg(test)]
mod routing_tests;
