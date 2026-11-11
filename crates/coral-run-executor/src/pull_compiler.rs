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
    AnalysisError, CompileError, CompilerClient, Diagnostics, PullStreams, Refused, StreamId,
    Unavailable,
};

/// Why a call too large to send is rejected. Only a typed save's Tool
/// declarations can grow a call that large.
const TOO_LARGE: &str =
    "The source and the Tool declarations that type it are too large to compile; call fewer Tools";

/// Coral's side of the compiles one held pull stream carries: a
/// [`CompilerClient`] that sends each call down the stream as `Compile` and
/// waits for its `Compiled` within the call's time left.
///
/// [`PullStreams`] numbers each call, spends its unit of room, and reads
/// each answer back. A call fails as a busy compiler when the stream has no
/// room, as a stopped one when the stream ends first, and as rejected when
/// it is too large to send.
#[derive(Clone)]
pub struct PullStreamCompiler {
    streams: PullStreams,
    stream: StreamId,
}

impl fmt::Debug for PullStreamCompiler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullStreamCompiler")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl PullStreamCompiler {
    /// The client for `stream`, one of `streams`.
    #[must_use]
    pub const fn new(streams: PullStreams, stream: StreamId) -> Self {
        Self { streams, stream }
    }

    /// Sends `call` and waits for its answer, which is no compile or
    /// prepare failure. The time left runs from `called`, when its caller
    /// called, so building and sending the call count.
    fn call(&self, called: Instant, time_left: Duration, call: Call) -> Result<Read, CompileError> {
        let left = || time_left.saturating_sub(called.elapsed());
        if left().is_zero() {
            return Err(CompileError::TimedOut);
        }
        let time_left_ms = u32::try_from(left().as_millis()).unwrap_or(u32::MAX);
        let answer = self
            .streams
            .compile(self.stream, time_left_ms, call)
            .map_err(|refused| match refused {
                Refused::NoRoom => CompileError::Unavailable(Unavailable::Busy),
                Refused::Closed => CompileError::Unavailable(Unavailable::Stopped),
                // The source, not the compiler, passed the bound. Nothing was
                // sent, so the stream and its compiles go on.
                Refused::TooLarge => CompileError::Diagnostics(Diagnostics::Message {
                    kind: "compilation",
                    message: TOO_LARGE.to_owned(),
                }),
            })?;
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
        match self.call(called, time_left, call).map_err(stopped)? {
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
        match self.call(called, time_left, call)? {
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
        match self.call(called, time_left, call)? {
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
    use crate::Kind;
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
            let stream = streams.insert("a-run-executor".to_owned(), Kind::Dialed, free, outbound);
            let caller = PullStreamCompiler::new(streams.clone(), stream);
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
        let stream = streams.insert("a-run-executor".to_owned(), Kind::Dialed, 1, outbound);
        streams.end(stream, None);
        let ended = PullStreamCompiler::new(streams, stream);
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
        let stream = streams.insert("a-run-executor".to_owned(), Kind::Dialed, 1, outbound);
        let caller = PullStreamCompiler::new(streams, stream);
        let source = "x".repeat(crate::MESSAGE_MAX_BYTES);
        let rejected = caller.compile(TEST_COMPILATION_TIMEOUT, &source, &[], None);
        let rejection = CompileError::Diagnostics(Diagnostics::Message {
            kind: "compilation",
            message: TOO_LARGE.to_owned(),
        });
        assert_eq!(rejected.map(drop), Err(rejection));
    }
}
