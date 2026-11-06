use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Mutex, MutexGuard, PoisonError, mpsc as sync_mpsc};
use std::time::{Duration, Instant};

use coral_v8::{
    CompiledProgram, CoralDeclarations, LinkedSavedFunctionSource, SavedFunctionCallPaths,
    SavedFunctionCompilation, SavedFunctionReference, ToolReferences, UnknownToolCall,
};
use tokio::sync::mpsc;

use crate::compile_wire::{self as codec, ProtocolError};
use crate::wire::v1::compile::Call;
use crate::wire::v1::compiled::Answer;
use crate::wire::v1::{self as wire, Compile, Compiled, PullResponse, pull_response};
use crate::{CompileError, CompilerClient, Unavailable};

/// Coral's side of the compiles one held pull stream carries: a
/// [`CompilerClient`] that sends each call down the stream as `Compile` and
/// waits for its `Compiled` within the call's time left.
///
/// The handler that holds the stream hands it each `Compiled` with
/// [`Self::answer`], and calls [`Self::close`] when the stream ends.
#[derive(Debug)]
pub struct PullStreamCompiler {
    outbound: mpsc::UnboundedSender<PullResponse>,
    calls: Mutex<Calls>,
}

#[derive(Debug, Default)]
struct Calls {
    /// The number of the last call sent: calls are numbered from 1.
    last: u64,
    /// The calls that wait for their answer, by number.
    waiting: HashMap<u64, Waiting>,
    closed: bool,
}

#[derive(Debug)]
struct Waiting {
    takes: Takes,
    answered: sync_mpsc::SyncSender<Read>,
}

/// The answer a call takes, besides a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Takes {
    CallPaths,
    UnknownToolCalls,
    References,
    Package,
    Prepared,
}

impl Takes {
    const fn of(call: &Call) -> Self {
        match call {
            Call::ProgramReferences(_) | Call::CallPaths(_) => Self::CallPaths,
            Call::UnknownToolCalls(_) => Self::UnknownToolCalls,
            Call::DependencyReferences(_) => Self::References,
            Call::Compile(_) => Self::Package,
            Call::Prepare(_) => Self::Prepared,
        }
    }
}

/// An answer read back into the value the in-process client returns.
#[derive(Debug)]
enum Read {
    CallPaths(SavedFunctionCallPaths),
    UnknownToolCalls(Vec<UnknownToolCall>),
    References(Vec<SavedFunctionReference>),
    Package(CompiledProgram),
    Prepared(SavedFunctionCompilation),
    Failure(CompileError),
}

impl Read {
    fn answer(answer: Answer) -> Result<Self, ProtocolError> {
        Ok(match answer {
            Answer::CallPaths(paths) => Self::CallPaths(codec::saved_function_call_paths(paths)?),
            Answer::UnknownToolCalls(calls) => {
                Self::UnknownToolCalls(codec::unknown_tool_calls(calls))
            }
            Answer::References(found) => Self::References(
                found
                    .references
                    .into_iter()
                    .map(codec::saved_function_reference)
                    .collect(),
            ),
            Answer::Package(package) => Self::Package(codec::compiled_program(package)?),
            Answer::Prepared(prepared) => {
                Self::Prepared(codec::saved_function_compilation(prepared)?)
            }
            Answer::Failure(failure) => Self::Failure(codec::compile_error(failure)?),
        })
    }

    const fn answers(&self, takes: Takes) -> bool {
        matches!(
            (self, takes),
            (Self::Failure(_), _)
                | (Self::CallPaths(_), Takes::CallPaths)
                | (Self::UnknownToolCalls(_), Takes::UnknownToolCalls)
                | (Self::References(_), Takes::References)
                | (Self::Package(_), Takes::Package)
                | (Self::Prepared(_), Takes::Prepared)
        )
    }
}

impl PullStreamCompiler {
    /// A client that sends its calls on `outbound`, the pull stream's Coral
    /// side.
    #[must_use]
    pub fn new(outbound: mpsc::UnboundedSender<PullResponse>) -> Self {
        Self {
            outbound,
            calls: Mutex::default(),
        }
    }

    /// Hands `compiled` to the call it answers. An answer to a call that
    /// already ended is read, then dropped.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] for an answer to a number never sent, an
    /// answer of the wrong kind, a missing field, or a package whose digest
    /// does not match. It changes nothing: the handler then ends the stream,
    /// and [`Self::close`] fails the calls in flight. Only a call still
    /// waiting shows the kind it takes.
    pub fn answer(&self, compiled: Compiled) -> Result<(), ProtocolError> {
        let answer = Read::answer(compiled.answer.ok_or(ProtocolError)?)?;
        let waiting = {
            let mut calls = self.calls();
            if compiled.number == 0 || compiled.number > calls.last {
                return Err(ProtocolError);
            }
            match calls.waiting.entry(compiled.number) {
                Entry::Vacant(_) => return Ok(()),
                Entry::Occupied(waiting) if !answer.answers(waiting.get().takes) => {
                    return Err(ProtocolError);
                }
                Entry::Occupied(waiting) => waiting.remove(),
            }
        };
        // Its caller waits on a channel of one, and this is the only send.
        drop(waiting.answered.try_send(answer));
        Ok(())
    }

    /// Fails the calls in flight, and every later call, as a stopped
    /// compiler: the stream ended.
    pub fn close(&self) {
        let mut calls = self.calls();
        calls.closed = true;
        calls.waiting.clear();
    }

    fn calls(&self) -> MutexGuard<'_, Calls> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sends `call` and waits for its answer, which is no failure. The time
    /// left runs from `called`, when its caller called, so building and
    /// sending the call count.
    fn call(&self, called: Instant, time_left: Duration, call: Call) -> Result<Read, CompileError> {
        let left = || time_left.saturating_sub(called.elapsed());
        if left().is_zero() {
            return Err(CompileError::TimedOut);
        }
        let (answered, answer) = sync_mpsc::sync_channel(1);
        let number = {
            let mut calls = self.calls();
            if calls.closed {
                return Err(CompileError::Unavailable(Unavailable::Stopped));
            }
            calls.last += 1;
            let number = calls.last;
            let takes = Takes::of(&call);
            calls.waiting.insert(number, Waiting { takes, answered });
            number
        };
        let compile = Compile {
            number,
            time_left_ms: u32::try_from(left().as_millis()).unwrap_or(u32::MAX),
            call: Some(call),
        };
        let sent = self.outbound.send(PullResponse {
            message: Some(pull_response::Message::Compile(compile)),
        });
        let answer = match sent {
            Ok(()) => answer.recv_timeout(left()),
            Err(_closed) => Err(sync_mpsc::RecvTimeoutError::Disconnected),
        };
        // A call that ends here takes no late answer.
        self.calls().waiting.remove(&number);
        match answer {
            Ok(Read::Failure(error)) => Err(error),
            Ok(answer) => Ok(answer),
            Err(sync_mpsc::RecvTimeoutError::Timeout) => Err(CompileError::TimedOut),
            Err(sync_mpsc::RecvTimeoutError::Disconnected) => {
                Err(CompileError::Unavailable(Unavailable::Stopped))
            }
        }
    }

    fn named(
        &self,
        called: Instant,
        time_left: Duration,
        call: Call,
    ) -> Result<SavedFunctionCallPaths, CompileError> {
        match self.call(called, time_left, call)? {
            Read::CallPaths(paths) => Ok(paths),
            _ => Err(ProtocolError.into()),
        }
    }
}

fn source(source: &str) -> wire::Source {
    wire::Source {
        source: source.to_owned(),
    }
}

impl CompilerClient for PullStreamCompiler {
    fn program_references(
        &self,
        time_left: Duration,
        source_text: &str,
    ) -> Result<SavedFunctionCallPaths, CompileError> {
        let called = Instant::now();
        self.named(
            called,
            time_left,
            Call::ProgramReferences(source(source_text)),
        )
    }

    fn unknown_tool_calls(
        &self,
        time_left: Duration,
        tools: &ToolReferences,
        callable: &[String],
    ) -> Result<Vec<UnknownToolCall>, CompileError> {
        let called = Instant::now();
        let call = Call::UnknownToolCalls(wire::ToolCalls {
            tools: Some(codec::tools_wire(tools)),
            callable: callable.to_vec(),
        });
        match self.call(called, time_left, call)? {
            Read::UnknownToolCalls(calls) => Ok(calls),
            _ => Err(ProtocolError.into()),
        }
    }

    fn dependency_references(
        &self,
        time_left: Duration,
        source_text: &str,
    ) -> Result<Vec<SavedFunctionReference>, CompileError> {
        let called = Instant::now();
        let call = Call::DependencyReferences(source(source_text));
        match self.call(called, time_left, call)? {
            Read::References(references) => Ok(references),
            _ => Err(ProtocolError.into()),
        }
    }

    fn compile(
        &self,
        time_left: Duration,
        source_text: &str,
        saved_functions: &[LinkedSavedFunctionSource],
    ) -> Result<CompiledProgram, CompileError> {
        let called = Instant::now();
        let call = Call::Compile(wire::ProgramSource {
            source: source_text.to_owned(),
            saved_functions: saved_functions.iter().map(codec::linked_wire).collect(),
        });
        match self.call(called, time_left, call)? {
            Read::Package(program) => Ok(program),
            _ => Err(ProtocolError.into()),
        }
    }

    fn call_paths(
        &self,
        time_left: Duration,
        source_text: &str,
    ) -> Result<SavedFunctionCallPaths, CompileError> {
        let called = Instant::now();
        self.named(called, time_left, Call::CallPaths(source(source_text)))
    }

    fn prepare(
        &self,
        time_left: Duration,
        source_text: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<SavedFunctionCompilation, CompileError> {
        let called = Instant::now();
        let call = Call::Prepare(wire::SavedFunctionSource {
            source: source_text.to_owned(),
            declarations: declarations.map(codec::declarations_wire),
        });
        match self.call(called, time_left, call)? {
            Read::Prepared(prepared) => Ok(prepared),
            _ => Err(ProtocolError.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread::{self, JoinHandle};

    use coral_test_support::TEST_COMPILATION_TIMEOUT;

    use super::*;

    type Calling = (
        Arc<PullStreamCompiler>,
        JoinHandle<Result<String, CompileError>>,
        u64,
    );

    /// A compile that waits for its answer, and the number it was sent as.
    fn calling() -> Calling {
        let (outbound, mut sent) = mpsc::unbounded_channel();
        let compiler = Arc::new(PullStreamCompiler::new(outbound));
        let caller = Arc::clone(&compiler);
        let waiting = thread::spawn(move || {
            let program = caller.compile(TEST_COMPILATION_TIMEOUT, "return 42;", &[])?;
            Ok(program.javascript().to_owned())
        });
        let Some(PullResponse {
            message: Some(pull_response::Message::Compile(compile)),
        }) = sent.blocking_recv()
        else {
            panic!("a Compile");
        };
        (compiler, waiting, compile.number)
    }

    /// A refused answer, and whether it is refused once its call has ended.
    type Refusal = (&'static str, fn(u64) -> Compiled, bool);

    fn package(script: &str, digest: [u8; 32]) -> Answer {
        Answer::Package(wire::Package {
            script: script.to_owned(),
            source_map: "{}".to_owned(),
            sha256: digest.to_vec(),
            diagnostics: Vec::new(),
        })
    }

    fn sound(number: u64) -> Compiled {
        let answer = Some(package("42", codec::package_sha256("42", "{}")));
        Compiled { number, answer }
    }

    #[test]
    fn an_answer_that_breaks_the_protocol_changes_nothing() {
        // Only a waiting call shows the kind it takes.
        let refusals: [Refusal; 6] = [
            ("a number never sent", |number| sound(number + 1), true),
            ("number zero", |_| sound(0), true),
            (
                "an empty answer",
                |number| Compiled {
                    number,
                    answer: None,
                },
                true,
            ),
            (
                "a digest that does not match",
                |number| Compiled {
                    number,
                    answer: Some(package("42", codec::package_sha256("4", "2{}"))),
                },
                true,
            ),
            (
                "a failure with no kind",
                |number| Compiled {
                    number,
                    answer: Some(Answer::Failure(wire::CompileFailure::default())),
                },
                true,
            ),
            (
                "an answer of the wrong kind",
                |number| Compiled {
                    number,
                    answer: Some(Answer::References(wire::References::default())),
                },
                false,
            ),
        ];
        for (name, refusal, refused_late) in refusals {
            let (compiler, waiting, number) = calling();
            let refused = compiler.answer(refusal(number));
            assert!(matches!(refused, Err(ProtocolError)), "{name}");
            // The call still waits, and takes its answer.
            compiler.answer(sound(number)).unwrap();
            assert_eq!(waiting.join().unwrap(), Ok("42".to_owned()), "{name}");
            let late = compiler.answer(refusal(number));
            assert_eq!(late.is_err(), refused_late, "{name}, once the call ended");
            assert!(compiler.answer(sound(number)).is_ok(), "{name}");
        }
    }

    #[test]
    fn a_closed_stream_fails_its_calls_as_a_stopped_compiler() {
        let (compiler, waiting, _) = calling();
        compiler.close();
        let stopped = CompileError::Unavailable(Unavailable::Stopped);
        assert_eq!(waiting.join().unwrap(), Err(stopped.clone()));
        let after = compiler.program_references(TEST_COMPILATION_TIMEOUT, "return 1;");
        assert_eq!(after.map(drop), Err(stopped));
    }
}
