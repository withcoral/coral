use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError, mpsc as sync_mpsc};
use std::time::Duration;

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
    waiting: HashMap<u64, sync_mpsc::SyncSender<Answer>>,
    closed: bool,
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
    /// already timed out is dropped unread.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] for an answer to a number never sent, an
    /// empty answer, or a package whose digest does not match; the handler
    /// then ends the stream. An answer of the wrong kind, or one that misses
    /// a field, fails only its call, as a compiler failure.
    pub fn answer(&self, compiled: Compiled) -> Result<(), ProtocolError> {
        let answer = compiled.answer.ok_or(ProtocolError)?;
        let waiting = {
            let mut calls = self.calls();
            if compiled.number == 0 || compiled.number > calls.last {
                return Err(ProtocolError);
            }
            calls.waiting.remove(&compiled.number)
        };
        let Some(waiting) = waiting else {
            return Ok(());
        };
        if let Answer::Package(package) = &answer
            && package.sha256 != codec::package_sha256(&package.script, &package.source_map)
        {
            // Dropping `waiting` fails its call as a stopped compiler.
            return Err(ProtocolError);
        }
        // Its caller waits on a channel of one, and this is the only send.
        drop(waiting.try_send(answer));
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

    /// Sends `call` and waits for its answer, which is no failure.
    fn call(&self, time_left: Duration, call: Call) -> Result<Answer, CompileError> {
        if time_left.is_zero() {
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
            calls.waiting.insert(number, answered);
            number
        };
        let compile = Compile {
            number,
            time_left_ms: u32::try_from(time_left.as_millis()).unwrap_or(u32::MAX),
            call: Some(call),
        };
        let sent = self.outbound.send(PullResponse {
            message: Some(pull_response::Message::Compile(compile)),
        });
        let answer = match sent {
            Ok(()) => answer.recv_timeout(time_left),
            Err(_closed) => Err(sync_mpsc::RecvTimeoutError::Disconnected),
        };
        // A call that ends here takes no late answer.
        self.calls().waiting.remove(&number);
        match answer {
            Ok(Answer::Failure(failure)) => Err(codec::compile_error(failure)?),
            Ok(answer) => Ok(answer),
            Err(sync_mpsc::RecvTimeoutError::Timeout) => Err(CompileError::TimedOut),
            Err(sync_mpsc::RecvTimeoutError::Disconnected) => {
                Err(CompileError::Unavailable(Unavailable::Stopped))
            }
        }
    }

    fn named(
        &self,
        time_left: Duration,
        call: Call,
    ) -> Result<SavedFunctionCallPaths, CompileError> {
        match self.call(time_left, call)? {
            Answer::CallPaths(paths) => Ok(codec::saved_function_call_paths(paths)?),
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
        self.named(time_left, Call::ProgramReferences(source(source_text)))
    }

    fn unknown_tool_calls(
        &self,
        time_left: Duration,
        tools: &ToolReferences,
        callable: &[String],
    ) -> Result<Vec<UnknownToolCall>, CompileError> {
        let call = Call::UnknownToolCalls(wire::ToolCalls {
            tools: Some(codec::tools_wire(tools)),
            callable: callable.to_vec(),
        });
        match self.call(time_left, call)? {
            Answer::UnknownToolCalls(calls) => Ok(codec::unknown_tool_calls(calls)),
            _ => Err(ProtocolError.into()),
        }
    }

    fn dependency_references(
        &self,
        time_left: Duration,
        source_text: &str,
    ) -> Result<Vec<SavedFunctionReference>, CompileError> {
        match self.call(time_left, Call::DependencyReferences(source(source_text)))? {
            Answer::References(found) => Ok(found
                .references
                .into_iter()
                .map(codec::saved_function_reference)
                .collect()),
            _ => Err(ProtocolError.into()),
        }
    }

    fn compile(
        &self,
        time_left: Duration,
        source_text: &str,
        saved_functions: &[LinkedSavedFunctionSource],
    ) -> Result<CompiledProgram, CompileError> {
        let call = Call::Compile(wire::ProgramSource {
            source: source_text.to_owned(),
            saved_functions: saved_functions.iter().map(codec::linked_wire).collect(),
        });
        match self.call(time_left, call)? {
            Answer::Package(package) => Ok(codec::compiled_program(package)?),
            _ => Err(ProtocolError.into()),
        }
    }

    fn call_paths(
        &self,
        time_left: Duration,
        source_text: &str,
    ) -> Result<SavedFunctionCallPaths, CompileError> {
        self.named(time_left, Call::CallPaths(source(source_text)))
    }

    fn prepare(
        &self,
        time_left: Duration,
        source_text: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<SavedFunctionCompilation, CompileError> {
        let call = Call::Prepare(wire::SavedFunctionSource {
            source: source_text.to_owned(),
            declarations: declarations.map(codec::declarations_wire),
        });
        match self.call(time_left, call)? {
            Answer::Prepared(prepared) => Ok(codec::saved_function_compilation(prepared)?),
            _ => Err(ProtocolError.into()),
        }
    }
}
