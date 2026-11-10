//! The compiler client's calls on the pull stream: the Run Executor serves a
//! `Compile` with its warm compiler, and Coral reads the `Compiled` back into
//! the value the in-process client returns.

use std::time::{Duration, Instant};

use coral_v8::{
    CallableDeclaration, CompiledProgram, CompilerDiagnostic, CoralDeclarations,
    DiagnosticCategory, LinkedSavedFunctionSource, SavedFunctionCallPaths,
    SavedFunctionCompilation, SavedFunctionDeclarationField, SavedFunctionReference, ToolChain,
    ToolReferences, UnknownToolCall,
};
use sha2::{Digest as _, Sha256};

use crate::wire::v1::compile::Call;
use crate::wire::v1::compile_failure::Failure;
use crate::wire::v1::compiled::Answer;
use crate::wire::v1::{self as wire, Compile, Compiled};
use crate::{CompileError, CompilerClient, Diagnostics, Unavailable, time_left};

/// The kinds a rejection can carry. A kind off this list reads as
/// `compilation`, so a peer never widens the set of `error.type` values.
const REJECTION_KINDS: [&str; 6] = [
    "compilation",
    "invalid_program_body",
    "invalid_saved_function",
    "source_too_deeply_nested",
    "forbidden_capability",
    "invalid_function_save",
];
/// The kinds a compiler failure can carry. Any other reads as
/// `compiler_protocol`.
const FAILURE_KINDS: [&str; 4] = [
    "initialization",
    "compiler_protocol",
    "invalid_source_map",
    "engine",
];

/// A compile message that breaks the protocol: a missing field, an answer
/// of the wrong kind, or a package whose digest does not match. The side
/// that reads one ends the stream that carried it: Coral's handler when
/// [`crate::PullStreamCompiler::answer`] returns it, and the Run Executor
/// when it cannot serve a `Compile`.
#[derive(Debug, thiserror::Error)]
#[error("a compile message broke the protocol")]
pub struct ProtocolError;

/// A call that cannot read the answer it took fails as a compiler failure.
impl From<ProtocolError> for CompileError {
    fn from(ProtocolError: ProtocolError) -> Self {
        Self::Unavailable(Unavailable::Failed("compiler_protocol"))
    }
}

/// The digest a package carries: SHA-256 of the script's length as eight
/// big-endian bytes, the script, and the source map.
#[must_use]
pub(crate) fn package_sha256(script: &str, source_map: &str) -> [u8; 32] {
    let length = u64::try_from(script.len()).unwrap_or(u64::MAX);
    Sha256::new()
        .chain_update(length.to_be_bytes())
        .chain_update(script)
        .chain_update(source_map)
        .finalize()
        .into()
}

/// Serves one `Compile` that arrived at `received` with `compiler`. Its time
/// runs from its arrival, so its wait in the queue counts.
pub(crate) fn serve(
    compiler: &dyn CompilerClient,
    received: Instant,
    compile: Compile,
) -> Result<Compiled, ProtocolError> {
    let time_left = time_left(received + Duration::from_millis(compile.time_left_ms.into()));
    let answer = match compile.call.ok_or(ProtocolError)? {
        Call::ProgramReferences(wire::Source { source }) => {
            answer(compiler.program_references(time_left, &source), |paths| {
                Answer::CallPaths(call_paths(&paths))
            })
        }
        Call::UnknownToolCalls(wire::ToolCalls { tools, callable }) => {
            let tools = tool_references(tools.ok_or(ProtocolError)?);
            answer(
                compiler.unknown_tool_calls(time_left, &tools, &callable),
                |calls| {
                    let calls = calls.iter().map(unknown_tool_call).collect();
                    Answer::UnknownToolCalls(wire::UnknownToolCalls { calls })
                },
            )
        }
        Call::DependencyReferences(wire::Source { source }) => answer(
            compiler.dependency_references(time_left, &source),
            |found| {
                Answer::References(wire::References {
                    references: found.iter().map(reference).collect(),
                })
            },
        ),
        Call::Compile(wire::ProgramSource {
            source,
            saved_functions,
        }) => {
            let linked: Vec<_> = saved_functions
                .into_iter()
                .map(|linked| LinkedSavedFunctionSource::new(linked.path_segments, linked.source))
                .collect();
            answer(compiler.compile(time_left, &source, &linked), |program| {
                Answer::Package(package(&program))
            })
        }
        Call::CallPaths(wire::Source { source }) => {
            answer(compiler.call_paths(time_left, &source), |paths| {
                Answer::CallPaths(call_paths(&paths))
            })
        }
        Call::Prepare(wire::SavedFunctionSource {
            source,
            declarations,
        }) => {
            let declarations = declarations.map(coral_declarations).transpose()?;
            answer(
                compiler.prepare(time_left, &source, declarations.as_ref()),
                |prepared| Answer::Prepared(prepared_answer(&prepared)),
            )
        }
    };
    Ok(Compiled {
        number: compile.number,
        answer: Some(answer),
    })
}

fn answer<T>(result: Result<T, CompileError>, encode: impl FnOnce(T) -> Answer) -> Answer {
    match result {
        Ok(value) => encode(value),
        Err(error) => Answer::Failure(failure(error)),
    }
}

pub(crate) fn failure(error: CompileError) -> wire::CompileFailure {
    let failure = match error {
        CompileError::TimedOut => Failure::TimedOut(true),
        CompileError::Diagnostics(Diagnostics::Message { kind, message }) => {
            Failure::Rejected(wire::Rejection {
                kind: kind.to_owned(),
                message: coral_v8::bounded_diagnostic_message(&message),
            })
        }
        CompileError::Diagnostics(Diagnostics::Reference {
            message,
            line,
            column,
        }) => Failure::Reference(wire::ReferenceRejection {
            message,
            line,
            column,
        }),
        CompileError::MemoryLimit => Failure::MemoryLimit(true),
        CompileError::Unavailable(Unavailable::Busy) => Failure::Busy(true),
        CompileError::Unavailable(Unavailable::Stopped) => Failure::Stopped(true),
        CompileError::Unavailable(Unavailable::Failed(kind)) => Failure::Failed(kind.to_owned()),
    };
    wire::CompileFailure {
        failure: Some(failure),
    }
}

/// The error a `failure` answer carries.
pub(crate) fn compile_error(failure: wire::CompileFailure) -> Result<CompileError, ProtocolError> {
    let known = |kinds: &[&'static str], kind: &str, other| {
        kinds
            .iter()
            .copied()
            .find(|known| *known == kind)
            .unwrap_or(other)
    };
    Ok(match failure.failure.ok_or(ProtocolError)? {
        Failure::TimedOut(_) => CompileError::TimedOut,
        Failure::Rejected(wire::Rejection { kind, message }) => {
            CompileError::Diagnostics(Diagnostics::Message {
                kind: known(&REJECTION_KINDS, &kind, "compilation"),
                message,
            })
        }
        Failure::Reference(wire::ReferenceRejection {
            message,
            line,
            column,
        }) => CompileError::Diagnostics(Diagnostics::Reference {
            message,
            line,
            column,
        }),
        Failure::MemoryLimit(_) => CompileError::MemoryLimit,
        Failure::Busy(_) => CompileError::Unavailable(Unavailable::Busy),
        Failure::Stopped(_) => CompileError::Unavailable(Unavailable::Stopped),
        Failure::Failed(kind) => CompileError::Unavailable(Unavailable::Failed(known(
            &FAILURE_KINDS,
            &kind,
            "compiler_protocol",
        ))),
    })
}

fn call_paths(paths: &SavedFunctionCallPaths) -> wire::CallPaths {
    wire::CallPaths {
        functions: paths.functions().iter().map(reference).collect(),
        tools: Some(tools_wire(paths.tools())),
    }
}

pub(crate) fn saved_function_call_paths(
    paths: wire::CallPaths,
) -> Result<SavedFunctionCallPaths, ProtocolError> {
    let functions = paths.functions.into_iter().map(saved_function_reference);
    let tools = tool_references(paths.tools.ok_or(ProtocolError)?);
    Ok(SavedFunctionCallPaths::new(functions.collect(), tools))
}

pub(crate) fn tools_wire(tools: &ToolReferences) -> wire::ToolReferences {
    wire::ToolReferences {
        call_paths: tools.call_paths().to_vec(),
        chains: tools.chains().iter().map(chain_wire).collect(),
    }
}

fn tool_references(tools: wire::ToolReferences) -> ToolReferences {
    let chains = tools.chains.into_iter().map(tool_chain).collect();
    ToolReferences::new(tools.call_paths, chains)
}

fn chain_wire(chain: &ToolChain) -> wire::ToolChain {
    wire::ToolChain {
        segments: chain.segments().to_vec(),
        start: chain.start(),
        called: chain.called(),
        line: chain.line(),
        column: chain.column(),
    }
}

fn tool_chain(chain: wire::ToolChain) -> ToolChain {
    ToolChain::new(
        chain.segments,
        chain.start,
        chain.called,
        chain.line,
        chain.column,
    )
}

fn reference(reference: &SavedFunctionReference) -> wire::Reference {
    wire::Reference {
        path: reference.path().to_owned(),
        line: reference.line(),
        column: reference.column(),
    }
}

pub(crate) fn saved_function_reference(reference: wire::Reference) -> SavedFunctionReference {
    SavedFunctionReference::new(reference.path, reference.line, reference.column)
}

fn unknown_tool_call(call: &UnknownToolCall) -> wire::UnknownToolCall {
    wire::UnknownToolCall {
        call_path: call.call_path().to_owned(),
        line: call.line(),
        column: call.column(),
    }
}

pub(crate) fn unknown_tool_calls(calls: wire::UnknownToolCalls) -> Vec<UnknownToolCall> {
    let calls = calls.calls.into_iter();
    calls
        .map(|call| UnknownToolCall::new(call.call_path, call.line, call.column))
        .collect()
}

pub(crate) fn linked_wire(linked: &LinkedSavedFunctionSource) -> wire::LinkedSavedFunction {
    wire::LinkedSavedFunction {
        path_segments: linked.path_segments().to_vec(),
        source: linked.source().to_owned(),
    }
}

pub(crate) fn declarations_wire(declarations: &CoralDeclarations) -> wire::Declarations {
    let callables = |declarations: &[CallableDeclaration]| {
        let declarations = declarations.iter();
        declarations
            .map(|declaration| wire::CallableDeclaration {
                path_segments: declaration.path_segments().to_vec(),
                declaration: declaration.declaration().to_owned(),
            })
            .collect()
    };
    wire::Declarations {
        declaration_max_bytes: u64::try_from(declarations.declaration_max_bytes())
            .unwrap_or(u64::MAX),
        tools: declarations
            .tools()
            .map(|(chains, tools)| wire::TypedTools {
                chains: chains.iter().map(chain_wire).collect(),
                declarations: callables(tools),
            }),
        functions: declarations
            .functions()
            .map(|functions| wire::TypedFunctions {
                declarations: callables(functions),
            }),
    }
}

fn coral_declarations(
    declarations: wire::Declarations,
) -> Result<CoralDeclarations, ProtocolError> {
    let callables = |declarations: Vec<wire::CallableDeclaration>| {
        let declarations = declarations.into_iter();
        declarations
            .map(|callable| CallableDeclaration::new(callable.path_segments, callable.declaration))
            .collect()
    };
    let max_bytes =
        usize::try_from(declarations.declaration_max_bytes).map_err(|_overflow| ProtocolError)?;
    let mut typed = CoralDeclarations::new(max_bytes);
    if let Some(tools) = declarations.tools {
        // Typing reads only the chains of the references.
        let chains = tools.chains.into_iter().map(tool_chain).collect();
        let references = ToolReferences::new(Vec::new(), chains);
        typed = typed.with_tools(&references, callables(tools.declarations));
    }
    if let Some(functions) = declarations.functions {
        typed = typed.with_functions(callables(functions.declarations));
    }
    Ok(typed)
}

fn package(program: &CompiledProgram) -> wire::Package {
    wire::Package {
        script: program.javascript().to_owned(),
        source_map: program.source_map().to_owned(),
        sha256: package_sha256(program.javascript(), program.source_map()).to_vec(),
        diagnostics: program.diagnostics().iter().map(diagnostic).collect(),
    }
}

/// The program a package carries, once its digest matches.
pub(crate) fn compiled_program(package: wire::Package) -> Result<CompiledProgram, ProtocolError> {
    if package.sha256 != package_sha256(&package.script, &package.source_map) {
        return Err(ProtocolError);
    }
    let diagnostics = package.diagnostics.into_iter().map(compiler_diagnostic);
    Ok(CompiledProgram::new(
        package.script,
        package.source_map,
        diagnostics.collect::<Result<_, _>>()?,
    ))
}

fn prepared_answer(prepared: &SavedFunctionCompilation) -> wire::Prepared {
    use wire::prepared::Projection;
    let projection = match prepared.declaration_fields() {
        Ok(fields) => Projection::Fields(wire::DeclarationFields {
            fields: fields
                .iter()
                .map(|field| wire::DeclarationField {
                    path: field.path().to_owned(),
                    text: field.text().to_owned(),
                })
                .collect(),
        }),
        Err(fault) => Projection::Fault(fault.to_owned()),
    };
    wire::Prepared {
        declaration: prepared.declaration().to_owned(),
        projection: Some(projection),
        diagnostics: prepared.diagnostics().iter().map(diagnostic).collect(),
        references: prepared.references().iter().map(reference).collect(),
    }
}

pub(crate) fn saved_function_compilation(
    prepared: wire::Prepared,
) -> Result<SavedFunctionCompilation, ProtocolError> {
    use wire::prepared::Projection;
    let projection = match prepared.projection.ok_or(ProtocolError)? {
        Projection::Fields(fields) => Ok(fields
            .fields
            .into_iter()
            .map(|field| SavedFunctionDeclarationField::new(field.path, field.text))
            .collect()),
        Projection::Fault(fault) => Err(fault),
    };
    let diagnostics = prepared.diagnostics.into_iter().map(compiler_diagnostic);
    let references = prepared
        .references
        .into_iter()
        .map(saved_function_reference);
    Ok(SavedFunctionCompilation::new(
        prepared.declaration,
        projection,
        diagnostics.collect::<Result<_, _>>()?,
        references.collect(),
    ))
}

fn diagnostic(diagnostic: &CompilerDiagnostic) -> wire::Diagnostic {
    use wire::DiagnosticCategory as Wire;
    let category = match diagnostic.category() {
        DiagnosticCategory::Message => Wire::Message,
        DiagnosticCategory::Warning => Wire::Warning,
        DiagnosticCategory::Error => Wire::Error,
        DiagnosticCategory::Suggestion => Wire::Suggestion,
    };
    wire::Diagnostic {
        category: category.into(),
        code: diagnostic.code(),
        message: diagnostic.message().to_owned(),
        line: diagnostic.line(),
        column: diagnostic.column(),
    }
}

fn compiler_diagnostic(diagnostic: wire::Diagnostic) -> Result<CompilerDiagnostic, ProtocolError> {
    use wire::DiagnosticCategory as Wire;
    let category = match diagnostic.category() {
        Wire::Message => DiagnosticCategory::Message,
        Wire::Warning => DiagnosticCategory::Warning,
        Wire::Error => DiagnosticCategory::Error,
        Wire::Suggestion => DiagnosticCategory::Suggestion,
        Wire::Unspecified => return Err(ProtocolError),
    };
    Ok(CompilerDiagnostic::new(
        category,
        diagnostic.code,
        diagnostic.message,
        diagnostic.line,
        diagnostic.column,
    ))
}

#[cfg(test)]
mod tests {
    use coral_test_support::TEST_COMPILATION_TIMEOUT;
    use coral_v8::IsolateLimits;

    use super::*;
    use crate::InProcessCompiler;

    #[test]
    fn a_call_past_its_time_answers_timed_out() {
        let compiler = InProcessCompiler::new(IsolateLimits::defaults()).unwrap();
        let compile = |time_left_ms| Compile {
            number: 7,
            time_left_ms,
            call: Some(Call::Compile(wire::ProgramSource {
                source: "return 42;".to_owned(),
                saved_functions: Vec::new(),
            })),
        };
        let now = Instant::now();
        // No time left, and a call that waited in the queue past its time.
        for (received, time_left_ms) in [
            (now, 0),
            (now.checked_sub(Duration::from_secs(3)).unwrap(), 2_000),
        ] {
            let served = serve(&compiler, received, compile(time_left_ms)).unwrap();
            let timed_out = Answer::Failure(wire::CompileFailure {
                failure: Some(Failure::TimedOut(true)),
            });
            assert_eq!((served.number, served.answer), (7, Some(timed_out)));
        }
        let time_left_ms = u32::try_from(TEST_COMPILATION_TIMEOUT.as_millis()).unwrap();
        let served = serve(&compiler, Instant::now(), compile(time_left_ms)).unwrap();
        assert!(matches!(served.answer, Some(Answer::Package(_))));
    }
}
