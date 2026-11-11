//! The compiler client's calls on the pull stream: the Run Executor serves a
//! `Compile` with its warm compiler, and Coral reads the `Compiled` back into
//! the value the in-process client returns.

use std::time::{Duration, Instant};

use coral_v8::{
    AnalyzedSource, AuthoredToolCompilation, AuthoredToolDeclarationField, BuiltinTools,
    CallableDeclaration, CompiledProgram, CompilerDiagnostic, CoralDeclarations,
    DiagnosticCategory, LinkedToolReference, LinkedToolRoot, LinkedToolSource, SourceReferences,
    ToolChain, ToolReferences, UnknownToolCall,
};
use sha2::{Digest as _, Sha256};

use crate::wire::v1::compile::Call;
use crate::wire::v1::compile_failure::Failure;
use crate::wire::v1::compiled::Answer;
use crate::wire::v1::{self as wire, Compile, Compiled};
use crate::{AnalysisError, CompileError, CompilerClient, Diagnostics, Unavailable, time_left};

/// A compile message that breaks the protocol: a missing field, an answer
/// of the wrong kind or count, or a package whose digest does not match. The
/// side that reads one ends the stream that carried it: Coral's
/// [`crate::LinkServer`] when a `Compiled` breaks it, and the Run Executor
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
        Call::Analyze(wire::Analyze { sources }) => {
            let sources = sources
                .into_iter()
                .map(analyzed_source)
                .collect::<Result<Vec<_>, _>>()?;
            match compiler.analyze(time_left, &sources) {
                Ok(analyzed) => Answer::Analyzed(wire::Analyzed {
                    sources: analyzed.iter().map(source_references_wire).collect(),
                }),
                Err(AnalysisError { index, error }) => {
                    Answer::AnalysisFailure(wire::AnalysisFailure {
                        index: u64::try_from(index).unwrap_or(u64::MAX),
                        failure: Some(failure(error)),
                    })
                }
            }
        }
        Call::Compile(wire::ProgramSource {
            source,
            linked_tools,
            callable,
            builtin_tools,
        }) => {
            let linked = linked_tools
                .into_iter()
                .map(linked_tool)
                .collect::<Result<Vec<_>, _>>()?;
            let callable = callable.map(|callable| callable.call_paths);
            let builtin_tools = BuiltinTools::new(builtin_tools);
            answer(
                compiler.compile(
                    time_left,
                    &source,
                    &linked,
                    &builtin_tools,
                    callable.as_deref(),
                ),
                |program| Answer::Package(package(program)),
            )
        }
        Call::Prepare(wire::AuthoredToolSource {
            source,
            declarations,
            builtin_tools,
        }) => {
            let declarations = declarations.map(coral_declarations).transpose()?;
            let builtin_tools = BuiltinTools::new(builtin_tools);
            answer(
                compiler.prepare(time_left, &source, &builtin_tools, declarations.as_ref()),
                |prepared| Answer::Prepared(prepared_answer(&prepared)),
            )
        }
    };
    Ok(Compiled {
        number: compile.number,
        answer: Some(answer),
    })
}

/// The call that analyzes `sources`.
pub(crate) fn analyze_call(sources: &[AnalyzedSource]) -> Call {
    Call::Analyze(wire::Analyze {
        sources: sources.iter().map(analyzed_source_wire).collect(),
    })
}

/// The call that compiles `source` with `linked_tools` and `builtin_tools`,
/// checked against `callable` when it is set.
pub(crate) fn compile_call(
    source: &str,
    linked_tools: &[LinkedToolSource],
    builtin_tools: &BuiltinTools,
    callable: Option<&[String]>,
) -> Call {
    Call::Compile(wire::ProgramSource {
        source: source.to_owned(),
        linked_tools: linked_tools.iter().map(linked_wire).collect(),
        callable: callable.map(|call_paths| wire::Callable {
            call_paths: call_paths.to_vec(),
        }),
        builtin_tools: builtin_tools.keys().to_vec(),
    })
}

/// The call that prepares `source` with `builtin_tools`, typed by
/// `declarations` when it is set.
pub(crate) fn prepare_call(
    source: &str,
    builtin_tools: &BuiltinTools,
    declarations: Option<&CoralDeclarations>,
) -> Call {
    Call::Prepare(wire::AuthoredToolSource {
        source: source.to_owned(),
        declarations: declarations.map(declarations_wire),
        builtin_tools: builtin_tools.keys().to_vec(),
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
        CompileError::Diagnostics(Diagnostics::UnknownTools { linked, calls }) => {
            Failure::UnknownTools(wire::UnknownToolCalls {
                calls: calls.iter().map(unknown_tool_call).collect(),
                linked: linked.map(|index| u64::try_from(index).unwrap_or(u64::MAX)),
            })
        }
        CompileError::MemoryLimit => Failure::MemoryLimit(true),
        CompileError::Unavailable(Unavailable::Busy) => Failure::Busy(true),
        // A Run Executor never waits for a compiler; its own stopped.
        CompileError::Unavailable(Unavailable::Stopped | Unavailable::NoCompiler) => {
            Failure::Stopped(true)
        }
        CompileError::Unavailable(Unavailable::Failed(kind)) => Failure::Failed(kind.to_owned()),
    };
    wire::CompileFailure {
        failure: Some(failure),
    }
}

/// The error a `failure` answer carries. A kind off coral-v8's lists reads
/// as `compilation` or `compiler_protocol`, so a peer never widens the set
/// of `error.type` values.
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
                kind: known(&coral_v8::Error::REJECTION_CODES, &kind, "compilation"),
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
        Failure::UnknownTools(calls) => {
            let linked = calls
                .linked
                .map(|index| usize::try_from(index).map_err(|_overflow| ProtocolError))
                .transpose()?;
            CompileError::Diagnostics(Diagnostics::UnknownTools {
                linked,
                calls: unknown_tool_calls(calls),
            })
        }
        Failure::MemoryLimit(_) => CompileError::MemoryLimit,
        Failure::Busy(_) => CompileError::Unavailable(Unavailable::Busy),
        Failure::Stopped(_) => CompileError::Unavailable(Unavailable::Stopped),
        Failure::Failed(kind) => CompileError::Unavailable(Unavailable::Failed(known(
            &coral_v8::Error::COMPILER_FAILURE_CODES,
            &kind,
            "compiler_protocol",
        ))),
    })
}

fn analyzed_source_wire(source: &AnalyzedSource) -> wire::AnalyzedSource {
    use wire::analyzed_source::Source;
    let source = match source {
        AnalyzedSource::Program(source) => Source::Program(source.clone()),
        AnalyzedSource::AuthoredTool(source) => Source::AuthoredTool(source.clone()),
    };
    wire::AnalyzedSource {
        source: Some(source),
    }
}

fn analyzed_source(source: wire::AnalyzedSource) -> Result<AnalyzedSource, ProtocolError> {
    use wire::analyzed_source::Source;
    Ok(match source.source.ok_or(ProtocolError)? {
        Source::Program(source) => AnalyzedSource::Program(source),
        Source::AuthoredTool(source) => AnalyzedSource::AuthoredTool(source),
    })
}

fn source_references_wire(references: &SourceReferences) -> wire::SourceReferences {
    wire::SourceReferences {
        private_tools: references.private_tools().iter().map(reference).collect(),
        tool_call_paths: references.tool_call_paths().to_vec(),
        shared_tools: references.shared_tools().iter().map(reference).collect(),
    }
}

fn source_references(references: wire::SourceReferences) -> SourceReferences {
    let private_tools = references.private_tools.into_iter();
    let shared_tools = references.shared_tools.into_iter();
    SourceReferences::new(
        private_tools.map(linked_tool_reference).collect(),
        shared_tools.map(linked_tool_reference).collect(),
        references.tool_call_paths,
    )
}

fn reference(reference: &LinkedToolReference) -> wire::Reference {
    wire::Reference {
        path: reference.path().to_owned(),
        line: reference.line(),
        column: reference.column(),
    }
}

fn linked_tool_reference(reference: wire::Reference) -> LinkedToolReference {
    LinkedToolReference::new(reference.path, reference.line, reference.column)
}

fn unknown_tool_call(call: &UnknownToolCall) -> wire::UnknownToolCall {
    wire::UnknownToolCall {
        call_path: call.call_path().to_owned(),
        line: call.line(),
        column: call.column(),
    }
}

fn unknown_tool_calls(calls: wire::UnknownToolCalls) -> Vec<UnknownToolCall> {
    let calls = calls.calls.into_iter();
    calls
        .map(|call| UnknownToolCall::new(call.call_path, call.line, call.column))
        .collect()
}

fn linked_wire(linked: &LinkedToolSource) -> wire::LinkedTool {
    let root = match linked.root() {
        LinkedToolRoot::Private => wire::LinkedToolRoot::Private,
        LinkedToolRoot::Shared => wire::LinkedToolRoot::Shared,
    };
    wire::LinkedTool {
        path_segments: linked.path_segments().to_vec(),
        source: linked.source().to_owned(),
        root: root.into(),
    }
}

fn linked_tool(linked: wire::LinkedTool) -> Result<LinkedToolSource, ProtocolError> {
    Ok(match linked.root() {
        wire::LinkedToolRoot::Private => {
            LinkedToolSource::private(linked.path_segments, linked.source)
        }
        wire::LinkedToolRoot::Shared => {
            LinkedToolSource::shared(linked.path_segments, linked.source)
        }
        wire::LinkedToolRoot::Unspecified => return Err(ProtocolError),
    })
}

fn declarations_wire(declarations: &CoralDeclarations) -> wire::Declarations {
    let callables = |declarations: &[CallableDeclaration]| wire::CallableDeclarations {
        declarations: declarations
            .iter()
            .map(|declaration| wire::CallableDeclaration {
                path_segments: declaration.path_segments().to_vec(),
                declaration: declaration.declaration().to_owned(),
            })
            .collect(),
    };
    wire::Declarations {
        declaration_max_bytes: u64::try_from(declarations.declaration_max_bytes())
            .unwrap_or(u64::MAX),
        tools: declarations.tools().map(callables),
        private_tools: declarations.private_tools().map(callables),
        shared_tools: declarations.shared_tools().map(callables),
    }
}

fn coral_declarations(
    declarations: wire::Declarations,
) -> Result<CoralDeclarations, ProtocolError> {
    let callables = |callables: wire::CallableDeclarations| {
        let declarations = callables.declarations.into_iter();
        declarations
            .map(|callable| CallableDeclaration::new(callable.path_segments, callable.declaration))
            .collect()
    };
    let max_bytes =
        usize::try_from(declarations.declaration_max_bytes).map_err(|_overflow| ProtocolError)?;
    let mut typed = CoralDeclarations::new(max_bytes);
    if let Some(tools) = declarations.tools {
        typed = typed.with_tools(callables(tools));
    }
    if let Some(private_tools) = declarations.private_tools {
        typed = typed.with_private_tools(callables(private_tools));
    }
    if let Some(shared_tools) = declarations.shared_tools {
        typed = typed.with_shared_tools(callables(shared_tools));
    }
    Ok(typed)
}

fn package(program: CompiledProgram) -> wire::Package {
    let (script, source_map, diagnostics) = program.into_parts();
    wire::Package {
        sha256: package_sha256(&script, &source_map).to_vec(),
        script,
        source_map,
        diagnostics: diagnostics.iter().map(diagnostic).collect(),
    }
}

/// The program a package carries, once its digest matches.
fn compiled_program(package: wire::Package) -> Result<CompiledProgram, ProtocolError> {
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

fn prepared_answer(prepared: &AuthoredToolCompilation) -> wire::Prepared {
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
        shared_references: prepared.shared_references().iter().map(reference).collect(),
        tool_chains: prepared
            .tools()
            .chains()
            .iter()
            .map(|chain| wire::ToolChain {
                segments: chain.segments().to_vec(),
                called: chain.called(),
                computed: chain.computed(),
            })
            .collect(),
        not_data: prepared.not_data().map(ToOwned::to_owned),
    }
}

fn authored_tool_compilation(
    prepared: wire::Prepared,
) -> Result<AuthoredToolCompilation, ProtocolError> {
    use wire::prepared::Projection;
    let projection = match prepared.projection.ok_or(ProtocolError)? {
        Projection::Fields(fields) => Ok(fields
            .fields
            .into_iter()
            .map(|field| AuthoredToolDeclarationField::new(field.path, field.text))
            .collect()),
        Projection::Fault(fault) => Err(fault),
    };
    let diagnostics = prepared.diagnostics.into_iter().map(compiler_diagnostic);
    let references = prepared.references.into_iter();
    let shared_references = prepared.shared_references.into_iter();
    let chains = prepared.tool_chains.into_iter();
    let chains = chains.map(|chain| ToolChain::new(chain.segments, chain.called, chain.computed));
    Ok(AuthoredToolCompilation::new(
        prepared.declaration,
        projection,
        diagnostics.collect::<Result<_, _>>()?,
        references.map(linked_tool_reference).collect(),
        shared_references.map(linked_tool_reference).collect(),
        ToolReferences::new(chains.collect()),
        prepared.not_data,
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

/// The answers a call accepts: its result, or the failure of its kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Takes {
    /// One `SourceReferences` per source it sent.
    Analyzed(usize),
    Package,
    Prepared,
}

impl Takes {
    pub(crate) fn of(call: &Call) -> Self {
        match call {
            Call::Analyze(analyze) => Self::Analyzed(analyze.sources.len()),
            Call::Compile(_) => Self::Package,
            Call::Prepare(_) => Self::Prepared,
        }
    }
}

/// An answer read back into the value the in-process client returns.
#[derive(Debug)]
pub(crate) enum Read {
    Analyzed(Vec<SourceReferences>),
    AnalysisFailure(AnalysisError),
    Package(CompiledProgram),
    Prepared(AuthoredToolCompilation),
    Failure(CompileError),
}

impl Read {
    pub(crate) fn answer(answer: Answer) -> Result<Self, ProtocolError> {
        Ok(match answer {
            Answer::Analyzed(analyzed) => Self::Analyzed(
                analyzed
                    .sources
                    .into_iter()
                    .map(source_references)
                    .collect(),
            ),
            Answer::AnalysisFailure(wire::AnalysisFailure { index, failure }) => {
                Self::AnalysisFailure(AnalysisError {
                    index: usize::try_from(index).map_err(|_overflow| ProtocolError)?,
                    error: compile_error(failure.ok_or(ProtocolError)?)?,
                })
            }
            Answer::Package(package) => Self::Package(compiled_program(package)?),
            Answer::Prepared(prepared) => Self::Prepared(authored_tool_compilation(prepared)?),
            Answer::Failure(failure) => Self::Failure(compile_error(failure)?),
        })
    }

    /// Whether this answers a call that `takes` it: analysis answers with one
    /// result per source, or names one of them as the one it stopped at.
    pub(crate) fn answers(&self, takes: Takes) -> bool {
        match (self, takes) {
            (Self::Analyzed(found), Takes::Analyzed(sources)) => found.len() == sources,
            (Self::AnalysisFailure(stopped), Takes::Analyzed(sources)) => stopped.index < sources,
            (Self::Package(_), Takes::Package)
            | (Self::Prepared(_), Takes::Prepared)
            | (Self::Failure(_), Takes::Package | Takes::Prepared) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use coral_test_support::TEST_COMPILATION_TIMEOUT;
    use coral_v8::IsolateLimits;

    use super::*;
    use crate::InProcessCompiler;

    // Catches: an error, or a kind coral-v8 reports, that the pull stream
    // reads back as another.
    #[test]
    fn every_compile_error_crosses_the_stream() {
        let errors = [
            CompileError::TimedOut,
            CompileError::MemoryLimit,
            CompileError::Unavailable(Unavailable::Busy),
            CompileError::Unavailable(Unavailable::Stopped),
            CompileError::Diagnostics(Diagnostics::Reference {
                message: "computed access".to_owned(),
                line: 2,
                column: 7,
            }),
            CompileError::Diagnostics(Diagnostics::UnknownTools {
                linked: None,
                calls: vec![UnknownToolCall::new(
                    "coral.providers.github.repos.get".to_owned(),
                    3,
                    9,
                )],
            }),
            CompileError::Diagnostics(Diagnostics::UnknownTools {
                linked: Some(1),
                calls: vec![UnknownToolCall::new(
                    "coral.providers.notion.retrievePage".to_owned(),
                    2,
                    5,
                )],
            }),
        ];
        let rejections = coral_v8::Error::REJECTION_CODES.map(|kind| {
            CompileError::Diagnostics(Diagnostics::Message {
                kind,
                message: "rejected".to_owned(),
            })
        });
        let failures = coral_v8::Error::COMPILER_FAILURE_CODES
            .map(|kind| CompileError::Unavailable(Unavailable::Failed(kind)));
        for error in errors.into_iter().chain(rejections).chain(failures) {
            let carried = compile_error(failure(error.clone()));
            assert_eq!(carried.ok(), Some(error.clone()), "{error:?}");
        }
    }

    #[test]
    fn a_call_past_its_time_answers_timed_out() {
        let compiler = InProcessCompiler::new(IsolateLimits::defaults()).unwrap();
        let compile = |time_left_ms| Compile {
            number: 7,
            time_left_ms,
            call: Some(Call::Compile(wire::ProgramSource {
                source: "return 42;".to_owned(),
                linked_tools: Vec::new(),
                callable: None,
                builtin_tools: Vec::new(),
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
