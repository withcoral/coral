//! Authoring that Private and Shared Tools share: both are Tools that Coral
//! performs from authored source (ADR 0033). One compiler seam prepares a
//! source of either kind without executing it: its declaration, its
//! advisory diagnostics, the Private and Shared Tools it names, and whether
//! its arguments and result are data.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use coral_catalog::AuthoredToolKind;
use coral_run_executor::{CompilerClient, time_left};
use coral_telemetry::safe;
use coral_v8::{CompileFailure, CoralDeclarations, NamedCallPaths};
use tokio::time::timeout_at;

use crate::Error;
use crate::shared_tool::SharedToolPath;
use crate::types::{
    AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT, AuthoredToolDeclaration,
    AuthoredToolDeclarationFieldProjection, AuthoredToolDiagnostic, PrivateToolPath,
};

pub(crate) fn now_unix_nanos() -> Result<i64, Error> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Clock(error.to_string()))?;
    i64::try_from(elapsed.as_nanos())
        .map_err(|_error| Error::Clock("current time exceeds durable nanosecond range".to_string()))
}

/// One Tool a source names by a literal path, and where it names it.
pub(crate) struct PathReference<Path> {
    pub(crate) path: Path,
    pub(crate) line: u64,
    pub(crate) column: u64,
}

/// What the compiler made of one authored source.
pub(crate) struct PreparedAuthoredTool {
    pub(crate) declaration: AuthoredToolDeclaration,
    pub(crate) declaration_field_projection: AuthoredToolDeclarationFieldProjection,
    pub(crate) diagnostics: Vec<AuthoredToolDiagnostic>,
    pub(crate) diagnostics_truncated: bool,
    /// The Private Tools the source names directly, relative to
    /// `coral.private`: the caller's own, whoever wrote the source.
    pub(crate) private_references: Vec<PathReference<PrivateToolPath>>,
    /// The Shared Tools the source names directly, relative to
    /// `coral.shared`.
    pub(crate) shared_references: Vec<PathReference<SharedToolPath>>,
    /// Where the declaration's arguments or result hold something that is
    /// not data, as the compiler found it; `None` when they are all data.
    pub(crate) not_data: Option<String>,
}

pub(crate) trait AuthoredToolCompiler: Send + Sync {
    /// Finds the Call Paths to type by `deadline`. `None` leaves the source
    /// untyped, so `prepare` reports any failure of a source this cannot
    /// analyze.
    fn call_paths(&self, deadline: Instant, source: &str) -> Option<NamedCallPaths>;

    /// Prepares `source`, typed by `declarations` when there are any.
    fn prepare(
        &self,
        deadline: Instant,
        source: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<PreparedAuthoredTool, CompileFailure>;
}

struct V8AuthoredToolCompiler {
    compiler: Arc<dyn CompilerClient>,
    /// The Built-in Tools of this Coral version, which type `coral`. Typing
    /// is advisory, so a Workspace whose Built-in catalog another Coral
    /// version wrote costs a save at most a diagnostic.
    builtin_tools: coral_run_executor::BuiltinTools,
}

impl AuthoredToolCompiler for V8AuthoredToolCompiler {
    fn call_paths(&self, deadline: Instant, source: &str) -> Option<NamedCallPaths> {
        self.compiler
            .call_paths(time_left(deadline), source)
            .inspect_err(|error| {
                tracing::debug!(
                    error.type = safe::kind(error.code()),
                    "Call Path analysis failed; the source is checked untyped"
                );
            })
            .ok()
    }

    fn prepare(
        &self,
        deadline: Instant,
        source: &str,
        declarations: Option<&CoralDeclarations>,
    ) -> Result<PreparedAuthoredTool, CompileFailure> {
        let compiled = self.compiler.prepare(
            time_left(deadline),
            source,
            &self.builtin_tools,
            declarations,
        )?;
        let declaration = AuthoredToolDeclaration::from_compiler(compiled.declaration())
            .map_err(|error| error.to_string())?;
        let declaration_field_projection =
            AuthoredToolDeclarationFieldProjection::from_compiler(compiled.declaration_fields());
        let diagnostics_truncated =
            compiled.diagnostics().len() > AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT;
        let diagnostics = compiled
            .diagnostics()
            .iter()
            .take(AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT)
            .map(AuthoredToolDiagnostic::from_compiler)
            .collect();
        Ok(PreparedAuthoredTool {
            declaration,
            declaration_field_projection,
            diagnostics,
            diagnostics_truncated,
            private_references: parsed_references(compiled.references(), PrivateToolPath::parse)?,
            shared_references: parsed_references(
                compiled.shared_references(),
                SharedToolPath::parse,
            )?,
            not_data: compiled.not_data().map(ToOwned::to_owned),
        })
    }
}

fn parsed_references<Path>(
    references: &[coral_v8::LinkedToolReference],
    parse: impl Fn(&str) -> Result<Path, Error>,
) -> Result<Vec<PathReference<Path>>, String> {
    references
        .iter()
        .map(|reference| {
            let path = parse(reference.path()).map_err(|error| {
                format!("at {}:{}: {error}", reference.line(), reference.column())
            })?;
            Ok(PathReference {
                path,
                line: reference.line(),
                column: reference.column(),
            })
        })
        .collect()
}

/// The authoring compiler over the process's one warm `compiler`, which
/// types `coral` with this Coral version's Built-in Tools.
pub(crate) fn v8_compiler(compiler: Arc<dyn CompilerClient>) -> Arc<dyn AuthoredToolCompiler> {
    Arc::new(V8AuthoredToolCompiler {
        compiler,
        builtin_tools: coral_run_executor::BuiltinTools::new(coral_catalog::BuiltinTool::keys()),
    })
}

/// Prepares the `source` of the Tool of `kind` at `relative_path`, typed by
/// `declarations` when there are any, by `deadline`, and refuses a source
/// whose arguments or result are not data. It never executes the source.
pub(crate) async fn prepare_authored_source(
    compiler: Arc<dyn AuthoredToolCompiler>,
    deadline: Instant,
    kind: AuthoredToolKind,
    relative_path: &str,
    source: &str,
    declarations: Option<CoralDeclarations>,
) -> Result<PreparedAuthoredTool, Error> {
    let source_name = kind.source_name(relative_path);
    let compile = {
        let source = source.to_string();
        tokio::task::spawn_blocking(move || {
            compiler.prepare(deadline, &source, declarations.as_ref())
        })
    };
    let prepared = timeout_at(deadline.into(), compile)
        .await
        .map_err(|_elapsed| timed_out(kind))?
        .map_err(|_join_error| Error::CompilerWorkerStopped)?
        .map_err(|failure| match failure {
            CompileFailure::Rejected(message) => {
                rejected(kind, format!("{source_name}: {message}"))
            }
            CompileFailure::Busy => Error::CompilerBusy,
            CompileFailure::TimedOut => timed_out(kind),
            CompileFailure::Stopped => Error::CompilerWorkerStopped,
            CompileFailure::Failed(failure_kind) => {
                tracing::warn!(
                    error.type = safe::kind(failure_kind),
                    "The authoring compiler failed; the caller sees an internal failure"
                );
                Error::CompilerUnavailable(failure_kind.to_string())
            }
        })?;
    // Every call crosses to Coral and back, so only data can (ADR 0033).
    if let Some(reason) = prepared.not_data {
        let message = format!("{source_name}: {reason}");
        return Err(match kind {
            AuthoredToolKind::Private => Error::PrivateToolNotData(message),
            AuthoredToolKind::Shared => Error::SharedToolNotData(message),
        });
    }
    Ok(prepared)
}

/// The refusal of a source of `kind` that cannot become a Tool.
pub(crate) fn rejected(kind: AuthoredToolKind, message: String) -> Error {
    match kind {
        AuthoredToolKind::Private => Error::PrivateToolAdmission(message),
        AuthoredToolKind::Shared => Error::SharedToolAdmission(message),
    }
}

/// The failure of a compilation of `kind` that passed its deadline.
pub(crate) const fn timed_out(kind: AuthoredToolKind) -> Error {
    match kind {
        AuthoredToolKind::Private => Error::PrivateToolCompilationTimedOut,
        AuthoredToolKind::Shared => Error::SharedToolCompilationTimedOut,
    }
}

/// Whether one of two different dotted Tool paths names a namespace that
/// holds the other, as `mail` holds `mail.remind`: both cannot be callable.
pub(crate) fn tool_paths_overlap(current: &str, requested: &str) -> bool {
    let holds = |namespace: &str, path: &str| {
        path.strip_prefix(namespace)
            .is_some_and(|suffix| suffix.starts_with('.'))
    };
    current != requested && (holds(current, requested) || holds(requested, current))
}
