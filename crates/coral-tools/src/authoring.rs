//! Authoring that Private and Shared Tools share: both are Tools that Coral
//! performs from authored source (ADR 0033).
//!
//! [`Authoring`] prepares a source of either kind without executing it, by
//! one deadline: it finds the Call Paths the source names, reads the
//! declarations that type them, and compiles. The result holds the
//! declaration, the advisory diagnostics, the Private and Shared Tools and
//! the Tool chains the source names, and whether its arguments and result
//! are data. A save and a publish type a source the same way, except that
//! `coral.private` is untyped inside a Shared Tool: it is each caller's own.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use coral_db::Database;
use coral_run_executor::{CompilerClient, time_left};
use coral_telemetry::safe;
use coral_v8::{CallableDeclaration, CompileFailure, CoralDeclarations, NamedCallPaths};
use coral_workspace::{Principal, WorkspaceId};
use tokio::time::timeout_at;

use crate::linking::LinkedToolReference;
use crate::shared_tool::SharedToolPath;
use crate::tool_typing::{self, ToolDeclarationReader};
use crate::types::AuthoredToolPath;
use crate::types::{
    AUTHORED_TOOL_DECLARATION_MAX_BYTES, AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT,
    AuthoredToolDeclaration, AuthoredToolDeclarationFieldProjection, AuthoredToolDiagnostic,
    PrivateToolPath,
};
use crate::{Error, repository, shared_tool_repository};

pub(crate) fn now_unix_nanos() -> Result<i64, Error> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Clock(error.to_string()))?;
    i64::try_from(elapsed.as_nanos())
        .map_err(|_error| Error::Clock("current time exceeds durable nanosecond range".to_string()))
}

/// What the compiler made of one authored source.
pub(crate) struct PreparedAuthoredTool {
    pub(crate) declaration: AuthoredToolDeclaration,
    pub(crate) declaration_field_projection: AuthoredToolDeclarationFieldProjection,
    pub(crate) diagnostics: Vec<AuthoredToolDiagnostic>,
    pub(crate) diagnostics_truncated: bool,
    /// The Private and Shared Tools the source names directly. A Private
    /// Tool is the caller's own, whoever wrote the source.
    pub(crate) linked: Vec<LinkedToolReference>,
    /// The literal Tool chains under `coral` the source names, from the
    /// compile that emitted the declaration.
    pub(crate) tools: coral_v8::ToolReferences,
    /// Where the declaration's arguments or result hold something that is
    /// not data, as the compiler found it; `None` when they are all data.
    pub(crate) not_data: Option<String>,
}

impl PreparedAuthoredTool {
    /// Every Call Path the source calls by a literal path, once each, in
    /// path order: Provider and Built-in Tools, the caller's Private Tools
    /// and Shared Tools.
    pub(crate) fn called_tools(&self) -> Vec<String> {
        let mut called = self.tools.called_paths();
        called.extend(
            self.linked
                .iter()
                .map(|reference| reference.path().call_path()),
        );
        called.sort();
        called.dedup();
        called
    }
}

/// What types an authored source beside the Workspace's current Shared
/// Tools, which always type `coral.shared`.
#[derive(Clone, Copy)]
pub(crate) struct Typing<'a> {
    /// The Principal whose current Private Tools type `coral.private`.
    /// `None` leaves the root untyped: inside a Shared Tool, `coral.private`
    /// is each caller's own.
    pub(crate) private_tools_of: Option<&'a Principal>,
    /// Reads the declarations of the Provider and Built-in Tools the author
    /// can discover: the saving Principal, or the publishing Owner. `None`
    /// leaves them untyped.
    pub(crate) tools: Option<&'a dyn ToolDeclarationReader>,
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
            linked: LinkedToolReference::from_compiler(
                compiled.references(),
                compiled.shared_references(),
            )
            .map_err(|invalid| {
                format!("at {}:{}: {}", invalid.line, invalid.column, invalid.error)
            })?,
            tools: compiled.tools().clone(),
            not_data: compiled.not_data().map(ToOwned::to_owned),
        })
    }
}

/// The authoring compiler over the process's one warm `compiler`, which
/// types `coral` with this Coral version's Built-in Tools.
pub(crate) fn v8_compiler(compiler: Arc<dyn CompilerClient>) -> Arc<dyn AuthoredToolCompiler> {
    Arc::new(V8AuthoredToolCompiler {
        compiler,
        builtin_tools: coral_run_executor::BuiltinTools::new(coral_catalog::BuiltinTool::keys()),
    })
}

/// Prepares authored sources of both kinds, through the process's one warm
/// compiler.
#[derive(Clone)]
pub(crate) struct Authoring {
    database: Arc<Database>,
    compiler: Arc<dyn AuthoredToolCompiler>,
    /// The deadline for preparing one source, including typing and queueing.
    compilation_timeout: Duration,
}

impl Authoring {
    pub(crate) const fn new(
        database: Arc<Database>,
        compiler: Arc<dyn AuthoredToolCompiler>,
        compilation_timeout: Duration,
    ) -> Self {
        Self {
            database,
            compiler,
            compilation_timeout,
        }
    }

    /// Prepares the `source` of the Tool at `path`, typed as `typing` says,
    /// and refuses a source whose arguments or result are not data. It
    /// never executes the source.
    ///
    /// Typing is advisory and stops by its own deadline, so a compile always
    /// keeps its reserve: a source that cannot be analyzed or typed in time
    /// is prepared untyped, and preparation reports its own failure.
    ///
    /// # Errors
    ///
    /// Returns the refusal of a source that cannot become a Tool, a timeout,
    /// or an unavailable compiler.
    pub(crate) async fn prepare(
        &self,
        workspace_id: &WorkspaceId,
        path: &AuthoredToolPath,
        source: &str,
        typing: Typing<'_>,
    ) -> Result<PreparedAuthoredTool, Error> {
        let kind = path.kind();
        let deadline = Instant::now() + self.compilation_timeout;
        let preparation = async {
            let declarations = self
                .declarations(workspace_id, path, source, typing, deadline)
                .await?;
            self.compile(deadline, path, source, declarations).await
        };
        timeout_at(deadline.into(), preparation)
            .await
            .map_err(|_elapsed| Error::CompilationTimedOut(kind))?
    }

    /// The declarations that type `source`: the current ones of the Shared
    /// Tools it names and, as `typing` says, of the Private Tools and the
    /// Provider and Built-in Tools it names. `None` leaves it untyped.
    async fn declarations(
        &self,
        workspace_id: &WorkspaceId,
        path: &AuthoredToolPath,
        source: &str,
        typing: Typing<'_>,
        deadline: Instant,
    ) -> Result<Option<CoralDeclarations>, Error> {
        let Some(typing_deadline) = tool_typing::typing_deadline(deadline) else {
            tracing::warn!("No time is left to type the source; it is checked untyped");
            return Ok(None);
        };
        let typed = async {
            let compiler = self.compiler.clone();
            let analyzed = source.to_string();
            let Some(named) = tokio::task::spawn_blocking(move || {
                compiler.call_paths(typing_deadline, &analyzed)
            })
            .await
            .map_err(|_join_error| Error::CompilerWorkerStopped)?
            else {
                return Ok(None);
            };
            let Ok(linked) =
                LinkedToolReference::from_compiler(named.private_tools(), named.shared_tools())
            else {
                return Ok(None);
            };
            let private_paths = linked
                .iter()
                .filter_map(|reference| match reference.path() {
                    AuthoredToolPath::Private(named) => Some(named),
                    AuthoredToolPath::Shared(_) => None,
                })
                .collect::<Vec<_>>();
            let shared_paths = linked
                .iter()
                .filter_map(|reference| match reference.path() {
                    AuthoredToolPath::Shared(named) => Some(named),
                    AuthoredToolPath::Private(_) => None,
                })
                .collect::<Vec<_>>();
            // A Private Tool that names itself is typed by its current
            // definition, as its other callers are.
            let private_tools = match typing.private_tools_of {
                Some(owner) if !private_paths.is_empty() => {
                    self.private_declarations(workspace_id, owner, &private_paths)
                        .await
                }
                _ => None,
            };
            // A Shared Tool that names itself is not typed by its current
            // revision: the declaration being prepared replaces it for every
            // caller at once. Naming only itself still types the root.
            let shared_tools = if shared_paths.is_empty() {
                None
            } else {
                let others = shared_paths
                    .into_iter()
                    .filter(|named| AuthoredToolPath::Shared((*named).clone()) != *path)
                    .collect::<Vec<_>>();
                self.shared_declarations(workspace_id, &others).await
            };
            let tools = match typing.tools {
                Some(reader) => {
                    tool_typing::tool_declarations(reader, named.tools().call_paths()).await
                }
                None => None,
            };
            if private_tools.is_none() && shared_tools.is_none() && tools.is_none() {
                return Ok(None);
            }
            let mut declarations = CoralDeclarations::new(AUTHORED_TOOL_DECLARATION_MAX_BYTES);
            if let Some(private_tools) = private_tools {
                declarations = declarations.with_private_tools(private_tools);
            }
            if let Some(shared_tools) = shared_tools {
                declarations = declarations.with_shared_tools(shared_tools);
            }
            if let Some(tools) = tools {
                declarations = declarations.with_tools(named.tools(), tools);
            }
            Ok(Some(declarations))
        };
        timeout_at(typing_deadline.into(), typed)
            .await
            .unwrap_or_else(|_elapsed| {
                tracing::warn!("Typing was too slow; the source is checked untyped");
                Ok(None)
            })
    }

    /// The current declaration of each Private Tool of `owner` at `paths`,
    /// or `None`, which leaves `coral.private` untyped, when they cannot be
    /// read.
    async fn private_declarations(
        &self,
        workspace_id: &WorkspaceId,
        owner: &Principal,
        paths: &[&PrivateToolPath],
    ) -> Option<Vec<CallableDeclaration>> {
        let read = async {
            let mut transaction = self.database.begin_read_snapshot().await?;
            let mut declarations = Vec::new();
            for path in paths {
                if let Some(tool) = repository::find_current_in_transaction(
                    &mut transaction,
                    workspace_id,
                    owner,
                    path,
                )
                .await?
                {
                    declarations.push(CallableDeclaration::new(
                        path.segments(),
                        tool.declaration().as_str().to_string(),
                    ));
                }
            }
            transaction.commit().await?;
            Ok::<_, Error>(declarations)
        };
        untyped_on_failure(read.await)
    }

    /// The current declaration of each live Shared Tool at `paths`, or
    /// `None`, which leaves `coral.shared` untyped, when they cannot be
    /// read.
    async fn shared_declarations(
        &self,
        workspace_id: &WorkspaceId,
        paths: &[&SharedToolPath],
    ) -> Option<Vec<CallableDeclaration>> {
        let read = async {
            let mut transaction = self.database.begin_read_snapshot().await?;
            let mut declarations = Vec::new();
            for path in paths {
                if let Some(tool) =
                    shared_tool_repository::find_live_tool(&mut transaction, workspace_id, path)
                        .await?
                {
                    declarations.push(CallableDeclaration::new(
                        path.segments(),
                        tool.current.declaration.as_str().to_string(),
                    ));
                }
            }
            transaction.commit().await?;
            Ok::<_, Error>(declarations)
        };
        untyped_on_failure(read.await)
    }

    /// Compiles `source`, typed by `declarations` when there are any, by
    /// `deadline`.
    async fn compile(
        &self,
        deadline: Instant,
        path: &AuthoredToolPath,
        source: &str,
        declarations: Option<CoralDeclarations>,
    ) -> Result<PreparedAuthoredTool, Error> {
        let kind = path.kind();
        // The error names the kind, so its reason names the path alone.
        let call_path = path.call_path();
        let compile = {
            let compiler = self.compiler.clone();
            let source = source.to_string();
            tokio::task::spawn_blocking(move || {
                compiler.prepare(deadline, &source, declarations.as_ref())
            })
        };
        let prepared = timeout_at(deadline.into(), compile)
            .await
            .map_err(|_elapsed| Error::CompilationTimedOut(kind))?
            .map_err(|_join_error| Error::CompilerWorkerStopped)?
            .map_err(|failure| match failure {
                CompileFailure::Rejected(message) => {
                    Error::Admission(kind, format!("{call_path}: {message}"))
                }
                CompileFailure::Busy => Error::CompilerBusy,
                CompileFailure::TimedOut => Error::CompilationTimedOut(kind),
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
            return Err(Error::NotData(kind, format!("{call_path}: {reason}")));
        }
        Ok(prepared)
    }
}

/// The declarations read, or `None` with a warning when the read failed:
/// typing never fails a preparation.
fn untyped_on_failure(
    read: Result<Vec<CallableDeclaration>, Error>,
) -> Option<Vec<CallableDeclaration>> {
    read.inspect_err(|error| {
        tracing::warn!(
            error.type = safe::kind(error.code()),
            "Tool declarations are unavailable; they are checked untyped"
        );
    })
    .ok()
}

/// The refusal of the source at `source` that names `reference`, a path no
/// Tool is at: worded as any path that names no Tool. Authoring looks up no
/// suggestions.
pub(crate) fn missing_linked_tool(
    source: &AuthoredToolPath,
    reference: &LinkedToolReference,
) -> Error {
    Error::Admission(
        source.kind(),
        format!(
            "{}:{}:{}: {}",
            source.call_path(),
            reference.line(),
            reference.column(),
            coral_catalog::unknown_call_path_message(&reference.path().call_path(), &[])
        ),
    )
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
