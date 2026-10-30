use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use coral_telemetry::metric::{self, CompilerOperation, CompilerOutcome};
use serde::Deserialize;

use crate::profile::TYPESCRIPT_PROFILE;
use crate::source_map::{AuthoredSourceMapping, normalize as normalize_source_map};
use crate::v8_support::{format_thrown_value, read_global_string, run_script};
use crate::{
    CompiledProgram, CompilerDiagnostic, CoralDeclarations, DiagnosticCategory, Error,
    IsolateLimits, LinkedSavedFunctionSource, ProgramExecutionControl, SavedFunctionCallPaths,
    SavedFunctionCompilation, SavedFunctionDeclarationField, SavedFunctionReference,
    ToolReferences,
};

const TYPESCRIPT_SOURCE: &str = include_str!("../vendor/typescript.js");
const TYPESCRIPT_LIBRARIES: &str = include_str!("../vendor/typescript-libs.json");
const FORBIDDEN_AMBIENT_GLOBALS: &str = include_str!("js/forbidden_ambient_globals.js");
// These fragments form one private lexical implementation. The final bridge
// fragment is the only one that publishes values into the isolate global.
const COMPILER_BRIDGE: &str = concat!(
    "\"use strict\";\n(() => {\n",
    include_str!("js/compiler/typescript_environment.js"),
    "\n",
    include_str!("js/compiler/authored_source.js"),
    "\n",
    include_str!("js/compiler/authored_source_policy.js"),
    "\n",
    include_str!("js/compiler/saved_function_authoring.js"),
    "\n",
    include_str!("js/compiler/saved_function_linking.js"),
    "\n",
    include_str!("js/compiler/program_compilation.js"),
    "\n",
    include_str!("js/compiler/bridge.js"),
    "\n})();\n",
);
const PROGRAM_FILE_NAME: &str = "program.ts";
const SAVED_FUNCTION_FILE_NAME: &str = "saved_function.ts";

/// Maximum elapsed time for one compiler request, including queueing.
pub const COMPILATION_TIMEOUT: Duration = Duration::from_secs(2);
// Leave headroom above the measured ~130 MiB transient authoring working set.
const COMPILER_RECYCLE_HEAP_BYTES: usize = 256 * 1024 * 1024;
type CompilerJob = Box<dyn FnOnce(&mut Option<CompilerIsolate>) + Send>;

/// A pinned official TypeScript compiler running on its own V8 thread.
///
/// Pass the same admission deadline to reference analysis and linked compilation.
/// Requests include queueing time and are capped by [`COMPILATION_TIMEOUT`]. An
/// expired request terminates only its own work; the worker replaces failed or
/// overgrown isolates before serving another request.
pub struct ProgramCompiler {
    sender: mpsc::SyncSender<Option<CompilerJob>>,
    worker: Option<thread::JoinHandle<()>>,
    limits: IsolateLimits,
}

impl ProgramCompiler {
    /// Starts one warm compiler-only isolate with deployment-selected heap budgets.
    ///
    /// The isolate receives only the packaged compiler assets and virtual
    /// source sent to this Interface. It has no Program runtime bindings.
    ///
    /// # Errors
    ///
    /// Returns an initialization or memory-limit error when V8 or the packaged
    /// compiler cannot start within the selected budget, or when the packaged
    /// compiler version differs from the profile.
    pub fn new(isolate_limits: IsolateLimits) -> Result<Self, Error> {
        crate::v8_support::initialize()?;
        // ponytail: one warm isolate and 32 queued requests; use a pool only if
        // measured authoring throughput needs it. Saturation never queues unbounded work.
        let (sender, receiver) = mpsc::sync_channel::<Option<CompilerJob>>(32);
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("coral-typescript-compiler".to_string())
            .spawn(move || {
                let mut compiler = match CompilerIsolate::new(isolate_limits) {
                    Ok(compiler) => Some(compiler),
                    Err(error) => {
                        let _sent = startup_sender.send(Err(error));
                        return;
                    }
                };
                if startup_sender.send(Ok(())).is_err() {
                    return;
                }
                while let Ok(Some(job)) = receiver.recv() {
                    job(&mut compiler);
                }
            })
            .map_err(Error::CompilerThread)?;

        match startup_receiver.recv() {
            Ok(Ok(())) => Ok(Self {
                sender,
                worker: Some(worker),
                limits: isolate_limits,
            }),
            Ok(Err(error)) => {
                if worker.join().is_err() {
                    return Err(Error::CompilerUnavailable);
                }
                Err(error)
            }
            Err(_) => {
                if worker.join().is_err() {
                    return Err(Error::CompilerUnavailable);
                }
                Err(Error::CompilerUnavailable)
            }
        }
    }

    /// Lowers one TypeScript async-function body into JavaScript and a source map.
    ///
    /// JavaScript is valid input because it is valid TypeScript source. The
    /// official compiler applies syntax and Coral capability checks here;
    /// ordinary semantic diagnostics are not an execution gate. The retained
    /// source map contains authored locations only.
    ///
    /// # Errors
    ///
    /// Returns blocking syntax or capability diagnostics, an emitted-source-map
    /// failure, a compiler-isolate failure, or a compiler worker availability
    /// error.
    pub fn compile(&self, deadline: Instant, source: &str) -> Result<CompiledProgram, Error> {
        let source = source.to_string();
        self.request(CompilerOperation::Program, deadline, move |compiler| {
            compiler.compile(&source)
        })
    }

    /// Emits and validates the self-contained declaration for one Saved Function.
    ///
    /// `coral.providers`, `coral.workspace` and `coral.functions` are `any`.
    /// Strict semantic diagnostics are returned as advisory authoring evidence.
    /// Syntax, imports, invalid export shape, declaration-emission failure, and
    /// unresolved names in the emitted public declaration are blocking.
    ///
    /// # Errors
    ///
    /// Returns a blocking authoring or compiler-isolate failure.
    pub fn prepare_saved_function(
        &self,
        deadline: Instant,
        source: &str,
    ) -> Result<SavedFunctionCompilation, Error> {
        let source = source.to_string();
        self.request(
            CompilerOperation::SavedFunction,
            deadline,
            move |compiler| compiler.prepare_saved_function(&source, "null"),
        )
    }

    /// Prepares one Saved Function as [`Self::prepare_saved_function`] does,
    /// with the Call Paths it names typed by `declarations`.
    ///
    /// A result type from `declarations` is inlined into the declaration. When
    /// it cannot be inlined, or the declaration would exceed the limit in
    /// `declarations`, the declaration is emitted untyped and a diagnostic says
    /// why, so typing never fails a save.
    ///
    /// # Errors
    ///
    /// Returns a blocking authoring or compiler-isolate failure.
    pub fn prepare_typed_saved_function(
        &self,
        deadline: Instant,
        source: &str,
        declarations: &CoralDeclarations,
    ) -> Result<SavedFunctionCompilation, Error> {
        let declarations_json = serde_json::to_string(declarations)
            .map_err(|error| Error::CompilerProtocol(error.to_string()))?;
        let source = source.to_string();
        self.request(
            CompilerOperation::SavedFunction,
            deadline,
            move |compiler| compiler.prepare_saved_function(&source, &declarations_json),
        )
    }

    /// Finds the Saved Functions and Tools one Saved Function module
    /// names directly, to type it.
    ///
    /// # Errors
    ///
    /// Rejects computed or aliased `coral.functions` access and malformed
    /// module source.
    pub fn saved_function_call_paths(
        &self,
        deadline: Instant,
        source: &str,
    ) -> Result<SavedFunctionCallPaths, Error> {
        let source = source.to_string();
        self.request(CompilerOperation::Analysis, deadline, move |compiler| {
            compiler.saved_function_call_paths(&source)
        })
    }

    /// Finds directly named `coral.functions` paths in one Program body.
    ///
    /// # Errors
    ///
    /// Rejects computed or aliased access and malformed wrapper escapes.
    pub fn saved_function_paths(
        &self,
        deadline: Instant,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, Error> {
        let source = source.to_string();
        self.request(CompilerOperation::Analysis, deadline, move |compiler| {
            compiler.saved_function_paths(&source)
        })
    }

    /// Finds directly named `coral.functions` paths in one Saved Function module.
    ///
    /// # Errors
    ///
    /// Rejects computed or aliased access and malformed module source.
    pub fn saved_function_dependency_paths(
        &self,
        deadline: Instant,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, Error> {
        let source = source.to_string();
        self.request(CompilerOperation::Analysis, deadline, move |compiler| {
            compiler.saved_function_dependency_paths(&source)
        })
    }

    /// Lowers one Program body together with its exact Saved Function source closure.
    ///
    /// Linked modules initialize once in the supplied dependency order before
    /// the submitted body. Their callables are installed under immutable
    /// `coral.functions` paths inside the package.
    ///
    /// # Errors
    ///
    /// Returns blocking compiler diagnostics, invalid linked source, or a
    /// compiler-isolate failure.
    pub fn compile_with_saved_functions(
        &self,
        deadline: Instant,
        source: &str,
        saved_functions: &[LinkedSavedFunctionSource],
    ) -> Result<CompiledProgram, Error> {
        let saved_functions_json = serde_json::to_string(saved_functions)
            .map_err(|error| Error::CompilerProtocol(error.to_string()))?;
        let source = source.to_string();
        self.request(CompilerOperation::Program, deadline, move |compiler| {
            compiler.compile_with_saved_functions(&source, &saved_functions_json)
        })
    }

    fn request<T: Send + 'static>(
        &self,
        operation: CompilerOperation,
        deadline: Instant,
        work: impl FnOnce(&mut CompilerIsolate) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let started = Instant::now();
        let result = self.request_kind(operation, deadline, work);
        metric::compiler_requested(operation, compiler_outcome(&result), started.elapsed());
        result
    }

    fn request_kind<T: Send + 'static>(
        &self,
        operation: CompilerOperation,
        deadline: Instant,
        work: impl FnOnce(&mut CompilerIsolate) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let deadline = deadline.min(Instant::now() + COMPILATION_TIMEOUT);
        let control = ProgramExecutionControl::new();
        let worker_control = control.clone();
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        let limits = self.limits;
        let queued = metric::compiler_queued(operation);
        self.sender
            .try_send(Some(Box::new(move |compiler| {
                let active_measurement = queued.dequeued();
                let result = (|| {
                    if Instant::now() >= deadline || worker_control.is_requested() {
                        return Err(Error::CompilationTimedOut);
                    }
                    if compiler.is_none() {
                        match CompilerIsolate::new(limits) {
                            Ok(fresh) => *compiler = Some(fresh),
                            Err(error) => {
                                return Err(error);
                            }
                        }
                    }
                    let active = compiler.as_mut().expect("compiler initialized");
                    worker_control.install(&active.isolate.thread_safe_handle());
                    let mut result = work(active);
                    worker_control.uninstall();
                    if worker_control.is_requested() || Instant::now() >= deadline {
                        result = Err(Error::CompilationTimedOut);
                    }
                    if result.is_err()
                        || active.heap_requires_recycling(COMPILER_RECYCLE_HEAP_BYTES)
                    {
                        *compiler = None;
                    }
                    result
                })();
                active_measurement.finish(compiler_outcome(&result));
                let _sent = response_sender.send(result);
            })))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => Error::CompilerBusy,
                mpsc::TrySendError::Disconnected(_) => Error::CompilerUnavailable,
            })?;
        match response_receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _installed = control.terminate();
                Err(Error::CompilationTimedOut)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(Error::CompilerUnavailable),
        }
    }
}

fn compiler_outcome<T>(result: &Result<T, Error>) -> CompilerOutcome {
    match result {
        Ok(_) => CompilerOutcome::Ok,
        Err(Error::CompilerBusy) => CompilerOutcome::QueueFull,
        Err(Error::CompilerUnavailable | Error::CompilerThread(_)) => CompilerOutcome::Stopped,
        Err(Error::CompilationTimedOut) => CompilerOutcome::TimedOut,
        Err(
            Error::Compilation { .. }
            | Error::InvalidProgramBody
            | Error::InvalidSavedFunction(_)
            | Error::ForbiddenCapability(_)
            | Error::InvalidSavedFunctionReference { .. },
        ) => CompilerOutcome::Rejected,
        Err(_) => CompilerOutcome::Failed,
    }
}

impl Drop for ProgramCompiler {
    fn drop(&mut self) {
        if self.sender.send(None).is_err() {
            // The worker has already stopped.
        }
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            // Drop cannot report a compiler-thread panic.
        }
    }
}

struct CompilerIsolate {
    isolate: v8::OwnedIsolate,
    context: v8::Global<v8::Context>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompileOutput {
    output_text: String,
    source_map_text: Option<String>,
    authored_mappings: Vec<AuthoredSourceMapping>,
    wrapper_valid: bool,
    validation_error: Option<String>,
    diagnostics: Vec<CompilerDiagnostic>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SavedFunctionOutput {
    declaration_text: Option<String>,
    declaration_fields: Option<Vec<SavedFunctionDeclarationField>>,
    declaration_projection_error: Option<String>,
    validation_error: Option<String>,
    reference_error: Option<SavedFunctionReferenceError>,
    blocking_diagnostics: Vec<CompilerDiagnostic>,
    diagnostics: Vec<CompilerDiagnostic>,
    references: Vec<SavedFunctionReference>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SavedFunctionPathsOutput {
    references: Vec<SavedFunctionReference>,
    validation_error: Option<SavedFunctionReferenceError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SavedFunctionCallPathsOutput {
    references: Vec<SavedFunctionReference>,
    validation_error: Option<SavedFunctionReferenceError>,
    tools: Option<ToolReferences>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SavedFunctionReferenceError {
    message: String,
    line: u64,
    column: u64,
}

impl CompilerIsolate {
    fn new(limits: IsolateLimits) -> Result<Self, Error> {
        let mut isolate = v8::Isolate::new(
            v8::CreateParams::default()
                .allow_atomics_wait(false)
                .heap_limits(0, limits.compiler_heap_bytes()),
        );
        let context = crate::memory::with_heap_limit(&mut isolate, |isolate| {
            v8::scope!(let scope, isolate);
            let context = v8::Context::new(scope, v8::ContextOptions::default());
            let context_global = v8::Global::new(scope, context);
            let scope = &mut v8::ContextScope::new(scope, context);
            run_script(scope, TYPESCRIPT_SOURCE, "typescript.js")?;
            run_script(
                scope,
                FORBIDDEN_AMBIENT_GLOBALS,
                "coral_forbidden_ambient_globals.js",
            )?;
            run_script(scope, COMPILER_BRIDGE, "coral_compiler_bridge.js")?;
            let actual_version = read_global_string(scope, "__coralTypeScriptVersion")?;
            if actual_version != TYPESCRIPT_PROFILE.compiler_version() {
                return Err(Error::Initialization(format!(
                    "packaged TypeScript version {actual_version} does not match profile {}",
                    TYPESCRIPT_PROFILE.compiler_version(),
                )));
            }
            Ok(context_global)
        })?;
        let mut compiler = Self { isolate, context };
        compiler.configure_libraries()?;
        Ok(compiler)
    }

    fn heap_requires_recycling(&mut self, maximum_bytes: usize) -> bool {
        if self.isolate.get_heap_statistics().used_heap_size() < maximum_bytes {
            return false;
        }
        // Allocated garbage is not retained growth. Let V8 collect before
        // discarding the compiler assets and warmed code in this isolate.
        self.isolate.low_memory_notification();
        self.isolate.get_heap_statistics().used_heap_size() >= maximum_bytes
    }

    fn configure_libraries(&mut self) -> Result<(), Error> {
        self.call("__coralConfigureCompiler", &[TYPESCRIPT_LIBRARIES])
            .map(drop)
    }

    fn compile(&mut self, body: &str) -> Result<CompiledProgram, Error> {
        let json = self.call(
            "__coralCompile",
            &[
                body,
                PROGRAM_FILE_NAME,
                TYPESCRIPT_PROFILE.javascript_target(),
            ],
        )?;
        decode_compiled_program(&json)
    }

    fn prepare_saved_function(
        &mut self,
        source: &str,
        declarations_json: &str,
    ) -> Result<SavedFunctionCompilation, Error> {
        let json = self.call(
            "__coralPrepareSavedFunction",
            &[
                source,
                declarations_json,
                SAVED_FUNCTION_FILE_NAME,
                TYPESCRIPT_PROFILE.javascript_target(),
            ],
        )?;
        let output: SavedFunctionOutput = serde_json::from_str(&json)
            .map_err(|error| Error::CompilerProtocol(error.to_string()))?;
        if !output.blocking_diagnostics.is_empty() {
            return Err(Error::Compilation {
                diagnostics: output.blocking_diagnostics,
            });
        }
        if let Some(error) = output.reference_error {
            return Err(invalid_saved_function_reference(error));
        }
        if let Some(message) = output.validation_error {
            return Err(Error::InvalidSavedFunction(message));
        }
        let declaration = output.declaration_text.ok_or_else(|| {
            Error::CompilerProtocol("compiler emitted no Saved Function declaration".to_string())
        })?;
        let declaration_field_projection = match (
            output.declaration_fields,
            output.declaration_projection_error,
        ) {
            (Some(fields), None) => Ok(fields),
            (None, Some(reason)) if !reason.is_empty() => Err(reason),
            _ => {
                return Err(Error::CompilerProtocol(
                    "compiler returned an invalid declaration-field projection outcome".to_string(),
                ));
            }
        };
        Ok(SavedFunctionCompilation::new(
            declaration,
            declaration_field_projection,
            output.diagnostics,
            output.references,
        ))
    }

    fn saved_function_call_paths(&mut self, source: &str) -> Result<SavedFunctionCallPaths, Error> {
        let json = self.call(
            "__coralAnalyzeSavedFunctionCallPaths",
            &[
                source,
                SAVED_FUNCTION_FILE_NAME,
                TYPESCRIPT_PROFILE.javascript_target(),
            ],
        )?;
        let output: SavedFunctionCallPathsOutput = serde_json::from_str(&json)
            .map_err(|error| Error::CompilerProtocol(error.to_string()))?;
        match (output.validation_error, output.tools) {
            (Some(error), _) => Err(invalid_saved_function_reference(error)),
            (None, Some(tools)) => Ok(SavedFunctionCallPaths {
                functions: output.references,
                tools,
            }),
            (None, None) => Err(Error::CompilerProtocol(
                "compiler returned no Tool references".to_string(),
            )),
        }
    }

    fn saved_function_paths(&mut self, source: &str) -> Result<Vec<SavedFunctionReference>, Error> {
        self.analyze_saved_function_paths("__coralAnalyzeSavedFunctionPaths", source)
    }

    fn saved_function_dependency_paths(
        &mut self,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, Error> {
        self.analyze_saved_function_paths("__coralAnalyzeSavedFunctionDependencies", source)
    }

    fn analyze_saved_function_paths(
        &mut self,
        function: &str,
        source: &str,
    ) -> Result<Vec<SavedFunctionReference>, Error> {
        let json = self.call(
            function,
            &[
                source,
                PROGRAM_FILE_NAME,
                TYPESCRIPT_PROFILE.javascript_target(),
            ],
        )?;
        let output: SavedFunctionPathsOutput = serde_json::from_str(&json)
            .map_err(|error| Error::CompilerProtocol(error.to_string()))?;
        if let Some(error) = output.validation_error {
            Err(invalid_saved_function_reference(error))
        } else {
            Ok(output.references)
        }
    }

    fn compile_with_saved_functions(
        &mut self,
        source: &str,
        saved_functions_json: &str,
    ) -> Result<CompiledProgram, Error> {
        let json = self.call(
            "__coralCompileLinkedProgram",
            &[
                source,
                saved_functions_json,
                PROGRAM_FILE_NAME,
                TYPESCRIPT_PROFILE.javascript_target(),
            ],
        )?;
        decode_compiled_program(&json)
    }

    fn call(&mut self, function_name: &str, arguments: &[&str]) -> Result<String, Error> {
        let context = &self.context;
        crate::memory::with_heap_limit(&mut self.isolate, |isolate| {
            v8::scope!(let scope, isolate);
            let context = v8::Local::new(scope, context);
            let scope = &mut v8::ContextScope::new(scope, context);
            let global = context.global(scope);
            let key = v8::String::new(scope, function_name).ok_or_else(|| {
                Error::Engine("failed to allocate compiler function key".to_string())
            })?;
            let value = global
                .get(scope, key.into())
                .ok_or_else(|| Error::Engine("compiler bridge function is missing".to_string()))?;
            let function = v8::Local::<v8::Function>::try_from(value).map_err(|_error| {
                Error::Engine("compiler bridge value is not a function".to_string())
            })?;
            let arguments = arguments
                .iter()
                .map(|argument| {
                    v8::String::new(scope, argument)
                        .map(Into::into)
                        .ok_or_else(|| {
                            Error::Engine("failed to allocate compiler input".to_string())
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let try_catch = std::pin::pin!(v8::TryCatch::new(scope));
            let mut try_catch = try_catch.init();
            let receiver = v8::undefined(&try_catch).into();
            let value = function
                .call(&try_catch, receiver, &arguments)
                .ok_or_else(|| match try_catch.exception() {
                    Some(exception) => {
                        Error::Engine(format_thrown_value(&mut try_catch, exception))
                    }
                    None => Error::Engine("TypeScript compiler bridge failed".to_string()),
                })?;
            Ok(value
                .to_string(&try_catch)
                .ok_or_else(|| Error::Engine("compiler bridge did not return JSON".to_string()))?
                .to_rust_string_lossy(&try_catch))
        })
    }
}

fn invalid_saved_function_reference(error: SavedFunctionReferenceError) -> Error {
    Error::InvalidSavedFunctionReference {
        message: error.message,
        line: error.line,
        column: error.column,
    }
}

fn decode_compiled_program(json: &str) -> Result<CompiledProgram, Error> {
    let output: CompileOutput =
        serde_json::from_str(json).map_err(|error| Error::CompilerProtocol(error.to_string()))?;
    if output
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.category() == DiagnosticCategory::Error)
    {
        return Err(Error::Compilation {
            diagnostics: output.diagnostics,
        });
    }
    if let Some(message) = output.validation_error {
        return Err(Error::ForbiddenCapability(message));
    }
    if !output.wrapper_valid {
        return Err(Error::InvalidProgramBody);
    }
    let raw_source_map = output
        .source_map_text
        .ok_or_else(|| Error::CompilerProtocol("compiler emitted no source map".to_string()))?;
    let source_map = normalize_source_map(&raw_source_map, &output.authored_mappings)?;
    Ok(CompiledProgram::new(
        output.output_text,
        source_map,
        output.diagnostics,
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn timed_out_callers_stay_queued_until_dequeued_and_full_is_distinct() {
        let metrics = coral_telemetry::testing::MetricCapture::default();
        let _measured = tracing::subscriber::set_default(metrics.subscriber());
        let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
        let (entered, started) = mpsc::channel();
        let (release, held) = mpsc::channel();
        std::thread::scope(|scope| {
            let capture = metrics.clone();
            let compiler = &compiler;
            let blocker = scope.spawn(move || {
                tracing::subscriber::with_default(capture.subscriber(), || {
                    compiler.request(
                        CompilerOperation::Analysis,
                        Instant::now() + COMPILATION_TIMEOUT,
                        move |_| {
                            let _sent = entered.send(());
                            let _released = held.recv();
                            Ok(())
                        },
                    )
                })
            });
            started
                .recv_timeout(COMPILATION_TIMEOUT)
                .expect("worker is held");
            for _ in 0..32 {
                assert!(matches!(
                    compiler.compile(Instant::now(), "return 1;"),
                    Err(Error::CompilationTimedOut)
                ));
            }
            let depth = || {
                metrics
                    .named("coral.compiler.queue.depth")
                    .iter()
                    .map(|point| point.value)
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                depth(),
                [32.0],
                "caller timeout does not remove the queued job"
            );
            assert!(matches!(
                compiler.prepare_saved_function(Instant::now(), "export default () => 1;"),
                Err(Error::CompilerBusy)
            ));
            assert_eq!(
                depth(),
                [32.0],
                "a rejected enqueue releases its gauge contribution"
            );
            assert!(
                metrics
                    .named("coral.compiler.request.duration")
                    .iter()
                    .any(|point| point.attributes.get("outcome").map(String::as_str)
                        == Some("queue_full"))
            );
            release.send(()).expect("release worker");
            blocker.join().unwrap().expect("blocking work ended");
        });
        compiler
            .compile(Instant::now() + COMPILATION_TIMEOUT, "return 42;")
            .expect("worker drains expired requests and recovers");
        for name in ["coral.compiler.queue.depth", "coral.compiler.active"] {
            assert_eq!(
                metrics
                    .named(name)
                    .iter()
                    .map(|point| point.value)
                    .collect::<Vec<_>>(),
                [0.0]
            );
        }
    }

    #[test]
    fn compiler_metrics_cover_programs_and_saved_function_authoring() {
        let metrics = coral_telemetry::testing::MetricCapture::default();
        let _measured = tracing::subscriber::set_default(metrics.subscriber());
        let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
        compiler
            .compile(Instant::now() + COMPILATION_TIMEOUT, "return 42;")
            .expect("Program");
        compiler
            .prepare_saved_function(
                Instant::now() + COMPILATION_TIMEOUT,
                "export default function answer() { return 42; }",
            )
            .expect("Saved Function");
        for name in [
            "coral.compiler.request.duration",
            "coral.compiler.queue.duration",
            "coral.compiler.work.duration",
        ] {
            let points = metrics.named(name);
            for operation in ["program", "saved_function"] {
                let point = points
                    .iter()
                    .find(|point| {
                        point.attributes.get("operation").map(String::as_str) == Some(operation)
                    })
                    .unwrap_or_else(|| panic!("{name} covers {operation}"));
                assert_eq!(point.count, 1);
            }
        }
        for name in ["coral.compiler.queue.depth", "coral.compiler.active"] {
            assert_eq!(
                metrics
                    .named(name)
                    .iter()
                    .map(|point| point.value)
                    .collect::<Vec<_>>(),
                [0.0]
            );
        }
    }
    use super::*;

    fn script(compiler: &mut CompilerIsolate, source: &str) -> Result<(), Error> {
        v8::scope!(let scope, &mut compiler.isolate);
        let context = v8::Local::new(scope, &compiler.context);
        let scope = &mut v8::ContextScope::new(scope, context);
        run_script(scope, source, "compiler_fault_injection.js")
    }

    #[test]
    fn deadline_terminates_noncooperative_compiler_and_next_request_recovers() {
        let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
        let entered = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_entered = entered.clone();
        let result = compiler.request(
            CompilerOperation::Program,
            Instant::now() + Duration::from_millis(200),
            move |compiler| {
                worker_entered.store(true, std::sync::atomic::Ordering::SeqCst);
                script(
                    compiler,
                    "globalThis.__coralCompile = null; while (true) {}",
                )
            },
        );
        assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            matches!(result, Err(Error::CompilationTimedOut)),
            "{result:?}"
        );
        compiler
            .compile(Instant::now() + COMPILATION_TIMEOUT, "return 42;")
            .expect("fresh compiler after termination");
    }

    #[test]
    fn expired_requests_never_execute_or_terminate_a_later_request() {
        let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
        let result = compiler.request::<()>(CompilerOperation::Program, Instant::now(), |_| {
            panic!("expired job executed")
        });
        assert!(matches!(result, Err(Error::CompilationTimedOut)));
        compiler
            .compile(Instant::now() + COMPILATION_TIMEOUT, "return 42;")
            .expect("live request after expired job");
    }

    #[test]
    fn heap_recycling_collects_garbage_but_detects_retained_growth() {
        // Exercise the real V8 collection policy with a smaller pressure budget,
        // rather than allocating hundreds of MiB under the admission deadline.
        const TEST_HEAP_BYTES: usize = 64 * 1024 * 1024;
        crate::v8_support::initialize().expect("V8 initialization");
        let mut compiler = CompilerIsolate::new(IsolateLimits::default()).expect("compiler");
        let elements = TEST_HEAP_BYTES / size_of::<f64>() + 1024 * 1024;
        script(&mut compiler, &format!(
            "{{ const temporary = new Array({elements}).fill(0.5); globalThis.__coralWarmProbe = temporary.length; }}"
        )).expect("allocate transient compiler garbage");
        assert!(compiler.isolate.get_heap_statistics().used_heap_size() >= TEST_HEAP_BYTES);
        assert!(
            !compiler.heap_requires_recycling(TEST_HEAP_BYTES),
            "garbage is collectable, not retained growth"
        );
        script(
            &mut compiler,
            "if (!globalThis.__coralWarmProbe) throw new Error('compiler state lost');",
        )
        .expect("collection preserves compiler state");

        script(
            &mut compiler,
            &format!("globalThis.__coralHeapProbe = new Array({elements}).fill(0.5);"),
        )
        .expect("retain compiler heap growth");
        assert!(
            compiler.heap_requires_recycling(TEST_HEAP_BYTES),
            "retained growth still requires recycling"
        );
    }

    #[test]
    fn compiler_recycles_after_failure() {
        let compiler = ProgramCompiler::new(IsolateLimits::default()).expect("compiler");
        let failed = compiler.request(
            CompilerOperation::Program,
            Instant::now() + COMPILATION_TIMEOUT,
            |compiler| {
                script(compiler, "globalThis.__coralCompile = null;")?;
                Err::<(), _>(Error::Engine("injected failure".to_string()))
            },
        );
        assert!(failed.is_err());
        compiler
            .compile(Instant::now() + COMPILATION_TIMEOUT, "return 42;")
            .expect("fresh compiler after failure");
    }

    #[test]
    fn compiler_terminates_cumulative_buffer_growth_and_recovers() {
        for allocate in [
            "return new Uint8Array(4 * 1024 * 1024);",
            "const buffer = new ArrayBuffer(0, { maxByteLength: 4 * 1024 * 1024 }); buffer.resize(4 * 1024 * 1024); return new Uint8Array(buffer);",
        ] {
            let limits = IsolateLimits::new(16 * 1024 * 1024, 64 * 1024 * 1024).expect("limits");
            let compiler = ProgramCompiler::new(limits).expect("compiler");
            compiler
                .request(
                    CompilerOperation::Program,
                    Instant::now() + COMPILATION_TIMEOUT,
                    move |compiler| {
                        script(
                            compiler,
                            &format!("globalThis.__coralAllocate = () => {{ {allocate} }};"),
                        )?;
                        script(
                            compiler,
                            r"
                    const compile = __coralCompile;
                    globalThis.__coralCompile = (...args) => {
                        const retained = [];
                        for (let i = 0; i < 40; i++) {
                            const buffer = __coralAllocate();
                            buffer.fill(1);
                            retained.push(buffer);
                        }
                        return compile(...args);
                    };
                ",
                        )
                    },
                )
                .expect("inject cumulative compiler buffer growth");
            let result = compiler.compile(Instant::now() + COMPILATION_TIMEOUT, "return 42;");
            assert!(
                matches!(result, Err(Error::MemoryLimitExceeded)),
                "{allocate}: {result:?}"
            );
            compiler
                .compile(
                    Instant::now() + COMPILATION_TIMEOUT,
                    "return Number(0x28n + 0b10n);",
                )
                .expect("compiler recovers with its typed-array intrinsics");
        }
    }
}
