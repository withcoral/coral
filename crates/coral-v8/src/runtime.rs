use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use crate::source_map::{authored_location, decode_authored};
use crate::v8_support::{evaluate_script, run_script};
use crate::{
    AuthoredLocation, AuthoredSource, CompiledProgram, ConsoleLevel, Error, ExecutionOutput,
    InvocationOrigin, IsolateLimits, MiddlewareExecutionOutcome, MiddlewareExecutionStart,
    MiddlewareScope, OperationFuture, OperationHostError, OperationPolicy, ProgramExecutionControl,
    ProgramHost, ProgramOutputHostError,
};

const FORBIDDEN_AMBIENT_GLOBALS: &str = include_str!("js/forbidden_ambient_globals.js");
const RUNTIME_BOOTSTRAP: &str = include_str!("js/runtime/runtime_bootstrap.js");
const PROGRAM_HOST_BOOTSTRAP: &str = include_str!("js/runtime/program_host_bootstrap.js");
const FINAL_VALUE_SERIALIZER: &str = include_str!("js/runtime/final_value.js");
const INVALID_FINAL_VALUE_MESSAGE: &str = "Final Value could not be serialized as JSON";
const INVALID_EMISSION_MESSAGE: &str = "Program Emission is not a valid Program Value";
const OUTPUT_LIMIT_MESSAGE: &str = "Program output exceeds its limit";
const HOST_ERROR_PRIVATE_KEY: &str = "withcoral.ProgramHost#errorKind";
// A host binding sits a few private and native frames above its authored caller.
const HOST_STACK_MAX_FRAMES: usize = 16;

/// Stateless executor that gives each Program a fresh V8 isolate.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProgramExecutor {
    limits: IsolateLimits,
}

impl ProgramExecutor {
    /// Builds an executor with deployment-selected per-isolate heap budgets.
    #[must_use]
    pub const fn new(isolate_limits: IsolateLimits) -> Self {
        Self {
            limits: isolate_limits,
        }
    }

    /// Executes emitted JavaScript in a new capability-restricted isolate.
    ///
    /// No compiler state or previous Program state enters the isolate. V8 code
    /// generation from strings is disabled before Program JavaScript starts.
    /// JavaScript `undefined` becomes an absent Final Value. Every present Final
    /// Value uses the captured native `JSON.stringify` semantics.
    ///
    /// # Errors
    ///
    /// Returns an engine error, Program rejection, unresolved-Promise error, or
    /// invalid Program Value error.
    pub fn execute(&self, program: &CompiledProgram) -> Result<ExecutionOutput, Error> {
        self.execute_inner(program, None, None)
    }

    /// Executes with the Provider Operation and Program output host installed.
    ///
    /// Operation callbacks return pending Promises and progress independently;
    /// no Promise settles before its Invocation evidence is durable. Emission
    /// and Console callbacks block until their output is durable.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::execute`], plus host-bootstrap
    /// initialization failures.
    pub fn execute_with_host(
        &self,
        program: &CompiledProgram,
        host: Arc<dyn ProgramHost>,
    ) -> Result<ExecutionOutput, Error> {
        self.execute_inner(program, Some(host), None)
    }

    /// Executes with a host and an opaque cross-thread termination control.
    ///
    /// A termination request is uncatchable by Program JavaScript. The control
    /// may be triggered before or during execution and remains safe after the
    /// fresh isolate has been destroyed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ProgramHostTerminated`] when termination is requested,
    /// or the same compiler/runtime and host failures as [`Self::execute_with_host`].
    pub fn execute_with_host_control(
        &self,
        program: &CompiledProgram,
        host: Arc<dyn ProgramHost>,
        control: &ProgramExecutionControl,
    ) -> Result<ExecutionOutput, Error> {
        self.execute_inner(program, Some(host), Some(control))
    }

    fn execute_inner(
        &self,
        program: &CompiledProgram,
        host: Option<Arc<dyn ProgramHost>>,
        control: Option<&ProgramExecutionControl>,
    ) -> Result<ExecutionOutput, Error> {
        crate::v8_support::initialize()?;
        let mut isolate = v8::Isolate::new(
            v8::CreateParams::default()
                .allow_atomics_wait(false)
                .heap_limits(0, self.limits.execution_heap_bytes()),
        );
        crate::memory::with_heap_limit(&mut isolate, |isolate| {
            Self::execute_in_isolate(isolate, program, host, control)
        })
    }

    fn execute_in_isolate(
        isolate: &mut v8::OwnedIsolate,
        program: &CompiledProgram,
        host: Option<Arc<dyn ProgramHost>>,
        control: Option<&ProgramExecutionControl>,
    ) -> Result<ExecutionOutput, Error> {
        let source_map = decode_authored(program.source_map())?;
        let isolate_handle = isolate.thread_safe_handle();
        isolate.set_capture_stack_trace_for_uncaught_exceptions(true, 32);
        isolate.set_microtasks_policy(v8::MicrotasksPolicy::Explicit);
        isolate.set_promise_reject_callback(track_host_promise_rejection);
        if let Some(host) = host {
            isolate.set_slot(ProgramHostSlot::new(host, source_map.clone()));
        }
        v8::scope!(let scope, isolate);
        let context = v8::Context::new(scope, v8::ContextOptions::default());
        let scope = &mut v8::ContextScope::new(scope, context);
        run_script(
            scope,
            FORBIDDEN_AMBIENT_GLOBALS,
            "coral_forbidden_ambient_globals.js",
        )?;
        run_script(scope, RUNTIME_BOOTSTRAP, "coral_runtime_bootstrap.js")?;
        if scope.get_slot::<ProgramHostSlot>().is_some() {
            install_program_host(scope, context)?;
        }
        crate::program_error::initialize(scope)?;
        let final_value_serializer = final_value_serializer(scope)?;
        context.set_allow_generation_from_strings(false);
        if let Some(control) = control {
            control.install(&isolate_handle);
        }
        let entry = evaluate_script(scope, program.javascript(), "program.js");
        if program_execution_terminated(scope, control) {
            return Err(Error::ProgramHostTerminated);
        }
        let entry = entry?;
        let entry = start_middleware_package(scope, entry);
        if program_execution_terminated(scope, control) {
            return Err(Error::ProgramHostTerminated);
        }
        let promise = v8::Local::<v8::Promise>::try_from(entry?).map_err(|_error| {
            Error::Engine("Program entry did not evaluate to a Promise".to_string())
        })?;

        drain_program_work(scope, control)?;

        let result = match promise.state() {
            v8::PromiseState::Pending => Err(Error::PendingProgram),
            v8::PromiseState::Rejected => {
                let reason = promise.result(scope);
                if let Some(error) = classify_output_exception(scope, reason) {
                    Err(output_execution_error(error))
                } else {
                    Err(Error::ProgramRejected(Box::new(
                        crate::program_error::from_exception(scope, reason, &source_map),
                    )))
                }
            }
            v8::PromiseState::Fulfilled => {
                let value = promise.result(scope);
                if value.is_undefined() {
                    Ok(ExecutionOutput::new(None))
                } else {
                    serialize_final_value(scope, final_value_serializer, value, &source_map)
                        .map(|json| ExecutionOutput::new(Some(json)))
                }
            }
        };

        // Native Final Value serialization can execute Program-owned `toJSON`
        // code after the entry checkpoint. Drain any continuations it created
        // before deciding whether a host-marked output rejection was handled.
        drain_program_work(scope, control)?;
        if let Some(error) = unhandled_output_error(scope) {
            Err(output_execution_error(error))
        } else if result.is_ok()
            && scope
                .get_slot::<ProgramHostSlot>()
                .is_some_and(|slot| slot.unhandled_operation_errors.get() > 0)
        {
            let reason = scope.get_slot::<ProgramHostSlot>().and_then(|slot| {
                slot.unhandled_operation_reasons
                    .borrow()
                    .values()
                    .next()
                    .cloned()
            });
            let mut error = reason.map_or_else(
                || crate::ProgramError::unavailable("Unhandled OperationError"),
                |reason| {
                    let reason = v8::Local::new(scope, reason);
                    crate::program_error::from_exception(scope, reason, &source_map)
                },
            );
            error.truncated |= scope
                .get_slot::<ProgramHostSlot>()
                .is_some_and(|slot| slot.unhandled_operation_errors.get() > 1);
            Err(Error::ProgramRejected(Box::new(error)))
        } else {
            result
        }
    }
}

struct ProgramHostSlot {
    host: Arc<dyn ProgramHost>,
    source_map: sourcemap::SourceMap,
    terminated: Cell<bool>,
    unhandled_invalid_outputs: Cell<usize>,
    unhandled_output_limits: Cell<usize>,
    unhandled_operation_errors: Cell<usize>,
    unhandled_operation_reasons: RefCell<HashMap<v8::Global<v8::Object>, v8::Global<v8::Value>>>,
    pending_operations: Vec<(v8::Global<v8::PromiseResolver>, OperationFuture, String)>,
    operation_error_factory: Option<v8::Global<v8::Function>>,
    middleware_runtime: Option<v8::Global<v8::Object>>,
}

impl ProgramHostSlot {
    fn new(host: Arc<dyn ProgramHost>, source_map: sourcemap::SourceMap) -> Self {
        Self {
            host,
            source_map,
            terminated: Cell::new(false),
            unhandled_invalid_outputs: Cell::new(0),
            unhandled_output_limits: Cell::new(0),
            unhandled_operation_errors: Cell::new(0),
            unhandled_operation_reasons: RefCell::new(HashMap::new()),
            pending_operations: Vec::new(),
            operation_error_factory: None,
            middleware_runtime: None,
        }
    }
}

// A Middleware package evaluates to a function that receives the private
// Middleware runtime. Ordinary packages evaluate directly to their Promise.
fn start_middleware_package<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    entry: v8::Local<'s, v8::Value>,
) -> Result<v8::Local<'s, v8::Value>, Error> {
    let Ok(function) = v8::Local::<v8::Function>::try_from(entry) else {
        return Ok(entry);
    };
    let runtime = scope
        .get_slot::<ProgramHostSlot>()
        .and_then(|slot| slot.middleware_runtime.clone())
        .ok_or_else(|| Error::Engine("Middleware package requires a Program host".to_string()))?;
    let runtime = v8::Local::new(scope, runtime);
    let receiver = v8::undefined(scope).into();
    function
        .call(scope, receiver, &[runtime.into()])
        .ok_or_else(|| Error::Engine("Middleware package did not start".to_string()))
}

#[derive(Clone, Copy)]
enum HostErrorKind {
    InvalidOutput = 1,
    OutputLimit = 2,
    Operation = 3,
}

struct ExecutionWake(std::thread::Thread);

impl Wake for ExecutionWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn drain_program_work(
    scope: &mut v8::PinScope<'_, '_>,
    control: Option<&ProgramExecutionControl>,
) -> Result<(), Error> {
    let waker = Waker::from(Arc::new(ExecutionWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        scope.perform_microtask_checkpoint();
        if program_execution_terminated(scope, control) {
            return Err(Error::ProgramHostTerminated);
        }
        let Some(slot) = scope.get_slot_mut::<ProgramHostSlot>() else {
            return Ok(());
        };
        if slot.pending_operations.is_empty() {
            return Ok(());
        }
        // ponytail: scan outstanding futures on wake; use FuturesUnordered if
        // measured large-fanout workloads make this scan material.
        let pending = std::mem::take(&mut slot.pending_operations);
        let mut completed = false;
        for (resolver, mut operation, call_stack) in pending {
            match operation.as_mut().poll(&mut context) {
                Poll::Pending => scope
                    .get_slot_mut::<ProgramHostSlot>()
                    .expect("host installed")
                    .pending_operations
                    .push((resolver, operation, call_stack)),
                Poll::Ready(result) => {
                    completed = true;
                    let resolver = v8::Local::new(scope, resolver);
                    match result {
                        Ok(json) => resolve_operation_promise(scope, resolver, &json),
                        Err(OperationHostError::Rejected {
                            code,
                            message,
                            trace_id,
                        }) => {
                            reject_coral_failure(
                                scope,
                                resolver,
                                code,
                                message,
                                trace_id.as_deref(),
                                &call_stack,
                            );
                        }
                        Err(OperationHostError::Fatal) => terminate_program_host(scope),
                    }
                    if program_execution_terminated(scope, control) {
                        return Err(Error::ProgramHostTerminated);
                    }
                }
            }
        }
        if !completed && !program_execution_terminated(scope, control) {
            // Both Future wakeups and cross-thread termination unpark this thread.
            std::thread::park();
        }
    }
}

fn program_execution_terminated(
    scope: &v8::PinScope<'_, '_>,
    control: Option<&ProgramExecutionControl>,
) -> bool {
    control.is_some_and(ProgramExecutionControl::is_requested)
        || scope.is_execution_terminating()
        || scope
            .get_slot::<ProgramHostSlot>()
            .is_some_and(|slot| slot.terminated.get())
}

fn unhandled_output_error(scope: &v8::PinScope<'_, '_>) -> Option<ProgramOutputHostError> {
    let slot = scope.get_slot::<ProgramHostSlot>()?;
    if slot.unhandled_invalid_outputs.get() > 0 {
        Some(ProgramOutputHostError::InvalidValue)
    } else if slot.unhandled_output_limits.get() > 0 {
        Some(ProgramOutputHostError::LimitExceeded)
    } else {
        None
    }
}

fn output_execution_error(error: ProgramOutputHostError) -> Error {
    match error {
        ProgramOutputHostError::InvalidValue => {
            Error::InvalidProgramValue(INVALID_EMISSION_MESSAGE.to_string())
        }
        ProgramOutputHostError::LimitExceeded => Error::ResourceLimitExceeded,
        ProgramOutputHostError::Fatal => Error::ProgramHostTerminated,
    }
}

// Track only privately marked host failures. Ordinary Program-created Promise
// rejections retain their existing entry-Promise semantics.
extern "C" fn track_host_promise_rejection(message: v8::PromiseRejectMessage) {
    v8::callback_scope!(unsafe scope, &message);
    let promise: v8::Local<v8::Object> = message.get_promise().into();
    let (kind, added) = match message.get_event() {
        v8::PromiseRejectEvent::PromiseRejectWithNoHandler => {
            let Some(kind) = message
                .get_value()
                .and_then(|value| v8::Local::<v8::Object>::try_from(value).ok())
                .and_then(|object| host_error_kind(scope, object))
            else {
                return;
            };
            if !mark_host_error(scope, promise, kind) {
                return;
            }
            (kind, true)
        }
        v8::PromiseRejectEvent::PromiseHandlerAddedAfterReject => {
            let Some(kind) = host_error_kind(scope, promise) else {
                return;
            };
            (kind, false)
        }
        _ => return,
    };
    if matches!(kind, HostErrorKind::Operation) {
        if added {
            if let Some(reason) = message.get_value() {
                let promise = v8::Global::new(scope, promise);
                let reason = v8::Global::new(scope, reason);
                if let Some(slot) = scope.get_slot::<ProgramHostSlot>() {
                    slot.unhandled_operation_reasons
                        .borrow_mut()
                        .insert(promise, reason);
                }
            }
        } else {
            let promise = v8::Global::new(scope, promise);
            if let Some(slot) = scope.get_slot::<ProgramHostSlot>() {
                slot.unhandled_operation_reasons
                    .borrow_mut()
                    .remove(&promise);
            }
        }
    }
    if let Some(counter) = scope.get_slot::<ProgramHostSlot>().map(|slot| match kind {
        HostErrorKind::InvalidOutput => &slot.unhandled_invalid_outputs,
        HostErrorKind::OutputLimit => &slot.unhandled_output_limits,
        HostErrorKind::Operation => &slot.unhandled_operation_errors,
    }) {
        counter.set(if added {
            counter.get() + 1
        } else {
            counter.get().saturating_sub(1)
        });
    }
}

fn install_program_host(
    scope: &mut v8::PinScope<'_, '_>,
    context: v8::Local<'_, v8::Context>,
) -> Result<(), Error> {
    for (name, function) in [
        (
            "__coralOperationInvoke",
            v8::Function::new(scope, operation_callback),
        ),
        (
            "__coralProgramEmit",
            v8::Function::new(scope, program_emission_callback),
        ),
        (
            "__coralProgramLog",
            v8::Function::new(scope, program_log_callback),
        ),
        (
            "__coralOperationPolicy",
            v8::Function::new(scope, operation_policy_callback),
        ),
        (
            "__coralCallLocation",
            v8::Function::new(scope, call_location_callback),
        ),
        (
            "__coralProgramEmitWithPolicy",
            v8::Function::new(scope, program_emission_policy_callback),
        ),
        (
            "__coralMiddlewareStarted",
            v8::Function::new(scope, middleware_started_callback),
        ),
        (
            "__coralMiddlewareContinued",
            v8::Function::new(scope, middleware_continued_callback),
        ),
        (
            "__coralMiddlewareEnded",
            v8::Function::new(scope, middleware_ended_callback),
        ),
        (
            "__coralFrameGet",
            v8::Function::new(scope, frame_get_callback),
        ),
        (
            "__coralFrameSet",
            v8::Function::new(scope, frame_set_callback),
        ),
    ] {
        let function = function
            .ok_or_else(|| Error::Engine("failed to allocate Program host function".to_string()))?;
        let key = v8::String::new(scope, name)
            .ok_or_else(|| Error::Engine("failed to allocate Program host key".to_string()))?;
        if context
            .global(scope)
            .set(scope, key.into(), function.into())
            != Some(true)
        {
            return Err(Error::Engine(
                "failed to install Program host function".to_string(),
            ));
        }
    }
    let run_id = scope
        .get_slot::<ProgramHostSlot>()
        .and_then(|slot| slot.host.run_id());
    if let Some(run_id) = run_id {
        let key = v8::String::new(scope, "__coralRunId")
            .ok_or_else(|| Error::Engine("failed to allocate Program host key".to_string()))?;
        let value = v8::String::new(scope, &run_id)
            .ok_or_else(|| Error::Engine("failed to allocate Run identity".to_string()))?;
        if context.global(scope).set(scope, key.into(), value.into()) != Some(true) {
            return Err(Error::Engine("failed to install Run identity".to_string()));
        }
    }
    let exports = evaluate_script(
        scope,
        PROGRAM_HOST_BOOTSTRAP,
        "coral_program_host_bootstrap.js",
    )?;
    let exports = v8::Local::<v8::Array>::try_from(exports)
        .map_err(|_error| Error::Engine("Program host bootstrap is invalid".to_string()))?;
    let factory = exports
        .get_index(scope, 0)
        .and_then(|value| v8::Local::<v8::Function>::try_from(value).ok())
        .ok_or_else(|| Error::Engine("OperationError factory is invalid".to_string()))?;
    let runtime = exports
        .get_index(scope, 1)
        .and_then(|value| v8::Local::<v8::Object>::try_from(value).ok())
        .ok_or_else(|| Error::Engine("Middleware runtime is invalid".to_string()))?;
    let factory = v8::Global::new(scope, factory);
    let runtime = v8::Global::new(scope, runtime);
    let slot = scope
        .get_slot_mut::<ProgramHostSlot>()
        .expect("host installed");
    slot.operation_error_factory = Some(factory);
    slot.middleware_runtime = Some(runtime);
    Ok(())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "V8's native Function callback Interface requires arguments by value"
)]
fn operation_callback(
    scope: &mut v8::PinScope<'_, '_>,
    arguments: v8::FunctionCallbackArguments<'_>,
    mut return_value: v8::ReturnValue,
) {
    let Some(path) = arguments.get(0).to_string(scope) else {
        reject_operation_promise(
            scope,
            &mut return_value,
            "operation_not_found",
            "Operation Call Path was not found",
        );
        return;
    };
    let path = path.to_rust_string_lossy(scope);
    let Ok(arguments_json) = v8::Local::<v8::String>::try_from(arguments.get(1)) else {
        reject_operation_promise(
            scope,
            &mut return_value,
            "invalid_input",
            "Operation arguments are invalid",
        );
        return;
    };
    let arguments_json = arguments_json.to_rust_string_lossy(scope);
    let origin = if let Ok(execution_id) = v8::Local::<v8::String>::try_from(arguments.get(3)) {
        InvocationOrigin::Middleware {
            execution_id: execution_id.to_rust_string_lossy(scope),
        }
    } else if arguments.get(2).is_undefined() {
        InvocationOrigin::Program { policy: None }
    } else {
        let policy = arguments
            .get(2)
            .to_string(scope)
            .and_then(|policy| OperationPolicy::parse(&policy.to_rust_string_lossy(scope)));
        let Some(policy) = policy else {
            reject_operation_promise(
                scope,
                &mut return_value,
                "invalid_input",
                "Middleware selected an invalid Operation policy",
            );
            return;
        };
        InvocationOrigin::Program {
            policy: Some(policy),
        }
    };
    let Some(host) = scope
        .get_slot::<ProgramHostSlot>()
        .map(|slot| slot.host.clone())
    else {
        terminate_program_host(scope);
        return;
    };
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        terminate_program_host(scope);
        return;
    };
    return_value.set(resolver.get_promise(scope).into());
    let resolver = v8::Global::new(scope, resolver);
    let frames = capture_program_frames(scope);
    let call_stack = operation_call_stack(&frames);
    // Middleware source is not the Run's authored source. An intercepted
    // Program call reaches here after its frame returned, so the private
    // runtime passes the call site it captured when the Program made the call.
    let location = match &origin {
        InvocationOrigin::Middleware { .. } => None,
        InvocationOrigin::Program { .. } if arguments.get(4).is_undefined() => {
            authored_location_of(scope, &frames)
        }
        InvocationOrigin::Program { .. } => passed_location(scope, arguments.get(4)),
    };
    if let Err(error) = host.value_limits().validate_json(&arguments_json) {
        let resolver = v8::Local::new(scope, resolver);
        let (code, message) = match error {
            coral_value::ValueError::Invalid => {
                ("invalid_input", "Operation arguments are invalid")
            }
            coral_value::ValueError::TooLarge => (
                "operation_input_too_large",
                "Operation input exceeds its limit",
            ),
        };
        reject_coral_failure(scope, resolver, code, message, None, &call_stack);
        return;
    }
    let operation = host.invoke_with_origin(path, arguments_json, origin, location);
    scope
        .get_slot_mut::<ProgramHostSlot>()
        .expect("host installed")
        .pending_operations
        .push((resolver, operation, call_stack));
}

/// Captures the private emitted script's frames, innermost first, while the
/// binding's caller is still on the stack and before any asynchronous host work.
///
/// Reads V8's native stack, so Program-controlled `Error.stackTraceLimit` and
/// `prepareStackTrace` cannot change it. Bootstrap and builtin frames are not
/// part of the emitted script and are skipped.
fn capture_program_frames(scope: &v8::PinScope<'_, '_>) -> Vec<(usize, usize)> {
    let Some(stack) = v8::StackTrace::current_stack_trace(scope, HOST_STACK_MAX_FRAMES) else {
        return Vec::new();
    };
    (0..stack.get_frame_count())
        .filter_map(|index| {
            let frame = stack.get_frame(scope, index)?;
            let name = frame.get_script_name(scope)?;
            (name.to_rust_string_lossy(scope) == "program.js")
                .then(|| (frame.get_line_number(), frame.get_column()))
        })
        .collect()
}

/// Maps the innermost emitted frame to its authored location. A private
/// wrapper position there, or no emitted frame at all, means no location
/// rather than a guess at an outer caller.
fn authored_location_of(
    scope: &v8::PinScope<'_, '_>,
    frames: &[(usize, usize)],
) -> Option<AuthoredLocation> {
    let &(line, column) = frames.first()?;
    let slot = scope.get_slot::<ProgramHostSlot>()?;
    authored_location(
        &slot.source_map,
        u32::try_from(line).ok()?,
        u32::try_from(column).ok()?,
    )
}

fn capture_authored_location(scope: &v8::PinScope<'_, '_>) -> Option<AuthoredLocation> {
    authored_location_of(scope, &capture_program_frames(scope))
}

/// Returns the calling authored frame to the private runtime as
/// `[line, column, savedFunctionPathOrNull]`, or `undefined` when there is none.
///
/// The runtime keeps it while Middleware runs and passes it back with the
/// intercepted call, so the call site does not depend on which frames are on
/// the stack when the wrapped call finally reaches the host.
fn call_location_callback(
    scope: &mut v8::PinScope<'_, '_>,
    _arguments: v8::FunctionCallbackArguments<'_>,
    mut return_value: v8::ReturnValue,
) {
    let Some(location) = capture_authored_location(scope) else {
        return;
    };
    let path: v8::Local<'_, v8::Value> = match &location.source {
        AuthoredSource::Program => v8::null(scope).into(),
        AuthoredSource::SavedFunction { path } => match v8::String::new(scope, path) {
            Some(path) => path.into(),
            None => return,
        },
    };
    let elements = [
        v8::Integer::new_from_unsigned(scope, location.line).into(),
        v8::Integer::new_from_unsigned(scope, location.column).into(),
        path,
    ];
    return_value.set(v8::Array::new_with_elements(scope, &elements).into());
}

/// Reads a call site that [`call_location_callback`] encoded. Anything else,
/// including the runtime's `null` for no location, means no location.
fn passed_location(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Option<AuthoredLocation> {
    let encoded = v8::Local::<v8::Array>::try_from(value).ok()?;
    if encoded.length() != 3 {
        return None;
    }
    let line = encoded.get_index(scope, 0)?.uint32_value(scope)?;
    let column = encoded.get_index(scope, 1)?.uint32_value(scope)?;
    let path = encoded.get_index(scope, 2)?;
    let source = if path.is_null() {
        AuthoredSource::Program
    } else {
        AuthoredSource::SavedFunction {
            path: v8::Local::<v8::String>::try_from(path)
                .ok()?
                .to_rust_string_lossy(scope),
        }
    };
    Some(AuthoredLocation {
        source,
        line,
        column,
    })
}

// Bounded emitted locations; existing source-map projection handles them.
fn operation_call_stack(frames: &[(usize, usize)]) -> String {
    use std::fmt::Write as _;
    frames
        .iter()
        .fold(String::new(), |mut stack, (line, column)| {
            let _infallible = writeln!(stack, "at program.js:{line}:{column}");
            stack
        })
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "V8's native Function callback Interface requires arguments by value"
)]
fn program_emission_callback(
    scope: &mut v8::PinScope<'_, '_>,
    arguments: v8::FunctionCallbackArguments<'_>,
    mut return_value: v8::ReturnValue,
) {
    let encoded = if (1..=3).contains(&arguments.length()) {
        v8::Local::<v8::String>::try_from(arguments.get(0)).ok()
    } else {
        None
    };
    // Only the private runtime passes a Middleware execution, for that
    // Middleware's own attributed Emission.
    let middleware_execution = v8::Local::<v8::String>::try_from(arguments.get(1))
        .ok()
        .map(|id| id.to_rust_string_lossy(scope));
    let Some(encoded) = encoded else {
        reject_output_promise(
            scope,
            &mut return_value,
            ProgramOutputHostError::InvalidValue,
        );
        return;
    };
    let encoded = encoded.to_rust_string_lossy(scope);
    // An intercepted emit reaches here after the Program's frame returned, so
    // the private runtime passes the call site it captured at the call.
    let location = if middleware_execution.is_some() {
        None
    } else if arguments.get(2).is_undefined() {
        capture_authored_location(scope)
    } else {
        passed_location(scope, arguments.get(2))
    };
    let Some(host) = scope
        .get_slot::<ProgramHostSlot>()
        .map(|slot| slot.host.clone())
    else {
        terminate_program_host(scope);
        return;
    };
    let validated = host
        .value_limits()
        .validate_json(&encoded)
        .map_err(|error| match error {
            coral_value::ValueError::Invalid => ProgramOutputHostError::InvalidValue,
            coral_value::ValueError::TooLarge => ProgramOutputHostError::LimitExceeded,
        });
    let emitted = validated.and_then(|_value| match &middleware_execution {
        None => host.emit(&encoded, location),
        Some(execution_id) => host.middleware_emit(&encoded, execution_id),
    });
    match emitted {
        Ok(()) => resolve_void_promise(scope, &mut return_value),
        Err(
            error @ (ProgramOutputHostError::InvalidValue | ProgramOutputHostError::LimitExceeded),
        ) => reject_output_promise(scope, &mut return_value, error),
        Err(ProgramOutputHostError::Fatal) => terminate_program_host(scope),
    }
}

/// The center of the Operation Middleware onion for one intercepted Program
/// Emission whose final policy is not plainly `enabled`. Called only by the
/// private runtime with `(encoded, policy, location)`.
///
/// Values the host would refuse fail exactly as an ordinary Emission does. A
/// `disabled` or `approval_required` policy goes to the host, and the Promise
/// settles only after the host's durable outcome.
#[expect(
    clippy::needless_pass_by_value,
    reason = "V8's native Function callback Interface requires arguments by value"
)]
fn program_emission_policy_callback(
    scope: &mut v8::PinScope<'_, '_>,
    arguments: v8::FunctionCallbackArguments<'_>,
    mut return_value: v8::ReturnValue,
) {
    let Ok(encoded) = v8::Local::<v8::String>::try_from(arguments.get(0)) else {
        reject_output_promise(
            scope,
            &mut return_value,
            ProgramOutputHostError::InvalidValue,
        );
        return;
    };
    let encoded = encoded.to_rust_string_lossy(scope);
    let policy = arguments
        .get(1)
        .to_string(scope)
        .and_then(|policy| OperationPolicy::parse(&policy.to_rust_string_lossy(scope)));
    let Some(policy) = policy else {
        reject_operation_promise(
            scope,
            &mut return_value,
            "invalid_input",
            "Middleware selected an invalid Operation policy",
        );
        return;
    };
    let location = passed_location(scope, arguments.get(2));
    let Some(host) = host_of(scope) else { return };
    if let Err(error) = host.value_limits().validate_json(&encoded) {
        let error = match error {
            coral_value::ValueError::Invalid => ProgramOutputHostError::InvalidValue,
            coral_value::ValueError::TooLarge => ProgramOutputHostError::LimitExceeded,
        };
        reject_output_promise(scope, &mut return_value, error);
        return;
    }
    if policy == OperationPolicy::Enabled {
        match host.emit(&encoded, location) {
            Ok(()) => resolve_void_promise(scope, &mut return_value),
            Err(
                error @ (ProgramOutputHostError::InvalidValue
                | ProgramOutputHostError::LimitExceeded),
            ) => reject_output_promise(scope, &mut return_value, error),
            Err(ProgramOutputHostError::Fatal) => terminate_program_host(scope),
        }
        return;
    }
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        terminate_program_host(scope);
        return;
    };
    return_value.set(resolver.get_promise(scope).into());
    let resolver = v8::Global::new(scope, resolver);
    let call_stack = operation_call_stack(&capture_program_frames(scope));
    let emission = host.emit_with_policy(encoded, policy, location);
    scope
        .get_slot_mut::<ProgramHostSlot>()
        .expect("host installed")
        .pending_operations
        .push((resolver, emission, call_stack));
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "V8's native Function callback Interface requires arguments by value"
)]
fn program_log_callback(
    scope: &mut v8::PinScope<'_, '_>,
    arguments: v8::FunctionCallbackArguments<'_>,
    _return_value: v8::ReturnValue,
) {
    let Some(level) = arguments.get(0).to_string(scope) else {
        throw_output_error(scope, ProgramOutputHostError::InvalidValue);
        return;
    };
    let Some(message) = arguments.get(1).to_string(scope) else {
        throw_output_error(scope, ProgramOutputHostError::InvalidValue);
        return;
    };
    let level = match level.to_rust_string_lossy(scope).as_str() {
        "debug" => ConsoleLevel::Debug,
        "info" => ConsoleLevel::Info,
        "warn" => ConsoleLevel::Warn,
        "error" => ConsoleLevel::Error,
        _ => {
            throw_output_error(scope, ProgramOutputHostError::InvalidValue);
            return;
        }
    };
    let message = message.to_rust_string_lossy(scope);
    let location = capture_authored_location(scope);
    let Some(host) = scope
        .get_slot::<ProgramHostSlot>()
        .map(|slot| slot.host.clone())
    else {
        terminate_program_host(scope);
        return;
    };
    match host.log(level, &message, location) {
        Ok(()) => {}
        Err(
            error @ (ProgramOutputHostError::InvalidValue | ProgramOutputHostError::LimitExceeded),
        ) => throw_output_error(scope, error),
        Err(ProgramOutputHostError::Fatal) => terminate_program_host(scope),
    }
}

fn resolve_void_promise(scope: &mut v8::PinScope<'_, '_>, return_value: &mut v8::ReturnValue) {
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        return;
    };
    let undefined = v8::undefined(scope).into();
    let _settled = resolver.resolve(scope, undefined);
    return_value.set(resolver.get_promise(scope).into());
}

fn reject_output_promise(
    scope: &mut v8::PinScope<'_, '_>,
    return_value: &mut v8::ReturnValue,
    error: ProgramOutputHostError,
) {
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        return;
    };
    reject_output_resolver(scope, resolver, error);
    return_value.set(resolver.get_promise(scope).into());
}

fn reject_output_resolver(
    scope: &mut v8::PinScope<'_, '_>,
    resolver: v8::Local<'_, v8::PromiseResolver>,
    error: ProgramOutputHostError,
) {
    let Some(exception) = output_exception(scope, error) else {
        return;
    };
    let _settled = resolver.reject(scope, exception);
}

fn throw_output_error(scope: &mut v8::PinScope<'_, '_>, error: ProgramOutputHostError) {
    if let Some(exception) = output_exception(scope, error) {
        let _thrown = scope.throw_exception(exception);
    }
}

fn output_exception<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    error: ProgramOutputHostError,
) -> Option<v8::Local<'s, v8::Value>> {
    let message = match error {
        ProgramOutputHostError::InvalidValue => INVALID_EMISSION_MESSAGE,
        ProgramOutputHostError::LimitExceeded => OUTPUT_LIMIT_MESSAGE,
        ProgramOutputHostError::Fatal => return None,
    };
    let message = v8::String::new(scope, message)?;
    let exception = match error {
        ProgramOutputHostError::InvalidValue => v8::Exception::type_error(scope, message),
        ProgramOutputHostError::LimitExceeded => v8::Exception::range_error(scope, message),
        ProgramOutputHostError::Fatal => return None,
    };
    let object = v8::Local::<v8::Object>::try_from(exception).ok()?;
    mark_output_object(scope, object, error).then_some(exception)
}

fn mark_output_object(
    scope: &mut v8::PinScope<'_, '_>,
    object: v8::Local<'_, v8::Object>,
    error: ProgramOutputHostError,
) -> bool {
    let kind = match error {
        ProgramOutputHostError::InvalidValue => HostErrorKind::InvalidOutput,
        ProgramOutputHostError::LimitExceeded => HostErrorKind::OutputLimit,
        ProgramOutputHostError::Fatal => return false,
    };
    mark_host_error(scope, object, kind)
}

fn classify_output_exception(
    scope: &mut v8::PinScope<'_, '_>,
    exception: v8::Local<'_, v8::Value>,
) -> Option<ProgramOutputHostError> {
    let object = v8::Local::<v8::Object>::try_from(exception).ok()?;
    classify_output_object(scope, object)
}

fn classify_output_object(
    scope: &mut v8::PinScope<'_, '_>,
    object: v8::Local<'_, v8::Object>,
) -> Option<ProgramOutputHostError> {
    match host_error_kind(scope, object)? {
        HostErrorKind::InvalidOutput => Some(ProgramOutputHostError::InvalidValue),
        HostErrorKind::OutputLimit => Some(ProgramOutputHostError::LimitExceeded),
        HostErrorKind::Operation => None,
    }
}

fn mark_host_error(
    scope: &mut v8::PinScope<'_, '_>,
    object: v8::Local<'_, v8::Object>,
    kind: HostErrorKind,
) -> bool {
    let Some(key) = host_error_private_key(scope) else {
        return false;
    };
    let marker = v8::Integer::new(scope, kind as i32);
    object.set_private(scope, key, marker.into()) == Some(true)
}

fn host_error_kind(
    scope: &mut v8::PinScope<'_, '_>,
    object: v8::Local<'_, v8::Object>,
) -> Option<HostErrorKind> {
    let key = host_error_private_key(scope)?;
    match object.get_private(scope, key)?.int32_value(scope)? {
        1 => Some(HostErrorKind::InvalidOutput),
        2 => Some(HostErrorKind::OutputLimit),
        3 => Some(HostErrorKind::Operation),
        _ => None,
    }
}

pub(crate) fn operation_error_fields(
    scope: &mut v8::PinScope<'_, '_>,
    object: v8::Local<'_, v8::Object>,
) -> Option<crate::ProgramOperationError> {
    if !matches!(
        host_error_kind(scope, object),
        Some(HostErrorKind::Operation)
    ) {
        return None;
    }
    let key = operation_fields_key(scope)?;
    let fields = object.get_private(scope, key)?;
    let fields = v8::Local::<v8::Array>::try_from(fields).ok()?;
    let code = fields.get_index(scope, 0)?;
    let trace = fields.get_index(scope, 1)?;
    Some(crate::ProgramOperationError {
        code: code.to_rust_string_lossy(scope),
        trace_id: (!trace.is_undefined()).then(|| trace.to_rust_string_lossy(scope)),
    })
}

fn operation_fields_key<'s>(scope: &v8::PinScope<'s, '_>) -> Option<v8::Local<'s, v8::Private>> {
    let name = v8::String::new(scope, "withcoral.ProgramHost#operationFields")?;
    Some(v8::Private::for_api(scope, Some(name)))
}

fn host_error_private_key<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Option<v8::Local<'s, v8::Private>> {
    let name = v8::String::new(scope, HOST_ERROR_PRIVATE_KEY)?;
    Some(v8::Private::for_api(scope, Some(name)))
}

fn terminate_program_host(scope: &mut v8::PinScope<'_, '_>) {
    if let Some(slot) = scope.get_slot::<ProgramHostSlot>() {
        slot.terminated.set(true);
    }
    let _terminated = scope.terminate_execution();
}

fn resolve_operation_promise(
    scope: &mut v8::PinScope<'_, '_>,
    resolver: v8::Local<'_, v8::PromiseResolver>,
    result_json: &str,
) {
    let limits = scope
        .get_slot::<ProgramHostSlot>()
        .map_or_else(coral_value::ValueLimits::default, |slot| {
            slot.host.value_limits()
        });
    if limits.validate_json(result_json).is_err() {
        terminate_program_host(scope);
        return;
    }
    let Some(encoded) = v8::String::new(scope, result_json) else {
        terminate_program_host(scope);
        return;
    };
    let Some(value) = v8::json::parse(scope, encoded) else {
        terminate_program_host(scope);
        return;
    };
    let _settled = resolver.resolve(scope, value);
}

fn reject_coral_failure(
    scope: &mut v8::PinScope<'_, '_>,
    resolver: v8::Local<'_, v8::PromiseResolver>,
    code: &str,
    message: &str,
    trace_id: Option<&str>,
    call_stack: &str,
) {
    let exception = (|| {
        let factory = scope
            .get_slot::<ProgramHostSlot>()?
            .operation_error_factory
            .clone()?;
        let factory = v8::Local::new(scope, factory);
        let code = v8::String::new(scope, code)?;
        let message_value = v8::String::new(scope, message)?;
        let trace: v8::Local<v8::Value> = match trace_id {
            Some(trace_id) => v8::String::new(scope, trace_id)?.into(),
            None => v8::undefined(scope).into(),
        };
        let stack = v8::String::new(scope, &format!("OperationError: {message}\n{call_stack}"))?;
        let receiver = v8::undefined(scope).into();
        let exception = factory.call(
            scope,
            receiver,
            &[code.into(), message_value.into(), trace, stack.into()],
        )?;
        let object = v8::Local::<v8::Object>::try_from(exception).ok()?;
        let key = operation_fields_key(scope)?;
        let fields = v8::Array::new_with_elements(scope, &[code.into(), trace]);
        if object.set_private(scope, key, fields.into()) != Some(true) {
            return None;
        }
        mark_host_error(scope, object, HostErrorKind::Operation).then_some(exception)
    })();
    if let Some(exception) = exception {
        let _settled = resolver.reject(scope, exception);
    } else {
        terminate_program_host(scope);
    }
}

fn reject_operation_promise(
    scope: &mut v8::PinScope<'_, '_>,
    return_value: &mut v8::ReturnValue,
    code: &str,
    message: &str,
) {
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        terminate_program_host(scope);
        return;
    };
    let call_stack = operation_call_stack(&capture_program_frames(scope));
    reject_coral_failure(scope, resolver, code, message, None, &call_stack);
    return_value.set(resolver.get_promise(scope).into());
}

fn final_value_serializer<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<v8::Local<'s, v8::Function>, Error> {
    let serializer = evaluate_script(
        scope,
        FINAL_VALUE_SERIALIZER,
        "coral_final_value_serializer.js",
    )?;
    v8::Local::<v8::Function>::try_from(serializer)
        .map_err(|_error| Error::Engine("Final Value serializer is invalid".to_string()))
}

fn serialize_final_value(
    scope: &mut v8::PinScope<'_, '_>,
    serializer: v8::Local<'_, v8::Function>,
    value: v8::Local<'_, v8::Value>,
    source_map: &sourcemap::SourceMap,
) -> Result<String, Error> {
    let try_catch = std::pin::pin!(v8::TryCatch::new(scope));
    let mut try_catch = try_catch.init();
    let receiver = v8::undefined(&try_catch).into();
    let Some(encoded) = serializer.call(&try_catch, receiver, &[value]) else {
        if let Some(exception) = try_catch.exception()
            && let Some(error) = classify_output_exception(&mut try_catch, exception)
        {
            return Err(output_execution_error(error));
        }
        return Err(try_catch
            .exception()
            .map_or_else(invalid_final_value, |exception| {
                Error::ProgramOutputRejected(Box::new(crate::program_error::from_exception(
                    &mut try_catch,
                    exception,
                    source_map,
                )))
            }));
    };
    if !encoded.is_string() {
        return Err(invalid_final_value());
    }
    let encoded =
        v8::Local::<v8::String>::try_from(encoded).map_err(|_error| invalid_final_value())?;
    let encoded = encoded.to_rust_string_lossy(&try_catch);
    let limits = try_catch
        .get_slot::<ProgramHostSlot>()
        .map_or_else(coral_value::ValueLimits::default, |slot| {
            slot.host.value_limits()
        });
    limits
        .validate_json(&encoded)
        .map_err(|error| match error {
            coral_value::ValueError::Invalid => invalid_final_value(),
            coral_value::ValueError::TooLarge => Error::ResourceLimitExceeded,
        })?;
    Ok(encoded)
}

fn invalid_final_value() -> Error {
    Error::InvalidProgramValue(INVALID_FINAL_VALUE_MESSAGE.to_string())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "V8's native Function callback Interface requires arguments by value"
)]
fn operation_policy_callback(
    scope: &mut v8::PinScope<'_, '_>,
    arguments: v8::FunctionCallbackArguments<'_>,
    mut return_value: v8::ReturnValue,
) {
    let Some(path) = arguments.get(0).to_string(scope) else {
        reject_operation_promise(
            scope,
            &mut return_value,
            "operation_not_found",
            "Operation Call Path was not found",
        );
        return;
    };
    let path = path.to_rust_string_lossy(scope);
    let Some(host) = scope
        .get_slot::<ProgramHostSlot>()
        .map(|slot| slot.host.clone())
    else {
        terminate_program_host(scope);
        return;
    };
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        terminate_program_host(scope);
        return;
    };
    return_value.set(resolver.get_promise(scope).into());
    let resolver = v8::Global::new(scope, resolver);
    let call_stack = operation_call_stack(&capture_program_frames(scope));
    let policy = host.operation_policy(path);
    scope
        .get_slot_mut::<ProgramHostSlot>()
        .expect("host installed")
        .pending_operations
        .push((resolver, policy, call_stack));
}

fn host_of(scope: &mut v8::PinScope<'_, '_>) -> Option<Arc<dyn ProgramHost>> {
    let host = scope
        .get_slot::<ProgramHostSlot>()
        .map(|slot| slot.host.clone());
    if host.is_none() {
        terminate_program_host(scope);
    }
    host
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "V8's native Function callback Interface requires arguments by value"
)]
fn middleware_started_callback(
    scope: &mut v8::PinScope<'_, '_>,
    arguments: v8::FunctionCallbackArguments<'_>,
    mut return_value: v8::ReturnValue,
) {
    let position = arguments
        .get(0)
        .uint32_value(scope)
        .and_then(|position| usize::try_from(position).ok());
    let middleware_scope = arguments
        .get(1)
        .to_string(scope)
        .and_then(|value| MiddlewareScope::parse(&value.to_rust_string_lossy(scope)));
    let target = arguments.get(2);
    let target = if target.is_undefined() {
        None
    } else {
        target
            .to_string(scope)
            .map(|target| target.to_rust_string_lossy(scope))
    };
    let (Some(position), Some(middleware_scope)) = (position, middleware_scope) else {
        terminate_program_host(scope);
        return;
    };
    let Some(host) = host_of(scope) else { return };
    let start = MiddlewareExecutionStart {
        position,
        scope: middleware_scope,
        target,
    };
    match host.middleware_started(&start) {
        Ok(id) => match v8::String::new(scope, &id) {
            Some(id) => return_value.set(id.into()),
            None => terminate_program_host(scope),
        },
        Err(_fatal) => terminate_program_host(scope),
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "V8's native Function callback Interface requires arguments by value"
)]
fn middleware_continued_callback(
    scope: &mut v8::PinScope<'_, '_>,
    arguments: v8::FunctionCallbackArguments<'_>,
    _return_value: v8::ReturnValue,
) {
    let Some(id) = arguments.get(0).to_string(scope) else {
        terminate_program_host(scope);
        return;
    };
    let id = id.to_rust_string_lossy(scope);
    let Some(host) = host_of(scope) else { return };
    if host.middleware_continued(&id).is_err() {
        terminate_program_host(scope);
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "V8's native Function callback Interface requires arguments by value"
)]
fn middleware_ended_callback(
    scope: &mut v8::PinScope<'_, '_>,
    arguments: v8::FunctionCallbackArguments<'_>,
    _return_value: v8::ReturnValue,
) {
    let Some(id) = arguments.get(0).to_string(scope) else {
        terminate_program_host(scope);
        return;
    };
    let id = id.to_rust_string_lossy(scope);
    let status = arguments
        .get(1)
        .to_string(scope)
        .map(|status| status.to_rust_string_lossy(scope));
    let value = v8::Local::new(scope, arguments.get(2));
    let outcome = match status.as_deref() {
        Some("succeeded") => MiddlewareExecutionOutcome::Succeeded {
            preview_json: crate::preview::value_preview(scope, value),
        },
        Some("failed") => MiddlewareExecutionOutcome::Failed {
            summary: crate::preview::error_summary(scope, value),
        },
        _ => {
            terminate_program_host(scope);
            return;
        }
    };
    let Some(host) = host_of(scope) else { return };
    if host.middleware_ended(&id, outcome).is_err() {
        terminate_program_host(scope);
    }
}

// Continuation-preserved embedder data follows each Promise reaction from the
// point where it was registered, which gives the trusted runtime one
// async-context frame per execution point. Programs never receive these natives.
fn frame_get_callback(
    scope: &mut v8::PinScope<'_, '_>,
    _arguments: v8::FunctionCallbackArguments<'_>,
    mut return_value: v8::ReturnValue,
) {
    let frame = scope.get_continuation_preserved_embedder_data();
    if frame.is_object() {
        return_value.set(frame);
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "V8's native Function callback Interface requires arguments by value"
)]
fn frame_set_callback(
    scope: &mut v8::PinScope<'_, '_>,
    arguments: v8::FunctionCallbackArguments<'_>,
    _return_value: v8::ReturnValue,
) {
    scope.set_continuation_preserved_embedder_data(arguments.get(0));
}
