use serde_json::Value as JsonValue;

use super::CompletionState;
use super::EXIT_SENTINEL;
use super::RuntimeState;
use super::value::json_to_v8;
use super::value::structured_result_to_json;
use super::value::value_to_error_text;
use crate::input::CODE_MODE_RESULT_SLOT;

pub(super) fn evaluate_main_module(
    scope: &mut v8::PinScope<'_, '_>,
    source_text: &str,
) -> Result<Option<v8::Global<v8::Promise>>, String> {
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let source = v8::String::new(&tc, source_text)
        .ok_or_else(|| "failed to allocate exec source".to_string())?;
    let origin = script_origin(&mut tc, "exec_main.mjs")?;
    let mut source = v8::script_compiler::Source::new(source, Some(&origin));
    let module = v8::script_compiler::compile_module(&tc, &mut source).ok_or_else(|| {
        tc.exception()
            .map(|exception| value_to_error_text(&mut tc, exception))
            .unwrap_or_else(|| "unknown code mode exception".to_string())
    })?;
    module
        .instantiate_module(&tc, resolve_module_callback)
        .ok_or_else(|| {
            tc.exception()
                .map(|exception| value_to_error_text(&mut tc, exception))
                .unwrap_or_else(|| "unknown code mode exception".to_string())
        })?;
    let result = match module.evaluate(&tc) {
        Some(result) => result,
        None => {
            if let Some(exception) = tc.exception() {
                if is_exit_exception(&mut tc, exception) {
                    return Ok(None);
                }
                return Err(value_to_error_text(&mut tc, exception));
            }
            return Err("unknown code mode exception".to_string());
        }
    };
    tc.perform_microtask_checkpoint();

    if result.is_promise() {
        let promise = v8::Local::<v8::Promise>::try_from(result)
            .map_err(|_| "failed to read exec promise".to_string())?;
        return Ok(Some(v8::Global::new(&tc, promise)));
    }

    Ok(None)
}

pub(super) fn is_exit_exception(
    scope: &mut v8::PinScope<'_, '_>,
    exception: v8::Local<'_, v8::Value>,
) -> bool {
    scope
        .get_slot::<RuntimeState>()
        .map(|state| state.exit_requested)
        .unwrap_or(false)
        && exception.is_string()
        && exception.to_rust_string_lossy(scope) == EXIT_SENTINEL
}

pub(super) fn resolve_tool_response(
    scope: &mut v8::PinScope<'_, '_>,
    id: &str,
    response: Result<JsonValue, String>,
) -> Result<(), String> {
    let pending_call = {
        let state = scope
            .get_slot_mut::<RuntimeState>()
            .ok_or_else(|| "runtime state unavailable".to_string())?;
        state.pending_tool_calls.remove(id)
    }
    .ok_or_else(|| format!("unknown tool call `{id}`"))?;

    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let resolver = v8::Local::new(&tc, &pending_call.resolver);
    match response {
        Ok(result) => {
            if !pending_call.allow_error_result
                && let Some(error_text) = nested_error_result_text(&result)
            {
                mark_fatal_error(&mut tc, &error_text);
                let value = v8::String::new(&tc, &error_text)
                    .ok_or_else(|| "failed to allocate tool error".to_string())?;
                resolver.reject(&tc, value.into());
                if tc.has_caught() {
                    return Err(tc
                        .exception()
                        .map(|exception| value_to_error_text(&mut tc, exception))
                        .unwrap_or_else(|| "unknown code mode exception".to_string()));
                }
                return Ok(());
            }
            let value = json_to_v8(&mut tc, &result)
                .ok_or_else(|| "failed to serialize tool response".to_string())?;
            resolver.resolve(&tc, value);
        }
        Err(error_text) => {
            mark_fatal_error(&mut tc, &error_text);
            let value = v8::String::new(&tc, &error_text)
                .ok_or_else(|| "failed to allocate tool error".to_string())?;
            resolver.reject(&tc, value.into());
        }
    }
    if tc.has_caught() {
        return Err(tc
            .exception()
            .map(|exception| value_to_error_text(&mut tc, exception))
            .unwrap_or_else(|| "unknown code mode exception".to_string()));
    }
    Ok(())
}

fn mark_fatal_error(scope: &mut v8::PinScope<'_, '_>, error_text: &str) {
    if let Some(state) = scope.get_slot_mut::<RuntimeState>()
        && state.fatal_error_text.is_none()
    {
        state.fatal_error_text = Some(error_text.to_string());
    }
}

fn nested_error_result_text(result: &JsonValue) -> Option<String> {
    let object = result.as_object()?;
    if object.get("isError").and_then(JsonValue::as_bool) == Some(true) {
        return Some("nested tool returned isError=true".to_string());
    }
    if object.get("ok").and_then(JsonValue::as_bool) != Some(false) {
        return None;
    }
    if !is_coral_error_result(object) {
        return None;
    }
    let message = object
        .get("error")
        .and_then(JsonValue::as_object)
        .and_then(|error| error.get("message"))
        .and_then(JsonValue::as_str)
        .filter(|message| !message.trim().is_empty())
        .unwrap_or("nested tool returned ok=false");
    Some(format!("nested tool returned ok=false: {message}"))
}

fn is_coral_error_result(object: &serde_json::Map<String, JsonValue>) -> bool {
    if !object.get("error").is_some_and(JsonValue::is_object) {
        return false;
    }
    (object.contains_key("value") && object.contains_key("envelope"))
        || (object.contains_key("complete")
            && object.contains_key("partial")
            && object.contains_key("source_status"))
}

pub(super) fn completion_state(
    scope: &mut v8::PinScope<'_, '_>,
    pending_promise: Option<&v8::Global<v8::Promise>>,
    max_output_tokens: usize,
) -> CompletionState {
    let (
        stored_values,
        stored_value_updates,
        exit_requested,
        fatal_error_text,
        has_pending_tool_calls,
    ) = scope
        .get_slot::<RuntimeState>()
        .map(|state| {
            (
                state.stored_values.clone(),
                state.stored_value_updates.clone(),
                state.exit_requested,
                state.fatal_error_text.clone(),
                !state.pending_tool_calls.is_empty(),
            )
        })
        .unwrap_or_default();
    if let Some(error_text) = fatal_error_text {
        return CompletionState::Completed {
            stored_values,
            stored_value_updates,
            result: None,
            error_text: Some(error_text),
        };
    }
    if exit_requested {
        return CompletionState::Completed {
            stored_values,
            stored_value_updates,
            result: None,
            error_text: None,
        };
    }

    let Some(pending_promise) = pending_promise else {
        if has_pending_tool_calls {
            return CompletionState::Pending;
        }
        return match read_result_slot(scope, max_output_tokens) {
            Ok(result) => CompletionState::Completed {
                stored_values,
                stored_value_updates,
                result,
                error_text: None,
            },
            Err(error_text) => CompletionState::Completed {
                stored_values,
                stored_value_updates,
                result: None,
                error_text: Some(error_text),
            },
        };
    };

    let promise = v8::Local::new(scope, pending_promise);
    match promise.state() {
        v8::PromiseState::Pending => CompletionState::Pending,
        v8::PromiseState::Fulfilled if has_pending_tool_calls => CompletionState::Pending,
        v8::PromiseState::Fulfilled => match read_result_slot(scope, max_output_tokens) {
            Ok(result) => CompletionState::Completed {
                stored_values,
                stored_value_updates,
                result,
                error_text: None,
            },
            Err(error_text) => CompletionState::Completed {
                stored_values,
                stored_value_updates,
                result: None,
                error_text: Some(error_text),
            },
        },
        v8::PromiseState::Rejected => {
            let result = promise.result(scope);
            let error_text = if is_exit_exception(scope, result) {
                None
            } else {
                Some(value_to_error_text(scope, result))
            };
            CompletionState::Completed {
                stored_values,
                stored_value_updates,
                result: None,
                error_text,
            }
        }
    }
}

fn read_result_slot(
    scope: &mut v8::PinScope<'_, '_>,
    max_output_tokens: usize,
) -> Result<Option<JsonValue>, String> {
    let global = scope.get_current_context().global(scope);
    let Some(key) = v8::String::new(scope, CODE_MODE_RESULT_SLOT) else {
        return Err("failed to allocate code mode result slot key".to_string());
    };
    let Some(value) = global.get(scope, key.into()) else {
        return Err("failed to read code mode result slot".to_string());
    };
    structured_result_to_json(scope, value, max_output_tokens)
}

fn script_origin<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    resource_name_: &str,
) -> Result<v8::ScriptOrigin<'s>, String> {
    let resource_name = v8::String::new(scope, resource_name_)
        .ok_or_else(|| "failed to allocate script origin".to_string())?;
    let source_map_url = v8::String::new(scope, resource_name_)
        .ok_or_else(|| "failed to allocate source map url".to_string())?;
    Ok(v8::ScriptOrigin::new(
        scope,
        resource_name.into(),
        0,
        0,
        true,
        0,
        Some(source_map_url.into()),
        true,
        false,
        true,
        None,
    ))
}

fn resolve_module_callback<'s>(
    context: v8::Local<'s, v8::Context>,
    specifier: v8::Local<'s, v8::String>,
    _import_attributes: v8::Local<'s, v8::FixedArray>,
    _referrer: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Module>> {
    v8::callback_scope!(unsafe scope, context);
    let specifier = specifier.to_rust_string_lossy(scope);
    resolve_module(scope, &specifier)
}

pub(super) fn dynamic_import_callback<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _host_defined_options: v8::Local<'s, v8::Data>,
    _resource_name: v8::Local<'s, v8::Value>,
    specifier: v8::Local<'s, v8::String>,
    _import_attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
    let specifier = specifier.to_rust_string_lossy(scope);
    let resolver = v8::PromiseResolver::new(scope)?;

    match resolve_module(scope, &specifier) {
        Some(module) => {
            if module.get_status() == v8::ModuleStatus::Uninstantiated
                && module
                    .instantiate_module(scope, resolve_module_callback)
                    .is_none()
            {
                let error = v8::String::new(scope, "failed to instantiate module")
                    .map(Into::into)
                    .unwrap_or_else(|| v8::undefined(scope).into());
                resolver.reject(scope, error);
                return Some(resolver.get_promise(scope));
            }
            if matches!(
                module.get_status(),
                v8::ModuleStatus::Instantiated | v8::ModuleStatus::Evaluated
            ) && module.evaluate(scope).is_none()
            {
                let error = v8::String::new(scope, "failed to evaluate module")
                    .map(Into::into)
                    .unwrap_or_else(|| v8::undefined(scope).into());
                resolver.reject(scope, error);
                return Some(resolver.get_promise(scope));
            }
            let namespace = module.get_module_namespace();
            resolver.resolve(scope, namespace);
            Some(resolver.get_promise(scope))
        }
        None => {
            let error = v8::String::new(scope, "unsupported import in exec")
                .map(Into::into)
                .unwrap_or_else(|| v8::undefined(scope).into());
            resolver.reject(scope, error);
            Some(resolver.get_promise(scope))
        }
    }
}

fn resolve_module<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    specifier: &str,
) -> Option<v8::Local<'s, v8::Module>> {
    if let Some(message) =
        v8::String::new(scope, &format!("Unsupported import in exec: {specifier}"))
    {
        scope.throw_exception(message.into());
    } else {
        scope.throw_exception(v8::undefined(scope).into());
    }
    None
}
