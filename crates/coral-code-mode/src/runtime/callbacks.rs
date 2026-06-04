use super::EXIT_SENTINEL;
use super::MAX_NESTED_CALLS_PER_CELL;
use super::MAX_NESTED_TOOL_INPUT_BYTES;
use super::MAX_NESTED_TOOL_INPUT_BYTES_PER_CELL;
use super::MAX_OUTPUT_CONTENT_ITEMS_PER_CELL;
use super::MAX_OUTPUT_IMAGE_URL_BYTES_PER_CELL;
use super::MAX_OUTPUT_IMAGE_URL_BYTES_PER_ITEM;
use super::MAX_STORED_VALUE_BYTES;
use super::MAX_STORED_VALUE_BYTES_PER_CELL;
use super::MAX_STORED_VALUE_KEY_BYTES;
use super::MAX_STORED_VALUES_PER_CELL;
use super::RuntimeEvent;
use super::RuntimeState;
use super::json_value_serialized_len;
use super::timers;
use super::value::json_to_v8;
use super::value::normalize_output_image;
use super::value::normalize_tool_input;
use super::value::throw_type_error;
use super::value::v8_value_to_json;
use crate::FunctionCallOutputContentItem;

pub(super) fn tool_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let tool_index = match args.data().to_rust_string_lossy(scope).parse::<usize>() {
        Ok(tool_index) => tool_index,
        Err(_) => {
            throw_type_error(scope, "invalid tool callback data");
            return;
        }
    };

    let (tool_name, tool_kind) = {
        let Some(state) = scope.get_slot_mut::<RuntimeState>() else {
            throw_type_error(scope, "runtime state unavailable");
            return;
        };
        let Some(tool) = state.enabled_tools.get(tool_index) else {
            throw_type_error(scope, "tool callback data is out of range");
            return;
        };
        if state.nested_tool_call_count >= MAX_NESTED_CALLS_PER_CELL {
            throw_type_error(
                scope,
                &format!(
                    "code mode cell exceeded the nested call limit of {MAX_NESTED_CALLS_PER_CELL}"
                ),
            );
            return;
        }
        state.nested_tool_call_count = state.nested_tool_call_count.saturating_add(1);
        (tool.tool_name.clone(), tool.kind)
    };
    let input = if args.length() == 0 {
        Ok(None)
    } else {
        normalize_tool_input(scope, args)
    };
    let input = match input {
        Ok(input) => input,
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };
    let input_bytes = input.as_ref().map(json_value_serialized_len).unwrap_or(0);

    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        throw_type_error(scope, "failed to create tool promise");
        return;
    };
    let promise = resolver.get_promise(scope);
    let resolver = v8::Global::new(scope, resolver);

    let Some(state) = scope.get_slot_mut::<RuntimeState>() else {
        throw_type_error(scope, "runtime state unavailable");
        return;
    };
    if input_bytes > MAX_NESTED_TOOL_INPUT_BYTES {
        throw_type_error(
            scope,
            &format!(
                "nested tool input exceeded the per-call size limit of {MAX_NESTED_TOOL_INPUT_BYTES} bytes"
            ),
        );
        return;
    }
    if state.nested_tool_input_bytes.saturating_add(input_bytes)
        > MAX_NESTED_TOOL_INPUT_BYTES_PER_CELL
    {
        throw_type_error(
            scope,
            &format!(
                "nested tool input exceeded the total size limit of {MAX_NESTED_TOOL_INPUT_BYTES_PER_CELL} bytes"
            ),
        );
        return;
    }
    let id = format!("tool-{}", state.next_tool_call_id);
    state.next_tool_call_id = state.next_tool_call_id.saturating_add(1);
    state.nested_tool_input_bytes = state.nested_tool_input_bytes.saturating_add(input_bytes);
    let event_tx = state.event_tx.clone();
    state.pending_tool_calls.insert(id.clone(), resolver);
    let _ = event_tx.send(RuntimeEvent::ToolCall {
        id,
        name: tool_name,
        kind: tool_kind,
        input,
    });
    retval.set(promise.into());
}

pub(super) fn image_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let value = if args.length() == 0 {
        v8::undefined(scope).into()
    } else {
        args.get(0)
    };
    let detail_override = if args.length() < 2 {
        None
    } else {
        let detail = args.get(1);
        if detail.is_string() {
            Some(detail.to_rust_string_lossy(scope))
        } else if detail.is_null() || detail.is_undefined() {
            None
        } else {
            throw_type_error(scope, "image detail must be a string when provided");
            return;
        }
    };
    let image_item = match normalize_output_image(scope, value, detail_override) {
        Ok(image_item) => image_item,
        Err(()) => return,
    };
    let image_url_bytes = content_item_image_url_bytes(&image_item);
    let Some(state) = scope.get_slot_mut::<RuntimeState>() else {
        throw_type_error(scope, "runtime state unavailable");
        return;
    };
    if state.output_content_item_count >= MAX_OUTPUT_CONTENT_ITEMS_PER_CELL {
        throw_type_error(
            scope,
            &format!(
                "image output exceeded the content item limit of {MAX_OUTPUT_CONTENT_ITEMS_PER_CELL}"
            ),
        );
        return;
    }
    if image_url_bytes > MAX_OUTPUT_IMAGE_URL_BYTES_PER_ITEM {
        throw_type_error(
            scope,
            &format!(
                "image URL exceeded the per-item size limit of {MAX_OUTPUT_IMAGE_URL_BYTES_PER_ITEM} bytes"
            ),
        );
        return;
    }
    if state.output_image_url_bytes.saturating_add(image_url_bytes)
        > MAX_OUTPUT_IMAGE_URL_BYTES_PER_CELL
    {
        throw_type_error(
            scope,
            &format!(
                "image output exceeded the total URL size limit of {MAX_OUTPUT_IMAGE_URL_BYTES_PER_CELL} bytes"
            ),
        );
        return;
    }
    state.output_content_item_count = state.output_content_item_count.saturating_add(1);
    state.output_image_url_bytes = state.output_image_url_bytes.saturating_add(image_url_bytes);
    let event_tx = state.event_tx.clone();
    let _ = event_tx.send(RuntimeEvent::ContentItem(image_item));
    retval.set(v8::undefined(scope).into());
}

fn content_item_image_url_bytes(item: &FunctionCallOutputContentItem) -> usize {
    match item {
        FunctionCallOutputContentItem::InputImage { image_url, .. } => image_url.len(),
    }
}

pub(super) fn store_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    _retval: v8::ReturnValue<v8::Value>,
) {
    let key = args.get(0);
    if !key.is_string() {
        throw_type_error(scope, "store key must be a string");
        return;
    }
    let key = key.to_rust_string_lossy(scope);
    let value = args.get(1);
    let serialized = match v8_value_to_json(scope, value) {
        Ok(Some(value)) => value,
        Ok(None) => {
            throw_type_error(
                scope,
                &format!("Unable to store {key:?}. Only plain serializable objects can be stored."),
            );
            return;
        }
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };
    let error_text = {
        let Some(state) = scope.get_slot_mut::<RuntimeState>() else {
            throw_type_error(scope, "runtime state unavailable");
            return;
        };
        match validate_store_update(state, &key, &serialized) {
            Ok(StoreUpdateSize {
                old_value_bytes,
                new_value_bytes,
            }) => {
                state.stored_value_bytes = state
                    .stored_value_bytes
                    .saturating_sub(old_value_bytes)
                    .saturating_add(new_value_bytes);
                state.stored_values.insert(key.clone(), serialized.clone());
                state.stored_value_updates.insert(key, serialized);
                None
            }
            Err(error_text) => Some(error_text),
        }
    };
    if let Some(error_text) = error_text {
        throw_type_error(scope, &error_text);
    }
}

struct StoreUpdateSize {
    old_value_bytes: usize,
    new_value_bytes: usize,
}

fn validate_store_update(
    state: &RuntimeState,
    key: &str,
    value: &serde_json::Value,
) -> Result<StoreUpdateSize, String> {
    if key.len() > MAX_STORED_VALUE_KEY_BYTES {
        return Err(format!(
            "store key exceeded the size limit of {MAX_STORED_VALUE_KEY_BYTES} bytes"
        ));
    }
    if !state.stored_values.contains_key(key)
        && state.stored_values.len() >= MAX_STORED_VALUES_PER_CELL
    {
        return Err(format!(
            "store exceeded the value count limit of {MAX_STORED_VALUES_PER_CELL}"
        ));
    }
    let new_value_bytes = json_value_serialized_len(value);
    if new_value_bytes > MAX_STORED_VALUE_BYTES {
        return Err(format!(
            "stored value exceeded the per-value size limit of {MAX_STORED_VALUE_BYTES} bytes"
        ));
    }
    let old_value_bytes = state
        .stored_values
        .get(key)
        .map(json_value_serialized_len)
        .unwrap_or(0);
    let next_total = state
        .stored_value_bytes
        .saturating_sub(old_value_bytes)
        .saturating_add(new_value_bytes);
    if next_total > MAX_STORED_VALUE_BYTES_PER_CELL {
        return Err(format!(
            "store exceeded the total size limit of {MAX_STORED_VALUE_BYTES_PER_CELL} bytes"
        ));
    }
    Ok(StoreUpdateSize {
        old_value_bytes,
        new_value_bytes,
    })
}

pub(super) fn load_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let key = args.get(0);
    if !key.is_string() {
        throw_type_error(scope, "load key must be a string");
        return;
    }
    let key = key.to_rust_string_lossy(scope);
    let value = scope
        .get_slot::<RuntimeState>()
        .and_then(|state| state.stored_values.get(&key))
        .cloned();
    let Some(value) = value else {
        retval.set(v8::undefined(scope).into());
        return;
    };
    let Some(value) = json_to_v8(scope, &value) else {
        throw_type_error(scope, "failed to load stored value");
        return;
    };
    retval.set(value);
}

pub(super) fn set_timeout_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let timeout_id = match timers::schedule_timeout(scope, args) {
        Ok(timeout_id) => timeout_id,
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };

    retval.set(v8::Number::new(scope, timeout_id as f64).into());
}

pub(super) fn clear_timeout_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    if let Err(error_text) = timers::clear_timeout(scope, args) {
        throw_type_error(scope, &error_text);
        return;
    }

    retval.set(v8::undefined(scope).into());
}

pub(super) fn yield_control_callback(
    scope: &mut v8::PinScope<'_, '_>,
    _args: v8::FunctionCallbackArguments,
    _retval: v8::ReturnValue<v8::Value>,
) {
    if let Some(state) = scope.get_slot::<RuntimeState>() {
        let _ = state.event_tx.send(RuntimeEvent::YieldRequested);
    }
}

pub(super) fn exit_callback(
    scope: &mut v8::PinScope<'_, '_>,
    _args: v8::FunctionCallbackArguments,
    _retval: v8::ReturnValue<v8::Value>,
) {
    if let Some(state) = scope.get_slot_mut::<RuntimeState>() {
        state.exit_requested = true;
    }
    if let Some(error) = v8::String::new(scope, EXIT_SENTINEL) {
        scope.throw_exception(error.into());
    }
}
