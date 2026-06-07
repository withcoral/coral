use super::DEFAULT_MAX_PARALLEL_TOOL_CALLS_PER_CELL;
use super::EXIT_SENTINEL;
use super::MAX_NESTED_CALLS_PER_CELL;
use super::MAX_NESTED_TOOL_INPUT_BYTES;
use super::MAX_NESTED_TOOL_INPUT_BYTES_PER_CELL;
use super::MAX_OUTPUT_CONTENT_ITEMS_PER_CELL;
use super::MAX_OUTPUT_IMAGE_URL_BYTES_PER_CELL;
use super::MAX_OUTPUT_IMAGE_URL_BYTES_PER_ITEM;
use super::MAX_OUTPUT_TEXT_BYTES_PER_CELL;
use super::MAX_OUTPUT_TEXT_BYTES_PER_ITEM;
use super::MAX_STORED_VALUE_BYTES;
use super::MAX_STORED_VALUE_BYTES_PER_CELL;
use super::MAX_STORED_VALUE_KEY_BYTES;
use super::MAX_STORED_VALUES_PER_CELL;
use super::PendingToolCall;
use super::RuntimeEvent;
use super::RuntimeState;
use super::json_value_serialized_len;
use super::nested_tool_budget_exceeded_error;
use super::parallel_tool_budget_exceeded_error;
use super::timers;
use super::value::is_tagged_template_strings;
use super::value::json_to_v8;
use super::value::normalize_output_image;
use super::value::normalize_tool_input;
use super::value::throw_type_error;
use super::value::v8_value_to_json;
use crate::FunctionCallOutputContentItem;
use crate::description::EnabledToolMetadata;
use crate::input::normalize_nested_tool_input;
use serde_json::Value as JsonValue;

pub(super) fn console_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
) {
    let method = args.data().to_rust_string_lossy(scope);
    let original_text = console_output_text(scope, &method, args);
    let original_text_bytes = original_text.len();
    let mut text = original_text.clone();
    let per_item_dropped_bytes = truncate_utf8_bytes(&mut text, MAX_OUTPUT_TEXT_BYTES_PER_ITEM);
    let text_bytes = text.len();
    let Some(state) = scope.get_slot_mut::<RuntimeState>() else {
        throw_type_error(scope, "runtime state unavailable");
        return;
    };
    state.console_output_shaping.observe(original_text_bytes);
    let item_limit_exceeded = state.output_content_item_count >= MAX_OUTPUT_CONTENT_ITEMS_PER_CELL;
    let text_limit_exceeded =
        state.output_text_bytes.saturating_add(text_bytes) > MAX_OUTPUT_TEXT_BYTES_PER_CELL;
    if item_limit_exceeded || text_limit_exceeded {
        state.console_output_shaping.record_dropped_item(
            &original_text,
            original_text_bytes,
            item_limit_exceeded,
            text_limit_exceeded,
        );
        retval.set(v8::undefined(scope).into());
        return;
    }
    state
        .console_output_shaping
        .record_per_item_truncation(per_item_dropped_bytes);
    if per_item_dropped_bytes > 0 {
        let _ = state.console_output_shaping.ensure_spill_path();
    }
    state.output_content_item_count = state.output_content_item_count.saturating_add(1);
    state.output_text_bytes = state.output_text_bytes.saturating_add(text_bytes);
    state
        .console_output_shaping
        .record_emitted(&original_text, text_bytes);
    let event_tx = state.event_tx.clone();
    let _ = event_tx.send(RuntimeEvent::ContentItem(
        FunctionCallOutputContentItem::Text { text },
    ));
    retval.set(v8::undefined(scope).into());
}

fn console_output_text(
    scope: &mut v8::PinScope<'_, '_>,
    method: &str,
    args: v8::FunctionCallbackArguments,
) -> String {
    let mut parts = Vec::new();
    for index in 0..args.length() {
        parts.push(console_argument_text(scope, args.get(index)));
    }
    let text = parts.join(" ");
    if matches!(method, "log" | "info") {
        text
    } else {
        format!("[{method}] {text}")
    }
}

fn console_argument_text(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> String {
    if value.is_string() {
        return value.to_rust_string_lossy(scope);
    }
    match v8_value_to_json(scope, value) {
        Ok(Some(value)) => serde_json::to_string(&value).unwrap_or_else(|_| value.to_string()),
        Ok(None) | Err(_) => value.to_rust_string_lossy(scope),
    }
}

fn truncate_utf8_bytes(value: &mut String, max_bytes: usize) -> usize {
    if value.len() <= max_bytes {
        return 0;
    }
    let original_bytes = value.len();
    let suffix = " ...";
    let mut end = max_bytes.saturating_sub(suffix.len());
    while end > 0 && !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value.truncate(end);
    let trimmed_len = value.trim_end().len();
    value.truncate(trimmed_len);
    value.push_str(suffix);
    original_bytes.saturating_sub(value.len())
}

pub(super) fn tool_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    retval: v8::ReturnValue<v8::Value>,
) {
    let tool_index = match args.data().to_rust_string_lossy(scope).parse::<usize>() {
        Ok(tool_index) => tool_index,
        Err(_) => {
            throw_type_error(scope, "invalid tool callback data");
            return;
        }
    };

    let tool = {
        let Some(state) = scope.get_slot_mut::<RuntimeState>() else {
            throw_type_error(scope, "runtime state unavailable");
            return;
        };
        let Some(tool) = state.enabled_tools.get(tool_index) else {
            throw_type_error(scope, "tool callback data is out of range");
            return;
        };
        tool.clone()
    };

    invoke_tool(scope, args, retval, tool);
}

fn invoke_tool(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue<v8::Value>,
    tool: EnabledToolMetadata,
) {
    let tool_name = tool.tool_name.clone();
    let tool_kind = tool.kind;
    let tagged_template_call = match if args.length() == 0 {
        Ok(false)
    } else {
        is_tagged_template_strings(scope, args.get(0))
    } {
        Ok(tagged_template_call) => tagged_template_call,
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };
    let allow_error_result = match if args.length() < 2 || tagged_template_call {
        Ok(false)
    } else {
        allow_error_result_option(scope, args.get(1))
    } {
        Ok(allow_error_result) => allow_error_result,
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            return;
        }
    };
    let input = if args.length() == 0 {
        Ok(None)
    } else {
        normalize_tool_input(scope, args)
    };
    let input = match input
        .and_then(|input| normalize_nested_tool_input(&tool_name.name, tool_kind, input).map(Some))
    {
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
    let observed_parallel = state.pending_tool_calls.len().saturating_add(1);
    if observed_parallel > DEFAULT_MAX_PARALLEL_TOOL_CALLS_PER_CELL {
        let error_text = parallel_tool_budget_exceeded_error(
            "runtime",
            &tool.global_name,
            DEFAULT_MAX_PARALLEL_TOOL_CALLS_PER_CELL,
            observed_parallel,
        );
        state.fatal_error_text = Some(error_text.clone());
        throw_type_error(scope, &error_text);
        return;
    }
    let observed_total = state.nested_tool_call_count.saturating_add(1);
    if observed_total > MAX_NESTED_CALLS_PER_CELL {
        let error_text = nested_tool_budget_exceeded_error(
            "runtime",
            &tool.global_name,
            MAX_NESTED_CALLS_PER_CELL,
            observed_total,
        );
        state.fatal_error_text = Some(error_text.clone());
        throw_type_error(scope, &error_text);
        return;
    }
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
    state.nested_tool_call_count = observed_total;
    state.nested_tool_input_bytes = state.nested_tool_input_bytes.saturating_add(input_bytes);
    let event_tx = state.event_tx.clone();
    state.pending_tool_calls.insert(
        id.clone(),
        PendingToolCall {
            resolver,
            allow_error_result,
        },
    );
    let _ = event_tx.send(RuntimeEvent::ToolCall {
        id,
        name: tool_name,
        kind: tool_kind,
        input,
        allow_error_result,
    });
    retval.set(promise.into());
}

fn allow_error_result_option(
    scope: &mut v8::PinScope<'_, '_>,
    option: v8::Local<'_, v8::Value>,
) -> Result<bool, String> {
    let Some(value) = v8_value_to_json(scope, option)? else {
        return Ok(false);
    };
    let JsonValue::Object(options) = value else {
        return Err("tool options must be an object when provided".to_string());
    };
    for key in options.keys() {
        if key != "allowErrorResult" {
            return Err(format!("unsupported tool option `{key}`"));
        }
    }
    let Some(value) = options.get("allowErrorResult") else {
        return Ok(false);
    };
    value
        .as_bool()
        .ok_or_else(|| "tool option `allowErrorResult` must be a boolean".to_string())
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
        FunctionCallOutputContentItem::Text { .. } => 0,
        FunctionCallOutputContentItem::InputImage { image_url, .. } => image_url.len(),
        FunctionCallOutputContentItem::OutputShaping { .. } => 0,
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
