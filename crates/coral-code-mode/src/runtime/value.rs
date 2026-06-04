use serde_json::Value as JsonValue;

use crate::input::CODE_MODE_TAGGED_TEMPLATE_KEY;
use crate::response::DEFAULT_IMAGE_DETAIL;
use crate::response::FunctionCallOutputContentItem;
use crate::response::ImageDetail;

const IMAGE_HELPER_EXPECTS_MESSAGE: &str = "image expects a non-empty image URL string, an object with image_url and optional detail, or a raw MCP image block";
const CORAL_IMAGE_DETAIL_META_KEY: &str = "coral/imageDetail";
const MAX_STRUCTURED_RESULT_DEPTH: usize = 64;
const MAX_STRUCTURED_RESULT_CONTAINER_ENTRIES: usize = 100_000;

pub(super) fn normalize_output_image(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
    detail_override: Option<String>,
) -> Result<FunctionCallOutputContentItem, ()> {
    let result = (|| -> Result<FunctionCallOutputContentItem, String> {
        let (image_url, detail) = if value.is_string() {
            (value.to_rust_string_lossy(scope), None)
        } else if value.is_object() && !value.is_array() {
            let object = v8::Local::<v8::Object>::try_from(value)
                .map_err(|_| IMAGE_HELPER_EXPECTS_MESSAGE.to_string())?;
            if let Some(image) = parse_non_mcp_output_image(scope, object)? {
                image
            } else {
                parse_mcp_output_image(scope, value)?
            }
        } else {
            return Err(IMAGE_HELPER_EXPECTS_MESSAGE.to_string());
        };

        if image_url.is_empty() {
            return Err(IMAGE_HELPER_EXPECTS_MESSAGE.to_string());
        }
        let lower = image_url.to_ascii_lowercase();
        if !(lower.starts_with("http://")
            || lower.starts_with("https://")
            || lower.starts_with("data:"))
        {
            return Err("image expects an http(s) or data URL".to_string());
        }

        let detail = detail_override.or(detail);
        let detail = match detail {
            Some(detail) => {
                let normalized = detail.to_ascii_lowercase();
                Some(match normalized.as_str() {
                    "high" => ImageDetail::High,
                    "original" => ImageDetail::Original,
                    _ => {
                        return Err("image detail must be one of: high, original".to_string());
                    }
                })
            }
            None => Some(DEFAULT_IMAGE_DETAIL),
        };

        Ok(FunctionCallOutputContentItem::InputImage { image_url, detail })
    })();

    match result {
        Ok(item) => Ok(item),
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            Err(())
        }
    }
}

fn parse_non_mcp_output_image(
    scope: &mut v8::PinScope<'_, '_>,
    object: v8::Local<'_, v8::Object>,
) -> Result<Option<(String, Option<String>)>, String> {
    let image_url_key = v8::String::new(scope, "image_url")
        .ok_or_else(|| "failed to allocate image helper keys".to_string())?;
    let Some(image_url) = object.get(scope, image_url_key.into()) else {
        return Ok(None);
    };
    if image_url.is_undefined() {
        return Ok(None);
    }
    if !image_url.is_string() {
        return Err(IMAGE_HELPER_EXPECTS_MESSAGE.to_string());
    }
    let detail_key = v8::String::new(scope, "detail")
        .ok_or_else(|| "failed to allocate image helper keys".to_string())?;
    let detail = parse_image_detail_value(scope, object.get(scope, detail_key.into()))?;
    Ok(Some((image_url.to_rust_string_lossy(scope), detail)))
}

fn parse_mcp_output_image(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Result<(String, Option<String>), String> {
    let Some(result) = v8_value_to_json(scope, value)? else {
        return Err(IMAGE_HELPER_EXPECTS_MESSAGE.to_string());
    };
    let JsonValue::Object(result) = result else {
        return Err(IMAGE_HELPER_EXPECTS_MESSAGE.to_string());
    };
    let Some(item_type) = result.get("type").and_then(JsonValue::as_str) else {
        return Err(IMAGE_HELPER_EXPECTS_MESSAGE.to_string());
    };
    if item_type != "image" {
        return Err(format!(
            "image only accepts MCP image blocks, got \"{item_type}\""
        ));
    }
    let data = result
        .get("data")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "image expected MCP image data".to_string())?;
    if data.is_empty() {
        return Err("image expected MCP image data".to_string());
    }

    let image_url = if data.to_ascii_lowercase().starts_with("data:") {
        data.to_string()
    } else {
        let mime_type = result
            .get("mimeType")
            .or_else(|| result.get("mime_type"))
            .and_then(JsonValue::as_str)
            .filter(|mime_type| !mime_type.is_empty())
            .unwrap_or("application/octet-stream");
        format!("data:{mime_type};base64,{data}")
    };
    let detail = result
        .get("_meta")
        .and_then(JsonValue::as_object)
        .and_then(|meta| meta.get(CORAL_IMAGE_DETAIL_META_KEY))
        .and_then(JsonValue::as_str)
        .filter(|detail| matches!(*detail, "high" | "original"))
        .map(str::to_string);
    Ok((image_url, detail))
}

fn parse_image_detail_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: Option<v8::Local<'s, v8::Value>>,
) -> Result<Option<String>, String> {
    match value {
        Some(value) if value.is_string() => Ok(Some(value.to_rust_string_lossy(scope))),
        Some(value) if value.is_null() || value.is_undefined() => Ok(None),
        Some(_) => Err("image detail must be a string when provided".to_string()),
        None => Ok(None),
    }
}

pub(super) fn v8_value_to_json(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Result<Option<JsonValue>, String> {
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let Some(stringified) = v8::json::stringify(&tc, value) else {
        if tc.has_caught() {
            return Err(tc
                .exception()
                .map(|exception| value_to_error_text(&mut tc, exception))
                .unwrap_or_else(|| "unknown code mode exception".to_string()));
        }
        return Ok(None);
    };
    let serialized = stringified.to_rust_string_lossy(&tc);
    if serialized == "undefined" {
        return Ok(None);
    }
    serde_json::from_str(&serialized)
        .map(Some)
        .map_err(|err| format!("failed to serialize JavaScript value: {err}"))
}

pub(super) fn normalize_tool_input(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
) -> Result<Option<JsonValue>, String> {
    let first = args.get(0);
    if is_tagged_template_strings(scope, first)? {
        return tagged_template_input_to_json(scope, args).map(Some);
    }
    v8_value_to_json(scope, first)?
        .map(Some)
        .ok_or_else(|| "tool input must be JSON-serializable".to_string())
}

fn is_tagged_template_strings(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Result<bool, String> {
    if !value.is_array() {
        return Ok(false);
    }
    let object = v8::Local::<v8::Object>::try_from(value)
        .map_err(|_| "failed to inspect tagged-template strings".to_string())?;
    let Some(raw_key) = v8::String::new(scope, "raw") else {
        return Err("failed to allocate tagged-template raw key".to_string());
    };
    Ok(object
        .get(scope, raw_key.into())
        .is_some_and(|raw| raw.is_array()))
}

fn tagged_template_input_to_json(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
) -> Result<JsonValue, String> {
    let strings = v8::Local::<v8::Array>::try_from(args.get(0))
        .map_err(|_| "failed to read tagged-template strings".to_string())?;
    let strings = tagged_template_strings_to_json(scope, strings)?;
    let mut values = Vec::new();
    for index in 1..args.length() {
        let Some(value) = v8_value_to_json(scope, args.get(index))? else {
            return Err(format!(
                "failed to serialize tagged-template value {}",
                index - 1
            ));
        };
        values.push(value);
    }
    let mut template = serde_json::Map::new();
    template.insert(
        "strings".to_string(),
        JsonValue::Array(strings.into_iter().map(JsonValue::String).collect()),
    );
    template.insert("values".to_string(), JsonValue::Array(values));

    let mut input = serde_json::Map::new();
    input.insert(
        CODE_MODE_TAGGED_TEMPLATE_KEY.to_string(),
        JsonValue::Object(template),
    );
    Ok(JsonValue::Object(input))
}

fn tagged_template_strings_to_json(
    scope: &mut v8::PinScope<'_, '_>,
    strings: v8::Local<'_, v8::Array>,
) -> Result<Vec<String>, String> {
    let mut result = Vec::with_capacity(strings.length() as usize);
    for index in 0..strings.length() {
        let Some(value) = strings.get_index(scope, index) else {
            return Err(format!(
                "failed to read tagged-template string segment {index}"
            ));
        };
        if !value.is_string() {
            return Err(format!(
                "tagged-template string segment {index} must be a string"
            ));
        }
        result.push(value.to_rust_string_lossy(scope));
    }
    Ok(result)
}

pub(super) fn structured_result_to_json<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
    max_output_tokens: usize,
) -> Result<Option<JsonValue>, String> {
    if value.is_undefined() {
        return Ok(None);
    }

    let mut seen = Vec::new();
    let mut budget = StructuredResultBudget::new(max_output_tokens);
    serialize_structured_result_value(scope, value, "result", &mut seen, &mut budget, 0).map(Some)
}

struct StructuredResultBudget {
    remaining_values: usize,
}

impl StructuredResultBudget {
    fn new(max_output_tokens: usize) -> Self {
        Self {
            remaining_values: max_output_tokens.saturating_mul(4).max(1),
        }
    }

    fn consume_value(&mut self, path: &str) -> Result<(), String> {
        if self.remaining_values == 0 {
            return Err(format!(
                "code mode result exceeded max_output_tokens before serialization at {path}"
            ));
        }
        self.remaining_values -= 1;
        Ok(())
    }

    fn ensure_container_entries(&self, path: &str, len: usize) -> Result<(), String> {
        if len > MAX_STRUCTURED_RESULT_CONTAINER_ENTRIES {
            return Err(format!(
                "code mode result exceeded the container entry limit of {MAX_STRUCTURED_RESULT_CONTAINER_ENTRIES} at {path}"
            ));
        }
        if len > self.remaining_values {
            return Err(format!(
                "code mode result exceeded max_output_tokens before serializing {path}"
            ));
        }
        Ok(())
    }
}

fn serialize_structured_result_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
    path: &str,
    seen: &mut Vec<v8::Global<v8::Object>>,
    budget: &mut StructuredResultBudget,
    depth: usize,
) -> Result<JsonValue, String> {
    if value.is_null() {
        budget.consume_value(path)?;
        return Ok(JsonValue::Null);
    }
    if value.is_undefined() {
        return Err(format!(
            "code mode result must be JSON-serializable; undefined is not valid at {path}"
        ));
    }
    if value.is_boolean() {
        budget.consume_value(path)?;
        return Ok(JsonValue::Bool(value.boolean_value(scope)));
    }
    if value.is_number() {
        budget.consume_value(path)?;
        let number = value.number_value(scope).ok_or_else(|| {
            format!("code mode result must be JSON-serializable; failed to read number at {path}")
        })?;
        if !number.is_finite() {
            return Err(format!(
                "code mode result must be JSON-serializable; non-finite number at {path}"
            ));
        }
        if number.fract() == 0.0 && number >= i64::MIN as f64 && number <= i64::MAX as f64 {
            return Ok(JsonValue::Number(serde_json::Number::from(number as i64)));
        }
        let Some(number) = serde_json::Number::from_f64(number) else {
            return Err(format!(
                "code mode result must be JSON-serializable; failed to encode number at {path}"
            ));
        };
        return Ok(JsonValue::Number(number));
    }
    if value.is_string() {
        budget.consume_value(path)?;
        return Ok(JsonValue::String(value.to_rust_string_lossy(scope)));
    }
    if value.is_big_int() {
        return Err(format!(
            "code mode result must be JSON-serializable; BigInt is not supported at {path}"
        ));
    }
    if value.is_function() {
        return Err(format!(
            "code mode result must be JSON-serializable; function is not supported at {path}"
        ));
    }
    if value.is_symbol() {
        return Err(format!(
            "code mode result must be JSON-serializable; symbol is not supported at {path}"
        ));
    }
    if value.is_proxy() {
        return Err(format!(
            "code mode result must be JSON-serializable; Proxy values are not supported at {path}"
        ));
    }
    if !value.is_object() {
        return Err(format!(
            "code mode result must be JSON-serializable; unsupported value at {path}"
        ));
    }
    budget.consume_value(path)?;
    if depth >= MAX_STRUCTURED_RESULT_DEPTH {
        return Err(format!(
            "code mode result must be JSON-serializable; exceeded max depth of {MAX_STRUCTURED_RESULT_DEPTH} at {path}"
        ));
    }

    let object = v8::Local::<v8::Object>::try_from(value).map_err(|_| {
        format!("code mode result must be JSON-serializable; failed to read object at {path}")
    })?;
    ensure_acyclic(scope, object, path, seen)?;

    let result = if value.is_array() {
        let array = v8::Local::<v8::Array>::try_from(value).map_err(|_| {
            format!("code mode result must be JSON-serializable; failed to read array at {path}")
        })?;
        serialize_structured_result_array(scope, array, path, seen, budget, depth + 1)
    } else {
        serialize_structured_result_object(scope, object, path, seen, budget, depth + 1)
    };
    let _ = seen.pop();
    result
}

fn ensure_acyclic<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    path: &str,
    seen: &mut Vec<v8::Global<v8::Object>>,
) -> Result<(), String> {
    let value: v8::Local<'_, v8::Value> = object.into();
    for ancestor in seen.iter() {
        let ancestor = v8::Local::new(scope, ancestor);
        let ancestor_value: v8::Local<'_, v8::Value> = ancestor.into();
        if value.strict_equals(ancestor_value) {
            return Err(format!(
                "code mode result must be JSON-serializable; cyclic value at {path}"
            ));
        }
    }
    seen.push(v8::Global::new(scope, object));
    Ok(())
}

fn serialize_structured_result_array<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    array: v8::Local<'s, v8::Array>,
    path: &str,
    seen: &mut Vec<v8::Global<v8::Object>>,
    budget: &mut StructuredResultBudget,
    depth: usize,
) -> Result<JsonValue, String> {
    let len = array.length() as usize;
    budget.ensure_container_entries(path, len)?;
    let mut items = Vec::with_capacity(len);
    let object: v8::Local<'_, v8::Object> = array.into();
    for index in 0..array.length() {
        let index_path = format!("{path}[{index}]");
        let Some(key) = v8::String::new(scope, &index.to_string()) else {
            return Err(format!(
                "code mode result must be JSON-serializable; failed to allocate key for {index_path}"
            ));
        };
        let Some(value) = own_data_property_value(scope, object, key.into(), &index_path)? else {
            budget.consume_value(&index_path)?;
            items.push(JsonValue::Null);
            continue;
        };
        if value.is_undefined() {
            budget.consume_value(&index_path)?;
            items.push(JsonValue::Null);
        } else {
            items.push(serialize_structured_result_value(
                scope,
                value,
                &index_path,
                seen,
                budget,
                depth,
            )?);
        }
    }
    Ok(JsonValue::Array(items))
}

fn serialize_structured_result_object<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    path: &str,
    seen: &mut Vec<v8::Global<v8::Object>>,
    budget: &mut StructuredResultBudget,
    depth: usize,
) -> Result<JsonValue, String> {
    let args = v8::GetPropertyNamesArgsBuilder::new()
        .mode(v8::KeyCollectionMode::OwnOnly)
        .property_filter(v8::PropertyFilter::ONLY_ENUMERABLE | v8::PropertyFilter::SKIP_SYMBOLS)
        .key_conversion(v8::KeyConversionMode::ConvertToString)
        .build();
    let Some(keys) = object.get_own_property_names(scope, args) else {
        return Err(format!(
            "code mode result must be JSON-serializable; failed to read object keys at {path}"
        ));
    };

    budget.ensure_container_entries(path, keys.length() as usize)?;
    let mut map = serde_json::Map::new();
    for index in 0..keys.length() {
        let Some(key) = keys.get_index(scope, index) else {
            return Err(format!(
                "code mode result must be JSON-serializable; failed to read object key at {path}"
            ));
        };
        let key_text = key.to_rust_string_lossy(scope);
        let key_name = v8::Local::<v8::Name>::try_from(key).map_err(|_| {
            format!(
                "code mode result must be JSON-serializable; failed to read object key at {path}"
            )
        })?;
        let Some(value) =
            own_data_property_value(scope, object, key_name, &format!("{path}.{key_text}"))?
        else {
            continue;
        };
        if value.is_undefined() {
            continue;
        }
        map.insert(
            key_text.clone(),
            serialize_structured_result_value(
                scope,
                value,
                &format!("{path}.{key_text}"),
                seen,
                budget,
                depth,
            )?,
        );
    }
    Ok(JsonValue::Object(map))
}

fn own_data_property_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    key: v8::Local<'s, v8::Name>,
    path: &str,
) -> Result<Option<v8::Local<'s, v8::Value>>, String> {
    let Some(descriptor) = object.get_own_property_descriptor(scope, key) else {
        return Ok(None);
    };
    let descriptor = v8::Local::<v8::Object>::try_from(descriptor).map_err(|_| {
        format!(
            "code mode result must be JSON-serializable; failed to inspect property descriptor at {path}"
        )
    })?;
    for accessor_key in ["get", "set"] {
        let Some(key) = v8::String::new(scope, accessor_key) else {
            return Err(format!(
                "code mode result must be JSON-serializable; failed to allocate descriptor key at {path}"
            ));
        };
        if descriptor
            .get(scope, key.into())
            .is_some_and(|value| !value.is_undefined())
        {
            return Err(format!(
                "code mode result must be JSON-serializable; accessor properties are not supported at {path}"
            ));
        }
    }
    let Some(value_key) = v8::String::new(scope, "value") else {
        return Err(format!(
            "code mode result must be JSON-serializable; failed to allocate descriptor value key at {path}"
        ));
    };
    let Some(value) = descriptor.get(scope, value_key.into()) else {
        return Err(format!(
            "code mode result must be JSON-serializable; failed to read property descriptor at {path}"
        ));
    };
    Ok(Some(value))
}

pub(super) fn json_to_v8<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: &JsonValue,
) -> Option<v8::Local<'s, v8::Value>> {
    let json = serde_json::to_string(value).ok()?;
    let json = v8::String::new(scope, &json)?;
    v8::json::parse(scope, json)
}

pub(super) fn value_to_error_text(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> String {
    if value.is_object()
        && let Ok(object) = v8::Local::<v8::Object>::try_from(value)
        && let Some(key) = v8::String::new(scope, "stack")
        && let Some(stack) = object.get(scope, key.into())
        && stack.is_string()
    {
        return stack.to_rust_string_lossy(scope);
    }
    value.to_rust_string_lossy(scope)
}

pub(super) fn throw_type_error(scope: &mut v8::PinScope<'_, '_>, message: &str) {
    if let Some(message) = v8::String::new(scope, message) {
        scope.throw_exception(message.into());
    }
}
