//! Bounded, non-executing previews of Middleware return values and errors.
//!
//! A preview never calls Program code: it does not read accessors, call
//! `toJSON` or `toString`, or enter Proxy traps. Values that cannot be shown
//! faithfully without executing code appear as bracketed placeholders.

use serde_json::{Map, Value};

const MAX_DEPTH: usize = 4;
const MAX_ENTRIES: u32 = 20;
const MAX_STRING_CHARS: usize = 256;
const MAX_PREVIEW_BYTES: usize = 4 * 1024;
const MAX_SUMMARY_BYTES: usize = 1024;
const SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// Returns the JSON preview, or `None` for `undefined`.
pub(crate) fn value_preview<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
) -> Option<String> {
    if value.is_undefined() {
        return None;
    }
    let preview = preview(scope, value, 0).to_string();
    if preview.len() <= MAX_PREVIEW_BYTES {
        Some(preview)
    } else {
        Some(Value::String("[preview exceeds 4 KiB]".to_string()).to_string())
    }
}

/// Returns a bounded safe summary of a thrown value.
pub(crate) fn error_summary<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
) -> String {
    let summary = if let Ok(object) = v8::Local::<v8::Object>::try_from(value)
        && !value.is_proxy()
        && value.is_native_error()
    {
        let name = crate::runtime::operation_error_fields(scope, object).map_or_else(
            || object.get_constructor_name().to_rust_string_lossy(scope),
            |fields| format!("OperationError({})", fields.code),
        );
        match own_data(scope, object, "message") {
            Some(message) if message.is_string() => {
                format!("{name}: {}", message.to_rust_string_lossy(scope))
            }
            _ => name,
        }
    } else {
        match preview(scope, value, MAX_DEPTH) {
            Value::String(text) => text,
            other => other.to_string(),
        }
    };
    truncate(&summary, MAX_SUMMARY_BYTES)
}

fn preview<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
    depth: usize,
) -> Value {
    if value.is_null() {
        return Value::Null;
    }
    if value.is_undefined() {
        return Value::String("[undefined]".to_string());
    }
    if value.is_boolean() {
        return Value::Bool(value.is_true());
    }
    if value.is_number() {
        let number = value.number_value(scope).unwrap_or(f64::NAN);
        if number.fract() == 0.0 && number.abs() <= SAFE_INTEGER {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "an integral value within the safe-integer range is exact"
            )]
            return Value::from(number as i64);
        }
        return serde_json::Number::from_f64(number)
            .map_or_else(|| Value::String(number.to_string()), Value::Number);
    }
    if value.is_string() {
        let text = value.to_rust_string_lossy(scope);
        return Value::String(truncate_chars(&text, MAX_STRING_CHARS));
    }
    if value.is_big_int() {
        return Value::String(format!("{}n", value.to_rust_string_lossy(scope)));
    }
    if value.is_symbol() {
        return Value::String("[Symbol]".to_string());
    }
    if value.is_proxy() {
        return Value::String("[Proxy]".to_string());
    }
    if value.is_function() {
        return Value::String("[Function]".to_string());
    }
    let Ok(object) = v8::Local::<v8::Object>::try_from(value) else {
        return Value::String("[Value]".to_string());
    };
    if value.is_native_error() {
        return Value::String(error_summary(scope, value));
    }
    if depth >= MAX_DEPTH {
        return Value::String("[Object]".to_string());
    }
    if let Ok(array) = v8::Local::<v8::Array>::try_from(value) {
        let length = array.length();
        let mut items = Vec::new();
        for index in 0..length.min(MAX_ENTRIES) {
            let Some(key) = v8::Integer::new_from_unsigned(scope, index).to_string(scope) else {
                break;
            };
            let key: v8::Local<v8::Name> = key.into();
            items.push(property_preview(scope, object, key, depth));
        }
        if length > MAX_ENTRIES {
            items.push(Value::String(format!("[{} more]", length - MAX_ENTRIES)));
        }
        return Value::Array(items);
    }
    let constructor = object.get_constructor_name().to_rust_string_lossy(scope);
    if constructor != "Object" && !object.get_prototype(scope).is_some_and(|p| p.is_null()) {
        return Value::String(format!("[{constructor}]"));
    }
    let Some(names) = object.get_own_property_names(
        scope,
        v8::GetPropertyNamesArgs {
            property_filter: v8::PropertyFilter::ONLY_ENUMERABLE | v8::PropertyFilter::SKIP_SYMBOLS,
            key_conversion: v8::KeyConversionMode::ConvertToString,
            ..Default::default()
        },
    ) else {
        return Value::String("[Object]".to_string());
    };
    let mut entries = Map::new();
    let count = names.length();
    for index in 0..count.min(MAX_ENTRIES) {
        let Some(name) = names.get_index(scope, index) else {
            continue;
        };
        let Ok(key) = v8::Local::<v8::Name>::try_from(name) else {
            continue;
        };
        let label = truncate_chars(&name.to_rust_string_lossy(scope), MAX_STRING_CHARS);
        let item = property_preview(scope, object, key, depth);
        entries.insert(label, item);
    }
    if count > MAX_ENTRIES {
        entries.insert(
            "[more]".to_string(),
            Value::String(format!("{} more", count - MAX_ENTRIES)),
        );
    }
    Value::Object(entries)
}

fn property_preview<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    key: v8::Local<'s, v8::Name>,
    depth: usize,
) -> Value {
    match own_data_by_key(scope, object, key) {
        OwnProperty::Data(value) => preview(scope, value, depth + 1),
        OwnProperty::Accessor => Value::String("[Getter]".to_string()),
        OwnProperty::Absent => Value::String("[undefined]".to_string()),
    }
}

fn own_data<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    name: &str,
) -> Option<v8::Local<'s, v8::Value>> {
    let key = v8::String::new(scope, name)?;
    match own_data_by_key(scope, object, key.into()) {
        OwnProperty::Data(value) => Some(value),
        OwnProperty::Accessor | OwnProperty::Absent => None,
    }
}

/// One own property, read without calling an accessor.
enum OwnProperty<'s> {
    Data(v8::Local<'s, v8::Value>),
    Accessor,
    Absent,
}

fn own_data_by_key<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    key: v8::Local<'_, v8::Name>,
) -> OwnProperty<'s> {
    let read = || -> Option<OwnProperty<'s>> {
        let descriptor = object.get_own_property_descriptor(scope, key)?;
        let descriptor = v8::Local::<v8::Object>::try_from(descriptor).ok()?;
        let get = v8::String::new(scope, "get")?;
        if descriptor.has_own_property(scope, get.into())? {
            return Some(OwnProperty::Accessor);
        }
        let value = v8::String::new(scope, "value")?;
        if !descriptor.has_own_property(scope, value.into())? {
            return Some(OwnProperty::Absent);
        }
        descriptor.get(scope, value.into()).map(OwnProperty::Data)
    };
    read().unwrap_or(OwnProperty::Absent)
}

fn truncate_chars(text: &str, maximum: usize) -> String {
    if text.chars().count() <= maximum {
        return text.to_string();
    }
    let mut truncated: String = text.chars().take(maximum).collect();
    truncated.push('…');
    truncated
}

fn truncate(text: &str, maximum_bytes: usize) -> String {
    if text.len() <= maximum_bytes {
        return text.to_string();
    }
    let mut end = maximum_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", text.get(..end).unwrap_or_default())
}
