use std::fmt;

use serde::{Deserialize, Serialize};
use sourcemap::SourceMap;

const MESSAGE_BYTES: usize = 1024;
const NAME_BYTES: usize = 64;
const STACK_BYTES: usize = 16 * 1024;
const ERROR_JSON_BYTES: usize = 8 * 1024;
const TOOL_BODY_BYTES: usize = 1024;

/// An authored location in the existing Program error's stack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramErrorFrame {
    /// Authored Program or Saved Function label.
    pub source: String,
    /// One-based authored line.
    pub line: u32,
    /// One-based authored column.
    pub column: u32,
}

impl fmt::Display for ProgramErrorFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "at {}:{}:{}",
            self.source, self.line, self.column
        )
    }
}

/// Trusted fields of a host-created `ToolError`, read from host-only
/// private state rather than from properties a Program can redefine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramToolError {
    /// Stable Tool error code; absent on Program-created errors.
    pub code: String,
    /// Existing correlation identifier, if supplied to the Program.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// HTTP status of the upstream response the Program did not receive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    /// The start of that response's encoded `data`, at most 1 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// `body` omits the rest of the encoded `data`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub body_truncated: bool,
}

/// The status and bounded encoded `data` of one Tool Result.
pub(crate) fn tool_outcome_excerpt(outcome_json: &str) -> (Option<u16>, Option<String>, bool) {
    let Ok(outcome) = serde_json::from_str::<serde_json::Value>(outcome_json) else {
        return (None, None, false);
    };
    let status = outcome
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .and_then(|status| u16::try_from(status).ok());
    let Some(body) = outcome.get("data").map(serde_json::Value::to_string) else {
        return (status, None, false);
    };
    let excerpt = prefix(&body, TOOL_BODY_BYTES);
    let truncated = excerpt.len() < body.len();
    (status, Some(excerpt.to_string()), truncated)
}

/// Bounded isolate-visible error retained for authorized Run readers, never telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramError {
    /// Program-controlled error name, not a trusted Coral failure classification.
    pub name: String,
    /// Program-controlled message.
    pub message: String,
    /// Mapped authored locations, excluding generated and host frames.
    pub frames: Vec<ProgramErrorFrame>,
    /// Existing fields available only on a privately marked `ToolError`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<ProgramToolError>,
    /// Existing cause, independently bounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<Box<Self>>,
    /// Existing `AggregateError` members, distinct from its cause.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<Self>,
    /// Some content was omitted by a finite bound.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
    /// The cause could not fit within the error budget.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cause_omitted: bool,
    /// Number of aggregate members excluded by bounds.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub omitted_errors: usize,
    /// This node refers back to an ancestor error.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub circular: bool,
}

impl ProgramError {
    /// Compact human-readable summary; authored frames are exposed structurally.
    #[must_use]
    pub fn summary(&self) -> String {
        // PostgreSQL text cannot store NUL; keep its exact value in the JSON error.
        let mut summary =
            format!("{}: {}", self.name, prefix(&self.message, 128)).replace('\0', "\\u0000");
        while serde_json::to_vec(&summary).is_ok_and(|json| json.len() > 256) {
            summary = prefix(&summary, summary.len() / 2).to_string();
        }
        summary
    }

    fn fit(&mut self, maximum_bytes: usize) {
        while serde_json::to_vec(self).is_ok_and(|json| {
            json.len() + serde_json::to_vec(&self.summary()).map_or(256, |summary| summary.len())
                > maximum_bytes
        }) {
            self.truncated = true;
            if self.errors.pop().is_some() {
                self.omitted_errors += 1;
            } else if self.cause.take().is_some() {
                self.cause_omitted = true;
            } else if self.frames.pop().is_some() {
                // Keep the original message even when no frame fits.
            } else if !self.message.is_empty() {
                self.message = prefix(&self.message, self.message.len() / 2).to_string();
            } else {
                break;
            }
        }
    }

    pub(crate) fn unavailable(message: &str) -> Self {
        Self {
            name: "Error".to_string(),
            message: message.to_string(),
            frames: Vec::new(),
            tool: None,
            cause: None,
            errors: Vec::new(),
            truncated: false,
            cause_omitted: false,
            omitted_errors: 0,
            circular: false,
        }
    }
}

impl fmt::Display for ProgramError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.name, self.message)?;
        for frame in &self.frames {
            write!(formatter, "\n{frame}")?;
        }
        Ok(())
    }
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde requires a borrowed field predicate"
)]
fn is_zero(value: &usize) -> bool {
    *value == 0
}

struct AggregatePrototype(v8::Global<v8::Object>);

// Capture the intrinsic before Program code can replace the global constructor
// or its display name. Reading members must not depend on an authored `name`.
pub(crate) fn initialize(scope: &mut v8::PinScope<'_, '_>) -> Result<(), crate::Error> {
    let global = scope.get_current_context().global(scope);
    let prototype = property(scope, global, "AggregateError")
        .and_then(|value| v8::Local::<v8::Object>::try_from(value).ok())
        .and_then(|constructor| property(scope, constructor, "prototype"))
        .and_then(|value| v8::Local::<v8::Object>::try_from(value).ok())
        .ok_or_else(|| {
            crate::Error::Engine("AggregateError intrinsic is unavailable".to_string())
        })?;
    let prototype = v8::Global::new(scope, prototype);
    scope.set_slot(AggregatePrototype(prototype));
    Ok(())
}

fn is_aggregate<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    mut object: v8::Local<'s, v8::Object>,
) -> bool {
    if !object.is_native_error() {
        return false;
    }
    let Some(prototype) = scope.get_slot::<AggregatePrototype>() else {
        return false;
    };
    let prototype = v8::Local::new(scope, &prototype.0);
    for _ in 0..8 {
        if object.is_proxy() {
            return false;
        }
        let Some(parent) = object
            .get_prototype(scope)
            .and_then(|value| v8::Local::<v8::Object>::try_from(value).ok())
        else {
            return false;
        };
        if parent.strict_equals(prototype.into()) {
            return true;
        }
        object = parent;
    }
    false
}

pub(crate) fn from_exception<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
    source_map: &SourceMap,
) -> ProgramError {
    let mut error = read_error(scope, value, source_map, &mut Vec::new(), &mut 16);
    error.fit(ERROR_JSON_BYTES);
    error
}

fn read_error<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
    source_map: &SourceMap,
    ancestors: &mut Vec<v8::Local<'s, v8::Value>>,
    remaining: &mut usize,
) -> ProgramError {
    let mut error = ProgramError::unavailable("Program rejected");
    *remaining = remaining.saturating_sub(1);
    if ancestors
        .iter()
        .any(|ancestor| ancestor.strict_equals(value))
    {
        error.message = "Circular error reference".to_string();
        error.circular = true;
        return error;
    }
    let Ok(object) = v8::Local::<v8::Object>::try_from(value) else {
        error.name = "ThrownValue".to_string();
        if value.is_big_int() {
            error.message = "Program threw a BigInt".to_string();
            error.truncated = true;
        } else if let Some(value) = value.to_string(scope) {
            error.message = bounded_string(scope, value, MESSAGE_BYTES, &mut error.truncated);
        }
        return error;
    };
    if !value.is_native_error() {
        error.name = "ThrownValue".to_string();
    }
    for (name, field, limit) in [
        ("name", &mut error.name, NAME_BYTES),
        ("message", &mut error.message, MESSAGE_BYTES),
    ] {
        if let Some(value) = property(scope, object, name)
            && let Ok(value) = v8::Local::<v8::String>::try_from(value)
        {
            *field = bounded_string(scope, value, limit, &mut error.truncated);
        }
    }
    if let Some(stack) = property(scope, object, "stack")
        && let Ok(stack) = v8::Local::<v8::String>::try_from(stack)
    {
        let stack = bounded_string(scope, stack, STACK_BYTES, &mut error.truncated);
        error.frames = crate::source_map::authored_frames(&stack, source_map);
    } else {
        error.frames = crate::source_map::authored_frames(&native_stack(scope, value), source_map);
    }
    error.tool = crate::runtime::tool_error_fields(scope, object);
    if error.frames.len() > 16 {
        error.frames.truncate(16);
        error.truncated = true;
    }
    ancestors.push(value);
    if let Some(cause) = property(scope, object, "cause").filter(|value| !value.is_undefined()) {
        if ancestors.len() <= 4 && *remaining > 0 {
            error.cause = Some(Box::new(read_error(
                scope, cause, source_map, ancestors, remaining,
            )));
        } else {
            error.cause_omitted = true;
            error.truncated = true;
        }
    }
    if is_aggregate(scope, object)
        && let Some(members) = property(scope, object, "errors")
        && let Ok(members) = v8::Local::<v8::Array>::try_from(members)
    {
        let count = members.length() as usize;
        for index in 0..count.min(8) {
            if ancestors.len() > 4 || *remaining == 0 {
                break;
            }
            if let Some(member) = property(scope, members.into(), &index.to_string()) {
                error
                    .errors
                    .push(read_error(scope, member, source_map, ancestors, remaining));
            }
        }
        error.omitted_errors = count.saturating_sub(error.errors.len());
        error.truncated |= error.omitted_errors != 0;
    }
    ancestors.pop();
    error
}

// Read data descriptors only. Accessors and proxy traps would run new Program
// code after the error, potentially adding effects or replacing it with a timeout.
fn property<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    mut object: v8::Local<'s, v8::Object>,
    name: &str,
) -> Option<v8::Local<'s, v8::Value>> {
    let key = v8::String::new(scope, name)?;
    let value_key = v8::String::new(scope, "value")?;
    for _ in 0..8 {
        if object.is_proxy() {
            return None;
        }
        let descriptor = object.get_own_property_descriptor(scope, key.into())?;
        if !descriptor.is_undefined() {
            let descriptor = v8::Local::<v8::Object>::try_from(descriptor).ok()?;
            return (descriptor.has_own_property(scope, value_key.into()) == Some(true))
                .then(|| descriptor.get(scope, value_key.into()))
                .flatten();
        }
        object = v8::Local::<v8::Object>::try_from(object.get_prototype(scope)?).ok()?;
    }
    None
}

fn native_stack(scope: &v8::PinScope<'_, '_>, value: v8::Local<'_, v8::Value>) -> String {
    let Some(stack) = v8::Exception::get_stack_trace(scope, value) else {
        return String::new();
    };
    let mut raw = String::new();
    for index in 0..stack.get_frame_count().min(32) {
        if let Some(frame) = stack.get_frame(scope, index)
            && let Some(script) = frame.get_script_name(scope)
            && script.to_rust_string_lossy(scope) == "program.js"
        {
            use std::fmt::Write as _;
            let _written = writeln!(
                raw,
                "at program.js:{}:{}",
                frame.get_line_number(),
                frame.get_column()
            );
        }
    }
    raw
}

fn bounded_string(
    scope: &v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::String>,
    limit: usize,
    truncated: &mut bool,
) -> String {
    let mut bytes = vec![0; limit];
    let mut characters = 0;
    let written = value.write_utf8_v2(
        scope,
        &mut bytes,
        v8::WriteFlags::kReplaceInvalidUtf8,
        Some(&mut characters),
    );
    bytes.truncate(written);
    *truncated |= characters < value.length();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn prefix(value: &str, maximum: usize) -> &str {
    let mut end = maximum.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.get(..end).unwrap_or_default()
}
