//! Checks every span and event in the workspace's production source.
//!
//! The export filters drop every key the field catalog does not list, so a
//! field under any other key never leaves the host. This test turns that
//! silent drop into a failure: every field of a span, an event or a
//! `#[tracing::instrument]`, and every key a span records later, must be one
//! the catalog lists. `otel.*` keys configure the span, and `coral.local.*`
//! keys stay on the host.
//!
//! A filter cannot tell what a cataloged key's value holds, so the value
//! must be a literal, `Empty`, or a call of a [`crate::safe`] constructor,
//! which takes only types that cannot hold caller or error text. A value
//! recorded with `%` or `?` is neither. A failure is named by `error.type`,
//! whose value is a code: a literal, or a constructor that returns
//! `&'static str`. An event leaves the host as a log record or span event
//! named by its message, so the message must be fixed text.
//!
//! The OpenTelemetry span API sets names, attributes, events and links past
//! all of this, so only this crate calls it.

use std::path::{Path, PathBuf};

const EVENT_MACROS: [&str; 6] = ["trace!", "debug!", "info!", "warn!", "error!", "event!"];
const SPAN_MACROS: [&str; 6] = [
    "trace_span!",
    "debug_span!",
    "info_span!",
    "warn_span!",
    "error_span!",
    "span!",
];
const INSTRUMENT_ATTRIBUTES: [&str; 2] = ["#[tracing::instrument", "#[instrument"];
const SAFE_CONSTRUCTORS: [&str; 9] = [
    "id",
    "kind",
    "number",
    "flag",
    "span_name",
    "http_span_name",
    "http_method",
    "http_status_code",
    "json_rpc_code",
];
/// The constructors whose value is a code, the only values of `error.type`.
const CODE_CONSTRUCTORS: [&str; 4] = ["kind", "http_method", "http_status_code", "json_rpc_code"];
const OPENTELEMETRY_SPAN_CALLS: [&str; 8] = [
    ".update_name",
    ".set_attribute",
    ".set_attributes",
    ".add_event",
    ".add_event_with_timestamp",
    ".add_link",
    ".set_status",
    ".record_error",
];
/// The calls that make runtime text `'static`, which [`crate::safe::kind`]
/// takes for build-time text.
const LEAKS: [&str; 2] = [".leak", "::leak"];

#[test]
fn every_span_and_event_uses_cataloged_keys_and_safe_values() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crates directory");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(crates).expect("read the crates directory") {
        let source = entry.expect("crate entry").path().join("src");
        if source.is_dir() {
            rust_files(&source, &mut files);
        }
    }
    assert!(
        files.contains(&crates.join("coral-run/src/trace.rs")),
        "the scan reads other crates' source"
    );
    let sources = files
        .into_iter()
        .map(|file| {
            let text = std::fs::read_to_string(&file).expect("read a source file");
            (file, text)
        })
        .collect::<Vec<_>>();
    let test_modules = sources
        .iter()
        .flat_map(|(file, text)| test_module_files(file, text))
        .collect::<Vec<_>>();
    let owner = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    for (file, text) in &sources {
        if test_modules.contains(file) {
            continue;
        }
        let display = file.strip_prefix(crates).unwrap_or(file).display();
        for (line, problem) in violations_in(&production_code(text), file.starts_with(&owner)) {
            violations.push(format!("{display}:{line}: {problem}"));
        }
    }
    assert!(
        violations.is_empty(),
        "spans and events must use cataloged keys and safe values:\n{}",
        violations.join("\n")
    );
}

/// The checks catch what they are written to catch: each line here that
/// breaks a rule is reported, and no other line is.
#[test]
fn the_checks_find_what_they_check() {
    let source = r#"
        tracing::warn!(coral.run.id = safe::id(&id), coral.local.path = %path, "fixed text");
        tracing::warn!("fixed {{braces}}");
        warn!(error.type = "clock", "fixed text");
        warn!(error.type = safe::kind(error.code()), "fixed text");
        tracing::event!(target: "coral_error", tracing::Level::ERROR, "fixed text");
        tracing::info_span!("coral.run.start", otel.kind = "internal", coral.run.status = Empty);
        tracing::info_span!(name: "coral.x", otel.name = safe::span_name("GET", route.as_ref()));
        tracing::info_span!("x", http.route = route.map(safe::id), process.pid = 1_i64);
        span.record("coral.run.status", safe::kind("ok"));
        span.record("coral.invocation.dispatch_started", true);
        metric.record(1, &[]);
        reply.record(outcome);
        #[tracing::instrument(skip_all, name = "coral.private.get_task")]
        tracing::warn!("failed: {error}");
        tracing::error!("failed: {}", detail);
        tracing::warn!(error.type = %error, "fixed text");
        tracing::error!(error.type = error.code(), "fixed text");
        tracing::error!(error.type = safe::id(&kind), "fixed text");
        tracing::event!(tracing::Level::WARN, coral.run.id = ?run, "fixed text");
        tracing::warn!(coral.tool.call_path = %detail, "fixed text");
        tracing::warn!(coral.tool.call_path = detail, "fixed text");
        tracing::warn!(coral.tool.call_path = decode(detail), "fixed text");
        tracing::warn!(misclassified = 1, "fixed text");
        tracing::warn!(%claim_id, "fixed text");
        tracing::warn!({ RUN_ID } = %id, "fixed text");
        tracing::info_span!("coral.run.start", detail = Empty);
        tracing::info_span!("x", otel.name = format!("GET {path}"));
        span.record("detail", 1);
        span.record(KEY, safe::number(1));
        span.record("coral.run.status", status);
        span.record("error.type", error.to_string());
        #[tracing::instrument(name = "coral.private.get_task")]
        #[instrument(skip_all, err)]
        #[tracing::instrument(skip_all, fields(coral.tool.call_path = %detail))]
        span.context().span().update_name(path);
        span.set_status(Status::error(text));
        safe::kind(String::leak(text))
        safe::kind(Box::leak(text.into_boxed_str()))
        #[cfg(test)]
        mod tests {
            fn ignored() { tracing::warn!(misclassified = 1, "test {error}"); }
        }
    "#;
    let lines = violations_in(&production_code(source), false)
        .into_iter()
        .map(|(line, _)| line)
        .collect::<Vec<_>>();
    assert_eq!(lines, (15..=39).collect::<Vec<_>>());
}

fn rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("read a source directory") {
        let path = entry.expect("source entry").path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if path.is_dir() {
            if name != "tests" {
                rust_files(&path, files);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && !name.ends_with("tests.rs")
            && name != "testing.rs"
        {
            files.push(path);
        }
    }
}

/// The files of the modules that `file`, whose source is `text`, declares
/// under `#[cfg(test)]`.
fn test_module_files(file: &Path, text: &str) -> Vec<PathBuf> {
    let stem = file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("");
    let directory = file.parent().unwrap_or(file);
    let directory = if ["lib", "main", "mod"].contains(&stem) {
        directory.to_path_buf()
    } else {
        directory.join(stem)
    };
    text.split("#[cfg(test)]")
        .skip(1)
        .filter_map(|after| {
            let item = after
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty() && !line.starts_with("#["))?;
            item.strip_prefix("mod ")?.strip_suffix(';').map(str::trim)
        })
        .flat_map(|name| {
            [
                directory.join(format!("{name}.rs")),
                directory.join(name).join("mod.rs"),
            ]
        })
        .collect()
}

/// `text` with every `#[cfg(test)]` module body blanked, keeping line breaks
/// so line numbers still match.
fn production_code(text: &str) -> String {
    let mut code = text.to_owned();
    let mut from = 0;
    while let Some(found) = code.get(from..).and_then(|rest| rest.find("#[cfg(test)]")) {
        let start = from + found;
        let after = start + "#[cfg(test)]".len();
        let rest = code.get(after..).unwrap_or("").trim_start();
        let is_module_body = rest.starts_with("mod ")
            && rest
                .find('{')
                .is_some_and(|brace| rest.find(';').is_none_or(|semicolon| brace < semicolon));
        if !is_module_body {
            from = after;
            continue;
        }
        let open = after
            + code
                .get(after..)
                .and_then(|rest| rest.find('{'))
                .unwrap_or(0);
        let end = closing(&code, open).unwrap_or(code.len());
        let blank = code
            .get(start..end)
            .unwrap_or("")
            .chars()
            .map(|character| if character == '\n' { '\n' } else { ' ' })
            .collect::<String>();
        code.replace_range(start..end, &blank);
        from = end;
    }
    code
}

/// The index just past the bracket that closes the one at `open`, skipping
/// string and character literals and comments.
fn closing(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0_usize;
    let mut index = open;
    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index + 1);
                }
            }
            b'"' => index = string_end(bytes, index)? - 1,
            b'\'' => index = char_end(bytes, index) - 1,
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += bytes.get(index..)?.iter().position(|&b| b == b'\n')?;
            }
            _ => {}
        }
        index += 1;
    }
    None
}

/// The index just past the string literal that starts at `start`. A raw
/// string's leading `r` and `#`s come before `start`; its closing quote must
/// be followed by as many `#`s.
fn string_end(bytes: &[u8], start: usize) -> Option<usize> {
    let hashes = bytes
        .get(..start)?
        .iter()
        .rev()
        .take_while(|&&byte| byte == b'#')
        .count();
    let raw = start
        .checked_sub(hashes + 1)
        .and_then(|index| bytes.get(index))
        == Some(&b'r');
    let mut index = start + 1;
    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'\\' if !raw => index += 1,
            b'"' if (1..=hashes).all(|offset| bytes.get(index + offset) == Some(&b'#')) => {
                return Some(index + 1 + hashes);
            }
            _ => {}
        }
        index += 1;
    }
    None
}

/// The index just past a character literal at `start`, or just past `start`
/// for a lifetime.
fn char_end(bytes: &[u8], start: usize) -> usize {
    match (
        bytes.get(start + 1),
        bytes.get(start + 2),
        bytes.get(start + 3),
    ) {
        (Some(b'\\'), _, _) => {
            let rest = bytes.get(start + 2..).unwrap_or(&[]);
            start + 3 + rest.iter().position(|&byte| byte == b'\'').unwrap_or(0)
        }
        (Some(_), Some(b'\''), _) => start + 3,
        _ => start + 1,
    }
}

/// Each span, event and span-API call in `code` that breaks a rule, by
/// line. `owner` is whether `code` is this crate's own, the one place that
/// calls the OpenTelemetry span API.
fn violations_in(code: &str, owner: bool) -> Vec<(usize, String)> {
    let mut violations = Vec::new();
    for name in EVENT_MACROS {
        for (line, arguments) in calls(code, name) {
            violations.extend(event_problem(arguments).map(|problem| (line, problem)));
        }
    }
    for name in SPAN_MACROS {
        for (line, arguments) in calls(code, name) {
            let problem = split_arguments(arguments)
                .into_iter()
                .find_map(|argument| field_problem(argument.trim()));
            violations.extend(problem.map(|problem| (line, problem)));
        }
    }
    for name in INSTRUMENT_ATTRIBUTES {
        for (line, arguments) in calls(code, name) {
            violations.extend(instrument_problem(arguments).map(|problem| (line, problem)));
        }
    }
    for (line, arguments) in calls(code, ".record") {
        violations.extend(record_problem(arguments).map(|problem| (line, problem)));
    }
    if !owner {
        for name in OPENTELEMETRY_SPAN_CALLS {
            for (line, _) in calls(code, name) {
                violations.push((
                    line,
                    format!("only coral-telemetry calls the OpenTelemetry span API: {name}"),
                ));
            }
        }
    }
    for name in LEAKS {
        for (line, _) in calls(code, name) {
            violations.push((
                line,
                format!("leaked text passes for build-time text: {name}"),
            ));
        }
    }
    violations.sort();
    violations
}

/// The line and arguments of each call of the macro, method or attribute
/// `name` in `code`.
fn calls<'a>(code: &'a str, name: &str) -> Vec<(usize, &'a str)> {
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(offset) = code.get(from..).and_then(|rest| rest.find(name)) {
        let start = from + offset;
        from = start + name.len();
        let preceded_by_word = code
            .get(..start)
            .and_then(|before| before.chars().next_back())
            .is_some_and(|character| character.is_alphanumeric() || character == '_');
        let open = code
            .get(from..)
            .and_then(|rest| rest.find(|character: char| !character.is_whitespace()))
            .map(|offset| from + offset)
            .filter(|&open| code.as_bytes().get(open) == Some(&b'('));
        let (Some(open), false) = (
            open,
            preceded_by_word && name.starts_with(char::is_alphabetic),
        ) else {
            continue;
        };
        let Some(end) = closing(code, open) else {
            continue;
        };
        let line = code.get(..start).unwrap_or("").matches('\n').count() + 1;
        found.push((line, code.get(open + 1..end - 1).unwrap_or("")));
    }
    found
}

/// What is wrong with an event whose macro arguments are `arguments`.
fn event_problem(arguments: &str) -> Option<String> {
    let arguments = split_arguments(arguments);
    for (index, argument) in arguments.iter().enumerate() {
        let argument = argument.trim();
        if argument.starts_with('"')
            && (index + 1 < arguments.len() || argument.replace("{{", "").contains('{'))
        {
            return Some(format!("message is formatted: {argument}"));
        }
        if let Some(problem) = field_problem(argument) {
            return Some(problem);
        }
    }
    None
}

/// What is wrong with one macro argument of a span or event, if it is a
/// field.
fn field_problem(argument: &str) -> Option<String> {
    // A macro's `$` arguments are fields its own callers name.
    if argument.starts_with(['"', '$'])
        || ["target:", "parent:"]
            .iter()
            .any(|prefix| argument.starts_with(prefix))
        || argument.contains("Level::")
    {
        return None;
    }
    if let Some(name) = argument.strip_prefix("name:") {
        return (!is_literal(name.trim())).then(|| format!("name is not fixed text: {argument}"));
    }
    let (key, value) = match argument.split_once('=') {
        Some((key, value)) if !value.starts_with('=') => (key.trim(), Some(value.trim())),
        _ => (argument.trim_start_matches(['%', '?']).trim(), None),
    };
    if !is_allowed_key(key) {
        return Some(format!("key is not cataloged: {argument}"));
    }
    match value {
        Some(value) => value_problem(key, value).map(|problem| format!("{problem}: {argument}")),
        None => Some(format!("value is not a safe value: {argument}")),
    }
}

/// What is wrong with `value`, the value of the field `key`. A
/// `coral.local.*` field stays on the host, so its value may be anything.
fn value_problem(key: &str, value: &str) -> Option<&'static str> {
    if key.starts_with(crate::LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX) {
        return None;
    }
    if value.starts_with('?') {
        return Some("value is recorded with Debug");
    }
    if value.starts_with('%') {
        return Some("value is recorded with Display");
    }
    let constructor = safe_constructor(value);
    if key == "error.type" {
        let code = is_literal(value)
            || is_empty(value)
            || constructor.is_some_and(|constructor| CODE_CONSTRUCTORS.contains(&constructor));
        return (!code).then_some("error.type is not a code");
    }
    let safe = is_literal(value) || is_empty(value) || constructor.is_some();
    (!safe).then_some("value is not a safe value")
}

/// The `safe` constructor that `value` is a call of, or that it maps an
/// `Option` with.
fn safe_constructor(value: &str) -> Option<&'static str> {
    let mapped = value
        .strip_suffix(')')
        .and_then(|value| value.rsplit_once(".map("))
        .map(|(_, constructor)| constructor);
    let path = if let Some(constructor) = mapped {
        constructor
    } else {
        let open = value.find('(')?;
        (closing(value, open)? == value.len()).then_some(())?;
        value.get(..open)?
    };
    let name = ["coral_telemetry::safe::", "crate::safe::", "safe::"]
        .iter()
        .find_map(|prefix| path.strip_prefix(prefix))?;
    SAFE_CONSTRUCTORS
        .iter()
        .copied()
        .find(|constructor| *constructor == name)
}

/// Whether `value` is a string, number or `bool` literal.
fn is_literal(value: &str) -> bool {
    (value.starts_with('"') && value.ends_with('"') && value.len() > 1)
        || value.starts_with(|character: char| character.is_ascii_digit())
        || value == "true"
        || value == "false"
}

/// Whether `value` declares a span field that is recorded later.
fn is_empty(value: &str) -> bool {
    value == "Empty" || value.ends_with("::Empty")
}

/// What is wrong with a `.record(key, value)` call whose arguments are
/// `arguments`. A call with another number of arguments, or with an
/// attribute slice, records something other than a span field.
fn record_problem(arguments: &str) -> Option<String> {
    let arguments = split_arguments(arguments);
    let [key, value] = arguments.as_slice() else {
        return None;
    };
    let (key, value) = (key.trim(), value.trim());
    if value.starts_with("&[") {
        return None;
    }
    let Some(name) = key.strip_prefix('"').and_then(|key| key.strip_suffix('"')) else {
        return Some(format!("key is not a literal: {key}"));
    };
    if !is_allowed_key(name) {
        return Some(format!("key is not cataloged: {name}"));
    }
    value_problem(name, value).map(|problem| format!("{problem}: {name} = {value}"))
}

/// What is wrong with a `#[tracing::instrument(..)]` whose arguments are
/// `arguments`. Without `skip_all` the span records every argument with
/// Debug; `err` and `ret` record the error and the return value.
fn instrument_problem(arguments: &str) -> Option<String> {
    let arguments = split_arguments(arguments)
        .into_iter()
        .map(str::trim)
        .collect::<Vec<_>>();
    if !arguments.contains(&"skip_all") {
        return Some("instrument records its arguments: add skip_all".to_owned());
    }
    for argument in arguments {
        let name = argument
            .split(|character: char| !(character.is_alphanumeric() || character == '_'))
            .next()
            .unwrap_or("");
        match name {
            "err" | "ret" => return Some(format!("instrument records a value: {argument}")),
            "fields" => {
                let fields = argument
                    .strip_prefix("fields")
                    .map(str::trim)
                    .and_then(|fields| fields.strip_prefix('(')?.strip_suffix(')'))
                    .unwrap_or("");
                let problem = split_arguments(fields)
                    .into_iter()
                    .find_map(|field| field_problem(field.trim()));
                if problem.is_some() {
                    return problem;
                }
            }
            "name" => {
                let value = argument
                    .split_once('=')
                    .map_or("", |(_, value)| value.trim());
                if !is_literal(value) {
                    return Some(format!("name is not fixed text: {argument}"));
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether a span or event may carry `key`: one the catalog lists, an
/// `otel.*` key that configures the span, or a `coral.local.*` key that
/// stays on the host.
fn is_allowed_key(key: &str) -> bool {
    crate::field::is_exported(key)
        || key.starts_with("otel.")
        || key.starts_with(crate::LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX)
}

fn split_arguments(arguments: &str) -> Vec<&str> {
    let bytes = arguments.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'(' | b'[' | b'{' => index = closing(arguments, index).unwrap_or(bytes.len()) - 1,
            b'"' => index = string_end(bytes, index).unwrap_or(bytes.len()) - 1,
            b'\'' => index = char_end(bytes, index) - 1,
            b',' => {
                parts.push(arguments.get(start..index).unwrap_or(""));
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    let last = arguments.get(start..).unwrap_or("");
    if !last.trim().is_empty() {
        parts.push(last);
    }
    parts
}
