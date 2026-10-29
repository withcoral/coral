//! Checks every span and event in the workspace's production source.
//!
//! The export filters drop every key the field catalog does not list, so a
//! field under any other key never leaves the host. This test turns that
//! silent drop into a failure: every field of a span or event, and every
//! key a span records later, must be one the catalog lists. `otel.*` keys
//! configure the span and are not exported, and `coral.local.*` keys stay on
//! the host.
//!
//! An event leaves the host as a log record whose text is its message, or as
//! a span event named by its message, and the filters cannot tell a message
//! written in the source from one formatted from a value. So an event's
//! message must be fixed text, and no field may take an error's value. A
//! failure is named by `error.type`, and its value is a bounded code: a
//! string literal, the error's `code()`, or a `*_code(..)` helper that maps a
//! library's error to one.

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

#[test]
fn every_span_and_event_uses_cataloged_keys_and_no_error_text() {
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
    let mut violations = Vec::new();
    for (file, text) in &sources {
        if test_modules.contains(file) {
            continue;
        }
        let display = file.strip_prefix(crates).unwrap_or(file).display();
        for (line, problem) in violations_in(&production_code(text)) {
            violations.push(format!("{display}:{line}: {problem}"));
        }
    }
    assert!(
        violations.is_empty(),
        "spans and events must use cataloged keys and carry no error text:\n{}",
        violations.join("\n")
    );
}

/// The checks catch what they are written to catch: each line here that
/// breaks a rule is reported, and no other line is.
#[test]
fn the_checks_find_what_they_check() {
    let source = r#"
        tracing::warn!(coral.run.id = %id, coral.local.path = %path, "fixed text");
        tracing::warn!("fixed {{braces}}");
        warn!(error.type = "clock", "fixed text");
        warn!(error.type = error.code(), "fixed text");
        warn!(error.type = sdk_error_code(&error), "fixed text");
        tracing::event!(target: "coral_error", tracing::Level::ERROR, "fixed text");
        tracing::info_span!("coral.run.start", otel.kind = "internal", coral.run.status = Empty);
        span.record("coral.run.status", "ok");
        metric.record(1, &[]);
        tracing::warn!("failed: {error}");
        tracing::error!("failed: {}", detail);
        tracing::warn!(error.type = %error, "fixed text");
        tracing::error!(error.type = error.to_string(), "fixed text");
        tracing::error!(error.type = %IndexError::Clock, "fixed text");
        tracing::event!(tracing::Level::WARN, coral.run.id = ?err, "fixed text");
        tracing::warn!(misclassified = 1, "fixed text");
        tracing::warn!(%claim_id, "fixed text");
        tracing::warn!({ RUN_ID } = %id, "fixed text");
        tracing::info_span!("coral.run.start", detail = Empty);
        span.record("detail", 1);
        warn!(error.type = std::any::type_name_of_val(&error), "fixed text");
        warn!(error.type = kind, "fixed text");
        #[cfg(test)]
        mod tests {
            fn ignored() { tracing::warn!(misclassified = 1, "test {error}"); }
        }
    "#;
    let lines = violations_in(&production_code(source))
        .into_iter()
        .map(|(line, _)| line)
        .collect::<Vec<_>>();
    assert_eq!(lines, (11..=23).collect::<Vec<_>>());
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

/// Each span and event in `code` that breaks a rule, by line.
fn violations_in(code: &str) -> Vec<(usize, String)> {
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
    for (line, arguments) in calls(code, ".record") {
        let key = split_arguments(arguments)
            .first()
            .map(|key| key.trim())
            .and_then(|key| key.strip_prefix('"')?.strip_suffix('"'));
        if let Some(key) = key.filter(|key| !is_allowed_key(key)) {
            violations.push((line, format!("key is not cataloged: {key}")));
        }
    }
    violations.sort();
    violations
}

/// The line and arguments of each call of the macro or method `name` in
/// `code`.
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
        let (Some(open), false) = (open, preceded_by_word && !name.starts_with('.')) else {
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
        || ["target:", "parent:", "name:"]
            .iter()
            .any(|prefix| argument.starts_with(prefix))
        || argument.contains("Level::")
    {
        return None;
    }
    let (key, value) = match argument.split_once('=') {
        Some((key, value)) if !value.starts_with('=') => (key.trim(), value.trim()),
        _ => {
            let shorthand = argument.trim_start_matches(['%', '?']).trim();
            (shorthand, shorthand)
        }
    };
    if !is_allowed_key(key) {
        return Some(format!("key is not cataloged: {argument}"));
    }
    let sigil = value.starts_with(['%', '?']);
    let value = value.trim_start_matches(['%', '?']).trim();
    if key == "error.type" {
        return (sigil || !is_code(value)).then(|| format!("error.type is not a code: {argument}"));
    }
    (!is_code(value) && names_an_error(value)).then(|| format!("field takes an error: {argument}"))
}

/// Whether `value` is a bounded code: a string literal, a `code()` call, or
/// a `*_code(..)` helper. A span field declared `Empty` is recorded later,
/// through `record_failure`.
fn is_code(value: &str) -> bool {
    if value.ends_with("Empty") {
        return true;
    }
    let literal = value.starts_with('"') && value.ends_with('"') && value.len() > 1;
    let helper = value
        .strip_suffix(')')
        .and_then(|call| call.split_once('('))
        .is_some_and(|(function, _)| function.ends_with("code"));
    literal || value.ends_with(".code()") || helper
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

/// Whether `expression` names an error value, by the names Coral gives them.
fn names_an_error(expression: &str) -> bool {
    expression
        .split(|character: char| !(character.is_alphanumeric() || character == '_'))
        .map(str::to_ascii_lowercase)
        .any(|word| {
            word == "e" || word == "err" || word.ends_with("error") || word.ends_with("_err")
        })
}
