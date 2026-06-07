use serde_json::{Map, Value, json};

use crate::description::CodeModeToolKind;

pub(crate) const CODE_MODE_RESULT_SLOT: &str = "__coral_code_mode_result";
pub(crate) const CODE_MODE_TAGGED_TEMPLATE_KEY: &str = "__coral_code_mode_tagged_template";

pub fn wrap_exec_source(source: &str) -> String {
    let source = source.trim();
    if looks_like_function_expression(source) {
        format!(
            r#"const __coral_code_mode_entry = ({source});
if (typeof __coral_code_mode_entry !== "function") {{
  throw new TypeError("Code Mode function expression must evaluate to a function");
}}
const __coral_code_mode_return_value = await __coral_code_mode_entry();
if (__coral_code_mode_return_value !== undefined) {{
  globalThis.{CODE_MODE_RESULT_SLOT} = __coral_code_mode_return_value;
}}"#
        )
    } else {
        format!(
            r"const __coral_code_mode_return_value = await (async () => {{
{source}
}})();"
        ) + &format!(
            r"
if (__coral_code_mode_return_value !== undefined) {{
  globalThis.{CODE_MODE_RESULT_SLOT} = __coral_code_mode_return_value;
}}"
        )
    }
}

pub fn normalize_nested_tool_input(
    tool_name: &str,
    tool_kind: CodeModeToolKind,
    input: Option<Value>,
) -> Result<Value, String> {
    match tool_kind {
        CodeModeToolKind::Function => normalize_function_tool_input(tool_name, input),
        CodeModeToolKind::Freeform => normalize_freeform_tool_input(tool_name, input),
    }
}

fn normalize_function_tool_input(tool_name: &str, input: Option<Value>) -> Result<Value, String> {
    match (tool_name, input) {
        ("sql" | "coral.sql.query", Some(Value::String(sql))) => Ok(json!({ "sql": sql })),
        ("coral.describe", Some(Value::String(reference))) => Ok(json!({ "reference": reference })),
        ("coral.search", Some(Value::String(query))) => Ok(json!({ "query": query })),
        ("sql" | "coral.sql.query", Some(Value::Object(input))) => {
            normalize_sql_tool_object_input(tool_name, input)
        }
        (_, Some(input @ Value::Object(_))) => Ok(input),
        (_, None) => Ok(Value::Object(Map::new())),
        (_, Some(other)) => Err(format!(
            "tool `{tool_name}` expects an object argument, got {}",
            json_type_name(&other)
        )),
    }
}

fn normalize_freeform_tool_input(tool_name: &str, input: Option<Value>) -> Result<Value, String> {
    match input {
        Some(input @ Value::String(_)) => Ok(input),
        None => Ok(Value::String(String::new())),
        Some(other) => Err(format!(
            "tool `{tool_name}` expects a string argument, got {}",
            json_type_name(&other)
        )),
    }
}

fn normalize_sql_tool_object_input(
    tool_name: &str,
    input: Map<String, Value>,
) -> Result<Value, String> {
    if input.contains_key(CODE_MODE_TAGGED_TEMPLATE_KEY) {
        return Err(format!(
            "tool `{tool_name}` does not support tagged-template SQL because SQL parameters are not implemented; pass a SQL string or {{ sql }}"
        ));
    }
    if input.contains_key("params") {
        return Err(format!(
            "tool `{tool_name}` does not support SQL params yet; pass a SQL string or {{ sql }}"
        ));
    }
    Ok(Value::Object(input))
}

fn looks_like_function_expression(source: &str) -> bool {
    if whole_function_expression(source) {
        return true;
    }
    if let Some(after_async) = source.strip_prefix("async")
        && after_async.chars().next().is_some_and(char::is_whitespace)
    {
        let after_async = after_async.trim_start();
        if whole_function_expression(after_async) {
            return true;
        }
        return looks_like_arrow_function_expression(after_async);
    }
    if source.starts_with('(') {
        parenthesized_bare_function_expression(source)
    } else {
        looks_like_arrow_function_expression(source)
    }
}

fn starts_like_function_keyword(source: &str) -> bool {
    let Some(after_function) = source.strip_prefix("function") else {
        return false;
    };
    after_function
        .chars()
        .next()
        .is_some_and(|ch| ch.is_whitespace() || ch == '(' || ch == '*')
}

fn whole_function_expression(source: &str) -> bool {
    if !starts_like_function_keyword(source) {
        return false;
    }
    let Some(open_brace_index) = source.find('{') else {
        return false;
    };
    let Some(close_brace_index) = matching_closing_delimiter(source, open_brace_index, '{', '}')
    else {
        return false;
    };
    let tail = source
        .get(close_brace_index + 1..)
        .unwrap_or_default()
        .trim();
    tail.is_empty() || tail == ";"
}

fn parenthesized_bare_function_expression(source: &str) -> bool {
    let Some(close_index) = matching_closing_paren(source) else {
        return false;
    };
    let tail = source.get(close_index + 1..).unwrap_or_default().trim();
    if !tail.is_empty() && tail != ";" {
        return tail.starts_with("=>");
    }
    let inner = source.get(1..close_index).unwrap_or_default().trim_start();
    whole_function_expression(inner)
        || inner.strip_prefix("async").is_some_and(|after_async| {
            after_async.chars().next().is_some_and(char::is_whitespace) && {
                let after_async = after_async.trim_start();
                whole_function_expression(after_async)
                    || looks_like_arrow_function_expression(after_async)
            }
        })
        || looks_like_arrow_function_expression(inner)
}

fn matching_closing_paren(source: &str) -> Option<usize> {
    matching_closing_delimiter(source, 0, '(', ')')
}

fn matching_closing_delimiter(
    source: &str,
    open_index: usize,
    open_delimiter: char,
    close_delimiter: char,
) -> Option<usize> {
    let mut depth = 0_u32;
    let mut string_delimiter = None;
    let mut escaped = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut chars = source.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if index < open_index {
            continue;
        }
        if line_comment {
            if matches!(ch, '\n' | '\r') {
                line_comment = false;
            }
            continue;
        }
        if block_comment {
            if ch == '*'
                && let Some((_, '/')) = chars.peek()
            {
                let _ = chars.next();
                block_comment = false;
            }
            continue;
        }
        if let Some(delimiter) = string_delimiter {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == delimiter {
                string_delimiter = None;
            }
            continue;
        }
        if ch == '/'
            && let Some((_, next_ch)) = chars.peek()
        {
            match next_ch {
                '/' => {
                    let _ = chars.next();
                    line_comment = true;
                    continue;
                }
                '*' => {
                    let _ = chars.next();
                    block_comment = true;
                    continue;
                }
                _ => {}
            }
        }
        if matches!(ch, '\'' | '"' | '`') {
            string_delimiter = Some(ch);
            continue;
        }
        match ch {
            ch if ch == open_delimiter => depth = depth.saturating_add(1),
            ch if ch == close_delimiter => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn looks_like_arrow_function_expression(source: &str) -> bool {
    if source.starts_with('(') {
        let Some(close_index) = matching_closing_paren(source) else {
            return false;
        };
        return source
            .get(close_index + 1..)
            .unwrap_or_default()
            .trim_start()
            .starts_with("=>");
    }
    let Some((parameter, _body)) = source.split_once("=>") else {
        return false;
    };
    is_simple_identifier(parameter.trim())
}

fn is_simple_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first == '$' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch == '$' || ch.is_ascii_alphanumeric())
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_nested_tool_input, wrap_exec_source};
    use crate::CodeModeToolKind;
    use crate::input::{CODE_MODE_RESULT_SLOT, CODE_MODE_TAGGED_TEMPLATE_KEY};
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn wrap_exec_source_treats_plain_source_as_async_function_body() {
        let wrapped = wrap_exec_source("async function f() { return 1; }\nreturn await f();");

        assert!(wrapped.contains(&format!("globalThis.{CODE_MODE_RESULT_SLOT}")));
        assert!(wrapped.contains("async function f()"));
        assert!(wrapped.contains("return await f();"));
        assert!(!wrapped.contains("const __coral_code_mode_entry"));
    }

    #[test]
    fn wrap_exec_source_invokes_function_expressions() {
        for source in [
            "async () => 1",
            "() => 1",
            "x => 1",
            "async x => 1",
            "function main() { return 1; }",
            "async function main() { return 1; }",
            r#"async function main() { return "}"; }"#,
        ] {
            let wrapped = wrap_exec_source(source);

            assert!(
                wrapped.contains(&format!("const __coral_code_mode_entry = ({source});")),
                "expected {source:?} to be invoked, got {wrapped}"
            );
            assert!(wrapped.contains(&format!("globalThis.{CODE_MODE_RESULT_SLOT}")));
        }
    }

    #[test]
    fn wrap_exec_source_invokes_parenthesized_bare_function_expressions() {
        let wrapped = wrap_exec_source("(async () => 1)");

        assert!(wrapped.contains("const __coral_code_mode_entry = ((async () => 1));"));
        assert!(wrapped.contains("Code Mode function expression must evaluate to a function"));
    }

    #[test]
    fn wrap_exec_source_treats_parenthesized_iife_calls_as_plain_source() {
        let wrapped = wrap_exec_source("(async () => 1)();");

        assert!(wrapped.contains("const __coral_code_mode_return_value = await (async () => {"));
        assert!(!wrapped.contains("Code Mode function expression must evaluate to a function"));
    }

    #[test]
    fn normalize_sql_string_input() {
        assert_eq!(
            normalize_nested_tool_input("sql", CodeModeToolKind::Function, Some(json!("SELECT 1")))
                .unwrap(),
            json!({ "sql": "SELECT 1" })
        );
    }

    #[test]
    fn normalize_sql_tagged_template_input_is_rejected() {
        let mut input = serde_json::Map::new();
        input.insert(
            CODE_MODE_TAGGED_TEMPLATE_KEY.to_string(),
            json!({
                "strings": ["SELECT ", " AS n"],
                "values": [1]
            }),
        );

        let error = normalize_nested_tool_input(
            "sql",
            CodeModeToolKind::Function,
            Some(serde_json::Value::Object(input)),
        )
        .expect_err("tagged-template SQL should be rejected");
        assert!(
            error.contains("does not support tagged-template SQL"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn normalize_sql_params_input_is_rejected() {
        let error = normalize_nested_tool_input(
            "coral.sql.query",
            CodeModeToolKind::Function,
            Some(json!({
                "sql": "SELECT $1 AS n",
                "params": [1]
            })),
        )
        .expect_err("SQL params should be rejected");

        assert!(
            error.contains("does not support SQL params yet"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn normalize_freeform_string_input() {
        assert_eq!(
            normalize_nested_tool_input(
                "write_file",
                CodeModeToolKind::Freeform,
                Some(json!("raw text"))
            )
            .unwrap(),
            json!("raw text")
        );
    }

    #[test]
    fn normalize_freeform_rejects_non_string_input() {
        let error = normalize_nested_tool_input(
            "write_file",
            CodeModeToolKind::Freeform,
            Some(json!({ "text": "raw text" })),
        )
        .unwrap_err();

        assert_eq!(
            error,
            "tool `write_file` expects a string argument, got object"
        );
    }
}
