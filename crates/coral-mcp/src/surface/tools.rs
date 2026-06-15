use std::sync::Arc;

use coral_capabilities::SchemaRenderMode;
use rmcp::{
    ErrorData,
    model::{CallToolResult, Tool, ToolAnnotations},
};
use serde_json::{Map, Value, json};

use crate::McpRuntimeExposure;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Pagination {
    pub(crate) limit: u32,
    pub(crate) offset: u32,
}

#[derive(Debug)]
pub(crate) struct SearchArguments {
    pub(crate) query: String,
    pub(crate) source_key: String,
    pub(crate) display_name: String,
    pub(crate) kind: String,
    pub(crate) capability_kind: String,
    pub(crate) effect: String,
    pub(crate) intent: String,
    pub(crate) expand_top: bool,
    pub(crate) pagination: Pagination,
}

#[derive(Debug)]
pub(crate) struct DescribeArguments {
    pub(crate) reference: String,
    pub(crate) view: DescribeView,
    pub(crate) path: Option<String>,
    pub(crate) schemas: SchemaRenderMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DescribeView {
    Compact,
    Detailed,
}

pub(crate) struct ExecArguments {
    pub(crate) source: String,
}

pub(crate) struct WaitArguments {
    pub(crate) run_id: String,
    pub(crate) cursor: u64,
    pub(crate) terminate: bool,
}

pub(crate) fn search_tool(exposure: McpRuntimeExposure) -> Tool {
    let (description, query_description) = match (exposure.typescript_enabled, exposure.sql_enabled)
    {
        (true, true) => (
            "Search generated Coral exports for capabilities, TypeScript bindings, and SQL bindings.",
            "Text, typed ref, capability id, generated tool path, TypeScript path, or SQL ref to search.",
        ),
        (true, false) => (
            "Search generated Coral exports for capabilities and TypeScript bindings visible in this runtime exposure.",
            "Text, typed ref, capability id, generated tool path, or TypeScript path to search.",
        ),
        (false, true) => (
            "Search generated Coral exports for capabilities and SQL bindings visible in this runtime exposure.",
            "Text, typed ref, capability id, SQL table ref, or SQL function ref to search.",
        ),
        (false, false) => (
            "Search generated Coral exports visible in this runtime exposure.",
            "Text, typed ref, or capability id to search.",
        ),
    };
    Tool::new(
        "search",
        description,
        json_object_schema(&json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "query": {
                    "type": "string",
                    "description": query_description
                },
                "source_key": {
                    "type": "string",
                    "description": "Restrict results to one source by its handle (e.g. \"slack\", \"github\")."
                },
                "display_name": { "type": "string" },
                "kind": {
                    "type": "string",
                    "enum": search_kind_enum(exposure)
                },
                "capability_kind": {
                    "type": "string",
                    "enum": ["query", "mutation", "action"]
                },
                "effect": {
                    "type": "string",
                    "enum": ["read", "write", "delete", "unknown"]
                },
                "intent": {
                    "type": "string",
                    "enum": ["read", "write", "any"],
                    "description": "Explicit ranking intent. read orders read capabilities first (the default when the query has no write verbs); write disables that demotion; any forces neutral ordering. Never filters results."
                },
                "expand_top": {
                    "type": "boolean",
                    "default": false,
                    "description": "Embed the top hit's compact describe entry as `top`. Exact-reference matches embed it automatically."
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 20
                },
                "offset": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": u32::MAX,
                    "default": 0
                }
            }
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("Search")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn describe_tool(exposure: McpRuntimeExposure) -> Tool {
    let reference_description = match (exposure.typescript_enabled, exposure.sql_enabled) {
        (true, true) => {
            "Typed ref, capability id, generated tool path, TypeScript path, SQL ref, or unambiguous alias."
        }
        (true, false) => {
            "Typed ref, capability id, generated tool path, TypeScript path, or unambiguous alias."
        }
        (false, true) => "Typed ref, capability id, SQL ref, or unambiguous alias.",
        (false, false) => "Typed ref, capability id, or unambiguous alias.",
    };
    Tool::new(
        "describe",
        "Describe a generated Coral export by typed ref, capability id, generated tool path, or unambiguous alias.",
        json_object_schema(&json!({
            "type": "object",
            "required": ["reference"],
            "properties": {
                "reference": {
                    "type": "string",
                    "description": reference_description
                },
                "view": {
                    "type": "string",
                    "enum": ["compact", "detailed"],
                    "default": "compact",
                    "description": "compact returns refs plus size-bounded JSON input/value schemas; detailed returns full generated provider artifacts."
                },
                "path": {
                    "type": "string",
                    "description": "Dot path into the input schema, e.g. \"filter\" or \"filter.team\"; prefix \"output.\" to address the value schema. Returns only that subtree, fully expanded one level with deeper levels boundable."
                },
                "schemas": {
                    "type": "string",
                    "enum": ["bounded", "full"],
                    "default": "bounded",
                    "description": "bounded caps rendered input/value schemas and marks elided subtrees with x-coral-truncated; full skips renderer-size bounding but cannot recover source/importer-level stubs."
                }
            }
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("Describe")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

fn search_kind_enum(exposure: McpRuntimeExposure) -> Value {
    Value::Array(
        exposure
            .visible_search_kinds()
            .map(|kind| json!(kind))
            .collect(),
    )
}

pub(crate) fn exec_tool() -> Tool {
    Tool::new(
        "exec",
        "Run Code Mode source, waiting briefly for fast completion before returning { run, result, output, cursor }: result is the script's return value, output is joined console text, and cursor appears only while run.status is \"running\" — resume with wait { run_id, cursor }. Optional first line `// @exec: {\"yield_time_ms\": 10000, \"max_output_tokens\": 10000}` tunes how long exec waits before returning a running run and the result-size budget before truncation (truncated results carry result_truncated with the spilled artifact path).",
        json_object_schema(&json!({
            "type": "object",
            "required": ["source"],
            "properties": {
                "source": {
                    "type": "string",
                    "description": "JavaScript Code Mode source. TypeScript declarations in descriptions are documentation-only signatures, not executable syntax."
                }
            }
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("Exec")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(false),
    )
}

pub(crate) fn wait_tool() -> Tool {
    Tool::new(
        "wait",
        "Wait for a Code Mode run by run id, returning the same { run, result, output, cursor } shape as exec or terminating the run. While run.status is \"running\", call wait again with the returned cursor.",
        json_object_schema(&json!({
            "type": "object",
            "required": ["run_id"],
            "properties": {
                "run_id": { "type": "string" },
                "cursor": {
                    "type": "integer",
                    "minimum": 0,
                    "default": 0,
                    "description": "Resume cursor from the previous exec/wait response; only newer output is returned."
                },
                "after_event_id": {
                    "type": "integer",
                    "minimum": 0,
                    "deprecated": true,
                    "description": "Deprecated alias for cursor."
                },
                "terminate": {
                    "type": "boolean",
                    "default": false,
                    "description": "Terminate the run instead of waiting for more output."
                }
            }
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("Wait")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(false),
    )
}

pub(crate) fn feedback_tool() -> Tool {
    Tool::new(
        "feedback",
        "Submit feedback when you are blocked. Coral stores the report locally and uploads an anonymous copy, without user identifiers, to Coral's hosted feedback service to improve Coral's performance.",
        json_object_schema(&json!({
            "type": "object",
            "required": ["trying_to_do", "tried", "stuck"],
            "properties": {
                "trying_to_do": {
                    "type": "string",
                    "description": "What you were trying to do."
                },
                "tried": {
                    "type": "string",
                    "description": "What you already tried."
                },
                "stuck": {
                    "type": "string",
                    "description": "Where you got blocked."
                }
            }
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("Store Feedback Report")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(true),
    )
}

pub(crate) fn search_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<SearchArguments, ErrorData> {
    Ok(SearchArguments {
        query: optional_string_argument(arguments, "query")?.unwrap_or_default(),
        source_key: optional_string_argument(arguments, "source_key")?.unwrap_or_default(),
        display_name: optional_string_argument(arguments, "display_name")?.unwrap_or_default(),
        kind: optional_enum_argument(arguments, "kind", McpRuntimeExposure::ALL_SEARCH_KINDS)?
            .unwrap_or_default(),
        capability_kind: optional_enum_argument(
            arguments,
            "capability_kind",
            &["query", "mutation", "action"],
        )?
        .unwrap_or_default(),
        effect: optional_enum_argument(
            arguments,
            "effect",
            &["read", "write", "delete", "unknown"],
        )?
        .unwrap_or_default(),
        intent: optional_enum_argument(arguments, "intent", &["read", "write", "any"])?
            .unwrap_or_default(),
        expand_top: optional_bool_argument(arguments, "expand_top", false)?,
        pagination: parse_pagination_with_limits(arguments, 20, 100)?,
    })
}

pub(crate) fn describe_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<DescribeArguments, ErrorData> {
    let view = optional_enum_argument(arguments, "view", &["compact", "detailed"])?
        .unwrap_or_else(|| "compact".to_string());
    let schemas = optional_enum_argument(arguments, "schemas", &["bounded", "full"])?
        .as_deref()
        .and_then(SchemaRenderMode::parse)
        .unwrap_or_default();
    Ok(DescribeArguments {
        reference: required_string_argument(arguments, "reference")?,
        view: match view.as_str() {
            "compact" => DescribeView::Compact,
            "detailed" => DescribeView::Detailed,
            _ => unreachable!("optional_enum_argument validates describe view"),
        },
        path: optional_string_argument(arguments, "path")?,
        schemas,
    })
}

pub(crate) fn exec_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<ExecArguments, ErrorData> {
    Ok(ExecArguments {
        source: required_string_argument(arguments, "source")?,
    })
}

pub(crate) fn wait_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<WaitArguments, ErrorData> {
    Ok(WaitArguments {
        run_id: required_string_argument(arguments, "run_id")?,
        cursor: wait_cursor_argument(arguments)?,
        terminate: optional_bool_argument(arguments, "terminate", false)?,
    })
}

fn wait_cursor_argument(arguments: Option<&Map<String, Value>>) -> Result<u64, ErrorData> {
    let cursor = optional_u64_argument_value(arguments, "cursor")?;
    let after_event_id = optional_u64_argument_value(arguments, "after_event_id")?;
    match (cursor, after_event_id) {
        (Some(cursor), Some(after_event_id)) if cursor != after_event_id => {
            Err(ErrorData::invalid_params(
                "arguments 'cursor' and 'after_event_id' must match when both are provided"
                    .to_string(),
                None,
            ))
        }
        (Some(cursor), _) => Ok(cursor),
        (None, Some(after_event_id)) => Ok(after_event_id),
        (None, None) => Ok(0),
    }
}

pub(crate) fn required_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<String, ErrorData> {
    let value = arguments
        .and_then(|arguments| arguments.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ErrorData::invalid_params(format!("missing string argument '{key}'"), None)
        })?;
    Ok(value.to_string())
}

pub(crate) fn build_tool_result(value: Value) -> CallToolResult {
    let mut result = CallToolResult::structured(value);
    result.content.clear();
    result
}

fn optional_string_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<String>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a string"), None)
    })?;
    let value = value.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}

fn optional_enum_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    allowed: &[&str],
) -> Result<Option<String>, ErrorData> {
    let Some(value) = optional_string_argument(arguments, key)? else {
        return Ok(None);
    };
    if allowed.contains(&value.as_str()) {
        Ok(Some(value))
    } else {
        Err(ErrorData::invalid_params(
            format!("argument '{key}' must be one of {}", allowed.join(", ")),
            None,
        ))
    }
}

fn optional_u64_argument_value(
    arguments: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<u64>, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(None);
    };
    value
        .as_u64()
        .ok_or_else(|| {
            ErrorData::invalid_params(
                format!("argument '{key}' must be a non-negative integer"),
                None,
            )
        })
        .map(Some)
}

fn parse_pagination_with_limits(
    arguments: Option<&Map<String, Value>>,
    default_limit: u32,
    max_limit: u32,
) -> Result<Pagination, ErrorData> {
    Ok(Pagination {
        limit: optional_u32_argument(arguments, "limit", default_limit, 1, max_limit)?,
        offset: optional_u32_argument(arguments, "offset", 0, 0, u32::MAX)?,
    })
}

fn optional_u32_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    default: u32,
    min: u32,
    max: u32,
) -> Result<u32, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(default);
    };
    let value = value.as_i64().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be an integer"), None)
    })?;
    if value < i64::from(min) || value > i64::from(max) {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be between {min} and {max}"),
            None,
        ));
    }
    u32::try_from(value).map_err(|_err| {
        ErrorData::invalid_params(
            format!("argument '{key}' must be between {min} and {max}"),
            None,
        )
    })
}

fn optional_bool_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    default: bool,
) -> Result<bool, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(default);
    };
    value.as_bool().ok_or_else(|| {
        ErrorData::invalid_params(format!("argument '{key}' must be a boolean"), None)
    })
}

fn json_object_schema(value: &Value) -> Arc<Map<String, Value>> {
    Arc::new(
        value
            .as_object()
            .expect("tool schema must be a JSON object")
            .clone(),
    )
}

#[cfg(test)]
mod tests {
    use coral_capabilities::SchemaRenderMode;
    use serde_json::json;

    use super::{
        DescribeView, build_tool_result, describe_arguments, describe_tool, exec_tool,
        search_arguments, search_tool, wait_arguments, wait_tool,
    };
    use crate::McpRuntimeExposure;

    #[test]
    fn search_tool_schema_hides_sql_when_sql_exposure_is_disabled() {
        let tool = search_tool(McpRuntimeExposure::typescript_only());
        let rendered = serde_json::to_string(&tool).expect("serialize tool");

        assert!(rendered.contains("\"typescript\""));
        assert!(!rendered.contains("sql_table"));
        assert!(!rendered.contains("sql_function"));
        assert!(!rendered.contains("SQL ref"));
    }

    #[test]
    fn search_tool_schema_includes_sql_kinds_when_sql_exposure_is_enabled() {
        let tool = search_tool(McpRuntimeExposure::both());
        let rendered = serde_json::to_string(&tool).expect("serialize tool");

        assert!(rendered.contains("sql_table"));
        assert!(rendered.contains("sql_function"));
        assert!(rendered.contains("SQL ref"));
    }

    #[test]
    fn describe_tool_schema_hides_sql_refs_when_sql_exposure_is_disabled() {
        let tool = describe_tool(McpRuntimeExposure::typescript_only());
        let rendered = serde_json::to_string(&tool).expect("serialize tool");

        assert!(!rendered.contains("SQL ref"));
        assert!(rendered.contains("\"compact\""));
        assert!(rendered.contains("\"detailed\""));
    }

    #[test]
    fn describe_arguments_default_to_compact_bounded_view() {
        let value = json!({ "reference": "tools.github.rest.search.issues" });
        let arguments = value.as_object().expect("object");
        let parsed = describe_arguments(Some(arguments)).expect("parse describe args");

        assert_eq!(parsed.reference, "tools.github.rest.search.issues");
        assert_eq!(parsed.view, DescribeView::Compact);
        assert_eq!(parsed.path, None);
        assert_eq!(parsed.schemas, SchemaRenderMode::Bounded);
    }

    #[test]
    fn describe_arguments_accept_detailed_view() {
        let value = json!({
            "reference": "tools.github.rest.search.issues",
            "view": "detailed"
        });
        let arguments = value.as_object().expect("object");
        let parsed = describe_arguments(Some(arguments)).expect("parse describe args");

        assert_eq!(parsed.view, DescribeView::Detailed);
    }

    #[test]
    fn describe_arguments_accept_path_and_full_schemas() {
        let value = json!({
            "reference": "tools.github.rest.search.issues",
            "path": "filter.team",
            "schemas": "full"
        });
        let arguments = value.as_object().expect("object");
        let parsed = describe_arguments(Some(arguments)).expect("parse describe args");

        assert_eq!(parsed.path.as_deref(), Some("filter.team"));
        assert_eq!(parsed.schemas, SchemaRenderMode::Full);
    }

    #[test]
    fn describe_arguments_reject_unknown_schemas_value() {
        let value = json!({
            "reference": "tools.github.rest.search.issues",
            "schemas": "everything"
        });
        let arguments = value.as_object().expect("object");
        let error = describe_arguments(Some(arguments)).expect_err("invalid schemas value");

        assert!(error.to_string().contains("schemas"));
    }

    #[test]
    fn exec_tool_schema_accepts_javascript_source_only() {
        let tool = exec_tool();
        let rendered = serde_json::to_string(&tool).expect("serialize tool");

        assert!(rendered.contains("JavaScript Code Mode source"));
        assert!(!rendered.contains("JavaScript or TypeScript"));
    }

    #[test]
    fn wait_tool_schema_mentions_after_event_id_alias() {
        let tool = wait_tool();
        let rendered = serde_json::to_string(&tool).expect("serialize tool");

        assert!(rendered.contains("after_event_id"));
        assert!(rendered.contains("Deprecated alias for cursor"));
    }

    #[test]
    fn wait_arguments_accept_after_event_id_alias() {
        let value = json!({ "run_id": "run-1", "after_event_id": 12 });
        let arguments = value.as_object().expect("object");
        let parsed = wait_arguments(Some(arguments)).expect("parse wait args");

        assert_eq!(parsed.run_id, "run-1");
        assert_eq!(parsed.cursor, 12);
    }

    #[test]
    fn wait_arguments_reject_conflicting_cursor_aliases() {
        let value = json!({ "run_id": "run-1", "cursor": 11, "after_event_id": 12 });
        let arguments = value.as_object().expect("object");
        let error = wait_arguments(Some(arguments))
            .err()
            .expect("conflicting cursors");

        assert!(error.to_string().contains("cursor"));
        assert!(error.to_string().contains("after_event_id"));
    }

    #[test]
    fn build_tool_result_returns_structured_json_without_text_preview() {
        let raw_markdown = "Format dates as ![](slack_date:2026-06-06)";
        let result = build_tool_result(json!({ "description": raw_markdown }));

        assert_eq!(
            result
                .structured_content
                .as_ref()
                .expect("structured content")["description"],
            raw_markdown
        );
        assert!(result.content.is_empty());
    }

    #[test]
    fn search_tool_schema_hides_typescript_when_typescript_exposure_is_disabled() {
        let tool = search_tool(McpRuntimeExposure::sql_only());
        let rendered = serde_json::to_string(&tool).expect("serialize tool");

        assert!(!rendered.contains("\"typescript\""));
        assert!(rendered.contains("sql_table"));
        assert!(rendered.contains("sql_function"));
        assert!(rendered.contains("SQL table ref"));
    }

    #[test]
    fn search_arguments_accept_sql_function_kind() {
        let value = json!({ "kind": "sql_function" });
        let arguments = value.as_object().expect("object");
        let parsed = search_arguments(Some(arguments)).expect("parse search args");

        assert_eq!(parsed.kind, "sql_function");
    }

    #[test]
    fn search_arguments_default_to_no_intent_and_no_expand_top() {
        let value = json!({ "query": "issues" });
        let arguments = value.as_object().expect("object");
        let parsed = search_arguments(Some(arguments)).expect("parse search args");

        assert_eq!(parsed.intent, "");
        assert!(!parsed.expand_top);
    }

    #[test]
    fn search_arguments_accept_intent_and_expand_top() {
        let value = json!({ "query": "issues", "intent": "write", "expand_top": true });
        let arguments = value.as_object().expect("object");
        let parsed = search_arguments(Some(arguments)).expect("parse search args");

        assert_eq!(parsed.intent, "write");
        assert!(parsed.expand_top);
    }

    #[test]
    fn search_arguments_reject_unknown_intent() {
        let value = json!({ "query": "issues", "intent": "destroy" });
        let arguments = value.as_object().expect("object");
        let error = search_arguments(Some(arguments)).expect_err("invalid intent");

        assert!(error.to_string().contains("intent"));
    }
}
