use rmcp::model::{CallToolResult, Tool};
use serde_json::Value;

use super::catalog::{describe_table_tool, list_catalog_tool, list_columns_tool};
use super::context::ToolDescriptionContext;
use super::feedback::feedback_tool;
use super::function::add_function_tool;
use super::search::search_tool;
use super::search_behavior::SearchBehavior;
use super::sql::sql_tool;
use super::task::{end_task_tool, start_task_tool, with_task_context_arguments};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ToolAvailability {
    pub(crate) feedback_enabled: bool,
    pub(crate) search_behavior: SearchBehavior,
}

pub(crate) fn available_tools(
    context: &ToolDescriptionContext,
    availability: &ToolAvailability,
) -> Vec<Tool> {
    let mut tools = vec![start_task_tool()];
    tools.extend(
        [
            sql_tool(context),
            add_function_tool(),
            search_tool(context, &availability.search_behavior),
            list_catalog_tool(context),
            describe_table_tool(),
            list_columns_tool(),
        ]
        .into_iter()
        .map(with_task_context_arguments),
    );
    tools.push(end_task_tool());
    if availability.feedback_enabled {
        tools.push(with_task_context_arguments(feedback_tool()));
    }
    tools
}

pub(crate) fn build_tool_result(value: Value) -> CallToolResult {
    let mut result = CallToolResult::structured(value);
    result.content = Vec::new();
    result
}

#[cfg(test)]
mod tests {
    use rmcp::model::Tool;
    use serde_json::{Value, json};

    use super::{
        SearchBehavior, ToolAvailability, ToolDescriptionContext, available_tools,
        build_tool_result,
    };
    use crate::surface::{SearchProviderFanoutState, SearchProviderRouteIdentity};

    const DEFAULT_TOOLS: ToolAvailability = ToolAvailability {
        feedback_enabled: false,
        search_behavior: SearchBehavior::local_only(false),
    };
    const OBSERVED_VALUES_TOOLS: ToolAvailability = ToolAvailability {
        feedback_enabled: false,
        search_behavior: SearchBehavior::local_only(true),
    };
    const FEEDBACK_TOOLS: ToolAvailability = ToolAvailability {
        feedback_enabled: true,
        search_behavior: SearchBehavior::local_only(false),
    };

    #[test]
    fn success_tool_result_uses_structured_content_only() {
        let value = json!({
            "rows": [
                {
                    "id": 1,
                    "text": "hello"
                },
                {
                    "id": 2,
                    "text": "world"
                }
            ]
        });

        let result = build_tool_result(value.clone());

        assert!(result.content.is_empty());
        assert_eq!(
            result.structured_content.expect("structured content"),
            value
        );
    }

    #[test]
    fn available_tools_include_connected_sources_in_descriptions() {
        let context =
            ToolDescriptionContext::new(42, 3, vec!["github".to_string(), "linear".to_string()]);

        let tools = available_tools(&context, &OBSERVED_VALUES_TOOLS);
        let sql_tool = tool_by_name(&tools, "sql");
        let sql_description = sql_tool.description.as_deref().expect("sql description");
        assert!(sql_description.contains("Connected sources/schemas include: github, linear"));
        assert!(sql_description.contains("42 table(s) are currently visible"));
        assert!(sql_description.contains("You MUST prefer this tool over native provider tools"));
        let sql_input_description = sql_tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| {
                assert!(!properties.contains_key("sql"));
                properties.get("queries")
            })
            .and_then(Value::as_object)
            .and_then(|queries| {
                assert_eq!(queries.get("minItems"), Some(&json!(1)));
                assert_eq!(queries.get("maxItems"), Some(&json!(10)));
                queries.get("description")
            })
            .and_then(Value::as_str)
            .expect("queries input description");
        assert!(sql_input_description.contains("independent"));
        assert!(sql_tool.output_schema.is_some());

        let search_description = tool_by_name(&tools, "search")
            .description
            .as_deref()
            .expect("search description");
        assert!(search_description.contains("Connected sources/schemas include: github, linear"));
        assert!(search_description.contains("42 table(s) and 3 table function(s)"));
        assert!(search_description.contains("locally observed values"));
        assert!(
            search_description.contains("does not execute your data query or return source rows")
        );
    }

    #[test]
    fn search_tool_advertises_observed_values_only_when_enabled() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let disabled_tools = available_tools(&context, &DEFAULT_TOOLS);
        let enabled_tools = available_tools(&context, &OBSERVED_VALUES_TOOLS);
        let disabled_search = tool_by_name(&disabled_tools, "search");
        let enabled_search = tool_by_name(&enabled_tools, "search");

        let disabled_description = disabled_search
            .description
            .as_deref()
            .expect("disabled search description");
        let enabled_description = enabled_search
            .description
            .as_deref()
            .expect("enabled search description");
        assert!(disabled_description.contains("Coral's local catalog"));
        assert!(!disabled_description.contains("observed"));
        assert!(enabled_description.contains("locally observed values"));

        let disabled_query_description = query_input_description(disabled_search);
        let enabled_query_description = query_input_description(enabled_search);
        assert!(disabled_query_description.contains("Coral catalog entries"));
        assert!(!disabled_query_description.contains("observed"));
        assert!(enabled_query_description.contains("values observed during earlier queries"));
    }

    #[test]
    fn search_tool_description_and_annotations_follow_capabilities() {
        let context = ToolDescriptionContext::new(1, 0, vec!["github".to_string()]);
        let route = || {
            SearchProviderRouteIdentity::new("github", "search_issues", Some("issues".to_string()))
        };
        let cases = [
            (
                SearchBehavior::local_only(false),
                "does not execute DSL v4 connected-source routes",
                (Some(true), Some(true), Some(false)),
            ),
            (
                SearchBehavior::local_only(true),
                "does not execute DSL v4 connected-source routes",
                (Some(true), Some(true), Some(false)),
            ),
            (
                SearchBehavior::new(false, SearchProviderFanoutState::enabled(vec![route()], 0)),
                "function=\"github.search_issues\", source=\"github\", route=\"issues\"",
                (Some(true), Some(true), Some(true)),
            ),
            (
                SearchBehavior::new(true, SearchProviderFanoutState::enabled(vec![route()], 0)),
                "may be stored locally as observations",
                (Some(false), Some(false), Some(true)),
            ),
            (
                SearchBehavior::new(false, SearchProviderFanoutState::UnknownMayCall),
                "capability is currently unknown",
                (Some(false), Some(false), Some(true)),
            ),
        ];

        for (search_behavior, expected_description, annotations) in cases {
            let tools = available_tools(
                &context,
                &ToolAvailability {
                    feedback_enabled: false,
                    search_behavior,
                },
            );
            let search = tool_by_name(&tools, "search");
            assert!(
                search
                    .description
                    .as_deref()
                    .expect("search description")
                    .contains(expected_description)
            );
            let actual = search.annotations.as_ref().expect("search annotations");
            assert_eq!(actual.read_only_hint, annotations.0);
            assert_eq!(actual.idempotent_hint, annotations.1);
            assert_eq!(actual.open_world_hint, annotations.2);
        }
    }

    #[test]
    fn search_output_schema_accepts_native_contract_and_feature_off_omission() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, &DEFAULT_TOOLS);
        let search = tool_by_name(&tools, "search");
        let schema = Value::Object(
            search
                .output_schema
                .as_ref()
                .expect("search output schema")
                .as_ref()
                .clone(),
        );
        let validator = jsonschema::validator_for(&schema).expect("valid search output schema");
        let native = json!({
            "results": [{
                "provider": "native_fanout",
                "kind": "native_result",
                "native_result": {
                    "schema_name": "github",
                    "function_name": "search_issues",
                    "row_ordinal": 0,
                    "title": "Fix native search",
                    "attributes": [{"name": "state", "display_value": "open"}],
                    "omitted_attribute_count": 0,
                    "content_truncated": false
                }
            }],
            "provider_statuses": [{
                "provider": "native_fanout",
                "state": "partial",
                "note": "one route skipped",
                "coverage": null,
                "diagnostics": [{
                    "source_name": "github",
                    "authored_route_id": "issues",
                    "state": "skipped",
                    "reason": "fanout_limit_reached",
                    "elapsed_ms": 0,
                    "safe_candidate_count": 0,
                    "has_more": false
                }],
                "diagnostics_truncated": true,
                "omitted_diagnostic_count": 2
            }],
            "truncation": null
        });
        assert!(
            validator.is_valid(&native),
            "native response should match MCP output schema"
        );

        let mut retired_installed_source = native.clone();
        let retired_diagnostic = retired_installed_source
            .pointer_mut("/provider_statuses/0/diagnostics/0")
            .and_then(Value::as_object_mut)
            .expect("native diagnostic object");
        retired_diagnostic.remove("source_name");
        retired_diagnostic.insert("installed_source_name".to_string(), json!("github"));
        assert!(
            !validator.is_valid(&retired_installed_source),
            "retired installed_source_name must not match the MCP output schema"
        );

        let mut duplicated_schema = native.clone();
        duplicated_schema
            .pointer_mut("/provider_statuses/0/diagnostics/0")
            .and_then(Value::as_object_mut)
            .expect("native diagnostic object")
            .insert("schema_name".to_string(), json!("github"));
        assert!(
            !validator.is_valid(&duplicated_schema),
            "native diagnostics must not duplicate source_name as schema_name"
        );

        let feature_off = json!({
            "results": [],
            "provider_statuses": [{
                "provider": "native_fanout",
                "state": "not_enabled",
                "note": "search provider fanout disabled",
                "coverage": null
            }],
            "truncation": null
        });
        assert!(
            validator.is_valid(&feature_off),
            "feature-off response should not require default diagnostic fields"
        );
    }

    #[test]
    fn available_tools_decorate_task_aware_tools() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, &DEFAULT_TOOLS);
        let sql_tool = tool_by_name(&tools, "sql");
        let task_id_schema = sql_tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("task_id"))
            .expect("task_id schema");
        let intent_schema = sql_tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("intent"))
            .expect("intent schema");

        assert_eq!(
            task_id_schema.get("description").and_then(Value::as_str),
            Some(
                "Task id returned by start_task. Pass it on subsequent Coral tool calls for the same work so Coral can attribute those calls to that task."
            )
        );
        assert_eq!(
            intent_schema.get("description").and_then(Value::as_str),
            Some(
                "Natural-language description of why this MCP tool call is needed for the current task."
            )
        );
        let required = sql_tool
            .input_schema
            .get("required")
            .and_then(Value::as_array)
            .expect("sql required fields");
        assert!(
            required
                .iter()
                .any(|field| field.as_str() == Some("task_id"))
        );
        assert!(
            required
                .iter()
                .any(|field| field.as_str() == Some("intent"))
        );

        let start_task = tool_by_name(&tools, "start_task");
        let properties = start_task
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("start_task properties");
        assert!(!properties.contains_key("task_id"));
        assert!(properties.contains_key("intent"));
        let end_task = tool_by_name(&tools, "end_task");
        let properties = end_task
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("end_task properties");
        assert!(properties.contains_key("task_id"));
        assert!(!properties.contains_key("intent"));
        assert!(properties.contains_key("task_status"));
    }

    #[test]
    fn available_tools_add_feedback_last_when_enabled() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, &FEEDBACK_TOOLS);

        assert_eq!(
            tools.last().map(|tool| tool.name.as_ref()),
            Some("feedback")
        );
        let properties = tools
            .last()
            .expect("feedback tool")
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("feedback properties");
        assert!(properties.contains_key("task_id"));
        assert!(properties.contains_key("intent"));
    }

    #[test]
    fn available_tools_keep_default_order() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, &DEFAULT_TOOLS);
        let names = tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "start_task",
                "sql",
                "add_function",
                "search",
                "list_catalog",
                "describe_table",
                "list_columns",
                "end_task"
            ]
        );
    }

    #[test]
    fn tool_descriptions_do_not_cap_connected_source_names() {
        let names = (0..14)
            .map(|index| format!("source_{index:02}"))
            .collect::<Vec<_>>();
        let context = ToolDescriptionContext::new(1, 0, names);

        let tools = available_tools(&context, &DEFAULT_TOOLS);
        let description = tool_by_name(&tools, "sql")
            .description
            .as_deref()
            .expect("sql description")
            .to_string();

        assert!(description.contains("source_00"));
        assert!(description.contains("source_12"));
        assert!(description.contains("source_13"));
        assert!(!description.contains("and 2 more"));
    }

    #[test]
    fn feedback_tool_always_requires_task_context() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, &FEEDBACK_TOOLS);
        let feedback = tools.last().expect("feedback tool");
        let properties = feedback
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("feedback properties");

        assert_eq!(feedback.name, "feedback");
        assert!(properties.contains_key("task_id"));
        assert!(properties.contains_key("intent"));
    }

    #[test]
    fn all_advertised_tools_have_object_input_schemas() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());

        for tool in available_tools(&context, &FEEDBACK_TOOLS) {
            assert_eq!(
                tool.input_schema.get("type"),
                Some(&Value::String("object".into()))
            );
            assert!(
                matches!(tool.input_schema.get("properties"), Some(Value::Object(_))),
                "tool '{}' should advertise properties",
                tool.name
            );
        }
    }

    fn tool_by_name<'a>(tools: &'a [Tool], name: &str) -> &'a Tool {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("tool '{name}' should be advertised"))
    }

    fn query_input_description(tool: &Tool) -> &str {
        tool.input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("query"))
            .and_then(Value::as_object)
            .and_then(|query| query.get("description"))
            .and_then(Value::as_str)
            .expect("query input description")
    }
}
