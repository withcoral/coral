use rmcp::model::{CallToolResult, Tool};
use serde_json::Value;

use super::catalog::{describe_table_tool, list_catalog_tool, list_columns_tool};
use super::context::ToolDescriptionContext;
use super::feedback::feedback_tool;
use super::search::search_tool;
use super::sql::sql_tool;
use super::task::{end_task_tool, start_task_tool, with_task_context_arguments};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ToolAvailability {
    pub(crate) tasks_enabled: bool,
    pub(crate) feedback_enabled: bool,
    pub(crate) observed_values_search_enabled: bool,
}

pub(crate) fn available_tools(
    context: &ToolDescriptionContext,
    availability: ToolAvailability,
) -> Vec<Tool> {
    let mut tools = vec![
        sql_tool(context),
        search_tool(context, availability.observed_values_search_enabled),
        list_catalog_tool(context),
        describe_table_tool(),
        list_columns_tool(),
    ];
    if availability.tasks_enabled {
        tools = tools.into_iter().map(with_task_context_arguments).collect();
        tools.push(start_task_tool());
        tools.push(end_task_tool());
    }
    if availability.feedback_enabled {
        let feedback = feedback_tool();
        let feedback = if availability.tasks_enabled {
            with_task_context_arguments(feedback)
        } else {
            feedback
        };
        tools.push(feedback);
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

    use super::{ToolAvailability, ToolDescriptionContext, available_tools, build_tool_result};

    const BASE_TOOLS: ToolAvailability = ToolAvailability {
        tasks_enabled: false,
        feedback_enabled: false,
        observed_values_search_enabled: false,
    };
    const OBSERVED_VALUES_TOOLS: ToolAvailability = ToolAvailability {
        tasks_enabled: false,
        feedback_enabled: false,
        observed_values_search_enabled: true,
    };
    const TASK_TOOLS: ToolAvailability = ToolAvailability {
        tasks_enabled: true,
        feedback_enabled: false,
        observed_values_search_enabled: false,
    };
    const FEEDBACK_TOOLS: ToolAvailability = ToolAvailability {
        tasks_enabled: false,
        feedback_enabled: true,
        observed_values_search_enabled: false,
    };
    const TASK_AND_FEEDBACK_TOOLS: ToolAvailability = ToolAvailability {
        tasks_enabled: true,
        feedback_enabled: true,
        observed_values_search_enabled: false,
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

        let tools = available_tools(&context, OBSERVED_VALUES_TOOLS);
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
        assert!(search_description.contains("does not query connected sources"));
    }

    #[test]
    fn search_tool_advertises_observed_values_only_when_enabled() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let disabled_tools = available_tools(&context, BASE_TOOLS);
        let enabled_tools = available_tools(&context, OBSERVED_VALUES_TOOLS);
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
    fn available_tools_decorate_task_aware_tools() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, TASK_TOOLS);
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
        let tools = available_tools(&context, TASK_AND_FEEDBACK_TOOLS);

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
        let tools = available_tools(&context, BASE_TOOLS);
        let names = tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "sql",
                "search",
                "list_catalog",
                "describe_table",
                "list_columns"
            ]
        );
    }

    #[test]
    fn tool_descriptions_do_not_cap_connected_source_names() {
        let names = (0..14)
            .map(|index| format!("source_{index:02}"))
            .collect::<Vec<_>>();
        let context = ToolDescriptionContext::new(1, 0, names);

        let tools = available_tools(&context, BASE_TOOLS);
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
    fn available_tools_do_not_mutate_base_tool_schemas() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let default_tools = available_tools(&context, BASE_TOOLS);
        let task_tools = available_tools(&context, TASK_TOOLS);

        let default_properties = tool_by_name(&default_tools, "sql")
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("default properties");
        let task_properties = tool_by_name(&task_tools, "sql")
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("task properties");

        assert!(!default_properties.contains_key("task_id"));
        assert!(!default_properties.contains_key("intent"));
        assert!(task_properties.contains_key("task_id"));
        assert!(task_properties.contains_key("intent"));
    }

    #[test]
    fn feedback_tool_is_not_decorated_when_tasks_are_disabled() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, FEEDBACK_TOOLS);
        let feedback = tools.last().expect("feedback tool");
        let properties = feedback
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("feedback properties");

        assert_eq!(feedback.name, "feedback");
        assert!(!properties.contains_key("task_id"));
        assert!(!properties.contains_key("intent"));
    }

    #[test]
    fn all_advertised_tools_have_object_input_schemas() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());

        for tool in available_tools(&context, TASK_AND_FEEDBACK_TOOLS) {
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
