use rmcp::model::{CallToolResult, Tool};
use serde_json::Value;

use super::catalog::{
    describe_table_tool, list_catalog_tool, list_columns_tool, search_catalog_tool,
};
use super::context::ToolDescriptionContext;
use super::episode::{open_episode_tool, with_episode_id_argument};
use super::feedback::feedback_tool;
use super::graph::{cypher_tool, describe_graph_tool, find_relationship_paths_tool};
use super::sql::sql_tool;

pub(crate) fn available_tools(
    context: &ToolDescriptionContext,
    episodes_enabled: bool,
    feedback_enabled: bool,
    graph_enabled: bool,
) -> Vec<Tool> {
    let mut tools = Vec::new();
    if graph_enabled {
        tools.extend([
            describe_graph_tool(),
            find_relationship_paths_tool(),
            cypher_tool(),
        ]);
    }
    tools.extend([
        sql_tool(context),
        list_catalog_tool(context),
        search_catalog_tool(context),
        describe_table_tool(),
        list_columns_tool(),
    ]);
    if episodes_enabled {
        tools = tools.into_iter().map(with_episode_id_argument).collect();
        tools.push(open_episode_tool());
    }
    if feedback_enabled {
        let feedback = feedback_tool();
        let feedback = if episodes_enabled {
            with_episode_id_argument(feedback)
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

    use super::{ToolDescriptionContext, available_tools, build_tool_result};

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

        let tools = available_tools(&context, false, false, false);
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

        let search_description = tool_by_name(&tools, "search_catalog")
            .description
            .as_deref()
            .expect("search description");
        assert!(search_description.contains("Connected sources/schemas include: github, linear"));
        assert!(search_description.contains("42 table(s) and 3 table function(s)"));
    }

    #[test]
    fn available_tools_decorate_episode_aware_tools() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, true, false, false);
        let sql_tool = tool_by_name(&tools, "sql");
        let episode_id_schema = sql_tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("episode_id"))
            .expect("episode_id schema");

        assert_eq!(
            episode_id_schema.get("description").and_then(Value::as_str),
            Some(
                "Optional episode id returned by open_episode. Pass it on subsequent Coral tool calls for the same task so Coral can attribute the call to that episode."
            )
        );

        let open_episode = tool_by_name(&tools, "open_episode");
        let properties = open_episode
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("open_episode properties");
        assert!(!properties.contains_key("episode_id"));
    }

    #[test]
    fn available_tools_add_feedback_last_when_enabled() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, true, true, false);

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
        assert!(properties.contains_key("episode_id"));
    }

    #[test]
    fn available_tools_keep_default_order() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, false, false, false);
        let names = tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "sql",
                "list_catalog",
                "search_catalog",
                "describe_table",
                "list_columns"
            ]
        );
    }

    #[test]
    fn available_tools_add_graph_surface_before_sql_when_enabled() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, false, false, true);
        let names = tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "describe_graph",
                "find_relationship_paths",
                "cypher",
                "sql",
                "list_catalog",
                "search_catalog",
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

        let tools = available_tools(&context, false, false, false);
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
        let default_tools = available_tools(&context, false, false, false);
        let episode_tools = available_tools(&context, true, false, false);

        let default_properties = tool_by_name(&default_tools, "sql")
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("default properties");
        let episode_properties = tool_by_name(&episode_tools, "sql")
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("episode properties");

        assert!(!default_properties.contains_key("episode_id"));
        assert!(episode_properties.contains_key("episode_id"));
    }

    #[test]
    fn feedback_tool_is_not_decorated_when_episodes_are_disabled() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, false, true, false);
        let feedback = tools.last().expect("feedback tool");
        let properties = feedback
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("feedback properties");

        assert_eq!(feedback.name, "feedback");
        assert!(!properties.contains_key("episode_id"));
    }

    #[test]
    fn all_advertised_tools_have_object_input_schemas() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());

        for tool in available_tools(&context, true, true, false) {
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
}
