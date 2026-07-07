use rmcp::model::{CallToolResult, Tool};
use serde_json::Value;

use super::catalog::{describe_table_tool, list_catalog_tool, list_columns_tool};
use super::context::ToolDescriptionContext;
use super::feedback::feedback_tool;
use super::search::search_tool;
use super::sql::sql_tool;

pub(crate) fn available_tools(
    context: &ToolDescriptionContext,
    feedback_enabled: bool,
) -> Vec<Tool> {
    let mut tools = vec![
        sql_tool(context),
        search_tool(context),
        list_catalog_tool(context),
        describe_table_tool(),
        list_columns_tool(),
    ];
    if feedback_enabled {
        tools.push(feedback_tool());
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

        let tools = available_tools(&context, false);
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
    }

    #[test]
    fn available_tools_add_feedback_last_when_enabled() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, true);

        assert_eq!(
            tools.last().map(|tool| tool.name.as_ref()),
            Some("feedback")
        );
    }

    #[test]
    fn available_tools_keep_default_order() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());
        let tools = available_tools(&context, false);
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

        let tools = available_tools(&context, false);
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
    fn all_advertised_tools_have_object_input_schemas() {
        let context = ToolDescriptionContext::new(1, 0, Vec::new());

        for tool in available_tools(&context, true) {
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
