//! Tool, resource, guide, and result-shaping helpers for the Coral MCP surface.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;

use coral_api::v1::{Source, Table};
use rmcp::{
    ErrorData,
    model::{AnnotateAble, CallToolResult, Content, RawResource, Resource, Tool, ToolAnnotations},
};
use serde_json::{Map, Value, json};

static INITIAL_INSTRUCTIONS: &str = "You are connected to Coral. Read `coral://guide` for query patterns, use `list_tables` to inspect queryable tables, and use `sql` against `coral.tables` and `coral.columns` for discovery.";
static GUIDE_TEMPLATE: &str = include_str!("guide_template.md");

#[derive(Debug, Clone)]
pub(crate) struct ToolError {
    pub(crate) summary: String,
    pub(crate) detail: String,
    pub(crate) hint: Option<String>,
    pub(crate) grpc_code: Option<String>,
}

pub(crate) fn initial_instructions() -> &'static str {
    INITIAL_INSTRUCTIONS
}

pub(crate) fn sql_tool(sources: &[Source], tables: &[Table]) -> Tool {
    Tool::new(
        "sql",
        sql_tool_description(sources, tables),
        json_object_schema(&json!({
            "type": "object",
            "required": ["sql"],
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A single SQL statement to execute."
                }
            }
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("Run SQL")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(true),
    )
}

pub(crate) fn list_tables_tool(tables: &[Table]) -> Tool {
    Tool::new(
        "list_tables",
        list_tables_description(tables),
        json_object_schema(&json!({
            "type": "object",
            "properties": {}
        })),
    )
    .with_annotations(
        ToolAnnotations::with_title("List Tables")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn guide_resource(sources: &[Source], tables: &[Table]) -> Resource {
    RawResource::new("coral://guide", "guide")
        .with_description(guide_resource_description(sources, tables))
        .with_mime_type("text/markdown")
        .no_annotation()
}

pub(crate) fn tables_resource(tables: &[Table]) -> Resource {
    RawResource::new("coral://tables", "tables")
        .with_description(tables_resource_description(tables))
        .with_mime_type("application/json")
        .no_annotation()
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

pub(crate) fn build_tool_result(value: Value) -> Result<CallToolResult, ErrorData> {
    let pretty = serde_json::to_string_pretty(&value)
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
    let mut result = CallToolResult::structured(value);
    result.content = vec![Content::text(pretty)];
    Ok(result)
}

pub(crate) fn tool_error_result(error: ToolError) -> CallToolResult {
    let mut text = format!("Error: {}\nDetail: {}", error.summary, error.detail);
    if let Some(hint) = &error.hint {
        write!(text, "\nHint: {hint}").expect("writing to String should not fail");
    }

    let mut structured = json!({
        "error": {
            "summary": error.summary,
            "detail": error.detail,
        }
    });
    if let Some(hint) = error.hint
        && let Some(value) = structured
            .get_mut("error")
            .and_then(serde_json::Value::as_object_mut)
    {
        value.insert("hint".to_string(), Value::String(hint));
    }
    if let Some(grpc_code) = error.grpc_code
        && let Some(value) = structured
            .get_mut("error")
            .and_then(serde_json::Value::as_object_mut)
    {
        value.insert("grpc_code".to_string(), Value::String(grpc_code));
    }

    let mut result = CallToolResult::structured_error(structured);
    result.content = vec![Content::text(text)];
    result
}

pub(crate) fn tool_error_from_status(operation: &str, status: &tonic::Status) -> ToolError {
    let (summary, hint) = match status.code() {
        tonic::Code::InvalidArgument => (
            format!("{operation} request is invalid"),
            Some(
                "Check the SQL and retry. Use `coral://guide`, `coral.tables`, and `coral.columns` for discovery.".to_string(),
            ),
        ),
        tonic::Code::NotFound => (
            format!("{operation} target was not found"),
            Some(
                "Confirm the visible source, schema, and table names before retrying.".to_string(),
            ),
        ),
        tonic::Code::FailedPrecondition => (
            format!("{operation} prerequisites are not satisfied"),
            Some(
                "Check source configuration and required filters, then retry.".to_string(),
            ),
        ),
        tonic::Code::Unavailable => (
            format!("{operation} is unavailable"),
            Some("Retry once the local query runtime is available.".to_string()),
        ),
        _ => (format!("{operation} failed"), None),
    };

    ToolError {
        summary,
        detail: status.message().to_string(),
        hint,
        grpc_code: Some(status.code().to_string()),
    }
}

pub(crate) fn status_to_error_data(status: &tonic::Status) -> ErrorData {
    match status.code() {
        tonic::Code::NotFound => ErrorData::resource_not_found(status.message().to_string(), None),
        tonic::Code::InvalidArgument => {
            ErrorData::invalid_params(status.message().to_string(), None)
        }
        _ => ErrorData::internal_error(status.message().to_string(), None),
    }
}

pub(crate) fn internal_status(error: &serde_json::Error) -> tonic::Status {
    tonic::Status::internal(error.to_string())
}

pub(crate) fn guide_resource_content(sources: &[Source], tables: &[Table]) -> String {
    let mut sources_section = String::from("## Available Schemas\n\n");
    sources_section.push_str(
        "- coral: System metadata schema. Use `coral.tables` and `coral.columns` to discover queryable tables, columns, descriptions, and required filters.\n",
    );
    let schemas = tables
        .iter()
        .map(|table| table.schema_name.as_str())
        .collect::<BTreeSet<_>>();
    if schemas.is_empty() {
        if sources.is_empty() {
            sources_section.push_str("\nNo source schemas are currently configured.\n");
        } else {
            sources_section
                .push_str("\nNo query-visible source schemas are currently available.\n");
        }
    } else {
        sources_section.push_str("\nVisible source schemas:\n");
        for schema in schemas {
            let _ = writeln!(sources_section, "- {schema}");
        }
    }

    let columns_example = first_visible_table(tables).map_or_else(
        || {
            "SELECT column_name, data_type, is_required_filter, description \
FROM coral.columns WHERE schema_name = '<schema>' AND table_name = '<table>' ORDER BY ordinal_position;"
                .to_string()
        },
        |(schema_name, table_name)| {
            format!(
                "SELECT column_name, data_type, is_required_filter, description \
FROM coral.columns WHERE schema_name = '{schema_name}' AND table_name = '{table_name}' ORDER BY ordinal_position;"
            )
        },
    );

    GUIDE_TEMPLATE
        .replace("{{SOURCES_SECTION}}", &sources_section)
        .replace("{{COLUMNS_EXAMPLE}}", &columns_example)
}

pub(crate) fn tables_resource_content(tables: &[Table]) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&json!({ "tables": queryable_tables(tables) }))
}

pub(crate) fn list_tables_value(tables: &[Table]) -> Value {
    json!({ "tables": queryable_tables(tables) })
}

fn sql_tool_description(sources: &[Source], tables: &[Table]) -> String {
    if tables.is_empty() {
        format!(
            "Run a SQL query against local Coral sources. {} configured source(s), but no visible SQL schemas are currently available.",
            source_count(sources)
        )
    } else {
        format!(
            "Run a SQL query against local Coral sources. {} visible SQL schema(s) are currently available.",
            schema_count(tables)
        )
    }
}

fn list_tables_description(tables: &[Table]) -> String {
    format!(
        "List queryable fully qualified tables. {} table(s) are currently visible.",
        table_count(tables)
    )
}

fn guide_resource_description(sources: &[Source], tables: &[Table]) -> String {
    format!(
        "Query workflow and schema discovery guidance for {} configured source(s), {} visible schema(s), and {} table(s).",
        source_count(sources),
        schema_count(tables),
        table_count(tables)
    )
}

fn tables_resource_description(tables: &[Table]) -> String {
    format!(
        "Queryable fully qualified Coral tables ({} table(s)).",
        table_count(tables)
    )
}

fn source_count(sources: &[Source]) -> usize {
    sources.len()
}

fn schema_count(tables: &[Table]) -> usize {
    tables
        .iter()
        .map(|table| table.schema_name.as_str())
        .collect::<BTreeSet<_>>()
        .len()
}

fn table_count(tables: &[Table]) -> usize {
    tables.len()
}

fn queryable_tables(tables: &[Table]) -> Vec<Value> {
    let mut summaries = tables
        .iter()
        .map(|table| {
            json!({
                "name": format!("{}.{}", table.schema_name, table.name),
                "description": table.description,
                "required_filters": table.required_filters,
            })
        })
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        left.get("name")
            .and_then(Value::as_str)
            .cmp(&right.get("name").and_then(Value::as_str))
    });
    summaries
}

fn first_visible_table(tables: &[Table]) -> Option<(&str, &str)> {
    tables
        .iter()
        .min_by(|left, right| {
            (&left.schema_name, &left.name).cmp(&(&right.schema_name, &right.name))
        })
        .map(|table| (table.schema_name.as_str(), table.name.as_str()))
}

fn json_object_schema(value: &Value) -> Arc<Map<String, Value>> {
    Arc::new(
        value
            .as_object()
            .cloned()
            .expect("tool schemas should be JSON objects"),
    )
}

#[cfg(test)]
mod tests {
    use coral_api::v1::{Source, Table, Workspace};

    use super::{ToolError, guide_resource_content, tool_error_result};

    fn source(name: &str) -> Source {
        Source {
            workspace: Some(Workspace {
                name: "default".to_string(),
            }),
            name: name.to_string(),
            version: String::new(),
            secrets: Vec::new(),
            variables: Vec::new(),
            origin: 0,
        }
    }

    fn table(schema_name: &str, name: &str) -> Table {
        Table {
            workspace: Some(Workspace {
                name: "default".to_string(),
            }),
            schema_name: schema_name.to_string(),
            name: name.to_string(),
            description: format!("{name} description"),
            columns: Vec::new(),
            required_filters: Vec::new(),
        }
    }

    #[test]
    fn guide_content_renders_placeholder_when_no_schemas_exist() {
        let content = guide_resource_content(&[source("demo")], &[]);
        assert!(content.contains("## Available Schemas"));
        assert!(content.contains("- coral: System metadata schema."));
        assert!(content.contains("No query-visible source schemas are currently available."));
        assert!(content.contains("schema_name = '<schema>'"));
    }

    #[test]
    fn guide_content_groups_visible_tables_by_schema() {
        let content = guide_resource_content(
            &[source("demo")],
            &[table("slack", "channels"), table("slack", "messages")],
        );
        assert!(content.contains("## Available Schemas"));
        assert!(content.contains("- coral: System metadata schema."));
        assert!(content.contains("Visible source schemas:"));
        assert!(content.contains("- slack"));
        assert!(content.contains("Fully qualify tables in SQL, for example `slack.messages`."));
    }

    #[test]
    fn tool_error_result_includes_structured_error_payload() {
        let result = tool_error_result(ToolError {
            summary: "Query failed".to_string(),
            detail: "planner error".to_string(),
            hint: Some("Retry with valid SQL.".to_string()),
            grpc_code: Some("InvalidArgument".to_string()),
        });
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            result.structured_content.expect("structured content")["error"]["grpc_code"],
            "InvalidArgument"
        );
    }
}
