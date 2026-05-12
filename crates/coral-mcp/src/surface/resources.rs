use std::collections::BTreeSet;
use std::fmt::Write as _;

use coral_api::v1::{ListTablesResponse, Source, Table, TableSummary};
use rmcp::model::{AnnotateAble, RawResource, Resource};
use serde_json::{Value, json};

static INITIAL_INSTRUCTIONS: &str = "You are connected to Coral. Read `coral://guide` for query patterns, use `list_tables`, `search_tables`, `describe_table`, and `list_columns` to inspect queryable tables, and use `sql` for final queries.";
static GUIDE_TEMPLATE: &str = include_str!("../guide_template.md");

pub(crate) fn initial_instructions() -> &'static str {
    INITIAL_INSTRUCTIONS
}

pub(crate) fn guide_resource(sources: &[Source], visible_table_count: usize) -> Resource {
    RawResource::new("coral://guide", "guide")
        .with_description(guide_resource_description(sources, visible_table_count))
        .with_mime_type("text/markdown")
        .no_annotation()
}

pub(crate) fn tables_resource(visible_table_count: usize) -> Resource {
    RawResource::new("coral://tables", "tables")
        .with_description(tables_resource_description(visible_table_count))
        .with_mime_type("application/json")
        .no_annotation()
}

pub(crate) fn guide_resource_content(sources: &[Source], tables: &[TableSummary]) -> String {
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
            writeln!(sources_section, "- {schema}").expect("writing to String is infallible");
        }
    }

    let columns_example = first_visible_table(tables).map_or_else(
        || {
            "SELECT column_name, data_type, is_nullable, is_virtual, is_required_filter, description \
FROM coral.columns WHERE schema_name = '<schema>' AND table_name = '<table>' ORDER BY ordinal_position;"
                .to_string()
        },
        |(schema_name, table_name)| {
            format!(
                "SELECT column_name, data_type, is_nullable, is_virtual, is_required_filter, description \
FROM coral.columns WHERE schema_name = '{schema_name}' AND table_name = '{table_name}' ORDER BY ordinal_position;"
            )
        },
    );

    GUIDE_TEMPLATE
        .replace("{{SOURCES_SECTION}}", &sources_section)
        .replace("{{COLUMNS_EXAMPLE}}", &columns_example)
}

pub(crate) fn tables_resource_content(
    tables: &[TableSummary],
) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&json!({ "tables": queryable_tables(tables) }))
}

pub(crate) fn list_tables_value(response: &ListTablesResponse) -> Value {
    let pagination = response.pagination.unwrap_or_default();
    let table_summaries = response_table_summaries(response);
    let mut value = json!({
        "tables": queryable_tables(&table_summaries),
        "total": pagination.total_count,
        "limit": pagination.limit,
        "offset": pagination.offset,
        "has_more": pagination.has_more,
    });
    if pagination.has_more {
        value
            .as_object_mut()
            .expect("list tables value is initialized as a JSON object")
            .insert("next_offset".to_string(), json!(pagination.next_offset));
    }
    value
}

fn guide_resource_description(sources: &[Source], visible_table_count: usize) -> String {
    format!(
        "Query workflow and schema discovery guidance for {} configured source(s) and {} visible table(s).",
        sources.len(),
        visible_table_count
    )
}

fn tables_resource_description(visible_table_count: usize) -> String {
    format!("Queryable fully qualified Coral tables ({visible_table_count} table(s)).")
}

fn queryable_tables(tables: &[TableSummary]) -> Vec<Value> {
    let mut summaries = tables
        .iter()
        .map(|table| {
            json!({
                "schema_name": table.schema_name,
                "table_name": table.name,
                "name": format!("{}.{}", table.schema_name, table.name),
                "sql_reference": format_schema_table_equivalent(&table.schema_name, &table.name),
                "description": table.description,
                "guide": table.guide,
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

fn response_table_summaries(response: &ListTablesResponse) -> Vec<TableSummary> {
    if response.table_summaries.is_empty() {
        response.tables.iter().map(table_to_summary).collect()
    } else {
        response.table_summaries.clone()
    }
}

fn table_to_summary(table: &Table) -> TableSummary {
    TableSummary {
        workspace: table.workspace.clone(),
        schema_name: table.schema_name.clone(),
        name: table.name.clone(),
        description: table.description.clone(),
        required_filters: table.required_filters.clone(),
        guide: table.guide.clone(),
    }
}

fn first_visible_table(tables: &[TableSummary]) -> Option<(&str, &str)> {
    tables
        .iter()
        .min_by(|left, right| {
            (&left.schema_name, &left.name).cmp(&(&right.schema_name, &right.name))
        })
        .map(|table| (table.schema_name.as_str(), table.name.as_str()))
}

pub(crate) fn format_schema_table_equivalent(schema_name: &str, table_name: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier_if_needed(schema_name),
        quote_identifier_if_needed(table_name)
    )
}

fn quote_identifier_if_needed(identifier: &str) -> String {
    if identifier_needs_quotes(identifier) {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    } else {
        identifier.to_string()
    }
}

fn identifier_needs_quotes(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return true;
    };
    if !(first.is_ascii_lowercase() || first == '_') {
        return true;
    }
    !chars.all(|char| char.is_ascii_lowercase() || char.is_ascii_digit() || char == '_')
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "JSON shape assertions intentionally fail loudly in tests"
    )]

    use coral_api::v1::{ListTablesResponse, PaginationResponse, Source, TableSummary, Workspace};
    use serde_json::json;

    use super::{format_schema_table_equivalent, guide_resource_content, list_tables_value};

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

    fn table(schema_name: &str, name: &str) -> TableSummary {
        TableSummary {
            workspace: Some(Workspace {
                name: "default".to_string(),
            }),
            schema_name: schema_name.to_string(),
            name: name.to_string(),
            description: format!("{name} description"),
            required_filters: Vec::new(),
            guide: format!("Query {name}."),
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
        assert!(
            content.contains(
                "Use each table's `sql_reference` from `list_tables` or `coral://tables`"
            )
        );
    }

    #[test]
    fn list_tables_value_includes_compatible_name_and_sql_reference() {
        let value = list_tables_value(&ListTablesResponse {
            tables: Vec::new(),
            pagination: Some(PaginationResponse {
                total_count: 1,
                limit: 50,
                offset: 0,
                has_more: false,
                next_offset: 0,
            }),
            table_summaries: vec![table("local_messages", "events")],
        });

        assert_eq!(value["tables"][0]["name"], "local_messages.events");
        assert_eq!(value["tables"][0]["sql_reference"], "local_messages.events");
        assert_eq!(
            value["tables"][0],
            json!({
                "schema_name": "local_messages",
                "table_name": "events",
                "name": "local_messages.events",
                "sql_reference": "local_messages.events",
                "description": "events description",
                "guide": "Query events.",
                "required_filters": [],
            })
        );
        assert_eq!(value["total"], 1);
        assert_eq!(value["limit"], 50);
        assert_eq!(value["offset"], 0);
        assert_eq!(value["has_more"], false);
    }

    #[test]
    fn sql_reference_quotes_each_identifier_independently() {
        assert_eq!(
            format_schema_table_equivalent("github", "pulls"),
            "github.pulls"
        );
        assert_eq!(
            format_schema_table_equivalent("github", "Pull.Requests"),
            "github.\"Pull.Requests\""
        );
        assert_eq!(
            format_schema_table_equivalent("git.hub", "pulls"),
            "\"git.hub\".pulls"
        );
        assert_eq!(
            format_schema_table_equivalent("git\"hub", "pulls"),
            "\"git\"\"hub\".pulls"
        );
    }
}
