use std::collections::BTreeSet;
use std::fmt::Write as _;

use coral_api::v1::{Source, Table};
use rmcp::model::{AnnotateAble, RawResource, Resource};
use serde_json::{Value, json};

static INITIAL_INSTRUCTIONS: &str = "You are connected to Coral. Read `coral://guide` for query patterns, use `list_tables` to inspect queryable tables, and use `sql` against `coral.tables` and `coral.columns` for discovery.";
static GUIDE_TEMPLATE: &str = include_str!("../guide_template.md");

pub(crate) fn initial_instructions() -> &'static str {
    INITIAL_INSTRUCTIONS
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

pub(crate) fn tables_resource_content(tables: &[Table]) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&json!({ "tables": queryable_tables(tables) }))
}

pub(crate) fn list_tables_value(tables: &[Table]) -> Value {
    json!({ "tables": queryable_tables(tables) })
}

pub(super) fn visible_schema_count(tables: &[Table]) -> usize {
    tables
        .iter()
        .map(|table| table.schema_name.as_str())
        .collect::<BTreeSet<_>>()
        .len()
}

pub(super) fn visible_table_count(tables: &[Table]) -> usize {
    tables.len()
}

fn guide_resource_description(sources: &[Source], tables: &[Table]) -> String {
    format!(
        "Query workflow and schema discovery guidance for {} configured source(s), {} visible schema(s), and {} table(s).",
        sources.len(),
        visible_schema_count(tables),
        visible_table_count(tables)
    )
}

fn tables_resource_description(tables: &[Table]) -> String {
    format!(
        "Queryable fully qualified Coral tables ({} table(s)).",
        visible_table_count(tables)
    )
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

#[cfg(test)]
mod tests {
    use coral_api::v1::{Source, Table, Workspace};

    use super::guide_resource_content;

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
}
