use std::collections::BTreeSet;
use std::fmt::Write as _;

use coral_api::v1::{Source, TableSummary};
use rmcp::model::{AnnotateAble, RawResource, Resource};
use serde::Serialize;
use serde_json::Value;

use super::values::queryable_table_summary_values;

static INITIAL_INSTRUCTIONS: &str = "You are connected to Coral. Read `coral://guide` for query patterns, use `list_catalog`, `search_catalog`, `describe_table`, and `list_columns` to inspect queryable tables and source-scoped table functions, and use `sql` for final queries.";
static GUIDE_TEMPLATE: &str = include_str!("../guide_template.md");

pub(crate) fn initial_instructions() -> &'static str {
    INITIAL_INSTRUCTIONS
}

pub(crate) fn guide_resource(
    sources: &[Source],
    visible_table_count: usize,
    visible_function_count: usize,
) -> Resource {
    RawResource::new("coral://guide", "guide")
        .with_description(guide_resource_description(
            sources,
            visible_table_count,
            visible_function_count,
        ))
        .with_mime_type("text/markdown")
        .no_annotation()
}

pub(crate) fn tables_resource(visible_table_count: usize) -> Resource {
    RawResource::new("coral://tables", "tables")
        .with_description(tables_resource_description(visible_table_count))
        .with_mime_type("application/json")
        .no_annotation()
}

pub(crate) fn guide_resource_content(
    sources: &[Source],
    tables: &[TableSummary],
    table_function_schema_names: &[String],
) -> String {
    let mut sources_section = String::from("## Available Schemas\n\n");
    sources_section.push_str(
        "- coral: System metadata schema. Use `coral.tables`, `coral.columns`, and `coral.table_functions` to discover queryable tables, source-scoped table functions, columns, descriptions, and required filters.\n",
    );
    let mut schemas = tables
        .iter()
        .map(|table| table.schema_name.as_str())
        .collect::<BTreeSet<_>>();
    schemas.extend(table_function_schema_names.iter().map(String::as_str));
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
    serde_json::to_string_pretty(&TablesResourceContent {
        tables: queryable_table_summary_values(tables),
    })
}

#[derive(Serialize)]
struct TablesResourceContent {
    tables: Vec<Value>,
}

fn guide_resource_description(
    sources: &[Source],
    visible_table_count: usize,
    visible_function_count: usize,
) -> String {
    format!(
        "Query workflow and schema discovery guidance for {} configured source(s), {} visible table(s), and {} visible table function(s).",
        sources.len(),
        visible_table_count,
        visible_function_count
    )
}

fn tables_resource_description(visible_table_count: usize) -> String {
    format!("Queryable fully qualified Coral tables ({visible_table_count} table(s)).")
}

fn first_visible_table(tables: &[TableSummary]) -> Option<(&str, &str)> {
    tables
        .iter()
        .min_by(|left, right| {
            (&left.schema_name, &left.name).cmp(&(&right.schema_name, &right.name))
        })
        .map(|table| (table.schema_name.as_str(), table.name.as_str()))
}

#[cfg(test)]
mod tests {
    use coral_api::v1::{Source, TableSummary, Workspace};

    use super::guide_resource_content;
    use crate::surface::values::format_schema_table_equivalent;

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
        let content = guide_resource_content(&[source("demo")], &[], &[]);
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
            &[],
        );
        assert!(content.contains("## Available Schemas"));
        assert!(content.contains("- coral: System metadata schema."));
        assert!(content.contains("Visible source schemas:"));
        assert!(content.contains("- slack"));
        assert!(
            content.contains(
                "Use each table's `sql_reference` from `list_catalog` or `coral://tables`"
            )
        );
    }

    #[test]
    fn guide_content_includes_function_only_schemas() {
        let function_schemas = vec!["searchy".to_string()];

        let content = guide_resource_content(&[source("searchy")], &[], &function_schemas);

        assert!(content.contains("Visible source schemas:"));
        assert!(content.contains("- searchy"));
        assert!(!content.contains("No query-visible source schemas are currently available."));
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
