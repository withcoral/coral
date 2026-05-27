use std::collections::BTreeSet;
use std::fmt::Write as _;

use coral_api::v1::{CatalogSourceSummary, TableSummary};
use rmcp::model::{AnnotateAble, RawResource, Resource};
use serde::Serialize;
use serde_json::Value;

use super::values::queryable_table_summary_values;

static INITIAL_INSTRUCTIONS: &str = "You are connected to Coral, a read-only SQL database. Treat exposed data as database schemas, tables, and table functions. Read `coral://catalog` when you need a compact all-source index with source context, tables, and table functions. Use source context from `list_catalog`, `search_catalog`, `coral://catalog`, `coral://guide`, or `coral.sources` before probing provider identity, auth scope, or source-specific search semantics. Use `list_catalog` and `search_catalog` as catalog helpers, use `search_columns` when you know a field but not the table, use `describe_table` and `list_columns` for table-specific metadata, use `sql` against `coral.sources`, `coral.tables`, `coral.columns`, `coral.filters`, `coral.table_functions`, and `coral.inputs` for deeper discovery, then answer with set-based SQL through `sql`. Prefer one SQL statement with joins, CROSS JOIN, CTEs, subqueries, and aggregates over row-by-row tool calls.";
static GUIDE_TEMPLATE: &str = include_str!("../guide_template.md");

pub(crate) fn initial_instructions() -> &'static str {
    INITIAL_INSTRUCTIONS
}

pub(crate) fn guide_resource() -> Resource {
    RawResource::new("coral://guide", "guide")
        .with_description(guide_resource_description())
        .with_mime_type("text/markdown")
        .no_annotation()
}

pub(crate) fn tables_resource() -> Resource {
    RawResource::new("coral://tables", "tables")
        .with_description(tables_resource_description())
        .with_mime_type("application/json")
        .no_annotation()
}

pub(crate) fn catalog_resource() -> Resource {
    RawResource::new("coral://catalog", "catalog")
        .with_description(catalog_resource_description())
        .with_mime_type("application/json")
        .no_annotation()
}

pub(crate) fn guide_resource_content(
    sources: &[CatalogSourceSummary],
    tables: &[TableSummary],
    table_function_schema_names: &[String],
) -> String {
    let mut sources_section = String::from("## Available Schemas\n\n");
    sources_section.push_str(
        "- coral: System catalog schema. Query `coral.tables`, `coral.columns`, `coral.filters`, `coral.table_functions`, and `coral.inputs` like database catalog tables to discover queryable tables, table functions, columns, and filter metadata.\n",
    );
    let mut schemas = tables
        .iter()
        .map(|table| table.schema_name.as_str())
        .collect::<BTreeSet<_>>();
    schemas.extend(table_function_schema_names.iter().map(String::as_str));
    if schemas.is_empty() {
        if sources.is_empty() {
            sources_section.push_str("\nNo user schemas are currently configured.\n");
        } else {
            sources_section.push_str("\nNo user-visible schemas are currently available.\n");
        }
    } else {
        sources_section.push_str("\nVisible schemas:\n");
        for schema in schemas {
            writeln!(sources_section, "- {schema}").expect("writing to String is infallible");
        }
    }
    let mut onboarding_sources = sources
        .iter()
        .filter_map(|source| {
            let instructions = source.onboarding_instructions.trim();
            (!instructions.is_empty()).then_some((source.schema_name.as_str(), instructions))
        })
        .collect::<Vec<_>>();
    onboarding_sources.sort_by(|left, right| left.0.cmp(right.0));
    if !onboarding_sources.is_empty() {
        sources_section.push_str("\nSource-authored context:\n");
        for (schema, instructions) in onboarding_sources {
            writeln!(sources_section, "- {schema}: {instructions}")
                .expect("writing to String is infallible");
        }
    }

    let columns_example = first_visible_table(tables).map_or_else(
        || {
            "SELECT column_name, data_type, is_nullable, is_virtual, is_required_filter, filter_mode, description \
FROM coral.columns WHERE schema_name = '<schema>' AND table_name = '<table>' ORDER BY ordinal_position;"
                .to_string()
        },
        |(schema_name, table_name)| {
            format!(
                "SELECT column_name, data_type, is_nullable, is_virtual, is_required_filter, filter_mode, description \
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

fn guide_resource_description() -> &'static str {
    "Database workflow and catalog discovery guidance generated from currently configured sources when read."
}

fn tables_resource_description() -> &'static str {
    "JSON summaries of fully qualified database tables generated from currently configured sources when read."
}

fn catalog_resource_description() -> &'static str {
    "JSON source context plus database table and table-function summaries generated from currently configured sources when read."
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
    use coral_api::v1::{CatalogSourceSummary, TableSummary, Workspace};

    use super::{guide_resource_content, initial_instructions};
    use crate::surface::values::format_schema_table_equivalent;

    fn source(name: &str) -> CatalogSourceSummary {
        CatalogSourceSummary {
            schema_name: name.to_string(),
            description: String::new(),
            onboarding_instructions: String::new(),
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
    fn initial_instructions_frame_coral_as_sql_database() {
        let instructions = initial_instructions();
        assert!(instructions.contains("read-only SQL database"));
        assert!(instructions.contains("catalog helpers"));
        assert!(instructions.contains("CROSS JOIN"));
        assert!(instructions.contains("row-by-row tool calls"));
    }

    #[test]
    fn guide_content_renders_placeholder_when_no_schemas_exist() {
        let content = guide_resource_content(&[source("demo")], &[], &[]);
        assert!(content.contains("## Available Schemas"));
        assert!(content.contains("- coral: System catalog schema."));
        assert!(content.contains("No user-visible schemas are currently available."));
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
        assert!(content.contains("- coral: System catalog schema."));
        assert!(content.contains("Visible schemas:"));
        assert!(content.contains("- slack"));
        assert!(
            content.contains(
                "Use each table's `sql_reference` from `list_catalog`, `coral://catalog`, or `coral://tables`"
            )
        );
    }

    #[test]
    fn guide_content_includes_function_only_schemas() {
        let function_schemas = vec!["searchy".to_string()];

        let content = guide_resource_content(&[source("searchy")], &[], &function_schemas);

        assert!(content.contains("Visible schemas:"));
        assert!(content.contains("- searchy"));
        assert!(!content.contains("No user-visible schemas are currently available."));
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
