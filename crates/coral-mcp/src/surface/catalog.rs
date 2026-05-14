use std::sync::Arc;

use coral_api::v1::TableSummary as ProtoTableSummary;
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::{
    Pagination, format_schema_table_equivalent, json_object_schema, page_items,
    paged_serialized_value,
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum CatalogItem {
    Table {
        schema_name: String,
        name: String,
        sql_reference: String,
        description: String,
        table: CatalogTableDetails,
    },
}

impl CatalogItem {
    pub(crate) fn from_table(table: &ProtoTableSummary) -> Self {
        Self::Table {
            schema_name: table.schema_name.clone(),
            name: format!("{}.{}", table.schema_name, table.name),
            sql_reference: format_schema_table_equivalent(&table.schema_name, &table.name),
            description: table.description.clone(),
            table: CatalogTableDetails {
                table_name: table.name.clone(),
                guide: table.guide.clone(),
                required_filters: table.required_filters.clone(),
            },
        }
    }

    fn sort_key(&self) -> String {
        match self {
            Self::Table {
                schema_name, name, ..
            } => format!("{schema_name}\0{name}\0table"),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct CatalogTableDetails {
    table_name: String,
    guide: String,
    required_filters: Vec<String>,
}

pub(crate) fn catalog_value(
    mut items: Vec<CatalogItem>,
    pagination: Pagination,
) -> Result<Value, serde_json::Error> {
    items.sort_by_key(CatalogItem::sort_key);
    paged_serialized_value("items", page_items(items, pagination))
}

pub(crate) fn catalog_output_schema() -> Arc<Map<String, Value>> {
    json_object_schema(&json!({
        "type": "object",
        "required": ["items", "total", "limit", "offset", "has_more"],
        "additionalProperties": false,
        "properties": {
            "items": {
                "type": "array",
                "items": catalog_table_item_output_schema()
            },
            "total": {
                "type": "integer",
                "minimum": 0
            },
            "limit": {
                "type": "integer",
                "minimum": 1
            },
            "offset": {
                "type": "integer",
                "minimum": 0
            },
            "has_more": { "type": "boolean" },
            "next_offset": {
                "type": "integer",
                "minimum": 0
            }
        }
    }))
}

fn catalog_table_item_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "schema_name", "name", "sql_reference", "description", "table"],
        "additionalProperties": false,
        "properties": {
            "kind": { "enum": ["table"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "description": { "type": "string" },
            "table": {
                "type": "object",
                "required": ["table_name", "guide", "required_filters"],
                "additionalProperties": false,
                "properties": {
                    "table_name": { "type": "string" },
                    "guide": { "type": "string" },
                    "required_filters": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "JSON shape assertions intentionally fail loudly in tests"
    )]

    use coral_api::v1::{TableSummary, Workspace};
    use jsonschema::JSONSchema;
    use serde_json::Value;

    use super::{CatalogItem, catalog_output_schema, catalog_value};
    use crate::surface::Pagination;

    #[test]
    fn catalog_value_matches_advertised_schema() {
        let value = catalog_value(
            vec![CatalogItem::from_table(&table("github", "pulls"))],
            Pagination {
                limit: 10,
                offset: 0,
            },
        )
        .expect("serialize catalog");
        let schema = Value::Object((*catalog_output_schema()).clone());
        let validator = JSONSchema::compile(&schema).expect("compile schema");
        if let Err(errors) = validator.validate(&value) {
            panic!(
                "catalog validates: {}",
                errors
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }

        assert_eq!(value["total"], 1);
        assert_eq!(value["items"][0]["kind"], "table");
        assert_eq!(value["items"][0]["name"], "github.pulls");
        assert_eq!(value["items"][0]["table"]["table_name"], "pulls");
    }

    fn table(schema_name: &str, name: &str) -> TableSummary {
        TableSummary {
            workspace: Some(Workspace {
                name: "default".to_string(),
            }),
            schema_name: schema_name.to_string(),
            name: name.to_string(),
            description: format!("{name} description"),
            required_filters: vec!["repo".to_string()],
            guide: format!("Query {name}."),
        }
    }
}
