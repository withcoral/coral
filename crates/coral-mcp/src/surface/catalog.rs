use std::sync::Arc;

use coral_api::v1::{TableFunction as ProtoTableFunction, TableSummary as ProtoTableSummary};
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
    TableFunction {
        schema_name: String,
        name: String,
        sql_reference: String,
        description: String,
        table_function: CatalogTableFunctionDetails,
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

    pub(crate) fn from_table_function(function: &ProtoTableFunction) -> Self {
        Self::TableFunction {
            schema_name: function.schema_name.clone(),
            name: format!("{}.{}", function.schema_name, function.name),
            sql_reference: format_schema_table_equivalent(&function.schema_name, &function.name),
            description: function.description.clone(),
            table_function: CatalogTableFunctionDetails {
                function_name: function.name.clone(),
                arguments: function
                    .arguments
                    .iter()
                    .map(|argument| CatalogTableFunctionArgument {
                        name: argument.name.clone(),
                        required: argument.required,
                        values: argument.values.clone(),
                    })
                    .collect(),
                result_columns: function
                    .result_columns
                    .iter()
                    .map(|column| CatalogTableFunctionResultColumn {
                        column_name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        is_nullable: column.nullable,
                        description: column.description.clone(),
                    })
                    .collect(),
            },
        }
    }

    fn sort_key(&self) -> String {
        let (schema_name, name, kind) = match self {
            Self::Table {
                schema_name, name, ..
            } => (schema_name, name, "table"),
            Self::TableFunction {
                schema_name, name, ..
            } => (schema_name, name, "table_function"),
        };
        format!("{schema_name}\0{name}\0{kind}")
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct CatalogTableDetails {
    table_name: String,
    guide: String,
    required_filters: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CatalogTableFunctionDetails {
    function_name: String,
    arguments: Vec<CatalogTableFunctionArgument>,
    result_columns: Vec<CatalogTableFunctionResultColumn>,
}

#[derive(Debug, Serialize)]
struct CatalogTableFunctionArgument {
    name: String,
    required: bool,
    values: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CatalogTableFunctionResultColumn {
    column_name: String,
    data_type: String,
    is_nullable: bool,
    description: String,
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
                "items": {
                    "oneOf": [
                        catalog_table_item_output_schema(),
                        catalog_table_function_item_output_schema()
                    ]
                }
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

fn catalog_table_function_item_output_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "kind",
            "schema_name",
            "name",
            "sql_reference",
            "description",
            "table_function"
        ],
        "additionalProperties": false,
        "properties": {
            "kind": { "enum": ["table_function"] },
            "schema_name": { "type": "string" },
            "name": { "type": "string" },
            "sql_reference": { "type": "string" },
            "description": { "type": "string" },
            "table_function": {
                "type": "object",
                "required": ["function_name", "arguments", "result_columns"],
                "additionalProperties": false,
                "properties": {
                    "function_name": { "type": "string" },
                    "arguments": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name", "required", "values"],
                            "additionalProperties": false,
                            "properties": {
                                "name": { "type": "string" },
                                "required": { "type": "boolean" },
                                "values": {
                                    "type": "array",
                                    "items": { "type": "string" }
                                }
                            }
                        }
                    },
                    "result_columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["column_name", "data_type", "is_nullable", "description"],
                            "additionalProperties": false,
                            "properties": {
                                "column_name": { "type": "string" },
                                "data_type": { "type": "string" },
                                "is_nullable": { "type": "boolean" },
                                "description": { "type": "string" }
                            }
                        }
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

    use coral_api::v1::{
        TableFunction, TableFunctionArgument, TableFunctionResultColumn, TableSummary, Workspace,
    };
    use jsonschema::JSONSchema;
    use serde_json::{Value, json};

    use super::{CatalogItem, catalog_output_schema, catalog_value};
    use crate::surface::Pagination;

    #[test]
    fn catalog_value_matches_advertised_schema() {
        let value = catalog_value(
            vec![
                CatalogItem::from_table(&table("github", "pulls")),
                CatalogItem::from_table_function(&table_function("github", "search_issues")),
            ],
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

        assert_eq!(value["total"], 2);
        assert_eq!(value["items"][0]["kind"], "table");
        assert_eq!(value["items"][0]["name"], "github.pulls");
        assert_eq!(value["items"][0]["table"]["table_name"], "pulls");
        assert_eq!(value["items"][1]["kind"], "table_function");
        assert_eq!(
            value["items"][1]["table_function"]["arguments"][0]["values"],
            json!(["open", "closed"])
        );
        assert_eq!(
            value["items"][1]["table_function"]["result_columns"][0]["column_name"],
            "title"
        );
    }

    fn table(schema_name: &str, name: &str) -> TableSummary {
        TableSummary {
            workspace: Some(workspace()),
            schema_name: schema_name.to_string(),
            name: name.to_string(),
            description: format!("{name} description"),
            required_filters: vec!["repo".to_string()],
            guide: format!("Query {name}."),
        }
    }

    fn table_function(schema_name: &str, name: &str) -> TableFunction {
        TableFunction {
            workspace: Some(workspace()),
            schema_name: schema_name.to_string(),
            name: name.to_string(),
            description: format!("{name} description"),
            arguments: vec![TableFunctionArgument {
                name: "state".to_string(),
                required: true,
                values: vec!["open".to_string(), "closed".to_string()],
            }],
            result_columns: vec![TableFunctionResultColumn {
                name: "title".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
                description: "Issue title.".to_string(),
            }],
        }
    }

    fn workspace() -> Workspace {
        Workspace {
            name: "default".to_string(),
        }
    }
}
