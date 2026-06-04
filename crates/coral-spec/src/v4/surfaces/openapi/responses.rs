use serde_json::{Map, Value};

use crate::ResponseSpec;
use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    IrEntityCandidate, IrOperationOutput, OutputCardinality, RestResponseAttachment,
};

use super::import::OpenApiImporter;

impl OpenApiImporter<'_> {
    pub(super) fn import_response(
        &mut self,
        path: &str,
        operation: &Map<String, Value>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> (
        IrOperationOutput,
        RestResponseAttachment,
        Option<IrEntityCandidate>,
    ) {
        let Some((status_code, media_type, schema)) = self.select_json_response(
            operation.get("responses").and_then(Value::as_object),
            operation_id,
            diagnostics,
        ) else {
            let response = ResponseSpec::default();
            return (
                IrOperationOutput {
                    cardinality: OutputCardinality::None,
                    type_ref: "none".to_string(),
                    row_path: Vec::new(),
                },
                RestResponseAttachment {
                    status_code: 204,
                    media_type: "application/json".to_string(),
                    response,
                },
                None,
            );
        };

        let Some(resolved) = self.resolve_ref(&schema, operation_id, diagnostics) else {
            diagnostics.push(Diagnostic::warning(
                "OPENAPI_RESPONSE_SCHEMA_UNRESOLVED",
                format!("operation '{operation_id}' response schema could not be resolved"),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            return (
                IrOperationOutput {
                    cardinality: OutputCardinality::Unknown,
                    type_ref: "json".to_string(),
                    row_path: Vec::new(),
                },
                RestResponseAttachment {
                    status_code,
                    media_type,
                    response: ResponseSpec::default(),
                },
                None,
            );
        };
        let (cardinality, row_path, row_schema, entity_name) =
            classify_response_schema(path, &resolved);
        let type_ref = self
            .import_schema(
                &row_schema,
                &format!("{operation_id}_row"),
                operation_id,
                diagnostics,
            )
            .unwrap_or_else(|| "json".to_string());
        let response = ResponseSpec {
            rows_path: row_path.clone(),
            ..ResponseSpec::default()
        };
        let entity = (cardinality != OutputCardinality::None
            && cardinality != OutputCardinality::Unknown)
            .then(|| IrEntityCandidate {
                name: entity_name.unwrap_or_else(|| entity_name_from_path(path)),
                type_ref: type_ref.clone(),
                identity_fields: vec!["id".to_string()],
            });
        (
            IrOperationOutput {
                cardinality,
                type_ref,
                row_path,
            },
            RestResponseAttachment {
                status_code,
                media_type,
                response,
            },
            entity,
        )
    }
    fn select_json_response(
        &self,
        responses: Option<&Map<String, Value>>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<(u16, String, Value)> {
        let responses = responses?;
        let mut candidates = Vec::new();
        for (status, response) in responses {
            let Ok(status_code) = status.parse::<u16>() else {
                continue;
            };
            if !(200..300).contains(&status_code) {
                continue;
            }
            let Some(response) = self.resolve_ref(response, operation_id, diagnostics) else {
                continue;
            };
            let Some(content) = response.get("content").and_then(Value::as_object) else {
                continue;
            };
            let Some(json) = content.get("application/json") else {
                continue;
            };
            let schema = json.get("schema").cloned().unwrap_or(Value::Null);
            candidates.push((status_code, "application/json".to_string(), schema));
        }
        candidates
            .iter()
            .position(|(status, _, _)| *status == 200)
            .and_then(|index| candidates.get(index).cloned())
            .or_else(|| candidates.into_iter().min_by_key(|(status, _, _)| *status))
    }
}

fn classify_response_schema(
    path: &str,
    schema: &Value,
) -> (OutputCardinality, Vec<String>, Value, Option<String>) {
    if schema == &Value::Null {
        return (OutputCardinality::None, Vec::new(), Value::Null, None);
    }
    if schema.get("type").and_then(Value::as_str) == Some("array") {
        let item = schema.get("items").cloned().unwrap_or(Value::Null);
        return (
            OutputCardinality::List,
            Vec::new(),
            item.clone(),
            item.get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref),
        );
    }
    if schema
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("object")
        == "object"
    {
        if let Some((property_name, items)) = schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(wrapped_list_property)
        {
            let item = items.get("items").cloned().unwrap_or(Value::Null);
            return (
                OutputCardinality::WrappedList,
                vec![property_name.to_string()],
                item.clone(),
                item.get("$ref")
                    .and_then(Value::as_str)
                    .map(entity_name_from_ref),
            );
        }
        return (
            OutputCardinality::Singleton,
            Vec::new(),
            schema.clone(),
            schema
                .get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref)
                .or_else(|| Some(entity_name_from_path(path))),
        );
    }
    (OutputCardinality::Unknown, Vec::new(), schema.clone(), None)
}

fn wrapped_list_property(properties: &Map<String, Value>) -> Option<(&str, &Value)> {
    ["items", "data", "results", "rows"]
        .iter()
        .find_map(|name| {
            properties
                .get(*name)
                .filter(|property| property.get("type").and_then(Value::as_str) == Some("array"))
                .map(|property| (*name, property))
        })
        .or_else(|| single_array_payload_property(properties))
}

fn single_array_payload_property(properties: &Map<String, Value>) -> Option<(&str, &Value)> {
    let array_properties = properties
        .iter()
        .filter(|(_, property)| property.get("type").and_then(Value::as_str) == Some("array"))
        .filter(|(name, _)| !is_wrapper_metadata_property(name))
        .collect::<Vec<_>>();
    match array_properties.as_slice() {
        [(name, property)] => Some((name.as_str(), *property)),
        [] | [_, _, ..] => None,
    }
}

fn is_wrapper_metadata_property(name: &str) -> bool {
    matches!(
        name,
        "total_count" | "incomplete_results" | "has_more" | "next" | "previous"
    )
}

fn entity_name_from_ref(reference: &str) -> String {
    reference
        .rsplit('/')
        .next()
        .map_or_else(|| "entity".to_string(), |raw| raw.replace(" Response", ""))
}

fn entity_name_from_path(path: &str) -> String {
    path.split('/')
        .rfind(|segment| !segment.is_empty() && !segment.starts_with('{'))
        .unwrap_or("entity")
        .to_string()
}
