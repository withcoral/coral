use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::ResponseSpec;
use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrOperationOutput, OutputCardinality, RestResponseAttachment};
use crate::v4::surfaces::json_schema::{
    json_schema_declares_only_type, json_schema_has_declared_type, json_schema_type_contains,
};

use super::import::OpenApiImporter;

pub(super) struct OpenApiResponsePaginationContext {
    pub(super) schema: Value,
    pub(super) headers: BTreeMap<String, Value>,
    pub(super) cardinality: OutputCardinality,
}

impl Default for OpenApiResponsePaginationContext {
    fn default() -> Self {
        Self {
            schema: Value::Null,
            headers: BTreeMap::new(),
            cardinality: OutputCardinality::None,
        }
    }
}

#[derive(Clone)]
struct SelectedJsonResponse {
    status_code: u16,
    media_type: String,
    schema: Value,
    headers: BTreeMap<String, Value>,
}

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
        Option<String>,
        OpenApiResponsePaginationContext,
    ) {
        let Some(selected) = self.select_json_response(
            operation.get("responses").and_then(Value::as_object),
            operation_id,
            diagnostics,
        ) else {
            let response = ResponseSpec::default();
            return (
                IrOperationOutput {
                    cardinality: OutputCardinality::None,
                    type_ref: "none".to_string(),
                },
                RestResponseAttachment {
                    status_code: 204,
                    media_type: "application/json".to_string(),
                    response,
                },
                None,
                OpenApiResponsePaginationContext::default(),
            );
        };

        // Composed before the reference is resolved, because this is where a
        // response written as a `$ref` with assertions beside it would lose
        // them: the schema handed on from here is the resolved one, so
        // `import_schema` never sees the siblings to ask about them.
        let selected_schema = self
            .ref_siblings_composed(&selected.schema, operation_id, diagnostics)
            .unwrap_or_else(|| selected.schema.clone());
        let Some(resolved) = self.resolve_ref(&selected_schema, operation_id, diagnostics) else {
            diagnostics.push(Diagnostic::new(
                format!("operation '{operation_id}' response schema could not be resolved"),
                Some(operation_id.to_string()),
            ));
            return (
                IrOperationOutput {
                    cardinality: OutputCardinality::Unknown,
                    type_ref: "json".to_string(),
                },
                RestResponseAttachment {
                    status_code: selected.status_code,
                    media_type: selected.media_type,
                    response: ResponseSpec::default(),
                },
                None,
                OpenApiResponsePaginationContext {
                    schema: Value::Null,
                    headers: selected.headers,
                    cardinality: OutputCardinality::Unknown,
                },
            );
        };
        let (cardinality, row_schema, schema_entity_name) =
            classify_response_schema(path, &resolved);
        // Only when the classification set this schema aside. A singleton hands
        // `import_schema` the very schema resolved here, which asks the same
        // question — reporting it here as well would say it twice. A collection
        // hands over `items` instead, so this is the one place its own keywords
        // are ever read.
        if row_schema != resolved {
            self.warn_removed_keywords(&resolved, "response schema", operation_id, diagnostics);
        }
        let type_ref = self
            .import_schema(
                &row_schema,
                &format!("{operation_id}_row"),
                operation_id,
                diagnostics,
            )
            .unwrap_or_else(|| "json".to_string());
        let response = ResponseSpec::default();
        let entity_name = (cardinality != OutputCardinality::None
            && cardinality != OutputCardinality::Unknown)
            .then(|| schema_entity_name.unwrap_or_else(|| entity_name_from_path(path)));
        (
            IrOperationOutput {
                cardinality,
                type_ref,
            },
            RestResponseAttachment {
                status_code: selected.status_code,
                media_type: selected.media_type,
                response,
            },
            entity_name,
            OpenApiResponsePaginationContext {
                schema: resolved,
                headers: selected.headers,
                cardinality,
            },
        )
    }
    fn select_json_response(
        &self,
        responses: Option<&Map<String, Value>>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<SelectedJsonResponse> {
        let responses = responses?;
        let mut numeric_candidates = Vec::new();
        let mut range_candidates = Vec::new();
        for (status, response) in responses {
            let Some(status) = success_response_status(status) else {
                continue;
            };
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
            let headers = response_headers(&response, operation_id, diagnostics, self);
            let candidate = SelectedJsonResponse {
                status_code: status.representative_status_code(),
                media_type: "application/json".to_string(),
                schema,
                headers,
            };
            if status.is_range() {
                range_candidates.push(candidate);
            } else {
                numeric_candidates.push(candidate);
            }
        }
        preferred_numeric_response(numeric_candidates)
            .or_else(|| range_candidates.into_iter().next())
    }
}

#[derive(Debug, Clone, Copy)]
enum SuccessResponseStatus {
    Numeric(u16),
    Range2xx,
}

impl SuccessResponseStatus {
    fn representative_status_code(self) -> u16 {
        match self {
            Self::Numeric(status_code) => status_code,
            Self::Range2xx => 200,
        }
    }

    fn is_range(self) -> bool {
        matches!(self, Self::Range2xx)
    }
}

fn success_response_status(status: &str) -> Option<SuccessResponseStatus> {
    if let Ok(status_code) = status.parse::<u16>() {
        return (200..300)
            .contains(&status_code)
            .then_some(SuccessResponseStatus::Numeric(status_code));
    }
    status
        .eq_ignore_ascii_case("2XX")
        .then_some(SuccessResponseStatus::Range2xx)
}

fn preferred_numeric_response(
    candidates: Vec<SelectedJsonResponse>,
) -> Option<SelectedJsonResponse> {
    candidates
        .iter()
        .position(|candidate| candidate.status_code == 200)
        .and_then(|index| candidates.get(index).cloned())
        .or_else(|| {
            candidates
                .into_iter()
                .min_by_key(|candidate| candidate.status_code)
        })
}

fn response_headers(
    response: &Value,
    operation_id: &str,
    diagnostics: &mut Vec<Diagnostic>,
    importer: &OpenApiImporter<'_>,
) -> BTreeMap<String, Value> {
    response
        .get("headers")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|headers| headers.iter())
        .filter_map(|(name, header)| {
            importer
                .resolve_ref(header, operation_id, diagnostics)
                .map(|resolved| (name.clone(), resolved))
        })
        .collect()
}

fn classify_response_schema(
    path: &str,
    schema: &Value,
) -> (OutputCardinality, Value, Option<String>) {
    if schema == &Value::Null {
        return (OutputCardinality::None, Value::Null, None);
    }
    // Matched through the array-aware helpers, because a nullable schema
    // declares its type as an array — `{"type": ["array", "null"]}` is 3.1's
    // spelling of a nullable collection. Reading only the string form missed
    // every one of them and fell through to the typeless default, so a nullable
    // collection was classified as a singleton object and its rows were lost.
    //
    // Object is tested first, in the same order `import_schema` tests it. Only a
    // schema claiming both types can tell the difference, and the two have to
    // agree about it: one deciding a response is a list while the other builds
    // its row type as an object would leave the cardinality and the row shape
    // describing different things.
    if json_schema_type_contains(schema, "object") || !json_schema_has_declared_type(schema) {
        return (
            OutputCardinality::Singleton,
            schema.clone(),
            schema
                .get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref)
                .or_else(|| Some(entity_name_from_path(path))),
        );
    }
    // The sole declared type, matching `import_schema`: a union that only
    // includes `array` is not a collection, and reading one as a list would
    // claim a row count for responses that are a single string.
    if json_schema_declares_only_type(schema, "array") {
        let item = schema.get("items").cloned().unwrap_or(Value::Null);
        return (
            OutputCardinality::List,
            item.clone(),
            item.get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref),
        );
    }
    (OutputCardinality::Unknown, schema.clone(), None)
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
