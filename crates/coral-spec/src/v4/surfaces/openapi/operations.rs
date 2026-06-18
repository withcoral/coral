use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput,
    IrOperationOutput, IrScalarType, OutputCardinality, RestExecutionAttachment,
    RestParameterBinding, RestRequestBody,
};
use crate::v4::naming::normalize_identifier;
use crate::v4::pagination::{
    OpenApiOperationTarget, OpenApiPaginationMatcher, PaginationProvenance, V4OperationTarget,
    V4PaginationMatcher, V4PaginationOutcome, duplicate_operation_overlay_error,
    multiple_profile_match_error,
};
use crate::v4::surfaces::json_schema::{
    json_schema_scalar_type_or_string, json_schema_type_display,
};
use crate::{ManifestError, PaginationSpec, Result};

use super::import::OpenApiImporter;

impl OpenApiImporter<'_> {
    pub(super) fn import_operation(
        &mut self,
        path: &str,
        path_item: &Map<String, Value>,
        method_name: &str,
        operation: &Value,
    ) -> Result<IrOperation> {
        let op_obj = operation.as_object().ok_or_else(|| {
            ManifestError::validation(format!(
                "OpenAPI operation {method_name} {path} must be a mapping"
            ))
        })?;
        let operation_id = op_obj
            .get("operationId")
            .and_then(Value::as_str)
            .map_or_else(
                || fallback_operation_id(method_name, path),
                |raw| normalize_identifier(raw, "operation"),
            );
        let method = parse_http_method(method_name);
        let mut diagnostics = Vec::new();
        let parameters = self.import_parameters(path_item, op_obj, &operation_id, &mut diagnostics);
        let request_body = self.import_request_body(op_obj, &operation_id, &mut diagnostics);
        let (output, response, entity) =
            self.import_response(path, op_obj, &operation_id, &mut diagnostics);
        let (pagination, pagination_provenance) = self.resolve_pagination_overlay(
            path,
            method,
            op_obj.get("operationId").and_then(Value::as_str),
            &operation_id,
            &parameters,
            &output,
            op_obj,
            &mut diagnostics,
        )?;
        let rest_parameters = parameters
            .iter()
            .map(|input| RestParameterBinding {
                input_name: input.name.clone(),
                location: input.location,
                wire_name: input.name.clone(),
                required: input.required,
                data_type: input.data_type,
            })
            .collect();
        Ok(IrOperation {
            id: operation_id.clone(),
            method_name: op_obj
                .get("operationId")
                .and_then(Value::as_str)
                .unwrap_or(method_name)
                .to_string(),
            description: op_obj
                .get("description")
                .or_else(|| op_obj.get("summary"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            deprecated: op_obj
                .get("deprecated")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            read_only: method == HttpMethod::Get,
            inputs: parameters,
            output,
            entity,
            execution: IrExecutionAttachment::Rest(Box::new(RestExecutionAttachment {
                method,
                path_template: path.to_string(),
                parameters: rest_parameters,
                request_body,
                response,
                pagination,
                pagination_provenance,
            })),
            diagnostics,
        })
    }

    fn import_parameters(
        &mut self,
        path_item: &Map<String, Value>,
        operation: &Map<String, Value>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<IrOperationInput> {
        let mut merged: BTreeMap<(IrInputLocation, String), Value> = BTreeMap::new();
        for parameter in path_item
            .get("parameters")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .chain(
                operation
                    .get("parameters")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten(),
            )
        {
            let Some(resolved) = self.resolve_ref(parameter, operation_id, diagnostics) else {
                continue;
            };
            let Some(parameter_obj) = resolved.as_object() else {
                diagnostics.push(Diagnostic::warning(
                    "OPENAPI_PARAMETER_INVALID",
                    format!("operation '{operation_id}' has a parameter that is not an object"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                continue;
            };
            let Some(name) = parameter_obj.get("name").and_then(Value::as_str) else {
                diagnostics.push(Diagnostic::warning(
                    "OPENAPI_PARAMETER_INVALID",
                    format!("operation '{operation_id}' has a parameter without a string name"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                continue;
            };
            let Some(location) = parameter_obj
                .get("in")
                .and_then(Value::as_str)
                .and_then(parse_parameter_location)
            else {
                diagnostics.push(Diagnostic::warning(
                    "OPENAPI_PARAMETER_SERIALIZATION_UNSUPPORTED",
                    format!("operation '{operation_id}' has unsupported parameter location"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                continue;
            };
            merged.insert((location, name.to_string()), resolved.clone());
        }

        merged
            .into_values()
            .filter_map(|parameter| {
                let parameter_obj = parameter.as_object()?;
                let name = parameter_obj.get("name")?.as_str()?.to_string();
                let location = parameter_obj
                    .get("in")
                    .and_then(Value::as_str)
                    .and_then(parse_parameter_location)?;
                let schema = parameter_obj.get("schema").unwrap_or(&Value::Null);
                let scalar =
                    self.import_parameter_scalar(schema, &name, operation_id, diagnostics)?;
                Some(IrOperationInput {
                    name,
                    location,
                    required: parameter_is_required(parameter_obj, location),
                    data_type: scalar,
                    default_value: schema.get("default").map(openapi_default_to_string),
                    description: parameter_obj
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                })
            })
            .collect()
    }

    fn import_parameter_scalar(
        &mut self,
        schema: &Value,
        name: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<IrScalarType> {
        let resolved = self.resolve_ref(schema, operation_id, diagnostics)?;
        let Some(scalar) = json_schema_scalar_type_or_string(&resolved) else {
            diagnostics.push(Diagnostic::warning(
                "PROJECTION_INPUT_UNSUPPORTED",
                format!(
                    "parameter '{name}' has unsupported schema type '{}'",
                    json_schema_type_display(&resolved)
                ),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            return None;
        };
        Some(scalar)
    }

    fn import_request_body(
        &mut self,
        operation: &Map<String, Value>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<RestRequestBody> {
        let body = operation.get("requestBody")?;
        let body = self.resolve_ref(body, operation_id, diagnostics)?;
        let body_obj = body.as_object()?;
        let content = body_obj.get("content")?.as_object()?;
        let json = content.get("application/json")?;
        let schema = json.get("schema").unwrap_or(&Value::Null);
        let type_ref = self
            .import_schema(
                schema,
                &format!("{operation_id}_request_body"),
                operation_id,
                diagnostics,
            )
            .unwrap_or_else(|| "json".to_string());
        diagnostics.push(Diagnostic::warning(
            "OPENAPI_REQUEST_BODY_UNPUBLISHED",
            format!("operation '{operation_id}' has a request body and will not be published"),
            self.surface.id.clone(),
            Some(operation_id.to_string()),
        ));
        Some(RestRequestBody {
            required: body_obj
                .get("required")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            media_type: "application/json".to_string(),
            type_ref,
        })
    }
}

fn parse_http_method(method: &str) -> HttpMethod {
    match method {
        "get" => HttpMethod::Get,
        "head" => HttpMethod::Head,
        "options" => HttpMethod::Options,
        "post" => HttpMethod::Post,
        "put" => HttpMethod::Put,
        "patch" => HttpMethod::Patch,
        "delete" => HttpMethod::Delete,
        "trace" => HttpMethod::Trace,
        other => unreachable!("unsupported method passed to OpenAPI importer: {other}"),
    }
}

fn parse_parameter_location(location: &str) -> Option<IrInputLocation> {
    match location {
        "path" => Some(IrInputLocation::Path),
        "query" => Some(IrInputLocation::Query),
        "header" => Some(IrInputLocation::Header),
        "cookie" => Some(IrInputLocation::Cookie),
        _ => None,
    }
}

fn parameter_is_required(parameter_obj: &Map<String, Value>, location: IrInputLocation) -> bool {
    if location == IrInputLocation::Path {
        return true;
    }
    parameter_obj
        .get("required")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn openapi_default_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null | Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn fallback_operation_id(method: &str, path: &str) -> String {
    normalize_identifier(
        &format!("{method}_{}", path.replace(['{', '}'], "")),
        "operation",
    )
}

impl OpenApiImporter<'_> {
    #[expect(
        clippy::too_many_arguments,
        reason = "Pagination overlay resolution needs source-native operation evidence."
    )]
    fn resolve_pagination_overlay(
        &mut self,
        path: &str,
        method: HttpMethod,
        raw_operation_id: Option<&str>,
        operation_id: &str,
        inputs: &[IrOperationInput],
        output: &IrOperationOutput,
        operation: &Map<String, Value>,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Result<(PaginationSpec, PaginationProvenance)> {
        let explicit_matches = self
            .surface
            .pagination
            .operations
            .iter()
            .enumerate()
            .filter(|(_, overlay)| {
                let V4OperationTarget::OpenApi(target) = &overlay.target else {
                    return false;
                };
                openapi_target_matches(target, raw_operation_id, method, path)
            })
            .map(|(index, overlay)| (index, overlay.outcome.clone()))
            .collect::<Vec<_>>();
        if explicit_matches.len() > 1 {
            return Err(duplicate_operation_overlay_error(
                &self.manifest.common.name,
                &self.surface.id,
                &format!("{method:?} {path}"),
            ));
        }
        if let Some((index, outcome)) = explicit_matches.into_iter().next() {
            if let Some(matched) = self.matched_pagination_overlays.get_mut(index) {
                *matched = true;
            }
            return self.pagination_from_outcome(outcome, PaginationProvenance::Authored);
        }

        let mut matching_profiles = Vec::new();
        for profile in &self.surface.pagination.profiles {
            let V4PaginationMatcher::OpenApi(matcher) = &profile.matcher else {
                continue;
            };
            if self.openapi_profile_matches(
                matcher,
                method,
                path,
                inputs,
                operation,
                operation_id,
                diagnostics,
            ) {
                matching_profiles.push((profile.name.clone(), profile.outcome.clone()));
            }
        }
        if matching_profiles.len() > 1 {
            let names = matching_profiles
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            return Err(multiple_profile_match_error(
                &self.manifest.common.name,
                &self.surface.id,
                &format!("{method:?} {path}"),
                &names,
            ));
        }
        if let Some((_, outcome)) = matching_profiles.into_iter().next() {
            return self.pagination_from_outcome(outcome, PaginationProvenance::ProfileGenerated);
        }

        if likely_openapi_pagination(inputs, output) {
            diagnostics.push(Diagnostic::warning(
                "PAGINATION_OVERLAY_MISSING",
                format!(
                    "operation '{operation_id}' has pagination-like parameters but no V4 pagination overlay matched"
                ),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
        }
        Ok((PaginationSpec::default(), PaginationProvenance::None))
    }

    fn pagination_from_outcome(
        &self,
        outcome: V4PaginationOutcome,
        provenance: PaginationProvenance,
    ) -> Result<(PaginationSpec, PaginationProvenance)> {
        match outcome {
            V4PaginationOutcome::Http(pagination) => Ok((*pagination, provenance)),
            V4PaginationOutcome::Unsupported { reason } => {
                let _ = reason;
                Ok((PaginationSpec::default(), PaginationProvenance::Unsupported))
            }
            V4PaginationOutcome::McpCursor(_) | V4PaginationOutcome::McpOffset(_) => {
                Err(ManifestError::validation(format!(
                    "source '{}' surface '{}' OpenAPI pagination overlay must use HTTP pagination",
                    self.manifest.common.name, self.surface.id
                )))
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Profile matching needs source-native operation evidence and diagnostics."
    )]
    fn openapi_profile_matches(
        &self,
        matcher: &OpenApiPaginationMatcher,
        method: HttpMethod,
        path: &str,
        inputs: &[IrOperationInput],
        operation: &Map<String, Value>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> bool {
        if !matcher.methods.is_empty() && !matcher.methods.contains(&method) {
            return false;
        }
        if !matcher.paths.is_empty() && !matcher.paths.iter().any(|candidate| candidate == path) {
            return false;
        }
        if !matcher.query_params.is_empty()
            && !matcher.query_params.iter().all(|name| {
                inputs
                    .iter()
                    .any(|input| input.location == IrInputLocation::Query && input.name == *name)
            })
        {
            return false;
        }
        matcher.response_cursor_path.is_empty()
            || self.response_schema_has_path(
                operation,
                operation_id,
                &matcher.response_cursor_path,
                diagnostics,
            )
    }

    fn response_schema_has_path(
        &self,
        operation: &Map<String, Value>,
        operation_id: &str,
        path: &[String],
        diagnostics: &mut Vec<Diagnostic>,
    ) -> bool {
        let Some((_, _, schema)) = self.select_json_response(
            operation.get("responses").and_then(Value::as_object),
            operation_id,
            diagnostics,
        ) else {
            return false;
        };
        let Some(schema) = self.resolve_ref(&schema, operation_id, diagnostics) else {
            return false;
        };
        schema_has_property_path(&schema, path)
    }
}

fn openapi_target_matches(
    target: &OpenApiOperationTarget,
    raw_operation_id: Option<&str>,
    method: HttpMethod,
    path: &str,
) -> bool {
    match target {
        OpenApiOperationTarget::OperationId(operation_id) => raw_operation_id == Some(operation_id),
        OpenApiOperationTarget::MethodPath {
            method: expected_method,
            path: expected_path,
        } => *expected_method == method && expected_path == path,
    }
}

fn schema_has_property_path(schema: &Value, path: &[String]) -> bool {
    if path.is_empty() {
        return true;
    }
    let Some((first, rest)) = path.split_first() else {
        return true;
    };
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return false;
    };
    let Some(property) = properties.get(first) else {
        return false;
    };
    schema_has_property_path(property, rest)
}

fn likely_openapi_pagination(inputs: &[IrOperationInput], output: &IrOperationOutput) -> bool {
    if !matches!(
        output.cardinality,
        OutputCardinality::List | OutputCardinality::WrappedList
    ) {
        return false;
    }
    inputs.iter().any(|input| {
        input.location == IrInputLocation::Query
            && matches!(
                input.name.as_str(),
                "page"
                    | "per_page"
                    | "limit"
                    | "offset"
                    | "cursor"
                    | "after"
                    | "starting_after"
                    | "ending_before"
                    | "page_token"
                    | "next_page_token"
            )
    })
}
