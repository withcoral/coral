use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput,
    IrOperationNaming, IrScalarType, RestExecutionAttachment, RestParameterBinding,
    RestRequestBody,
};
use crate::v4::naming::normalize_identifier;
use crate::v4::surfaces::json_schema::{
    json_schema_default_to_string, json_schema_scalar_type_or_string, json_schema_type_display,
};
use crate::{ManifestError, PageSizeSpec, PaginationMode, PaginationSpec, Result};

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
        let raw_operation_id = op_obj.get("operationId").and_then(Value::as_str);
        let operation_id = raw_operation_id.map_or_else(
            || fallback_operation_id(method_name, path),
            |raw| normalize_identifier(raw, "operation"),
        );
        let naming = openapi_operation_naming(op_obj, raw_operation_id, &operation_id);
        let method = parse_http_method(method_name);
        let mut diagnostics = Vec::new();
        let parameters = self.import_parameters(path_item, op_obj, &operation_id, &mut diagnostics);
        let request_body = self.import_request_body(op_obj, &operation_id, &mut diagnostics);
        let (output, response, entity) =
            self.import_response(path, op_obj, &operation_id, &mut diagnostics);
        let pagination = detect_pagination(&parameters);
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
            naming,
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
                    default_value: schema.get("default").map(json_schema_default_to_string),
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

fn openapi_operation_naming(
    operation: &Map<String, Value>,
    raw_operation_id: Option<&str>,
    normalized_operation_id: &str,
) -> Option<IrOperationNaming> {
    let group = operation
        .get("tags")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .find(|tag| !tag.is_empty())
        .map(|tag| normalize_identifier(tag, "group"));
    let operation = raw_operation_id
        .and_then(operation_id_leaf)
        .or_else(|| group.as_ref().map(|_| normalized_operation_id))
        .map(|leaf| normalize_identifier(leaf, "operation"));

    if group.is_none() && operation.is_none() {
        return None;
    }

    Some(IrOperationNaming { group, operation })
}

fn operation_id_leaf(raw_operation_id: &str) -> Option<&str> {
    raw_operation_id.rsplit('/').find_map(|segment| {
        let trimmed = segment.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
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

fn detect_pagination(inputs: &[IrOperationInput]) -> PaginationSpec {
    const DEFAULT_PAGE_SIZE: usize = 10;
    const DEFAULT_MAX_PAGE_SIZE: usize = 100;
    const PAGE_PARAM_NAMES: [&str; 3] = ["page", "page_number", "pagenum"];
    const PAGE_SIZE_PARAM_NAMES: [&str; 6] = [
        "max_results",
        "maxresults",
        "page_size",
        "pagesize",
        "per_page",
        "perpage",
    ];
    const OFFSET_PARAM_NAMES: [&str; 1] = ["offset"];
    const LIMIT_PARAM_NAMES: [&str; 1] = ["limit"];

    if let Some(page_input) = inputs
        .iter()
        .find(|input| PAGE_PARAM_NAMES.contains(&input.name.as_str()))
        && let Some(page_size_input) = inputs
            .iter()
            .find(|input| PAGE_SIZE_PARAM_NAMES.contains(&input.name.as_str()))
    {
        let default_page_size: usize = std::cmp::max(
            page_size_input
                .default_value
                .as_deref()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0),
            DEFAULT_PAGE_SIZE,
        );
        PaginationSpec {
            mode: PaginationMode::Page,
            page_size: Some(PageSizeSpec {
                default: default_page_size,
                max: std::cmp::max(default_page_size, DEFAULT_MAX_PAGE_SIZE),
                query_param: if page_size_input.location == IrInputLocation::Query {
                    Some(page_size_input.name.clone())
                } else {
                    // TODO: log diagnostics
                    None
                },
                // TODO: if input's location is IrInputLocation::Body then set the body path here.
                body_path: Vec::new(),
            }),
            page_param: if page_input.location == IrInputLocation::Query {
                Some(page_input.name.clone())
            } else {
                // TODO: log diagnostics
                None
            },
            page_start: 1,
            page_step: 1,
            ..PaginationSpec::default()
        }
    } else {
        PaginationSpec::default()
    }
}

fn fallback_operation_id(method: &str, path: &str) -> String {
    normalize_identifier(
        &format!("{method}_{}", path.replace(['{', '}'], "")),
        "operation",
    )
}
