use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::{Map, Value};

use crate::{ManifestError, PageSizeSpec, PaginationMode, PaginationSpec, ResponseSpec, Result};

use super::super::diagnostic::Diagnostic;
use super::super::identifiers::{entity_name_from_path, normalize_identifier, type_id_from_ref};
use super::super::ir::{
    HttpMethod, IrEntityCandidate, IrExecutionAttachment, IrField, IrOperation, IrOperationInput,
    IrOperationOutput, IrScalarType, IrType, IrTypeShape, OpenApiParameterLocation,
    OutputCardinality, RestExecutionAttachment, RestParameterBinding, RestRequestBody,
    RestResponseAttachment, SemanticIr,
};
use super::super::manifest::{V4SourceManifest, V4Surface};
use super::super::{OPENAPI_IMPORTER_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
use super::response::{classify_response_schema, select_json_response};

pub fn import_openapi_surface(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    document_bytes: &[u8],
) -> Result<SemanticIr> {
    let document: Value =
        serde_yaml::from_slice(document_bytes).map_err(ManifestError::parse_yaml)?;
    let openapi = document
        .get("openapi")
        .and_then(Value::as_str)
        .ok_or_else(|| ManifestError::validation("OpenAPI document is missing openapi version"))?;
    if !openapi.starts_with("3.0.") {
        return Err(ManifestError::validation(format!(
            "OpenAPI document for surface '{}' uses unsupported version '{openapi}'",
            surface.id
        )));
    }

    let mut importer = OpenApiImporter::new(manifest, surface, &document);
    importer.import()
}

struct OpenApiImporter<'a> {
    manifest: &'a V4SourceManifest,
    surface: &'a V4Surface,
    document: &'a Value,
    types: BTreeMap<String, IrType>,
    diagnostics: Vec<Diagnostic>,
}

impl<'a> OpenApiImporter<'a> {
    fn new(manifest: &'a V4SourceManifest, surface: &'a V4Surface, document: &'a Value) -> Self {
        Self {
            manifest,
            surface,
            document,
            types: BTreeMap::new(),
            diagnostics: Vec::new(),
        }
    }

    fn import(&mut self) -> Result<SemanticIr> {
        let paths = self
            .document
            .get("paths")
            .and_then(Value::as_object)
            .ok_or_else(|| ManifestError::validation("OpenAPI document is missing paths"))?;
        let mut operations = Vec::new();
        let mut operation_ids = HashSet::new();
        for (path, path_item) in paths {
            let Some(path_item) = path_item.as_object() else {
                continue;
            };
            for method_name in [
                "get", "head", "options", "post", "put", "patch", "delete", "trace",
            ] {
                let Some(operation_value) = path_item.get(method_name) else {
                    continue;
                };
                let operation =
                    self.import_operation(path, path_item, method_name, operation_value)?;
                if !operation_ids.insert(operation.id.clone()) {
                    return Err(ManifestError::validation(format!(
                        "source '{}' surface '{}' imports duplicate operation id '{}'",
                        self.manifest.common.name, self.surface.id, operation.id
                    )));
                }
                operations.push(operation);
            }
        }
        Ok(SemanticIr {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: self.manifest.common.name.clone(),
            source_version: self.manifest.common.version.clone(),
            surface_id: self.surface.id.clone(),
            surface_type: self.surface.surface_type,
            importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
            operations,
            types: self.types.values().cloned().collect(),
            diagnostics: self.diagnostics.clone(),
        })
    }

    fn import_operation(
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
            inputs: parameters,
            output,
            entity,
            execution: IrExecutionAttachment::Rest(RestExecutionAttachment {
                method,
                path_template: path.to_string(),
                parameters: rest_parameters,
                request_body,
                response,
                pagination,
            }),
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
        let mut merged: BTreeMap<(OpenApiParameterLocation, String), Value> = BTreeMap::new();
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
                    required: parameter_obj
                        .get("required")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
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
        let schema_type = resolved
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("string");
        let scalar = match schema_type {
            "string" => {
                if resolved.get("format").and_then(Value::as_str) == Some("date-time") {
                    IrScalarType::Timestamp
                } else {
                    IrScalarType::String
                }
            }
            "integer" => IrScalarType::Integer,
            "number" => IrScalarType::Number,
            "boolean" => IrScalarType::Boolean,
            other => {
                diagnostics.push(Diagnostic::warning(
                    "PROJECTION_INPUT_UNSUPPORTED",
                    format!("parameter '{name}' has unsupported schema type '{other}'"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                return None;
            }
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

    fn import_response(
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
        let Some((status_code, media_type, schema)) =
            select_json_response(operation.get("responses").and_then(Value::as_object))
        else {
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

        let Some(resolved) = self.resolve_ref(schema, operation_id, diagnostics) else {
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

    #[expect(
        clippy::too_many_lines,
        reason = "OpenAPI schema import is deliberately kept in one local recursive routine for the first v4 slice."
    )]
    fn import_schema(
        &mut self,
        schema: &Value,
        suggested_id: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<String> {
        let resolved = self.resolve_ref(schema, operation_id, diagnostics)?;
        let type_id = schema.get("$ref").and_then(Value::as_str).map_or_else(
            || normalize_identifier(suggested_id, "type"),
            type_id_from_ref,
        );
        if self.types.contains_key(&type_id) {
            return Some(type_id);
        }
        let description = resolved
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let nullable = resolved
            .get("nullable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.types.insert(
            type_id.clone(),
            IrType {
                id: type_id.clone(),
                shape: IrTypeShape::Json,
                nullable,
                description: description.clone(),
            },
        );
        let shape = if let Some(all_of) = resolved.get("allOf").and_then(Value::as_array) {
            let mut merged = Map::new();
            for item in all_of {
                let item = self.resolve_ref(item, operation_id, diagnostics)?;
                if let Some(properties) = item.get("properties").and_then(Value::as_object) {
                    for (name, property) in properties {
                        if let Some(existing) = merged.get(name)
                            && existing != property
                        {
                            diagnostics.push(Diagnostic::warning(
                                "OPENAPI_ALLOF_CONFLICT",
                                format!("allOf property '{name}' conflicts in operation '{operation_id}'"),
                                self.surface.id.clone(),
                                Some(operation_id.to_string()),
                            ));
                            return None;
                        }
                        merged.insert(name.clone(), property.clone());
                    }
                }
            }
            IrTypeShape::Object {
                fields: self.import_object_fields(
                    &merged,
                    &BTreeSet::new(),
                    &type_id,
                    operation_id,
                    diagnostics,
                ),
            }
        } else if let Some(values) = resolved.get("enum").and_then(Value::as_array) {
            IrTypeShape::Enum {
                values: values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect(),
            }
        } else {
            match resolved
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("object")
            {
                "object" => {
                    if let Some(properties) = resolved.get("properties").and_then(Value::as_object)
                    {
                        let required = required_fields(&resolved);
                        IrTypeShape::Object {
                            fields: self.import_object_fields(
                                properties,
                                &required,
                                &type_id,
                                operation_id,
                                diagnostics,
                            ),
                        }
                    } else if let Some(additional) = resolved.get("additionalProperties") {
                        let value_type_ref = self
                            .import_schema(
                                additional,
                                &format!("{type_id}_value"),
                                operation_id,
                                diagnostics,
                            )
                            .unwrap_or_else(|| "json".to_string());
                        IrTypeShape::Map { value_type_ref }
                    } else {
                        IrTypeShape::Json
                    }
                }
                "array" => {
                    let item = resolved.get("items").unwrap_or(&Value::Null);
                    let item_type_ref = self
                        .import_schema(item, &format!("{type_id}_item"), operation_id, diagnostics)
                        .unwrap_or_else(|| "json".to_string());
                    IrTypeShape::List { item_type_ref }
                }
                "string" => {
                    let scalar =
                        if resolved.get("format").and_then(Value::as_str) == Some("date-time") {
                            IrScalarType::Timestamp
                        } else {
                            IrScalarType::String
                        };
                    IrTypeShape::Scalar(scalar)
                }
                "integer" => IrTypeShape::Scalar(IrScalarType::Integer),
                "number" => IrTypeShape::Scalar(IrScalarType::Number),
                "boolean" => IrTypeShape::Scalar(IrScalarType::Boolean),
                _ => IrTypeShape::Json,
            }
        };
        self.types.insert(
            type_id.clone(),
            IrType {
                id: type_id.clone(),
                shape,
                nullable,
                description,
            },
        );
        Some(type_id)
    }

    fn import_object_fields(
        &mut self,
        properties: &Map<String, Value>,
        required: &BTreeSet<String>,
        parent_id: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<IrField> {
        properties
            .iter()
            .map(|(name, schema)| {
                let type_ref = self
                    .import_schema(
                        schema,
                        &format!("{parent_id}_{name}"),
                        operation_id,
                        diagnostics,
                    )
                    .unwrap_or_else(|| "json".to_string());
                IrField {
                    name: name.clone(),
                    type_ref,
                    required: required.contains(name),
                    nullable: true,
                    description: schema
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                }
            })
            .collect()
    }

    fn resolve_ref(
        &self,
        value: &Value,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<Value> {
        let Some(reference) = value.get("$ref").and_then(Value::as_str) else {
            return Some(value.clone());
        };
        if !reference.starts_with("#/") {
            diagnostics.push(Diagnostic::warning(
                "OPENAPI_EXTERNAL_REF_UNSUPPORTED",
                format!("external reference '{reference}' is unsupported"),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            return None;
        }
        let pointer = reference.strip_prefix('#').unwrap_or(reference);
        if let Some(target) = self.document.pointer(pointer) {
            Some(target.clone())
        } else {
            diagnostics.push(Diagnostic::warning(
                "OPENAPI_REF_NOT_FOUND",
                format!("reference '{reference}' was not found"),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            None
        }
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

fn parse_parameter_location(location: &str) -> Option<OpenApiParameterLocation> {
    match location {
        "path" => Some(OpenApiParameterLocation::Path),
        "query" => Some(OpenApiParameterLocation::Query),
        "header" => Some(OpenApiParameterLocation::Header),
        "cookie" => Some(OpenApiParameterLocation::Cookie),
        _ => None,
    }
}

fn openapi_default_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null | Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn required_fields(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn detect_pagination(inputs: &[IrOperationInput]) -> PaginationSpec {
    let has_page = inputs
        .iter()
        .any(|input| input.location == OpenApiParameterLocation::Query && input.name == "page");
    let has_per_page = inputs
        .iter()
        .any(|input| input.location == OpenApiParameterLocation::Query && input.name == "per_page");
    if has_page && has_per_page {
        PaginationSpec {
            mode: PaginationMode::Page,
            page_size: Some(PageSizeSpec {
                default: 30,
                max: 100,
                query_param: Some("per_page".to_string()),
                body_path: Vec::new(),
            }),
            page_param: Some("page".to_string()),
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
