use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput,
    IrOperationNaming, IrScalarType, OutputCardinality, RestExecutionAttachment,
    RestParameterBinding, RestRequestBody,
};
use crate::v4::naming::normalize_identifier;
use crate::v4::surfaces::json_schema::{
    json_schema_default_to_string, json_schema_scalar_type_or_string, json_schema_type_contains,
    json_schema_type_display,
};
use crate::{ManifestError, PageSizeSpec, PaginationMode, PaginationSpec, Result};

use super::import::OpenApiImporter;
use super::responses::OpenApiResponsePaginationContext;

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
        let (output, response, entity, pagination_context) =
            self.import_response(path, op_obj, &operation_id, &mut diagnostics);
        let pagination = detect_pagination(&parameters, &pagination_context);
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
                let scalar = self.import_parameter_scalar(
                    parameter_obj,
                    schema,
                    &name,
                    operation_id,
                    diagnostics,
                )?;
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
        parameter: &Map<String, Value>,
        schema: &Value,
        name: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<IrScalarType> {
        let resolved = self.resolve_ref(schema, operation_id, diagnostics)?;
        if parameter_serializes_as_comma_delimited_array(parameter, &resolved) {
            let items = resolved.get("items")?;
            let item_schema = self.resolve_ref(items, operation_id, diagnostics)?;
            if json_schema_scalar_type_or_string(&item_schema).is_some() {
                return Some(IrScalarType::String);
            }
        }
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

fn parameter_serializes_as_comma_delimited_array(
    parameter: &Map<String, Value>,
    schema: &Value,
) -> bool {
    parameter.get("in").and_then(Value::as_str) == Some("query")
        && parameter
            .get("style")
            .and_then(Value::as_str)
            .unwrap_or("form")
            == "form"
        && parameter.get("explode").and_then(Value::as_bool) == Some(false)
        && json_schema_type_contains(schema, "array")
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

fn detect_pagination(
    inputs: &[IrOperationInput],
    context: &OpenApiResponsePaginationContext,
) -> PaginationSpec {
    if !is_paginated_cardinality(context.cardinality) {
        return PaginationSpec::default();
    }
    detect_link_header_pagination(inputs, context)
        .or_else(|| detect_cursor_query_pagination(inputs, context))
        .or_else(|| detect_offset_pagination(inputs))
        .or_else(|| detect_page_pagination(inputs))
        .unwrap_or_default()
}

fn detect_link_header_pagination(
    inputs: &[IrOperationInput],
    context: &OpenApiResponsePaginationContext,
) -> Option<PaginationSpec> {
    let has_link_header = response_header(context, &["link"]).is_some();
    let next_url_header = response_next_url_header(context);
    if !has_link_header && next_url_header.is_none() {
        return None;
    }
    let page_input = find_numeric_page_input(inputs);
    Some(PaginationSpec {
        mode: PaginationMode::LinkHeader,
        page_size: detect_page_size(inputs),
        page_param: page_input.map(|input| input.name.clone()),
        page_start: page_input.and_then(numeric_input_default).unwrap_or(1),
        next_url_header,
        ..PaginationSpec::default()
    })
}

fn detect_cursor_query_pagination(
    inputs: &[IrOperationInput],
    context: &OpenApiResponsePaginationContext,
) -> Option<PaginationSpec> {
    let cursor_input = find_optional_query_input(
        inputs,
        &[
            "after",
            "continuationtoken",
            "cursor",
            "marker",
            "nextcursor",
            "nextpage",
            "nextpagetoken",
            "nexttoken",
            "page",
            "paginationtoken",
            "pagetoken",
            "startingafter",
        ],
    )?;
    if name_token(&cursor_input.name) == "page" && cursor_input.data_type != IrScalarType::String {
        return None;
    }

    let response_cursor_path = find_response_cursor_path(&context.schema).unwrap_or_default();
    let response_cursor_header = response_cursor_header(context);
    if response_cursor_path.is_empty() && response_cursor_header.is_none() {
        return None;
    }
    Some(PaginationSpec {
        mode: PaginationMode::CursorQuery,
        page_size: detect_page_size(inputs),
        cursor_param: Some(cursor_input.name.clone()),
        response_cursor_path,
        response_cursor_header,
        ..PaginationSpec::default()
    })
}

fn detect_offset_pagination(inputs: &[IrOperationInput]) -> Option<PaginationSpec> {
    let offset_input = find_query_input(inputs, &["offset"])?;
    let page_size_input = find_query_input(inputs, &["limit"])?;
    Some(PaginationSpec {
        mode: PaginationMode::Offset,
        page_size: Some(page_size_spec(page_size_input)),
        offset_param: Some(offset_input.name.clone()),
        offset_start: numeric_input_default(offset_input).unwrap_or(0),
        offset_step: None,
        ..PaginationSpec::default()
    })
}

fn detect_page_pagination(inputs: &[IrOperationInput]) -> Option<PaginationSpec> {
    let page_input = find_page_input(inputs)?;
    let page_size = detect_page_size(inputs)?;
    Some(PaginationSpec {
        mode: PaginationMode::Page,
        page_size: Some(page_size),
        page_param: Some(page_input.name.clone()),
        page_start: numeric_input_default(page_input).unwrap_or(1),
        page_step: 1,
        ..PaginationSpec::default()
    })
}

fn detect_page_size(inputs: &[IrOperationInput]) -> Option<PageSizeSpec> {
    find_query_input(
        inputs,
        &[
            "limit",
            "maxresults",
            "pagesize",
            "perpage",
            "resultsperpage",
        ],
    )
    .map(page_size_spec)
}

fn find_page_input(inputs: &[IrOperationInput]) -> Option<&IrOperationInput> {
    find_query_input(inputs, &["page", "pagenumber", "pagenum"])
}

fn find_numeric_page_input(inputs: &[IrOperationInput]) -> Option<&IrOperationInput> {
    find_page_input(inputs).filter(|input| {
        matches!(
            input.data_type,
            IrScalarType::Integer | IrScalarType::Number
        ) || numeric_input_default(input).is_some()
    })
}

fn find_query_input<'a>(
    inputs: &'a [IrOperationInput],
    candidate_tokens: &[&str],
) -> Option<&'a IrOperationInput> {
    inputs
        .iter()
        .filter(|input| input.location == IrInputLocation::Query)
        .find(|input| candidate_tokens.contains(&name_token(&input.name).as_str()))
}

fn find_optional_query_input<'a>(
    inputs: &'a [IrOperationInput],
    candidate_tokens: &[&str],
) -> Option<&'a IrOperationInput> {
    find_query_input(inputs, candidate_tokens).filter(|input| !input.required)
}

fn response_header<'a>(
    context: &'a OpenApiResponsePaginationContext,
    candidate_tokens: &[&str],
) -> Option<&'a Value> {
    context
        .headers
        .iter()
        .find(|(name, _)| candidate_tokens.contains(&name_token(name).as_str()))
        .map(|(_, header)| header)
}

fn response_cursor_header(context: &OpenApiResponsePaginationContext) -> Option<String> {
    const RESPONSE_CURSOR_HEADER_TOKENS: &[&str] = &[
        "continuationtoken",
        "nextcursor",
        "nextmarker",
        "nextpagetoken",
        "nexttoken",
        "xcontinuationtoken",
        "xnextcursor",
        "xnextmarker",
        "xnextpagetoken",
        "xnexttoken",
    ];

    context
        .headers
        .iter()
        .find(|(name, header)| {
            RESPONSE_CURSOR_HEADER_TOKENS.contains(&name_token(name).as_str())
                && response_header_allows_string(header)
        })
        .map(|(name, _)| name.clone())
}

fn response_next_url_header(context: &OpenApiResponsePaginationContext) -> Option<String> {
    const RESPONSE_NEXT_URL_HEADER_TOKENS: &[&str] = &[
        "next",
        "nextpage",
        "nextpageurl",
        "nexturl",
        "xnext",
        "xnextpage",
        "xnextpageurl",
        "xnexturl",
    ];

    context
        .headers
        .iter()
        .find(|(name, header)| {
            RESPONSE_NEXT_URL_HEADER_TOKENS.contains(&name_token(name).as_str())
                && response_header_allows_string(header)
        })
        .map(|(name, _)| name.clone())
}

fn response_header_allows_string(header: &Value) -> bool {
    header.get("schema").is_none_or(|schema| {
        json_schema_type_contains(schema, "string") || schema.get("type").is_none()
    })
}

fn find_response_cursor_path(schema: &Value) -> Option<Vec<String>> {
    let properties = schema.get("properties").and_then(Value::as_object)?;
    for (name, property) in properties {
        if is_response_cursor_property(name, property) {
            return Some(vec![name.clone()]);
        }
    }
    for (name, property) in properties {
        if !json_schema_type_contains(property, "object") {
            continue;
        }
        if let Some(mut path) = find_response_cursor_path(property) {
            path.insert(0, name.clone());
            return Some(path);
        }
    }
    None
}

fn is_response_cursor_property(name: &str, schema: &Value) -> bool {
    const RESPONSE_CURSOR_TOKENS: &[&str] = &[
        "endcursor",
        "nextcursor",
        "nextmarker",
        "nextpage",
        "nextpagetoken",
        "nexttoken",
    ];

    RESPONSE_CURSOR_TOKENS.contains(&name_token(name).as_str())
        && (json_schema_type_contains(schema, "string") || schema.get("type").is_none())
}

fn is_paginated_cardinality(cardinality: OutputCardinality) -> bool {
    matches!(
        cardinality,
        OutputCardinality::List | OutputCardinality::WrappedList
    )
}

fn page_size_spec(input: &IrOperationInput) -> PageSizeSpec {
    const DEFAULT_PAGE_SIZE: usize = 10;
    const DEFAULT_MAX_PAGE_SIZE: usize = 100;

    let default = numeric_input_default(input)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_PAGE_SIZE);
    PageSizeSpec {
        default,
        max: default.max(DEFAULT_MAX_PAGE_SIZE),
        query_param: Some(input.name.clone()),
        body_path: Vec::new(),
    }
}

fn numeric_input_default(input: &IrOperationInput) -> Option<i64> {
    input.default_value.as_deref()?.parse().ok()
}

fn name_token(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}

fn fallback_operation_id(method: &str, path: &str) -> String {
    normalize_identifier(
        &format!("{method}_{}", path.replace(['{', '}'], "")),
        "operation",
    )
}
