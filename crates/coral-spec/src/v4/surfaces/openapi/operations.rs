use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::v4::OperationMetadata;
use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput,
    IrOperationNaming, IrOperationOutput, IrScalarType, OutputCardinality, RestExecutionAttachment,
    RestParameterBinding, RestRequestBody,
};
use crate::v4::lookup_keys::infer_rest_lookup_keys;
use crate::v4::naming::normalize_identifier;
use crate::v4::response_cursors::{StringTypeRequirement, find_response_cursor_path};
use crate::v4::surfaces::json_schema::{
    SchemaRoot, json_schema_default_to_string, json_schema_scalar_type_or_string,
    json_schema_type_contains, json_schema_type_display,
};
use crate::v4::wrapped_lists::{WrappedListInferenceContext, infer_wrapped_list_row_path};
use crate::{
    ManifestError, PageSizeSpec, PaginationMode, PaginationSpec, Result,
    v4::resolve_output_row_type_ref,
};

use super::import::OpenApiImporter;
use super::responses::OpenApiResponsePaginationContext;

impl OpenApiImporter<'_> {
    pub(super) fn import_operation(
        &mut self,
        path: &str,
        path_item: &Map<String, Value>,
        method_name: &str,
        operation: &Value,
    ) -> Result<(IrOperation, OperationMetadata)> {
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
        let (output, response, entity_name, pagination_context) =
            self.import_response(path, op_obj, &operation_id, &mut diagnostics);
        let contract =
            detect_pagination_contract(self.schema_root(), &parameters, &pagination_context);
        let row_path = self.infer_row_path(
            raw_operation_id.unwrap_or(&operation_id),
            contract.as_ref().is_some_and(signals_page_envelope),
            &pagination_context,
            &output,
        );
        // A contract only becomes this operation's pagination once Coral reads
        // the response as a list, whether by declaration or by row path.
        let pagination = is_list_like_output(pagination_context.cardinality, &row_path)
            .then_some(contract)
            .flatten()
            .unwrap_or_default();
        let lookup_keys = infer_rest_lookup_keys(&parameters, &pagination);
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
        let operation = IrOperation {
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
            entity_name,
            execution: IrExecutionAttachment::Rest(Box::new(RestExecutionAttachment {
                method,
                path_template: path.to_string(),
                parameters: rest_parameters,
                request_body,
                response,
            })),
            diagnostics,
        };
        Ok((
            operation,
            OperationMetadata::Rest {
                row_path,
                pagination,
                lookup_keys,
            },
        ))
    }

    /// Infers where the rows of a wrapped-list response live, discarding a path
    /// the imported types cannot resolve so the importer never emits metadata
    /// that fails plan validation.
    fn infer_row_path(
        &self,
        operation_name: &str,
        paginated_operation: bool,
        pagination_context: &OpenApiResponsePaginationContext,
        output: &IrOperationOutput,
    ) -> Vec<String> {
        let row_path = infer_wrapped_list_row_path(WrappedListInferenceContext {
            operation_name,
            paginated_operation,
            schema_root: self.schema_root(),
            response_schema: &pagination_context.schema,
        });
        if row_path.is_empty() {
            return row_path;
        }
        let types = self
            .types
            .iter()
            .map(|(id, ty)| (id.as_str(), ty))
            .collect::<BTreeMap<_, _>>();
        if resolve_output_row_type_ref(output, &row_path, &types).is_ok() {
            row_path
        } else {
            Vec::new()
        }
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
                diagnostics.push(Diagnostic::new(
                    format!("operation '{operation_id}' has a parameter that is not an object"),
                    Some(operation_id.to_string()),
                ));
                continue;
            };
            let Some(name) = parameter_obj.get("name").and_then(Value::as_str) else {
                diagnostics.push(Diagnostic::new(
                    format!("operation '{operation_id}' has a parameter without a string name"),
                    Some(operation_id.to_string()),
                ));
                continue;
            };
            let Some(location) = parameter_obj
                .get("in")
                .and_then(Value::as_str)
                .and_then(parse_parameter_location)
            else {
                diagnostics.push(Diagnostic::new(
                    format!("operation '{operation_id}' has unsupported parameter location"),
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
                let schema = self.read_schema(parameter_obj.get("schema").unwrap_or(&Value::Null));
                let scalar =
                    self.import_parameter_scalar(&schema, &name, operation_id, diagnostics)?;
                Some(IrOperationInput {
                    name,
                    location,
                    required: parameter_is_required(parameter_obj, location),
                    data_type: scalar,
                    // A null default declares that the parameter has none, so
                    // it is dropped rather than stringified. `default: null`
                    // beside a nullable union is what Pydantic emits for
                    // `param: int | None = None`, and it survives the unwrap
                    // as an annotation; stringifying it would bind the filter
                    // to the literal `null` and send it on every unfiltered
                    // query.
                    default_value: schema
                        .get("default")
                        .filter(|default| !default.is_null())
                        .map(json_schema_default_to_string),
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
            diagnostics.push(Diagnostic::new(
                format!(
                    "parameter '{name}' has unsupported schema type '{}'",
                    json_schema_type_display(&resolved)
                ),
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
        diagnostics.push(Diagnostic::new(
            format!("operation '{operation_id}' has a request body and will not be published"),
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

/// Finds the pagination contract the operation's inputs and response describe,
/// independently of whether Coral will read the response as a list.
///
/// None of the detectors consults the row path, so this runs first and answers
/// the question wrapped-list inference needs — is this operation paginated? —
/// without the two inferences having to predict each other.
fn detect_pagination_contract(
    document: SchemaRoot<'_>,
    inputs: &[IrOperationInput],
    context: &OpenApiResponsePaginationContext,
) -> Option<PaginationSpec> {
    detect_link_header_pagination(document, inputs, context)
        .or_else(|| detect_next_url_body_pagination(document, inputs, context))
        .or_else(|| detect_cursor_query_pagination(document, inputs, context))
        .or_else(|| detect_offset_pagination(inputs))
        .or_else(|| detect_page_pagination(inputs))
}

/// Whether a detected contract is evidence that the response is a page
/// envelope.
///
/// Two signals qualify. Binding a request input — a cursor, offset, page, or
/// page-size parameter — says the caller can ask for another page. A next-page
/// URL *at the response root* says the response hands one back unprompted; that
/// path comes out of this operation's own response schema, so it is evidence
/// about this response rather than about the API at large. Graph needs the
/// second signal: `{"@odata.nextLink": ..., "value": [...]}` on an operation
/// that declares no `$top` binds nothing at all.
///
/// Root-only is the whole point of the length test. `find_response_cursor_path`
/// descends into nested objects up to depth 8, but what this unlocks —
/// `accept_fallback` in `infer_wrapped_list_row_path` — applies at the root, so
/// deep evidence would answer a question it was not asked. A singleton
/// `GET /tracks/{id}` returning `{id, tags: [...], links: {next_href}}` matches
/// `nexthref` two levels down; counting that would fire the sole-array fallback
/// and promote `tags` to the whole relation, discarding the declared resource.
/// Both shapes this exists for — `@odata.nextLink` and Stripe's
/// `next_page_url` — sit at the root as single segments, and a legitimate
/// `{data, meta: {next_url}}` still qualifies through `meta` in the
/// metadata-object lexicon.
///
/// A bare `Link` header is neither. It reads no input, and providers declare
/// that header on singleton resources too — GitHub does, on
/// `GET /orgs/{org}/actions/hosted-runners/{id}` among others. Treating it as
/// evidence would promote a resource's incidental array, such as that runner's
/// `public_ips`, to the whole relation.
fn signals_page_envelope(contract: &PaginationSpec) -> bool {
    contract.cursor_param.is_some()
        || contract.offset_param.is_some()
        || contract.page_param.is_some()
        || contract.page_size.is_some()
        || contract.next_url_path.len() == 1
}

fn detect_link_header_pagination(
    document: SchemaRoot<'_>,
    inputs: &[IrOperationInput],
    context: &OpenApiResponsePaginationContext,
) -> Option<PaginationSpec> {
    let has_link_header = response_header(context, &["link"]).is_some();
    let next_url_header = response_next_url_header(document, context);
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

/// Detects a response body that hands back the complete URL of the next page,
/// as `OData` does with `@odata.nextLink`.
///
/// Ranked above cursor-query and offset detection because a whole URL is
/// unambiguous: there is no guessing which request parameter a token belongs
/// in, and no risk of driving the endpoint with a parameter it rejects. Graph
/// declares `$skip` on collections that reject it at runtime, so that ordering
/// is load-bearing, not a preference. It stays below `Link` header detection,
/// which is a declared, cheaper signal.
///
/// Outranking the input-corroborated modes is what makes
/// [`StringTypeRequirement::Declared`] necessary. Every other detector has a
/// second signal — a matching cursor, offset or page parameter — so a name-only
/// false positive costs them nothing. This one commits on the name alone, and
/// if the property does not hold a string `Value::as_str` reads `None` at
/// runtime, pagination stops, and the query returns page one with no error and
/// no diagnostic, where offset pagination would have fetched everything.
/// Requiring the descriptor to declare `string` is the second signal.
fn detect_next_url_body_pagination(
    document: SchemaRoot<'_>,
    inputs: &[IrOperationInput],
    context: &OpenApiResponsePaginationContext,
) -> Option<PaginationSpec> {
    /// Response property names that carry a whole URL rather than a token.
    ///
    /// Deliberately narrower than `RESPONSE_NEXT_URL_HEADER_TOKENS`, which
    /// accepts bare `next`/`nextpage`. A *header* called `Next` is nearly
    /// always a URL; a *body field* called `next` is very often a continuation
    /// token, and those must keep falling through to cursor-query detection so
    /// they end up in the request parameter that expects them. Resist
    /// harmonising the two lists.
    const RESPONSE_NEXT_URL_BODY_TOKENS: &[&str] = &[
        "odatanextlink",
        "nextlink",
        "nexturl",
        "nexturi",
        "nextpageurl",
        "nextpageuri",
        "nextpagelink",
        "nextpagehref",
        "nexthref",
    ];

    let next_url_path = find_response_cursor_path(
        document,
        &context.schema,
        RESPONSE_NEXT_URL_BODY_TOKENS,
        StringTypeRequirement::Declared,
    )?;
    Some(PaginationSpec {
        mode: PaginationMode::NextUrlBody,
        page_size: detect_page_size(inputs),
        next_url_path,
        ..PaginationSpec::default()
    })
}

fn detect_cursor_query_pagination(
    document: SchemaRoot<'_>,
    inputs: &[IrOperationInput],
    context: &OpenApiResponsePaginationContext,
) -> Option<PaginationSpec> {
    /// Response property names that conventionally carry a continuation token.
    const RESPONSE_CURSOR_TOKENS: &[&str] = &[
        "after",
        "continuationtoken",
        "cursor",
        "endcursor",
        "iterator",
        "next",
        "nextcursor",
        "nextmarker",
        "nextpage",
        "nextpagetoken",
        "nexttoken",
    ];

    let cursor_input = find_optional_string_query_input(
        inputs,
        &[
            "cursor",
            "continuationtoken",
            "iterator",
            "marker",
            "paginationtoken",
            "pagetoken",
            "startcursor",
            "startingafter",
            "nextcursor",
            "nextpagetoken",
            "nexttoken",
            "nextpage",
            "next",
            "after",
            "before",
            "page",
        ],
    )?;
    if name_token(&cursor_input.name) == "page" && cursor_input.data_type != IrScalarType::String {
        return None;
    }

    // The pagination context holds the response schema with only its own `$ref`
    // resolved, so the document is still needed to see inside a referenced
    // metadata sibling.
    // Permissive about the declared type, unlike body next-URL detection: this
    // detector has already found a cursor query parameter to corroborate the
    // name, and descriptors routinely leave envelope metadata untyped.
    let response_cursor_path = find_response_cursor_path(
        document,
        &context.schema,
        RESPONSE_CURSOR_TOKENS,
        StringTypeRequirement::Untyped,
    )
    .unwrap_or_default();
    let response_cursor_header = response_cursor_header(document, context);
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
    let offset_input = find_numeric_query_input(
        inputs,
        &["offset", "offsetindex", "pageoffset", "skip", "startindex"],
    )?;
    let page_size_input = detect_page_size_input(inputs)?;
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
    let page_input = find_numeric_page_input(inputs)?;
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
    detect_page_size_input(inputs).map(page_size_spec)
}

fn detect_page_size_input(inputs: &[IrOperationInput]) -> Option<&IrOperationInput> {
    find_numeric_query_input(
        inputs,
        &[
            "amount",
            "count",
            "itemsperpage",
            "limit",
            "maxresults",
            "pagelimit",
            "pagesize",
            "perpage",
            "resultsperpage",
            "size",
            "take",
            "top",
        ],
    )
}

fn find_numeric_page_input(inputs: &[IrOperationInput]) -> Option<&IrOperationInput> {
    find_numeric_query_input(
        inputs,
        &["currentpage", "page", "pageindex", "pagenumber", "pagenum"],
    )
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

/// Pagination metadata validation only accepts numeric inputs for page,
/// offset, and page-size parameters, so detection must not select inputs of
/// any other type.
fn find_numeric_query_input<'a>(
    inputs: &'a [IrOperationInput],
    candidate_tokens: &[&str],
) -> Option<&'a IrOperationInput> {
    find_query_input(inputs, candidate_tokens).filter(|input| {
        matches!(
            input.data_type,
            IrScalarType::Integer | IrScalarType::Number
        )
    })
}

fn find_optional_string_query_input<'a>(
    inputs: &'a [IrOperationInput],
    candidate_tokens: &[&str],
) -> Option<&'a IrOperationInput> {
    candidate_tokens.iter().find_map(|candidate| {
        inputs.iter().find(|input| {
            input.location == IrInputLocation::Query
                && !input.required
                && input.data_type == IrScalarType::String
                && name_token(&input.name) == *candidate
        })
    })
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

fn response_cursor_header(
    root: SchemaRoot<'_>,
    context: &OpenApiResponsePaginationContext,
) -> Option<String> {
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
                && response_header_allows_string(root, header)
        })
        .map(|(name, _)| name.clone())
}

fn response_next_url_header(
    root: SchemaRoot<'_>,
    context: &OpenApiResponsePaginationContext,
) -> Option<String> {
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
                && response_header_allows_string(root, header)
        })
        .map(|(name, _)| name.clone())
}

fn response_header_allows_string(root: SchemaRoot<'_>, header: &Value) -> bool {
    header.get("schema").is_none_or(|schema| {
        let schema = root.read(schema);
        json_schema_type_contains(&schema, "string") || schema.get("type").is_none()
    })
}

/// A wrapped-list envelope is a singleton by cardinality but yields rows, so it
/// paginates like a declared list.
fn is_list_like_output(cardinality: OutputCardinality, row_path: &[String]) -> bool {
    cardinality == OutputCardinality::List || !row_path.is_empty()
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
