use crate::AuthenticationConfiguration;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Write as _;

use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};

use crate::openapi::projection::{fallback_typescript, project_typescript, typescript_names};
use crate::openapi::schema::{
    OpenApiSchemaGraphBuilder, ReferenceObjectKind, ResolvedObject, child_pointer as source_child,
    response_key_is_legal,
};
use crate::paths::assign_tool_paths;
use crate::schema::canonicalize;
use crate::{
    CatalogDiagnostic, HttpContentBinding, HttpEncodingBinding, HttpExecutionBinding,
    HttpHeaderBinding, HttpMethod, HttpParameterBinding, HttpParameterLocation, HttpParameterStyle,
    HttpRedirectPolicy, HttpRequestBodyBinding, HttpResponseBinding, HttpResponseStatus,
    MaterializationOutcome, MaterializedTool, OpenApiDocument, PreparedProviderCatalog, SchemaId,
    SchemaSemantics, ToolKey, ToolPathAssignment, ToolPresentation,
};

/// Stable version of the bounded OpenAPI profile materializer.
pub(crate) const MATERIALIZER_VERSION: &str = "openapi-schema-faithful-search-profile-v5";

/// Materializes one exact pinned OpenAPI input and carries forward paths owned
/// by Tool Keys that remain active in the replacement catalog.
#[must_use]
pub(crate) fn materialize(
    document: &OpenApiDocument,
    previous_paths: &[ToolPathAssignment],
) -> MaterializationOutcome {
    match build_openapi(document, previous_paths) {
        Ok(package) => MaterializationOutcome::Ready(package),
        Err(diagnostic) => MaterializationOutcome::Rejected(diagnostic),
    }
}

fn build_openapi(
    input: &OpenApiDocument,
    previous_paths: &[ToolPathAssignment],
) -> Result<PreparedProviderCatalog, CatalogDiagnostic> {
    let document = parse_document(input)?;
    let root = document.as_object().ok_or_else(|| {
        diagnostic(
            "invalid_openapi_document",
            "the pinned OpenAPI document root must be an object",
        )
    })?;
    let schema_semantics = validate_version(root)?;
    let display_name =
        crate::configuration::configured_display_name(input.configuration(), || {
            provider_display_name(root)
        })?;
    let mut schemas = OpenApiSchemaGraphBuilder::new(&document, schema_semantics)?;
    let credentials = match input
        .configuration()
        .and_then(|configuration| configuration.authentication.as_ref())
    {
        Some(AuthenticationConfiguration::None) => CredentialSelection::default(),
        Some(contract) => CredentialSelection {
            shared: vec![crate::HttpCredentialSlot::new(
                "configured".into(),
                contract.clone().into(),
            )],
            by_operation: BTreeMap::new(),
        },
        None => validate_credential_contract(root, &document)?,
    };
    let configuration = input.configuration();
    let root_server = configuration.map_or_else(
        || selected_server(root),
        |configuration| super::configuration::configured_server(root, "#/servers", configuration),
    );
    let drafts = collect_operations(root, &root_server, &document, configuration)?;
    let mut credential_parameters =
        credential_parameter_addresses(root, &credentials.shared, configuration.is_some());
    if let Some(configuration) = configuration {
        credential_parameters.extend(
            configuration
                .request_headers
                .keys()
                .map(|name| (HttpParameterLocation::Header, name.clone())),
        );
    }
    let assigned_paths = assign_tool_paths(drafts.iter().map(|draft| &draft.key), previous_paths)?;

    let mut builds = Vec::with_capacity(drafts.len());
    for draft in drafts {
        let relative_path = assigned_paths
            .get(&draft.key)
            .cloned()
            .ok_or_else(invalid_tool_index)?;
        let slots = credentials.for_tool(&draft.key);
        let mut build =
            materialize_operation(draft, relative_path, &mut schemas, &credential_parameters)?;
        build.binding.set_credential_slots(slots);
        if let Some(configuration) = configuration {
            build
                .binding
                .set_request_headers(configuration.request_headers.clone());
        }
        builds.push(build);
    }
    let schema_graph = schemas.finish()?;
    let schema_graph_json = schema_graph.to_storage_json().map_err(|_error| {
        diagnostic(
            "invalid_schema_graph",
            "the authoritative OpenAPI schema graph cannot be encoded",
        )
    })?;
    let (materialized, shared_types) = finish_tools(builds, &display_name, &schema_graph)?;
    let catalog = PreparedProviderCatalog::new(
        display_name,
        schema_graph,
        schema_graph_json,
        materialized,
        shared_types,
    );
    let Some(configuration) = configuration else {
        return Ok(catalog);
    };
    catalog
        .bind_http_credential_destinations()
        .map_err(|_error| {
            diagnostic(
                "invalid_execution_binding",
                "an HTTP Execution Binding cannot be encoded",
            )
        })?
        .with_configuration(configuration)
        .map_err(|_error| {
            diagnostic(
                "invalid_provider_configuration",
                "the Provider configuration cannot be applied to the catalog",
            )
        })
}

fn finish_tools(
    builds: Vec<OperationBuild>,
    display_name: &str,
    schema_graph: &crate::SchemaGraph,
) -> Result<(Vec<MaterializedTool>, crate::schema::SharedTypes), CatalogDiagnostic> {
    let mut materialized = Vec::with_capacity(builds.len());
    let type_names = typescript_names(schema_graph);
    let mut shared_types = crate::schema::SharedTypes::default();
    for mut build in builds {
        let (declaration, type_references) = match project_typescript(
            &build.input_schema,
            &build.binding,
            schema_graph,
            &type_names,
            &mut shared_types,
        ) {
            Ok(projection) => {
                build.diagnostics.extend(projection.diagnostics);
                (projection.declaration, projection.type_references)
            }
            Err(diagnostic) => {
                build.diagnostics.push(diagnostic);
                (fallback_typescript(), Vec::new())
            }
        };
        if let Some(diagnostic) = crate::types::tool_description_too_large(
            &build.relative_path,
            display_name,
            &declaration,
            &build.presentation,
        ) {
            build.diagnostics.push(diagnostic);
        }
        let binding_json = build.binding.to_storage_json().map_err(|_error| {
            diagnostic(
                "invalid_execution_binding",
                "an HTTP Execution Binding cannot be encoded",
            )
        })?;
        let diagnostics_json = serde_json::to_string(&build.diagnostics).map_err(|_error| {
            diagnostic(
                "invalid_tool_diagnostic",
                "a Tool diagnostic cannot be encoded safely",
            )
        })?;
        materialized.push(MaterializedTool::new(
            build.key,
            build.relative_path,
            build.summary,
            build.presentation,
            crate::ToolHints::default(),
            crate::ExecutionBinding::Http(build.binding),
            binding_json,
            build.input_schema,
            build.input_schema_json,
            build.output_schema_json,
            declaration,
            type_references,
            build.diagnostics,
            diagnostics_json,
        ));
    }
    Ok((materialized, shared_types))
}

pub(super) fn parse_document(document: &OpenApiDocument) -> Result<Value, CatalogDiagnostic> {
    let yaml: serde_yaml::Value =
        serde_path_to_error::deserialize(serde_yaml::Deserializer::from_str(document.as_str()))
            .map_err(|error| {
                diagnostic(
                    "invalid_openapi_document",
                    "the pinned input is not a valid OpenAPI YAML or JSON document",
                )
                .at_location(&parse_failure_location(&error))
            })?;
    let value = serde_json::to_value(yaml).map_err(|_error| {
        diagnostic(
            "invalid_openapi_document",
            "the pinned OpenAPI document contains unsupported mapping keys",
        )
    })?;
    Ok(canonicalize(&value))
}

/// Names where parsing failed by line and column, then as a JSON pointer.
///
/// The position comes first because `at_location` truncates long locations.
/// The parser's own message can quote the offending value, so it stays out.
fn parse_failure_location(error: &serde_path_to_error::Error<serde_yaml::Error>) -> String {
    use serde_path_to_error::Segment;

    let pointer = error
        .path()
        .iter()
        // Past an unknown segment, the path no longer names a JSON location.
        .map_while(|segment| match segment {
            Segment::Seq { index } => Some(index.to_string()),
            Segment::Map { key } => Some(key.clone()),
            // A YAML tag becomes a `!tag` key in the JSON document.
            Segment::Enum { variant } => Some(format!("!{variant}")),
            Segment::Unknown => None,
        })
        .fold(String::from("#"), |pointer, token| {
            source_child(&pointer, &token)
        });
    let position = error
        .inner()
        .location()
        .map(|location| format!("line {}, column {}", location.line(), location.column()));
    match (position, pointer.as_str()) {
        (Some(position), "#") => position,
        (Some(position), _) => format!("{position} ({pointer})"),
        (None, _) => pointer,
    }
}

struct OperationDraft {
    key: ToolKey,
    path: String,
    query: Option<String>,
    method: HttpMethod,
    operation: Map<String, Value>,
    operation_pointer: String,
    path_parameters: Option<String>,
    server_url: Option<String>,
    server_diagnostic: Option<CatalogDiagnostic>,
}

fn collect_operations(
    root: &Map<String, Value>,
    root_server: &Result<Option<String>, CatalogDiagnostic>,
    document: &Value,
    configuration: Option<&crate::ProviderConfiguration>,
) -> Result<Vec<OperationDraft>, CatalogDiagnostic> {
    let located = super::operations::locate_operations(root, document)?;
    located
        .operations
        .into_iter()
        .map(|operation| {
            let local_server = operation.server_declaration != "#/servers";
            let selected = if local_server {
                let object = located
                    .servers
                    .get(&operation.server_declaration)
                    .ok_or_else(invalid_tool_index)?;
                configuration.map_or_else(
                    || selected_server(object),
                    |configuration| {
                        super::configuration::configured_server(
                            object,
                            &operation.server_declaration,
                            configuration,
                        )
                    },
                )
            } else {
                root_server.clone()
            };
            let (server_url, server_diagnostic) = match selected {
                Ok(Some(url)) => (Some(url), None),
                Ok(None) => {
                    return Err(diagnostic(
                        "missing_openapi_server",
                        "an OpenAPI Operation has no selected HTTP server",
                    ));
                }
                Err(diagnostic) if local_server => (None, Some(diagnostic)),
                Err(diagnostic) => return Err(diagnostic),
            };
            Ok(OperationDraft {
                key: operation.key,
                path: operation.path,
                query: operation.query,
                method: operation.method,
                operation: operation.object,
                operation_pointer: operation.pointer,
                path_parameters: operation.path_parameters,
                server_url,
                server_diagnostic,
            })
        })
        .collect()
}

struct OperationBuild {
    key: ToolKey,
    relative_path: String,
    summary: String,
    presentation: ToolPresentation,
    binding: HttpExecutionBinding,
    input_schema: SchemaId,
    input_schema_json: String,
    output_schema_json: String,
    diagnostics: Vec<CatalogDiagnostic>,
}

fn materialize_operation(
    draft: OperationDraft,
    relative_path: String,
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    credential_parameters: &BTreeSet<(HttpParameterLocation, String)>,
) -> Result<OperationBuild, CatalogDiagnostic> {
    let summary = safe_summary(&draft.operation, draft.method, &draft.path);
    let presentation = operation_presentation(&draft.operation, &summary);
    let source = operation_schema_source(&draft.key);
    let mut diagnostics: Vec<CatalogDiagnostic> =
        draft.server_diagnostic.clone().into_iter().collect();
    let binding = match build_http_binding(&draft, schemas, &source, credential_parameters) {
        Ok(binding) => binding,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            HttpExecutionBinding::new(
                draft.method,
                draft.server_url.clone(),
                draft.path.clone(),
                None,
                HttpRedirectPolicy::Follow,
                Vec::new(),
                None,
                Vec::new(),
                Vec::new(),
            )
        }
    };
    if draft.operation.contains_key("callbacks") {
        diagnostics.push(diagnostic(
            "unsupported_openapi_callback",
            "an OpenAPI callback cannot be invoked by the v1 HTTP binding",
        ));
    }
    let (input_schema, input_schema_json) = match build_input_schema(&binding) {
        Ok(schema) => {
            let json = serialize_catalog_value(&schema)?;
            let id = schemas
                .add_generated_schema(&format!("operation:{source}:input"), &schema)
                .inspect_err(|diagnostic| diagnostics.push(diagnostic.clone()));
            match id {
                Ok(id) => (id, json),
                Err(_) => fallback_input_schema(schemas, &source)?,
            }
        }
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            fallback_input_schema(schemas, &source)?
        }
    };
    let output_schema_json = serialize_catalog_value(
        draft
            .operation
            .get("responses")
            .unwrap_or(&Value::Object(Map::new())),
    )?;
    Ok(OperationBuild {
        key: draft.key,
        relative_path,
        summary,
        presentation,
        binding,
        input_schema,
        input_schema_json,
        output_schema_json,
        diagnostics,
    })
}

fn fallback_input_schema(
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    source: &str,
) -> Result<(SchemaId, String), CatalogDiagnostic> {
    let schema = serde_json::json!({"type": "object", "additionalProperties": false});
    let json = serialize_catalog_value(&schema)?;
    let id =
        schemas.add_generated_schema(&format!("operation:{source}:fallback-input"), &schema)?;
    Ok((id, json))
}

// Authentication replacement also removes document-declared credential inputs;
// they must never reappear as ordinary Program arguments behind the replacement.
fn credential_parameter_addresses(
    root: &Map<String, Value>,
    selected: &[crate::HttpCredentialSlot],
    replaced: bool,
) -> BTreeSet<(HttpParameterLocation, String)> {
    let mut addresses = BTreeSet::new();
    for slot in selected {
        match slot.contract().authentication() {
            AuthenticationConfiguration::HeaderCredentials { headers } => addresses.extend(
                headers
                    .keys()
                    .map(|header| (HttpParameterLocation::Header, header.to_ascii_lowercase())),
            ),
            AuthenticationConfiguration::QueryCredentials { parameters } => addresses.extend(
                parameters
                    .keys()
                    .map(|name| (HttpParameterLocation::Query, name.clone())),
            ),
            _ => {}
        }
    }
    if replaced {
        for scheme in root
            .get("components")
            .and_then(|value| value.get("securitySchemes"))
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(Map::values)
        {
            if scheme.get("type").and_then(Value::as_str) != Some("apiKey") {
                continue;
            }
            let Some(name) = scheme.get("name").and_then(Value::as_str) else {
                continue;
            };
            let address = match scheme.get("in").and_then(Value::as_str) {
                Some("header") => (HttpParameterLocation::Header, name.to_ascii_lowercase()),
                Some("query") => (HttpParameterLocation::Query, name.to_owned()),
                Some("cookie") => (HttpParameterLocation::Cookie, name.to_owned()),
                _ => continue,
            };
            addresses.insert(address);
        }
    }
    addresses
}

fn build_http_binding(
    draft: &OperationDraft,
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    source: &str,
    credential_parameters: &BTreeSet<(HttpParameterLocation, String)>,
) -> Result<HttpExecutionBinding, CatalogDiagnostic> {
    let operation_parameters = draft
        .operation
        .contains_key("parameters")
        .then(|| source_child(&draft.operation_pointer, "parameters"));
    let mut parameters = materialize_parameters(
        draft.path_parameters.as_deref(),
        operation_parameters.as_deref(),
        schemas,
        source,
    )?;
    parameters.retain(|parameter| {
        let name = if parameter.location() == HttpParameterLocation::Header {
            parameter.wire_name().to_ascii_lowercase()
        } else {
            parameter.wire_name().to_owned()
        };
        !credential_parameters.contains(&(parameter.location(), name))
    });
    validate_path_parameters(&draft.path, &parameters)?;
    validate_path_query(draft.query.as_deref())?;
    let request_body = draft
        .operation
        .contains_key("requestBody")
        .then(|| source_child(&draft.operation_pointer, "requestBody"))
        .map(|pointer| materialize_request_body(&pointer, schemas, source))
        .transpose()?;
    let responses = materialize_responses(draft, schemas, source)?;
    Ok(HttpExecutionBinding::new(
        draft.method,
        draft.server_url.clone(),
        draft.path.clone(),
        draft.query.clone(),
        HttpRedirectPolicy::Follow,
        parameters,
        request_body,
        responses,
        Vec::new(),
    ))
}

fn materialize_parameters(
    path_parameters: Option<&str>,
    operation_parameters: Option<&str>,
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    source: &str,
) -> Result<Vec<HttpParameterBinding>, CatalogDiagnostic> {
    let mut parameters = BTreeMap::new();
    for (level, pointer) in [
        ("path", path_parameters),
        ("operation", operation_parameters),
    ] {
        let Some(pointer) = pointer else {
            continue;
        };
        let length = schemas
            .value(pointer)
            .and_then(Value::as_array)
            .map(Vec::len)
            .ok_or_else(|| {
                diagnostic(
                    "invalid_openapi_parameter",
                    "an OpenAPI parameters value is not an array",
                )
            })?;
        let mut level_keys = HashSet::new();
        for index in 0..length {
            let parameter_pointer = format!("{pointer}/{index}");
            let parameter = schemas
                .resolve_reference_object(&parameter_pointer, ReferenceObjectKind::Parameter)?;
            let binding = materialize_parameter(
                &parameter,
                schemas,
                &format!("operation:{source}:parameter:{level}:{index}"),
            )?;
            if binding.location() == HttpParameterLocation::Header
                && matches!(
                    binding.wire_name().to_ascii_lowercase().as_str(),
                    "accept" | "content-type" | "authorization"
                )
            {
                continue;
            }
            let wire_name = if binding.location() == HttpParameterLocation::Header {
                binding.wire_name().to_ascii_lowercase()
            } else {
                binding.wire_name().to_string()
            };
            let key = (binding.location(), wire_name);
            if !level_keys.insert(key.clone()) {
                return Err(diagnostic(
                    "invalid_openapi_parameter",
                    "an OpenAPI parameter list contains a duplicate wire address",
                ));
            }
            parameters.insert(key, binding);
        }
    }
    let mut result = parameters.into_values().collect::<Vec<_>>();
    result.sort_by(|left, right| {
        (left.location(), left.wire_name()).cmp(&(right.location(), right.wire_name()))
    });
    Ok(result)
}

fn materialize_parameter(
    parameter: &ResolvedObject,
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    generated_source: &str,
) -> Result<HttpParameterBinding, CatalogDiagnostic> {
    let object = &parameter.object;
    let name = safe_nonempty_string(object, "name", "invalid_openapi_parameter")?;
    let location = match object.get("in").and_then(Value::as_str) {
        Some("path") => HttpParameterLocation::Path,
        Some("query") => HttpParameterLocation::Query,
        Some("header") => HttpParameterLocation::Header,
        Some("cookie") => HttpParameterLocation::Cookie,
        _ => {
            return Err(diagnostic(
                "invalid_openapi_parameter",
                "an OpenAPI parameter has an invalid wire location",
            ));
        }
    };
    let required = bool_field(object, "required", false, "invalid_openapi_parameter")?;
    if location == HttpParameterLocation::Path && !required {
        return Err(diagnostic(
            "invalid_openapi_parameter",
            "an OpenAPI path parameter is not required",
        ));
    }
    let allow_reserved = bool_field(object, "allowReserved", false, "invalid_openapi_parameter")?;
    let allow_empty_value = bool_field(
        object,
        "allowEmptyValue",
        false,
        "invalid_openapi_parameter",
    )?;
    let (style, explode, content_media_type, schema_id) =
        match (object.get("schema"), object.get("content")) {
            (Some(_schema), None) => {
                let style = parameter_style(object.get("style"), location)?;
                let explode = bool_field(
                    object,
                    "explode",
                    style == HttpParameterStyle::Form,
                    "invalid_openapi_parameter",
                )?;
                validate_style_explode(style, explode)?;
                let pointer = source_child(&parameter.pointer, "schema");
                let schema_id = schemas.retain_provider_schema(&pointer)?;
                (Some(style), explode, None, schema_id)
            }
            (None, Some(content)) => {
                let content = content
                    .as_object()
                    .filter(|content| content.len() == 1)
                    .ok_or_else(|| {
                        diagnostic(
                            "invalid_openapi_parameter",
                            "content-based OpenAPI parameters require exactly one media type",
                        )
                    })?;
                let (media_type, media) = content.iter().next().ok_or_else(|| {
                    diagnostic(
                        "invalid_openapi_parameter",
                        "an OpenAPI parameter content map is empty",
                    )
                })?;
                validate_media_type(media_type)?;
                if !supported_content_serialization_media_type(media_type) {
                    return Err(unsupported_parameter_serialization());
                }
                let media = media.as_object().ok_or_else(invalid_media_type_object)?;
                let media_pointer =
                    source_child(&source_child(&parameter.pointer, "content"), media_type);
                let schema_id = if media.contains_key("schema") {
                    schemas.retain_provider_schema(&source_child(&media_pointer, "schema"))?
                } else {
                    schemas.add_generated_schema(
                        &format!("{generated_source}:content-schema"),
                        &Value::Bool(true),
                    )?
                };
                (None, false, Some(media_type.clone()), schema_id)
            }
            _ => {
                return Err(diagnostic(
                    "invalid_openapi_parameter",
                    "an OpenAPI parameter must declare exactly one schema or content map",
                ));
            }
        };
    Ok(HttpParameterBinding::new(
        name.clone(),
        name,
        location,
        required,
        style,
        explode,
        allow_reserved,
        allow_empty_value,
        content_media_type,
        schema_id,
    ))
}

fn parameter_style(
    value: Option<&Value>,
    location: HttpParameterLocation,
) -> Result<HttpParameterStyle, CatalogDiagnostic> {
    let default = match location {
        HttpParameterLocation::Path | HttpParameterLocation::Header => HttpParameterStyle::Simple,
        HttpParameterLocation::Query | HttpParameterLocation::Cookie => HttpParameterStyle::Form,
    };
    let style = match value {
        None => default,
        Some(Value::String(style)) => match style.as_str() {
            "matrix" => HttpParameterStyle::Matrix,
            "label" => HttpParameterStyle::Label,
            "simple" => HttpParameterStyle::Simple,
            "form" => HttpParameterStyle::Form,
            "spaceDelimited" => HttpParameterStyle::SpaceDelimited,
            "pipeDelimited" => HttpParameterStyle::PipeDelimited,
            "deepObject" => HttpParameterStyle::DeepObject,
            _ => return Err(unsupported_parameter_serialization()),
        },
        Some(_) => return Err(unsupported_parameter_serialization()),
    };
    let supported = match location {
        HttpParameterLocation::Path => matches!(
            style,
            HttpParameterStyle::Matrix | HttpParameterStyle::Label | HttpParameterStyle::Simple
        ),
        HttpParameterLocation::Query => matches!(
            style,
            HttpParameterStyle::Form
                | HttpParameterStyle::SpaceDelimited
                | HttpParameterStyle::PipeDelimited
                | HttpParameterStyle::DeepObject
        ),
        HttpParameterLocation::Header => style == HttpParameterStyle::Simple,
        HttpParameterLocation::Cookie => style == HttpParameterStyle::Form,
    };
    supported
        .then_some(style)
        .ok_or_else(unsupported_parameter_serialization)
}

fn validate_style_explode(
    style: HttpParameterStyle,
    explode: bool,
) -> Result<(), CatalogDiagnostic> {
    if style == HttpParameterStyle::DeepObject && !explode {
        return Err(unsupported_parameter_serialization());
    }
    Ok(())
}

fn validate_path_parameters(
    path: &str,
    parameters: &[HttpParameterBinding],
) -> Result<(), CatalogDiagnostic> {
    let mut placeholders = BTreeSet::new();
    let mut characters = path.chars();
    while let Some(character) = characters.next() {
        if character == '}' {
            return Err(unmatched_path_parameter_brace());
        }
        if character != '{' {
            continue;
        }
        let mut name = String::new();
        loop {
            match characters.next() {
                Some('}') if !name.is_empty() => break,
                Some('{' | '}') | None => return Err(unmatched_path_parameter_brace()),
                Some(character) => name.push(character),
            }
        }
        placeholders.insert(name);
    }
    let declared = parameters
        .iter()
        .filter(|parameter| parameter.location() == HttpParameterLocation::Path)
        .map(|parameter| parameter.wire_name().to_string())
        .collect::<BTreeSet<_>>();
    if placeholders != declared {
        return Err(diagnostic(
            "invalid_tool_path",
            "OpenAPI path parameters do not exactly match the path template",
        ));
    }
    Ok(())
}

fn validate_path_query(query: Option<&str>) -> Result<(), CatalogDiagnostic> {
    if query.is_some_and(|query| query.contains(['#', '{', '}'])) {
        return Err(diagnostic(
            "invalid_tool_path",
            "an OpenAPI path query contains a parameter or a fragment",
        ));
    }
    Ok(())
}

fn unmatched_path_parameter_brace() -> CatalogDiagnostic {
    diagnostic(
        "invalid_tool_path",
        "an OpenAPI path template contains an unmatched or empty parameter brace",
    )
}

fn materialize_request_body(
    pointer: &str,
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    source: &str,
) -> Result<HttpRequestBodyBinding, CatalogDiagnostic> {
    let body = schemas.resolve_reference_object(pointer, ReferenceObjectKind::RequestBody)?;
    let required = bool_field(
        &body.object,
        "required",
        false,
        "invalid_openapi_request_body",
    )?;
    let content = body
        .object
        .get("content")
        .and_then(Value::as_object)
        .filter(|content| !content.is_empty())
        .cloned()
        .ok_or_else(|| {
            diagnostic(
                "invalid_openapi_request_body",
                "an OpenAPI request body has no media alternatives",
            )
        })?;
    let mut alternatives = Vec::new();
    for (index, media_type) in content.keys().enumerate() {
        alternatives.push(materialize_content(
            &source_child(&source_child(&body.pointer, "content"), media_type),
            media_type,
            schemas,
            &format!("operation:{source}:request:{index}"),
            true,
        )?);
    }
    alternatives.sort_by(|left, right| left.media_type().cmp(right.media_type()));
    Ok(HttpRequestBodyBinding::new(required, alternatives))
}

fn materialize_responses(
    draft: &OperationDraft,
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    source: &str,
) -> Result<Vec<HttpResponseBinding>, CatalogDiagnostic> {
    let responses = draft
        .operation
        .get("responses")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            diagnostic(
                "invalid_openapi_operation",
                "an OpenAPI Operation has no nonempty responses object",
            )
        })?;
    let response_keys = responses
        .keys()
        .filter(|status| !status.starts_with("x-"))
        .cloned()
        .collect::<Vec<_>>();
    if response_keys.is_empty() {
        return Err(diagnostic(
            "invalid_openapi_operation",
            "an OpenAPI Operation has no nonempty responses object",
        ));
    }
    let mut bindings = Vec::new();
    for (index, response_key) in response_keys.iter().enumerate() {
        let status = response_status(response_key)?;
        let response_pointer = source_child(
            &source_child(&draft.operation_pointer, "responses"),
            response_key,
        );
        let response =
            schemas.resolve_reference_object(&response_pointer, ReferenceObjectKind::Response)?;
        // OpenAPI requires the key, but generators such as drf-spectacular
        // emit it blank and nothing downstream reads it, so only absence
        // or a non-string value rejects the response.
        if !response
            .object
            .get("description")
            .is_some_and(Value::is_string)
        {
            return Err(diagnostic(
                "invalid_openapi_operation",
                "an OpenAPI Response Object has no description",
            ));
        }
        let mut headers = Vec::new();
        if let Some(header_values) = response.object.get("headers") {
            let header_values = header_values.as_object().ok_or_else(|| {
                diagnostic(
                    "invalid_openapi_response_header",
                    "an OpenAPI response headers value is not an object",
                )
            })?;
            for (header_index, name) in header_values.keys().enumerate() {
                headers.push(materialize_header(
                    &source_child(&source_child(&response.pointer, "headers"), name),
                    name,
                    schemas,
                    &format!("operation:{source}:response:{index}:header:{header_index}"),
                )?);
            }
            headers.sort_by(|left, right| left.wire_name().cmp(right.wire_name()));
        }
        let mut content = Vec::new();
        if let Some(media_values) = response.object.get("content") {
            let media_values = media_values.as_object().ok_or_else(|| {
                diagnostic(
                    "invalid_openapi_operation",
                    "an OpenAPI response content value is not an object",
                )
            })?;
            for (media_index, media_type) in media_values.keys().enumerate() {
                content.push(materialize_content(
                    &source_child(&source_child(&response.pointer, "content"), media_type),
                    media_type,
                    schemas,
                    &format!("operation:{source}:response:{index}:content:{media_index}"),
                    false,
                )?);
            }
            content.sort_by(|left, right| left.media_type().cmp(right.media_type()));
        }
        bindings.push(HttpResponseBinding::new(status, headers, content));
    }
    bindings.sort_by_key(HttpResponseBinding::status);
    Ok(bindings)
}

fn materialize_content(
    pointer: &str,
    media_type: &str,
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    generated_source: &str,
    request: bool,
) -> Result<HttpContentBinding, CatalogDiagnostic> {
    validate_media_type(media_type)?;
    let media = schemas
        .value(pointer)
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(invalid_media_type_object)?;
    let schema_id = if media.contains_key("schema") {
        schemas.retain_provider_schema(&source_child(pointer, "schema"))?
    } else {
        schemas.add_generated_schema(
            &format!("{generated_source}:implicit-schema"),
            &Value::Bool(true),
        )?
    };
    if request
        && !supported_request_media_type(media_type)
        && (media_type.contains('*')
            || !crate::schema::is_binary_schema(&schema_id, |id| schemas.schema(id)))
    {
        return Err(unsupported_http_request_media_type());
    }
    if request
        && media_type_essence(media_type).starts_with("text/")
        && !schema_is_text_serializable(schemas, &schema_id, &mut HashSet::new())
    {
        return Err(unsupported_http_request_media_type());
    }
    let mut encoding = Vec::new();
    if let Some(encodings) = media.get("encoding") {
        if !request
            || !matches!(
                media_type.split(';').next().map(str::trim),
                Some("multipart/form-data" | "application/x-www-form-urlencoded")
            )
        {
            return Err(diagnostic(
                "unsupported_openapi_encoding",
                "OpenAPI encoding is supported only for form and multipart requests",
            ));
        }
        let encodings = encodings.as_object().ok_or_else(|| {
            diagnostic(
                "invalid_openapi_encoding",
                "an OpenAPI encoding value is not an object",
            )
        })?;
        for property in encodings.keys() {
            materialize_encoding(
                &source_child(&source_child(pointer, "encoding"), property),
                property,
                schemas,
                generated_source,
                media_type,
            )
            .map(|binding| encoding.push(binding))?;
        }
        encoding.sort_by(|left, right| left.property_name().cmp(right.property_name()));
    }
    Ok(HttpContentBinding::new(
        media_type.to_string(),
        schema_id,
        encoding,
    ))
}

fn materialize_encoding(
    pointer: &str,
    property: &str,
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    generated_source: &str,
    parent_media_type: &str,
) -> Result<HttpEncodingBinding, CatalogDiagnostic> {
    let encoding = schemas
        .value(pointer)
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| {
            diagnostic(
                "invalid_openapi_encoding",
                "an OpenAPI Encoding Object is not an object",
            )
        })?;
    let content_type = optional_safe_string(&encoding, "contentType", "invalid_openapi_encoding")?;
    if let Some(content_type) = &content_type
        && media_type_essence(parent_media_type) == "application/x-www-form-urlencoded"
    {
        validate_media_type(content_type)?;
        if !supported_content_serialization_media_type(content_type) {
            return Err(unsupported_http_request_media_type());
        }
    }
    let style = encoding
        .get("style")
        .map(|style| parameter_style(Some(style), HttpParameterLocation::Query))
        .transpose()?;
    let explode = bool_field(
        &encoding,
        "explode",
        style.is_none_or(|style| style == HttpParameterStyle::Form),
        "invalid_openapi_encoding",
    )?;
    if let Some(style) = style {
        validate_style_explode(style, explode)?;
    }
    let allow_reserved = bool_field(
        &encoding,
        "allowReserved",
        false,
        "invalid_openapi_encoding",
    )?;
    let mut headers = Vec::new();
    if let Some(values) = encoding.get("headers") {
        let values = values.as_object().ok_or_else(|| {
            diagnostic(
                "invalid_openapi_encoding",
                "an OpenAPI encoding headers value is not an object",
            )
        })?;
        for (index, name) in values.keys().enumerate() {
            headers.push(materialize_header(
                &source_child(&source_child(pointer, "headers"), name),
                name,
                schemas,
                &format!("{generated_source}:encoding:{property}:header:{index}"),
            )?);
        }
    }
    Ok(HttpEncodingBinding::new(
        property.to_string(),
        content_type,
        headers,
        style,
        explode,
        allow_reserved,
    ))
}

fn materialize_header(
    pointer: &str,
    name: &str,
    schemas: &mut OpenApiSchemaGraphBuilder<'_>,
    generated_source: &str,
) -> Result<HttpHeaderBinding, CatalogDiagnostic> {
    if name.is_empty() || name.chars().any(char::is_control) {
        return Err(diagnostic(
            "invalid_openapi_response_header",
            "an OpenAPI response header has an unsafe name",
        ));
    }
    let header = schemas.resolve_reference_object(pointer, ReferenceObjectKind::Header)?;
    let required = bool_field(
        &header.object,
        "required",
        false,
        "invalid_openapi_response_header",
    )?;
    let (style, explode, content_media_type, schema_id) =
        match (header.object.get("schema"), header.object.get("content")) {
            (Some(_schema), None) => {
                let style =
                    parameter_style(header.object.get("style"), HttpParameterLocation::Header)?;
                let explode = bool_field(
                    &header.object,
                    "explode",
                    false,
                    "invalid_openapi_response_header",
                )?;
                let schema_id =
                    schemas.retain_provider_schema(&source_child(&header.pointer, "schema"))?;
                (Some(style), explode, None, schema_id)
            }
            (None, Some(content)) => {
                let content = content
                    .as_object()
                    .filter(|content| content.len() == 1)
                    .ok_or_else(|| {
                        diagnostic(
                            "invalid_openapi_response_header",
                            "content-based OpenAPI headers require exactly one media type",
                        )
                    })?;
                let (media_type, media) = content.iter().next().ok_or_else(|| {
                    diagnostic(
                        "invalid_openapi_response_header",
                        "an OpenAPI header content map is empty",
                    )
                })?;
                validate_media_type(media_type)?;
                let media = media.as_object().ok_or_else(invalid_media_type_object)?;
                let media_pointer =
                    source_child(&source_child(&header.pointer, "content"), media_type);
                let schema_id = if media.contains_key("schema") {
                    schemas.retain_provider_schema(&source_child(&media_pointer, "schema"))?
                } else {
                    schemas.add_generated_schema(
                        &format!("{generated_source}:content-schema"),
                        &Value::Bool(true),
                    )?
                };
                (None, false, Some(media_type.clone()), schema_id)
            }
            _ => {
                return Err(diagnostic(
                    "invalid_openapi_response_header",
                    "an OpenAPI header must declare exactly one schema or content map",
                ));
            }
        };
    Ok(HttpHeaderBinding::new(
        name.to_string(),
        required,
        style,
        explode,
        content_media_type,
        schema_id,
    ))
}

fn build_input_schema(binding: &HttpExecutionBinding) -> Result<Value, CatalogDiagnostic> {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for parameter in binding.parameters() {
        if properties
            .insert(
                parameter.logical_name().to_string(),
                serde_json::json!({"$ref": parameter.schema().as_str()}),
            )
            .is_some()
        {
            return Err(diagnostic(
                "unsupported_logical_input_collision",
                "OpenAPI parameters collide in the flat logical input schema",
            ));
        }
        if parameter.required() {
            required.push(Value::String(parameter.logical_name().to_string()));
        }
    }
    if let Some(body) = binding.request_body() {
        if properties.contains_key("body") {
            return Err(diagnostic(
                "unsupported_logical_input_collision",
                "an OpenAPI parameter collides with the logical request body",
            ));
        }
        let body_schema = match body.content() {
            [] => Value::Bool(true),
            [content] => serde_json::json!({"$ref": content.schema().as_str()}),
            alternatives => Value::Object(Map::from_iter([(
                "oneOf".to_string(),
                Value::Array(
                    alternatives
                        .iter()
                        .map(|content| {
                            serde_json::json!({
                                "additionalProperties": false,
                                "properties": {
                                    "data": {"$ref": content.schema().as_str()},
                                    "mediaType": {"const": content.media_type()},
                                },
                                "required": ["data", "mediaType"],
                                "type": "object",
                            })
                        })
                        .collect(),
                ),
            )])),
        };
        properties.insert("body".to_string(), body_schema);
        if body.required() {
            required.push(Value::String("body".to_string()));
        }
    }
    required.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    Ok(Value::Object(Map::from_iter([
        ("additionalProperties".to_string(), Value::Bool(false)),
        ("properties".to_string(), Value::Object(properties)),
        ("required".to_string(), Value::Array(required)),
        ("type".to_string(), Value::String("object".to_string())),
    ])))
}

/// The one credential contract every Tool shares. Each Tool keeps
/// the security-scheme name its document gave it, as provenance only.
#[derive(Default)]
pub(super) struct CredentialSelection {
    shared: Vec<crate::HttpCredentialSlot>,
    by_operation: BTreeMap<ToolKey, Vec<crate::HttpCredentialSlot>>,
}

impl CredentialSelection {
    fn for_tool(&self, key: &ToolKey) -> Vec<crate::HttpCredentialSlot> {
        self.by_operation.get(key).unwrap_or(&self.shared).clone()
    }
}

pub(super) fn validate_credential_contract(
    root: &Map<String, Value>,
    document: &Value,
) -> Result<CredentialSelection, CatalogDiagnostic> {
    let security_schemes = root
        .get("components")
        .and_then(Value::as_object)
        .and_then(|components| components.get("securitySchemes"));
    if let Some(security_schemes) = security_schemes {
        let credential_schemes = security_schemes.as_object().ok_or_else(|| {
            diagnostic(
                "invalid_credential_contract",
                "OpenAPI components.securitySchemes is not an object",
            )
        })?;
        for (name, scheme) in credential_schemes {
            let mut resolving = HashSet::new();
            resolving.insert(name.clone());
            validate_security_scheme(scheme, credential_schemes, &mut resolving)?;
        }
    }
    // Differently named schemes that produce the same contract need the same
    // credential, so only contracts decide compatibility.
    let mut selected: Option<Vec<crate::HttpCredentialSlot>> = None;
    let mut by_operation = BTreeMap::new();
    for operation in super::operations::locate_operations(root, document)?.operations {
        let slots = select_credentials(
            operation.object.get("security").or(root.get("security")),
            security_schemes,
        )?;
        selected = Some(match selected {
            None => slots.clone(),
            Some(previous) => shared_contracts(&previous, &slots).map_err(|conflict| {
                let (message, detail) = match conflict {
                    ContractConflict::Different => (
                        "the Provider's Tools require different credential contracts",
                        format!(
                            "{} differs from {}",
                            scheme_names(&slots),
                            scheme_names(&previous)
                        ),
                    ),
                    ContractConflict::ScopeLimit => (
                        "the Provider's Operations together request more OAuth scopes than \
                         one grant can hold",
                        format!(
                            "{} and {} together exceed the scope limit",
                            scheme_names(&slots),
                            scheme_names(&previous)
                        ),
                    ),
                };
                diagnostic("credentials_not_supported", message)
                    .at_location(&format!("{}: {detail}", operation.pointer))
            })?,
        });
        by_operation.insert(operation.key, slots);
    }
    let shared = selected.unwrap_or_default();
    // Every Tool retains the shared contract under its own scheme name,
    // so one account grant covers every scope the Tools ask for.
    for slots in by_operation.values_mut() {
        for (slot, shared) in slots.iter_mut().zip(&shared) {
            *slot = crate::HttpCredentialSlot::new(
                slot.scheme_name().to_owned(),
                shared.contract().clone(),
            );
        }
    }
    Ok(CredentialSelection {
        shared,
        by_operation,
    })
}

/// Why a Tool cannot join the credential contract its predecessors share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContractConflict {
    /// The contracts differ in more than their OAuth scopes.
    Different,
    /// The contracts agree, but one grant cannot request every scope they ask for.
    ScopeLimit,
}

/// The one contract the Tools so far and the next one can share: their
/// common contract, or one OAuth contract requesting every scope when they
/// differ only in scopes.
fn shared_contracts(
    previous: &[crate::HttpCredentialSlot],
    next: &[crate::HttpCredentialSlot],
) -> Result<Vec<crate::HttpCredentialSlot>, ContractConflict> {
    if same_contracts(previous, next) {
        return Ok(previous.to_vec());
    }
    let ([previous], [next]) = (previous, next) else {
        return Err(ContractConflict::Different);
    };
    let merged = union_oauth_scopes(
        previous.contract().authentication(),
        next.contract().authentication(),
    )?;
    Ok(vec![crate::HttpCredentialSlot::new(
        previous.scheme_name().to_owned(),
        merged.into(),
    )])
}

/// One authorization-code contract requesting both scope sets, when the two
/// agree on everything but the scopes.
fn union_oauth_scopes(
    left: &AuthenticationConfiguration,
    right: &AuthenticationConfiguration,
) -> Result<AuthenticationConfiguration, ContractConflict> {
    let mut merged = left.clone();
    let mut other = right.clone();
    let (
        AuthenticationConfiguration::OAuthAuthorizationCode { scopes, .. },
        AuthenticationConfiguration::OAuthAuthorizationCode {
            scopes: other_scopes,
            ..
        },
    ) = (&mut merged, &mut other)
    else {
        return Err(ContractConflict::Different);
    };
    scopes.append(other_scopes);
    // Each list is already normalized, so only the union counts against the
    // scope limit, and the limit is the only check the union can fail.
    scopes.sort();
    scopes.dedup();
    *scopes = super::authentication::scopes(scopes)
        .map_err(|_over_limit| ContractConflict::ScopeLimit)?;
    other_scopes.clone_from(scopes);
    if merged == other {
        Ok(merged)
    } else {
        Err(ContractConflict::Different)
    }
}

fn same_contracts(left: &[crate::HttpCredentialSlot], right: &[crate::HttpCredentialSlot]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.contract() == right.contract())
}

fn scheme_names(slots: &[crate::HttpCredentialSlot]) -> String {
    if slots.is_empty() {
        return "no security scheme".into();
    }
    slots
        .iter()
        .map(crate::HttpCredentialSlot::scheme_name)
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn select_credentials(
    security: Option<&Value>,
    schemes: Option<&Value>,
) -> Result<Vec<crate::HttpCredentialSlot>, CatalogDiagnostic> {
    if credential_free(security) {
        return Ok(Vec::new());
    }
    let requirements = security
        .and_then(Value::as_array)
        .ok_or_else(invalid_credential_contract)?;
    let [requirement] = requirements.as_slice() else {
        return Err(credentials_not_supported());
    };
    let requirement = requirement
        .as_object()
        .ok_or_else(invalid_credential_contract)?;
    // Schemes in one requirement are all required together. Coral can send
    // several only as API keys that all travel the same way.
    if requirement.len() > 1 {
        let names = requirement.keys().cloned().collect::<Vec<_>>().join("+");
        return Ok(vec![crate::HttpCredentialSlot::new(
            names,
            api_key_contract(api_keys(requirement, schemes)?).into(),
        )]);
    }
    let (name, scopes) = requirement
        .iter()
        .next()
        .ok_or_else(invalid_credential_contract)?;
    let scheme = resolve_scheme(name, schemes)?;
    let scopes = scopes.as_array().ok_or_else(invalid_credential_contract)?;
    let scopes = scopes
        .iter()
        .map(|scope| {
            scope
                .as_str()
                .map(str::to_owned)
                .ok_or_else(invalid_credential_contract)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let scopes = super::authentication::scopes(&scopes)?;
    let contract = match scheme.get("type").and_then(Value::as_str) {
        Some("http")
            if scheme.get("scheme").and_then(Value::as_str) == Some("bearer")
                && scopes.is_empty() =>
        {
            AuthenticationConfiguration::Bearer
        }
        Some("http")
            if scheme
                .get("scheme")
                .and_then(Value::as_str)
                .is_some_and(|scheme| scheme.eq_ignore_ascii_case("basic"))
                && scopes.is_empty() =>
        {
            AuthenticationConfiguration::BasicAuth {
                username: crate::CredentialSource::Field {
                    field: "username".into(),
                },
                password: crate::CredentialSource::Field {
                    field: "password".into(),
                },
            }
        }
        Some("apiKey") => api_key_contract(api_keys(requirement, schemes)?),
        Some("oauth2") => oauth_contract(scheme, scopes)?,
        _ => return Err(credentials_not_supported()),
    };
    Ok(vec![crate::HttpCredentialSlot::new(
        name.clone(),
        contract.into(),
    )])
}

/// One API key a document requires, named as a Credential Field.
pub(super) struct ApiKey {
    pub(super) field: String,
    /// The header name in lowercase, or the exact query parameter name.
    pub(super) name: String,
    pub(super) scheme: String,
    pub(super) description: String,
}

/// The API keys one requirement names, and whether they travel in the query
/// rather than in headers.
pub(super) struct ApiKeys {
    pub(super) keys: Vec<ApiKey>,
    pub(super) in_query: bool,
}

/// Every scheme in one requirement, when each is an API key and all of them
/// travel the same way: in headers, or in the query.
pub(super) fn api_keys(
    requirement: &Map<String, Value>,
    schemes: Option<&Value>,
) -> Result<ApiKeys, CatalogDiagnostic> {
    if requirement.len() > super::authentication::MAX_BINDINGS {
        return Err(credentials_not_supported());
    }
    let mut keys = Vec::with_capacity(requirement.len());
    let mut in_query = None;
    for (name, scopes) in requirement {
        let scheme = resolve_scheme(name, schemes)?;
        let query = match scheme.get("in").and_then(Value::as_str) {
            Some("header") => false,
            Some("query") => true,
            _ => return Err(credentials_not_supported()),
        };
        if scheme.get("type").and_then(Value::as_str) != Some("apiKey")
            || scopes.as_array().is_none_or(|scopes| !scopes.is_empty())
            || *in_query.get_or_insert(query) != query
        {
            return Err(credentials_not_supported());
        }
        let declared = scheme
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(invalid_credential_contract)?;
        let sent = if query {
            super::authentication::query_parameter(declared)?
        } else {
            super::authentication::header(declared)?
        };
        let field =
            super::authentication::field_name_for(name).ok_or_else(credentials_not_supported)?;
        if keys
            .iter()
            .any(|key: &ApiKey| key.name == sent || key.field == field)
        {
            return Err(credentials_not_supported());
        }
        // The scheme name becomes the field's label, so it must be presentable.
        if name.is_empty()
            || name.len() > super::authentication::MAX_FIELD_LABEL_BYTES
            || name.chars().any(char::is_control)
        {
            return Err(credentials_not_supported());
        }
        keys.push(ApiKey {
            field,
            name: sent,
            scheme: name.clone(),
            description: scheme
                .get("description")
                .and_then(Value::as_str)
                .filter(|text| {
                    text.len() <= super::authentication::MAX_FIELD_DESCRIPTION_BYTES
                        && !text.chars().any(char::is_control)
                })
                .unwrap_or_default()
                .to_owned(),
        });
    }
    Ok(ApiKeys {
        keys,
        in_query: in_query.unwrap_or_default(),
    })
}

/// Each key sent alone, as header credentials or query credentials.
fn api_key_contract(keys: ApiKeys) -> AuthenticationConfiguration {
    if keys.in_query {
        AuthenticationConfiguration::QueryCredentials {
            parameters: keys
                .keys
                .into_iter()
                .map(|key| {
                    (
                        key.name,
                        crate::CredentialSource::Field { field: key.field },
                    )
                })
                .collect(),
        }
    } else {
        AuthenticationConfiguration::HeaderCredentials {
            headers: keys
                .keys
                .into_iter()
                .map(|key| {
                    (
                        key.name,
                        crate::HeaderBinding::Field {
                            field: key.field,
                            prefix: String::new(),
                        },
                    )
                })
                .collect(),
        }
    }
}

fn resolve_scheme<'a>(
    name: &str,
    schemes: Option<&'a Value>,
) -> Result<&'a Value, CatalogDiagnostic> {
    let mut scheme = schemes
        .and_then(|schemes| schemes.get(name))
        .ok_or_else(invalid_credential_contract)?;
    let mut seen = HashSet::new();
    while let Some(reference) = scheme.get("$ref") {
        let reference = reference
            .as_str()
            .and_then(|value| value.strip_prefix("#/components/securitySchemes/"))
            .ok_or_else(invalid_credential_contract)?;
        if !seen.insert(reference) {
            return Err(invalid_credential_contract());
        }
        scheme = schemes
            .and_then(|schemes| schemes.get(reference.replace("~1", "/").replace("~0", "~")))
            .ok_or_else(invalid_credential_contract)?;
    }
    Ok(scheme)
}

fn oauth_contract(
    scheme: &Value,
    scopes: Vec<String>,
) -> Result<crate::AuthenticationConfiguration, CatalogDiagnostic> {
    let flow = scheme
        .pointer("/flows/authorizationCode")
        .ok_or_else(credentials_not_supported)?;
    let endpoint = |field| -> Result<String, CatalogDiagnostic> {
        let value = flow
            .get(field)
            .and_then(Value::as_str)
            .ok_or_else(invalid_credential_contract)?;
        super::authentication::endpoint(value)
    };
    let declared = flow
        .get("scopes")
        .and_then(Value::as_object)
        .ok_or_else(invalid_credential_contract)?;
    if scopes.iter().any(|scope| !declared.contains_key(scope)) {
        return Err(invalid_credential_contract());
    }
    Ok(AuthenticationConfiguration::OAuthAuthorizationCode {
        authorization_url: endpoint("authorizationUrl")?,
        token_url: endpoint("tokenUrl")?,
        scopes,
        scope_format: crate::OAuthScopeFormat::Space,
        resource: None,
        issuer: None,
        client: None,
    })
}

fn validate_security_scheme(
    scheme: &Value,
    schemes: &Map<String, Value>,
    resolving: &mut HashSet<String>,
) -> Result<(), CatalogDiagnostic> {
    let object = scheme.as_object().ok_or_else(invalid_credential_contract)?;
    let Some(reference) = object.get("$ref") else {
        if !object.get("type").is_some_and(Value::is_string) {
            return Err(diagnostic(
                "invalid_credential_contract",
                "an OpenAPI Security Scheme has no type",
            ));
        }
        return Ok(());
    };
    let reference = reference
        .as_str()
        .and_then(|reference| reference.strip_prefix("#/components/securitySchemes/"))
        .ok_or_else(invalid_credential_contract)?;
    let name = reference.replace("~1", "/").replace("~0", "~");
    if !resolving.insert(name.clone()) {
        return Err(invalid_credential_contract());
    }
    let target = schemes.get(&name).ok_or_else(invalid_credential_contract)?;
    validate_security_scheme(target, schemes, resolving)?;
    resolving.remove(&name);
    Ok(())
}

fn invalid_credential_contract() -> CatalogDiagnostic {
    diagnostic(
        "invalid_credential_contract",
        "an OpenAPI Security Scheme or reference is invalid",
    )
}

fn credential_free(security: Option<&Value>) -> bool {
    security.is_none_or(|value| {
        value.as_array().is_some_and(|requirements| {
            requirements.is_empty()
                || requirements
                    .iter()
                    .any(|requirement| requirement.as_object().is_some_and(Map::is_empty))
        })
    })
}

fn credentials_not_supported() -> CatalogDiagnostic {
    diagnostic(
        "credentials_not_supported",
        "the Provider requires an unsupported or ambiguous credential contract",
    )
}

pub(super) fn validate_version(
    root: &Map<String, Value>,
) -> Result<SchemaSemantics, CatalogDiagnostic> {
    let version = root.get("openapi").and_then(Value::as_str).ok_or_else(|| {
        diagnostic(
            "invalid_openapi_document",
            "the pinned OpenAPI document has no OpenAPI version",
        )
    })?;
    let parts = version.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        ["3", "0", patch]
            if !patch.is_empty() && patch.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            Ok(SchemaSemantics::OpenApiV3_0)
        }
        ["3", "1", patch]
            if !patch.is_empty() && patch.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            Ok(SchemaSemantics::JsonSchema2020_12)
        }
        _ => Err(diagnostic(
            "unsupported_openapi_version",
            "the pinned OpenAPI document does not use a valid OpenAPI 3.0 or 3.1 version",
        )),
    }
}

pub(super) fn provider_display_name(
    root: &Map<String, Value>,
) -> Result<String, CatalogDiagnostic> {
    let info = root.get("info").and_then(Value::as_object).ok_or_else(|| {
        diagnostic(
            "invalid_provider_metadata",
            "the pinned OpenAPI document has no info object",
        )
    })?;
    let version_is_valid = info
        .get("version")
        .and_then(Value::as_str)
        .is_some_and(|version| {
            !version.trim().is_empty() && !version.chars().any(char::is_control)
        });
    if !version_is_valid {
        return Err(diagnostic(
            "invalid_provider_metadata",
            "the pinned OpenAPI document has no safe nonempty info.version",
        ));
    }
    let title = info
        .get("title")
        .and_then(Value::as_str)
        .filter(|title| {
            !title.trim().is_empty() && !title.chars().any(char::is_control) && title.len() <= 256
        })
        .ok_or_else(|| {
            diagnostic(
                "invalid_provider_metadata",
                "the pinned OpenAPI document has no safe nonempty info.title",
            )
        })?;
    Ok(title.to_string())
}

fn selected_server(object: &Map<String, Value>) -> Result<Option<String>, CatalogDiagnostic> {
    let Some(servers) = object.get("servers") else {
        return Ok(None);
    };
    let servers = servers.as_array().ok_or_else(|| {
        diagnostic(
            "invalid_openapi_server",
            "an OpenAPI servers value is not an array",
        )
    })?;
    if servers.is_empty() {
        return Err(diagnostic(
            "invalid_openapi_server",
            "an explicit OpenAPI servers array cannot be empty",
        ));
    }
    if servers.len() != 1 {
        return Err(diagnostic(
            "unsupported_server_selection",
            "the OpenAPI profile requires exactly one server at each selected level",
        ));
    }
    let server = servers.first().and_then(Value::as_object).ok_or_else(|| {
        diagnostic(
            "invalid_openapi_server",
            "the selected OpenAPI server is not an object",
        )
    })?;
    let template = server.get("url").and_then(Value::as_str).ok_or_else(|| {
        diagnostic(
            "invalid_openapi_server",
            "the selected OpenAPI server has no URL",
        )
    })?;
    let expanded = expand_server_template(template, server.get("variables"))?;
    let url = url::Url::parse(&expanded).map_err(|_error| {
        diagnostic(
            "invalid_openapi_server",
            "the selected OpenAPI server URL is not absolute",
        )
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(diagnostic(
            "invalid_openapi_server",
            "the selected OpenAPI server URL is not HTTP or HTTPS",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(credentials_not_supported());
    }
    Ok(Some(url.to_string()))
}

fn expand_server_template(
    template: &str,
    variables: Option<&Value>,
) -> Result<String, CatalogDiagnostic> {
    let mut expanded = template.to_string();
    let variables = match variables {
        None => Map::new(),
        Some(variables) => variables.as_object().cloned().ok_or_else(|| {
            diagnostic(
                "invalid_openapi_server",
                "OpenAPI server variables are not an object",
            )
        })?,
    };
    for (name, variable) in variables {
        let variable = variable.as_object().ok_or_else(|| {
            diagnostic(
                "invalid_openapi_server",
                "an OpenAPI server variable is not an object",
            )
        })?;
        let default = variable
            .get("default")
            .and_then(Value::as_str)
            .filter(|value| !value.chars().any(char::is_control))
            .ok_or_else(|| {
                diagnostic(
                    "invalid_openapi_server",
                    "an OpenAPI server variable has no safe string default",
                )
            })?;
        if let Some(enumeration) = variable.get("enum") {
            let enumeration = enumeration.as_array().ok_or_else(|| {
                diagnostic(
                    "invalid_openapi_server",
                    "an OpenAPI server variable enum is not an array",
                )
            })?;
            if !enumeration
                .iter()
                .any(|value| value.as_str() == Some(default))
            {
                return Err(diagnostic(
                    "invalid_openapi_server",
                    "an OpenAPI server variable default is outside its enum",
                ));
            }
        }
        expanded = expanded.replace(&format!("{{{name}}}"), default);
    }
    if expanded.contains(['{', '}']) {
        return Err(diagnostic(
            "invalid_openapi_server",
            "an OpenAPI server URL has an unresolved variable",
        ));
    }
    Ok(expanded)
}

fn response_status(value: &str) -> Result<HttpResponseStatus, CatalogDiagnostic> {
    if !response_key_is_legal(value) {
        return Err(invalid_response_status());
    }
    if value == "default" {
        return Ok(HttpResponseStatus::Default);
    }
    let bytes = value.as_bytes();
    if bytes.iter().all(u8::is_ascii_digit) {
        let status = value
            .parse::<u16>()
            .map_err(|_error| invalid_response_status())?;
        return Ok(HttpResponseStatus::Code(status));
    }
    let range = bytes.first().copied().ok_or_else(invalid_response_status)? - b'0';
    Ok(HttpResponseStatus::Range(u16::from(range)))
}

fn bool_field(
    object: &Map<String, Value>,
    name: &str,
    default: bool,
    code: &'static str,
) -> Result<bool, CatalogDiagnostic> {
    match object.get(name) {
        None => Ok(default),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(diagnostic(
            code,
            "an OpenAPI boolean field has an invalid value",
        )),
    }
}

fn safe_nonempty_string(
    object: &Map<String, Value>,
    name: &str,
    code: &'static str,
) -> Result<String, CatalogDiagnostic> {
    object
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .map(str::to_string)
        .ok_or_else(|| diagnostic(code, "an OpenAPI string field is missing or unsafe"))
}

fn optional_safe_string(
    object: &Map<String, Value>,
    name: &str,
    code: &'static str,
) -> Result<Option<String>, CatalogDiagnostic> {
    object
        .get(name)
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
                .map(str::to_string)
                .ok_or_else(|| diagnostic(code, "an OpenAPI string field is unsafe"))
        })
        .transpose()
}

fn validate_media_type(value: &str) -> Result<(), CatalogDiagnostic> {
    let essence = value.split(';').next().map(str::trim).unwrap_or_default();
    let mut parts = essence.split('/');
    let valid = parts
        .next()
        .is_some_and(|part| !part.is_empty() && !part.chars().any(char::is_control))
        && parts
            .next()
            .is_some_and(|part| !part.is_empty() && !part.chars().any(char::is_control))
        && parts.next().is_none();
    valid.then_some(()).ok_or_else(|| {
        diagnostic(
            "invalid_openapi_media_type",
            "an OpenAPI media type is invalid",
        )
    })
}

fn safe_summary(operation: &Map<String, Value>, method: HttpMethod, path: &str) -> String {
    operation
        .get("summary")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|summary| crate::types::valid_presentation_text(summary) && summary.len() <= 1024)
        .map_or_else(
            || format!("{} {path}", method.as_str().to_ascii_uppercase()),
            str::to_string,
        )
}

fn operation_presentation(operation: &Map<String, Value>, summary: &str) -> ToolPresentation {
    let title = safe_presentation_text(operation.get("summary")).map(str::to_string);
    let description = safe_presentation_text(operation.get("description"))
        .unwrap_or(summary)
        .to_string();
    let deprecated = operation
        .get("deprecated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let tags = operation
        .get("tags")
        .and_then(Value::as_array)
        .filter(|tags| tags.len() <= 64)
        .map(|tags| {
            tags.iter()
                .map(|tag| safe_presentation_text(Some(tag)).map(str::to_string))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default()
        })
        .unwrap_or_default();
    let guidance = operation
        .get("externalDocs")
        .and_then(Value::as_object)
        .and_then(|external| safe_presentation_text(external.get("description")))
        .map(str::to_string);
    ToolPresentation::new(title, description, deprecated, tags, guidance)
}

fn safe_presentation_text(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| crate::types::valid_presentation_text(value))
}

fn operation_schema_source(key: &ToolKey) -> String {
    let digest = Sha256::digest(key.as_str().as_bytes());
    digest
        .iter()
        .take(12)
        .fold(String::with_capacity(24), |mut encoded, byte| {
            write!(encoded, "{byte:02x}").expect("writing hexadecimal to a String cannot fail");
            encoded
        })
}

fn serialize_catalog_value(value: &Value) -> Result<String, CatalogDiagnostic> {
    serde_json::to_string(&canonicalize(value)).map_err(|_error| {
        diagnostic(
            "invalid_openapi_operation",
            "an OpenAPI Operation cannot be retained as canonical JSON",
        )
    })
}

fn media_type_essence(media_type: &str) -> String {
    media_type
        .split(';')
        .next()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn supported_content_serialization_media_type(media_type: &str) -> bool {
    let essence = media_type_essence(media_type);
    essence == "application/json" || essence.ends_with("+json") || essence.starts_with("text/")
}

fn supported_request_media_type(media_type: &str) -> bool {
    let essence = media_type_essence(media_type);
    essence == "application/json"
        || essence.ends_with("+json")
        || essence.starts_with("text/")
        || matches!(
            essence.as_str(),
            "application/x-www-form-urlencoded"
                | "multipart/form-data"
                | "application/octet-stream"
        )
}

fn schema_is_text_serializable(
    schemas: &OpenApiSchemaGraphBuilder<'_>,
    schema_id: &SchemaId,
    resolving: &mut HashSet<SchemaId>,
) -> bool {
    if !resolving.insert(schema_id.clone()) {
        return false;
    }
    let serializable = schemas
        .schema(schema_id)
        .is_some_and(|schema| schema_value_is_text_serializable(schemas, schema, resolving));
    resolving.remove(schema_id);
    serializable
}

fn schema_value_is_text_serializable(
    schemas: &OpenApiSchemaGraphBuilder<'_>,
    schema: &Value,
    resolving: &mut HashSet<SchemaId>,
) -> bool {
    let Some(schema) = schema.as_object() else {
        return false;
    };
    if schemas.semantics().nullable_applies() && crate::schema::declares_nullable(schema) {
        return false;
    }
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let referenced = SchemaId::from_storage(reference.to_string()).ok();
        return referenced.is_some_and(|referenced| {
            schema_is_text_serializable(schemas, &referenced, resolving)
        });
    }
    match schema.get("type") {
        Some(Value::String(value)) => value == "string",
        Some(Value::Array(values)) => {
            matches!(values.as_slice(), [Value::String(value)] if value == "string")
        }
        Some(_) => false,
        None => {
            schema.get("const").is_some_and(Value::is_string)
                || schema
                    .get("enum")
                    .and_then(Value::as_array)
                    .is_some_and(|values| !values.is_empty() && values.iter().all(Value::is_string))
                || schema
                    .get("allOf")
                    .and_then(Value::as_array)
                    .is_some_and(|values| {
                        values.iter().any(|value| {
                            schema_value_is_text_serializable(schemas, value, resolving)
                        })
                    })
                || ["anyOf", "oneOf"].iter().any(|keyword| {
                    schema
                        .get(*keyword)
                        .and_then(Value::as_array)
                        .is_some_and(|values| {
                            !values.is_empty()
                                && values.iter().all(|value| {
                                    schema_value_is_text_serializable(schemas, value, resolving)
                                })
                        })
                })
        }
    }
}

fn invalid_media_type_object() -> CatalogDiagnostic {
    diagnostic(
        "invalid_openapi_media_type",
        "an OpenAPI Media Type Object is not an object",
    )
}

fn invalid_response_status() -> CatalogDiagnostic {
    diagnostic(
        "invalid_openapi_operation",
        "an OpenAPI response has an invalid status key",
    )
}

fn invalid_tool_index() -> CatalogDiagnostic {
    diagnostic(
        "invalid_tool_path_index",
        "the Materialized Tool Catalog has an incoherent path index",
    )
}

fn unsupported_http_request_media_type() -> CatalogDiagnostic {
    diagnostic(
        "unsupported_http_request_media_type",
        "an OpenAPI request body uses a media type without a faithful v1 encoder",
    )
}

fn unsupported_parameter_serialization() -> CatalogDiagnostic {
    diagnostic(
        "unsupported_parameter_serialization",
        "an OpenAPI parameter uses a serialization style that is invalid for its location",
    )
}

fn diagnostic(code: &'static str, message: &'static str) -> CatalogDiagnostic {
    CatalogDiagnostic::stable(code, message)
}
