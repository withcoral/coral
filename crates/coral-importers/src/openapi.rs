use std::collections::{BTreeMap, BTreeSet};

use coral_capabilities::{
    Capability, CapabilityDisplay, Diagnostic, DiagnosticSeverity, DiagnosticStage, HttpMethod,
    InvocationSchema, OutputContract, ProviderOrigin, ProviderOriginKind, RestOutputVariant,
    RestParameterBinding, RestParameterLocation, RestRequestBody, RestResponseVariant,
    RestUpstreamBinding, ShapeHints, SourceId, StatusRange, SupportStatus, UpstreamBinding,
};
use coral_spec::{OpenApiInterface, SourceSpec, openapi_document_metadata_from_value};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};

use crate::auth::credential_requirements;
use crate::hash::sha256_hex;
use crate::naming::OperationIdAllocator;
use crate::schema_shape::{schema_shape_view, shape_hints_from_json_schema};
use crate::{
    ImportedInterface, ImporterError, ProviderSnapshotArtifact, RawInterfaceInput, Result,
};

#[expect(
    clippy::too_many_lines,
    reason = "OpenAPI import intentionally walks one provider document pass while path, operation, response, and security context are all live"
)]
pub(super) fn import_openapi(
    source_id: &SourceId,
    spec: &SourceSpec,
    interface: &OpenApiInterface,
    raw_inputs: &BTreeMap<String, RawInterfaceInput>,
) -> Result<ImportedInterface> {
    let raw = raw_inputs
        .get(&interface.id)
        .ok_or_else(|| ImporterError::MissingRawInput(interface.id.clone()))?;
    let RawInterfaceInput::OpenApiDocument { bytes } = raw else {
        return Err(ImporterError::Parse {
            interface_id: interface.id.clone(),
            message: "expected OpenAPI document bytes".to_string(),
        });
    };
    let document: Value = match serde_json::from_slice(bytes) {
        Ok(document) => document,
        Err(json_error) => {
            serde_yaml::from_slice(bytes).map_err(|yaml_error| ImporterError::Parse {
                interface_id: interface.id.clone(),
                message: format!(
                    "JSON parse failed: {json_error}; YAML parse failed: {yaml_error}"
                ),
            })?
        }
    };
    let metadata =
        openapi_document_metadata_from_value(&document).map_err(|error| ImporterError::Parse {
            interface_id: interface.id.clone(),
            message: error.to_string(),
        })?;
    if interface.base_url.is_none()
        && !metadata
            .server_url
            .as_deref()
            .is_some_and(is_allowed_runtime_base_url)
    {
        let diagnostic = Diagnostic {
            source_id: Some(source_id.clone()),
            interface_id: Some(interface.id.clone()),
            details: serde_json::json!({
                "requires": ["SourceSpec openapi.base_url", "OpenAPI servers[].url"],
            }),
            ..Diagnostic::new(
                "OPENAPI_RUNTIME_BASE_URL_MISSING",
                DiagnosticSeverity::Warning,
                DiagnosticStage::CapabilityGeneration,
                format!(
                    "OpenAPI interface '{}' does not declare SourceSpec openapi.base_url and the document has no absolute HTTP server URL; REST capabilities were not generated",
                    interface.id
                ),
            )
        };
        let snapshot = ProviderSnapshotArtifact {
            artifact_schema_version: 1,
            source_id: source_id.clone(),
            interface_id: interface.id.clone(),
            interface_type: "openapi".to_string(),
            importer_version: "openapi-3.0-v1".to_string(),
            source_document_sha256: sha256_hex(bytes),
            snapshot: serde_json::json!({ "operations": [] }),
            diagnostics: vec![diagnostic],
        };
        return Ok(ImportedInterface {
            snapshot,
            capabilities: Vec::new(),
        });
    }
    let mut snapshot_ops = Vec::new();
    let mut capabilities = Vec::new();
    let mut operation_ids = OperationIdAllocator::default();
    let paths = document
        .get("paths")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (path, path_item) in paths {
        let Some(path_item) = path_item.as_object() else {
            continue;
        };
        let path_parameters = path_item
            .get("parameters")
            .and_then(Value::as_array)
            .map(|parameters| {
                parameters
                    .iter()
                    .map(|parameter| {
                        resolve_openapi_parameter_schema_refs(
                            &document,
                            resolve_local_openapi_ref(&document, parameter),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for (method_name, operation) in path_item {
            let Some(method) = http_method(method_name) else {
                continue;
            };
            let Some(operation) = operation.as_object() else {
                continue;
            };
            let provider_operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .map_or_else(|| format!("{method_name}_{path}"), ToString::to_string);
            let operation_id = operation_ids.allocate(&provider_operation_id);
            let mut parameters = path_parameters.clone();
            let operation_parameters = operation
                .get("parameters")
                .and_then(Value::as_array)
                .map(|parameters| {
                    parameters
                        .iter()
                        .map(|parameter| {
                            resolve_openapi_parameter_schema_refs(
                                &document,
                                resolve_local_openapi_ref(&document, parameter),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            merge_operation_parameters(&mut parameters, operation_parameters);
            let request_bodies = rest_request_bodies(&document, operation);
            let responses = rest_responses(&document, operation);
            let provider_ref = format!(
                "interfaces/{}/provider-snapshot.yaml#/operations/{operation_id}",
                interface.id
            );
            snapshot_ops.push(serde_json::json!({
                "operation_id": operation_id,
                "method": method_name,
                "path": path,
                "parameters": parameters,
                "request_body_media_types": request_bodies.iter().map(|body| body.media_type.clone()).collect::<Vec<_>>(),
                "response_variants": responses.iter().map(|response| serde_json::json!({
                    "status": response.status,
                    "media_type": response.media_type,
                })).collect::<Vec<_>>(),
                "security": operation.get("security").cloned().or_else(|| document.get("security").cloned()),
            }));
            let mut capability = Capability::new(
                source_id.clone(),
                interface.id.clone(),
                operation_id.clone(),
                ProviderOrigin {
                    kind: ProviderOriginKind::RestOperation,
                    snapshot_ref: provider_ref.clone(),
                    provider_name: provider_operation_id,
                },
                UpstreamBinding::Rest(RestUpstreamBinding {
                    operation_ref: provider_ref,
                    method,
                    path_template: path.clone(),
                    parameter_bindings: rest_parameter_bindings(&parameters),
                    request_bodies: request_bodies.clone(),
                    responses: responses.clone(),
                    pagination: None,
                }),
            );
            capability.display = CapabilityDisplay {
                title: operation
                    .get("summary")
                    .and_then(Value::as_str)
                    .unwrap_or(&operation_id)
                    .to_string(),
                description: operation
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                deprecated: operation
                    .get("deprecated")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                support_status: SupportStatus::Generated,
            };
            capability.effect_profile = method.default_effect_profile();
            capability.input_schema = rest_input_schema(&parameters, &request_bodies);
            capability.output_contract = OutputContract::RestResponseVariants {
                variants: responses
                    .iter()
                    .map(|response| RestOutputVariant {
                        status: response.status.clone(),
                        media_type: response.media_type.clone(),
                        schema: response.schema.clone(),
                        provider_origin: response.media_type.clone(),
                    })
                    .collect(),
            };
            capability.shape_hints = rest_shape_hints(method, &responses);
            capability.credential_requirements =
                credential_requirements(spec, interface.auth.as_ref());
            capabilities.push(capability);
        }
    }

    let snapshot = ProviderSnapshotArtifact {
        artifact_schema_version: 1,
        source_id: source_id.clone(),
        interface_id: interface.id.clone(),
        interface_type: "openapi".to_string(),
        importer_version: "openapi-3.0-v1".to_string(),
        source_document_sha256: sha256_hex(bytes),
        snapshot: serde_json::json!({ "operations": snapshot_ops }),
        diagnostics: Vec::new(),
    };
    Ok(ImportedInterface {
        snapshot,
        capabilities,
    })
}

fn is_allowed_runtime_base_url(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| match url.scheme() {
        "https" => true,
        "http" => match url.host() {
            Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        },
        _ => false,
    })
}

fn rest_input_schema(parameters: &[Value], request_bodies: &[RestRequestBody]) -> InvocationSchema {
    let mut root = Map::new();
    let mut root_required = Vec::new();
    for location in ["path", "query", "header", "cookie"] {
        let mut properties = Map::new();
        let mut required = Vec::new();
        for parameter in parameters {
            if parameter.get("in").and_then(Value::as_str) == Some(location)
                && let Some(name) = parameter.get("name").and_then(Value::as_str)
            {
                if matches!(location, "path")
                    || parameter
                        .get("required")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                {
                    required.push(name.to_string());
                }
                properties.insert(
                    name.to_string(),
                    parameter
                        .get("schema")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({"type":"string"})),
                );
            }
        }
        if !properties.is_empty() {
            let mut location_schema = serde_json::json!({
                "type": "object",
                "properties": properties,
                "additionalProperties": false
            });
            if !required.is_empty() {
                location_schema
                    .as_object_mut()
                    .expect("location schema object")
                    .insert("required".to_string(), serde_json::json!(required));
                root_required.push(location.to_string());
            }
            root.insert(location.to_string(), location_schema);
        }
    }
    if let Some(body) = request_bodies.first() {
        root.insert("body".to_string(), body.schema.schema.clone());
        if body.required {
            root_required.push("body".to_string());
        }
        if request_bodies.len() > 1 {
            root.insert(
                "contentType".to_string(),
                serde_json::json!({
                    "type": "string",
                    "enum": request_bodies.iter().map(|body| body.media_type.clone()).collect::<Vec<_>>()
                }),
            );
        }
    }
    let mut schema = serde_json::json!({
        "type": "object",
        "properties": root,
        "additionalProperties": false
    });
    if !root_required.is_empty() {
        schema
            .as_object_mut()
            .expect("root schema object")
            .insert("required".to_string(), serde_json::json!(root_required));
    }
    hoist_nested_schema_defs(&mut schema);
    InvocationSchema::new(schema)
}

fn rest_parameter_bindings(parameters: &[Value]) -> Vec<RestParameterBinding> {
    parameters
        .iter()
        .filter_map(|parameter| {
            let name = parameter.get("name")?.as_str()?.to_string();
            let location = match parameter.get("in")?.as_str()? {
                "path" => RestParameterLocation::Path,
                "query" => RestParameterLocation::Query,
                "header" => RestParameterLocation::Header,
                "cookie" => RestParameterLocation::Cookie,
                _ => return None,
            };
            let style = parameter.get("style").and_then(Value::as_str).map_or_else(
                || {
                    match location {
                        RestParameterLocation::Path | RestParameterLocation::Header => "simple",
                        RestParameterLocation::Query | RestParameterLocation::Cookie => "form",
                    }
                    .to_string()
                },
                str::to_string,
            );
            let explode = parameter
                .get("explode")
                .and_then(Value::as_bool)
                .unwrap_or(matches!(
                    location,
                    RestParameterLocation::Query | RestParameterLocation::Cookie
                ));
            Some(RestParameterBinding {
                name,
                location,
                required: matches!(location, RestParameterLocation::Path)
                    || parameter
                        .get("required")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                style,
                explode,
                allow_reserved: matches!(location, RestParameterLocation::Query)
                    && parameter
                        .get("allowReserved")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
            })
        })
        .collect()
}

fn rest_request_bodies(document: &Value, operation: &Map<String, Value>) -> Vec<RestRequestBody> {
    let Some(request_body) = operation
        .get("requestBody")
        .map(|body| resolve_local_openapi_ref(document, body))
    else {
        return Vec::new();
    };
    let required = request_body
        .get("required")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    request_body
        .get("content")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|content| {
            content
                .iter()
                .map(move |(media_type, media)| RestRequestBody {
                    media_type: media_type.clone(),
                    required,
                    schema: InvocationSchema::new(media.get("schema").map_or_else(
                        || serde_json::json!({ "type": "object" }),
                        |schema| resolve_openapi_schema_refs(document, schema),
                    )),
                })
        })
        .collect()
}

fn rest_responses(document: &Value, operation: &Map<String, Value>) -> Vec<RestResponseVariant> {
    operation
        .get("responses")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|responses| {
            responses.iter().filter_map(|(status, response)| {
                if !is_success_or_default_status(status) {
                    return None;
                }
                let response = resolve_local_openapi_ref(document, response);
                let content = response
                    .get("content")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_else(|| {
                        Map::from_iter([(
                            "application/json".to_string(),
                            serde_json::json!({"schema":{"type":"object"}}),
                        )])
                    });
                Some(
                    content
                        .into_iter()
                        .map(|(media_type, media)| RestResponseVariant {
                            status: status_range(status),
                            media_type,
                            schema: InvocationSchema::new(media.get("schema").map_or_else(
                                || serde_json::json!({"type":"object"}),
                                |schema| resolve_openapi_schema_refs(document, schema),
                            )),
                        }),
                )
            })
        })
        .flatten()
        .collect()
}

fn rest_shape_hints(method: HttpMethod, responses: &[RestResponseVariant]) -> ShapeHints {
    if method != HttpMethod::Get {
        return ShapeHints::unknown();
    }
    let Some(response) = responses
        .iter()
        .find(|response| response.media_type.contains("json"))
        .or_else(|| responses.first())
    else {
        return ShapeHints::unknown();
    };
    shape_hints_from_json_schema(&schema_shape_view(&response.schema.schema))
}

fn merge_operation_parameters(path_parameters: &mut Vec<Value>, operation_parameters: Vec<Value>) {
    for operation_parameter in operation_parameters {
        let key = parameter_key(&operation_parameter);
        if let Some(key) = key
            && let Some(index) = path_parameters
                .iter()
                .position(|candidate| parameter_key(candidate).as_ref() == Some(&key))
        {
            if let Some(path_parameter) = path_parameters.get_mut(index) {
                *path_parameter = operation_parameter;
            }
            continue;
        }
        path_parameters.push(operation_parameter);
    }
}

fn resolve_local_openapi_ref(document: &Value, value: &Value) -> Value {
    let mut current = value;
    for _ in 0..8 {
        let Some(reference) = current.get("$ref").and_then(Value::as_str) else {
            break;
        };
        let Some(pointer) = reference.strip_prefix('#') else {
            break;
        };
        let Some(resolved) = document.pointer(pointer) else {
            break;
        };
        current = resolved;
    }
    current.clone()
}

fn resolve_openapi_parameter_schema_refs(document: &Value, mut parameter: Value) -> Value {
    let Some(parameter) = parameter.as_object_mut() else {
        return parameter;
    };
    if let Some(schema) = parameter.get_mut("schema") {
        let resolved = resolve_openapi_schema_refs(document, schema);
        *schema = resolved;
    }
    if let Some(content) = parameter.get_mut("content").and_then(Value::as_object_mut) {
        for media in content.values_mut() {
            let Some(media) = media.as_object_mut() else {
                continue;
            };
            if let Some(schema) = media.get_mut("schema") {
                let resolved = resolve_openapi_schema_refs(document, schema);
                *schema = resolved;
            }
        }
    }
    Value::Object(parameter.clone())
}

fn resolve_openapi_schema_refs(document: &Value, schema: &Value) -> Value {
    let mut defs = Map::new();
    let mut resolving = BTreeSet::new();
    let mut normalized =
        normalize_openapi_schema_value(document, schema, &mut defs, &mut resolving);
    if !defs.is_empty() {
        insert_schema_defs(&mut normalized, defs);
    }
    normalized
}

fn normalize_openapi_schema_value(
    document: &Value,
    schema: &Value,
    defs: &mut Map<String, Value>,
    resolving: &mut BTreeSet<String>,
) -> Value {
    match schema {
        Value::Object(object) => normalize_openapi_schema_object(document, object, defs, resolving),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| normalize_openapi_schema_value(document, value, defs, resolving))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn normalize_openapi_schema_object(
    document: &Value,
    object: &Map<String, Value>,
    defs: &mut Map<String, Value>,
    resolving: &mut BTreeSet<String>,
) -> Value {
    let nullable = object
        .get("nullable")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if let Some(reference) = object.get("$ref").and_then(Value::as_str)
        && let Some(pointer) = reference.strip_prefix('#')
        && let Some(resolved) = document.pointer(pointer)
    {
        let def_name = openapi_schema_def_name(reference, pointer);
        if !defs.contains_key(&def_name) && resolving.insert(reference.to_string()) {
            let normalized = normalize_openapi_schema_value(document, resolved, defs, resolving);
            resolving.remove(reference);
            defs.insert(def_name.clone(), normalized);
        }
        let ref_schema = serde_json::json!({ "$ref": format!("#/$defs/{def_name}") });
        let siblings = object
            .iter()
            .filter(|(key, _)| !matches!(key.as_str(), "$ref" | "nullable"))
            .map(|(key, value)| {
                (
                    key.clone(),
                    normalize_openapi_schema_value(document, value, defs, resolving),
                )
            })
            .collect::<Map<_, _>>();
        let schema = if siblings.is_empty() {
            ref_schema
        } else {
            serde_json::json!({ "allOf": [ref_schema, Value::Object(siblings)] })
        };
        return lower_openapi_nullable(schema, nullable);
    }

    let normalized = object
        .iter()
        .filter(|(key, _)| key.as_str() != "nullable")
        .map(|(key, value)| {
            (
                key.clone(),
                normalize_openapi_schema_value(document, value, defs, resolving),
            )
        })
        .collect::<Map<_, _>>();
    lower_openapi_nullable(Value::Object(normalized), nullable)
}

fn insert_schema_defs(schema: &mut Value, defs: Map<String, Value>) {
    let Value::Object(object) = schema else {
        return;
    };
    match object.get_mut("$defs") {
        Some(Value::Object(existing)) => {
            for (key, value) in defs {
                existing.entry(key).or_insert(value);
            }
        }
        Some(_) => {}
        None => {
            object.insert("$defs".to_string(), Value::Object(defs));
        }
    }
}

fn hoist_nested_schema_defs(schema: &mut Value) {
    let mut defs = Map::new();
    collect_nested_schema_defs(schema, true, &mut defs);
    if !defs.is_empty() {
        insert_schema_defs(schema, defs);
    }
}

fn collect_nested_schema_defs(value: &mut Value, is_root: bool, defs: &mut Map<String, Value>) {
    match value {
        Value::Object(object) => {
            if !is_root && let Some(Value::Object(nested_defs)) = object.remove("$defs") {
                merge_schema_defs(defs, nested_defs);
            }
            for (key, value) in object {
                if key == "$defs" {
                    continue;
                }
                collect_nested_schema_defs(value, false, defs);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_nested_schema_defs(value, false, defs);
            }
        }
        _ => {}
    }
}

fn merge_schema_defs(target: &mut Map<String, Value>, source: Map<String, Value>) {
    for (key, value) in source {
        target.entry(key).or_insert(value);
    }
}

fn lower_openapi_nullable(mut schema: Value, nullable: bool) -> Value {
    if !nullable {
        return schema;
    }
    let Value::Object(object) = &mut schema else {
        return serde_json::json!({ "anyOf": [schema, { "type": "null" }] });
    };
    if let Some(enum_values) = object.get_mut("enum").and_then(Value::as_array_mut)
        && !enum_values.iter().any(Value::is_null)
    {
        enum_values.push(Value::Null);
    }
    match object.get_mut("type") {
        Some(Value::String(kind)) if kind != "null" => {
            let kind = std::mem::take(kind);
            object.insert(
                "type".to_string(),
                Value::Array(vec![Value::String(kind), Value::String("null".to_string())]),
            );
            schema
        }
        Some(Value::Array(kinds)) => {
            if !kinds.iter().any(|kind| kind.as_str() == Some("null")) {
                kinds.push(Value::String("null".to_string()));
            }
            schema
        }
        _ => serde_json::json!({ "anyOf": [schema, { "type": "null" }] }),
    }
}

fn openapi_schema_def_name(reference: &str, pointer: &str) -> String {
    let base = pointer
        .rsplit('/')
        .next()
        .map_or_else(|| "schema".to_string(), json_pointer_token_unescape);
    let mut name = base
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    if name.is_empty() {
        name.push_str("schema");
    }
    let digest = Sha256::digest(reference.as_bytes());
    let digest = format!("{digest:x}");
    let short_digest = digest.chars().take(8).collect::<String>();
    format!("{name}_{short_digest}")
}

fn json_pointer_token_unescape(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn parameter_key(parameter: &Value) -> Option<(String, String)> {
    Some((
        parameter.get("in")?.as_str()?.to_string(),
        parameter.get("name")?.as_str()?.to_string(),
    ))
}

fn http_method(method: &str) -> Option<HttpMethod> {
    match method {
        "get" => Some(HttpMethod::Get),
        "head" => Some(HttpMethod::Head),
        "options" => Some(HttpMethod::Options),
        "post" => Some(HttpMethod::Post),
        "put" => Some(HttpMethod::Put),
        "patch" => Some(HttpMethod::Patch),
        "delete" => Some(HttpMethod::Delete),
        _ => None,
    }
}

fn is_success_or_default_status(status: &str) -> bool {
    status == "default"
        || status
            .parse::<u16>()
            .is_ok_and(|code| (200..300).contains(&code))
}

fn status_range(status: &str) -> StatusRange {
    if status == "default" {
        StatusRange::Default
    } else {
        StatusRange::Code {
            code: status.parse().unwrap_or(200),
        }
    }
}
