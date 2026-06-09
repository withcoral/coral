use std::collections::BTreeSet;

use serde_json::{Map, Value, json};

use crate::model::{
    Capability, OutputContract, RestParameterLocation, RestRequestBody, RestUpstreamBinding,
    UpstreamBinding,
};

const REST_BODY_VALUE_KEYS: &[&str] = &["body", "json"];
const REST_BODY_MEDIA_TYPE_KEYS: &[&str] = &[
    "contentType",
    "content_type",
    "body_media_type",
    "media_type",
];
const JSON_SCHEMA_DEFS_KEY: &str = "$defs";
const UNRESOLVED_SCHEMA_REF_KEY: &str = "x-coral-unresolved-ref";

/// Derives the Code Mode input schema used by generated `tools.*` call sites.
///
/// This is the executable invocation shape: REST capabilities accept both the
/// canonical grouped `path`/`query`/`header`/`cookie` object form and the flat
/// argument form that generated Code Mode bindings support.
#[must_use]
pub fn code_mode_tool_input_schema(capability: &Capability) -> Value {
    let canonical = effective_input_schema(capability);
    let schema = if matches!(&capability.upstream_binding, UpstreamBinding::Rest(_)) {
        if let Some(mixed) = mixed_rest_argument_schema(&canonical) {
            mixed
        } else {
            canonical
        }
    } else {
        canonical
    };
    strip_unresolved_schema_refs(hoist_nested_schema_defs(schema))
}

/// Derives the generated Code Mode tool output schema for one capability.
#[must_use]
pub fn generated_tool_output_schema(capability: &Capability) -> Value {
    let provider_value_schema = provider_value_schema(&capability.output_contract)
        .unwrap_or_else(|| json!({ "description": "Provider response value." }));
    strip_unresolved_schema_refs(hoist_nested_schema_defs(json!({
        "type": "object",
        "required": ["ok", "complete", "partial", "errors", "source_status", "value", "error", "envelope"],
        "properties": {
            "ok": {
                "type": "boolean",
                "description": "True when the provider invocation succeeded. Provider failures throw by default unless allowErrorResult is explicitly set."
            },
            "complete": {
                "type": "boolean",
                "description": "True when every required provider call represented by this result completed successfully."
            },
            "partial": {
                "type": "boolean",
                "description": "True only for explicit raw-error/partial flows with incomplete provider data."
            },
            "errors": {
                "type": "array",
                "description": "Provider or invocation errors surfaced when raw error results are explicitly allowed.",
                "items": {
                    "type": "object",
                    "required": ["kind", "message", "details"],
                    "properties": {
                        "kind": { "type": "string" },
                        "message": { "type": "string" },
                        "details": {
                            "description": "Structured invocation error details when available."
                        }
                    },
                    "additionalProperties": false
                }
            },
            "source_status": {
                "type": "array",
                "description": "Per-source/capability provenance and completeness status for this generated invocation.",
                "items": {
                    "type": "object",
                    "required": ["source_id", "capability_id", "binding_ref", "full_path", "ok", "complete", "partial", "error"],
                    "properties": {
                        "source_id": { "type": "string" },
                        "capability_id": { "type": "string" },
                        "binding_ref": { "type": "string" },
                        "full_path": { "type": "string" },
                        "ok": { "type": "boolean" },
                        "complete": { "type": "boolean" },
                        "partial": { "type": "boolean" },
                        "error": {
                            "anyOf": [
                                {
                                    "type": "object",
                                    "required": ["kind", "message", "details"],
                                    "properties": {
                                        "kind": { "type": "string" },
                                        "message": { "type": "string" },
                                        "details": {
                                            "description": "Structured invocation error details when available."
                                        }
                                    },
                                    "additionalProperties": false
                                },
                                { "type": "null" }
                            ]
                        }
                    },
                    "additionalProperties": false
                }
            },
            "value": nullable_schema(&provider_value_schema),
            "error": {
                "anyOf": [
                    {
                        "type": "object",
                        "required": ["kind", "message", "details"],
                        "properties": {
                            "kind": { "type": "string" },
                            "message": { "type": "string" },
                            "details": {
                                "description": "Structured invocation error details when allowErrorResult is explicitly set."
                            }
                        },
                        "additionalProperties": false
                    },
                    { "type": "null" }
                ]
            },
            "envelope": generated_tool_envelope_schema()
        },
        "additionalProperties": false
    })))
}

fn generated_tool_envelope_schema() -> Value {
    json!({
        "anyOf": [
            {
                "type": "object",
                "description": "Coral invocation envelope with provider transport status and metadata. REST providers include lowercase response headers at envelope.provider.headers.",
                "properties": {
                    "kind": {
                        "type": "string",
                        "description": "Provider binding kind, such as rest, graphql, mcp_tool, or file_read."
                    },
                    "capability_id": { "type": "string" },
                    "source_id": { "type": "string" },
                    "provider": {
                        "type": "object",
                        "description": "Provider transport envelope. REST providers include status, lowercase response headers, media_type, body, and response_trust.",
                        "properties": {
                            "kind": { "type": "string" },
                            "status": { "type": "integer" },
                            "headers": {
                                "type": "object",
                                "description": "Lowercase REST response headers captured from the provider response.",
                                "additionalProperties": { "type": "string" }
                            },
                            "media_type": {
                                "anyOf": [
                                    { "type": "string" },
                                    { "type": "null" }
                                ]
                            },
                            "body": {
                                "description": "Parsed REST response body; this matches value for REST invocations."
                            },
                            "response_trust": {
                                "description": "Trust classification for provider-originated data."
                            }
                        },
                        "additionalProperties": true
                    }
                },
                "additionalProperties": true
            },
            { "type": "null" }
        ]
    })
}

/// Returns unresolved provider references annotated in an executable schema.
#[must_use]
pub fn executable_schema_unresolved_refs(schema: &Value) -> Vec<String> {
    let mut refs = Vec::new();
    collect_unresolved_schema_refs(schema, &mut refs);
    refs.sort();
    refs.dedup();
    refs
}

fn collect_unresolved_schema_refs(value: &Value, refs: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object
                .get(UNRESOLVED_SCHEMA_REF_KEY)
                .and_then(Value::as_str)
            {
                refs.push(reference.to_string());
            }
            for value in object.values() {
                collect_unresolved_schema_refs(value, refs);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_unresolved_schema_refs(value, refs);
            }
        }
        _ => {}
    }
}

fn strip_unresolved_schema_refs(value: Value) -> Value {
    let root = value.clone();
    strip_unresolved_schema_refs_inner(value, &root)
}

/// Hoists every nested `$defs` block up to the schema root.
///
/// Nested definitions are merged into the root `$defs` object using
/// vacant-only insertion (existing root entries win on collision). When the
/// root already carries a non-object `$defs`, it is left untouched.
#[must_use]
pub fn hoist_nested_schema_defs(mut value: Value) -> Value {
    let mut defs = Map::new();
    collect_nested_schema_defs(&mut value, true, &mut defs);
    if !defs.is_empty() {
        insert_schema_defs(&mut value, defs);
    }
    value
}

/// Merges `defs` into the schema's root `$defs` block.
///
/// Existing root definitions win on collision (vacant-only insertion). A
/// non-object root `$defs` is left untouched, and non-object schemas are
/// ignored.
pub fn insert_schema_defs(schema: &mut Value, defs: Map<String, Value>) {
    let Value::Object(root) = schema else {
        return;
    };
    match root.get_mut(JSON_SCHEMA_DEFS_KEY) {
        Some(Value::Object(existing)) => merge_schema_defs(existing, defs),
        Some(_) => {}
        None => {
            root.insert(JSON_SCHEMA_DEFS_KEY.to_string(), Value::Object(defs));
        }
    }
}

/// Collects nested `$defs` blocks into `defs`, removing them from the tree.
///
/// The root `$defs` (when `is_root` is true) is preserved in place; every
/// other `$defs` encountered is removed and merged into `defs` using
/// vacant-only insertion.
pub fn collect_nested_schema_defs(value: &mut Value, is_root: bool, defs: &mut Map<String, Value>) {
    match value {
        Value::Object(object) => {
            if !is_root
                && let Some(Value::Object(nested_defs)) = object.remove(JSON_SCHEMA_DEFS_KEY)
            {
                merge_schema_defs(defs, nested_defs);
            }
            for (key, value) in object {
                if key == JSON_SCHEMA_DEFS_KEY {
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

/// Merges `source` definitions into `target` using vacant-only insertion.
///
/// Keys already present in `target` are preserved; only missing keys are added.
pub fn merge_schema_defs(target: &mut Map<String, Value>, source: Map<String, Value>) {
    for (key, value) in source {
        match target.entry(key) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
            serde_json::map::Entry::Occupied(_) => {}
        }
    }
}

fn strip_unresolved_schema_refs_inner(value: Value, root: &Value) -> Value {
    match value {
        Value::Object(mut object) => {
            let reference = object
                .remove("$ref")
                .and_then(|value| value.as_str().map(str::to_string));
            for value in object.values_mut() {
                let original = std::mem::take(value);
                *value = strip_unresolved_schema_refs_inner(original, root);
            }
            if let Some(reference) = reference {
                if local_schema_ref_is_resolvable(&reference, root) {
                    object.insert("$ref".to_string(), Value::String(reference));
                    return Value::Object(object);
                }
                let unresolved_ref = unresolved_schema_ref_failure(&reference);
                if object.is_empty() {
                    return unresolved_ref;
                }
                append_root_all_of(&mut object, unresolved_ref);
            }
            Value::Object(object)
        }
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|value| strip_unresolved_schema_refs_inner(value, root))
                .collect(),
        ),
        other => other,
    }
}

fn local_schema_ref_is_resolvable(reference: &str, root: &Value) -> bool {
    match reference {
        "#" => true,
        reference => reference
            .strip_prefix('#')
            .is_some_and(|pointer| pointer.starts_with('/') && root.pointer(pointer).is_some()),
    }
}

fn unresolved_schema_ref_failure(reference: &str) -> Value {
    json!({
        "not": {},
        UNRESOLVED_SCHEMA_REF_KEY: reference,
        "description": format!(
            "Unresolved provider schema reference '{reference}' fails closed in executable schemas; re-add the source to regenerate materialized artifacts."
        )
    })
}

fn effective_input_schema(capability: &Capability) -> Value {
    match &capability.upstream_binding {
        UpstreamBinding::Rest(_) => effective_rest_input_schema(capability),
        UpstreamBinding::FileRead(_) => effective_file_read_input_schema(capability),
        UpstreamBinding::McpTool(_) | UpstreamBinding::Graphql(_) => {
            capability.input_schema.schema.clone()
        }
    }
}

fn effective_rest_input_schema(capability: &Capability) -> Value {
    let mut canonical = capability.input_schema.schema.clone();
    let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
        return canonical;
    };
    let parameters = effective_rest_parameters(binding);
    if parameters.is_empty() {
        augment_rest_body_schema(&mut canonical, binding);
        return canonical;
    }
    augment_rest_parameter_schema(&mut canonical, &parameters);
    augment_rest_body_schema(&mut canonical, binding);
    canonical
}

fn augment_rest_body_schema(schema: &mut Value, binding: &RestUpstreamBinding) {
    if binding.request_bodies.is_empty() {
        return;
    }
    let Some(root) = schema.as_object_mut() else {
        return;
    };
    root.entry("type".to_string())
        .or_insert_with(|| Value::String("object".to_string()));
    let mut root_required = required_string_set(root);
    for key in REST_BODY_VALUE_KEYS
        .iter()
        .chain(REST_BODY_MEDIA_TYPE_KEYS.iter())
    {
        root_required.remove(*key);
    }
    let properties = root
        .entry("properties".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(properties) = properties.as_object_mut() else {
        return;
    };
    let body_schema = rest_request_body_schema(binding);
    for key in REST_BODY_VALUE_KEYS {
        properties.insert((*key).to_string(), body_schema.clone());
    }
    let content_type_schema = rest_request_body_content_type_schema(binding);
    for key in REST_BODY_MEDIA_TYPE_KEYS {
        properties.insert((*key).to_string(), content_type_schema.clone());
    }
    append_root_all_of(root, rest_request_body_single_value_alias_schema());
    append_root_all_of(root, rest_request_body_single_media_selector_schema());
    if binding.request_bodies.iter().any(|body| body.required) {
        append_root_all_of(root, rest_request_body_required_alias_schema());
    }
    if let Some(constraints) = rest_request_body_media_type_constraints(binding) {
        append_root_all_of(root, constraints);
    }
    if root_required.is_empty() {
        root.remove("required");
    } else {
        root.insert(
            "required".to_string(),
            Value::Array(root_required.into_iter().map(Value::String).collect()),
        );
    }
}

fn rest_request_body_schema(binding: &RestUpstreamBinding) -> Value {
    let schemas = supported_rest_request_bodies(binding)
        .into_iter()
        .map(|body| body.schema.schema.clone())
        .collect::<Vec<_>>();
    match schemas.as_slice() {
        [] => {
            json!({ "not": {}, "description": "REST request body. Coral currently supports JSON request bodies only." })
        }
        [schema] => schema.clone(),
        schemas => json!({ "anyOf": schemas }),
    }
}

fn rest_request_body_content_type_schema(binding: &RestUpstreamBinding) -> Value {
    let media_types = supported_rest_request_bodies(binding)
        .into_iter()
        .map(|body| Value::String(body.media_type.clone()))
        .collect::<Vec<_>>();
    json!({
        "type": "string",
        "enum": media_types
    })
}

fn rest_request_body_required_alias_schema() -> Value {
    json!({
        "anyOf": REST_BODY_VALUE_KEYS
            .iter()
            .map(|key| json!({ "required": [key] }))
            .collect::<Vec<_>>()
    })
}

fn rest_request_body_single_value_alias_schema() -> Value {
    json!({
        "not": {
            "required": ["body", "json"]
        }
    })
}

fn rest_request_body_single_media_selector_schema() -> Value {
    let mut conflicts = Vec::new();
    for (left_index, left_key) in REST_BODY_MEDIA_TYPE_KEYS.iter().enumerate() {
        for right_key in REST_BODY_MEDIA_TYPE_KEYS.iter().skip(left_index + 1) {
            conflicts.push(json!({
                "required": [
                    left_key,
                    right_key
                ]
            }));
        }
    }
    json!({
        "not": {
            "anyOf": conflicts
        }
    })
}

fn rest_request_body_media_type_constraints(binding: &RestUpstreamBinding) -> Option<Value> {
    let bodies = supported_rest_request_bodies(binding);
    if bodies.len() <= 1 {
        return None;
    }
    let default_body = bodies.first().copied()?;
    let selector_present = json!({
        "anyOf": REST_BODY_MEDIA_TYPE_KEYS
            .iter()
            .map(|key| json!({ "required": [key] }))
            .collect::<Vec<_>>()
    });
    let mut variants = vec![json!({
        "not": selector_present,
        "properties": rest_request_body_value_properties(&default_body.schema.schema)
    })];
    for body in bodies {
        for selector_key in REST_BODY_MEDIA_TYPE_KEYS {
            let mut properties = rest_request_body_value_properties(&body.schema.schema);
            properties.insert(
                (*selector_key).to_string(),
                json!({ "type": "string", "enum": [body.media_type.clone()] }),
            );
            variants.push(json!({
                "required": [selector_key],
                "properties": properties
            }));
        }
    }
    Some(json!({ "anyOf": variants }))
}

fn rest_request_body_value_properties(body_schema: &Value) -> Map<String, Value> {
    REST_BODY_VALUE_KEYS
        .iter()
        .map(|key| ((*key).to_string(), body_schema.clone()))
        .collect()
}

fn append_root_all_of(root: &mut Map<String, Value>, constraint: Value) {
    match root.get_mut("allOf") {
        Some(Value::Array(items)) => items.push(constraint),
        Some(_) => {}
        None => {
            root.insert("allOf".to_string(), Value::Array(vec![constraint]));
        }
    }
}

fn supported_rest_request_bodies(binding: &RestUpstreamBinding) -> Vec<&RestRequestBody> {
    binding
        .request_bodies
        .iter()
        .filter(|body| is_json_media_type(&body.media_type))
        .collect()
}

/// Returns whether `media_type` denotes a JSON payload.
///
/// Any media-type parameters (after `;`) are dropped, the essence is trimmed
/// and lowercased, then matched against `application/json` and any `+json`
/// structured-suffix type (which includes `application/problem+json`).
#[must_use]
pub fn is_json_media_type(media_type: &str) -> bool {
    let media_type = media_type
        .split_once(';')
        .map_or(media_type, |(value, _)| value)
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json" || media_type.ends_with("+json")
}

/// Extracts the primary JSON-Schema `type` keyword for a schema.
///
/// A string `type` is returned unless it is `"null"`. For a `type` array, the
/// first non-`"null"` entry is returned. All other shapes yield `None`.
#[must_use]
pub fn json_schema_primary_type(schema: &Value) -> Option<&str> {
    match schema.get("type") {
        Some(Value::String(value)) if value != "null" => Some(value.as_str()),
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .find(|value| *value != "null"),
        _ => None,
    }
}

fn effective_file_read_input_schema(capability: &Capability) -> Value {
    let mut canonical = capability.input_schema.schema.clone();
    let Some(root) = canonical.as_object_mut() else {
        return canonical;
    };
    root.entry("type".to_string())
        .or_insert_with(|| Value::String("object".to_string()));
    let properties = root
        .entry("properties".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(properties) = properties.as_object_mut() else {
        return canonical;
    };
    properties.entry("limit".to_string()).or_insert_with(|| {
        json!({
            "type": "integer",
            "minimum": 1,
            "maximum": 10000
        })
    });
    properties
        .entry("file_id".to_string())
        .or_insert_with(|| json!({ "type": "string" }));
    canonical
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct EffectiveRestParameter {
    location: &'static str,
    name: String,
    required: bool,
}

fn effective_rest_parameters(binding: &RestUpstreamBinding) -> Vec<EffectiveRestParameter> {
    let mut seen = BTreeSet::new();
    let mut parameters = Vec::new();
    for parameter in &binding.parameter_bindings {
        let location = rest_location_schema_key(parameter.location);
        if seen.insert((location.to_string(), parameter.name.clone())) {
            parameters.push(EffectiveRestParameter {
                location,
                name: parameter.name.clone(),
                required: parameter.required,
            });
        }
    }
    for name in path_template_parameter_names(&binding.path_template) {
        if seen.insert(("path".to_string(), name.clone())) {
            parameters.push(EffectiveRestParameter {
                location: "path",
                name,
                required: true,
            });
        }
    }
    parameters.sort_by(|left, right| {
        left.location
            .cmp(right.location)
            .then_with(|| left.name.cmp(&right.name))
    });
    parameters
}

const fn rest_location_schema_key(location: RestParameterLocation) -> &'static str {
    match location {
        RestParameterLocation::Path => "path",
        RestParameterLocation::Query => "query",
        RestParameterLocation::Header => "header",
        RestParameterLocation::Cookie => "cookie",
    }
}

fn path_template_parameter_names(template: &str) -> Vec<String> {
    let mut names = BTreeSet::new();
    let mut remaining = template;
    while let Some((_, after_start)) = remaining.split_once('{') {
        let Some((name, after_end)) = after_start.split_once('}') else {
            break;
        };
        let name = name.trim();
        if !name.is_empty() {
            names.insert(name.to_string());
        }
        remaining = after_end;
    }
    names.into_iter().collect()
}

fn augment_rest_parameter_schema(schema: &mut Value, parameters: &[EffectiveRestParameter]) {
    let Some(root) = schema.as_object_mut() else {
        return;
    };
    root.entry("type".to_string())
        .or_insert_with(|| Value::String("object".to_string()));
    let mut root_required = required_string_set(root);
    {
        let properties = root
            .entry("properties".to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        let Some(properties) = properties.as_object_mut() else {
            return;
        };
        for parameter in parameters {
            let location_schema = properties
                .entry(parameter.location.to_string())
                .or_insert_with(rest_location_object_schema);
            let Some(location_object) = location_schema.as_object_mut() else {
                continue;
            };
            location_object
                .entry("type".to_string())
                .or_insert_with(|| Value::String("object".to_string()));
            location_object
                .entry("additionalProperties".to_string())
                .or_insert(Value::Bool(false));
            let mut location_required = required_string_set(location_object);
            let location_properties = location_object
                .entry("properties".to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            let Some(location_properties) = location_properties.as_object_mut() else {
                continue;
            };
            location_properties
                .entry(parameter.name.clone())
                .or_insert_with(rest_scalar_parameter_schema);
            if parameter.required {
                location_required.insert(parameter.name.clone());
                root_required.insert(parameter.location.to_string());
            }
            if !location_required.is_empty() {
                location_object.insert(
                    "required".to_string(),
                    Value::Array(location_required.into_iter().map(Value::String).collect()),
                );
            }
        }
    }
    if !root_required.is_empty() {
        root.insert(
            "required".to_string(),
            Value::Array(root_required.into_iter().map(Value::String).collect()),
        );
    }
}

fn rest_location_object_schema() -> Value {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
}

fn rest_scalar_parameter_schema() -> Value {
    json!({ "type": ["string", "number", "boolean"] })
}

fn mixed_rest_argument_schema(canonical: &Value) -> Option<Value> {
    let root = canonical.as_object()?;
    let properties = root.get("properties")?.as_object()?;
    let root_required = root_required_set(root);
    let mut mixed_properties = properties.clone();
    let mut mixed_required = required_string_set(root);
    let mut required_alias_groups = Vec::new();
    let mut found_rest_locations = false;

    for location in ["path", "query", "header", "cookie"] {
        let Some(location_schema) = properties.get(location).and_then(Value::as_object) else {
            continue;
        };
        let Some(location_properties) =
            location_schema.get("properties").and_then(Value::as_object)
        else {
            continue;
        };
        found_rest_locations = true;
        mixed_required.remove(location);
        remove_location_required_properties(&mut mixed_properties, location);
        let location_required = root_required_set(location_schema);
        for (name, schema) in location_properties {
            insert_or_union_property(&mut mixed_properties, name.clone(), schema.clone());
            if root_required.contains(location) && location_required.contains(name.as_str()) {
                required_alias_groups.push(rest_parameter_required_alias_schema(location, name));
            }
        }
    }

    let found_body_aliases = insert_rest_body_aliases(
        properties,
        &root_required,
        &mut mixed_properties,
        &mut required_alias_groups,
    );

    if !found_rest_locations && !found_body_aliases {
        return None;
    }

    let mut schema = Map::from_iter([
        ("type".to_string(), Value::String("object".to_string())),
        ("properties".to_string(), Value::Object(mixed_properties)),
        (
            "additionalProperties".to_string(),
            Value::Bool(
                canonical
                    .get("additionalProperties")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            ),
        ),
    ]);
    if !mixed_required.is_empty() {
        schema.insert(
            "required".to_string(),
            Value::Array(mixed_required.into_iter().map(Value::String).collect()),
        );
    }
    if let Some(defs) = canonical.get(JSON_SCHEMA_DEFS_KEY) {
        schema.insert(JSON_SCHEMA_DEFS_KEY.to_string(), defs.clone());
    }
    let mut all_of = canonical
        .get("allOf")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    all_of.extend(required_alias_groups);
    if !all_of.is_empty() {
        schema.insert("allOf".to_string(), Value::Array(all_of));
    }
    Some(Value::Object(schema))
}

fn remove_location_required_properties(properties: &mut Map<String, Value>, location: &str) {
    if let Some(location_schema) = properties.get_mut(location).and_then(Value::as_object_mut) {
        location_schema.remove("required");
    }
}

fn insert_or_union_property(properties: &mut Map<String, Value>, name: String, schema: Value) {
    match properties.entry(name) {
        serde_json::map::Entry::Vacant(entry) => {
            entry.insert(schema);
        }
        serde_json::map::Entry::Occupied(mut entry) => {
            if entry.get() == &schema {
                return;
            }
            let existing = std::mem::take(entry.get_mut());
            entry.insert(json!({ "anyOf": [existing, schema] }));
        }
    }
}

fn rest_parameter_required_alias_schema(location: &str, name: &str) -> Value {
    json!({
        "anyOf": [
            { "required": [name] },
            {
                "required": [location],
                "properties": {
                    location: {
                        "required": [name]
                    }
                }
            }
        ]
    })
}

fn insert_rest_body_aliases(
    properties: &Map<String, Value>,
    root_required: &BTreeSet<&str>,
    flat_properties: &mut Map<String, Value>,
    required_alias_groups: &mut Vec<Value>,
) -> bool {
    let mut found_aliases = false;
    if let Some(body_schema) = properties.get("body") {
        flat_properties.insert("body".to_string(), body_schema.clone());
        flat_properties.insert("json".to_string(), body_schema.clone());
        found_aliases = true;
        if root_required.contains("body") {
            required_alias_groups.push(json!({
                "anyOf": [
                    { "required": ["body"] },
                    { "required": ["json"] }
                ]
            }));
        }
    }
    if let Some(content_type_schema) = properties.get("contentType") {
        let aliases = [
            "contentType",
            "content_type",
            "body_media_type",
            "media_type",
        ];
        for key in aliases {
            flat_properties.insert(key.to_string(), content_type_schema.clone());
        }
        found_aliases = true;
        if root_required.contains("contentType") {
            required_alias_groups.push(json!({
                "anyOf": aliases
                    .into_iter()
                    .map(|key| json!({ "required": [key] }))
                    .collect::<Vec<_>>()
            }));
        }
    }
    found_aliases
}

fn required_string_set(object: &Map<String, Value>) -> BTreeSet<String> {
    object
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

fn root_required_set(object: &Map<String, Value>) -> BTreeSet<&str> {
    object
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

fn provider_value_schema(output_contract: &OutputContract) -> Option<Value> {
    match output_contract {
        OutputContract::Single { schema } | OutputContract::GraphqlData { schema } => {
            Some(schema.schema.clone())
        }
        OutputContract::McpStructuredContent { schema } => {
            schema.as_ref().map(|schema| schema.schema.clone())
        }
        OutputContract::RestResponseVariants { variants } => match variants.as_slice() {
            [] => None,
            [variant] => Some(variant.schema.schema.clone()),
            variants => Some(json!({
                "anyOf": variants
                    .iter()
                    .map(|variant| variant.schema.schema.clone())
                    .collect::<Vec<_>>()
            })),
        },
        OutputContract::Unknown => None,
    }
}

fn nullable_schema(schema: &Value) -> Value {
    json!({
        "anyOf": [schema, { "type": "null" }]
    })
}

#[cfg(test)]
mod tests {
    use crate::model::{
        CapabilityId, CapabilityKind, EffectKind, FileFormatDescriptor, FileScanBinding,
        HttpMethod, IdempotencyKind, InvocationSchema, McpTaskSupport, McpToolUpstreamBinding,
        ProviderOrigin, ProviderOriginKind, SourceCapabilitySet, SourceId,
    };

    use super::*;

    #[test]
    fn capability_id_uses_source_interface_and_operation() {
        let source_id = SourceId("src_github".to_string());
        let id = CapabilityId::new(&source_id, "rest", "list_issues");
        assert_eq!(
            id.as_str(),
            "source/src_github/interface/rest/operation/list_issues"
        );
    }

    #[test]
    fn http_methods_have_deterministic_effect_defaults() {
        assert_eq!(
            HttpMethod::Get.default_effect_profile().capability_kind,
            CapabilityKind::Query
        );
        assert_eq!(
            HttpMethod::Post.default_effect_profile().effects,
            vec![EffectKind::Write]
        );
        assert_eq!(
            HttpMethod::Delete.default_effect_profile().idempotency,
            IdempotencyKind::NonIdempotent
        );
    }

    #[test]
    fn capability_set_rejects_projection_refs() {
        let source_id = SourceId("src_demo".to_string());
        let mut capability = Capability::new(
            source_id.clone(),
            "rest",
            "list",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref: "interfaces/rest/provider-snapshot.yaml#/operations/list".to_string(),
                provider_name: "list".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Rest(RestUpstreamBinding {
                operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/list"
                    .to_string(),
                method: HttpMethod::Get,
                path_template: "/items".to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        );
        capability.display.description = "bad sql_table:demo.items leak".to_string();
        let set = SourceCapabilitySet::new(source_id, vec![capability]);
        let error = set.validate().expect_err("projection refs must fail");
        assert!(error.to_string().contains("sql_table:"));
    }

    #[test]
    fn code_mode_input_schema_infers_stale_rest_path_args() {
        let mut capability = rest_test_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }));

        let schema = code_mode_tool_input_schema(&capability);
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert_eq!(
            schema
                .pointer("/properties/path/properties/owner/type")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(3)
        );
        assert_eq!(
            schema
                .pointer("/properties/owner/type")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(3)
        );
        assert!(compiled.is_valid(&json!({
            "path": {
                "owner": "withcoral",
                "repo": "coral"
            },
            "pull_number": 123
        })));
        assert!(compiled.is_valid(&json!({
            "owner": "withcoral",
            "repo": "coral",
            "pull_number": 123
        })));
        assert!(!compiled.is_valid(&json!({
            "repo": "coral",
            "pull_number": "123"
        })));
    }

    #[test]
    fn code_mode_input_schema_validates_rest_body_aliases() {
        let mut capability = rest_test_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "required": ["path", "body", "contentType"],
            "properties": {
                "path": {
                    "type": "object",
                    "required": ["owner", "repo", "pull_number"],
                    "properties": {
                        "owner": { "type": "string" },
                        "repo": { "type": "string" },
                        "pull_number": { "type": "string" }
                    },
                    "additionalProperties": false
                },
                "body": {
                    "type": "object",
                    "required": ["title"],
                    "properties": {
                        "title": { "type": "string" }
                    },
                    "additionalProperties": false
                },
                "contentType": { "type": "string" }
            },
            "additionalProperties": false
        }));
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("test capability must be REST");
        };
        binding.request_bodies.push(RestRequestBody {
            media_type: "application/json".to_string(),
            required: true,
            schema: InvocationSchema::new(json!({
                "type": "object",
                "required": ["title"],
                "properties": {
                    "title": { "type": "string" }
                },
                "additionalProperties": false
            })),
        });

        let schema = code_mode_tool_input_schema(&capability);
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert!(compiled.is_valid(&json!({
            "owner": "withcoral",
            "repo": "coral",
            "pull_number": "123",
            "json": { "title": "hello" },
            "content_type": "application/json"
        })));
        assert!(compiled.is_valid(&json!({
            "path": {
                "owner": "withcoral",
                "repo": "coral",
                "pull_number": "123"
            },
            "json": { "title": "hello" },
            "content_type": "application/json"
        })));
        assert!(compiled.is_valid(&json!({
            "owner": "withcoral",
            "repo": "coral",
            "pull_number": "123",
            "json": { "title": "hello" }
        })));
        assert!(!compiled.is_valid(&json!({
            "owner": "withcoral",
            "repo": "coral",
            "pull_number": "123",
            "body": { "title": "old" },
            "json": { "title": "new" }
        })));
        assert!(!compiled.is_valid(&json!({
            "owner": "withcoral",
            "repo": "coral",
            "pull_number": "123"
        })));
    }

    #[test]
    fn code_mode_input_schema_validates_body_only_rest_aliases() {
        let mut capability = rest_test_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }));
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("test capability must be REST");
        };
        binding.path_template = "/issues".to_string();
        binding.parameter_bindings.clear();
        binding.method = HttpMethod::Post;
        binding.request_bodies.push(RestRequestBody {
            media_type: "application/json".to_string(),
            required: true,
            schema: InvocationSchema::new(json!({
                "type": "object",
                "required": ["title"],
                "properties": {
                    "title": { "type": "string" }
                },
                "additionalProperties": false
            })),
        });

        let schema = code_mode_tool_input_schema(&capability);
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert!(compiled.is_valid(&json!({
            "json": { "title": "hello" }
        })));
        assert!(compiled.is_valid(&json!({
            "body": { "title": "hello" },
            "media_type": "application/json"
        })));
        assert!(!compiled.is_valid(&json!({
            "body": { "title": "old" },
            "json": { "title": "new" }
        })));
        assert!(!compiled.is_valid(&json!({})));
    }

    #[test]
    fn code_mode_input_schema_widens_existing_rest_body_for_declared_media_types() {
        let mut capability = rest_test_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "required": ["body", "contentType"],
            "properties": {
                "body": {
                    "type": "object",
                    "required": ["xml"],
                    "properties": {
                        "xml": { "type": "string" }
                    },
                    "additionalProperties": false
                },
                "contentType": {
                    "type": "string",
                    "enum": ["application/xml"]
                }
            },
            "additionalProperties": false
        }));
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("test capability must be REST");
        };
        binding.path_template = "/issues".to_string();
        binding.parameter_bindings.clear();
        binding.method = HttpMethod::Post;
        binding.request_bodies = vec![
            RestRequestBody {
                media_type: "application/xml".to_string(),
                required: true,
                schema: InvocationSchema::new(json!({
                    "type": "object",
                    "required": ["xml"],
                    "properties": {
                        "xml": { "type": "string" }
                    },
                    "additionalProperties": false
                })),
            },
            RestRequestBody {
                media_type: "application/vnd.github+json".to_string(),
                required: true,
                schema: InvocationSchema::new(json!({
                    "type": "object",
                    "required": ["title"],
                    "properties": {
                        "title": { "type": "string" }
                    },
                    "additionalProperties": false
                })),
            },
            RestRequestBody {
                media_type: "application/json".to_string(),
                required: true,
                schema: InvocationSchema::new(json!({
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": { "type": "string" }
                    },
                    "additionalProperties": false
                })),
            },
        ];

        let schema = code_mode_tool_input_schema(&capability);
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert!(compiled.is_valid(&json!({
            "body": { "name": "hello" },
            "contentType": "application/json"
        })));
        assert!(compiled.is_valid(&json!({
            "json": { "title": "hello" },
            "media_type": "application/vnd.github+json"
        })));
        assert!(compiled.is_valid(&json!({
            "body": { "title": "hello" }
        })));
        assert!(!compiled.is_valid(&json!({
            "body": { "title": "old" },
            "json": { "title": "new" }
        })));
        assert!(!compiled.is_valid(&json!({
            "body": { "title": "hello" },
            "contentType": "application/json"
        })));
        assert!(!compiled.is_valid(&json!({
            "body": { "title": "hello" },
            "contentType": "application/json",
            "media_type": "application/vnd.github+json"
        })));
        assert!(!compiled.is_valid(&json!({
            "body": { "title": "hello" },
            "contentType": "application/xml"
        })));
    }

    #[test]
    fn code_mode_input_schema_fails_closed_for_unresolved_provider_refs() {
        let mut capability = rest_test_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "properties": {
                "body": { "$ref": "#/components/schemas/Issue" }
            },
            "additionalProperties": false
        }));
        let UpstreamBinding::Rest(binding) = &mut capability.upstream_binding else {
            panic!("test capability must be REST");
        };
        binding.path_template = "/issues".to_string();
        binding.parameter_bindings.clear();
        binding.method = HttpMethod::Post;
        binding.request_bodies = vec![RestRequestBody {
            media_type: "application/json".to_string(),
            required: true,
            schema: InvocationSchema::new(json!({
                "$ref": "#/components/schemas/Issue"
            })),
        }];

        let schema = code_mode_tool_input_schema(&capability);
        assert!(
            !serde_json::to_string(&schema)
                .expect("schema json")
                .contains("\"$ref\"")
        );
        assert_eq!(
            executable_schema_unresolved_refs(&schema),
            vec!["#/components/schemas/Issue".to_string()]
        );
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert!(!compiled.is_valid(&json!({
            "body": { "title": "hello" }
        })));
    }

    #[test]
    fn code_mode_input_schema_preserves_resolvable_local_refs() {
        let mut capability = Capability::new(
            SourceId("src_mcp".to_string()),
            "mcp",
            "create_issue",
            ProviderOrigin {
                kind: ProviderOriginKind::McpTool,
                snapshot_ref: "interfaces/mcp/provider-snapshot.yaml#/tools/create_issue"
                    .to_string(),
                provider_name: "create_issue".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::McpTool(McpToolUpstreamBinding {
                server_ref: "source/src_mcp/interface/mcp/server/default".to_string(),
                tool_name: "create_issue".to_string(),
                task_support: McpTaskSupport::Unknown,
            }),
        );
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "required": ["issue"],
            "properties": {
                "issue": { "$ref": "#/$defs/Issue" }
            },
            "$defs": {
                "Issue": {
                    "type": "object",
                    "required": ["title"],
                    "properties": {
                        "title": { "type": "string" }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false
        }));

        let schema = code_mode_tool_input_schema(&capability);
        assert_eq!(
            executable_schema_unresolved_refs(&schema),
            Vec::<String>::new()
        );
        assert!(
            serde_json::to_string(&schema)
                .expect("schema json")
                .contains("\"$ref\":\"#/$defs/Issue\"")
        );
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert!(compiled.is_valid(&json!({
            "issue": { "title": "hello" }
        })));
        assert!(!compiled.is_valid(&json!({
            "issue": {}
        })));
    }

    #[test]
    fn code_mode_input_schema_does_not_rewrite_non_rest_path_properties() {
        let mut capability = Capability::new(
            SourceId("src_mcp".to_string()),
            "mcp",
            "walk_path",
            ProviderOrigin {
                kind: ProviderOriginKind::McpTool,
                snapshot_ref: "interfaces/mcp/provider-snapshot.yaml#/tools/walk_path".to_string(),
                provider_name: "walk_path".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::McpTool(McpToolUpstreamBinding {
                server_ref: "source/src_mcp/interface/mcp/server/default".to_string(),
                tool_name: "walk_path".to_string(),
                task_support: McpTaskSupport::Unknown,
            }),
        );
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": {
                    "type": "object",
                    "required": ["segments"],
                    "properties": {
                        "segments": {
                            "type": "array",
                            "items": { "type": "string" }
                        }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false
        }));

        let schema = code_mode_tool_input_schema(&capability);
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert!(schema.pointer("/properties/segments").is_none());
        assert!(compiled.is_valid(&json!({
            "path": { "segments": ["root", "leaf"] }
        })));
        assert!(!compiled.is_valid(&json!({
            "segments": ["root", "leaf"]
        })));
    }

    #[test]
    fn generated_tool_output_schema_hoists_provider_defs() {
        let mut capability = rest_test_capability();
        capability.output_contract = OutputContract::Single {
            schema: InvocationSchema::new(json!({
                "$ref": "#/$defs/Issue",
                "$defs": {
                    "Issue": {
                        "type": "object",
                        "required": ["title"],
                        "properties": {
                            "title": { "type": "string" }
                        },
                        "additionalProperties": false
                    }
                }
            })),
        };

        let schema = generated_tool_output_schema(&capability);
        assert_eq!(
            executable_schema_unresolved_refs(&schema),
            Vec::<String>::new()
        );
        assert!(schema.pointer("/$defs/Issue").is_some());
        assert_eq!(
            schema
                .pointer(
                    "/properties/envelope/anyOf/0/properties/provider/properties/headers/additionalProperties/type"
                )
                .and_then(Value::as_str),
            Some("string")
        );
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert!(compiled.is_valid(&json!({
            "ok": true,
            "complete": true,
            "partial": false,
            "errors": [],
            "source_status": [],
            "value": { "title": "hello" },
            "error": null,
            "envelope": null
        })));
        assert!(!compiled.is_valid(&json!({
            "ok": true,
            "complete": true,
            "partial": false,
            "errors": [],
            "source_status": [],
            "value": {},
            "error": null,
            "envelope": null
        })));
    }

    #[test]
    fn code_mode_input_schema_exposes_file_read_args() {
        let capability = Capability::new(
            SourceId("src_files".to_string()),
            "files",
            "read_files",
            ProviderOrigin {
                kind: ProviderOriginKind::FileRelation,
                snapshot_ref: "interfaces/files/provider-snapshot.yaml".to_string(),
                provider_name: "files".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::FileRead(FileScanBinding {
                file_refs: Vec::new(),
                format: FileFormatDescriptor::Jsonl,
                schema_ref: None,
            }),
        );

        let schema = code_mode_tool_input_schema(&capability);
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert!(compiled.is_valid(&json!({
            "limit": 1,
            "file_id": "file_0"
        })));
        assert!(!compiled.is_valid(&json!({
            "limit": 0
        })));
        assert!(schema.pointer("/properties/limit/maximum").is_some());
    }

    fn rest_test_capability() -> Capability {
        Capability::new(
            SourceId("src_github".to_string()),
            "rest",
            "pulls_list_reviews",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/pulls_list_reviews"
                        .to_string(),
                provider_name: "GitHub".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Rest(RestUpstreamBinding {
                operation_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/pulls_list_reviews"
                        .to_string(),
                method: HttpMethod::Get,
                path_template: "/repos/{owner}/{repo}/pulls/{pull_number}/reviews".to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        )
    }
}
