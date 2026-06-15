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
/// Marker key set on schema subtrees elided by [`bound_schema_to_budget`].
pub const SCHEMA_TRUNCATION_KEY: &str = "x-coral-truncated";
const BOUND_SCHEMA_PRUNE_DEPTHS: [usize; 4] = [6, 5, 4, 3];
const BOUND_SCHEMA_DEFS_PRUNE_DEPTH: usize = 2;
const SCHEMA_PATH_RESOLUTION_LIMIT: usize = 32;
const LOCAL_DEFS_REF_PREFIX: &str = "#/$defs/";
/// Maximum parameters rendered inline by [`code_mode_call_signature`].
const SIGNATURE_PARAM_LIMIT: usize = 8;
/// Rough character cap for one [`code_mode_call_signature`] line.
const SIGNATURE_MAX_CHARS: usize = 220;
/// Maximum enum variants rendered inline per signature parameter.
const SIGNATURE_ENUM_VARIANT_LIMIT: usize = 3;

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

/// Bounds a schema to roughly `budget_bytes` of serialized JSON.
///
/// Schemas already within budget are returned unchanged with `false`.
/// Oversized schemas are pruned progressively at schema-node depth
/// 6 -> 5 -> 4 -> 3, replacing deeper subtrees with
/// `{ "type": <primary type>, "x-coral-truncated": true }` stubs and dropping
/// `$defs` entries that are no longer transitively reachable. Flat define-once
/// `$defs` layouts (the GraphQL importer's named-type graphs) put almost every
/// byte at depth <= 2, so when depth pruning cannot meet the budget, whole
/// `$defs` entries are retained in body-reachability (breadth-first) order
/// until the budget is filled and `$ref` sites of dropped defs become
/// truncation stubs: near types stay fully described, far types are elided.
/// If the schema is still over budget, surviving `$defs` entries are pruned at
/// depth 2 as a best-effort pass, then the remaining root is replaced by a
/// truncation stub if needed. The second tuple element is `true` whenever any
/// pruning happened or the input already carried truncation markers.
#[must_use]
pub fn bound_schema_to_budget(schema: Value, budget_bytes: usize) -> (Value, bool) {
    let already_truncated = schema_contains_truncation_marker(&schema);
    if schema_serialized_len(&schema) <= budget_bytes {
        return (schema, already_truncated);
    }
    let mut bounded = schema;
    for depth_limit in BOUND_SCHEMA_PRUNE_DEPTHS {
        bounded = prune_schema_tree(bounded, depth_limit);
        drop_unreachable_schema_defs(&mut bounded);
        if schema_serialized_len(&bounded) <= budget_bytes {
            return (bounded, true);
        }
    }
    retain_reachable_schema_defs_within_budget(&mut bounded, budget_bytes);
    if schema_serialized_len(&bounded) <= budget_bytes {
        return (bounded, true);
    }
    if let Some(Value::Object(defs)) = bounded.get_mut(JSON_SCHEMA_DEFS_KEY) {
        for def in defs.values_mut() {
            let original = std::mem::take(def);
            *def = prune_schema_node(original, 0, BOUND_SCHEMA_DEFS_PRUNE_DEPTH);
        }
    }
    drop_unreachable_schema_defs(&mut bounded);
    if schema_serialized_len(&bounded) <= budget_bytes {
        return (bounded, true);
    }
    bounded = truncated_schema_stub(&bounded);
    (bounded, true)
}

pub(crate) fn schema_contains_truncation_marker(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.get(SCHEMA_TRUNCATION_KEY).and_then(Value::as_bool) == Some(true)
                || object.values().any(schema_contains_truncation_marker)
        }
        Value::Array(values) => values.iter().any(schema_contains_truncation_marker),
        _ => false,
    }
}

/// Keeps whole `$defs` entries in body-reachability (breadth-first) order
/// while the serialized schema fits `budget_bytes`, replacing every `$ref` to
/// a dropped def with a `{ "type": <primary type>, "x-coral-truncated": true }`
/// stub.
///
/// Breadth-first retention keeps each kept def reachable: a def's discovery
/// parent always precedes it, so retaining a prefix of the order never leaves
/// dangling kept defs. Stubs are slightly larger than the refs they replace,
/// so retention re-checks the real serialized size and sheds the farthest
/// kept defs until the schema fits (or no defs remain).
fn retain_reachable_schema_defs_within_budget(schema: &mut Value, budget_bytes: usize) {
    let order = breadth_first_def_order(schema);
    if order.is_empty() {
        return;
    }
    let mut keep = order.len();
    loop {
        let mut candidate = schema.clone();
        apply_def_retention(&mut candidate, order.get(..keep).unwrap_or_default());
        if schema_serialized_len(&candidate) <= budget_bytes || keep == 0 {
            *schema = candidate;
            return;
        }
        keep -= 1;
    }
}

/// Returns root `$defs` names in breadth-first reachability order from the
/// schema body (unreachable defs are omitted).
///
/// Sibling references at each node are visited in lexicographic order: JSON
/// object key order is not preserved across every transport (proto map fields
/// hash their keys), so retention must not depend on map iteration order or
/// the same capability would bound differently per surface.
fn breadth_first_def_order(schema: &Value) -> Vec<String> {
    fn enqueue_sorted(
        mut names: Vec<String>,
        seen: &mut BTreeSet<String>,
        queue: &mut std::collections::VecDeque<String>,
    ) {
        names.sort();
        for name in names {
            if seen.insert(name.clone()) {
                queue.push_back(name);
            }
        }
    }
    let Value::Object(root) = schema else {
        return Vec::new();
    };
    let Some(Value::Object(defs)) = root.get(JSON_SCHEMA_DEFS_KEY) else {
        return Vec::new();
    };
    let mut discovered = Vec::new();
    collect_local_def_refs_from_schema_body(root, &mut discovered);
    let mut queue = std::collections::VecDeque::new();
    let mut seen = BTreeSet::new();
    enqueue_sorted(discovered, &mut seen, &mut queue);
    let mut order = Vec::new();
    while let Some(name) = queue.pop_front() {
        let Some(def) = defs.get(&name) else {
            continue;
        };
        let mut nested = Vec::new();
        collect_local_def_refs(def, &mut nested);
        enqueue_sorted(nested, &mut seen, &mut queue);
        order.push(name);
    }
    order
}

/// Retains only `kept` defs and stubs every `$ref` to the rest.
fn apply_def_retention(schema: &mut Value, kept: &[String]) {
    let Value::Object(root) = &mut *schema else {
        return;
    };
    let Some(Value::Object(defs)) = root.get_mut(JSON_SCHEMA_DEFS_KEY) else {
        return;
    };
    let kept = kept.iter().cloned().collect::<BTreeSet<_>>();
    let stubs = defs
        .iter()
        .filter(|(name, _)| !kept.contains(*name))
        .map(|(name, def)| (name.clone(), truncated_schema_stub(def)))
        .collect::<Map<_, _>>();
    if stubs.is_empty() {
        return;
    }
    defs.retain(|name, _| kept.contains(name));
    if defs.is_empty() {
        root.remove(JSON_SCHEMA_DEFS_KEY);
    }
    stub_dropped_def_refs(schema, &stubs);
}

/// Replaces every `{ "$ref": "#/$defs/<dropped>" }` node with its stub.
fn stub_dropped_def_refs(value: &mut Value, stubs: &Map<String, Value>) {
    match value {
        Value::Object(object) => {
            let dropped_ref = object
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix(LOCAL_DEFS_REF_PREFIX))
                .and_then(|name| stubs.get(name));
            if let Some(stub) = dropped_ref {
                *value = stub.clone();
                return;
            }
            for child in object.values_mut() {
                stub_dropped_def_refs(child, stubs);
            }
        }
        Value::Array(values) => {
            for child in values {
                stub_dropped_def_refs(child, stubs);
            }
        }
        _ => {}
    }
}

fn schema_serialized_len(schema: &Value) -> usize {
    serde_json::to_string(schema).map_or(usize::MAX, |text| text.len())
}

/// Prunes the schema body and each root `$defs` entry at `depth_limit`.
///
/// Every `$defs` entry counts as its own depth-0 root so shared named types
/// keep the same amount of visible structure as the schema body.
fn prune_schema_tree(schema: Value, depth_limit: usize) -> Value {
    let mut root = match schema {
        Value::Object(root) => root,
        other => return other,
    };
    let defs = root.remove(JSON_SCHEMA_DEFS_KEY);
    let mut pruned = prune_schema_node(Value::Object(root), 0, depth_limit);
    if let Some(defs) = defs
        && let Value::Object(object) = &mut pruned
    {
        let defs = match defs {
            Value::Object(defs) => Value::Object(
                defs.into_iter()
                    .map(|(name, def)| (name, prune_schema_node(def, 0, depth_limit)))
                    .collect(),
            ),
            other => other,
        };
        object.insert(JSON_SCHEMA_DEFS_KEY.to_string(), defs);
    }
    pruned
}

fn prune_schema_node(node: Value, depth: usize, depth_limit: usize) -> Value {
    let mut object = match node {
        Value::Object(object) => object,
        other => return other,
    };
    if depth >= depth_limit {
        return truncated_schema_stub(&Value::Object(object));
    }
    for (key, value) in &mut object {
        match key.as_str() {
            "properties" | "patternProperties" => {
                if let Value::Object(properties) = value {
                    for child in properties.values_mut() {
                        let original = std::mem::take(child);
                        *child = prune_schema_node(original, depth + 1, depth_limit);
                    }
                }
            }
            "items"
            | "additionalProperties"
            | "not"
            | "contains"
            | "if"
            | "then"
            | "else"
            | "propertyNames"
            | "anyOf"
            | "oneOf"
            | "allOf"
            | "prefixItems" => match value {
                Value::Object(_) => {
                    let original = std::mem::take(value);
                    *value = prune_schema_node(original, depth + 1, depth_limit);
                }
                Value::Array(children) => {
                    for child in children {
                        let original = std::mem::take(child);
                        *child = prune_schema_node(original, depth + 1, depth_limit);
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
    Value::Object(object)
}

fn truncated_schema_stub(schema: &Value) -> Value {
    let mut stub = Map::new();
    if let Some(primary) = json_schema_primary_type(schema) {
        stub.insert("type".to_string(), Value::String(primary.to_string()));
    }
    stub.insert(SCHEMA_TRUNCATION_KEY.to_string(), Value::Bool(true));
    Value::Object(stub)
}

/// Drops root `$defs` entries that are no longer transitively reachable from
/// the schema body through local `#/$defs/<Name>` references.
fn drop_unreachable_schema_defs(schema: &mut Value) {
    let Value::Object(root) = schema else {
        return;
    };
    let reachable = {
        let Some(Value::Object(defs)) = root.get(JSON_SCHEMA_DEFS_KEY) else {
            return;
        };
        let mut queue = Vec::new();
        collect_local_def_refs_from_schema_body(root, &mut queue);
        let mut reachable = BTreeSet::new();
        while let Some(name) = queue.pop() {
            if defs.contains_key(&name)
                && reachable.insert(name.clone())
                && let Some(def) = defs.get(&name)
            {
                collect_local_def_refs(def, &mut queue);
            }
        }
        reachable
    };
    if let Some(Value::Object(defs)) = root.get_mut(JSON_SCHEMA_DEFS_KEY) {
        defs.retain(|name, _| reachable.contains(name));
        if defs.is_empty() {
            root.remove(JSON_SCHEMA_DEFS_KEY);
        }
    }
}

fn collect_local_def_refs_from_schema_body(root: &Map<String, Value>, out: &mut Vec<String>) {
    for (key, value) in root {
        if key == JSON_SCHEMA_DEFS_KEY {
            continue;
        }
        if key == "$ref" {
            collect_local_def_ref_value(value, out);
        }
        collect_local_def_refs(value, out);
    }
}

fn collect_local_def_refs(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(value) = object.get("$ref") {
                collect_local_def_ref_value(value, out);
            }
            for value in object.values() {
                collect_local_def_refs(value, out);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_local_def_refs(value, out);
            }
        }
        _ => {}
    }
}

fn collect_local_def_ref_value(value: &Value, out: &mut Vec<String>) {
    if let Some(name) = value
        .as_str()
        .and_then(|reference| reference.strip_prefix(LOCAL_DEFS_REF_PREFIX))
    {
        out.push(name.to_string());
    }
}

/// Returns the dot paths of every subtree elided by [`bound_schema_to_budget`].
///
/// Paths name property segments (`filter.team`), `items` for array elements,
/// indexed combinators (`anyOf[0]`), and `$defs.<Name>` for named types. A
/// fully elided root yields one empty path.
#[must_use]
pub fn truncated_schema_paths(schema: &Value) -> Vec<String> {
    let mut paths = Vec::new();
    collect_truncated_schema_paths(schema, &mut Vec::new(), &mut paths);
    paths
}

fn collect_truncated_schema_paths(node: &Value, trail: &mut Vec<String>, out: &mut Vec<String>) {
    let Value::Object(object) = node else {
        return;
    };
    if object.get(SCHEMA_TRUNCATION_KEY).and_then(Value::as_bool) == Some(true) {
        out.push(trail.join("."));
        return;
    }
    for (key, value) in object {
        match key.as_str() {
            "properties" | "patternProperties" | JSON_SCHEMA_DEFS_KEY => {
                if let Value::Object(children) = value {
                    for (name, child) in children {
                        let segment = if key == JSON_SCHEMA_DEFS_KEY {
                            format!("$defs.{name}")
                        } else {
                            name.clone()
                        };
                        trail.push(segment);
                        collect_truncated_schema_paths(child, trail, out);
                        trail.pop();
                    }
                }
            }
            "items"
            | "additionalProperties"
            | "not"
            | "contains"
            | "if"
            | "then"
            | "else"
            | "propertyNames"
            | "anyOf"
            | "oneOf"
            | "allOf"
            | "prefixItems" => match value {
                Value::Object(_) => {
                    trail.push(key.clone());
                    collect_truncated_schema_paths(value, trail, out);
                    trail.pop();
                }
                Value::Array(children) => {
                    for (index, child) in children.iter().enumerate() {
                        trail.push(format!("{key}[{index}]"));
                        collect_truncated_schema_paths(child, trail, out);
                        trail.pop();
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
}

/// Extracts the schema subtree addressed by a dot path such as
/// `"filter.labels.every"` (a `/`-separated JSON-pointer style path is also
/// accepted).
///
/// Navigation descends through `properties/<segment>`, auto-descends array
/// `items`, and transparently resolves local `$ref`s against root `$defs`.
/// The returned subtree is self-contained: `$defs` entries it still references
/// are attached to it.
///
/// # Errors
///
/// Returns an error when the path is empty, a `$ref` cannot be resolved, or a
/// segment does not exist; invalid segments list the keys available at the
/// deepest valid node.
pub fn schema_subtree_at_path(schema: &Value, path: &str) -> Result<Value, String> {
    let segments = schema_path_segments(path);
    if segments.is_empty() {
        return Err("schema path is empty".to_string());
    }
    schema_subtree_at_segments(schema, &segments)
}

/// Splits a schema path into segments, accepting dot paths and
/// `/`-separated JSON-pointer style paths.
#[must_use]
pub fn schema_path_segments(path: &str) -> Vec<&str> {
    let path = path.trim();
    let separator = if path.contains('/') { '/' } else { '.' };
    path.split(separator)
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .collect()
}

/// Extracts the schema subtree addressed by pre-split path segments.
///
/// See [`schema_subtree_at_path`] for navigation semantics.
///
/// # Errors
///
/// Returns an error when a `$ref` cannot be resolved or a segment does not
/// exist; invalid segments list the keys available at the deepest valid node.
pub fn schema_subtree_at_segments(schema: &Value, segments: &[&str]) -> Result<Value, String> {
    let mut node = schema;
    let mut consumed: Vec<String> = Vec::new();
    let mut index = 0;
    while index < segments.len() {
        let segment = segments[index];
        let location = schema_path_location(&consumed);
        if segment == JSON_SCHEMA_DEFS_KEY {
            let Some(def_name) = segments.get(index + 1) else {
                return Err(format!(
                    "schema path segment '$defs' at {location} needs a type name"
                ));
            };
            let current = deref_schema_node(node, schema)
                .map_err(|error| format!("{error} at {location}"))?;
            let Some(defs) = current
                .get(JSON_SCHEMA_DEFS_KEY)
                .or_else(|| schema.get(JSON_SCHEMA_DEFS_KEY))
                .and_then(Value::as_object)
            else {
                return Err(format!("schema at {location} has no $defs to descend into"));
            };
            let Some(child) = defs.get(*def_name) else {
                return Err(format!(
                    "schema $defs segment '{def_name}' not found at {location}; available keys: {}",
                    defs.keys().cloned().collect::<Vec<_>>().join(", ")
                ));
            };
            node = child;
            consumed.push(JSON_SCHEMA_DEFS_KEY.to_string());
            consumed.push((*def_name).to_string());
            index += 2;
            continue;
        }

        if let Some((keyword, child_index)) = indexed_schema_segment(segment) {
            let current = deref_schema_node(node, schema)
                .map_err(|error| format!("{error} at {location}"))?;
            let Some(children) = current.get(keyword).and_then(Value::as_array) else {
                return Err(format!(
                    "schema path segment '{segment}' not found at {location}; available keys: {}",
                    schema_object_keys(current)
                ));
            };
            let Some(child) = children.get(child_index) else {
                return Err(format!(
                    "schema path segment '{segment}' index is out of range at {location}; available entries: {}",
                    children.len()
                ));
            };
            node = child;
            consumed.push(segment.to_string());
            index += 1;
            continue;
        }

        if is_direct_schema_keyword_segment(segment) {
            let current = deref_schema_node(node, schema)
                .map_err(|error| format!("{error} at {location}"))?;
            if let Some(child) = current.get(segment) {
                node = child;
                consumed.push(segment.to_string());
                index += 1;
                continue;
            };
        }

        let current = descend_to_properties(node, schema)
            .map_err(|error| format!("{error} at {location}"))?;
        let Some(properties) = current.get("properties").and_then(Value::as_object) else {
            return Err(format!(
                "schema at {location} has no named properties to descend into"
            ));
        };
        let Some(child) = properties.get(segment) else {
            return Err(format!(
                "schema path segment '{segment}' not found at {location}; available keys: {}",
                properties.keys().cloned().collect::<Vec<_>>().join(", ")
            ));
        };
        node = child;
        consumed.push(segment.to_string());
        index += 1;
    }
    let resolved = deref_schema_node(node, schema)
        .map_err(|error| format!("{error} at '{}'", consumed.join(".")))?;
    let mut subtree = resolved.clone();
    attach_reachable_schema_defs(&mut subtree, schema);
    Ok(subtree)
}

fn schema_path_location(consumed: &[String]) -> String {
    if consumed.is_empty() {
        "the schema root".to_string()
    } else {
        format!("'{}'", consumed.join("."))
    }
}

fn indexed_schema_segment(segment: &str) -> Option<(&str, usize)> {
    let (keyword, raw_index) = segment.split_once('[')?;
    if !matches!(
        keyword,
        "anyOf" | "oneOf" | "allOf" | "prefixItems" | "items"
    ) {
        return None;
    }
    let index = raw_index.strip_suffix(']')?.parse().ok()?;
    Some((keyword, index))
}

fn is_direct_schema_keyword_segment(segment: &str) -> bool {
    matches!(
        segment,
        "items"
            | "additionalProperties"
            | "not"
            | "contains"
            | "if"
            | "then"
            | "else"
            | "propertyNames"
    )
}

fn schema_object_keys(node: &Value) -> String {
    node.as_object()
        .map(|object| object.keys().cloned().collect::<Vec<_>>().join(", "))
        .filter(|keys| !keys.is_empty())
        .unwrap_or_else(|| "<none>".to_string())
}

/// Resolves `$ref`s and auto-descends array `items` until the node exposes
/// `properties` (or cannot descend further).
fn descend_to_properties<'a>(node: &'a Value, root: &'a Value) -> Result<&'a Value, String> {
    let mut node = deref_schema_node(node, root)?;
    for _ in 0..SCHEMA_PATH_RESOLUTION_LIMIT {
        if node.get("properties").is_some() {
            return Ok(node);
        }
        let Some(items) = node.get("items").filter(|items| items.is_object()) else {
            return Ok(node);
        };
        node = deref_schema_node(items, root)?;
    }
    Err("schema nesting exceeds resolution limit".to_string())
}

fn deref_schema_node<'a>(mut node: &'a Value, root: &'a Value) -> Result<&'a Value, String> {
    for _ in 0..SCHEMA_PATH_RESOLUTION_LIMIT {
        let Some(reference) = node.get("$ref").and_then(Value::as_str) else {
            return Ok(node);
        };
        let target = if reference == "#" {
            Some(root)
        } else {
            reference
                .strip_prefix('#')
                .filter(|pointer| pointer.starts_with('/'))
                .and_then(|pointer| root.pointer(pointer))
        };
        let Some(target) = target else {
            return Err(format!("unresolvable schema reference '{reference}'"));
        };
        node = target;
    }
    Err("schema $ref chain exceeds resolution limit".to_string())
}

/// Copies the root `$defs` entries still referenced by `subtree` onto it so
/// the extracted subtree stays resolvable on its own.
fn attach_reachable_schema_defs(subtree: &mut Value, root: &Value) {
    let Some(Value::Object(root_defs)) = root.get(JSON_SCHEMA_DEFS_KEY) else {
        return;
    };
    let mut queue = Vec::new();
    collect_local_def_refs(subtree, &mut queue);
    let mut needed = Map::new();
    while let Some(name) = queue.pop() {
        if needed.contains_key(&name) {
            continue;
        }
        if let Some(def) = root_defs.get(&name) {
            collect_local_def_refs(def, &mut queue);
            needed.insert(name, def.clone());
        }
    }
    if !needed.is_empty() {
        insert_schema_defs(subtree, needed);
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

/// Derives the provider value schema for one capability output contract,
/// without the generated result-envelope wrapper.
///
/// The returned schema is self-contained: nested `$defs` are hoisted to its
/// root and unresolved provider references fail closed.
#[must_use]
pub fn provider_value_schema(output_contract: &OutputContract) -> Option<Value> {
    raw_provider_value_schema(output_contract)
        .map(|schema| strip_unresolved_schema_refs(hoist_nested_schema_defs(schema)))
}

/// Renders the one-line generated-call signature surfaced on search hits,
/// e.g. `tools.slack.conversations.history({ channel: string, limit?:
/// integer, …+4 }) -> value: object`.
///
/// Parameters come from the executable Code Mode input schema's top-level
/// properties: required parameters first as `name: type`, optional parameters
/// as `name?: type`, enums as up to three variants,
/// and nested objects rendered as `object` so agents know to `describe` them.
/// For REST capabilities the grouped `path`/`query`/`header`/`cookie`
/// location objects and the body media-type alias keys are skipped — the
/// flattened argument names plus `body` carry the same information. The list
/// caps at eight parameters and roughly 220 chars with an `…+N` marker
/// (`SIGNATURE_PARAM_LIMIT`/`SIGNATURE_MAX_CHARS`), and the return hint
/// names the bare provider value type when the output contract is known.
#[must_use]
pub fn code_mode_call_signature(capability: &Capability, full_path: &str) -> String {
    let schema = code_mode_tool_input_schema(capability);
    let parameters = signature_parameters(capability, &schema);
    let return_hint = provider_value_schema(&capability.output_contract)
        .map(|schema| {
            format!(
                " -> value: {}",
                json_schema_primary_type(&schema).unwrap_or("any")
            )
        })
        .unwrap_or_default();
    let mut visible = parameters.len().min(SIGNATURE_PARAM_LIMIT);
    loop {
        let signature = render_signature(full_path, &parameters, visible, &return_hint);
        if visible == 0 || signature.chars().count() <= SIGNATURE_MAX_CHARS {
            return signature;
        }
        visible -= 1;
    }
}

struct SignatureParameter {
    name: String,
    required: bool,
    type_label: String,
}

fn render_signature(
    full_path: &str,
    parameters: &[SignatureParameter],
    visible: usize,
    return_hint: &str,
) -> String {
    let omitted = parameters.len().saturating_sub(visible);
    let mut rendered = parameters
        .iter()
        .take(visible)
        .map(|parameter| {
            let optional = if parameter.required { "" } else { "?" };
            format!("{}{optional}: {}", parameter.name, parameter.type_label)
        })
        .collect::<Vec<_>>();
    if omitted > 0 {
        rendered.push(format!("…+{omitted}"));
    }
    let args = if rendered.is_empty() {
        String::new()
    } else {
        format!("{{ {} }}", rendered.join(", "))
    };
    format!("{full_path}({args}){return_hint}")
}

fn signature_parameters(capability: &Capability, schema: &Value) -> Vec<SignatureParameter> {
    let Some(root) = schema.as_object() else {
        return Vec::new();
    };
    let Some(properties) = root.get("properties").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut required = required_string_set(root);
    let mut skipped: BTreeSet<&str> = BTreeSet::new();
    if let UpstreamBinding::Rest(binding) = &capability.upstream_binding {
        // The mixed REST schema keeps the grouped location objects and the
        // body media-type aliases alongside the flattened argument names; the
        // flat names plus `body` already carry the same information. Required
        // flat parameters are encoded as `allOf` alias groups there, so
        // requiredness is recovered from the binding facts instead.
        skipped.extend(["path", "query", "header", "cookie", "json"]);
        skipped.extend(REST_BODY_MEDIA_TYPE_KEYS.iter().copied());
        for parameter in effective_rest_parameters(binding) {
            if parameter.required {
                required.insert(parameter.name);
            }
        }
        if binding.request_bodies.iter().any(|body| body.required) {
            required.insert("body".to_string());
        }
    }
    let mut parameters = properties
        .iter()
        .filter(|(name, _)| !skipped.contains(name.as_str()))
        .map(|(name, property)| SignatureParameter {
            required: required.contains(name),
            type_label: signature_type_label(property, schema),
            name: name.clone(),
        })
        .collect::<Vec<_>>();
    // Required parameters first; schema declaration order within each group.
    parameters.sort_by_key(|parameter| !parameter.required);
    parameters
}

fn signature_type_label(property: &Value, root: &Value) -> String {
    let resolved = deref_schema_node(property, root).unwrap_or(property);
    if let Some(values) = resolved.get("enum").and_then(Value::as_array)
        && !values.is_empty()
    {
        let mut variants = values
            .iter()
            .take(SIGNATURE_ENUM_VARIANT_LIMIT)
            .map(|value| serde_json::to_string(value).unwrap_or_else(|_| "?".to_string()))
            .collect::<Vec<_>>();
        if values.len() > SIGNATURE_ENUM_VARIANT_LIMIT {
            variants.push("…".to_string());
        }
        return variants.join("|");
    }
    if let Some(primary) = json_schema_primary_type(resolved) {
        return primary.to_string();
    }
    if resolved.get("properties").is_some() {
        return "object".to_string();
    }
    if resolved.get("items").is_some() {
        return "array".to_string();
    }
    "any".to_string()
}

fn raw_provider_value_schema(output_contract: &OutputContract) -> Option<Value> {
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
    fn provider_value_schema_is_bare_value_shape_with_hoisted_defs() {
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

        let schema =
            provider_value_schema(&capability.output_contract).expect("provider value schema");
        assert_eq!(
            executable_schema_unresolved_refs(&schema),
            Vec::<String>::new()
        );
        assert!(schema.pointer("/$defs/Issue").is_some());
        // The schema describes the bare provider value: no generated result
        // envelope wrapper around it.
        assert!(schema.pointer("/properties/ok").is_none());
        assert!(schema.pointer("/properties/envelope").is_none());
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert!(compiled.is_valid(&json!({ "title": "hello" })));
        assert!(!compiled.is_valid(&json!({})));
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

    #[test]
    fn bound_schema_to_budget_keeps_small_schemas_unchanged() {
        let schema = json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": { "type": "string" }
            }
        });

        let (bounded, truncated) = bound_schema_to_budget(schema.clone(), 8192);

        assert!(!truncated);
        assert_eq!(bounded, schema);
    }

    #[test]
    fn bound_schema_to_budget_reports_preexisting_truncation_markers() {
        let schema = json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "x-coral-truncated": true,
                    "description": "type Filter omitted"
                }
            }
        });

        let (bounded, truncated) = bound_schema_to_budget(schema.clone(), 8192);

        assert!(truncated);
        assert_eq!(bounded, schema);
        assert_eq!(truncated_schema_paths(&bounded), vec!["filter"]);
    }

    #[test]
    fn bound_schema_to_budget_falls_back_to_root_stub_for_wide_shallow_schemas() {
        let properties = (0..300)
            .map(|index| {
                (
                    format!("param_{index}"),
                    json!({
                        "type": "string",
                        "description": "x".repeat(80)
                    }),
                )
            })
            .collect::<Map<_, _>>();
        let schema = json!({
            "type": "object",
            "properties": properties,
            "additionalProperties": false
        });

        let (bounded, truncated) = bound_schema_to_budget(schema, 1024);

        assert!(truncated);
        assert!(
            serde_json::to_string(&bounded).expect("bounded json").len() <= 1024,
            "wide shallow schema must fit the budget"
        );
        assert_eq!(
            bounded.get(SCHEMA_TRUNCATION_KEY).and_then(Value::as_bool),
            Some(true)
        );
        assert!(bounded.get("properties").is_none());
        assert_eq!(truncated_schema_paths(&bounded), vec![""]);
    }

    fn oversized_test_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "shallow": { "$ref": "#/$defs/Shallow" },
                "a": {
                    "type": "object",
                    "properties": {
                        "b": {
                            "type": "object",
                            "properties": {
                                "c": {
                                    "type": "object",
                                    "properties": {
                                        "d": {
                                            "type": "object",
                                            "$ref": "#/$defs/Deep",
                                            "description": "x".repeat(6000)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "$defs": {
                "Shallow": {
                    "type": "string",
                    "description": "kept: referenced from the surviving body"
                },
                "Deep": {
                    "type": "object",
                    "description": "dropped once its only reference is pruned"
                }
            }
        })
    }

    #[test]
    fn bound_schema_to_budget_prunes_deep_subtrees_and_garbage_collects_defs() {
        let (bounded, truncated) = bound_schema_to_budget(oversized_test_schema(), 1024);

        assert!(truncated);
        assert!(
            serde_json::to_string(&bounded).expect("bounded json").len() <= 1024,
            "bounded schema must fit the budget"
        );
        // The depth-4 subtree collapsed into a typed truncation stub.
        let stub = bounded
            .pointer("/properties/a/properties/b/properties/c/properties/d")
            .expect("pruned subtree stub");
        assert_eq!(
            stub.get(SCHEMA_TRUNCATION_KEY).and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(stub.get("type").and_then(Value::as_str), Some("object"));
        assert!(stub.get("description").is_none());
        // Defs referenced only from pruned subtrees are garbage collected;
        // defs referenced from surviving nodes stay.
        assert!(bounded.pointer("/$defs/Shallow").is_some());
        assert!(bounded.pointer("/$defs/Deep").is_none());
        assert_eq!(
            truncated_schema_paths(&bounded),
            vec!["a.b.c.d".to_string()]
        );
    }

    #[test]
    fn bound_schema_to_budget_preserves_defs_reachable_from_root_ref() {
        let schema = json!({
            "$ref": "#/$defs/Foo",
            "properties": {
                "deep": {
                    "type": "object",
                    "properties": {
                        "a": {
                            "type": "object",
                            "properties": {
                                "b": {
                                    "type": "object",
                                    "properties": {
                                        "c": {
                                            "type": "object",
                                            "description": "x".repeat(6000)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "$defs": {
                "Foo": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                }
            }
        });

        let (bounded, truncated) = bound_schema_to_budget(schema, 1024);

        assert!(truncated);
        assert_eq!(
            bounded.get("$ref").and_then(Value::as_str),
            Some("#/$defs/Foo")
        );
        assert!(
            bounded.pointer("/$defs/Foo").is_some(),
            "root $ref target must stay reachable"
        );
        assert_eq!(
            executable_schema_unresolved_refs(&bounded),
            Vec::<String>::new()
        );
    }

    /// Flat define-once `$defs` graphs keep almost every byte at depth <= 2,
    /// so depth pruning alone cannot meet the budget: bounding must retain
    /// near defs whole (breadth-first from the body) and stub refs to the
    /// defs it drops.
    #[test]
    fn bound_schema_to_budget_retains_near_defs_and_stubs_far_refs_in_flat_graphs() {
        let wide_fields = |prefix: &str| {
            (0..24)
                .map(|index| (format!("{prefix}{index}"), json!({ "type": "string" })))
                .collect::<Map<_, _>>()
        };
        let mut near = wide_fields("near_field_");
        near.insert("far".to_string(), json!({ "$ref": "#/$defs/Far" }));
        let schema = json!({
            "type": "object",
            "properties": {
                "filter": { "$ref": "#/$defs/Filter" }
            },
            "$defs": {
                "Filter": {
                    "type": "object",
                    "properties": {
                        "and": { "type": "array", "items": { "$ref": "#/$defs/Filter" } },
                        "near": { "$ref": "#/$defs/Near" }
                    }
                },
                "Near": { "type": "object", "properties": near },
                "Far": { "type": "object", "properties": wide_fields("far_field_") }
            }
        });
        let budget = serde_json::to_string(&schema).expect("schema json").len() - 200;

        let (bounded, truncated) = bound_schema_to_budget(schema, budget);

        assert!(truncated);
        assert!(
            serde_json::to_string(&bounded).expect("bounded json").len() <= budget,
            "flat defs graph must actually meet the budget"
        );
        // Near defs survive whole, in body-reachability order.
        assert_eq!(
            bounded
                .pointer("/$defs/Near/properties/near_field_0/type")
                .and_then(Value::as_str),
            Some("string")
        );
        assert_eq!(
            bounded
                .pointer("/$defs/Filter/properties/and/items/$ref")
                .and_then(Value::as_str),
            Some("#/$defs/Filter")
        );
        // The farthest def is dropped and its ref site becomes a typed stub.
        assert!(bounded.pointer("/$defs/Far").is_none());
        let stub = bounded
            .pointer("/$defs/Near/properties/far")
            .expect("stubbed far ref");
        assert_eq!(
            stub.get(SCHEMA_TRUNCATION_KEY).and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(stub.get("type").and_then(Value::as_str), Some("object"));
        assert_eq!(
            truncated_schema_paths(&bounded),
            vec!["$defs.Near.far".to_string()]
        );
    }

    fn ref_navigation_test_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "filter": { "$ref": "#/$defs/Filter" }
            },
            "$defs": {
                "Filter": {
                    "type": "object",
                    "properties": {
                        "team": { "$ref": "#/$defs/StringComparator" },
                        "and": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/Filter" }
                        }
                    }
                },
                "StringComparator": {
                    "type": "object",
                    "properties": {
                        "eq": { "type": "string" }
                    }
                }
            }
        })
    }

    #[test]
    fn schema_subtree_at_path_resolves_refs_and_attaches_defs() {
        let schema = ref_navigation_test_schema();

        let subtree = schema_subtree_at_path(&schema, "filter").expect("filter subtree");

        assert_eq!(
            subtree
                .pointer("/properties/team/$ref")
                .and_then(Value::as_str),
            Some("#/$defs/StringComparator")
        );
        // The subtree stays self-contained: every reachable def, including the
        // recursive Filter self-ref, is attached.
        assert!(subtree.pointer("/$defs/StringComparator").is_some());
        assert!(subtree.pointer("/$defs/Filter").is_some());
    }

    #[test]
    fn schema_subtree_at_path_auto_descends_array_items() {
        let schema = ref_navigation_test_schema();

        let subtree =
            schema_subtree_at_path(&schema, "filter.and.team.eq").expect("nested subtree");

        assert_eq!(subtree.get("type").and_then(Value::as_str), Some("string"));
    }

    #[test]
    fn schema_subtree_at_path_accepts_json_pointer_style() {
        let schema = ref_navigation_test_schema();

        let dotted = schema_subtree_at_path(&schema, "filter.team").expect("dot path");
        let pointer = schema_subtree_at_path(&schema, "/filter/team").expect("pointer path");

        assert_eq!(dotted, pointer);
        assert_eq!(
            dotted
                .pointer("/properties/eq/type")
                .and_then(Value::as_str),
            Some("string")
        );
    }

    #[test]
    fn schema_subtree_at_path_reports_available_keys_on_invalid_segment() {
        let schema = ref_navigation_test_schema();

        let error =
            schema_subtree_at_path(&schema, "filter.bogus").expect_err("invalid segment fails");

        assert!(error.contains("'bogus'"));
        assert!(error.contains("'filter'"));
        assert!(error.contains("team"));
        assert!(error.contains("and"));
    }

    #[test]
    fn schema_subtree_at_path_resolves_schema_keyword_paths() {
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "anyOf": [
                            { "type": "string", "x-coral-truncated": true },
                            { "$ref": "#/$defs/Thing" }
                        ]
                    }
                }
            },
            "$defs": {
                "Thing": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                }
            }
        });

        let paths = truncated_schema_paths(&schema);
        assert_eq!(paths, vec!["items.items.anyOf[0]"]);
        let elided = schema_subtree_at_path(&schema, &paths[0]).expect("elided schema path");
        assert_eq!(elided.get("type").and_then(Value::as_str), Some("string"));

        let definition = schema_subtree_at_path(&schema, "$defs.Thing").expect("$defs subtree");
        assert_eq!(
            definition
                .pointer("/properties/id/type")
                .and_then(Value::as_str),
            Some("string")
        );

        let combinator_ref =
            schema_subtree_at_path(&schema, "items.items.anyOf[1]").expect("anyOf subtree");
        assert_eq!(
            combinator_ref
                .pointer("/properties/id/type")
                .and_then(Value::as_str),
            Some("string")
        );
    }

    #[test]
    fn call_signature_renders_rest_flat_args_with_required_first() {
        let mut capability = rest_test_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": {
                    "type": "object",
                    "required": ["owner", "repo", "pull_number"],
                    "properties": {
                        "owner": { "type": "string" },
                        "repo": { "type": "string" },
                        "pull_number": { "type": "integer" }
                    },
                    "additionalProperties": false
                },
                "query": {
                    "type": "object",
                    "properties": {
                        "per_page": { "type": "integer" },
                        "sort": {
                            "type": "string",
                            "enum": ["created", "updated", "popularity", "long-running"]
                        }
                    },
                    "additionalProperties": false
                }
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
                "required": ["event"],
                "properties": {
                    "event": { "type": "string" }
                },
                "additionalProperties": false
            })),
        });
        capability.output_contract = OutputContract::Single {
            schema: InvocationSchema::new(json!({ "type": "object" })),
        };

        let signature =
            code_mode_call_signature(&capability, "tools.github.rest.pulls.listReviews");

        assert_eq!(
            signature,
            "tools.github.rest.pulls.listReviews({ body: object, owner: string, repo: string, \
             pull_number: integer, per_page?: integer, \
             sort?: \"created\"|\"updated\"|\"popularity\"|… }) -> value: object"
        );
        assert!(signature.chars().count() <= 220);
    }

    #[test]
    fn call_signature_resolves_graphql_input_refs_to_object() {
        let mut capability = Capability::new(
            SourceId("src_linear".to_string()),
            "graph",
            "query_issues",
            ProviderOrigin {
                kind: ProviderOriginKind::GraphqlRootField,
                snapshot_ref: "interfaces/graph/provider-snapshot.yaml#/root_fields/issues"
                    .to_string(),
                provider_name: "issues".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Graphql(crate::model::GraphqlOperationBinding {
                endpoint_ref: "source/src_linear/interface/graph/endpoint/default".to_string(),
                operation_name: "QueryIssues".to_string(),
                graphql_operation_kind: crate::model::GraphqlOperationKind::Query,
                document_ref: "source/src_linear/interface/graph/generated/query_issues.graphql"
                    .to_string(),
                selection_set: None,
                variable_bindings: Vec::new(),
                response_path: vec!["issues".to_string()],
            }),
        );
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "required": ["filter"],
            "properties": {
                "filter": { "$ref": "#/$defs/IssueFilter" },
                "first": { "type": "integer" },
                "orderBy": { "type": "string", "enum": ["createdAt", "updatedAt"] }
            },
            "$defs": {
                "IssueFilter": {
                    "type": "object",
                    "properties": {
                        "team": { "type": "string" }
                    },
                    "additionalProperties": true
                }
            },
            "additionalProperties": false
        }));
        capability.output_contract = OutputContract::GraphqlData {
            schema: InvocationSchema::new(json!({ "type": "object" })),
        };

        let signature = code_mode_call_signature(&capability, "tools.linear.graph.query.issues");

        assert_eq!(
            signature,
            "tools.linear.graph.query.issues({ filter: object, first?: integer, \
             orderBy?: \"createdAt\"|\"updatedAt\" }) -> value: object"
        );
    }

    #[test]
    fn call_signature_caps_mcp_tool_parameters_with_overflow_marker() {
        let mut properties = Map::new();
        properties.insert("channel".to_string(), json!({ "type": "string" }));
        for index in 0..11 {
            properties.insert(format!("option_{index:02}"), json!({ "type": "boolean" }));
        }
        let mut capability = Capability::new(
            SourceId("src_demo".to_string()),
            "mcp",
            "list_items",
            ProviderOrigin {
                kind: ProviderOriginKind::McpTool,
                snapshot_ref: "interfaces/mcp/provider-snapshot.yaml#/tools/list_items".to_string(),
                provider_name: "list_items".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::McpTool(McpToolUpstreamBinding {
                server_ref: "source/src_demo/interface/mcp/server/default".to_string(),
                tool_name: "list_items".to_string(),
                task_support: McpTaskSupport::Unknown,
            }),
        );
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "required": ["channel"],
            "properties": properties,
            "additionalProperties": false
        }));

        let signature = code_mode_call_signature(&capability, "tools.demo.mcp.listItems");

        // Unknown output contract: no return hint. The required parameter
        // leads, the 8-parameter cap holds, and the overflow marker counts
        // every elided parameter.
        assert!(
            signature
                .starts_with("tools.demo.mcp.listItems({ channel: string, option_00?: boolean,")
        );
        assert!(signature.ends_with("…+4 })"));
        assert!(!signature.contains("-> value"));
        assert!(signature.chars().count() <= 220);
        assert_eq!(signature.matches(':').count(), 8);
    }

    #[test]
    fn call_signature_shrinks_below_char_cap_for_wide_parameters() {
        let mut properties = Map::new();
        for index in 0..8 {
            properties.insert(
                format!("very_long_generated_parameter_name_number_{index:02}"),
                json!({ "type": "string" }),
            );
        }
        let mut capability = Capability::new(
            SourceId("src_demo".to_string()),
            "mcp",
            "wide",
            ProviderOrigin {
                kind: ProviderOriginKind::McpTool,
                snapshot_ref: "interfaces/mcp/provider-snapshot.yaml#/tools/wide".to_string(),
                provider_name: "wide".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::McpTool(McpToolUpstreamBinding {
                server_ref: "source/src_demo/interface/mcp/server/default".to_string(),
                tool_name: "wide".to_string(),
                task_support: McpTaskSupport::Unknown,
            }),
        );
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "properties": properties,
            "additionalProperties": false
        }));

        let signature = code_mode_call_signature(&capability, "tools.demo.mcp.wide");

        assert!(signature.chars().count() <= 220, "{signature}");
        assert!(signature.contains("…+"), "{signature}");
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
