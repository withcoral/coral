use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::v4::ir::IrScalarType;

const ANNOTATION_KEYS: &[&str] = &["$comment", "default", "description", "examples", "title"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefError<'a> {
    External(&'a str),
    NotFound(&'a str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonSchemaWalkError<'a> {
    ExternalRef(&'a str),
    RefCycle(&'a str),
    RefNotFound(&'a str),
    DepthExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JsonSchemaComparisonError {
    PropertyConflict(String),
    DepthExceeded,
}

#[derive(Debug, Default)]
pub(crate) struct JsonObjectShape {
    pub(crate) properties: BTreeMap<String, Value>,
    pub(crate) required: BTreeSet<String>,
}

pub(crate) fn resolve_local_ref<'a>(
    root: &'a Value,
    schema: &'a Value,
) -> Result<&'a Value, RefError<'a>> {
    let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
        return Ok(schema);
    };
    if !reference.starts_with("#/") {
        return Err(RefError::External(reference));
    }
    let pointer = reference.strip_prefix('#').unwrap_or(reference);
    root.pointer(pointer).ok_or(RefError::NotFound(reference))
}

pub(crate) fn with_resolved_json_schema<'a, T>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
    visit: impl FnOnce(&'a Value, &mut BTreeSet<String>, usize) -> Result<T, JsonSchemaWalkError<'a>>,
) -> Result<T, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }

    let reference = schema.get("$ref").and_then(Value::as_str);
    let guarded_reference = match reference {
        Some(reference) if reference.starts_with("#/") => {
            if !resolving_refs.insert(reference.to_string()) {
                return Err(JsonSchemaWalkError::RefCycle(reference));
            }
            Some(reference)
        }
        _ => None,
    };

    let resolved = resolve_local_ref(root, schema).map_err(json_schema_walk_error_from_ref);
    let next_depth = depth + 1;
    let result = match resolved {
        Ok(resolved) if resolved.get("$ref").is_some() => {
            with_resolved_json_schema(root, resolved, resolving_refs, next_depth, max_depth, visit)
        }
        Ok(resolved) => visit(resolved, resolving_refs, next_depth),
        Err(error) => Err(error),
    };

    if let Some(reference) = guarded_reference {
        resolving_refs.remove(reference);
    }

    result
}

pub(crate) fn resolve_json_schema_ref_with_siblings<'a>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        max_depth,
        |resolved, resolving_refs, next_depth| {
            let mut resolved = resolve_json_schema_child_refs_allow_cycles(
                root,
                resolved,
                resolving_refs,
                next_depth,
                max_depth,
            )?;
            if let (Some(referrer), Some(resolved)) = (schema.as_object(), resolved.as_object_mut())
            {
                for (key, value) in referrer {
                    if is_ref_site_metadata_key(key) {
                        resolved.insert(key.clone(), value.clone());
                    }
                }
            }
            Ok(resolved)
        },
    )
}

fn is_ref_site_metadata_key(key: &str) -> bool {
    ANNOTATION_KEYS.contains(&key)
}

fn resolve_json_schema_refs_allow_cycles<'a>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    match with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        max_depth,
        |resolved, resolving_refs, next_depth| {
            resolve_json_schema_child_refs_allow_cycles(
                root,
                resolved,
                resolving_refs,
                next_depth,
                max_depth,
            )
        },
    ) {
        Ok(resolved) => Ok(resolved),
        Err(JsonSchemaWalkError::RefCycle(_reference)) => Ok(schema.clone()),
        Err(error) => Err(error),
    }
}

fn resolve_json_schema_child_refs_allow_cycles<'a>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    let Some(object) = schema.as_object() else {
        return Ok(schema.clone());
    };

    let mut resolved = object.clone();
    for key in ["items", "additionalProperties", "not"] {
        if let Some(value) = object.get(key).filter(|value| value.is_object()) {
            resolved.insert(
                key.to_string(),
                resolve_json_schema_refs_allow_cycles(
                    root,
                    value,
                    resolving_refs,
                    depth,
                    max_depth,
                )?,
            );
        }
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(values) = object.get(key).and_then(Value::as_array) {
            resolved.insert(
                key.to_string(),
                Value::Array(
                    values
                        .iter()
                        .map(|value| {
                            resolve_json_schema_refs_allow_cycles(
                                root,
                                value,
                                resolving_refs,
                                depth,
                                max_depth,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            );
        }
    }
    for key in ["$defs", "definitions", "patternProperties", "properties"] {
        if let Some(schemas) = object.get(key).and_then(Value::as_object) {
            resolved.insert(
                key.to_string(),
                Value::Object(
                    schemas
                        .iter()
                        .map(|(name, schema)| {
                            resolve_json_schema_refs_allow_cycles(
                                root,
                                schema,
                                resolving_refs,
                                depth,
                                max_depth,
                            )
                            .map(|schema| (name.clone(), schema))
                        })
                        .collect::<Result<serde_json::Map<_, _>, _>>()?,
                ),
            );
        }
    }
    Ok(Value::Object(resolved))
}

fn json_schema_walk_error_from_ref(error: RefError<'_>) -> JsonSchemaWalkError<'_> {
    match error {
        RefError::External(reference) => JsonSchemaWalkError::ExternalRef(reference),
        RefError::NotFound(reference) => JsonSchemaWalkError::RefNotFound(reference),
    }
}

pub(crate) fn direct_json_object_shape(schema: &Value) -> JsonObjectShape {
    let Some(schema) = schema.as_object() else {
        return JsonObjectShape::default();
    };
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| {
            properties
                .iter()
                .map(|(name, property)| (name.clone(), property.clone()))
                .collect()
        })
        .unwrap_or_default();
    JsonObjectShape {
        properties,
        required: json_schema_required_fields(schema),
    }
}

pub(crate) fn json_schema_required_fields(
    schema: &serde_json::Map<String, Value>,
) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

pub(crate) fn json_schema_default_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

pub(crate) fn merge_json_object_shape_annotation_insensitive(
    target: &mut JsonObjectShape,
    source: JsonObjectShape,
    depth: usize,
    max_depth: usize,
) -> Result<(), JsonSchemaComparisonError> {
    for (name, property) in source.properties {
        if let Some(existing) = target.properties.get_mut(&name) {
            if json_schema_property_schemas_conflict(existing, &property, depth, max_depth)? {
                return Err(JsonSchemaComparisonError::PropertyConflict(name));
            }
            merge_json_schema_property_metadata(existing, &property);
        } else {
            target.properties.insert(name, property);
        }
    }
    target.required.extend(source.required);
    Ok(())
}

pub(crate) fn json_schema_scalar_type(schema: &Value) -> Option<IrScalarType> {
    json_schema_scalar_type_with_default(schema, None)
}

pub(crate) fn json_schema_scalar_type_or_string(schema: &Value) -> Option<IrScalarType> {
    json_schema_scalar_type_with_default(schema, Some("string"))
}

/// A schema's properties and declared types after folding its `allOf` branches
/// together.
#[derive(Debug, Default)]
pub(crate) struct MergedObjectView {
    /// Every `type` any branch declares. Usually one entry; an empty set means
    /// no branch declared a type at all.
    pub(crate) declared_types: BTreeSet<String>,
    pub(crate) properties: BTreeMap<String, Value>,
}

impl MergedObjectView {
    pub(crate) fn declares_type(&self, expected: &str) -> bool {
        self.declared_types.contains(expected)
    }
}

/// Folds a schema's `allOf` branches into one view of its properties.
///
/// Inference passes that ask shape questions of a response — is this a page
/// envelope, does it carry a cursor — cannot answer them through a composed
/// schema without this: the properties live on the branches, not the schema.
/// `OData` collection responses are the motivating case, being uniformly
/// `type: object` plus an `allOf` of a shared pagination base and the rows.
///
/// Only `allOf` is folded. `anyOf`, `oneOf`, and `not` describe alternation and
/// negation rather than intersection, so there is no single property map to
/// answer questions against, and this returns `None` for them — which is what
/// the callers did for all composition before.
///
/// Returns `None` when the schema is not object-like, so callers can keep
/// treating "not an object" and "cannot be read as an envelope" alike.
///
/// On duplicate property names the first branch wins, and no conflict is
/// reported. This answers a heuristic question, so an annotation-only
/// disagreement between branches must not discard the whole envelope. Type
/// import has the opposite need and folds through
/// [`merge_json_object_shape_annotation_insensitive`], which reports a genuine
/// disagreement rather than resolving it by branch order.
pub(crate) fn merged_all_of_object_view<'a>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
) -> Option<MergedObjectView> {
    let mut view = MergedObjectView::default();
    let mut alternation = false;
    collect_all_of_branches(
        root,
        schema,
        resolving_refs,
        depth,
        max_depth,
        &mut view,
        &mut alternation,
    );
    if alternation {
        return None;
    }

    // Some branch has to declare `type: object`. Declaring `properties` is not
    // enough on its own: in JSON Schema `properties` constrains an instance
    // only if it happens to be an object, and asserts nothing about whether it
    // is one — `{properties: {...}}` validates a string quite happily. Reading
    // that constraint as an assertion would promote schemas the author never
    // said were objects, and the expensive failure of this walk is a *wrong*
    // envelope, which silently discards the declared resource.
    //
    // Nothing in the ingested catalog would gain from the looser rule: every
    // typeless response root across the six v4 sources is `allOf`, `anyOf`, or
    // `oneOf`, never a bare property bag. Revisit if a real spec turns up where
    // it would help.
    view.declares_type("object").then_some(view)
}

/// Walks the schema and its `allOf` members, accumulating types and properties.
///
/// An unresolvable, cyclic, or too-deep branch contributes nothing rather than
/// abandoning the merge: its siblings may still describe the envelope. An
/// alternation anywhere is different — it makes the whole merge meaningless, so
/// it is recorded and the caller discards the view.
fn collect_all_of_branches(
    root: &Value,
    schema: &Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
    view: &mut MergedObjectView,
    alternation: &mut bool,
) {
    // Discarded on purpose: a branch that cannot be walked contributes nothing,
    // and its siblings still can.
    let _unwalkable_branch = with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        max_depth,
        |resolved, resolving_refs, next_depth| {
            if schema_uses_alternation(resolved) {
                *alternation = true;
                return Ok(());
            }
            match resolved.get("type") {
                Some(Value::String(value)) => {
                    view.declared_types.insert(value.clone());
                }
                Some(Value::Array(values)) => {
                    view.declared_types.extend(
                        values
                            .iter()
                            .filter_map(Value::as_str)
                            .map(ToString::to_string),
                    );
                }
                _ => {}
            }
            if let Some(properties) = resolved.get("properties").and_then(Value::as_object) {
                for (name, property) in properties {
                    view.properties
                        .entry(name.clone())
                        .or_insert_with(|| property.clone());
                }
            }
            for branch in resolved
                .get("allOf")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                collect_all_of_branches(
                    root,
                    branch,
                    resolving_refs,
                    next_depth,
                    max_depth,
                    view,
                    alternation,
                );
            }
            Ok(())
        },
    );
}

/// Whether the schema describes a choice between shapes rather than one shape.
pub(crate) fn schema_uses_alternation(schema: &Value) -> bool {
    ["anyOf", "oneOf", "not"]
        .iter()
        .any(|keyword| schema.get(*keyword).is_some())
}

pub(crate) fn json_schema_type_contains(schema: &Value, expected: &str) -> bool {
    match schema.get("type") {
        Some(Value::String(value)) => value == expected,
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .any(|value| value == expected),
        _ => false,
    }
}

/// Whether `expected` is the only type the schema declares, ignoring `null`.
///
/// Weaker than `json_schema_type_contains` on purpose. A nullable schema spells
/// itself as a union with `null` — `{"type": ["array", "null"]}` is 3.1's
/// nullable collection — so `null` cannot count against a single-shape reading.
/// Every other extra type can: `{"type": ["array", "string"]}` describes a
/// response that is sometimes a collection and sometimes one string, and no
/// single shape is true of both.
pub(crate) fn json_schema_declares_only_type(schema: &Value, expected: &str) -> bool {
    let mut declared = schema_type_values(schema)
        .into_iter()
        .filter(|value| *value != "null");
    declared.next() == Some(expected) && declared.next().is_none()
}

/// Whether the schema declares a `type` at all.
///
/// Separates "declared a type this code has no shape for" from "declared
/// nothing", which JSON Schema reads as accepting any instance and the importer
/// reads as an object.
pub(crate) fn json_schema_has_declared_type(schema: &Value) -> bool {
    !schema_type_values(schema).is_empty()
}

pub(crate) fn json_schema_type_display(schema: &Value) -> String {
    match schema.get("type") {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("|"),
        _ => "unknown".to_string(),
    }
}

fn json_schema_scalar_type_with_default(
    schema: &Value,
    missing_type_default: Option<&str>,
) -> Option<IrScalarType> {
    let schema_types = schema_type_values(schema);
    if schema_types.is_empty() {
        if let Some(scalar) = scalar_for_typeless_schema_format(schema) {
            return Some(scalar);
        }
        return missing_type_default
            .and_then(scalar_for_schema_type)
            .map(|scalar| apply_string_format(schema, scalar));
    }

    let mut scalar = None;
    for schema_type in schema_types {
        if schema_type == "null" {
            continue;
        }
        let candidate = scalar_for_schema_type(schema_type)?;
        if scalar.is_some_and(|existing| existing != candidate) {
            return None;
        }
        scalar = Some(candidate);
    }
    scalar.map(|scalar| apply_string_format(schema, scalar))
}

fn schema_type_values(schema: &Value) -> Vec<&str> {
    match schema.get("type") {
        Some(Value::String(value)) => vec![value.as_str()],
        Some(Value::Array(values)) => values.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

fn scalar_for_schema_type(schema_type: &str) -> Option<IrScalarType> {
    match schema_type {
        "string" => Some(IrScalarType::String),
        "integer" => Some(IrScalarType::Integer),
        "number" => Some(IrScalarType::Number),
        "boolean" => Some(IrScalarType::Boolean),
        _ => None,
    }
}

fn apply_string_format(schema: &Value, scalar: IrScalarType) -> IrScalarType {
    if scalar == IrScalarType::String
        && schema
            .get("format")
            .and_then(Value::as_str)
            .is_some_and(|format| matches!(format, "date-time" | "datetime"))
    {
        IrScalarType::Timestamp
    } else {
        scalar
    }
}

fn scalar_for_typeless_schema_format(schema: &Value) -> Option<IrScalarType> {
    schema
        .get("format")
        .and_then(Value::as_str)
        .and_then(|format| match format {
            "date-time" | "datetime" => Some(IrScalarType::Timestamp),
            _ => None,
        })
}

fn json_schema_property_schemas_conflict(
    existing: &Value,
    candidate: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<bool, JsonSchemaComparisonError> {
    let Ok(left) = schema_validation_fingerprint(existing, depth, max_depth) else {
        return Err(JsonSchemaComparisonError::DepthExceeded);
    };
    let Ok(right) = schema_validation_fingerprint(candidate, depth, max_depth) else {
        return Err(JsonSchemaComparisonError::DepthExceeded);
    };
    Ok(left != right)
}

fn schema_validation_fingerprint<'a>(
    schema: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }
    let next_depth = depth + 1;

    let Some(object) = schema.as_object() else {
        return Ok(schema.clone());
    };

    let mut out = serde_json::Map::new();
    for (key, value) in object
        .iter()
        .filter(|(key, _value)| !ANNOTATION_KEYS.contains(&key.as_str()))
    {
        let value = match key.as_str() {
            "$defs" | "definitions" | "dependentSchemas" | "patternProperties" | "properties" => {
                schema_map_validation_fingerprint(value, next_depth, max_depth)?
            }
            "dependencies" => {
                schema_dependency_map_validation_fingerprint(value, next_depth, max_depth)?
            }
            "additionalItems"
            | "additionalProperties"
            | "contains"
            | "contentSchema"
            | "else"
            | "if"
            | "items"
            | "not"
            | "propertyNames"
            | "then"
            | "unevaluatedItems"
            | "unevaluatedProperties" => {
                schema_or_schema_array_validation_fingerprint(value, next_depth, max_depth)?
            }
            "allOf" | "anyOf" | "oneOf" | "prefixItems" => {
                schema_array_validation_fingerprint(value, next_depth, max_depth)?
            }
            "type" => schema_type_validation_fingerprint(value),
            _ => value.clone(),
        };
        out.insert(key.clone(), value);
    }
    Ok(Value::Object(out))
}

fn schema_map_validation_fingerprint<'a>(
    schemas: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }
    let next_depth = depth + 1;

    let Some(object) = schemas.as_object() else {
        return Ok(schemas.clone());
    };

    Ok(Value::Object(
        object
            .iter()
            .map(|(name, schema)| {
                schema_validation_fingerprint(schema, next_depth, max_depth)
                    .map(|schema| (name.clone(), schema))
            })
            .collect::<Result<serde_json::Map<_, _>, _>>()?,
    ))
}

fn schema_dependency_map_validation_fingerprint<'a>(
    dependencies: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }
    let next_depth = depth + 1;

    let Some(object) = dependencies.as_object() else {
        return Ok(dependencies.clone());
    };

    Ok(Value::Object(
        object
            .iter()
            .map(|(name, dependency)| {
                schema_or_schema_array_validation_fingerprint(dependency, next_depth, max_depth)
                    .map(|dependency| (name.clone(), dependency))
            })
            .collect::<Result<serde_json::Map<_, _>, _>>()?,
    ))
}

fn schema_or_schema_array_validation_fingerprint<'a>(
    value: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    match value {
        Value::Array(_values) => schema_array_validation_fingerprint(value, depth, max_depth),
        Value::Object(_) | Value::Bool(_) => schema_validation_fingerprint(value, depth, max_depth),
        other => Ok(other.clone()),
    }
}

fn schema_array_validation_fingerprint<'a>(
    schemas: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }
    let next_depth = depth + 1;

    let Some(values) = schemas.as_array() else {
        return Ok(schemas.clone());
    };

    Ok(Value::Array(
        values
            .iter()
            .map(|schema| schema_validation_fingerprint(schema, next_depth, max_depth))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn schema_type_validation_fingerprint(value: &Value) -> Value {
    let Value::Array(values) = value else {
        return value.clone();
    };
    let mut values = values.clone();
    values.sort_by_key(Value::to_string);
    Value::Array(values)
}

fn merge_json_schema_property_metadata(existing: &mut Value, candidate: &Value) {
    let (Some(existing), Some(candidate)) = (existing.as_object_mut(), candidate.as_object())
    else {
        return;
    };
    for key in ANNOTATION_KEYS {
        if !existing.contains_key(*key)
            && let Some(value) = candidate.get(*key)
        {
            existing.insert((*key).to_string(), value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::*;

    #[test]
    fn scalar_type_accepts_nullable_type_arrays() {
        assert_eq!(
            json_schema_scalar_type(&json!({"type": ["integer", "null"]})),
            Some(IrScalarType::Integer)
        );
    }

    #[test]
    fn scalar_type_rejects_ambiguous_scalar_type_arrays() {
        assert_eq!(
            json_schema_scalar_type(&json!({"type": ["integer", "string"]})),
            None
        );
    }

    #[test]
    fn scalar_type_maps_string_datetime_formats_to_timestamp() {
        assert_eq!(
            json_schema_scalar_type(&json!({"type": "string", "format": "datetime"})),
            Some(IrScalarType::Timestamp)
        );
    }

    #[test]
    fn scalar_type_maps_typeless_datetime_formats_to_timestamp() {
        assert_eq!(
            json_schema_scalar_type(&json!({"format": "date-time"})),
            Some(IrScalarType::Timestamp)
        );
    }

    #[test]
    fn resolve_local_ref_returns_input_without_ref() {
        let root = json!({"$defs": {}});
        let schema = json!({"type": "string"});

        let resolved = resolve_local_ref(&root, &schema).expect("schema");

        assert!(std::ptr::eq(
            std::ptr::from_ref(resolved),
            std::ptr::from_ref(&schema)
        ));
    }

    #[test]
    fn resolve_local_ref_returns_local_pointer_target() {
        let root = json!({
            "$defs": {
                "Name": {"type": "string"}
            }
        });
        let schema = json!({"$ref": "#/$defs/Name"});

        let resolved = resolve_local_ref(&root, &schema).expect("schema");

        assert_eq!(resolved, &json!({"type": "string"}));
    }

    #[test]
    fn resolve_local_ref_rejects_external_refs() {
        let root = json!({});
        let schema = json!({"$ref": "https://example.com/schema.json#/Name"});

        assert_eq!(
            resolve_local_ref(&root, &schema),
            Err(RefError::External("https://example.com/schema.json#/Name"))
        );
    }

    #[test]
    fn resolve_local_ref_reports_missing_refs() {
        let root = json!({});
        let schema = json!({"$ref": "#/$defs/Missing"});

        assert_eq!(
            resolve_local_ref(&root, &schema),
            Err(RefError::NotFound("#/$defs/Missing"))
        );
    }

    #[test]
    fn resolved_schema_walk_keeps_ref_guard_during_visit() {
        let root = json!({
            "$defs": {
                "Name": {"type": "string"}
            }
        });
        let schema = json!({"$ref": "#/$defs/Name"});
        let mut resolving_refs = BTreeSet::new();

        let guard_was_active = with_resolved_json_schema(
            &root,
            &schema,
            &mut resolving_refs,
            0,
            8,
            |_schema, resolving_refs, _depth| Ok(resolving_refs.contains("#/$defs/Name")),
        )
        .expect("walk");

        assert!(guard_was_active);
        assert!(resolving_refs.is_empty());
    }

    #[test]
    fn resolved_schema_ref_with_siblings_resolves_schema_bearing_children() {
        let root = json!({
            "$defs": {
                "Value": {"type": "integer"}
            },
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "value": {"$ref": "#/$defs/Value"}
                    }
                }
            }
        });
        let mut resolving_refs = BTreeSet::new();
        let filter = root.pointer("/properties/filter").expect("filter schema");

        let resolved =
            resolve_json_schema_ref_with_siblings(&root, filter, &mut resolving_refs, 0, 8)
                .expect("resolved");

        assert_eq!(
            resolved
                .pointer("/properties/value/type")
                .and_then(Value::as_str),
            Some("integer")
        );
        assert!(resolving_refs.is_empty());
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_keeps_metadata() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": ["string", "null"],
                    "title": "Query"
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {
                    "type": ["null", "string"],
                    "description": "Search query"
                }
            }
        }));

        merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100).expect("merge");

        let query = target.properties.get("query").expect("query property");
        assert_eq!(query.get("title").and_then(Value::as_str), Some("Query"));
        assert_eq!(
            query.get("description").and_then(Value::as_str),
            Some("Search query")
        );
        assert!(target.required.contains("query"));
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_ignores_nested_schema_annotations() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "description": "Public status"
                        }
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "description": "Internal workflow state"
                        }
                    }
                }
            }
        }));

        merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100).expect("merge");
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_keeps_const_values_opaque() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "const": {
                                "description": "open"
                            }
                        }
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "const": {}
                        }
                    }
                }
            }
        }));

        assert_eq!(
            merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100),
            Err(JsonSchemaComparisonError::PropertyConflict(
                "filter".to_string()
            ))
        );
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_keeps_enum_values_opaque() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "enum": [
                                {"description": "open"}
                            ]
                        }
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "enum": [
                                {}
                            ]
                        }
                    }
                }
            }
        }));

        assert_eq!(
            merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100),
            Err(JsonSchemaComparisonError::PropertyConflict(
                "filter".to_string()
            ))
        );
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_keeps_unknown_keyword_values_opaque() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "x-coral-metadata": {
                        "description": "left"
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "x-coral-metadata": {}
                }
            }
        }));

        assert_eq!(
            merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100),
            Err(JsonSchemaComparisonError::PropertyConflict(
                "filter".to_string()
            ))
        );
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_recurses_into_schema_dependencies() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "dependencies": {
                        "status": {
                            "type": "object",
                            "properties": {
                                "reason": {
                                    "type": "string",
                                    "description": "Public reason"
                                }
                            }
                        },
                        "owner": ["team"]
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "dependencies": {
                        "status": {
                            "type": "object",
                            "properties": {
                                "reason": {
                                    "type": "string",
                                    "description": "Internal reason"
                                }
                            }
                        },
                        "owner": ["team"]
                    }
                }
            }
        }));

        merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100).expect("merge");
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_reports_depth_exceeded() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"}
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"}
                    }
                }
            }
        }));

        assert_eq!(
            merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 1),
            Err(JsonSchemaComparisonError::DepthExceeded)
        );
    }

    #[test]
    fn default_to_string_preserves_string_values_and_serializes_other_json() {
        assert_eq!(json_schema_default_to_string(&json!("text")), "text");
        assert_eq!(json_schema_default_to_string(&json!(30)), "30");
        assert_eq!(json_schema_default_to_string(&json!(true)), "true");
        assert_eq!(
            json_schema_default_to_string(&json!({"enabled": true})),
            r#"{"enabled":true}"#
        );
    }
}
