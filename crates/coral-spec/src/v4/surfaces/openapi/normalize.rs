//! Rewrites the `OpenAPI` 3.1 nullability spellings into the 3.0 forms the rest
//! of the importer already reads.
//!
//! 3.1 aligned with JSON Schema, which dropped the `nullable` keyword in favour
//! of declaring `null` as a type. Generators picked two spellings for it, and
//! both are everywhere: a `null` member in a `type` array (Discord declares 911
//! of them) and a union whose only other variant carries the real shape (`OpenAI`
//! declares 844 as `anyOf`, Discord 305 as `oneOf`).
//!
//! Neither survives the import untouched. A `type` array reads as `None`
//! through [`serde_json::Value::as_str`], so `{type: [array, "null"]}` lands on
//! the `object` default in `schemas.rs` and becomes an opaque
//! [`IrTypeShape::Json`](crate::v4::ir::IrTypeShape::Json), and at a response
//! root `classify_response_schema` calls the same array a `Singleton` when it
//! is a collection. A union is worse: nothing imports `anyOf`/`oneOf`, so the
//! declared shape is lost, and the inference passes refuse alternation
//! outright — a wrapped list whose rows sit behind `anyOf: [{type: array},
//! {type: "null"}]` fails row-path inference and collapses the whole relation
//! to a single JSON row.
//!
//! Normalizing the document once, before import, fixes every consumer at once:
//! the same parsed value is threaded into schema import, response
//! classification, row-path inference, cursor detection, and the validation
//! fingerprints. The alternative — teaching each reader both spellings — means
//! relaxing `schema_uses_alternation`, which the MCP importer shares.
//!
//! Only unions that reduce to exactly one declared shape are rewritten. A
//! genuine choice between shapes still reads as alternation, because it still
//! is one.

use serde_json::{Map, Value};

/// Collapses every nullable-union spelling in `value` into the shape it wraps.
///
/// Applied unconditionally rather than to 3.1 documents alone. Both rewrites
/// require a literal `null` type, which 3.0 does not have — it spells the same
/// thing with `nullable` — so this is a no-op on a valid 3.0 document, and the
/// existing 3.0 suite exercises the pass for free. A 3.0-labelled document that
/// smuggles in a 3.1 spelling is fixed rather than misread, which is the same
/// tolerance the importer already extends in the other direction by reading
/// `nullable` out of 3.1 documents.
pub(super) fn normalize_nullable_unions(value: &mut Value) {
    match value {
        Value::Object(object) => {
            // Children first, so a promoted variant is already normalized and a
            // second pass over the tree is never needed.
            for child in object.values_mut() {
                normalize_nullable_unions(child);
            }
            unwrap_nullable_union(object);
            drop_null_from_type_array(object);
        }
        Value::Array(items) => {
            for item in items {
                normalize_nullable_unions(item);
            }
        }
        _ => {}
    }
}

/// Replaces `{anyOf | oneOf: [T, {type: null}]}` with `T`, as long as the keys
/// the union sits beside do nothing but annotate it.
///
/// The variant is merged into those keys rather than replacing them, because
/// they carry the annotations the importer reads at the reference site: a
/// nullable `$ref` variant is routinely paired with a sibling `description`,
/// which `field_description` prefers over the referent's own. The siblings win
/// a conflict — they annotate the field at this site, while the variant only
/// supplies its shape. `import_parameters` reads `default` straight off this
/// map, so a variant's own `default` overwriting it would silently change what
/// every generated projection sends.
///
/// A sibling that declares shape blocks the unwrap outright. Both halves of
/// `{type: object, properties: {...}, anyOf: [{$ref: T}, {type: null}]}` apply,
/// and neither the merged map nor the 3.0 forms can say so: promoting the
/// `$ref` makes the in-place `properties` unreachable, because `resolve_ref`
/// answers with the referent alone, and keeping the properties loses `T`. Left
/// alone the object still reads as alternation and imports as opaque JSON,
/// which is what it did before this pass existed. The same guard settles an
/// object carrying two reducible unions, where each is shape the other would
/// overwrite.
///
/// Keeping the variant's `$ref` intact is most of the value. `import_schema`
/// names a type after the ref it came through, so unwrapping recovers hundreds
/// of named types per document that would otherwise import as anonymous JSON.
fn unwrap_nullable_union(object: &mut Map<String, Value>) {
    for keyword in ["anyOf", "oneOf"] {
        let Some(variant) = sole_declared_variant(object.get(keyword)).cloned() else {
            continue;
        };
        if object.keys().any(|key| key != keyword && !annotates(key)) {
            continue;
        }
        object.remove(keyword);
        for (key, value) in variant {
            object.entry(key).or_insert(value);
        }
    }
}

/// Whether `key` annotates the schema it sits in rather than constraining the
/// instance it describes.
///
/// An allowlist rather than a list of the keywords that do constrain, because
/// the two mistakes are not symmetric: an annotation missing from here leaves a
/// union unwrapped, which imports exactly as it did before this pass, while a
/// constraint missing from a denylist is dropped from the document silently.
///
/// Deliberately wider than `ANNOTATION_KEYS` in `json_schema.rs`, which answers
/// a different question — which of a reference site's keys may be carried onto
/// the schema it resolves to. The keys that walk drops (`example`,
/// `deprecated`) are still not constraints and still reach no reader, and
/// reusing the narrower list would refuse 21 of the 844 reducible unions in
/// `OpenAI`'s document over a sibling `example`.
fn annotates(key: &str) -> bool {
    // A vendor extension cannot constrain anything the importer reads.
    key.starts_with("x-")
        || matches!(
            key,
            "$comment"
                | "default"
                | "deprecated"
                | "description"
                | "example"
                | "examples"
                | "externalDocs"
                | "nullable"
                | "readOnly"
                | "title"
                | "writeOnly"
                | "xml"
        )
}

/// The single member of a union that declares a shape, when every other member
/// declares only `null`.
///
/// `None` for a union that declares no `null` member, or more than one shape.
/// A single-variant union with no `null` is left alone on purpose: GitHub's 3.0
/// document spells nullable refs as `anyOf: [{$ref: T}]`, and unwrapping those
/// would change the output of a source that imports correctly today.
fn sole_declared_variant(union: Option<&Value>) -> Option<&Map<String, Value>> {
    let variants = union?.as_array()?;
    if !variants.iter().any(declares_only_null) {
        return None;
    }
    let mut declared = variants
        .iter()
        .filter(|variant| !declares_only_null(variant));
    let variant = declared.next()?;
    if declared.next().is_some() {
        return None;
    }
    // A boolean schema (`true`/`false`) declares no keys to merge.
    variant.as_object()
}

/// Whether a schema accepts nothing but `null`.
fn declares_only_null(schema: &Value) -> bool {
    match schema.get("type") {
        Some(Value::String(declared)) => declared == "null",
        Some(Value::Array(declared)) => {
            !declared.is_empty() && declared.iter().all(|value| value.as_str() == Some("null"))
        }
        _ => false,
    }
}

/// Rewrites `type: [T, "null"]` to `type: T`.
///
/// Scalars already survive the array spelling — `json_schema_scalar_type` skips
/// `null` members — so this exists for `array` and `object`, which are read
/// through [`serde_json::Value::as_str`] and would otherwise fall through to
/// `Json`.
///
/// Does not fire on an array that declares no `null`: a single-member `[T]` is
/// equivalent to `T`, but rewriting it would touch documents this pass has no
/// business changing.
fn drop_null_from_type_array(object: &mut Map<String, Value>) {
    let Some(declared) = object.get("type").and_then(Value::as_array) else {
        return;
    };
    // A `type` array holds type names. Anything else is not one — most likely a
    // `properties` map for a property named `type`, which must be left alone.
    if !declared.iter().all(Value::is_string) {
        return;
    }
    let mut named = declared
        .iter()
        .filter(|value| value.as_str() != Some("null"));
    let Some(declared_type) = named.next() else {
        return;
    };
    // More than one shape is a genuine union; a single name with nothing
    // dropped was never a nullable spelling.
    if named.next().is_some() || declared.len() == 1 {
        return;
    }
    let declared_type = declared_type.clone();
    object.insert("type".to_string(), declared_type);
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::normalize_nullable_unions;

    fn normalized(mut value: serde_json::Value) -> serde_json::Value {
        normalize_nullable_unions(&mut value);
        value
    }

    #[test]
    fn unwraps_nullable_unions_into_the_shape_they_wrap() {
        assert_eq!(
            normalized(json!({"anyOf": [{"type": "string"}, {"type": "null"}]})),
            json!({"type": "string"})
        );
        assert_eq!(
            normalized(json!({"oneOf": [{"$ref": "#/components/schemas/user"}, {"type": "null"}]})),
            json!({"$ref": "#/components/schemas/user"})
        );
        // The `[null]` spelling of a null variant, which the cursor pass
        // already treats as declaring nothing.
        assert_eq!(
            normalized(json!({"anyOf": [{"type": "integer"}, {"type": ["null"]}]})),
            json!({"type": "integer"})
        );
    }

    #[test]
    fn merges_union_variants_under_their_sibling_annotations() {
        assert_eq!(
            normalized(json!({
                "description": "The owner, if assigned.",
                "title": "Owner",
                "x-oai-meta": {"name": "owner"},
                "anyOf": [{"$ref": "#/components/schemas/user"}, {"type": "null"}],
            })),
            json!({
                "$ref": "#/components/schemas/user",
                "description": "The owner, if assigned.",
                "title": "Owner",
                "x-oai-meta": {"name": "owner"},
            })
        );
        // The sibling annotates the field at this site; the variant only
        // supplies its shape, so the sibling wins a conflict.
        assert_eq!(
            normalized(json!({
                "description": "sibling",
                "anyOf": [{"type": "string", "description": "variant"}, {"type": "null"}],
            })),
            json!({"type": "string", "description": "sibling"})
        );
        // `import_parameters` reads this `default` off the parameter's own
        // schema map, so the variant's must not displace it.
        assert_eq!(
            normalized(json!({
                "default": 20,
                "anyOf": [{"type": "integer", "default": 100}, {"type": "null"}],
            })),
            json!({"type": "integer", "default": 20})
        );
    }

    #[test]
    fn leaves_unions_that_sit_beside_a_declared_shape_alone() {
        for schema in [
            // Both the in-place `properties` and the ref apply. Promoting the
            // ref would hide the properties, because `resolve_ref` answers with
            // the referent alone.
            json!({
                "type": "object",
                "properties": {"extra": {"type": "string"}},
                "anyOf": [{"$ref": "#/components/schemas/base"}, {"type": "null"}],
            }),
            // Two reducible unions: each is shape the other would overwrite.
            json!({
                "anyOf": [{"$ref": "#/components/schemas/a"}, {"type": "null"}],
                "oneOf": [{"$ref": "#/components/schemas/b"}, {"type": "null"}],
            }),
            // A reducible union beside a genuine alternation.
            json!({
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "oneOf": [{"$ref": "#/components/schemas/a"}, {"$ref": "#/components/schemas/b"}],
            }),
        ] {
            assert_eq!(normalized(schema.clone()), schema);
        }
    }

    #[test]
    fn drops_null_from_type_arrays() {
        assert_eq!(
            normalized(json!({"type": ["array", "null"], "items": {"type": "string"}})),
            json!({"type": "array", "items": {"type": "string"}})
        );
        assert_eq!(
            normalized(json!({"type": ["null", "object"], "properties": {}})),
            json!({"type": "object", "properties": {}})
        );
    }

    #[test]
    fn leaves_schemas_that_are_not_nullable_spellings_alone() {
        for schema in [
            // A genuine choice of shapes.
            json!({"anyOf": [{"type": "string"}, {"type": "integer"}, {"type": "null"}]}),
            json!({"oneOf": [{"$ref": "#/components/schemas/a"}, {"$ref": "#/components/schemas/b"}]}),
            // GitHub's 3.0 nullable-ref spelling.
            json!({"anyOf": [{"$ref": "#/components/schemas/a"}]}),
            json!({"type": ["string"]}),
            json!({"type": ["null"]}),
            json!({"type": "string", "nullable": true}),
        ] {
            assert_eq!(normalized(schema.clone()), schema);
        }
    }

    #[test]
    fn normalizes_every_schema_position() {
        assert_eq!(
            normalized(json!({
                "type": "object",
                "properties": {"id": {"anyOf": [{"type": "string"}, {"type": "null"}]}},
                "items": {"type": ["integer", "null"]},
                "additionalProperties": {"anyOf": [{"type": "boolean"}, {"type": "null"}]},
                "allOf": [{"type": ["object", "null"], "properties": {}}],
            })),
            json!({
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "items": {"type": "integer"},
                "additionalProperties": {"type": "boolean"},
                "allOf": [{"type": "object", "properties": {}}],
            })
        );
        // A union nested inside a union variant: children are normalized first,
        // so the promoted variant is already collapsed.
        assert_eq!(
            normalized(json!({
                "anyOf": [
                    {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    {"type": "null"},
                ],
            })),
            json!({"type": "string"})
        );
    }

    #[test]
    fn leaves_property_names_that_collide_with_keywords_alone() {
        // The rewrites key off array values, and a `properties` map holds
        // objects — so a property named `type` or `anyOf` is normalized as the
        // schema it is, and the map itself is untouched.
        assert_eq!(
            normalized(json!({
                "type": "object",
                "properties": {
                    "type": {"type": ["string", "null"]},
                    "anyOf": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
                    "default": {"type": "boolean"},
                },
            })),
            json!({
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "anyOf": {"type": "integer"},
                    "default": {"type": "boolean"},
                },
            })
        );
    }

    #[test]
    fn leaves_example_payloads_recognisable() {
        // Example and default payloads are never read into the IR, so a
        // rewrite inside one is inert — but a payload that merely looks like a
        // schema should still come through intact.
        assert_eq!(
            normalized(json!({
                "type": "object",
                "properties": {"kind": {"type": "string"}},
                "example": {"kind": "text"},
                "default": {"type": ["a", "b"]},
            })),
            json!({
                "type": "object",
                "properties": {"kind": {"type": "string"}},
                "example": {"kind": "text"},
                "default": {"type": ["a", "b"]},
            })
        );
    }
}
