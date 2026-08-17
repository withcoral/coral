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
//! Applied as each schema is read rather than to the document up front. The
//! parsed document stays exactly as the provider published it, which keeps
//! every `$ref` resolvable: a pointer may name a subschema —
//! `#/components/schemas/Page/anyOf/0` is legal, and `resolve_local_ref` serves
//! it — and rewriting the tree in place would leave that pointer dangling, so
//! the column it describes would import as opaque JSON or the operation would
//! be dropped. Reading through [`SchemaRoot`](crate::v4::surfaces::json_schema::SchemaRoot)
//! costs a clone only where a schema actually reduces — a union that declares a
//! genuine choice is decided against the borrowed map and left alone — and
//! confines the rewrite to schema positions: an `example` or `default` payload
//! is never walked, so a sample that merely looks like a schema is never
//! mangled.
//!
//! Only unions that reduce to exactly one declared shape are rewritten. A
//! genuine choice between shapes still reads as alternation, because it still
//! is one.

use serde_json::{Map, Value};

/// Ceiling on re-applying the rewrites to one schema.
///
/// Each pass consumes one level of nesting the document actually declares — a
/// union whose variant is itself a union — so this only bounds how much of that
/// nesting is followed, and is never reached by a schema anyone writes.
const MAX_NORMALIZE_PASSES: usize = 8;

/// The schema as the importer's readers should see it, or `None` when it is
/// already in a spelling they read.
///
/// `None` is the answer for almost every schema in every document, which is
/// what makes reading through this affordable: [`reduces`] decides against the
/// borrowed map, so nothing is cloned unless a rewrite really applies.
///
/// Applied to 3.0 documents as well as 3.1. Both rewrites require a literal
/// `null` type, which 3.0 does not have — it spells the same thing with
/// `nullable` — so this answers `None` for every schema in a valid 3.0
/// document, and the existing 3.0 suite exercises it for free. A 3.0-labelled
/// document that smuggles in a 3.1 spelling is read rather than misread, which
/// is the same tolerance the importer already extends in the other direction by
/// reading `nullable` out of 3.1 documents.
pub(crate) fn normalized_schema(schema: &Value) -> Option<Value> {
    let object = schema.as_object()?;
    if !reduces(object) {
        return None;
    }
    let mut object = object.clone();
    let mut rewritten = false;
    // A promoted variant can carry a union of its own. The document-wide pass
    // this replaced normalized children first and never had to look twice;
    // reading one schema at a time, the second look happens here.
    for _ in 0..MAX_NORMALIZE_PASSES {
        let unwrapped = unwrap_nullable_union(&mut object);
        let collapsed = drop_null_from_type_array(&mut object);
        if !(unwrapped || collapsed) {
            break;
        }
        rewritten = true;
    }
    rewritten.then(|| Value::Object(object))
}

/// Whether either rewrite would change this schema.
///
/// Decided against the borrowed map, so the clone below is spent only where a
/// rewrite really applies. Answering the looser "declares a keyword a rewrite
/// could act on" would be a line shorter and would clone every union in every
/// document — including the ones that never reduce, like the `anyOf: [{$ref:
/// T}]` spelling GitHub's 3.0 document uses throughout, which no 3.1 rewrite
/// touches and which would pay a clone per read forever.
///
/// The common answer still costs three key lookups: a schema declaring no
/// union and no `type` array leaves both halves on their first lookup.
///
/// Each half pairs with the rewrite it predicts — [`unwrappable_union`] with
/// [`unwrap_nullable_union`], [`collapsible_type_array`] with
/// [`drop_null_from_type_array`] — and the rewrites re-check through the same
/// predicates rather than repeating the conditions, so the two cannot drift
/// into disagreeing about what reduces.
fn reduces(object: &Map<String, Value>) -> bool {
    collapsible_type_array(object)
        || ["anyOf", "oneOf"]
            .iter()
            .any(|keyword| unwrappable_union(object, keyword))
}

/// Whether `keyword` holds a union that reduces to one declared variant, with
/// nothing beside it that the merge would have to discard.
fn unwrappable_union(object: &Map<String, Value>, keyword: &str) -> bool {
    sole_declared_variant(object.get(keyword)).is_some()
        && !object.keys().any(|key| key != keyword && !annotates(key))
}

/// Whether `type` is an array of type names that reduces to a single name.
fn collapsible_type_array(object: &Map<String, Value>) -> bool {
    let Some(declared) = object.get("type").and_then(Value::as_array) else {
        return false;
    };
    // A `type` array holds type names, and nothing else belongs in one — a
    // payload that happens to carry a `type` key must be left alone.
    if !declared
        .iter()
        .all(|value| value.is_string() || value.is_null())
    {
        return false;
    }
    let mut named = declared.iter().filter(|value| !names_null_type(value));
    // More than one name is a genuine union; none at all is `null` alone,
    // which says what it means already.
    named.next().is_some() && named.next().is_none()
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
fn unwrap_nullable_union(object: &mut Map<String, Value>) -> bool {
    let mut unwrapped = false;
    for keyword in ["anyOf", "oneOf"] {
        if !unwrappable_union(object, keyword) {
            continue;
        }
        let Some(variant) = sole_declared_variant(object.get(keyword)).cloned() else {
            continue;
        };
        object.remove(keyword);
        for (key, value) in variant {
            object.entry(key).or_insert(value);
        }
        unwrapped = true;
    }
    unwrapped
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
                // Says which of a union's variants applies to a payload. A
                // union that reduces to one declared variant has nothing left
                // to choose between, so the mapping describes the shape being
                // promoted rather than constraining it further.
                | "discriminator"
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
        Some(Value::Array(declared)) => {
            !declared.is_empty() && declared.iter().all(names_null_type)
        }
        Some(declared) => names_null_type(declared),
        None => false,
    }
}

/// Whether a `type` member names the null type, in either spelling.
///
/// JSON Schema's `type` holds strings, so an unquoted `null` is authored by
/// mistake — but it is a mistake YAML invites and JSON does not. `type: [array,
/// null]` is what the correct thing looks like in YAML, and a plain `null`
/// scalar resolves to the null value rather than to the string, so the member
/// reaches this pass as [`Value::Null`]; writing a bare `null` where a string
/// belongs in JSON is visibly wrong instead. Reading both costs nothing, since
/// a member that is literally null cannot be naming some other type.
fn names_null_type(value: &Value) -> bool {
    value.is_null() || value.as_str() == Some("null")
}

/// Rewrites a `type` array that names one type to that name, dropping any
/// `null` alongside it.
///
/// Scalars already survive the array spelling — `json_schema_scalar_type` skips
/// `null` members — so this exists for `array` and `object`, which are read
/// through [`serde_json::Value::as_str`] and would otherwise fall through to
/// `Json`.
///
/// A single-member `[T]` is collapsed too, even though it drops nothing. It
/// means exactly what `T` means, no reader here can see through the array, and
/// 3.0 forbids `type` arrays outright — so unlike the `anyOf: [{$ref: T}]`
/// spelling that GitHub's 3.0 document relies on, collapsing it cannot reach a
/// document that imports correctly today.
fn drop_null_from_type_array(object: &mut Map<String, Value>) -> bool {
    if !collapsible_type_array(object) {
        return false;
    }
    let Some(declared_type) = object
        .get("type")
        .and_then(Value::as_array)
        .and_then(|declared| declared.iter().find(|value| !names_null_type(value)))
        .cloned()
    else {
        return false;
    };
    object.insert("type".to_string(), declared_type);
    true
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::normalized_schema;

    /// The schema as a reader would see it. `None` means it was already
    /// readable, so these assertions read the same either way.
    fn normalized(value: serde_json::Value) -> serde_json::Value {
        normalized_schema(&value).unwrap_or(value)
    }

    /// [`reduces`] is what decides whether a schema is cloned, and it is the
    /// only place that can be asserted: `normalized_schema` answers `None` for
    /// everything here either way, because it clones first and reports the
    /// clone unchanged. The cost is invisible in the return value and real on
    /// every read.
    ///
    /// Worth guarding because the schemas that reduce to nothing are not rare:
    /// `anyOf: [{$ref: T}]` is how GitHub's 3.0 document spells a nullable ref
    /// throughout, and it is read on every walk of every operation.
    #[test]
    fn decides_that_nothing_reduces_before_cloning_anything() {
        for schema in [
            // A union that declares more than one shape.
            json!({"anyOf": [{"type": "string"}, {"type": "integer"}]}),
            // A single-variant union with no `null` member.
            json!({"anyOf": [{"$ref": "#/components/schemas/user"}]}),
            // Reducible, but sitting beside a shape the merge would discard.
            json!({
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "anyOf": [{"$ref": "#/components/schemas/user"}, {"type": "null"}],
            }),
            // A `type` array naming two real types.
            json!({"type": ["string", "integer"]}),
            // Not a schema at all: a payload that happens to carry `type`.
            json!({"type": ["a", 1]}),
            json!({"description": "no keyword either rewrite acts on"}),
        ] {
            let object = schema.as_object().expect("object");
            assert!(!super::reduces(object), "{schema}");
            assert_eq!(normalized_schema(&schema), None, "{schema}");
        }
    }

    /// The other half of the same contract: everything that does reduce is
    /// decided as reducible before the clone, so the predicate cannot drift
    /// into refusing work the rewrites would have done.
    #[test]
    fn decides_that_a_reducible_schema_reduces() {
        for schema in [
            json!({"anyOf": [{"type": "string"}, {"type": "null"}]}),
            json!({"oneOf": [{"$ref": "#/components/schemas/user"}, {"type": "null"}]}),
            json!({"type": ["array", "null"]}),
            json!({"type": ["string"]}),
            // Reducible beside annotations, which the merge keeps.
            json!({
                "description": "the user, or nothing",
                "anyOf": [{"$ref": "#/components/schemas/user"}, {"type": "null"}],
            }),
        ] {
            let object = schema.as_object().expect("object");
            assert!(super::reduces(object), "{schema}");
            assert!(normalized_schema(&schema).is_some(), "{schema}");
        }
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
        // `- type: null` in YAML, where the plain scalar resolves to the null
        // value rather than to the string.
        assert_eq!(
            normalized(json!({"anyOf": [{"type": "integer"}, {"type": null}]})),
            json!({"type": "integer"})
        );
        assert_eq!(
            normalized(json!({"oneOf": [{"type": "object"}, {"type": [null]}]})),
            json!({"type": "object"})
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
        // A discriminator beside a union that reduces to one variant has
        // nothing left to choose between, so it does not block the unwrap.
        assert_eq!(
            normalized(json!({
                "discriminator": {"propertyName": "kind"},
                "oneOf": [{"$ref": "#/components/schemas/user"}, {"type": "null"}],
            })),
            json!({
                "$ref": "#/components/schemas/user",
                "discriminator": {"propertyName": "kind"},
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
        // `type: [array, null]` in YAML, where the unquoted `null` never
        // reaches this pass as a string.
        assert_eq!(
            normalized(json!({"type": ["array", null], "items": {"type": "string"}})),
            json!({"type": "array", "items": {"type": "string"}})
        );
    }

    #[test]
    fn collapses_type_arrays_that_name_one_type() {
        // Nothing is dropped here — the array spelling itself is what no
        // reader can see through, and only 3.1 can produce it.
        assert_eq!(
            normalized(json!({"type": ["array"], "items": {"type": "string"}})),
            json!({"type": "array", "items": {"type": "string"}})
        );
        assert_eq!(
            normalized(json!({"type": ["string"]})),
            json!({"type": "string"})
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
            // Nothing but `null` is left to name.
            json!({"type": ["null"]}),
            json!({"type": [null]}),
            json!({"type": "string", "nullable": true}),
        ] {
            assert_eq!(normalized(schema.clone()), schema);
        }
    }

    #[test]
    fn normalizes_the_schema_it_is_given_and_leaves_its_children_to_their_own_reads() {
        // A child is a schema in its own right and is normalized when a reader
        // resolves it, so rewriting it here would be doing it twice — and doing
        // it to whatever else happened to be nested, which is how the
        // document-wide pass this replaced reached `example` payloads.
        assert_eq!(
            normalized(json!({
                "type": ["object", "null"],
                "properties": {"id": {"anyOf": [{"type": "string"}, {"type": "null"}]}},
            })),
            json!({
                "type": "object",
                "properties": {"id": {"anyOf": [{"type": "string"}, {"type": "null"}]}},
            })
        );
    }

    #[test]
    fn unwraps_a_union_whose_variant_is_itself_a_union() {
        // The promoted variant carries its own union, which the document-wide
        // pass had already collapsed on the way down. One schema at a time, the
        // rewrites are re-applied until the schema settles.
        assert_eq!(
            normalized(json!({
                "anyOf": [
                    {"anyOf": [{"type": ["string", "null"]}, {"type": "null"}]},
                    {"type": "null"},
                ],
            })),
            json!({"type": "string"})
        );
    }

    #[test]
    fn leaves_property_names_that_collide_with_keywords_alone() {
        // A `properties` map holds schemas, not keywords, and is never the
        // schema being read — so a property named `type` or `anyOf` reaches
        // these rewrites only as the schema it is.
        let schema = json!({
            "type": "object",
            "properties": {
                "type": {"type": ["string", "null"]},
                "anyOf": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
            },
        });
        assert_eq!(normalized(schema.clone()), schema);
        assert_eq!(
            normalized(json!({"type": ["string", "null"]})),
            json!({"type": "string"})
        );
    }

    #[test]
    fn leaves_payloads_that_are_not_schemas_alone() {
        // Reading one schema at a time is what keeps these safe: an `example`
        // or `default` payload is never resolved as a schema, so nothing here
        // is ever applied to it — not even when it looks exactly like one.
        let schema = json!({
            "type": "object",
            "properties": {"kind": {"type": "string"}},
            "example": {"type": ["admin", null]},
            "default": {"type": ["a"]},
        });
        assert_eq!(normalized(schema.clone()), schema);
    }
}
