//! Ref-aware discovery of the response property that carries the next page —
//! either a continuation token or a whole next-page URL.
//!
//! Three callers ask the same question of a response schema: which property
//! carries the next page? They ask it of different vocabularies — `OpenAPI`
//! accepts a wider set of cursor names than MCP, and body next-URL detection
//! has its own narrower lexicon — so the lexicon is a parameter, but the walk
//! itself is shared.
//!
//! What each caller does with the answer differs enough to matter here. Cursor
//! detection still has to find a request parameter to put the token in before
//! it commits to a contract, so a false positive costs nothing. Body next-URL
//! detection has no such second signal: the name is the whole case, and a
//! property that turns out not to hold a string means the query silently
//! returns its first page. That is what [`StringTypeRequirement`] exists for —
//! do not relax it back to one shared rule.
//!
//! The walk resolves `$ref` because the responses it now runs against are page
//! envelopes, and an envelope's `meta`/`pageInfo` sibling is routinely a
//! reference to a shared component. Wrapped-list inference already resolves
//! refs when it decides a response *is* an envelope (see
//! [`crate::v4::wrapped_lists`]); if cursor discovery did not, the two halves of
//! one decision would disagree, and an operation would be presented as a
//! paginated table that silently stops after its first page.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::v4::surfaces::json_schema::{
    JsonSchemaWalkError, json_schema_type_contains, merged_all_of_object_view,
    with_resolved_json_schema,
};

const MAX_DEPTH: usize = 8;

/// Returns the path from the response root to the property holding the
/// next page, or `None` when no property matches `name_tokens`.
///
/// Names are matched after collapsing case and separators, so `nextCursor`,
/// `next_cursor`, and `next-cursor` all match the token `nextcursor`.
pub(in crate::v4) fn find_response_cursor_path(
    root: &Value,
    schema: &Value,
    name_tokens: &[&str],
    string_type: StringTypeRequirement,
) -> Option<Vec<String>> {
    cursor_path(
        root,
        schema,
        name_tokens,
        string_type,
        &mut BTreeSet::new(),
        0,
    )
    .ok()
    .flatten()
}

/// How much a name match has to be corroborated by the declared type.
///
/// The walk matches on property *names*, which is a guess. This says how much
/// the schema has to agree before the guess is accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::v4) enum StringTypeRequirement {
    /// Accept a property that declares no type. For cursor detection, where a
    /// matching request parameter still has to be found before anything is
    /// committed, and descriptors routinely leave envelope metadata untyped.
    Untyped,
    /// Require a declared `string`, or a union of `string` and `null` and
    /// nothing else. For body next-URL detection, which has no second signal:
    /// a property that is not a string reads back as `None` at runtime,
    /// `advance_pagination_state` stops, and the query returns page one with no
    /// error and no diagnostic. A union that merely *permits* a string is not
    /// enough — the response can still carry the other variant.
    Declared,
}

/// Prefers a cursor on the level being walked before descending, so a nested
/// `meta.next_cursor` never wins over one the envelope declares itself.
fn cursor_path<'a>(
    root: &'a Value,
    schema: &'a Value,
    name_tokens: &[&str],
    string_type: StringTypeRequirement,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
) -> Result<Option<Vec<String>>, JsonSchemaWalkError<'a>> {
    with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        MAX_DEPTH,
        |resolved, resolving_refs, next_depth| {
            // Folding `allOf` keeps this walk agreeing with wrapped-list
            // inference: an envelope assembled from a shared pagination base
            // declares its cursor on a branch, not on the schema itself. If one
            // half of the decision could see through composition and the other
            // could not, an operation would be presented as a paginated table
            // that silently stops after its first page.
            let Some(view) =
                merged_all_of_object_view(root, resolved, resolving_refs, next_depth, MAX_DEPTH)
            else {
                return Ok(None);
            };
            // Tokens outside, so a response declaring two of them picks by
            // `name_tokens` order rather than by property name.
            for token in name_tokens {
                for (name, property) in &view.properties {
                    if normalized_name(name) == *token
                        && property_holds_string(
                            root,
                            property,
                            string_type,
                            resolving_refs,
                            next_depth,
                        )
                    {
                        return Ok(Some(vec![name.clone()]));
                    }
                }
            }
            for (name, property) in &view.properties {
                if !property_is_object(root, property, resolving_refs, next_depth) {
                    continue;
                }
                // A branch that cannot be walked — unresolvable, cyclic, too
                // deep — declares no cursor, which is not a reason to abandon
                // its siblings. This matches how `property_holds_string` and
                // `property_is_object` already treat the same failures.
                if let Some(mut path) = cursor_path(
                    root,
                    property,
                    name_tokens,
                    string_type,
                    resolving_refs,
                    next_depth,
                )
                .ok()
                .flatten()
                {
                    path.insert(0, name.clone());
                    return Ok(Some(path));
                }
            }
            Ok(None)
        },
    )
}

/// Whether a conventionally named property carries a string, to the standard
/// `string_type` asks for.
///
/// An unresolvable or cyclic reference declares nothing. Under
/// [`StringTypeRequirement::Untyped`] that cannot rule a token out, so the
/// property is kept; under [`StringTypeRequirement::Declared`] it is precisely
/// the absent declaration the caller insists on, so it is rejected.
fn property_holds_string(
    root: &Value,
    schema: &Value,
    string_type: StringTypeRequirement,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
) -> bool {
    with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        MAX_DEPTH,
        |resolved, _, _| {
            Ok(match string_type {
                StringTypeRequirement::Declared => declares_only_string(resolved),
                StringTypeRequirement::Untyped => {
                    json_schema_type_contains(resolved, "string") || resolved.get("type").is_none()
                }
            })
        },
    )
    .unwrap_or(string_type == StringTypeRequirement::Untyped)
}

/// Whether every type the schema permits is one the runtime can read as a URL.
///
/// `json_schema_type_contains` would be too weak here: it asks whether `string`
/// is *among* the permitted types, so a union like `["string", "integer"]`
/// passes. That union is exactly the shape the strict requirement exists to
/// reject — a response carrying the integer variant reads back as `None` and
/// stops pagination after page one, which is the silent failure, not a louder
/// version of it.
///
/// `null` is the one companion allowed, because that is how a descriptor says
/// "absent on the last page" — Graph's `@odata.nextLink` is declared
/// `{type: string, nullable: true}` — and the runtime already treats an absent
/// or empty link as the end of the collection.
fn declares_only_string(schema: &Value) -> bool {
    match schema.get("type") {
        Some(Value::String(value)) => value == "string",
        Some(Value::Array(values)) => {
            // Every member must be one of the two readable names, and a member
            // that is not even a string counts against that. Filtering
            // non-strings out first would discard the evidence: `["string", 1]`
            // is not a JSON Schema type union, and reading it as one lets a
            // malformed descriptor buy the mode. `json_schema_type_contains`
            // still has to confirm one member *is* `string`, because `all` is
            // vacuously true on `[]` and `["null"]` alone declares no URL.
            let all_readable = values
                .iter()
                .all(|value| matches!(value.as_str(), Some("string" | "null")));
            all_readable && json_schema_type_contains(schema, "string")
        }
        _ => false,
    }
}

/// Descent is the opposite case: a property that cannot be resolved into an
/// object is not one to search.
fn property_is_object(
    root: &Value,
    schema: &Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
) -> bool {
    with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        MAX_DEPTH,
        |resolved, resolving_refs, next_depth| {
            if json_schema_type_contains(resolved, "object") {
                return Ok(true);
            }
            // Without `allOf` the merge has only this schema to collect, so it
            // could only re-derive the answer above. Skipping it matters here
            // because this runs for every property on every walked level, and
            // building the view clones the properties of each one.
            if resolved.get("allOf").is_none() {
                return Ok(false);
            }
            Ok(
                merged_all_of_object_view(root, resolved, resolving_refs, next_depth, MAX_DEPTH)
                    .is_some_and(|view| view.declares_type("object")),
            )
        },
    )
    .unwrap_or(false)
}

fn normalized_name(name: &str) -> String {
    name.chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{StringTypeRequirement, find_response_cursor_path};

    const TOKENS: &[&str] = &["nextcursor", "nextpagetoken", "nexttoken", "endcursor"];

    /// The cursor-detection standard: a name match is enough unless the schema
    /// declares a type that rules it out.
    fn find_untyped(root: &Value, schema: &Value, tokens: &[&str]) -> Option<Vec<String>> {
        find_response_cursor_path(root, schema, tokens, StringTypeRequirement::Untyped)
    }

    /// The body-next-URL standard: the schema has to say `string`.
    fn find_declared(root: &Value, schema: &Value, tokens: &[&str]) -> Option<Vec<String>> {
        find_response_cursor_path(root, schema, tokens, StringTypeRequirement::Declared)
    }

    #[test]
    fn finds_a_cursor_declared_on_the_response_root() {
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {"type": "array", "items": {"type": "object"}},
                "next_cursor": {"type": "string"},
            },
        });

        assert_eq!(
            find_untyped(&schema, &schema, TOKENS),
            Some(vec!["next_cursor".to_string()])
        );
    }

    #[test]
    fn finds_a_cursor_inside_a_referenced_metadata_object() {
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {"type": "array", "items": {"type": "object"}},
                "meta": {"$ref": "#/$defs/PageMeta"},
            },
            "$defs": {
                "PageMeta": {
                    "type": "object",
                    "properties": {"nextCursor": {"type": ["string", "null"]}},
                },
            },
        });

        assert_eq!(
            find_untyped(&schema, &schema, TOKENS),
            Some(vec!["meta".to_string(), "nextCursor".to_string()])
        );
    }

    #[test]
    fn finds_a_cursor_that_is_itself_a_reference() {
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {"type": "array", "items": {"type": "object"}},
                "next_token": {"$ref": "#/$defs/Token"},
            },
            "$defs": {"Token": {"type": "string"}},
        });

        assert_eq!(
            find_untyped(&schema, &schema, TOKENS),
            Some(vec!["next_token".to_string()])
        );
    }

    #[test]
    fn resolves_a_referenced_response_root() {
        let root = json!({
            "$defs": {
                "Page": {
                    "type": "object",
                    "properties": {"end_cursor": {"type": "string"}},
                },
            },
        });

        assert_eq!(
            find_untyped(&root, &json!({"$ref": "#/$defs/Page"}), TOKENS),
            Some(vec!["end_cursor".to_string()])
        );
    }

    #[test]
    fn rejects_a_conventional_name_whose_referenced_type_is_not_a_string() {
        let schema = json!({
            "type": "object",
            "properties": {"next_cursor": {"$ref": "#/$defs/Count"}},
            "$defs": {"Count": {"type": "integer"}},
        });

        assert_eq!(find_untyped(&schema, &schema, TOKENS), None);
    }

    #[test]
    fn the_two_standards_disagree_only_about_an_undeclared_type() {
        let untyped = json!({
            "type": "object",
            "properties": {"next_cursor": {}},
        });

        assert_eq!(
            find_untyped(&untyped, &untyped, TOKENS),
            Some(vec!["next_cursor".to_string()]),
            "cursor detection still needs a matching request parameter, so a \
             name match on an untyped property costs nothing"
        );
        assert_eq!(
            find_declared(&untyped, &untyped, TOKENS),
            None,
            "body next-URL detection commits on the name alone, and a property \
             that is not a string stops pagination silently after page one"
        );

        // A declared string satisfies both, which is why Graph keeps working:
        // `@odata.nextLink` is declared `{type: string, nullable: true}`.
        let declared = json!({
            "type": "object",
            "properties": {"next_cursor": {"type": ["string", "null"]}},
        });
        for found in [
            find_untyped(&declared, &declared, TOKENS),
            find_declared(&declared, &declared, TOKENS),
        ] {
            assert_eq!(found, Some(vec!["next_cursor".to_string()]));
        }
    }

    #[test]
    fn the_declared_standard_rejects_a_union_that_permits_a_non_string() {
        // Merely permitting a string is not enough: a response carrying the
        // other variant reads back as `None` and stops after page one, which
        // is the silent failure the strict standard exists to prevent.
        let union = json!({
            "type": "object",
            "properties": {"next_cursor": {"type": ["string", "integer"]}},
        });

        assert_eq!(
            find_untyped(&union, &union, TOKENS),
            Some(vec!["next_cursor".to_string()])
        );
        assert_eq!(find_declared(&union, &union, TOKENS), None);

        // `null` is the one companion allowed, because that is how a
        // descriptor says "absent on the last page", and the runtime already
        // treats an absent link as the end.
        let nullable = json!({
            "type": "object",
            "properties": {"next_cursor": {"type": ["string", "null"]}},
        });

        assert_eq!(
            find_declared(&nullable, &nullable, TOKENS),
            Some(vec!["next_cursor".to_string()])
        );

        // A union with no string at all fails both standards.
        let no_string = json!({
            "type": "object",
            "properties": {"next_cursor": {"type": ["integer", "null"]}},
        });

        assert_eq!(find_declared(&no_string, &no_string, TOKENS), None);
        assert_eq!(find_untyped(&no_string, &no_string, TOKENS), None);

        // A member that is not even a type name means the descriptor is
        // malformed, not that it declared a string. Skipping it would let
        // `["string", 1]` read as a clean string-only union and buy the mode.
        let malformed = json!({
            "type": "object",
            "properties": {"next_cursor": {"type": ["string", 1]}},
        });

        assert_eq!(find_declared(&malformed, &malformed, TOKENS), None);

        // `null` alone declares no URL, and an empty union declares nothing —
        // neither is vacuously acceptable.
        for values in [json!(["null"]), json!([])] {
            let schema = json!({
                "type": "object",
                "properties": {"next_cursor": {"type": values}},
            });

            assert_eq!(find_declared(&schema, &schema, TOKENS), None);
        }
    }

    #[test]
    fn a_reference_that_declares_nothing_fails_the_declared_standard() {
        // An unresolvable ref cannot rule a token out, but neither can it be
        // the declaration the strict caller insists on.
        let schema = json!({
            "type": "object",
            "properties": {"next_cursor": {"$ref": "#/$defs/Missing"}},
        });

        assert_eq!(
            find_untyped(&schema, &schema, TOKENS),
            Some(vec!["next_cursor".to_string()])
        );
        assert_eq!(find_declared(&schema, &schema, TOKENS), None);
    }

    /// Callers rank their lexicons — `odatanextlink` ahead of `nexthref`,
    /// `nextcursor` ahead of `endcursor` — so the rank has to beat the
    /// alphabetical order of the property names.
    #[test]
    fn prefers_the_earlier_token_over_the_earlier_property_name() {
        let schema = json!({
            "type": "object",
            "properties": {
                "end_cursor": {"type": "string"},
                "next_cursor": {"type": "string"},
            },
        });

        assert_eq!(
            find_untyped(&schema, &schema, &["nextcursor", "endcursor"]),
            Some(vec!["next_cursor".to_string()])
        );

        // Reversing the list reverses the answer: the ranking is the decision,
        // not the spelling.
        assert_eq!(
            find_untyped(&schema, &schema, &["endcursor", "nextcursor"]),
            Some(vec!["end_cursor".to_string()])
        );
    }

    #[test]
    fn a_self_referential_envelope_terminates() {
        let schema = json!({
            "type": "object",
            "properties": {"page": {"$ref": "#/$defs/Page"}},
            "$defs": {
                "Page": {
                    "type": "object",
                    "properties": {"page": {"$ref": "#/$defs/Page"}},
                },
            },
        });

        assert_eq!(find_untyped(&schema, &schema, TOKENS), None);
    }

    /// The cursor walk and wrapped-list inference must agree about composed
    /// envelopes. If only one of them could see through `allOf`, an operation
    /// would be presented as a paginated table that stops after page one.
    #[test]
    fn finds_a_cursor_through_an_all_of_envelope() {
        let schema = json!({
            "$ref": "#/components/schemas/Page",
            "components": {
                "schemas": {
                    "PaginationBase": {
                        "type": "object",
                        "properties": {"@odata.nextLink": {"type": "string"}},
                    },
                    "Page": {
                        "type": "object",
                        "allOf": [
                            {"$ref": "#/components/schemas/PaginationBase"},
                            {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "array", "items": {"type": "object"}},
                                },
                            },
                        ],
                    },
                },
            },
        });

        // The strict standard, because this is the body next-URL lexicon: an
        // `allOf` branch still has to declare the property a string, and
        // Graph's base does.
        assert_eq!(
            find_declared(&schema, &schema, &["odatanextlink"]),
            Some(vec!["@odata.nextLink".to_string()])
        );
    }
}
