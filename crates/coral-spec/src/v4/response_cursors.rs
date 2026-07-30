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
    JsonSchemaWalkError, json_schema_type_contains, with_resolved_json_schema,
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
    /// Require a declared `string`. For body next-URL detection, which has no
    /// second signal: a property that is not a string reads back as `None` at
    /// runtime, `advance_pagination_state` stops, and the query returns page
    /// one with no error and no diagnostic.
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
            let Some(properties) = resolved.get("properties").and_then(Value::as_object) else {
                return Ok(None);
            };
            for (name, property) in properties {
                if name_tokens.contains(&normalized_name(name).as_str())
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
            for (name, property) in properties {
                if !property_is_object(root, property, resolving_refs, next_depth) {
                    continue;
                }
                if let Some(mut path) = cursor_path(
                    root,
                    property,
                    name_tokens,
                    string_type,
                    resolving_refs,
                    next_depth,
                )? {
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
            let declares_string = json_schema_type_contains(resolved, "string");
            Ok(match string_type {
                StringTypeRequirement::Declared => declares_string,
                StringTypeRequirement::Untyped => declares_string || resolved.get("type").is_none(),
            })
        },
    )
    .unwrap_or(string_type == StringTypeRequirement::Untyped)
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
        |resolved, _, _| Ok(json_schema_type_contains(resolved, "object")),
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
}
