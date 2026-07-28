//! Ref-aware discovery of the response property that carries a continuation
//! token.
//!
//! Both surfaces ask the same question of a response schema: which property
//! holds the cursor for the next page? They ask it of different vocabularies —
//! `OpenAPI` accepts a wider set of names than MCP — so the lexicon is a
//! parameter, but the walk itself is shared.
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
/// continuation token, or `None` when no property matches `cursor_tokens`.
///
/// Names are matched after collapsing case and separators, so `nextCursor`,
/// `next_cursor`, and `next-cursor` all match the token `nextcursor`.
pub(in crate::v4) fn find_response_cursor_path(
    root: &Value,
    schema: &Value,
    cursor_tokens: &[&str],
) -> Option<Vec<String>> {
    cursor_path(root, schema, cursor_tokens, &mut BTreeSet::new(), 0)
        .ok()
        .flatten()
}

/// Prefers a cursor on the level being walked before descending, so a nested
/// `meta.next_cursor` never wins over one the envelope declares itself.
fn cursor_path<'a>(
    root: &'a Value,
    schema: &'a Value,
    cursor_tokens: &[&str],
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
                if cursor_tokens.contains(&normalized_name(name).as_str())
                    && property_allows_string(root, property, resolving_refs, next_depth)
                {
                    return Ok(Some(vec![name.clone()]));
                }
            }
            for (name, property) in properties {
                if !property_is_object(root, property, resolving_refs, next_depth) {
                    continue;
                }
                if let Some(mut path) =
                    cursor_path(root, property, cursor_tokens, resolving_refs, next_depth)?
                {
                    path.insert(0, name.clone());
                    return Ok(Some(path));
                }
            }
            Ok(None)
        },
    )
}

/// A conventionally named property is accepted unless it declares a type that
/// rules a token out. An unresolvable or cyclic reference declares nothing, so
/// it cannot rule anything out either.
fn property_allows_string(
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
        |resolved, _, _| {
            Ok(json_schema_type_contains(resolved, "string") || resolved.get("type").is_none())
        },
    )
    .unwrap_or(true)
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
    use serde_json::json;

    use super::find_response_cursor_path;

    const TOKENS: &[&str] = &["nextcursor", "nextpagetoken", "nexttoken", "endcursor"];

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
            find_response_cursor_path(&schema, &schema, TOKENS),
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
            find_response_cursor_path(&schema, &schema, TOKENS),
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
            find_response_cursor_path(&schema, &schema, TOKENS),
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
            find_response_cursor_path(&root, &json!({"$ref": "#/$defs/Page"}), TOKENS),
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

        assert_eq!(find_response_cursor_path(&schema, &schema, TOKENS), None);
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

        assert_eq!(find_response_cursor_path(&schema, &schema, TOKENS), None);
    }
}
