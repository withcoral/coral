//! Heuristic inference of wrapped-list row paths for `OpenAPI` and MCP surfaces.
//!
//! A "wrapped list" response wraps rows inside an envelope: the declared type is
//! an object, but the rows live at a nested path such as `items` or
//! `results.data`. Providers do not declare which property holds the rows, so
//! Coral guesses, and a wrong guess reshapes a whole relation: a resource object
//! with a `fields: [...]` child would become a list of those fields.
//!
//! The guess is therefore an opinion, not a fact. It is recorded in operation
//! metadata, where it is visible and overridable, and never in the semantic IR.
//!
//! Inference excludes aggressively. Picking a candidate array is not enough; the
//! response or the operation must also carry evidence that it really is a page
//! envelope (see [`has_envelope_evidence`]). A missed envelope leaves a JSON
//! column the user can still unnest; a wrong envelope silently discards the
//! declared resource.
//!
//! `allOf` is folded before any of this runs, because providers routinely
//! assemble an envelope from a shared pagination base and a rows branch rather
//! than declaring it whole — every `OData` collection response is written that
//! way. `anyOf`, `oneOf`, and `not` are still refused: they describe a choice
//! between shapes, and there is no single property map to ask questions of.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::v4::surfaces::json_schema::{
    JsonSchemaWalkError, json_schema_type_contains, merged_all_of_object_view,
    schema_uses_alternation, with_resolved_json_schema,
};

const MAX_DEPTH: usize = 8;

/// Property names that conventionally hold the rows of a page envelope, in
/// preference order.
const PREFERRED_ROW_PROPERTIES: &[&str] = &["items", "data", "results", "rows"];

/// Envelope metadata names, matched together with the declared JSON type so a
/// resource field only counts as evidence when name and type agree.
// `links` is deliberately absent: a HAL-style `_links` object is at least as
// common on a singleton resource as on a page envelope.
const METADATA_OBJECT_NAMES: &[&str] = &[
    "meta",
    "metadata",
    "paging",
    "pagination",
    "pageinfo",
    "cursor",
    "cursors",
];
const METADATA_STRING_NAMES: &[&str] = &[
    "cursor",
    "next",
    "nextcursor",
    "nextpage",
    "nextpagetoken",
    "nexttoken",
    "nexturl",
    "nextlink",
    // OData spells its annotations `@odata.nextLink`, which normalizes with the
    // prefix intact. Listed explicitly rather than stripping `@odata.` in
    // `normalized_name`, because that function also decides which arrays are
    // *excluded* as metadata — stripping there would silently reclassify every
    // `@odata.*` property in every source.
    "odatanextlink",
    "odatadeltalink",
    "endcursor",
    "continuationtoken",
    "scrollid",
];
const METADATA_INTEGER_NAMES: &[&str] = &[
    "count",
    "total",
    "totalcount",
    "totalresults",
    "totalsize",
    "totalitems",
    "resultcount",
    "odatacount",
    "page",
    "pages",
    "pagecount",
    "perpage",
    "pagesize",
];
const METADATA_BOOLEAN_NAMES: &[&str] = &[
    "hasmore",
    "hasnext",
    "hasnextpage",
    "hasprevious",
    "incompleteresults",
    "moreresults",
    "truncated",
];

/// Envelope metadata names paired with the JSON type they must declare.
const METADATA_LEXICON: [(&[&str], &str); 4] = [
    (METADATA_OBJECT_NAMES, "object"),
    (METADATA_STRING_NAMES, "string"),
    (METADATA_INTEGER_NAMES, "integer"),
    (METADATA_BOOLEAN_NAMES, "boolean"),
];

/// Inputs available to wrapped-list inference.
///
/// The operation name is deliberately part of the context even though the
/// current policy never reads it. Naming signals (plural nouns, `list`/`search`
/// prefixes) can be added here without changing every surface importer.
#[derive(Clone, Copy)]
pub(in crate::v4) struct WrappedListInferenceContext<'a> {
    pub(in crate::v4) operation_name: &'a str,
    /// Whether the surface's own pagination detection found a contract on this
    /// operation. Each surface answers this before inferring a row path, so
    /// that the vocabulary of pagination parameter names lives with the
    /// detectors that own it rather than being predicted here.
    pub(in crate::v4) paginated_operation: bool,
    /// Document the response schema was taken from, used to resolve `$ref`.
    pub(in crate::v4) schema_root: &'a Value,
    pub(in crate::v4) response_schema: &'a Value,
}

/// Returns the path from the response root to the property holding the rows, or
/// an empty path when the response is not recognized as a wrapped list.
pub(in crate::v4) fn infer_wrapped_list_row_path(
    context: WrappedListInferenceContext<'_>,
) -> Vec<String> {
    let _ = context.operation_name;
    candidate_row_path(
        context.schema_root,
        context.response_schema,
        context.paginated_operation,
        false,
        &mut BTreeSet::new(),
        0,
    )
    .ok()
    .flatten()
    .unwrap_or_default()
}

/// Walks the envelope, returning the first candidate path that also carries
/// envelope evidence.
///
/// `inherited_evidence` records that an ancestor level looked like an envelope.
/// It admits a conventionally named row property one level down — the
/// `{success, results: {data: [...]}, pagination}` shape — but deliberately not
/// the sole-array fallback, which would let a nested resource's only array child
/// inherit its parent's envelope evidence.
fn candidate_row_path<'a>(
    root: &'a Value,
    schema: &'a Value,
    paginated_operation: bool,
    inherited_evidence: bool,
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
            // `allOf` is folded in, so an envelope assembled from a shared
            // pagination base and a rows branch reads as one object. `anyOf`,
            // `oneOf`, and `not` still bail: they describe a choice of shapes,
            // not one shape.
            let Some(view) =
                merged_all_of_object_view(root, resolved, resolving_refs, next_depth, MAX_DEPTH)
            else {
                return Ok(None);
            };
            let properties = &view.properties;
            if properties.is_empty() {
                return Ok(None);
            }
            let evidence = has_envelope_evidence(root, properties, resolving_refs, next_depth);
            let accept_preferred = paginated_operation || inherited_evidence || evidence;
            let accept_fallback = paginated_operation || evidence;

            for name in PREFERRED_ROW_PROPERTIES {
                if let Some(property) = properties.get(*name)
                    && schema_has_type(root, property, resolving_refs, next_depth, "array")
                {
                    return Ok(accept_preferred.then(|| vec![(*name).to_string()]));
                }
            }

            // Recurse only through the same preferred names: nested envelopes
            // such as `results.data` are worth finding, an unrestricted walk of
            // every resource child is not.
            for name in PREFERRED_ROW_PROPERTIES {
                // A branch that cannot be walked — unresolvable, cyclic, too
                // deep — holds no rows, which is not a reason to abandon the
                // other candidate names. `schema_has_type` already treats the
                // same failures this way.
                if let Some(property) = properties.get(*name)
                    && let Some(mut path) = candidate_row_path(
                        root,
                        property,
                        paginated_operation,
                        inherited_evidence || evidence,
                        resolving_refs,
                        next_depth,
                    )
                    .ok()
                    .flatten()
                {
                    path.insert(0, (*name).to_string());
                    return Ok(Some(path));
                }
            }

            let mut arrays = Vec::new();
            for (name, property) in properties {
                // An array named like envelope metadata is a link or token
                // collection, never the rows the envelope is wrapping.
                if is_metadata_name(name) {
                    continue;
                }
                if schema_has_type(root, property, resolving_refs, next_depth, "array") {
                    arrays.push(name);
                }
            }
            match arrays.as_slice() {
                [name] => Ok(accept_fallback.then(|| vec![(*name).clone()])),
                [] | [_, _, ..] => Ok(None),
            }
        },
    )
}

/// Reports whether the properties of an envelope level read as list metadata.
///
/// A single agreeing name-and-type pair is enough: providers rarely put a
/// `has_more` boolean or a `next_cursor` string on a plain resource.
fn has_envelope_evidence(
    root: &Value,
    properties: &BTreeMap<String, Value>,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
) -> bool {
    // A sole property is the envelope itself: `{"data": [...]}` has nothing
    // else it could be.
    if properties.len() == 1 {
        return true;
    }
    properties.iter().any(|(name, property)| {
        let normalized = normalized_name(name);
        METADATA_LEXICON.iter().any(|(names, expected)| {
            names.contains(&normalized.as_str())
                && schema_has_type(root, property, resolving_refs, depth, expected)
        })
    })
}

fn is_metadata_name(name: &str) -> bool {
    let normalized = normalized_name(name);
    METADATA_LEXICON
        .iter()
        .any(|(names, _)| names.contains(&normalized.as_str()))
}

/// An unresolvable or cyclic property simply does not declare the type asked
/// about; it must not abandon inference for its siblings.
///
/// `allOf` branches count, so a property assembled from a base and an extension
/// still declares its type. An alternation does not: a property that is one of
/// several shapes has no single type to check against.
fn schema_has_type(
    root: &Value,
    schema: &Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    expected: &str,
) -> bool {
    with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        MAX_DEPTH,
        |resolved, resolving_refs, next_depth| {
            if schema_uses_alternation(resolved) {
                return Ok(false);
            }
            if json_schema_type_contains(resolved, expected) {
                return Ok(true);
            }
            if resolved.get("allOf").is_none() {
                return Ok(false);
            }
            Ok(
                merged_all_of_object_view(root, resolved, resolving_refs, next_depth, MAX_DEPTH)
                    .is_some_and(|view| view.declares_type(expected)),
            )
        },
    )
    .unwrap_or(false)
}

/// Collapses case and separators so `perPage`, `per_page`, and `per-page` all
/// match the lexicon entry `perpage`.
fn normalized_name(name: &str) -> String {
    name.chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{WrappedListInferenceContext, infer_wrapped_list_row_path};

    fn row_path(schema: &Value, paginated_operation: bool) -> Vec<String> {
        infer_wrapped_list_row_path(WrappedListInferenceContext {
            operation_name: "list_things",
            paginated_operation,
            schema_root: schema,
            response_schema: schema,
        })
    }

    #[test]
    fn selects_preferred_property_when_a_metadata_sibling_agrees() {
        let schema = json!({
            "type": "object",
            "properties": {
                "total_count": {"type": "integer"},
                "incomplete_results": {"type": "boolean"},
                "items": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert_eq!(row_path(&schema, false), ["items"]);
    }

    #[test]
    fn selects_nested_preferred_property_through_wrapper_objects() {
        let schema = json!({
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "results": {
                    "type": "object",
                    "properties": {
                        "data": {"type": "array", "items": {"type": "object"}},
                    },
                },
                "pagination": {"type": "object"},
            },
        });

        assert_eq!(row_path(&schema, false), ["results", "data"]);
    }

    #[test]
    fn selects_sole_array_property_when_a_metadata_sibling_agrees() {
        let schema = json!({
            "type": "object",
            "properties": {
                "total_count": {"type": "integer"},
                "repositories": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert_eq!(row_path(&schema, false), ["repositories"]);
    }

    #[test]
    fn selects_the_only_property_of_a_pure_envelope() {
        let schema = json!({
            "type": "object",
            "properties": {
                "data": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert_eq!(row_path(&schema, false), ["data"]);
    }

    #[test]
    fn selects_candidate_of_a_paginated_operation_without_metadata_siblings() {
        let schema = json!({
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "data": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert!(row_path(&schema, false).is_empty());
        assert_eq!(row_path(&schema, true), ["data"]);
    }

    #[test]
    fn ignores_resource_objects_with_named_array_children() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "items": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert!(
            row_path(&schema, false).is_empty(),
            "an unpaginated resource is not an envelope"
        );
    }

    #[test]
    fn ignores_resource_objects_with_sole_array_children() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": {"type": "number"},
                "content_type": {"type": "string"},
                "item_url": {"type": "string"},
                "fields": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert!(row_path(&schema, false).is_empty());
    }

    #[test]
    fn ignores_metadata_names_whose_declared_type_disagrees() {
        // A resource may carry a `count` string; only an integer `count` reads
        // as a result total.
        let string_count = json!({
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "count": {"type": "string"},
                "steps": {"type": "array", "items": {"type": "object"}},
            },
        });
        let integer_count = json!({
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "count": {"type": "integer"},
                "steps": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert!(row_path(&string_count, false).is_empty());
        assert_eq!(row_path(&integer_count, false), ["steps"]);
    }

    #[test]
    fn ignores_hal_style_link_objects_on_resources() {
        // GitHub's `activity/get-feeds` shape: a resource whose only array is
        // incidental and whose `_links` object describes relations, not pages.
        let schema = json!({
            "type": "object",
            "properties": {
                "timeline_url": {"type": "string"},
                "user_url": {"type": "string"},
                "current_user_organization_urls": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "_links": {"type": "object"},
            },
        });

        assert!(row_path(&schema, false).is_empty());
    }

    #[test]
    fn ignores_metadata_named_arrays_as_row_candidates() {
        let schema = json!({
            "type": "object",
            "properties": {
                "has_more": {"type": "boolean"},
                "cursors": {"type": "array", "items": {"type": "string"}},
            },
        });

        assert!(row_path(&schema, false).is_empty());
    }

    #[test]
    fn does_not_let_a_nested_resource_inherit_envelope_evidence() {
        let schema = json!({
            "type": "object",
            "properties": {
                "has_more": {"type": "boolean"},
                "data": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                    },
                },
            },
        });

        assert!(row_path(&schema, false).is_empty());
    }

    #[test]
    fn ignores_responses_without_arrays() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "nextCursor": {"type": "string"},
            },
        });

        assert!(
            row_path(&schema, true).is_empty(),
            "a singleton with a cursor field has no row collection"
        );
    }

    #[test]
    fn ignores_multiple_array_properties_without_a_preferred_name() {
        let schema = json!({
            "type": "object",
            "properties": {
                "total_count": {"type": "integer"},
                "issues": {"type": "array", "items": {"type": "object"}},
                "pull_requests": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert!(row_path(&schema, false).is_empty());
    }

    #[test]
    fn merges_all_of_branches_into_one_envelope() {
        let schema = json!({
            "allOf": [{
                "type": "object",
                "properties": {
                    "total_count": {"type": "integer"},
                    "items": {"type": "array", "items": {"type": "object"}},
                },
            }],
        });

        assert_eq!(row_path(&schema, false), ["items"]);
    }

    #[test]
    fn ignores_alternation_schemas() {
        // `anyOf`/`oneOf`/`not` describe a choice of shapes, so there is no one
        // property map to read as an envelope.
        for keyword in ["anyOf", "oneOf"] {
            let schema = json!({
                keyword: [{
                    "type": "object",
                    "properties": {
                        "total_count": {"type": "integer"},
                        "items": {"type": "array", "items": {"type": "object"}},
                    },
                }],
            });

            assert!(
                row_path(&schema, false).is_empty(),
                "{keyword} must not be read as an envelope"
            );
        }

        let negated = json!({
            "type": "object",
            "not": {"type": "string"},
            "properties": {
                "total_count": {"type": "integer"},
                "items": {"type": "array", "items": {"type": "object"}},
            },
        });
        assert!(row_path(&negated, false).is_empty());
    }

    /// The Microsoft Graph shape, and every other `OData` collection response:
    /// a shared pagination base merged with a branch declaring the rows, with
    /// the annotations that make it recognizable spelled `@odata.*`.
    #[test]
    fn selects_rows_through_an_odata_collection_envelope() {
        let schema = json!({
            "$ref": "#/components/schemas/ChatCollectionResponse",
            "components": {
                "schemas": {
                    "BaseCollectionPaginationCountResponse": {
                        "type": "object",
                        "properties": {
                            "@odata.count": {"type": "integer", "format": "int64"},
                            "@odata.nextLink": {"type": "string"},
                        },
                    },
                    "ChatCollectionResponse": {
                        "title": "Collection of chat",
                        "type": "object",
                        "allOf": [
                            {"$ref": "#/components/schemas/BaseCollectionPaginationCountResponse"},
                            {
                                "type": "object",
                                "properties": {
                                    "value": {
                                        "type": "array",
                                        "items": {"type": "object"},
                                    },
                                },
                            },
                        ],
                    },
                },
            },
        });

        assert_eq!(
            row_path(&schema, false),
            ["value"],
            "the OData annotations are the envelope evidence; no pagination contract is needed"
        );
    }

    #[test]
    fn odata_annotations_count_as_envelope_evidence_without_composition() {
        let schema = json!({
            "type": "object",
            "properties": {
                "@odata.count": {"type": "integer"},
                "@odata.nextLink": {"type": "string"},
                "value": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert_eq!(row_path(&schema, false), ["value"]);
    }

    #[test]
    fn ignores_an_all_of_branch_that_cycles() {
        let schema = json!({
            "$ref": "#/components/schemas/Node",
            "components": {
                "schemas": {
                    "Node": {
                        "type": "object",
                        "allOf": [{"$ref": "#/components/schemas/Node"}],
                    },
                },
            },
        });

        assert!(row_path(&schema, false).is_empty());
    }

    #[test]
    fn ignores_all_of_branches_nested_past_the_depth_cap() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "has_more": {"type": "boolean"},
                "data": {"type": "array", "items": {"type": "object"}},
            },
        });
        for _ in 0..super::MAX_DEPTH {
            schema = json!({"type": "object", "allOf": [schema]});
        }

        assert!(row_path(&schema, false).is_empty());
    }

    #[test]
    fn resolves_local_refs() {
        let schema = json!({
            "$ref": "#/components/schemas/Page",
            "components": {
                "schemas": {
                    "Page": {
                        "type": "object",
                        "properties": {
                            "has_more": {"type": "boolean"},
                            "data": {"$ref": "#/components/schemas/Rows"},
                        },
                    },
                    "Rows": {"type": "array", "items": {"type": "object"}},
                },
            },
        });

        assert_eq!(row_path(&schema, false), ["data"]);
    }

    #[test]
    fn ignores_ref_cycles() {
        let schema = json!({
            "$ref": "#/components/schemas/Node",
            "components": {
                "schemas": {
                    "Node": {"$ref": "#/components/schemas/Node"},
                },
            },
        });

        assert!(row_path(&schema, false).is_empty());
    }

    #[test]
    fn ignores_envelopes_nested_past_the_depth_cap() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "has_more": {"type": "boolean"},
                "data": {"type": "array", "items": {"type": "object"}},
            },
        });
        for _ in 0..super::MAX_DEPTH {
            schema = json!({
                "type": "object",
                "properties": {"results": schema},
            });
        }

        assert!(row_path(&schema, false).is_empty());
    }
}
