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

use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::v4::ir::{IrInputLocation, IrOperationInput};
use crate::v4::surfaces::json_schema::{
    JsonSchemaWalkError, json_schema_type_contains, with_resolved_json_schema,
};

const MAX_DEPTH: usize = 8;

/// Property names that conventionally hold the rows of a page envelope, in
/// preference order.
const PREFERRED_ROW_PROPERTIES: &[&str] = &["items", "data", "results", "rows"];

/// Envelope metadata names, matched together with the declared JSON type so a
/// resource field only counts as evidence when name and type agree.
const METADATA_OBJECT_NAMES: &[&str] = &[
    "meta",
    "metadata",
    "paging",
    "pagination",
    "pageinfo",
    "links",
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

/// Request-side names that mark an operation as paginated. This is a syntactic
/// check on declared inputs rather than a look at the inferred `PaginationSpec`,
/// because pagination inference consumes the row path this module produces.
const PAGINATION_INPUT_NAMES: &[&str] = &[
    "page",
    "pagenumber",
    "pagetoken",
    "pagesize",
    "perpage",
    "limit",
    "maxresults",
    "offset",
    "cursor",
    "startcursor",
    "continuationtoken",
    "after",
    "nexttoken",
    "nextcursor",
    "iterator",
];

/// Inputs available to wrapped-list inference.
///
/// The operation name is deliberately part of the context even though the
/// current policy never reads it. Naming signals (plural nouns, `list`/`search`
/// prefixes) can be added here without changing every surface importer.
#[derive(Clone, Copy)]
pub(in crate::v4) struct WrappedListInferenceContext<'a> {
    pub(in crate::v4) operation_name: &'a str,
    pub(in crate::v4) inputs: &'a [IrOperationInput],
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
    let paginated_operation = declares_pagination_input(context.inputs);
    candidate_row_path(
        context.schema_root,
        context.response_schema,
        paginated_operation,
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
            // Composed schemas describe a merge Coral does not model here, so
            // their properties cannot be read as a complete envelope.
            if schema_uses_composition(resolved) {
                return Ok(None);
            }
            if !json_schema_type_contains(resolved, "object") {
                return Ok(None);
            }
            let Some(properties) = resolved.get("properties").and_then(Value::as_object) else {
                return Ok(None);
            };
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
                if let Some(property) = properties.get(*name)
                    && let Some(mut path) = candidate_row_path(
                        root,
                        property,
                        paginated_operation,
                        inherited_evidence || evidence,
                        resolving_refs,
                        next_depth,
                    )?
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
    properties: &Map<String, Value>,
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

fn declares_pagination_input(inputs: &[IrOperationInput]) -> bool {
    inputs
        .iter()
        .filter(|input| {
            matches!(
                input.location,
                IrInputLocation::Query | IrInputLocation::ToolArg
            )
        })
        .any(|input| PAGINATION_INPUT_NAMES.contains(&normalized_name(&input.name).as_str()))
}

fn schema_uses_composition(schema: &Value) -> bool {
    ["allOf", "anyOf", "oneOf", "not"]
        .iter()
        .any(|keyword| schema.get(*keyword).is_some())
}

/// An unresolvable or cyclic property simply does not declare the type asked
/// about; it must not abandon inference for its siblings.
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
        |resolved, _, _| {
            Ok(!schema_uses_composition(resolved) && json_schema_type_contains(resolved, expected))
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

    use crate::v4::ir::{IrInputLocation, IrOperationInput, IrScalarType};

    use super::{WrappedListInferenceContext, infer_wrapped_list_row_path};

    fn inputs(specs: &[(&str, IrInputLocation)]) -> Vec<IrOperationInput> {
        specs
            .iter()
            .map(|(name, location)| IrOperationInput {
                name: (*name).to_string(),
                location: *location,
                required: false,
                data_type: IrScalarType::String,
                default_value: None,
                description: String::new(),
            })
            .collect()
    }

    fn row_path(schema: &Value, operation_inputs: &[IrOperationInput]) -> Vec<String> {
        infer_wrapped_list_row_path(WrappedListInferenceContext {
            operation_name: "list_things",
            inputs: operation_inputs,
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

        assert_eq!(row_path(&schema, &[]), ["items"]);
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

        assert_eq!(row_path(&schema, &[]), ["results", "data"]);
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

        assert_eq!(row_path(&schema, &[]), ["repositories"]);
    }

    #[test]
    fn selects_the_only_property_of_a_pure_envelope() {
        let schema = json!({
            "type": "object",
            "properties": {
                "data": {"type": "array", "items": {"type": "object"}},
            },
        });

        assert_eq!(row_path(&schema, &[]), ["data"]);
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

        assert!(row_path(&schema, &[]).is_empty());
        assert_eq!(
            row_path(
                &schema,
                &inputs(&[("start_cursor", IrInputLocation::Query)])
            ),
            ["data"]
        );
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
            row_path(&schema, &inputs(&[("bundle_id", IrInputLocation::Path)])).is_empty(),
            "a path parameter is not a pagination signal"
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

        assert!(row_path(&schema, &[]).is_empty());
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

        assert!(row_path(&string_count, &[]).is_empty());
        assert_eq!(row_path(&integer_count, &[]), ["steps"]);
    }

    #[test]
    fn ignores_metadata_named_arrays_as_row_candidates() {
        let schema = json!({
            "type": "object",
            "properties": {
                "has_more": {"type": "boolean"},
                "links": {"type": "array", "items": {"type": "string"}},
            },
        });

        assert!(row_path(&schema, &[]).is_empty());
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

        assert!(row_path(&schema, &[]).is_empty());
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
            row_path(&schema, &inputs(&[("cursor", IrInputLocation::Query)])).is_empty(),
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

        assert!(row_path(&schema, &[]).is_empty());
    }

    #[test]
    fn ignores_composed_schemas() {
        let schema = json!({
            "allOf": [{
                "type": "object",
                "properties": {
                    "total_count": {"type": "integer"},
                    "items": {"type": "array", "items": {"type": "object"}},
                },
            }],
        });

        assert!(row_path(&schema, &[]).is_empty());
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

        assert_eq!(row_path(&schema, &[]), ["data"]);
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

        assert!(row_path(&schema, &[]).is_empty());
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

        assert!(row_path(&schema, &[]).is_empty());
    }
}
