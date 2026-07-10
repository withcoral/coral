//! Heuristic inference of lookup key joinability for REST surfaces.
//!
//! During OpenAPI import Coral guesses which REST query parameters are NOT
//! complete exact lookups and stamps that fact into the semantic IR. Excluded
//! parameters stay pushdown filters; they just cannot anchor dependent joins
//! (`FilterSpec.lookup_key` stays false). MCP pagination is handled separately
//! by MCP import/projection code, and MCP inputs do not use lookup-key
//! exclusions. The heuristic excludes aggressively: a wrong exclusion only
//! makes a join fall back to the regular plan, while a wrong inclusion lets the
//! dependent-join optimizer trust incomplete or fuzzy result sets and produce
//! wrong rows.

use crate::PaginationSpec;
use crate::v4::ir::{IrInputLocation, IrOperationInput};
use crate::v4::projections::pagination_query_param_names;

/// Normalized parameter names that only shape response presentation and never
/// select result candidates; joining on them is meaningless.
const PRESENTATION_NAME_LEXICON: &[&str] = &[
    "callback",
    "direction",
    "embed",
    "envelope",
    "expand",
    "fields",
    "format",
    "include",
    "order",
    "orderby",
    "ordering",
    "pretty",
    "select",
    "sort",
    "sortby",
    "sortdir",
    "sortfield",
    "sortorder",
];

/// Normalized names whose values drive fuzzy or expression matching rather
/// than exact attribute equality (`q=foo` selects search matches, not rows
/// where `q` equals `foo`). They stay pushdown filters but never become
/// lookup keys: their virtual columns only echo the request value, so
/// join-equality enforcement cannot catch the mismatch.
const SEARCH_NAME_LEXICON: &[&str] = &[
    "filter", "filters", "keyword", "keywords", "q", "query", "search",
];

pub(in crate::v4) fn infer_rest_lookup_key_exclusions(
    inputs: &mut [IrOperationInput],
    pagination: &PaginationSpec,
) {
    let pagination_params = pagination_query_param_names(pagination);
    for input in inputs {
        input.exclude_from_lookup_keys = false;
        if input.location != IrInputLocation::Query {
            continue;
        }
        // Pagination-owned parameters are internal today, but a later
        // pagination override can remap the scheme and surface them as
        // filters. Detection has proven they are windowing, so record that
        // in the IR instead of relying on their exposure.
        if pagination_params.contains(input.name.as_str()) || !joinable_param_name(&input.name) {
            input.exclude_from_lookup_keys = true;
        }
    }
}

/// Exact lexicon match only: token-level matching (e.g. treating
/// `sort_field` as presentation) also caught identity keys like `order_id`,
/// and losing joinability on a foreign key costs more than missing a
/// presentation alias. Unrecognized names stay joinable; the semantic IR is
/// the audit trail for correcting either direction.
fn joinable_param_name(name: &str) -> bool {
    let normalized = normalized_param_name(name);
    !(SEARCH_NAME_LEXICON.contains(&normalized.as_str())
        || PRESENTATION_NAME_LEXICON.contains(&normalized.as_str()))
}

/// Collapses case and separators so `orderBy`, `order_by`, and `order-by`
/// all match the lexicon entry `orderby`.
fn normalized_param_name(name: &str) -> String {
    name.chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use crate::v4::ir::{IrInputLocation, IrOperationInput, IrScalarType};
    use crate::{PaginationMode, PaginationSpec};

    use super::infer_rest_lookup_key_exclusions;

    #[test]
    fn infers_exclusions_for_presentation_and_search_params() {
        let page_pagination = PaginationSpec {
            mode: PaginationMode::Page,
            page_param: Some("page".to_string()),
            ..PaginationSpec::default()
        };
        let mut widgets = query_inputs(&[
            "order_by",
            "sort ",
            "sort_field",
            "q",
            "search",
            "state",
            "category",
            "page",
        ]);
        let mut gadgets = query_inputs(&["filter", "since"]);

        // `order_by`/`sort_field` are exact presentation hits after name
        // normalization, `q`/`search`/`filter` are search-like, and
        // pagination-owned `page` is recorded as non-joinable so a later
        // pagination remap cannot surface it as a lookup key. Padded `sort `
        // is still excluded because no generated side file has to represent
        // the raw padded name. `state`/`category`/`since` remain joinable.
        infer_rest_lookup_key_exclusions(&mut widgets, &page_pagination);
        infer_rest_lookup_key_exclusions(&mut gadgets, &PaginationSpec::default());
        assert!(input_excluded(&widgets, "order_by"));
        assert!(input_excluded(&widgets, "sort "));
        assert!(input_excluded(&widgets, "sort_field"));
        assert!(input_excluded(&widgets, "q"));
        assert!(input_excluded(&widgets, "search"));
        assert!(input_excluded(&widgets, "page"));
        assert!(input_excluded(&gadgets, "filter"));
        assert!(!input_excluded(&widgets, "state"));
        assert!(!input_excluded(&widgets, "category"));
        assert!(!input_excluded(&gadgets, "since"));
    }

    #[test]
    fn pagination_exclusions_are_scoped_to_the_owning_operation() {
        let pagination = PaginationSpec {
            mode: PaginationMode::CursorQuery,
            cursor_param: Some("cursor".to_string()),
            ..PaginationSpec::default()
        };
        let mut paginated = query_inputs(&["cursor"]);
        let mut ordinary = query_inputs(&["cursor"]);

        infer_rest_lookup_key_exclusions(&mut paginated, &pagination);
        infer_rest_lookup_key_exclusions(&mut ordinary, &PaginationSpec::default());

        assert!(input_excluded(&paginated, "cursor"));
        assert!(!input_excluded(&ordinary, "cursor"));
    }

    fn query_inputs(query_params: &[&str]) -> Vec<IrOperationInput> {
        query_params
            .iter()
            .map(|name| IrOperationInput {
                name: (*name).to_string(),
                location: IrInputLocation::Query,
                required: false,
                data_type: IrScalarType::String,
                default_value: None,
                description: String::new(),
                exclude_from_lookup_keys: false,
            })
            .collect()
    }

    fn input_excluded(inputs: &[IrOperationInput], input_name: &str) -> bool {
        inputs
            .iter()
            .find(|input| input.name == input_name)
            .expect("input")
            .exclude_from_lookup_keys
    }
}
