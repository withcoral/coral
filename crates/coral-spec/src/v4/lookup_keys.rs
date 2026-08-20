//! Heuristic inference of lookup key joinability for REST surfaces.
//!
//! During OpenAPI import Coral guesses which REST query parameters are
//! complete exact lookups and records the positive allowlist in operation
//! metadata. Other parameters stay pushdown filters; they just cannot anchor dependent joins
//! (`FilterSpec.lookup_key` stays false). MCP pagination is handled separately
//! by MCP import/projection code, and MCP inputs never enter REST lookup-key
//! allowlists. The heuristic excludes aggressively: a wrong exclusion only
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

pub(in crate::v4) fn infer_rest_lookup_keys(
    inputs: &[IrOperationInput],
    pagination: &PaginationSpec,
) -> Vec<String> {
    let pagination_params = pagination_query_param_names(pagination);
    inputs
        .iter()
        .filter(|input| input.location == IrInputLocation::Query)
        .filter(|input| !pagination_params.contains(input.name.as_str()))
        // A multi-valued parameter is never an exact lookup: its SQL value is a
        // JSON array, while a dependent join binds a bare scalar. Letting one
        // through would push `alice` where `["alice"]` is expected and fail the
        // scan at execution time.
        .filter(|input| input.collection_encoding.is_none())
        .filter(|input| joinable_param_name(&input.name))
        .map(|input| input.name.clone())
        .collect()
}

/// Exact lexicon match only: token-level matching (e.g. treating
/// `sort_field` as presentation) also caught identity keys like `order_id`,
/// and losing joinability on a foreign key costs more than missing a
/// presentation alias. Unrecognized names stay joinable; operation metadata
/// is the audit trail for correcting either direction.
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

    use super::infer_rest_lookup_keys;

    #[test]
    fn infers_positive_allowlist_without_presentation_search_or_pagination_params() {
        let page_pagination = PaginationSpec {
            mode: PaginationMode::Page,
            page_param: Some("page".to_string()),
            ..PaginationSpec::default()
        };
        let widgets = query_inputs(&[
            "order_by",
            "sort ",
            "sort_field",
            "q",
            "search",
            "state",
            "category",
            "page",
        ]);
        let gadgets = query_inputs(&["filter", "since"]);

        // `order_by`/`sort_field` are exact presentation hits after name
        // normalization, `q`/`search`/`filter` are search-like, and
        // pagination-owned `page` is omitted so a later pagination remap
        // cannot surface it as a lookup key. Padded `sort ` is still omitted.
        // `state`/`category`/`since` remain joinable.
        let widget_keys = infer_rest_lookup_keys(&widgets, &page_pagination);
        let gadget_keys = infer_rest_lookup_keys(&gadgets, &PaginationSpec::default());
        assert_eq!(widget_keys, ["state", "category"]);
        assert_eq!(gadget_keys, ["since"]);
    }

    #[test]
    fn pagination_omission_is_scoped_to_the_owning_operation() {
        let pagination = PaginationSpec {
            mode: PaginationMode::CursorQuery,
            cursor_param: Some("cursor".to_string()),
            ..PaginationSpec::default()
        };
        let paginated = query_inputs(&["cursor"]);
        let ordinary = query_inputs(&["cursor"]);

        assert!(infer_rest_lookup_keys(&paginated, &pagination).is_empty());
        assert_eq!(
            infer_rest_lookup_keys(&ordinary, &PaginationSpec::default()),
            ["cursor"]
        );
    }

    fn query_inputs(query_params: &[&str]) -> Vec<IrOperationInput> {
        query_params
            .iter()
            .map(|name| IrOperationInput {
                name: (*name).to_string(),
                location: IrInputLocation::Query,
                required: false,
                data_type: IrScalarType::String,
                collection_encoding: None,
                default_value: None,
                description: String::new(),
            })
            .collect()
    }
}
