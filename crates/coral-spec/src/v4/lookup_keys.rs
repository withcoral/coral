//! Heuristic inference of lookup key joinability for REST surfaces.
//!
//! At materialization time Coral guesses which query parameters are NOT
//! complete exact lookups and writes them to the `exclude` list of the
//! generated `parameter_metadata.yaml`. Excluded parameters stay pushdown
//! filters; they just cannot anchor dependent joins (`FilterSpec.lookup_key`
//! stays false). The heuristic excludes aggressively: a wrong exclusion only
//! makes a join fall back to the regular plan, while a wrong inclusion lets
//! the dependent-join optimizer trust incomplete or fuzzy result sets and
//! produce wrong rows.

use std::collections::BTreeSet;

use crate::v4::ir::{IrExecutionAttachment, IrInputLocation, SemanticIr};
use crate::v4::parameter_metadata::LookupKeysMetadata;
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

/// Infers the surface-wide lookup key exclude list from the semantic IR.
/// Returns `None` for surfaces without REST operations.
#[must_use]
pub fn infer_lookup_keys(ir: &SemanticIr) -> Option<LookupKeysMetadata> {
    let mut has_rest_operations = false;
    let mut exclude = BTreeSet::new();

    for operation in &ir.operations {
        let IrExecutionAttachment::Rest(rest) = &operation.execution else {
            continue;
        };
        has_rest_operations = true;
        let pagination_params = pagination_query_param_names(&rest.pagination);
        for input in &operation.inputs {
            if input.location != IrInputLocation::Query {
                continue;
            }
            // A padded name cannot legally appear in the generated file (the
            // shape rule rejects it), so skip it rather than emit an invalid
            // artifact; normalizing pathological specs is the importer's job.
            if input.name != input.name.trim() {
                continue;
            }
            // Pagination-owned parameters are internal today, but a later
            // pagination override can remap the scheme and surface them as
            // filters. Detection has proven they are windowing, so record
            // that in the artifact instead of relying on their exposure.
            if pagination_params.contains(input.name.as_str()) || !joinable_param_name(&input.name)
            {
                exclude.insert(input.name.clone());
            }
        }
    }

    has_rest_operations.then(|| LookupKeysMetadata {
        enabled: true,
        exclude: exclude.into_iter().collect(),
    })
}

/// Exact lexicon match only: token-level matching (e.g. treating
/// `sort_field` as presentation) also caught identity keys like `order_id`,
/// and losing joinability on a foreign key costs more than missing a
/// presentation alias. Unrecognized names stay joinable; the generated file
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
    use crate::v4::ir::{
        HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput,
        IrOperationOutput, IrScalarType, OutputCardinality, RestExecutionAttachment,
        RestResponseAttachment, SemanticIr,
    };
    use crate::v4::manifest::SurfaceType;
    use crate::v4::{OPENAPI_IMPORTER_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
    use crate::{PaginationMode, PaginationSpec, ResponseSpec};

    use super::infer_lookup_keys;

    #[test]
    fn infers_exclusions_for_presentation_and_search_params() {
        let page_pagination = PaginationSpec {
            mode: PaginationMode::Page,
            page_param: Some("page".to_string()),
            ..PaginationSpec::default()
        };
        let ir = semantic_ir(vec![
            rest_operation(
                "widgets_list",
                &[
                    "order_by",
                    "sort ",
                    "sort_field",
                    "q",
                    "search",
                    "state",
                    "category",
                    "page",
                ],
                page_pagination,
            ),
            rest_operation(
                "gadgets_list",
                &["filter", "since"],
                PaginationSpec::default(),
            ),
        ]);

        let metadata = infer_lookup_keys(&ir).expect("rest surface metadata");

        assert!(metadata.enabled);
        // `order_by`/`sort_field` are exact presentation hits after name
        // normalization, `q`/`search`/`filter` are search-like, and
        // pagination-owned `page` is recorded as non-joinable so a later
        // pagination remap cannot surface it as a lookup key. Padded
        // `sort ` is skipped entirely: the generated file's shape rule
        // rejects padded values. `state`/`category`/`since` remain joinable.
        assert_eq!(
            metadata.exclude,
            ["filter", "order_by", "page", "q", "search", "sort_field"]
        );

        let mcp_only = semantic_ir(Vec::new());
        assert!(infer_lookup_keys(&mcp_only).is_none());
    }

    fn semantic_ir(operations: Vec<IrOperation>) -> SemanticIr {
        SemanticIr {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "demo".to_string(),
            surface_id: "rest".to_string(),
            surface_type: SurfaceType::OpenApi,
            importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
            operations,
            types: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    fn rest_operation(id: &str, query_params: &[&str], pagination: PaginationSpec) -> IrOperation {
        let inputs = query_params
            .iter()
            .map(|name| IrOperationInput {
                name: (*name).to_string(),
                location: IrInputLocation::Query,
                required: false,
                data_type: IrScalarType::String,
                default_value: None,
                description: String::new(),
            })
            .collect();
        IrOperation {
            id: id.to_string(),
            method_name: "GET".to_string(),
            description: String::new(),
            deprecated: false,
            read_only: true,
            naming: None,
            inputs,
            output: IrOperationOutput {
                cardinality: OutputCardinality::List,
                type_ref: "row".to_string(),
                row_path: Vec::new(),
            },
            entity: None,
            execution: IrExecutionAttachment::Rest(Box::new(RestExecutionAttachment {
                method: HttpMethod::Get,
                path_template: format!("/{id}"),
                parameters: Vec::new(),
                request_body: None,
                response: RestResponseAttachment {
                    status_code: 200,
                    media_type: "application/json".to_string(),
                    response: ResponseSpec::default(),
                },
                pagination,
            })),
            diagnostics: Vec::new(),
        }
    }
}
