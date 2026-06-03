use std::collections::HashSet;

use crate::PaginationSpec;
use crate::v4::ir::OpenApiParameterLocation;

use super::model::ProjectionInput;

pub(super) fn pagination_query_param_names(pagination: &PaginationSpec) -> HashSet<&str> {
    let mut names = HashSet::new();
    if let Some(name) = pagination.page_param.as_deref() {
        names.insert(name);
    }
    if let Some(name) = pagination.offset_param.as_deref() {
        names.insert(name);
    }
    if let Some(name) = pagination.cursor_param.as_deref() {
        names.insert(name);
    }
    if let Some(page_size) = &pagination.page_size
        && let Some(name) = page_size.query_param.as_deref()
    {
        names.insert(name);
    }
    names
}

pub(super) fn pagination_owns_input(
    input: &ProjectionInput,
    pagination_query_params: &HashSet<&str>,
) -> bool {
    input.source_location == OpenApiParameterLocation::Query
        && pagination_query_params.contains(input.wire_name.as_str())
}
