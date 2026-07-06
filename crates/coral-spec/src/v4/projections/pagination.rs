use std::collections::HashSet;

use crate::PaginationSpec;

pub(in crate::v4) fn pagination_query_param_names(pagination: &PaginationSpec) -> HashSet<&str> {
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
