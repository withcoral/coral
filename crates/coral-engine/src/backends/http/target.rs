//! Engine-local request shape shared by HTTP tables and table functions.

use coral_spec::backends::http::HttpTableSpec;
use coral_spec::{ColumnSpec, PaginationSpec, RequestSpec, ResponseSpec};

/// The HTTP request/response description needed to fetch rows.
///
/// Tables and table functions are distinct manifest concepts, but once their
/// SQL-facing inputs have been resolved they share the same HTTP execution path.
#[derive(Debug, Clone)]
pub(crate) struct HttpRequestTarget {
    name: String,
    columns: Vec<ColumnSpec>,
    fetch_limit_default: Option<usize>,
    pub(crate) request: RequestSpec,
    pub(crate) response: ResponseSpec,
    pub(crate) pagination: PaginationSpec,
}

impl HttpRequestTarget {
    pub(crate) fn from_table(table: &HttpTableSpec, request: RequestSpec) -> Self {
        Self {
            name: table.name().to_string(),
            columns: table.columns().to_vec(),
            fetch_limit_default: table.fetch_limit_default(),
            request,
            response: table.response.clone(),
            pagination: table.pagination.clone(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }

    pub(crate) fn fetch_limit_default(&self) -> Option<usize> {
        self.fetch_limit_default
    }
}
