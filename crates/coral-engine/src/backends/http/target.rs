//! Engine-local fetch shape shared by HTTP tables and table functions.

use std::sync::Arc;

use coral_spec::backends::http::HttpTableSpec;
use coral_spec::{ColumnSpec, PaginationSpec, RequestSpec, ResponseSpec};

/// The HTTP request/response description needed to fetch rows.
///
/// Tables and table functions are distinct manifest concepts, but once their
/// SQL-facing inputs have been resolved they share the same HTTP execution path.
#[derive(Clone)]
pub(crate) struct HttpFetchTarget {
    name: Arc<str>,
    columns: Arc<[ColumnSpec]>,
    fetch_limit_default: Option<usize>,
    resolved_request: RequestSpec,
    response: Arc<ResponseSpec>,
    pagination: Arc<PaginationSpec>,
}

impl std::fmt::Debug for HttpFetchTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpFetchTarget")
            .field("name", &self.name)
            .field("columns", &self.columns)
            .field("fetch_limit_default", &self.fetch_limit_default)
            .finish_non_exhaustive()
    }
}

impl HttpFetchTarget {
    pub(crate) fn from_resolved_table_request(
        table: &HttpTableSpec,
        resolved_request: RequestSpec,
    ) -> Self {
        Self {
            name: Arc::from(table.name()),
            columns: Arc::from(table.columns().to_vec()),
            fetch_limit_default: table.fetch_limit_default(),
            resolved_request,
            response: Arc::new(table.response.clone()),
            pagination: Arc::new(table.pagination.clone()),
        }
    }

    pub(crate) fn with_resolved_request(&self, resolved_request: RequestSpec) -> Self {
        Self {
            name: Arc::clone(&self.name),
            columns: Arc::clone(&self.columns),
            fetch_limit_default: self.fetch_limit_default,
            resolved_request,
            response: Arc::clone(&self.response),
            pagination: Arc::clone(&self.pagination),
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

    pub(crate) fn resolved_request(&self) -> &RequestSpec {
        &self.resolved_request
    }

    pub(crate) fn response(&self) -> &ResponseSpec {
        &self.response
    }

    pub(crate) fn pagination(&self) -> &PaginationSpec {
        &self.pagination
    }
}
