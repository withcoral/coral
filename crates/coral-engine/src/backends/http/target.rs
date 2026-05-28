//! Engine-local fetch shape shared by HTTP tables and table functions.

use std::sync::Arc;

use coral_spec::backends::http::{HttpCachePolicySpec, HttpTableSpec};
use coral_spec::{
    ColumnSpec, PaginationSpec, RequestSpec, ResponseSpec, SearchLimitsSpec,
    SourceTableFunctionSpec,
};

/// The HTTP request/response description needed to fetch rows.
///
/// Tables and table functions are distinct manifest concepts, but once their
/// SQL-facing inputs have been resolved they share the same HTTP execution path.
#[derive(Clone)]
pub(crate) struct HttpFetchTarget {
    name: Arc<str>,
    columns: Arc<[ColumnSpec]>,
    fetch_limit_default: Option<usize>,
    search_limits: Option<SearchLimitsSpec>,
    resolved_request: RequestSpec,
    response: Arc<ResponseSpec>,
    pagination: Arc<PaginationSpec>,
    cache: Option<Arc<HttpCachePolicySpec>>,
}

impl std::fmt::Debug for HttpFetchTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpFetchTarget")
            .field("name", &self.name)
            .field("columns", &self.columns)
            .field("fetch_limit_default", &self.fetch_limit_default)
            .field("search_limits", &self.search_limits)
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
            search_limits: table.common.search_limits.clone(),
            resolved_request,
            response: Arc::new(table.response.clone()),
            pagination: Arc::new(table.pagination.clone()),
            cache: table.cache.clone().map(Arc::new),
        }
    }

    pub(crate) fn with_resolved_request(&self, resolved_request: RequestSpec) -> Self {
        Self {
            name: Arc::clone(&self.name),
            columns: Arc::clone(&self.columns),
            fetch_limit_default: self.fetch_limit_default,
            search_limits: self.search_limits.clone(),
            resolved_request,
            response: Arc::clone(&self.response),
            pagination: Arc::clone(&self.pagination),
            cache: self.cache.clone(),
        }
    }

    pub(crate) fn from_function(function: &SourceTableFunctionSpec) -> Self {
        Self {
            name: Arc::from(function.name.as_str()),
            columns: Arc::from(function.columns.clone()),
            fetch_limit_default: function.fetch_limit_default,
            search_limits: function.search_limits.clone(),
            resolved_request: function.request.clone(),
            response: Arc::new(function.response.clone()),
            pagination: Arc::new(function.pagination.clone()),
            cache: None,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }

    pub(crate) fn fetch_limit_default(&self) -> Option<usize> {
        self.fetch_limit_default.or_else(|| {
            self.search_limits
                .as_ref()
                .map(|limits| limits.default_top_k)
        })
    }

    pub(crate) fn search_limits(&self) -> Option<&SearchLimitsSpec> {
        self.search_limits.as_ref()
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

    pub(crate) fn cache(&self) -> Option<&HttpCachePolicySpec> {
        self.cache.as_deref()
    }
}
