//! `DataFusion` table provider for manifest-driven HTTP-backed tables.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::project_schema;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use serde_json::Value;

use crate::backends::http::HttpSourceClient;
use crate::backends::http::ProviderQueryError;
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::schema_from_columns;
use crate::backends::shared::filter_expr::{
    FilterExtraction, classify_filter_pushdown_for_consumed, extract_exact_filter_values_checked,
    extract_filter_values, extract_filter_values_checked,
};
use crate::backends::shared::json_exec::{JsonExec, RowFetcher};
use crate::backends::shared::mapping::{convert_items, filter_items_by_column_values};
use coral_spec::backends::http::HttpTableSpec;

/// Table provider that exposes one manifest-defined HTTP table to `DataFusion`.
pub(crate) struct HttpSourceTableProvider {
    backend: HttpSourceClient,
    source_schema: String,
    table: Arc<HttpTableSpec>,
    target: HttpFetchTarget,
    schema: SchemaRef,
}

impl std::fmt::Debug for HttpSourceTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSourceTableProvider")
            .field("source_schema", &self.source_schema)
            .field("table", &self.table.name())
            .finish_non_exhaustive()
    }
}

impl HttpSourceTableProvider {
    /// Build a table provider for an `HTTP`-backed source table.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the table schema declared in the manifest
    /// is invalid.
    pub(crate) fn new(
        backend: HttpSourceClient,
        source_schema: String,
        table: HttpTableSpec,
    ) -> Result<Self> {
        let schema = schema_from_columns(table.columns(), &source_schema, table.name())?;
        let target = HttpFetchTarget::from_resolved_table_request(&table, table.request.clone());
        Ok(Self {
            backend,
            source_schema,
            table: Arc::new(table),
            target,
            schema,
        })
    }

    pub(crate) fn source_schema(&self) -> &str {
        &self.source_schema
    }

    pub(crate) fn client(&self) -> &HttpSourceClient {
        &self.backend
    }

    pub(crate) fn table_spec(&self) -> &Arc<HttpTableSpec> {
        &self.table
    }
}

#[derive(Debug)]
struct HttpFetchPlan {
    backend: HttpSourceClient,
    target: Arc<HttpFetchTarget>,
    request_filter_values: Arc<HashMap<String, String>>,
    arg_values: Arc<HashMap<String, String>>,
    limit: Option<usize>,
    has_residual_filters: bool,
}

pub(crate) struct HttpJsonExecRequest<'a> {
    pub(crate) backend: HttpSourceClient,
    pub(crate) source_schema: &'a str,
    pub(crate) target: HttpFetchTarget,
    pub(crate) schema: SchemaRef,
    pub(crate) request_filter_values: HashMap<String, String>,
    pub(crate) local_filter_values: HashMap<String, String>,
    pub(crate) active_filter_values: HashMap<String, String>,
    pub(crate) has_residual_filters: bool,
    pub(crate) arg_values: HashMap<String, String>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) limit: Option<usize>,
}

#[async_trait]
impl RowFetcher for HttpFetchPlan {
    async fn fetch(&self) -> Result<Vec<Value>> {
        if self.has_residual_filters {
            return self
                .backend
                .fetch_complete(
                    self.target.as_ref(),
                    &self.request_filter_values,
                    &self.arg_values,
                    None,
                    None,
                )
                .await;
        }

        self.backend
            .fetch(
                self.target.as_ref(),
                &self.request_filter_values,
                &self.arg_values,
                self.limit,
            )
            .await
    }
}

pub(crate) fn http_json_exec(request: HttpJsonExecRequest<'_>) -> Result<Arc<dyn ExecutionPlan>> {
    let HttpJsonExecRequest {
        backend,
        source_schema,
        target,
        schema,
        request_filter_values,
        local_filter_values,
        active_filter_values,
        has_residual_filters,
        arg_values,
        projection,
        limit,
    } = request;
    let target = Arc::new(target);
    let mut conversion_filter_values = request_filter_values.clone();
    conversion_filter_values.extend(
        local_filter_values
            .iter()
            .map(|(filter, value)| (filter.clone(), value.clone())),
    );
    let request_filter_values = Arc::new(request_filter_values);
    let local_filter_values = Arc::new(local_filter_values);
    let active_filter_values = Arc::new(active_filter_values);
    let arg_values = Arc::new(arg_values);
    let post_filter_limit = if local_filter_values.is_empty() || has_residual_filters {
        None
    } else {
        limit.or(target.fetch_limit_default())
    };
    let fetcher = Arc::new(HttpFetchPlan {
        backend,
        target: target.clone(),
        request_filter_values: request_filter_values.clone(),
        arg_values: arg_values.clone(),
        limit,
        has_residual_filters,
    });

    let converter = {
        let target = target.clone();
        let schema = schema.clone();
        let conversion_filter_values = Arc::new(conversion_filter_values);
        let local_filter_values = local_filter_values.clone();
        let active_filter_values = active_filter_values.clone();
        let arg_values = arg_values.clone();
        Arc::new(move |items: &[Value]| {
            let mut filtered_items;
            let items = if local_filter_values.is_empty() {
                items
            } else {
                filtered_items = filter_items_by_column_values(
                    target.columns(),
                    &local_filter_values,
                    &active_filter_values,
                    &arg_values,
                    items,
                );
                if let Some(limit) = post_filter_limit {
                    filtered_items.truncate(limit);
                }
                &filtered_items
            };
            convert_items(
                target.columns(),
                schema.clone(),
                &conversion_filter_values,
                &arg_values,
                items,
            )
        })
    };

    let exec = JsonExec::new(
        source_schema,
        target.name(),
        schema,
        fetcher,
        converter,
        projection.cloned(),
    )?;

    Ok(Arc::new(exec))
}

#[async_trait]
impl TableProvider for HttpSourceTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let filter_exprs = filters
            .iter()
            .map(|expr| (*expr).clone())
            .collect::<Vec<_>>();
        let filter_values = extract_filter_values(&filter_exprs, self.table.filters());
        let filter_value_keys: HashSet<String> = filter_values.keys().cloned().collect();
        let active_request = self.table.resolve_request(&filter_value_keys);
        let consumed_filters = self.backend.request_filter_names(active_request);

        Ok(classify_filter_pushdown_for_consumed(
            filters,
            self.table.filters(),
            &consumed_filters,
        ))
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter_values = match extract_filter_values_checked(filters, self.table.filters()) {
            FilterExtraction::Values(values) => values,
            FilterExtraction::Contradiction => {
                let projected_schema = project_schema(&self.schema, projection)?;
                return Ok(Arc::new(EmptyExec::new(projected_schema)));
            }
        };

        for required in self.table.filters().iter().filter(|f| f.required) {
            if !filter_values.contains_key(&required.name) {
                return Err(DataFusionError::External(Box::new(
                    ProviderQueryError::MissingRequiredFilter {
                        schema: self.source_schema.clone(),
                        table: self.table.name().to_string(),
                        column: required.name.clone(),
                    },
                )));
            }
        }

        let filter_value_keys: HashSet<String> = filter_values.keys().cloned().collect();
        let active_request = self.table.resolve_request(&filter_value_keys).clone();
        let consumed_filters = self.backend.request_filter_names(&active_request);
        let request_filter_values = filter_values
            .iter()
            .filter(|(filter, _)| consumed_filters.contains(*filter))
            .map(|(filter, value)| (filter.clone(), value.clone()))
            .collect();
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let has_residual_filters = classify_filter_pushdown_for_consumed(
            &filter_refs,
            self.table.filters(),
            &consumed_filters,
        )
        .iter()
        .any(|pushdown| !matches!(pushdown, TableProviderFilterPushDown::Exact));
        let local_filter_values =
            match extract_exact_filter_values_checked(filters, self.table.filters()) {
                FilterExtraction::Values(values) => values
                    .into_iter()
                    .filter(|(filter, _)| !consumed_filters.contains(filter))
                    .collect(),
                FilterExtraction::Contradiction => HashMap::new(),
            };
        let target = self.target.with_resolved_request(active_request);

        http_json_exec(HttpJsonExecRequest {
            backend: self.backend.clone(),
            source_schema: &self.source_schema,
            target,
            schema: self.schema.clone(),
            request_filter_values,
            local_filter_values,
            active_filter_values: filter_values,
            has_residual_filters,
            arg_values: HashMap::new(),
            projection,
            limit,
        })
    }
}
