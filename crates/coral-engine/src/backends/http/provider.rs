//! `DataFusion` table provider for manifest-driven HTTP-backed tables.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use serde_json::Value;

use crate::backends::http::HttpSourceClient;
use crate::backends::http::ProviderQueryError;
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::schema_from_columns;
use crate::backends::shared::filter_expr::{classify_filter_pushdown, extract_filter_values};
use crate::backends::shared::json_exec::{JsonExec, RowFetcher};
use crate::backends::shared::mapping::convert_items;
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
}

#[derive(Debug)]
struct HttpFetchPlan {
    backend: HttpSourceClient,
    target: Arc<HttpFetchTarget>,
    filter_values: Arc<HashMap<String, String>>,
    arg_values: Arc<HashMap<String, String>>,
    limit: Option<usize>,
}

pub(crate) struct HttpJsonExecRequest<'a> {
    pub(crate) backend: HttpSourceClient,
    pub(crate) source_schema: &'a str,
    pub(crate) target: HttpFetchTarget,
    pub(crate) schema: SchemaRef,
    pub(crate) filter_values: HashMap<String, String>,
    pub(crate) arg_values: HashMap<String, String>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) limit: Option<usize>,
}

#[async_trait]
impl RowFetcher for HttpFetchPlan {
    async fn fetch(&self) -> Result<Vec<Value>> {
        self.backend
            .fetch(
                self.target.as_ref(),
                &self.filter_values,
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
        filter_values,
        arg_values,
        projection,
        limit,
    } = request;
    let target = Arc::new(target);
    let filter_values = Arc::new(filter_values);
    let arg_values = Arc::new(arg_values);
    let fetcher = Arc::new(HttpFetchPlan {
        backend,
        target: target.clone(),
        filter_values: filter_values.clone(),
        arg_values: arg_values.clone(),
        limit,
    });

    let converter = {
        let target = target.clone();
        let schema = schema.clone();
        let filter_values = filter_values.clone();
        let arg_values = arg_values.clone();
        Arc::new(move |items: &[Value]| {
            convert_items(
                target.columns(),
                schema.clone(),
                &filter_values,
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
        Ok(classify_filter_pushdown(filters, self.table.filters()))
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter_values = extract_filter_values(filters, self.table.filters());

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
        let target = self.target.with_resolved_request(active_request);

        http_json_exec(HttpJsonExecRequest {
            backend: self.backend.clone(),
            source_schema: &self.source_schema,
            target,
            schema: self.schema.clone(),
            filter_values,
            arg_values: HashMap::new(),
            projection,
            limit,
        })
    }
}
