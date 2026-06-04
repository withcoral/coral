//! `DataFusion` table functions for manifest-driven HTTP-backed sources.
//!
//! `TableFunctionImpl::call` runs while `DataFusion` is planning a query. At
//! that point we validate the positional call arguments and bind them into HTTP
//! request values. The returned table provider is scanned later during
//! execution, using the same `http_json_exec` path as manifest-backed tables.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use coral_spec::SourceTableFunctionSpec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::backends::http::HttpSourceClient;
use crate::backends::http::provider::{HttpJsonExecRequest, http_json_exec};
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::schema_from_columns;
use crate::backends::shared::filter_expr::literal_to_string;
use crate::backends::shared::function_args::{FunctionCallContext, bind_table_function_args};

/// Immutable execution state shared by every invocation of one registered HTTP
/// table function.
struct HttpSourceFunctionState {
    backend: HttpSourceClient,
    source_schema: String,
    function_name: String,
    target: Arc<HttpFetchTarget>,
    schema: SchemaRef,
}

/// Table-valued function that turns manifest-declared function args into an
/// HTTP-backed result provider.
pub(crate) struct HttpSourceTableFunction {
    spec: Arc<SourceTableFunctionSpec>,
    state: Arc<HttpSourceFunctionState>,
}

impl fmt::Debug for HttpSourceTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpSourceTableFunction")
            .field("source_schema", &self.state.source_schema)
            .field("function", &self.state.function_name)
            .finish_non_exhaustive()
    }
}

impl HttpSourceTableFunction {
    pub(crate) fn new(
        backend: HttpSourceClient,
        source_schema: String,
        function: SourceTableFunctionSpec,
    ) -> Result<Self> {
        let schema = schema_from_columns(&function.columns, &source_schema, &function.name)?;
        let target = HttpFetchTarget::from_function(&function);
        let function_name = function.name.clone();
        Ok(Self {
            spec: Arc::new(function),
            state: Arc::new(HttpSourceFunctionState {
                backend,
                source_schema,
                function_name,
                target: Arc::new(target),
                schema,
            }),
        })
    }
}

impl TableFunctionImpl for HttpSourceTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let arg_values = bind_function_args(&self.state.source_schema, &self.spec, args)?;
        Ok(Arc::new(HttpSourceFunctionCallTableProvider {
            state: Arc::clone(&self.state),
            arg_values,
        }))
    }
}

/// Concrete table provider returned for one function call, with SQL arguments
/// already bound into HTTP request values.
struct HttpSourceFunctionCallTableProvider {
    state: Arc<HttpSourceFunctionState>,
    arg_values: HashMap<String, String>,
}

impl fmt::Debug for HttpSourceFunctionCallTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpSourceFunctionCallTableProvider")
            .field("source_schema", &self.state.source_schema)
            .field("function", &self.state.function_name)
            .field("arg_values", &self.arg_values.keys())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl TableProvider for HttpSourceFunctionCallTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.state.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Function arguments have already been bound into the request. WHERE
        // filters apply to the returned rows, so DataFusion should evaluate them.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        http_json_exec(HttpJsonExecRequest {
            backend: self.state.backend.clone(),
            source_schema: &self.state.source_schema,
            target: (*self.state.target).clone(),
            schema: self.state.schema.clone(),
            filter_values: HashMap::new(),
            arg_values: self.arg_values.clone(),
            projection,
            limit,
        })
    }
}

fn bind_function_args(
    source_schema: &str,
    function: &SourceTableFunctionSpec,
    args: &[Expr],
) -> Result<HashMap<String, String>> {
    bind_table_function_args(
        &FunctionCallContext {
            source_schema,
            function_name: function.name.as_str(),
        },
        &function.args,
        args,
        literal_to_string,
        Clone::clone,
        |context, required_missing| {
            DataFusionError::Plan(format!(
                "{}.{} missing required argument(s): {}",
                context.source_schema,
                context.function_name,
                required_missing.join(", ")
            ))
        },
    )
}
