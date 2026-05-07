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
use datafusion::scalar::ScalarValue;

use crate::backends::http::HttpSourceClient;
use crate::backends::http::provider::http_json_exec;
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::schema_from_columns;
use crate::backends::shared::filter_expr::literal_to_string;

struct FunctionCallContext<'a> {
    source_schema: &'a str,
    function_name: &'a str,
}

/// Table-valued function that turns manifest-declared function args into an
/// HTTP-backed result provider.
pub(crate) struct HttpSourceTableFunction {
    backend: HttpSourceClient,
    source_schema: String,
    spec: Arc<SourceTableFunctionSpec>,
    target: Arc<HttpFetchTarget>,
    schema: SchemaRef,
}

impl fmt::Debug for HttpSourceTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpSourceTableFunction")
            .field("source_schema", &self.source_schema)
            .field("function", &self.spec.name)
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
        Ok(Self {
            backend,
            source_schema,
            spec: Arc::new(function),
            target: Arc::new(target),
            schema,
        })
    }
}

impl TableFunctionImpl for HttpSourceTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let request_values = bind_function_args(&self.source_schema, &self.spec, args)?;
        Ok(Arc::new(HttpSourceFunctionCallProvider {
            backend: self.backend.clone(),
            source_schema: self.source_schema.clone(),
            function_name: self.spec.name.clone(),
            target: Arc::clone(&self.target),
            schema: self.schema.clone(),
            request_values,
        }))
    }
}

struct HttpSourceFunctionCallProvider {
    backend: HttpSourceClient,
    source_schema: String,
    function_name: String,
    target: Arc<HttpFetchTarget>,
    schema: SchemaRef,
    request_values: HashMap<String, String>,
}

impl fmt::Debug for HttpSourceFunctionCallProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpSourceFunctionCallProvider")
            .field("source_schema", &self.source_schema)
            .field("function", &self.function_name)
            .field("request_values", &self.request_values.keys())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl TableProvider for HttpSourceFunctionCallProvider {
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
        http_json_exec(
            self.backend.clone(),
            &self.source_schema,
            (*self.target).clone(),
            self.schema.clone(),
            self.request_values.clone(),
            projection,
            limit,
        )
    }
}

fn bind_function_args(
    source_schema: &str,
    function: &SourceTableFunctionSpec,
    args: &[Expr],
) -> Result<HashMap<String, String>> {
    let context = FunctionCallContext {
        source_schema,
        function_name: function.name.as_str(),
    };
    ensure_no_extra_args(&context, function.args.len(), args.len())?;

    let mut required_missing = Vec::new();
    let mut request_values = HashMap::with_capacity(function.args.len());

    for (index, spec) in function.args.iter().enumerate() {
        let Some(value) = resolve_call_arg_literal(&context, spec.name.as_str(), args.get(index))?
        else {
            if spec.required {
                required_missing.push(spec.name.as_str());
            }
            continue;
        };
        ensure_call_arg_allowed_value(&context, spec.name.as_str(), &value, &spec.values)?;
        request_values.insert(spec.bind.arg.clone(), value);
    }

    if !required_missing.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{}.{} missing required argument(s): {}",
            context.source_schema,
            context.function_name,
            required_missing.join(", ")
        )));
    }

    Ok(request_values)
}

fn ensure_no_extra_args(
    context: &FunctionCallContext<'_>,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if actual > expected {
        return Err(DataFusionError::Plan(format!(
            "{}.{} expected at most {} arguments, got {}",
            context.source_schema, context.function_name, expected, actual
        )));
    }
    Ok(())
}

fn resolve_call_arg_literal(
    context: &FunctionCallContext<'_>,
    arg_name: &str,
    expr: Option<&Expr>,
) -> Result<Option<String>> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    if is_null_literal(expr) {
        return Ok(None);
    }
    let Some(value) = literal_to_string(expr) else {
        return Err(DataFusionError::Plan(format!(
            "{}.{} argument '{}' must be a literal",
            context.source_schema, context.function_name, arg_name
        )));
    };
    Ok(Some(value))
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Null, _))
}

fn ensure_call_arg_allowed_value(
    context: &FunctionCallContext<'_>,
    arg: &str,
    value: &str,
    allowed_values: &[String],
) -> Result<()> {
    if !allowed_values.is_empty() && !allowed_values.iter().any(|allowed| allowed == value) {
        return Err(DataFusionError::Plan(format!(
            "{}.{} argument '{arg}' has invalid value '{value}'; expected one of: {}",
            context.source_schema,
            context.function_name,
            allowed_values.join(", ")
        )));
    }
    Ok(())
}
