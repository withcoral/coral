//! `DataFusion` table functions for manifest-driven HTTP-backed sources.

use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;

use coral_spec::backends::http::HttpTableSpec;
use coral_spec::{FunctionArgBinding, SourceTableFunctionSpec, TableCommon};

use crate::backends::http::HttpSourceClient;
use crate::backends::http::provider::{BoundRequestArgs, HttpSourceTableProvider};
use crate::backends::shared::filter_expr::literal_to_string;

/// Table-valued function that turns manifest-declared function args into an
/// `HTTP` table provider with pre-bound request filters.
#[derive(Debug)]
pub(crate) struct HttpSourceTableFunction {
    backend: HttpSourceClient,
    source_schema: String,
    function: SourceTableFunctionSpec,
}

impl HttpSourceTableFunction {
    pub(crate) fn new(
        backend: HttpSourceClient,
        source_schema: String,
        function: SourceTableFunctionSpec,
    ) -> Self {
        Self {
            backend,
            source_schema,
            function,
        }
    }
}

impl TableFunctionImpl for HttpSourceTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let table = function_table_spec(&self.function);
        let prebound = bind_function_args(&self.source_schema, &self.function, args)?;
        Ok(Arc::new(
            HttpSourceTableProvider::with_prebound_request_args(
                self.backend.clone(),
                self.source_schema.clone(),
                table,
                prebound,
            )?,
        ))
    }
}

fn bind_function_args(
    source_schema: &str,
    function: &SourceTableFunctionSpec,
    args: &[Expr],
) -> Result<BoundRequestArgs> {
    if args.len() > function.args.len() {
        return Err(DataFusionError::Plan(format!(
            "{}.{} expected at most {} arguments, got {}",
            source_schema,
            function.name,
            function.args.len(),
            args.len()
        )));
    }

    let mut required_missing = Vec::new();
    let mut bound = BoundRequestArgs::default();

    for fixed in &function.fixed {
        bind_value(&mut bound, &fixed.bind, &fixed.value);
    }

    for (index, spec) in function.args.iter().enumerate() {
        let Some(expr) = args.get(index) else {
            if spec.required {
                required_missing.push(spec.name.as_str());
            }
            continue;
        };
        if is_null_literal(expr) {
            if spec.required {
                required_missing.push(spec.name.as_str());
            }
            continue;
        }
        let Some(value) = literal_to_string(expr) else {
            return Err(DataFusionError::Plan(format!(
                "{}.{} argument '{}' must be a literal",
                source_schema, function.name, spec.name
            )));
        };
        validate_arg_value(
            source_schema,
            function.name.as_str(),
            spec.name.as_str(),
            &value,
            &spec.values,
        )?;
        bind_value(&mut bound, &spec.bind, &value);
    }

    if !required_missing.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{}.{} missing required argument(s): {}",
            source_schema,
            function.name,
            required_missing.join(", ")
        )));
    }

    Ok(bound)
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Null, _))
}

fn validate_arg_value(
    source_schema: &str,
    function_name: &str,
    arg: &str,
    value: &str,
    arg_values: &[String],
) -> Result<()> {
    if !arg_values.is_empty() && !arg_values.iter().any(|allowed| allowed == value) {
        return Err(DataFusionError::Plan(format!(
            "{source_schema}.{function_name} argument '{arg}' has invalid value '{value}'; expected one of: {}",
            arg_values.join(", ")
        )));
    }
    Ok(())
}

fn bind_value(bound: &mut BoundRequestArgs, binding: &FunctionArgBinding, value: &str) {
    match binding {
        FunctionArgBinding::RequestArg { arg } => {
            bound.insert_direct(arg.clone(), value.to_string());
        }
    }
}

fn function_table_spec(function: &SourceTableFunctionSpec) -> HttpTableSpec {
    HttpTableSpec {
        common: TableCommon::new(
            function.name.clone(),
            function.description.clone(),
            String::new(),
            vec![],
            function.fetch_limit_default,
            function.columns.clone(),
        ),
        request: function.request.clone(),
        requests: vec![],
        response: function.response.clone(),
        pagination: function.pagination.clone(),
    }
}
