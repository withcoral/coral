//! Source table functions for manifest-driven HTTP-backed sources.
//!
//! Binding runs when Coral resolves a parked source-function call — at
//! relation planning for fully-literal calls, or in the analyzer once query
//! parameters are bound. It validates the positional call arguments and
//! captures them as HTTP request values. The returned table provider is
//! scanned later during execution, using the same `http_json_exec` path as
//! manifest-backed tables.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use coral_spec::{ManifestDataType, SourceTableFunctionSpec};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::SourceObservationSurfaceKind;
use crate::backends::http::HttpSourceClient;
use crate::backends::http::provider::{HttpJsonExecRequest, http_json_exec};
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::schema_from_columns;
use crate::backends::shared::source_observation::SourceObservationPublishers;
use crate::backends::{BoundSourceFunctionArg, SourceFunctionProviderFactory};

struct FunctionCallContext<'a> {
    source_schema: &'a str,
    function_name: &'a str,
}

/// Immutable execution state shared by every invocation of one registered HTTP
/// table function.
struct HttpSourceFunctionState {
    backend: HttpSourceClient,
    source_schema: String,
    function_name: String,
    target: Arc<HttpFetchTarget>,
    schema: SchemaRef,
    source_observation_publishers: SourceObservationPublishers,
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
        source_observation_publishers: SourceObservationPublishers,
    ) -> Result<Self> {
        let schema =
            schema_from_columns(&function.columns, &source_schema, &function.function_name)?;
        let target = HttpFetchTarget::from_function(&function);
        let function_name = function.function_name.clone();
        Ok(Self {
            spec: Arc::new(function),
            state: Arc::new(HttpSourceFunctionState {
                backend,
                source_schema,
                function_name,
                target: Arc::new(target),
                schema,
                source_observation_publishers,
            }),
        })
    }
}

impl SourceFunctionProviderFactory for HttpSourceTableFunction {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.state.schema)
    }

    fn provider_for_args(&self, args: &[BoundSourceFunctionArg]) -> Result<Arc<dyn TableProvider>> {
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
            request_filter_values: HashMap::new(),
            local_filter_values: HashMap::new(),
            active_filter_values: HashMap::new(),
            has_residual_filters: false,
            arg_values: self.arg_values.clone(),
            projection,
            limit,
            surface_kind: SourceObservationSurfaceKind::Function,
            source_observation_publishers: Arc::clone(&self.state.source_observation_publishers),
        })
    }
}

fn bind_function_args(
    source_schema: &str,
    function: &SourceTableFunctionSpec,
    args: &[BoundSourceFunctionArg],
) -> Result<HashMap<String, String>> {
    let context = FunctionCallContext {
        source_schema,
        function_name: function.function_name.as_str(),
    };
    ensure_no_extra_args(&context, function.args.len(), args.len())?;

    let mut required_missing = Vec::new();
    let mut arg_values = HashMap::with_capacity(function.args.len());

    for (index, spec) in function.args.iter().enumerate() {
        let Some(value) = args.get(index).and_then(Option::as_ref) else {
            if spec.required {
                required_missing.push(spec.name.as_str());
            }
            continue;
        };
        ensure_call_arg_allowed_value(
            &context,
            spec.name.as_str(),
            &value.source_text,
            &spec.values,
        )?;
        let value = match (spec.data_type, &value.value) {
            (ManifestDataType::Json, value) => value.to_string(),
            (_, serde_json::Value::String(value)) => value.clone(),
            (_, value) => value.to_string(),
        };
        arg_values.insert(spec.bind.arg.clone(), value);
    }

    if !required_missing.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{}.{} missing required argument(s): {}",
            context.source_schema,
            context.function_name,
            required_missing.join(", ")
        )));
    }

    Ok(arg_values)
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
