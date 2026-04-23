//! Concrete `DataFusion` runtime assembly for the data plane.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result as DataFusionResult};
use datafusion::execution::FunctionRegistry;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::expr::{Cast, Expr, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use datafusion_functions_json::udfs::{
    json_as_text_udf, json_contains_udf, json_from_scalar_udf, json_get_array_udf,
    json_get_bool_udf, json_get_float_udf, json_get_int_udf, json_get_json_udf, json_get_str_udf,
    json_get_udf, json_length_udf, json_object_keys_udf,
};

use crate::backends::compile_query_source;
use crate::runtime::catalog;
use crate::runtime::error::datafusion_to_core;
use crate::runtime::registry::{
    CompiledQuerySource, SourceRegistrationCandidate, SourceRegistrationFailure, register_sources,
};
use crate::{
    CoreError, EngineExtensions, QueryExecution, QueryRuntimeProvider, QuerySource, TableInfo,
};

pub(crate) struct QueryRuntimeAdapter {
    ctx: Arc<SessionContext>,
    tables: Vec<TableInfo>,
    failures: Vec<SourceRegistrationFailure>,
}

pub(crate) async fn build_runtime(
    sources: &[QuerySource],
    runtime: &dyn QueryRuntimeProvider,
) -> Result<QueryRuntimeAdapter, CoreError> {
    let session_config = SessionConfig::new().with_information_schema(true);
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_object_list_cache_limit(0)
            .build()
            .map_err(|err| datafusion_to_core(&err))?,
    );
    let mut ctx = SessionContext::new_with_config_rt(session_config, runtime_env);
    register_json_udfs(&mut ctx).map_err(|err| datafusion_to_core(&err))?;
    let ctx = Arc::new(ctx);

    let runtime_context = runtime.runtime_context();
    let mut build_options: EngineExtensions = runtime.engine_extensions();
    let mut source_candidates = Vec::new();
    for source in sources {
        match compile_query_source(source, &runtime_context) {
            Ok(compiled) => {
                source_candidates.push(SourceRegistrationCandidate::Compiled(
                    CompiledQuerySource {
                        source: source.clone(),
                        compiled,
                    },
                ));
            }
            Err(error) => source_candidates.push(SourceRegistrationCandidate::CompileFailed {
                source: source.clone(),
                error,
            }),
        }
    }
    let registration = register_sources(
        &ctx,
        source_candidates,
        build_options.source_decorators.as_mut_slice(),
    )
    .await?;
    catalog::register(&ctx, &registration.active_sources)
        .map_err(|err| datafusion_to_core(&err))?;
    let tables = catalog::collect_tables(&registration.active_sources);
    for failure in &registration.failures {
        tracing::warn!(
            source = %failure.schema_name,
            detail = %failure.detail,
            "skipping source during runtime build"
        );
    }

    Ok(QueryRuntimeAdapter {
        ctx,
        tables,
        failures: registration.failures,
    })
}

impl QueryRuntimeAdapter {
    pub(crate) fn list_tables(&self, source_filter: Option<&str>) -> Vec<TableInfo> {
        self.tables
            .iter()
            .filter(|table| source_filter.is_none_or(|value| table.schema_name == value))
            .cloned()
            .collect()
    }

    pub(crate) fn registration_failure(
        &self,
        source_name: &str,
    ) -> Option<&SourceRegistrationFailure> {
        self.failures
            .iter()
            .find(|failure| failure.schema_name == source_name)
    }

    pub(crate) async fn execute_sql(&self, sql: &str) -> Result<QueryExecution, CoreError> {
        let df = self
            .ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .map_err(|err| datafusion_to_core(&err))?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await.map_err(|err| datafusion_to_core(&err))?;
        Ok(QueryExecution::new(arrow_schema, batches))
    }
}

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn register_json_udfs(registry: &mut dyn FunctionRegistry) -> datafusion::common::Result<()> {
    let functions: [Arc<ScalarUDF>; 12] = [
        json_get_udf(),
        json_get_bool_udf(),
        json_get_float_udf(),
        json_get_int_udf(),
        json_get_json_udf(),
        json_get_array_udf(),
        json_as_text_udf(),
        json_get_str_udf(),
        json_contains_udf(),
        json_length_udf(),
        json_object_keys_udf(),
        json_from_scalar_udf(),
    ];
    for udf in functions {
        registry.register_udf(udf)?;
    }
    registry.register_function_rewrite(Arc::new(JsonFunctionRewriter))?;
    Ok(())
}

#[derive(Debug)]
struct JsonFunctionRewriter;

impl FunctionRewrite for JsonFunctionRewriter {
    fn name(&self) -> &'static str {
        "JsonFunctionRewriter"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> DataFusionResult<Transformed<Expr>> {
        let transform = match &expr {
            Expr::Cast(cast) => optimise_json_get_cast(cast),
            Expr::ScalarFunction(func) => unnest_json_calls(func),
            _ => None,
        };
        Ok(transform.unwrap_or_else(|| Transformed::no(expr)))
    }
}

fn optimise_json_get_cast(cast: &Cast) -> Option<Transformed<Expr>> {
    let scalar_func = extract_scalar_function(&cast.expr)?;
    if scalar_func.func.name() != "json_get" {
        return None;
    }
    let func = match &cast.data_type {
        DataType::Boolean => json_get_bool_udf(),
        DataType::Float64
        | DataType::Float32
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => json_get_float_udf(),
        DataType::Int64 | DataType::Int32 => json_get_int_udf(),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => json_get_str_udf(),
        _ => return None,
    };
    Some(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
        func,
        args: scalar_func.args.clone(),
    })))
}

fn unnest_json_calls(func: &ScalarFunction) -> Option<Transformed<Expr>> {
    if !matches!(
        func.func.name(),
        "json_get"
            | "json_get_bool"
            | "json_get_float"
            | "json_get_int"
            | "json_get_json"
            | "json_get_str"
            | "json_as_text"
    ) {
        return None;
    }
    let mut outer_args_iter = func.args.iter();
    let first_arg = outer_args_iter.next()?;
    let inner_func = extract_scalar_function(first_arg)?;

    if !matches!(inner_func.func.name(), "json_get" | "json_as_text") {
        return None;
    }

    let mut args = inner_func.args.clone();
    args.extend(outer_args_iter.cloned());
    if args
        .iter()
        .skip(1)
        .all(|arg| matches!(arg, Expr::Literal(_, _)))
    {
        Some(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
            func: func.func.clone(),
            args,
        })))
    } else {
        None
    }
}

fn extract_scalar_function(expr: &Expr) -> Option<&ScalarFunction> {
    match expr {
        Expr::ScalarFunction(func) => Some(func),
        Expr::Alias(alias) => extract_scalar_function(&alias.expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datafusion_to_core_unwraps_context_wrapped_schema_error_to_invalid_input() {
        use datafusion::common::{Column, SchemaError};
        use datafusion::error::DataFusionError;

        let schema_err = Box::new(SchemaError::FieldNotFound {
            field: Box::new(Column::new_unqualified("user_login")),
            valid_fields: vec![
                Column::new_unqualified("user__login"),
                Column::new_unqualified("title"),
            ],
        });
        let inner = DataFusionError::SchemaError(schema_err, Box::new(None));
        let wrapped = DataFusionError::Context("wrapping context".to_string(), Box::new(inner));

        let core = datafusion_to_core(&wrapped);

        match core {
            CoreError::InvalidInput(msg) => {
                assert!(msg.contains("user_login"), "expected field name in: {msg}");
            }
            other => panic!("expected CoreError::InvalidInput, got {other:?}"),
        }
    }
}
