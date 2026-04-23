//! JSON query function registration and rewrite helpers for the runtime.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result as DataFusionResult};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::expr::{Cast, Expr, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion_functions_json::udfs::{
    json_as_text_udf, json_contains_udf, json_from_scalar_udf, json_get_array_udf,
    json_get_bool_udf, json_get_float_udf, json_get_int_udf, json_get_json_udf, json_get_str_udf,
    json_get_udf, json_length_udf, json_object_keys_udf,
};

pub(crate) fn register_json_support(
    registry: &mut dyn FunctionRegistry,
) -> datafusion::common::Result<()> {
    // We intentionally do not call `datafusion_functions_json::register_all` here
    // because it also installs the JSON expr planner, which enables `->`, `->>`,
    // and `?`. Coral exposes JSON support through functions only.
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
        // Keep the function-side conveniences from the upstream package even
        // though we skip its operator registration.
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
        // Keep decimal casts on the normal cast path. Rewriting them to
        // `json_get_float` would erase the requested decimal precision/scale.
        DataType::Float64 | DataType::Float32 => json_get_float_udf(),
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
    use arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::expr::{Cast, Expr, ScalarFunction};
    use datafusion_functions_json::udfs::json_get_udf;

    use super::optimise_json_get_cast;

    #[test]
    fn decimal_casts_are_not_rewritten_to_float() {
        let json_get = Expr::ScalarFunction(ScalarFunction {
            func: json_get_udf(),
            args: vec![
                Expr::Literal(ScalarValue::Utf8(Some("{\"amount\": 12.34}".into())), None),
                Expr::Literal(ScalarValue::Utf8(Some("amount".into())), None),
            ],
        });

        let decimal128 = Cast::new(Box::new(json_get.clone()), DataType::Decimal128(18, 2));
        let decimal256 = Cast::new(Box::new(json_get), DataType::Decimal256(18, 2));

        assert!(optimise_json_get_cast(&decimal128).is_none());
        assert!(optimise_json_get_cast(&decimal256).is_none());
    }
}
