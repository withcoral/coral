use std::collections::HashMap;
use std::fmt::Display;

use coral_spec::TableFunctionArgSpec;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;

pub(crate) struct FunctionCallContext<'a> {
    pub(crate) source_schema: &'a str,
    pub(crate) function_name: &'a str,
}

pub(crate) fn bind_table_function_args<T>(
    context: &FunctionCallContext<'_>,
    specs: &[TableFunctionArgSpec],
    args: &[Expr],
    literal_value: impl Fn(&Expr) -> Option<T>,
    allowed_value: impl Fn(&T) -> String,
    missing_required_error: impl Fn(&FunctionCallContext<'_>, Vec<String>) -> DataFusionError,
) -> Result<HashMap<String, T>>
where
    T: Display,
{
    ensure_no_extra_args(context, specs.len(), args.len())?;

    let mut required_missing = Vec::new();
    let mut arg_values = HashMap::with_capacity(specs.len());

    for (index, spec) in specs.iter().enumerate() {
        let Some(value) =
            resolve_call_arg_literal(context, spec.name.as_str(), args.get(index), &literal_value)?
        else {
            if spec.required {
                required_missing.push(spec.name.clone());
            }
            continue;
        };
        ensure_call_arg_allowed_value(
            context,
            spec.name.as_str(),
            &value,
            &spec.values,
            &allowed_value,
        )?;
        arg_values.insert(spec.bind.arg.clone(), value);
    }

    if required_missing.is_empty() {
        Ok(arg_values)
    } else {
        Err(missing_required_error(context, required_missing))
    }
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

fn resolve_call_arg_literal<T>(
    context: &FunctionCallContext<'_>,
    arg_name: &str,
    expr: Option<&Expr>,
    literal_value: impl Fn(&Expr) -> Option<T>,
) -> Result<Option<T>> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    if is_null_literal(expr) {
        return Ok(None);
    }
    let Some(value) = literal_value(expr) else {
        return Err(DataFusionError::Plan(format!(
            "{}.{} argument '{}' must be a literal",
            context.source_schema, context.function_name, arg_name
        )));
    };
    Ok(Some(value))
}

fn is_null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Cast(cast) => is_null_literal(cast.expr.as_ref()),
        Expr::TryCast(cast) => is_null_literal(cast.expr.as_ref()),
        _ => false,
    }
}

fn ensure_call_arg_allowed_value<T>(
    context: &FunctionCallContext<'_>,
    arg: &str,
    value: &T,
    allowed_values: &[String],
    allowed_value: impl Fn(&T) -> String,
) -> Result<()>
where
    T: Display,
{
    let comparable_value = allowed_value(value);
    if !allowed_values.is_empty()
        && !allowed_values
            .iter()
            .any(|allowed| allowed == comparable_value.as_str())
    {
        return Err(DataFusionError::Plan(format!(
            "{}.{} argument '{arg}' has invalid value '{value}'; expected one of: {}",
            context.source_schema,
            context.function_name,
            allowed_values.join(", ")
        )));
    }
    Ok(())
}
