//! Recipe SQL parameter binding helpers.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr as _;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};

use crate::backends::common::{
    RegisteredTableFunctionArgument, RegisteredTableFunctionResultColumn,
};
use crate::backends::{RegisteredTableFunction, SourceFunctionProviderFactory};
use crate::runtime::query::QueryRuntimeAdapter;
use crate::runtime::query::{parameter_scalar_value, reject_unknown_parameters};
use crate::{
    CoreError, QueryParameterValue, QueryParameters, RecipeRuntimeArgument,
    RecipeRuntimeArgumentType, RecipeRuntimeArgumentValue, RecipeRuntimeDefinition,
    RecipeRuntimeImplementation, RecipeRuntimePublish, RecipeRuntimeResultColumn,
};

pub(crate) fn recipe_sql(recipe: &RecipeRuntimeDefinition) -> &str {
    let RecipeRuntimeImplementation::CoralSql { query } = &recipe.implementation;
    query
}

pub(crate) fn recipe_query_parameters(
    recipe: &RecipeRuntimeDefinition,
    arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<QueryParameters> {
    reject_unknown_arguments(recipe, arguments)?;

    recipe
        .arguments
        .iter()
        .map(|argument| {
            let value = arguments.get(&argument.name);
            let query_value = recipe_query_parameter(&recipe.name, argument, value)?;
            Ok((argument.name.clone(), query_value))
        })
        .collect()
}

pub(crate) async fn infer_recipe_schema(
    query_runtime: &QueryRuntimeAdapter,
    recipe: &RecipeRuntimeDefinition,
) -> Result<std::sync::Arc<Schema>, CoreError> {
    let (sample_schema, sample_values) = infer_recipe_sample_schema(query_runtime, recipe).await?;
    let Some(null_values) = recipe_optional_null_values(recipe, &sample_values) else {
        return Ok(sample_schema);
    };
    let null_schema = infer_recipe_schema_with_values(query_runtime, recipe, &null_values).await?;
    merge_recipe_inferred_schemas(
        recipe.name.as_str(),
        sample_schema.as_ref(),
        null_schema.as_ref(),
    )
}

pub(crate) fn published_table_functions(
    recipes: &[RecipeRuntimeDefinition],
) -> Result<Vec<RegisteredTableFunction>> {
    let mut functions = Vec::new();
    let mut seen = BTreeSet::new();

    for recipe in recipes {
        for publish in &recipe.publish {
            let RecipeRuntimePublish::TableFunction {
                schema,
                name,
                description,
            } = publish;
            let key = (schema.clone(), name.clone());
            if !seen.insert(key.clone()) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate recipe table function {}.{}",
                    key.0, key.1
                )));
            }

            functions.push(RegisteredTableFunction {
                schema_name: schema.clone(),
                function_name: name.clone(),
                factory: Arc::new(RecipeTableFunctionFactory::new(recipe)?),
                kind: "recipe".to_string(),
                description: publish_description(description, &recipe.description),
                arguments: recipe
                    .arguments
                    .iter()
                    .map(|argument| RegisteredTableFunctionArgument {
                        name: argument.name.clone(),
                        required: argument.required,
                        values: Vec::new(),
                    })
                    .collect(),
                result_columns: recipe
                    .result_columns
                    .iter()
                    .map(|column| RegisteredTableFunctionResultColumn {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        description: column.description.clone(),
                    })
                    .collect(),
                arg_names: recipe
                    .arguments
                    .iter()
                    .map(|argument| argument.name.clone())
                    .collect(),
                search_limits_json: None,
            });
        }
    }

    functions.sort_by(|left, right| {
        (&left.schema_name, &left.function_name).cmp(&(&right.schema_name, &right.function_name))
    });
    Ok(functions)
}

#[derive(Debug, Clone)]
struct RecipeTableFunctionFactory {
    recipe: RecipeRuntimeDefinition,
    schema: Arc<Schema>,
}

impl RecipeTableFunctionFactory {
    fn new(recipe: &RecipeRuntimeDefinition) -> Result<Self> {
        Ok(Self {
            recipe: recipe.clone(),
            schema: recipe_arrow_schema(recipe)?,
        })
    }
}

impl SourceFunctionProviderFactory for RecipeTableFunctionFactory {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn provider_for_args(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let arguments = recipe_argument_values(&self.recipe, args)?;
        let params = recipe_query_parameters(&self.recipe, &arguments)?;
        Ok(Arc::new(RecipeSqlTableProvider {
            sql: recipe_sql(&self.recipe).to_string(),
            params,
            schema: Arc::clone(&self.schema),
        }))
    }
}

#[derive(Debug)]
struct RecipeSqlTableProvider {
    sql: String,
    params: QueryParameters,
    schema: Arc<Schema>,
}

#[async_trait]
impl TableProvider for RecipeSqlTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session_state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Plan("recipe execution requires SessionState".to_string())
            })?;
        let logical_plan = session_state.create_logical_plan(&self.sql).await?;
        reject_unknown_parameters(&logical_plan, &self.params)?;
        let logical_plan = logical_plan.with_param_values(recipe_param_values(&self.params))?;
        let plan = state.create_physical_plan(&logical_plan).await?;
        project_recipe_plan(plan, projection)
    }
}

fn recipe_argument_values(
    recipe: &RecipeRuntimeDefinition,
    args: &[Expr],
) -> Result<BTreeMap<String, RecipeRuntimeArgumentValue>> {
    if args.len() > recipe.arguments.len() {
        return Err(DataFusionError::Plan(format!(
            "recipe '{}' expected at most {} arguments, got {}",
            recipe.name,
            recipe.arguments.len(),
            args.len()
        )));
    }

    recipe
        .arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            let value = args
                .get(index)
                .map(|expr| recipe_argument_value(recipe, argument, expr))
                .transpose()?
                .unwrap_or(RecipeRuntimeArgumentValue::Null);
            Ok((argument.name.clone(), value))
        })
        .collect()
}

fn recipe_argument_value(
    recipe: &RecipeRuntimeDefinition,
    argument: &RecipeRuntimeArgument,
    expr: &Expr,
) -> Result<RecipeRuntimeArgumentValue> {
    let Expr::Literal(value, _) = expr else {
        return Err(DataFusionError::Plan(format!(
            "recipe '{}' argument '{}' must be a literal after parameter binding",
            recipe.name, argument.name
        )));
    };
    scalar_recipe_argument_value(value).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "recipe '{}' argument '{}' expected {}, got {}",
            recipe.name,
            argument.name,
            argument_type_name(argument.data_type),
            scalar_value_name(value)
        ))
    })
}

fn scalar_recipe_argument_value(value: &ScalarValue) -> Option<RecipeRuntimeArgumentValue> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::String(value.clone()))
        }
        ScalarValue::Int64(Some(value)) => Some(RecipeRuntimeArgumentValue::Integer(*value)),
        ScalarValue::Int32(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::Int16(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::Int8(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value)
            .ok()
            .map(RecipeRuntimeArgumentValue::Integer),
        ScalarValue::UInt32(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::UInt16(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::UInt8(Some(value)) => {
            Some(RecipeRuntimeArgumentValue::Integer(i64::from(*value)))
        }
        ScalarValue::Boolean(Some(value)) => Some(RecipeRuntimeArgumentValue::Boolean(*value)),
        value if value.is_null() => Some(RecipeRuntimeArgumentValue::Null),
        _ => None,
    }
}

fn recipe_param_values(params: &QueryParameters) -> Vec<(String, ScalarValue)> {
    params
        .iter()
        .map(|(name, value)| (name.clone(), parameter_scalar_value(value)))
        .collect()
}

fn recipe_arrow_schema(recipe: &RecipeRuntimeDefinition) -> Result<Arc<Schema>> {
    if recipe.result_columns.is_empty()
        && recipe
            .publish
            .iter()
            .any(|publish| matches!(publish, RecipeRuntimePublish::TableFunction { .. }))
    {
        return Err(DataFusionError::Plan(format!(
            "published recipe '{}' requires inferred result columns",
            recipe.name
        )));
    }

    let fields = recipe
        .result_columns
        .iter()
        .map(recipe_result_field)
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn recipe_result_field(column: &RecipeRuntimeResultColumn) -> Result<Field> {
    let data_type = DataType::from_str(&column.data_type).map_err(|error| {
        DataFusionError::Plan(format!(
            "recipe result column '{}' has unsupported inferred type '{}': {error}",
            column.name, column.data_type
        ))
    })?;
    Ok(Field::new(&column.name, data_type, column.nullable))
}

fn project_recipe_plan(
    plan: Arc<dyn ExecutionPlan>,
    projection: Option<&Vec<usize>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(projection) = projection else {
        return Ok(plan);
    };
    let input_schema = plan.schema();
    let mut exprs = Vec::with_capacity(projection.len());
    for &index in projection {
        let field = input_schema.fields().get(index).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "recipe projection index {index} out of bounds for {} column(s)",
                input_schema.fields().len()
            ))
        })?;
        exprs.push(ProjectionExpr {
            expr: Arc::new(Column::new(field.name(), index)),
            alias: field.name().clone(),
        });
    }
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}

fn publish_description(target_description: &str, recipe_description: &str) -> String {
    if target_description.trim().is_empty() {
        recipe_description.to_string()
    } else {
        target_description.to_string()
    }
}

fn scalar_value_name(value: &ScalarValue) -> &'static str {
    match value {
        ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) => "string",
        ScalarValue::Int64(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int8(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt8(_) => "integer",
        ScalarValue::Boolean(_) => "boolean",
        value if value.is_null() => "null",
        _ => "unsupported literal",
    }
}

async fn infer_recipe_sample_schema(
    query_runtime: &QueryRuntimeAdapter,
    recipe: &RecipeRuntimeDefinition,
) -> Result<
    (
        std::sync::Arc<Schema>,
        BTreeMap<String, RecipeRuntimeArgumentValue>,
    ),
    CoreError,
> {
    let mut sample_values = recipe_sample_values(recipe);
    let max_attempts = sample_values.len() + 1;
    let mut attempts = 0;
    loop {
        attempts += 1;
        match infer_recipe_schema_with_values(query_runtime, recipe, &sample_values).await {
            Ok(schema) => return Ok((schema, sample_values)),
            Err(error)
                if attempts < max_attempts
                    && update_recipe_sample_from_allowed_value(&error, &mut sample_values) => {}
            Err(error) => return Err(error),
        }
    }
}

async fn infer_recipe_schema_with_values(
    query_runtime: &QueryRuntimeAdapter,
    recipe: &RecipeRuntimeDefinition,
    values: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<std::sync::Arc<Schema>, CoreError> {
    let params = recipe_query_parameters(recipe, values)
        .map_err(|error| CoreError::InvalidInput(error.to_string()))?;
    query_runtime
        .infer_sql_schema(recipe_sql(recipe), &params)
        .await
}

fn recipe_sample_values(
    recipe: &RecipeRuntimeDefinition,
) -> BTreeMap<String, RecipeRuntimeArgumentValue> {
    recipe
        .arguments
        .iter()
        .map(|argument| {
            (
                argument.name.clone(),
                recipe_sample_value(argument.data_type, &argument.name),
            )
        })
        .collect()
}

fn recipe_optional_null_values(
    recipe: &RecipeRuntimeDefinition,
    sample_values: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Option<BTreeMap<String, RecipeRuntimeArgumentValue>> {
    if recipe.arguments.iter().all(|argument| argument.required) {
        return None;
    }
    Some(
        recipe
            .arguments
            .iter()
            .map(|argument| {
                let value = if argument.required {
                    sample_values
                        .get(&argument.name)
                        .cloned()
                        .unwrap_or_else(|| recipe_sample_value(argument.data_type, &argument.name))
                } else {
                    RecipeRuntimeArgumentValue::Null
                };
                (argument.name.clone(), value)
            })
            .collect(),
    )
}

fn recipe_sample_value(
    data_type: RecipeRuntimeArgumentType,
    argument_name: &str,
) -> RecipeRuntimeArgumentValue {
    match data_type {
        RecipeRuntimeArgumentType::String => {
            RecipeRuntimeArgumentValue::String(format!("__coral_recipe_sample_{argument_name}"))
        }
        RecipeRuntimeArgumentType::Integer => RecipeRuntimeArgumentValue::Integer(0),
        RecipeRuntimeArgumentType::Boolean => RecipeRuntimeArgumentValue::Boolean(false),
    }
}

fn update_recipe_sample_from_allowed_value(
    error: &CoreError,
    sample_values: &mut BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> bool {
    let message = error.to_string();
    let Some(allowed_value) = first_allowed_source_function_value(&message) else {
        return false;
    };
    for sample in sample_values.values_mut() {
        if message.contains(sample_error_value(sample).as_str())
            && let Some(value) = parse_allowed_value_for_sample(sample, allowed_value)
        {
            *sample = value;
            return true;
        }
    }
    false
}

fn sample_error_value(sample: &RecipeRuntimeArgumentValue) -> String {
    match sample {
        RecipeRuntimeArgumentValue::String(value) => value.clone(),
        RecipeRuntimeArgumentValue::Integer(value) => value.to_string(),
        RecipeRuntimeArgumentValue::Boolean(value) => value.to_string(),
        RecipeRuntimeArgumentValue::Null => "NULL".to_string(),
    }
}

fn parse_allowed_value_for_sample(
    sample: &RecipeRuntimeArgumentValue,
    allowed_value: &str,
) -> Option<RecipeRuntimeArgumentValue> {
    match sample {
        RecipeRuntimeArgumentValue::String(_) => Some(RecipeRuntimeArgumentValue::String(
            allowed_value.to_string(),
        )),
        RecipeRuntimeArgumentValue::Integer(_) => allowed_value
            .parse()
            .ok()
            .map(RecipeRuntimeArgumentValue::Integer),
        RecipeRuntimeArgumentValue::Boolean(_) => allowed_value
            .parse()
            .ok()
            .map(RecipeRuntimeArgumentValue::Boolean),
        RecipeRuntimeArgumentValue::Null => None,
    }
}

fn first_allowed_source_function_value(message: &str) -> Option<&str> {
    let (_, values) = message.split_once("expected one of: ")?;
    values
        .split([',', '\n'])
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn merge_recipe_inferred_schemas(
    recipe_name: &str,
    sample_schema: &Schema,
    null_schema: &Schema,
) -> Result<std::sync::Arc<Schema>, CoreError> {
    if sample_schema.fields().len() != null_schema.fields().len() {
        return Err(CoreError::FailedPrecondition(format!(
            "recipe '{recipe_name}' inferred different result column counts for sample and omitted optional arguments"
        )));
    }

    let fields = sample_schema
        .fields()
        .iter()
        .zip(null_schema.fields())
        .map(|(sample, null)| {
            if sample.name() != null.name()
                || !compatible_recipe_data_type(sample.data_type(), null.data_type())
            {
                return Err(CoreError::FailedPrecondition(format!(
                    "recipe '{recipe_name}' inferred incompatible result column '{}' for omitted optional arguments",
                    sample.name()
                )));
            }
            Ok(Field::new(
                sample.name(),
                sample.data_type().clone(),
                sample.is_nullable() || null.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    Ok(std::sync::Arc::new(Schema::new(fields)))
}

fn compatible_recipe_data_type(left: &DataType, right: &DataType) -> bool {
    left == right || matches!(left, DataType::Null) || matches!(right, DataType::Null)
}

fn reject_unknown_arguments(
    recipe: &RecipeRuntimeDefinition,
    arguments: &BTreeMap<String, RecipeRuntimeArgumentValue>,
) -> Result<()> {
    let declared_arguments = recipe
        .arguments
        .iter()
        .map(|argument| argument.name.as_str())
        .collect::<BTreeSet<_>>();

    if let Some(argument_name) = arguments
        .keys()
        .find(|argument_name| !declared_arguments.contains(argument_name.as_str()))
    {
        return Err(DataFusionError::Plan(format!(
            "recipe '{}' received unknown argument '{}'",
            recipe.name, argument_name
        )));
    }

    Ok(())
}

fn recipe_query_parameter(
    recipe_name: &str,
    argument: &RecipeRuntimeArgument,
    value: Option<&RecipeRuntimeArgumentValue>,
) -> Result<QueryParameterValue> {
    match value {
        Some(RecipeRuntimeArgumentValue::String(value))
            if argument.data_type == RecipeRuntimeArgumentType::String =>
        {
            Ok(QueryParameterValue::String(value.clone()))
        }
        Some(RecipeRuntimeArgumentValue::Integer(value))
            if argument.data_type == RecipeRuntimeArgumentType::Integer =>
        {
            Ok(QueryParameterValue::Integer(*value))
        }
        Some(RecipeRuntimeArgumentValue::Boolean(value))
            if argument.data_type == RecipeRuntimeArgumentType::Boolean =>
        {
            Ok(QueryParameterValue::Boolean(*value))
        }
        Some(RecipeRuntimeArgumentValue::Null) if argument.required => {
            Err(DataFusionError::Plan(format!(
                "recipe '{}' argument '{}' is required and cannot be null",
                recipe_name, argument.name
            )))
        }
        Some(RecipeRuntimeArgumentValue::Null) | None if !argument.required => {
            Ok(QueryParameterValue::Null)
        }
        None => Err(DataFusionError::Plan(format!(
            "recipe '{}' is missing required argument '{}'",
            recipe_name, argument.name
        ))),
        Some(value) => Err(DataFusionError::Plan(format!(
            "recipe '{}' argument '{}' expected {}, got {}",
            recipe_name,
            argument.name,
            argument_type_name(argument.data_type),
            argument_value_name(value)
        ))),
    }
}

fn argument_type_name(data_type: RecipeRuntimeArgumentType) -> &'static str {
    match data_type {
        RecipeRuntimeArgumentType::String => "string",
        RecipeRuntimeArgumentType::Integer => "integer",
        RecipeRuntimeArgumentType::Boolean => "boolean",
    }
}

fn argument_value_name(value: &RecipeRuntimeArgumentValue) -> &'static str {
    match value {
        RecipeRuntimeArgumentValue::String(_) => "string",
        RecipeRuntimeArgumentValue::Integer(_) => "integer",
        RecipeRuntimeArgumentValue::Boolean(_) => "boolean",
        RecipeRuntimeArgumentValue::Null => "null",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recipe() -> RecipeRuntimeDefinition {
        RecipeRuntimeDefinition {
            name: "open_pull_requests".to_string(),
            description: String::new(),
            arguments: vec![
                RecipeRuntimeArgument {
                    name: "author".to_string(),
                    data_type: RecipeRuntimeArgumentType::String,
                    required: true,
                    description: String::new(),
                },
                RecipeRuntimeArgument {
                    name: "limit".to_string(),
                    data_type: RecipeRuntimeArgumentType::Integer,
                    required: false,
                    description: String::new(),
                },
                RecipeRuntimeArgument {
                    name: "draft".to_string(),
                    data_type: RecipeRuntimeArgumentType::Boolean,
                    required: false,
                    description: String::new(),
                },
            ],
            implementation: RecipeRuntimeImplementation::CoralSql {
                query: "select * from github.pull_requests where author = $author".to_string(),
            },
            publish: Vec::new(),
            result_columns: Vec::new(),
        }
    }

    #[test]
    fn recipe_sql_returns_coral_sql_body() {
        let recipe = recipe();

        assert_eq!(
            recipe_sql(&recipe),
            "select * from github.pull_requests where author = $author"
        );
    }

    #[test]
    fn recipe_query_parameters_binds_declared_arguments() {
        let recipe = recipe();
        let arguments = BTreeMap::from([
            (
                "author".to_string(),
                RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
            ),
            ("limit".to_string(), RecipeRuntimeArgumentValue::Integer(25)),
            (
                "draft".to_string(),
                RecipeRuntimeArgumentValue::Boolean(false),
            ),
        ]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments).unwrap(),
            QueryParameters::from([
                (
                    "author".to_string(),
                    QueryParameterValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit".to_string(), QueryParameterValue::Integer(25)),
                ("draft".to_string(), QueryParameterValue::Boolean(false)),
            ])
        );
    }

    #[test]
    fn recipe_query_parameters_binds_missing_optional_arguments_as_nulls() {
        let recipe = recipe();
        let arguments = BTreeMap::from([(
            "author".to_string(),
            RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
        )]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments).unwrap(),
            QueryParameters::from([
                (
                    "author".to_string(),
                    QueryParameterValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit".to_string(), QueryParameterValue::Null),
                ("draft".to_string(), QueryParameterValue::Null),
            ])
        );
    }

    #[test]
    fn recipe_query_parameters_binds_explicit_optional_nulls_as_nulls() {
        let recipe = recipe();
        let arguments = BTreeMap::from([
            (
                "author".to_string(),
                RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
            ),
            ("limit".to_string(), RecipeRuntimeArgumentValue::Null),
            ("draft".to_string(), RecipeRuntimeArgumentValue::Null),
        ]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments).unwrap(),
            QueryParameters::from([
                (
                    "author".to_string(),
                    QueryParameterValue::String("Bradley-Butcher".to_string()),
                ),
                ("limit".to_string(), QueryParameterValue::Null),
                ("draft".to_string(), QueryParameterValue::Null),
            ])
        );
    }

    #[test]
    fn recipe_query_parameters_rejects_unknown_arguments() {
        let recipe = recipe();
        let arguments = BTreeMap::from([
            (
                "author".to_string(),
                RecipeRuntimeArgumentValue::String("Bradley-Butcher".to_string()),
            ),
            (
                "repository".to_string(),
                RecipeRuntimeArgumentValue::String("withcoral/coral".to_string()),
            ),
        ]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: recipe 'open_pull_requests' received unknown argument 'repository'"
        );
    }

    #[test]
    fn recipe_query_parameters_rejects_missing_required_arguments() {
        let recipe = recipe();
        let arguments = BTreeMap::new();

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: recipe 'open_pull_requests' is missing required argument 'author'"
        );
    }

    #[test]
    fn recipe_query_parameters_rejects_required_nulls() {
        let recipe = recipe();
        let arguments = BTreeMap::from([("author".to_string(), RecipeRuntimeArgumentValue::Null)]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: recipe 'open_pull_requests' argument 'author' is required and cannot be null"
        );
    }

    #[test]
    fn recipe_query_parameters_rejects_type_mismatches() {
        let recipe = recipe();
        let arguments = BTreeMap::from([(
            "author".to_string(),
            RecipeRuntimeArgumentValue::Integer(42),
        )]);

        assert_eq!(
            recipe_query_parameters(&recipe, &arguments)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: recipe 'open_pull_requests' argument 'author' expected string, got integer"
        );
    }
}
