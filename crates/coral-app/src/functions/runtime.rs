use coral_engine::{
    CoralQuery, QueryRuntimeConfig, QuerySource, UdfRuntimeDefinition, UdfRuntimeImplementation,
    UdfRuntimePublish, UdfRuntimeSqlDefinition, UdfRuntimeTableFunctionPublish,
};
use coral_spec::{FunctionImplementationSpec, FunctionSpec};

use crate::bootstrap::AppError;

pub(crate) async fn infer_runtime_function(
    selected_sources: &[QuerySource],
    runtime_config: QueryRuntimeConfig,
    spec: &FunctionSpec,
) -> Result<UdfRuntimeDefinition, AppError> {
    let runtime_function = runtime_function_without_signature(spec);
    let mut results =
        infer_runtime_functions(selected_sources, runtime_config, vec![runtime_function]).await?;
    results.pop().ok_or_else(|| {
        AppError::FailedPrecondition("function runtime validation returned no result".to_string())
    })?
}

pub(crate) async fn infer_runtime_functions(
    selected_sources: &[QuerySource],
    runtime_config: QueryRuntimeConfig,
    runtime_functions: Vec<UdfRuntimeDefinition>,
) -> Result<Vec<Result<UdfRuntimeDefinition, AppError>>, AppError> {
    let sql_definitions = runtime_functions
        .iter()
        .map(runtime_sql_definition)
        .collect();
    let signatures =
        CoralQuery::infer_udf_signatures(selected_sources, runtime_config, sql_definitions)
            .await
            .map_err(|error| runtime_validation_error(&error))?;

    Ok(runtime_functions
        .into_iter()
        .zip(signatures)
        .map(|(mut function, signature)| {
            let signature = signature.map_err(|error| runtime_validation_error(&error))?;
            function.arguments = signature.arguments;
            function.result_columns = signature.result_columns;
            Ok(function)
        })
        .collect())
}

fn runtime_validation_error(error: &coral_engine::CoreError) -> AppError {
    AppError::FailedPrecondition(format!("function failed runtime validation: {error}"))
}

pub(crate) fn runtime_function_without_signature(spec: &FunctionSpec) -> UdfRuntimeDefinition {
    UdfRuntimeDefinition {
        name: spec.name().to_string(),
        description: spec.description().to_string(),
        arguments: Vec::new(),
        implementation: runtime_implementation(spec.implementation()),
        publish: runtime_publish(spec),
        result_columns: Vec::new(),
    }
}

fn runtime_sql_definition(function: &UdfRuntimeDefinition) -> UdfRuntimeSqlDefinition {
    UdfRuntimeSqlDefinition {
        name: function.name.clone(),
        implementation: function.implementation.clone(),
    }
}

fn runtime_implementation(spec: &FunctionImplementationSpec) -> UdfRuntimeImplementation {
    UdfRuntimeImplementation::CoralSql {
        query: spec.coral_sql.query.clone(),
    }
}

fn runtime_publish(spec: &FunctionSpec) -> UdfRuntimePublish {
    UdfRuntimePublish {
        table_function: UdfRuntimeTableFunctionPublish {
            schema: spec.schema().to_string(),
            name: spec.name().to_string(),
            description: String::new(),
        },
    }
}
