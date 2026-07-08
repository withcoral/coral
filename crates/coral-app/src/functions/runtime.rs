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
    let mut runtime_function = runtime_function_without_signature(spec);
    let sql_definition = runtime_sql_definition(&runtime_function);
    let signature =
        CoralQuery::infer_udf_signature(selected_sources, runtime_config, sql_definition)
            .await
            .map_err(|error| {
                AppError::FailedPrecondition(format!("function failed runtime validation: {error}"))
            })?;
    runtime_function.arguments = signature.arguments;
    runtime_function.result_columns = signature.result_columns;
    Ok(runtime_function)
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
