//! UDF SQL signature inference orchestration.

use crate::runtime::parameter_inference;
use crate::runtime::query::QueryRuntimeAdapter;
use crate::{
    CoreError, UdfRuntimeArgument, UdfRuntimeResultColumn, UdfRuntimeSignature,
    UdfRuntimeSqlDefinition,
};
use arrow::datatypes::Schema;

pub(crate) async fn infer_udf_signature(
    query_runtime: &QueryRuntimeAdapter,
    udf: &UdfRuntimeSqlDefinition,
) -> Result<UdfRuntimeSignature, CoreError> {
    let planned = query_runtime.plan_sql(udf.sql()).await?;
    let mut arguments = parameter_inference::infer_parameters(&planned.plan)
        .map_err(|error| CoreError::InvalidInput(format!("udf '{}': {error}", udf.name)))?
        .into_iter()
        .map(|parameter| UdfRuntimeArgument {
            name: parameter
                .name
                .strip_prefix('$')
                .unwrap_or(&parameter.name)
                .to_string(),
            data_type: parameter.data_type,
        })
        .collect::<Vec<_>>();
    arguments.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(UdfRuntimeSignature {
        arguments,
        result_columns: result_columns(planned.schema.as_ref()),
    })
}

fn result_columns(schema: &Schema) -> Vec<UdfRuntimeResultColumn> {
    schema
        .fields()
        .iter()
        .map(|field| UdfRuntimeResultColumn {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        })
        .collect()
}
