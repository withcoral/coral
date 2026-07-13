//! UDF SQL signature inference orchestration.

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
    let signature = query_runtime.infer_sql_signature(udf.sql()).await?;
    let mut arguments = Vec::new();
    for (placeholder, field) in signature.parameter_fields {
        let name = placeholder
            .strip_prefix('$')
            .unwrap_or(&placeholder)
            .to_string();
        let Some(field) = field else {
            return Err(CoreError::InvalidInput(format!(
                "udf '{}' SQL parameter '{}' has no inferred type; cast it in SQL, for example CAST({} AS VARCHAR)",
                udf.name, placeholder, placeholder
            )));
        };
        let data_type = match signature.declared_parameter_types.get(&placeholder) {
            Some(declared) => *declared,
            None => {
                crate::types::manifest_data_type_for_arrow(field.data_type()).ok_or_else(|| {
                    CoreError::InvalidInput(format!(
                        "udf '{}' SQL parameter '{}' inferred unsupported type {}",
                        udf.name,
                        placeholder,
                        field.data_type()
                    ))
                })?
            }
        };
        arguments.push(UdfRuntimeArgument { name, data_type });
    }
    arguments.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(UdfRuntimeSignature {
        arguments,
        result_columns: result_columns(signature.planned_schema.as_ref()),
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
