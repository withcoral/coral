use std::collections::HashMap;

use crate::contracts::{StatusCode, StructuredQueryError};

pub(crate) fn missing_required_filter_error(
    schema: &str,
    table: &str,
    column: &str,
) -> StructuredQueryError {
    let mut metadata = HashMap::new();
    metadata.insert("schema".to_string(), schema.to_string());
    metadata.insert("table".to_string(), table.to_string());
    metadata.insert("column".to_string(), column.to_string());
    StructuredQueryError::new(
        "MISSING_REQUIRED_FILTER",
        format!("{schema}.{table} requires `WHERE {column} = <constant>`"),
        format!("{schema}.{table} requires a constant equality filter on {column}"),
        Some(format!(
            "Add a constant equality filter on `{column}` or inspect \
             `coral.columns` / `coral.tables` first."
        )),
        false,
        StatusCode::FailedPrecondition,
        metadata,
    )
}
