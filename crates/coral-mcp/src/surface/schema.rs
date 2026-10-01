use std::sync::Arc;

use schemars::JsonSchema;
use serde_json::{Map, Value};

pub(crate) fn tool_input_schema<T>() -> Arc<Map<String, Value>>
where
    T: JsonSchema + std::any::Any,
{
    rmcp::handler::server::tool::schema_for_input::<T>().unwrap_or_else(|error| {
        panic!(
            "invalid MCP input schema for {}: {error}",
            std::any::type_name::<T>()
        )
    })
}

pub(crate) fn tool_output_schema<T>() -> Arc<Map<String, Value>>
where
    T: JsonSchema + std::any::Any,
{
    rmcp::handler::server::tool::schema_for_output::<T>().unwrap_or_else(|error| {
        panic!(
            "invalid MCP output schema for {}: {error}",
            std::any::type_name::<T>()
        )
    })
}
