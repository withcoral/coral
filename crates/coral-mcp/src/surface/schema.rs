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

pub(crate) fn json_schema_value<T>() -> Value
where
    T: JsonSchema,
{
    let mut schema = serde_json::to_value(schemars::schema_for!(T))
        .expect("generated tool schema should serialize");
    if let Some(object) = schema.as_object_mut() {
        object.remove("$schema");
        object.remove("title");
        object.remove("description");
    }
    schema
}
