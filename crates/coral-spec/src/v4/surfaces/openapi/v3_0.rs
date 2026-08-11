use serde_json::Value;

use super::dialect::OpenApiDialect;

/// Rules for documents declaring `OpenAPI` 3.0.x.
pub(super) struct OpenApi30Importer;

impl OpenApiDialect for OpenApi30Importer {
    /// 3.0 predates JSON Schema's `null` type and spells nullability as its own
    /// `nullable` keyword.
    fn schema_nullable(&self, schema: &Value) -> bool {
        schema
            .get("nullable")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }
}
