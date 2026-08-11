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

    /// `const` arrived with 3.1. A 3.0 document spelling the same thing wrote a
    /// one-element `enum`, which the shared dispatch already reads.
    fn const_enum_values(&self, _schema: &Value) -> Option<Vec<String>> {
        None
    }

    /// 3.0 is the oldest version supported here, so it has removed nothing.
    fn removed_keyword_warning(&self, _schema: &Value) -> Option<String> {
        None
    }
}
