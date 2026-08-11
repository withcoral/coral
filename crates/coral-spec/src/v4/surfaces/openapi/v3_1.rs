use serde_json::Value;

use crate::v4::surfaces::json_schema::json_schema_type_contains;

use super::dialect::OpenApiDialect;
use super::schemas::enum_value;

/// Rules for documents declaring `OpenAPI` 3.1.x.
///
/// 3.1 realigned its schema object with JSON Schema 2020-12 instead of carrying
/// its own dialect of it. Most of that realignment costs the importer nothing —
/// `$defs` resolves through the same JSON Pointer walk as any other local
/// reference, and the keywords 3.1 gained for describing binary payloads only
/// matter to file uploads, which Coral does not support. What is left is the
/// handful of spellings below.
pub(super) struct OpenApi31Importer;

impl OpenApiDialect for OpenApi31Importer {
    /// 3.1 dropped `nullable` in favour of JSON Schema's own `null` type, which
    /// a schema admits by listing it alongside the type it otherwise is.
    fn schema_nullable(&self, schema: &Value) -> bool {
        json_schema_type_contains(schema, "null")
    }

    /// `const` is 2020-12's single-value constraint, and 3.1 documents reach for
    /// it where a 3.0 one wrote a one-element `enum` — most often to pin a
    /// discriminator on each branch of a union. Read as the enum it is
    /// equivalent to, so those branches keep their fixed value.
    fn const_enum_values(&self, schema: &Value) -> Option<Vec<String>> {
        schema.get("const").map(|value| vec![enum_value(value)])
    }

    /// A 3.1 document still carrying `nullable` is usually a 3.0 one whose
    /// version string was raised without converting its schemas. Reading it
    /// would be wrong — 3.1 gives the keyword no meaning — but dropping it in
    /// silence turns every one of those columns non-nullable with nothing said,
    /// so say it.
    fn removed_keyword_warning(&self, schema: &Value) -> Option<String> {
        // Only `nullable: true` is worth reporting. `nullable: false` asked for
        // the default under 3.0 as well, so ignoring it changes nothing — and
        // real documents carry leftover `false`s that would otherwise be pure
        // noise. GitHub's own 3.1 publication has five of them and not one
        // `true`.
        (schema.get("nullable").and_then(Value::as_bool) == Some(true)).then(|| {
            "the 'nullable' keyword was removed in OpenAPI 3.1 and is ignored; list 'null' in the schema's type instead".to_string()
        })
    }
}
