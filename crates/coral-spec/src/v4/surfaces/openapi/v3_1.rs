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
    ///
    /// `type` is not the only keyword that admits it, though, and a schema
    /// constraining its value can say so without naming a type at all: `{const:
    /// null}` and `{enum: [null, ...]}` each accept a null instance. Reading
    /// only `type` left those columns marked non-nullable while the document
    /// said the opposite — and for the typeless spellings, nothing else in the
    /// schema said anything about them either.
    fn schema_nullable(&self, schema: &Value) -> bool {
        json_schema_type_contains(schema, "null")
            || schema.get("const").is_some_and(Value::is_null)
            // Any null member, not only a lone one: `enum: [null, 'a']` admits
            // null exactly as `enum: [null]` does, and the values themselves are
            // what the shape dispatch reads.
            || schema
                .get("enum")
                .and_then(Value::as_array)
                .is_some_and(|values| values.iter().any(Value::is_null))
    }

    /// `const` is 2020-12's single-value constraint, and 3.1 documents reach for
    /// it where a 3.0 one wrote a one-element `enum` — most often to pin a
    /// discriminator on each branch of a union. Read as the enum it is
    /// equivalent to, so those branches keep their fixed value.
    ///
    /// Only a scalar constant is read this way. `const` may hold any JSON value,
    /// and an object or array one describes a shape rather than a value: taking
    /// `{type: object, properties: {...}, const: {...}}` for an enum would stand
    /// the stringified constant where the declared fields should be and drop
    /// every column. Those fall through to the shape dispatch instead, which is
    /// what describes them.
    fn const_enum_values(&self, schema: &Value) -> Option<Vec<String>> {
        let value = schema.get("const")?;
        // Scalars are stringified exactly as `enum` members already are, so
        // `const: 4` and `enum: [4]` agree rather than the newer keyword
        // inventing its own reading. `null` is excluded with the structured
        // values: it constrains the schema to hold nothing, which the type
        // already says better than a one-value enum of "null" would.
        matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_))
            .then(|| vec![enum_value(value)])
    }

    /// 2020-12 made `$ref` an ordinary keyword, so a 3.1 schema may write one
    /// alongside the assertions that narrow what it resolves to.
    fn ref_siblings_apply(&self) -> bool {
        true
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
