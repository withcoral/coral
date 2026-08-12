use serde_json::Value;

use crate::{ManifestError, Result};

/// The dialects the 3.1 importer's reading of a schema is true of.
///
/// 3.1 aligned its schema object with JSON Schema 2020-12 and named that as the
/// default, but it also let a document choose another: `jsonSchemaDialect`
/// declares one for the whole document, and `$schema` declares one for a schema
/// resource within it. The keywords the importer reads are not spelled the same
/// way in every dialect — draft-04 has no `const` at all, so a document
/// declaring it means something different by `{type: string, const: 4}` than
/// the closed enum `["4"]` this importer would persist.
///
/// The OAS base dialect is 2020-12 plus the vocabulary describing the keywords
/// `OpenAPI` adds — `discriminator`, `xml`, `externalDocs`, `example` — none of
/// which changes how anything read here is written.
const SUPPORTED_DIALECTS: [&str; 2] = [
    "https://json-schema.org/draft/2020-12/schema",
    "https://spec.openapis.org/oas/3.1/dialect/base",
];

/// Keywords whose values are instances rather than schemas.
///
/// The walk below reads every `$schema` string it passes as a dialect
/// declaration, and under these keywords that would be wrong: they hold example
/// and default *data*, which is free to contain a field called `$schema` that
/// declares nothing about how this document is to be read. A JSON Schema
/// document used as an example of itself is the obvious case.
const INSTANCE_KEYWORDS: [&str; 5] = ["example", "examples", "default", "const", "enum"];

/// Rejects a 3.1 document that declares a dialect this importer does not read.
///
/// Rejecting rather than warning, because the alternative is importing a
/// document under rules its author did not choose and persisting the result:
/// a surface whose columns are wrong is worse than one that does not exist,
/// and unlike a diagnostic it cannot be noticed after the fact.
pub(super) fn validate_json_schema_dialect(document: &Value) -> Result<()> {
    if let Some(declared) = document.get("jsonSchemaDialect").and_then(Value::as_str) {
        reject_unsupported(declared, "document's 'jsonSchemaDialect'")?;
    }
    // `$schema` may appear on any schema resource, so where the declaration sits
    // is not something the traversal can know up front — a single schema deep
    // inside `components` can select a dialect for itself alone.
    validate_schema_keywords(document)
}

fn validate_schema_keywords(value: &Value) -> Result<()> {
    match value {
        Value::Object(members) => {
            if let Some(declared) = members.get("$schema").and_then(Value::as_str) {
                reject_unsupported(declared, "schema's '$schema'")?;
            }
            members
                .iter()
                .filter(|(name, _)| !INSTANCE_KEYWORDS.contains(&name.as_str()))
                .try_for_each(|(_, member)| validate_schema_keywords(member))
        }
        Value::Array(members) => members.iter().try_for_each(validate_schema_keywords),
        _ => Ok(()),
    }
}

fn reject_unsupported(declared: &str, source: &str) -> Result<()> {
    // Trailing-slash and empty-fragment spellings of the same URI are the same
    // dialect. `https://json-schema.org/draft/2020-12/schema#` is how a document
    // that copied the identifier out of the specification's own `$id` line
    // writes it.
    let normalized = declared.trim().trim_end_matches('#').trim_end_matches('/');
    if SUPPORTED_DIALECTS.contains(&normalized) {
        return Ok(());
    }
    Err(ManifestError::validation(format!(
        "{source} selects JSON Schema dialect '{declared}', which Coral does not read; \
         OpenAPI 3.1 schemas are read as JSON Schema 2020-12"
    )))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn accepts_the_dialects_openapi_31_is_written_in() {
        for dialect in [
            "https://json-schema.org/draft/2020-12/schema",
            "https://json-schema.org/draft/2020-12/schema#",
            "https://spec.openapis.org/oas/3.1/dialect/base",
        ] {
            validate_json_schema_dialect(&json!({"jsonSchemaDialect": dialect}))
                .unwrap_or_else(|error| panic!("{dialect} should be accepted: {error}"));
        }
    }

    #[test]
    fn rejects_a_dialect_whose_keywords_read_differently() {
        let error = validate_json_schema_dialect(&json!({
            "jsonSchemaDialect": "http://json-schema.org/draft-04/schema#",
        }))
        .expect_err("draft-04 has no 'const' to read");
        assert!(
            error.to_string().contains("draft-04"),
            "the message should name the dialect: {error}"
        );
    }

    #[test]
    fn rejects_a_dialect_selected_by_a_single_schema() {
        validate_json_schema_dialect(&json!({
            "components": {
                "schemas": {
                    "Item": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                    },
                },
            },
        }))
        .expect_err("a schema may select a dialect for itself alone");
    }

    #[test]
    fn reads_a_property_named_schema_as_the_property_it_is() {
        // `$schema` in this position is a field of the described object, and its
        // value is a schema rather than a dialect identifier.
        validate_json_schema_dialect(&json!({
            "components": {
                "schemas": {
                    "Meta": {
                        "type": "object",
                        "properties": {"$schema": {"type": "string"}},
                    },
                },
            },
        }))
        .expect("a property called '$schema' declares nothing");
    }

    #[test]
    fn reads_example_data_as_data() {
        // The document describes JSON Schema documents, so its examples are
        // JSON Schema documents. Nothing in here selects a dialect for it.
        validate_json_schema_dialect(&json!({
            "components": {
                "schemas": {
                    "Schema": {
                        "type": "object",
                        "example": {"$schema": "http://json-schema.org/draft-04/schema#"},
                        "default": {"$schema": "http://json-schema.org/draft-04/schema#"},
                    },
                },
            },
        }))
        .expect("an example is data, not a declaration");
    }
}
