use serde_json::Value;

use crate::{ManifestError, Result};

/// A specification version the `OpenAPI` importer has a dialect for.
///
/// Only the major and minor components are modelled. The patch component
/// separates editorial revisions of one specification — 3.0.0 through 3.0.4 —
/// and nothing the importer does varies across them.
///
/// 3.1 joins this as its dialect lands. Recognising a version here is the same
/// statement as supporting it, so an unsupported version is rejected by the
/// parse rather than admitted and turned away later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpenApiVersion {
    V3_0,
}

/// Reads a document's declared `openapi` version.
///
/// Matches on major and minor rather than testing a `"3.0."` prefix, which read
/// the patch separator as mandatory and so rejected a bare `3.0` — a spelling
/// the field's own grammar allows.
pub(super) fn parse_openapi_version(document: &Value) -> Result<OpenApiVersion> {
    let Some(declared) = document.get("openapi").and_then(Value::as_str) else {
        // A Swagger 2 document carries `swagger` where this one looks for
        // `openapi`. It is a common enough input to name for what it is rather
        // than report as a missing field.
        if let Some(swagger) = document.get("swagger").and_then(Value::as_str) {
            return Err(ManifestError::validation(format!(
                "document declares Swagger version '{swagger}'; Coral requires OpenAPI 3.0"
            )));
        }
        return Err(ManifestError::validation(
            "OpenAPI document is missing openapi version",
        ));
    };
    let mut components = declared.trim().split('.');
    match (components.next(), components.next()) {
        (Some("3"), Some("0")) => Ok(OpenApiVersion::V3_0),
        _ => Err(ManifestError::validation(format!(
            "OpenAPI document uses unsupported version '{declared}'"
        ))),
    }
}
