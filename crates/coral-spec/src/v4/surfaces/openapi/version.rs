use serde_json::Value;

use crate::{ManifestError, Result};

/// A specification version the `OpenAPI` importer has a dialect for.
///
/// Only the major and minor components are modelled. The patch component
/// separates editorial revisions of one specification — 3.0.0 through 3.0.4 —
/// and nothing the importer does varies across them.
///
/// Recognising a version here is the same statement as supporting it, so an
/// unsupported version is rejected by the parse rather than admitted and turned
/// away later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpenApiVersion {
    V3_0,
    V3_1,
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
                "document declares Swagger version '{swagger}'; Coral requires OpenAPI 3.0 or 3.1"
            )));
        }
        return Err(ManifestError::validation(
            "OpenAPI document is missing openapi version",
        ));
    };
    let unsupported = || {
        ManifestError::validation(format!(
            "OpenAPI document uses unsupported version '{declared}'"
        ))
    };
    let mut components = declared.trim().split('.');
    let version = match (components.next(), components.next()) {
        (Some("3"), Some("0")) => OpenApiVersion::V3_0,
        (Some("3"), Some("1")) => OpenApiVersion::V3_1,
        _ => return Err(unsupported()),
    };
    // The patch component is optional, but a present one has to be a number and
    // has to be the last thing in the string. Matching only the first two
    // components would take `3.1.banana`, `3.1.` and `3.1.1.2` for well-formed
    // 3.1 and import them, and this is the one place left that reads the
    // version — so it is the place to say what the field may hold.
    //
    // Both remaining components are taken up front, mirroring the match above:
    // deciding this in a guard would leave whether `3.0.1.2` is rejected resting
    // on a mid-match side effect, which is not something a reader should have to
    // trace.
    let is_numeric = |component: &str| {
        !component.is_empty() && component.bytes().all(|byte| byte.is_ascii_digit())
    };
    match (components.next(), components.next()) {
        (None, _) => Ok(version),
        (Some(patch), None) if is_numeric(patch) => Ok(version),
        _ => Err(unsupported()),
    }
}
