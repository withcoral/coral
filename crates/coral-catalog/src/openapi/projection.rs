use std::collections::BTreeSet;

use crate::schema::{
    Direction, ProjectionOptions, SharedTypes, TypeScriptNames, TypeScriptProjection,
    TypeScriptProjector, render_callable_declaration,
};
use crate::{CatalogDiagnostic, HttpExecutionBinding, HttpResponseStatus, SchemaGraph, SchemaId};

/// The type an HTTP Operation declares for each response that matches a
/// declared status and media type and decodes. Its type arguments are, in
/// order, the response's data type, its status, and its media type:
/// `HttpResult<Data, Status, MediaType>`. Readers of a declaration, such as
/// the search signature line, rely on this name and order. A Provider schema
/// with this name is renamed.
pub const HTTP_RESULT: &str = "HttpResult";
/// The type an HTTP Operation declares for any other response; it is never a
/// declared success. A Provider schema with this name is renamed.
pub const HTTP_UNKNOWN_RESULT: &str = "HttpUnknownResult";

/// The status type of a `2XX` response, which readers of a declaration, such
/// as the search signature line, treat as a success.
pub const HTTP_SUCCESS_STATUS: &str = "HttpStatus2xx";
/// The status type of each OpenAPI status range, `1XX` to `5XX`. Each is a
/// `number` branded with its range, so a declaration neither claims every
/// code in the range nor merges the range with `default`. A Provider schema
/// with one of these names is renamed.
const HTTP_STATUS_RANGES: [&str; 5] = [
    "HttpStatus1xx",
    HTTP_SUCCESS_STATUS,
    "HttpStatus3xx",
    "HttpStatus4xx",
    "HttpStatus5xx",
];
/// The `unique symbol` that brands each status range type. Programs cannot
/// name it, so no Program reads a brand the runtime status never carries.
const HTTP_STATUS_RANGE_BRAND: &str = "HttpStatusRangeBrand";
/// Every type name an HTTP Operation's declaration declares itself.
pub(crate) const DECLARED_TYPE_NAMES: [&str; 7] = [
    HTTP_RESULT,
    HTTP_UNKNOWN_RESULT,
    HTTP_STATUS_RANGES[0],
    HTTP_STATUS_RANGES[1],
    HTTP_STATUS_RANGES[2],
    HTTP_STATUS_RANGES[3],
    HTTP_STATUS_RANGES[4],
];

const DECODE_DIAGNOSTIC: &str = "{ readonly code: string; readonly message: string }";

/// The metadata of one complete HTTP response, with its status, media type,
/// and decode diagnostics as given.
fn http_metadata(status: &str, media_type: &str, decode_diagnostics: &str) -> String {
    format!(
        "{{ readonly kind: \"http\"; readonly status: {status}; readonly headers: readonly (readonly [string, string])[]; readonly mediaType: {media_type}; readonly charset: string | null; readonly finalUrl: string; readonly redirects: readonly {{ readonly status: number; readonly url: string; readonly crossOrigin: boolean }}[]; readonly decodeDiagnostics: {decode_diagnostics} }}"
    )
}

/// The HTTP Operation Result of a response that matches a declared status
/// and media type and decodes.
fn http_result_declaration() -> String {
    format!(
        "type {HTTP_RESULT}<Data, Status, MediaType> = {{ data: Data; metadata: {} }};",
        http_metadata("Status", "MediaType", "readonly []")
    )
}

/// The HTTP Operation Result of any other response: untyped data and at
/// least one decode diagnostic.
fn http_unknown_result_declaration() -> String {
    format!(
        "type {HTTP_UNKNOWN_RESULT} = {{ data: unknown; metadata: {} }};",
        http_metadata(
            "number",
            "string | null",
            &format!("readonly [{DECODE_DIAGNOSTIC}, ...{DECODE_DIAGNOSTIC}[]]")
        )
    )
}

pub(super) fn fallback_typescript() -> String {
    render_callable_declaration(
        "input: unknown",
        &format!(
            "Promise<{{ data: unknown; metadata: {} }}>",
            http_metadata(
                "number",
                "string | null",
                &format!("readonly {DECODE_DIAGNOSTIC}[]")
            )
        ),
    )
}

/// Names every schema of an OpenAPI Provider's graph for projection.
pub(crate) fn typescript_names(graph: &SchemaGraph) -> TypeScriptNames {
    TypeScriptNames::new(graph, ProjectionOptions::openapi())
}

pub(super) fn project_typescript(
    input_schema: &SchemaId,
    binding: &HttpExecutionBinding,
    graph: &SchemaGraph,
    type_names: &TypeScriptNames,
    shared: &mut SharedTypes,
) -> Result<TypeScriptProjection, CatalogDiagnostic> {
    let mut projector = TypeScriptProjector::new(graph, type_names, ProjectionOptions::openapi());
    let input = projector.project(input_schema, Direction::Input)?;
    let mut outputs = Vec::new();
    let mut ranges = BTreeSet::new();
    for response in binding.responses() {
        if let HttpResponseStatus::Range(range) = response.status() {
            ranges.insert(range);
        }
        let status = projected_status(response.status())?;
        if response.content().is_empty() {
            outputs.push(format!("{HTTP_RESULT}<null, {status}, string | null>"));
        } else {
            for content in response.content() {
                let data = projector.project(content.schema(), Direction::Output)?;
                let media_type = projected_media_type(content.media_type());
                outputs.push(format!("{HTTP_RESULT}<{data}, {status}, {media_type}>"));
            }
        }
    }
    outputs.sort();
    outputs.dedup();
    let mut result_declarations = Vec::new();
    if !ranges.is_empty() {
        result_declarations.push(format!("const {HTTP_STATUS_RANGE_BRAND}: unique symbol;"));
    }
    for range in ranges {
        result_declarations.push(status_range_declaration(range)?);
    }
    if !outputs.is_empty() {
        result_declarations.push(http_result_declaration());
    }
    result_declarations.push(http_unknown_result_declaration());
    outputs.push(HTTP_UNKNOWN_RESULT.to_string());
    let output = outputs.join(" | ");
    let input_declared = !matches!(input.as_str(), "{}" | "Record<string, never>");
    let input_parameter = if input_declared {
        format!("input: {input}")
    } else {
        String::new()
    };
    let type_references = projector.share_named_declarations(shared)?;
    let mut declarations = Vec::new();
    if projector.uses_named_scalar("BinaryValue") {
        declarations.push(
            "type BinaryValue = { readonly _tag: \"BinaryValue\"; readonly encoding: \"base64\"; readonly data: string; readonly mimeType: string; readonly byteLength: number; readonly name?: string };".to_string(),
        );
    }
    declarations.extend(result_declarations);
    declarations.push(render_callable_declaration(
        &input_parameter,
        &format!("Promise<{output}>"),
    ));
    Ok(TypeScriptProjection {
        declaration: declarations.join("\n"),
        type_references,
        input_declared,
        diagnostics: projector.into_diagnostics(),
    })
}

/// Returns whether a stored HTTP Operation's callable takes `input`, from the
/// same projection that materialization ran.
pub(crate) fn declares_input(
    input_schema: &SchemaId,
    binding: &HttpExecutionBinding,
    graph: &SchemaGraph,
    type_names: &TypeScriptNames,
) -> Result<bool, CatalogDiagnostic> {
    project_typescript(
        input_schema,
        binding,
        graph,
        type_names,
        &mut SharedTypes::default(),
    )
    .map(|projection| projection.input_declared)
}

fn projected_status(status: HttpResponseStatus) -> Result<String, CatalogDiagnostic> {
    Ok(match status {
        HttpResponseStatus::Code(status) => status.to_string(),
        HttpResponseStatus::Range(range) => status_range_name(range)?.to_string(),
        HttpResponseStatus::Default => "number".to_string(),
    })
}

/// A stored binding is deserialized, so its range is checked again here.
fn status_range_name(range: u16) -> Result<&'static str, CatalogDiagnostic> {
    usize::from(range)
        .checked_sub(1)
        .and_then(|index| HTTP_STATUS_RANGES.get(index).copied())
        .ok_or_else(|| {
            CatalogDiagnostic::stable(
                "invalid_openapi_operation",
                "an OpenAPI response has an invalid status key",
            )
        })
}

fn status_range_declaration(range: u16) -> Result<String, CatalogDiagnostic> {
    Ok(format!(
        "type {} = number & {{ readonly [{HTTP_STATUS_RANGE_BRAND}]: \"{range}xx\" }};",
        status_range_name(range)?
    ))
}

fn projected_media_type(media_type: &str) -> String {
    let essence = media_type
        .split(';')
        .next()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if essence.contains('*') {
        "string".to_string()
    } else {
        format!("{essence:?}")
    }
}
