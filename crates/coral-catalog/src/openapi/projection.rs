use crate::schema::{
    Direction, ProjectionOptions, SharedTypes, TypeScriptNames, TypeScriptProjection,
    TypeScriptProjector, render_callable_declaration,
};
use crate::{CatalogDiagnostic, HttpExecutionBinding, HttpResponseStatus, SchemaGraph, SchemaId};

/// The type an HTTP Tool declares for each declared success response:
/// `{ status, data, metadata }`, with `status` beside `data` so checking it
/// narrows `data`. Its type arguments are, in order, the response's data
/// type, its status, and its media type: `HttpResult<Data, Status, MediaType>`.
/// Readers of a declaration, such as the search signature line, rely on this
/// name and order. A Provider schema with this name is renamed. Every other
/// response rejects the call, so it has no declared type.
pub const HTTP_RESULT: &str = "HttpResult";

/// The status type of a `2XX` success range: a `number` branded with the
/// range, so a declaration neither claims every 2xx code nor merges the range
/// with `default`. A Provider schema with this name is renamed.
const HTTP_SUCCESS_STATUS: &str = "HttpStatus2xx";
/// The `unique symbol` that brands the success range type. Programs cannot
/// name it, so no Program reads a brand the runtime status never carries.
const HTTP_STATUS_RANGE_BRAND: &str = "HttpStatusRangeBrand";
/// Every type name an HTTP Tool's declaration declares itself.
pub(crate) const DECLARED_TYPE_NAMES: [&str; 2] = [HTTP_RESULT, HTTP_SUCCESS_STATUS];

const DECODE_DIAGNOSTIC: &str = "{ readonly code: string; readonly message: string }";

/// The metadata of one complete HTTP response with its media type as given.
/// A declared success can still report a schema mismatch, or a body for a
/// success that declares no content, so its diagnostics can be non-empty.
fn http_metadata(media_type: &str) -> String {
    format!(
        "{{ readonly kind: \"http\"; readonly headers: readonly (readonly [string, string])[]; readonly mediaType: {media_type}; readonly charset: string | null; readonly finalUrl: string; readonly redirects: readonly {{ readonly status: number; readonly url: string; readonly crossOrigin: boolean }}[]; readonly decodeDiagnostics: readonly {DECODE_DIAGNOSTIC}[] }}"
    )
}

/// The HTTP Tool Result of one declared success.
fn http_result_declaration() -> String {
    format!(
        "type {HTTP_RESULT}<Data, Status, MediaType> = {{ status: Status; data: Data; metadata: {} }};",
        http_metadata("MediaType")
    )
}

pub(super) fn fallback_typescript() -> String {
    render_callable_declaration(
        "input: unknown",
        &format!(
            "Promise<{{ status: number; data: unknown; metadata: {} }}>",
            http_metadata("string | null")
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
    let mut declares_range = false;
    for response in binding.declared_successes() {
        let status = match response.status() {
            HttpResponseStatus::Code(code) => code.to_string(),
            // A declared success range is `2XX`.
            HttpResponseStatus::Range(_) => {
                declares_range = true;
                HTTP_SUCCESS_STATUS.to_string()
            }
            HttpResponseStatus::Default => "number".to_string(),
        };
        if response.content().is_empty() {
            // A body for a success that declares no content is still a
            // success, so only a 204, which carries no body, promises `null`.
            let data = if response.status() == HttpResponseStatus::Code(204) {
                "null"
            } else {
                "unknown"
            };
            outputs.push(format!("{HTTP_RESULT}<{data}, {status}, string | null>"));
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
    if declares_range {
        result_declarations.extend([
            format!("const {HTTP_STATUS_RANGE_BRAND}: unique symbol;"),
            format!(
                "type {HTTP_SUCCESS_STATUS} = number & {{ readonly [{HTTP_STATUS_RANGE_BRAND}]: \"2xx\" }};"
            ),
        ]);
    }
    result_declarations.push(http_result_declaration());
    // With no declared success, only `SuccessSelection::Untyped` can succeed,
    // and nothing describes it.
    if outputs.is_empty() {
        outputs.push(format!("{HTTP_RESULT}<unknown, number, string | null>"));
    }
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

/// Returns whether a stored HTTP Tool's callable takes `input`, from the
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
