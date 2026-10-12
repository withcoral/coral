use crate::schema::{
    Direction, ProjectionOptions, TypeScriptNames, TypeScriptProjection, TypeScriptProjector,
    render_callable_declaration,
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

pub(super) fn fallback_typescript(relative_path: &str) -> String {
    format!(
        "function {relative_path}(input: unknown): Promise<{{ data: unknown; metadata: {} }}>;",
        http_metadata(
            "number",
            "string | null",
            &format!("readonly {DECODE_DIAGNOSTIC}[]")
        )
    )
}

/// Names every schema of an OpenAPI Provider's graph for projection.
pub(crate) fn typescript_names(graph: &SchemaGraph) -> TypeScriptNames {
    TypeScriptNames::new(graph, ProjectionOptions::openapi())
}

pub(super) fn project_typescript(
    relative_path: &str,
    input_schema: &SchemaId,
    binding: &HttpExecutionBinding,
    graph: &SchemaGraph,
    type_names: &TypeScriptNames,
) -> Result<TypeScriptProjection, CatalogDiagnostic> {
    let mut projector = TypeScriptProjector::new(graph, type_names, ProjectionOptions::openapi());
    let input = projector.project(input_schema, Direction::Input)?;
    let mut outputs = Vec::new();
    for response in binding.responses() {
        let status = projected_status(response.status());
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
    let mut declarations = projector.render_named_declarations()?;
    if projector.uses_named_scalar("BinaryValue") {
        declarations.insert(
            0,
            "type BinaryValue = { readonly _tag: \"BinaryValue\"; readonly encoding: \"base64\"; readonly data: string; readonly mimeType: string; readonly byteLength: number; readonly name?: string };".to_string(),
        );
    }
    declarations.extend(result_declarations);
    declarations.push(render_callable_declaration(
        relative_path,
        &input_parameter,
        &format!("Promise<{output}>"),
    )?);
    Ok(TypeScriptProjection {
        declaration: declarations.join("\n"),
        input_declared,
        diagnostics: projector.into_diagnostics(),
    })
}

/// Returns whether a stored HTTP Operation's callable takes `input`, from the
/// same projection that materialization ran.
pub(crate) fn declares_input(
    relative_path: &str,
    input_schema: &SchemaId,
    binding: &HttpExecutionBinding,
    graph: &SchemaGraph,
    type_names: &TypeScriptNames,
) -> Result<bool, CatalogDiagnostic> {
    project_typescript(relative_path, input_schema, binding, graph, type_names)
        .map(|projection| projection.input_declared)
}

fn projected_status(status: HttpResponseStatus) -> String {
    match status {
        HttpResponseStatus::Code(status) => status.to_string(),
        HttpResponseStatus::Range(range) => (range * 100..=range * 100 + 99)
            .map(|status| status.to_string())
            .collect::<Vec<_>>()
            .join(" | "),
        HttpResponseStatus::Default => "number".to_string(),
    }
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
