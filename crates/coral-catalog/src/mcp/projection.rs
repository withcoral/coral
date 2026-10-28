use crate::schema::{
    Direction, ProjectionOptions, SharedTypes, TypeScriptNames, TypeScriptProjection,
    TypeScriptProjector, render_callable_declaration,
};
use crate::{CatalogDiagnostic, McpExecutionBinding, McpInvocationMethod, SchemaGraph, SchemaId};

pub(super) fn fallback_typescript() -> String {
    render_callable_declaration(
        "input: unknown",
        "Promise<{ data: unknown; metadata: { kind: \"mcp\" } }>",
    )
}

/// Names every schema of an MCP Provider's graph for projection.
pub(crate) fn typescript_names(graph: &SchemaGraph) -> TypeScriptNames {
    TypeScriptNames::new(graph, ProjectionOptions::mcp())
}

pub(super) fn project_mcp_typescript(
    input_schema: &SchemaId,
    output_schema: Option<&SchemaId>,
    reads_resource: bool,
    graph: &SchemaGraph,
    type_names: &TypeScriptNames,
    shared: &mut SharedTypes,
) -> Result<TypeScriptProjection, CatalogDiagnostic> {
    let mut projector = TypeScriptProjector::new(graph, type_names, ProjectionOptions::mcp());
    let input = if reads_resource {
        "{ reference: McpResourceRef }".to_string()
    } else {
        projector.project(input_schema, Direction::Input)?
    };
    let output = output_schema.map_or_else(
        || Ok("unknown".to_string()),
        |schema| projector.project(schema, Direction::Output),
    )?;
    let input_declared = !matches!(input.as_str(), "{}" | "Record<string, never>");
    let input_parameter = if input_declared {
        format!("input: {input}")
    } else {
        String::new()
    };
    let type_references = projector.share_named_declarations(shared)?;
    let mut declarations = Vec::new();
    declarations.extend([
        "declare const McpResourceRefBrand: unique symbol;".to_string(),
        "type McpResourceRef = { readonly [McpResourceRefBrand]: true };".to_string(),
        "type McpSafeMeta = Record<string, unknown>;".to_string(),
        "type McpAnnotations = { readonly audience?: Array<\"user\" | \"assistant\">; readonly priority?: number; readonly lastModified?: string };".to_string(),
        "type McpIcon = { readonly src: string; readonly mimeType?: string; readonly sizes?: Array<string>; readonly theme?: \"light\" | \"dark\" };".to_string(),
        "type McpResourceContents = { readonly reference: McpResourceRef; readonly text: string; readonly mimeType?: string; readonly _meta?: McpSafeMeta } | { readonly reference: McpResourceRef; readonly blob: string; readonly mimeType?: string; readonly _meta?: McpSafeMeta };".to_string(),
        "type McpContentBlock = { readonly type: \"text\"; readonly text: string; readonly annotations?: McpAnnotations; readonly _meta?: McpSafeMeta } | { readonly type: \"image\" | \"audio\"; readonly data: string; readonly mimeType: string; readonly annotations?: McpAnnotations; readonly _meta?: McpSafeMeta } | { readonly type: \"resource_link\"; readonly resource: McpResourceRef; readonly name: string; readonly title?: string; readonly description?: string; readonly mimeType?: string; readonly size?: number; readonly icons?: Array<McpIcon>; readonly annotations?: McpAnnotations; readonly _meta?: McpSafeMeta } | { readonly type: \"resource\"; readonly resource: McpResourceContents; readonly annotations?: McpAnnotations; readonly _meta?: McpSafeMeta };".to_string(),
    ]);
    let data = if reads_resource {
        "{ contents: Array<McpResourceContents> }".to_string()
    } else {
        format!(
            "{{ content: Array<McpContentBlock>; structuredContent?: {output}; isError?: false }}"
        )
    };
    declarations.push(render_callable_declaration(
        &input_parameter,
        &format!("Promise<{{ data: {data}; metadata: {{ kind: \"mcp\"; _meta?: McpSafeMeta }} }}>"),
    ));
    Ok(TypeScriptProjection {
        declaration: declarations.join("\n"),
        type_references,
        input_declared,
        diagnostics: projector.into_diagnostics(),
    })
}

/// Returns whether a stored MCP-backed Tool's callable takes `input`, from the
/// same projection that materialization ran.
pub(crate) fn declares_input(
    input_schema: &SchemaId,
    binding: &McpExecutionBinding,
    graph: &SchemaGraph,
    type_names: &TypeScriptNames,
) -> Result<bool, CatalogDiagnostic> {
    project_mcp_typescript(
        input_schema,
        binding.output_schema(),
        binding.invocation_method() == McpInvocationMethod::ReadResource,
        graph,
        type_names,
        &mut SharedTypes::default(),
    )
    .map(|projection| projection.input_declared)
}
