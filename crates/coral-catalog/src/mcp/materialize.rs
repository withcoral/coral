use crate::AuthenticationConfiguration;
use std::collections::{BTreeMap, HashSet};

use serde_json::{Map, Value};

use crate::mcp::projection::{fallback_typescript, project_mcp_typescript, typescript_names};
use crate::paths::assign_tool_paths;
use crate::schema::{SchemaGraphAccumulator, canonicalize};
use crate::{
    CatalogDiagnostic, MaterializationOutcome, MaterializedTool, McpDiscoverySnapshot,
    McpExecutionBinding, McpInvocationMethod, McpTaskSupport, McpToolAnnotations, McpTransport,
    PreparedProviderCatalog, SchemaId, SchemaSemantics, ToolHints, ToolKey, ToolPathAssignment,
    ToolPresentation,
};

/// Stable version of the bounded schema-faithful MCP profile materializer.
pub(crate) const MATERIALIZER_VERSION: &str = "mcp-schema-faithful-search-profile-v3";
const RESOURCE_READ_KEY: &str = "resources/read";
const RESOURCE_READ_PATH: &str = "resources.read";

/// Materializes one exact normalized MCP discovery snapshot and carries forward
/// paths owned by Tool Keys that remain active.
#[must_use]
pub(crate) fn materialize(
    snapshot: &McpDiscoverySnapshot,
    previous_paths: &[ToolPathAssignment],
) -> MaterializationOutcome {
    match build_mcp(snapshot, previous_paths) {
        Ok(package) => MaterializationOutcome::Ready(package),
        Err(diagnostic) => MaterializationOutcome::Rejected(diagnostic),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one bounded snapshot is assembled into one atomic catalog package"
)]
fn build_mcp(
    input: &McpDiscoverySnapshot,
    previous_paths: &[ToolPathAssignment],
) -> Result<PreparedProviderCatalog, CatalogDiagnostic> {
    let snapshot: Value = serde_json::from_str(input.as_str()).map_err(|_error| {
        diagnostic(
            "invalid_mcp_discovery_snapshot",
            "the pinned input is not a valid normalized MCP discovery snapshot",
        )
    })?;
    let snapshot = canonicalize(&snapshot);
    let root = snapshot.as_object().ok_or_else(invalid_snapshot)?;
    let (transport, server_url) = server_binding(root)?;
    let credential_contract = input
        .configuration()
        .and_then(|configuration| configuration.authentication.clone())
        .map_or_else(
            || credential_contract(root),
            |authentication| Ok(authentication.at_destinations(vec![server_url.clone()])),
        )?;
    let protocol_version = protocol_version(root)?;
    let display_name =
        crate::configuration::configured_display_name(input.configuration(), || {
            display_name(root)
        })?;
    let resources_advertised = resources_advertised(root)?;
    let tools_advertised = tools_advertised(root)?;
    let server_task_capability = server_task_capability(root)?;
    let tools: &[Value] = match root.get("tools") {
        Some(tools) => tools
            .as_array()
            .map(Vec::as_slice)
            .ok_or_else(invalid_snapshot)?,
        None if !tools_advertised => &[],
        None => return Err(invalid_snapshot()),
    };
    if !tools.is_empty() && !tools_advertised {
        return Err(invalid_snapshot());
    }

    let mut seen = HashSet::new();
    let mut drafts = Vec::with_capacity(tools.len());
    for tool in tools {
        let object = tool.as_object().ok_or_else(invalid_snapshot)?;
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| {
                !name.is_empty()
                    && name.len() <= 128
                    && name.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                    })
            })
            .ok_or_else(|| {
                diagnostic(
                    "invalid_mcp_tool_name",
                    "an MCP tool has an invalid Tool Key",
                )
            })?;
        let key = ToolKey::parse(name).map_err(|_error| {
            diagnostic(
                "invalid_mcp_tool_name",
                "an MCP tool has an invalid Tool Key",
            )
        })?;
        if !seen.insert(key.clone()) {
            return Err(diagnostic(
                "duplicate_mcp_tool_name",
                "the MCP discovery snapshot contains duplicate tool names",
            ));
        }
        drafts.push(McpToolDraft {
            key,
            definition: Value::Object(object.clone()),
        });
    }
    if resources_advertised {
        let resource_key =
            ToolKey::parse(RESOURCE_READ_KEY).map_err(|_error| invalid_tool_index())?;
        if !seen.insert(resource_key.clone()) {
            return Err(diagnostic(
                "reserved_mcp_tool_conflict",
                "an MCP tool conflicts with the reserved resource-read Tool",
            ));
        }
    }
    if seen.is_empty() {
        return Err(diagnostic(
            "empty_mcp_catalog",
            "the MCP discovery snapshot contains no callable surface",
        ));
    }

    let mut assigned_paths = assign_tool_paths(seen.iter(), previous_paths)?;
    if resources_advertised {
        reserve_resource_read_path(&mut assigned_paths, previous_paths)?;
    }

    let mut schemas = SchemaGraphAccumulator::new(SchemaSemantics::JsonSchema2020_12);
    let mut builds = Vec::with_capacity(seen.len());
    for draft in drafts {
        let relative_path = assigned_paths
            .get(&draft.key)
            .cloned()
            .ok_or_else(invalid_tool_index)?;
        builds.push(materialize_tool(
            &protocol_version,
            transport,
            &server_url,
            server_task_capability.as_ref(),
            draft,
            relative_path,
            &mut schemas,
        )?);
    }
    if resources_advertised {
        builds.push(materialize_resource_read(
            &protocol_version,
            transport,
            &server_url,
            server_task_capability.as_ref(),
            &mut schemas,
        )?);
    }

    let schema_graph = schemas.finish()?;
    let schema_graph_json = schema_graph.to_storage_json().map_err(|_error| {
        diagnostic(
            "invalid_schema_graph",
            "the authoritative MCP schema graph cannot be encoded",
        )
    })?;
    let mut materialized = Vec::with_capacity(builds.len());
    let type_names = typescript_names(&schema_graph);
    let mut shared_types = crate::schema::SharedTypes::default();
    for mut build in builds {
        if let Some(configuration) = input.configuration() {
            build
                .binding
                .set_request_headers(configuration.request_headers.clone());
        }
        build
            .binding
            .set_credential_contract(credential_contract.clone());
        let reads_resource = build.binding.invocation_method() == McpInvocationMethod::ReadResource;
        let (declaration, type_references) = match project_mcp_typescript(
            &build.input_schema,
            build.output_schema.as_ref(),
            reads_resource,
            &schema_graph,
            &type_names,
            &mut shared_types,
        ) {
            Ok(projection) => {
                build.diagnostics.extend(projection.diagnostics);
                (projection.declaration, projection.type_references)
            }
            Err(diagnostic) => {
                build.diagnostics.push(diagnostic);
                (fallback_typescript(), Vec::new())
            }
        };
        if let Some(diagnostic) = crate::types::tool_description_too_large(
            &build.relative_path,
            &display_name,
            &declaration,
            &build.presentation,
        ) {
            build.diagnostics.push(diagnostic);
        }
        let binding_json = build.binding.to_storage_json().map_err(|_error| {
            diagnostic(
                "invalid_execution_binding",
                "an MCP Execution Binding cannot be encoded",
            )
        })?;
        let diagnostics_json = serde_json::to_string(&build.diagnostics).map_err(|_error| {
            diagnostic(
                "invalid_tool_diagnostic",
                "a Tool diagnostic cannot be encoded safely",
            )
        })?;
        materialized.push(MaterializedTool::new(
            build.key,
            build.relative_path,
            build.summary,
            build.presentation,
            build.hints,
            crate::ExecutionBinding::Mcp(build.binding),
            binding_json,
            build.input_schema,
            build.input_schema_json,
            build.output_schema_json,
            declaration,
            type_references,
            build.diagnostics,
            diagnostics_json,
        ));
    }
    let catalog = PreparedProviderCatalog::new(
        display_name,
        schema_graph,
        schema_graph_json,
        materialized,
        shared_types,
    );
    let Some(configuration) = input.configuration() else {
        return Ok(catalog);
    };
    catalog.with_configuration(configuration).map_err(|_error| {
        diagnostic(
            "invalid_provider_configuration",
            "the Provider configuration cannot be applied to the catalog",
        )
    })
}

fn protocol_version(root: &Map<String, Value>) -> Result<String, CatalogDiagnostic> {
    match root.get("protocolVersion").and_then(Value::as_str) {
        Some(version @ ("2025-06-18" | "2025-11-25")) => Ok(version.to_string()),
        _ => Err(diagnostic(
            "unsupported_mcp_protocol_version",
            "the MCP discovery snapshot uses an unsupported protocol version",
        )),
    }
}

pub(super) fn display_name(root: &Map<String, Value>) -> Result<String, CatalogDiagnostic> {
    let info = root
        .get("serverInfo")
        .and_then(Value::as_object)
        .ok_or_else(invalid_snapshot)?;
    let name = safe_server_text(info.get("name")).ok_or_else(|| {
        diagnostic(
            "invalid_mcp_server_info",
            "the MCP discovery snapshot has invalid server identity metadata",
        )
    })?;
    if safe_server_text(info.get("version")).is_none() {
        return Err(diagnostic(
            "invalid_mcp_server_info",
            "the MCP discovery snapshot has invalid server identity metadata",
        ));
    }
    let title = match info.get("title") {
        None => None,
        Some(value) => Some(safe_server_text(Some(value)).ok_or_else(|| {
            diagnostic(
                "invalid_mcp_server_info",
                "the MCP discovery snapshot has invalid server presentation metadata",
            )
        })?),
    };
    Ok(title.unwrap_or(name).to_string())
}

fn safe_server_text(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str).filter(|value| {
        !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
    })
}

pub(super) fn server_binding(
    root: &Map<String, Value>,
) -> Result<(McpTransport, String), CatalogDiagnostic> {
    let server = root
        .get("server")
        .and_then(Value::as_object)
        .ok_or_else(invalid_snapshot)?;
    let transport = match server.get("transport").and_then(Value::as_str) {
        Some("streamable_http") => McpTransport::StreamableHttp,
        _ => {
            return Err(diagnostic(
                "unsupported_mcp_transport",
                "the MCP discovery snapshot uses an unsupported server transport",
            ));
        }
    };
    let server_url = server
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(invalid_snapshot)?;
    let url = url::Url::parse(server_url).map_err(|_error| {
        diagnostic(
            "invalid_mcp_server",
            "the MCP discovery snapshot has an invalid server URL",
        )
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(diagnostic(
            "invalid_mcp_server",
            "the MCP discovery snapshot has an invalid server URL",
        ));
    }
    Ok((transport, server_url.to_string()))
}

fn resources_advertised(root: &Map<String, Value>) -> Result<bool, CatalogDiagnostic> {
    let capabilities = root
        .get("capabilities")
        .and_then(Value::as_object)
        .ok_or_else(invalid_snapshot)?;
    if capabilities
        .get("tools")
        .is_some_and(|value| !capability_with_optional_bools(value, &["listChanged"]))
        || capabilities.get("resources").is_some_and(|value| {
            !capability_with_optional_bools(value, &["subscribe", "listChanged"])
        })
    {
        return Err(invalid_snapshot());
    }
    Ok(capabilities.contains_key("resources"))
}

fn capability_with_optional_bools(value: &Value, fields: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        fields
            .iter()
            .all(|field| object.get(*field).is_none_or(Value::is_boolean))
    })
}

fn tools_advertised(root: &Map<String, Value>) -> Result<bool, CatalogDiagnostic> {
    let capabilities = root
        .get("capabilities")
        .and_then(Value::as_object)
        .ok_or_else(invalid_snapshot)?;
    Ok(capabilities.contains_key("tools"))
}

fn server_task_capability(root: &Map<String, Value>) -> Result<Option<Value>, CatalogDiagnostic> {
    let capabilities = root
        .get("capabilities")
        .and_then(Value::as_object)
        .ok_or_else(invalid_snapshot)?;
    match capabilities.get("tasks") {
        None => Ok(None),
        Some(value) if valid_task_capability(value) => Ok(Some(canonicalize(value))),
        Some(_) => Err(invalid_snapshot()),
    }
}

fn valid_task_capability(value: &Value) -> bool {
    let Some(tasks) = value.as_object() else {
        return false;
    };
    if ["list", "cancel"]
        .iter()
        .any(|field| tasks.get(*field).is_some_and(|value| !value.is_object()))
    {
        return false;
    }
    let Some(requests) = tasks.get("requests") else {
        return true;
    };
    let Some(requests) = requests.as_object() else {
        return false;
    };
    for (kind, method) in [
        ("sampling", "createMessage"),
        ("elicitation", "create"),
        ("tools", "call"),
    ] {
        let Some(capability) = requests.get(kind) else {
            continue;
        };
        let Some(capability) = capability.as_object() else {
            return false;
        };
        if capability
            .get(method)
            .is_some_and(|value| !value.is_object())
        {
            return false;
        }
    }
    true
}

fn reserve_resource_read_path(
    assigned: &mut BTreeMap<ToolKey, String>,
    previous_paths: &[ToolPathAssignment],
) -> Result<(), CatalogDiagnostic> {
    let resource_key = ToolKey::parse(RESOURCE_READ_KEY).map_err(|_error| invalid_tool_index())?;
    if previous_paths.iter().any(|previous| {
        (previous.key() == &resource_key && previous.relative_path() != RESOURCE_READ_PATH)
            || (previous.key() != &resource_key && previous.relative_path() == RESOURCE_READ_PATH)
    }) {
        return Err(invalid_tool_index());
    }
    if assigned
        .iter()
        .any(|(key, path)| key != &resource_key && path == RESOURCE_READ_PATH)
    {
        return Err(invalid_tool_index());
    }
    assigned.insert(resource_key, RESOURCE_READ_PATH.to_string());
    Ok(())
}

struct McpToolDraft {
    key: ToolKey,
    definition: Value,
}

struct McpToolBuild {
    key: ToolKey,
    relative_path: String,
    summary: String,
    presentation: ToolPresentation,
    hints: ToolHints,
    binding: McpExecutionBinding,
    input_schema: SchemaId,
    output_schema: Option<SchemaId>,
    input_schema_json: String,
    output_schema_json: String,
    diagnostics: Vec<CatalogDiagnostic>,
}

fn materialize_tool(
    protocol_version: &str,
    transport: McpTransport,
    server_url: &str,
    server_task_capability: Option<&Value>,
    draft: McpToolDraft,
    relative_path: String,
    schemas: &mut SchemaGraphAccumulator,
) -> Result<McpToolBuild, CatalogDiagnostic> {
    let object = draft.definition.as_object().ok_or_else(invalid_snapshot)?;
    let mut diagnostics = Vec::new();
    let summary = tool_summary(object, draft.key.as_str(), &mut diagnostics);
    let input_value = object.get("inputSchema").cloned().unwrap_or(Value::Null);
    let input_source = tool_schema_source(&draft.key, "input");
    let (input_schema, input_schema_json) = retain_tool_schema(
        schemas,
        &input_source,
        &input_value,
        "unsupported_mcp_input_schema",
        "an MCP tool input schema cannot be invoked faithfully",
        &mut diagnostics,
    )?;
    let (output_schema, output_schema_json) = match object.get("outputSchema") {
        Some(output) => {
            let source = tool_schema_source(&draft.key, "output");
            let (schema, json) = retain_tool_schema(
                schemas,
                &source,
                output,
                "unsupported_mcp_output_schema",
                "an MCP tool output schema cannot be projected faithfully",
                &mut diagnostics,
            )?;
            (Some(schema), json)
        }
        None => (None, "null".to_string()),
    };
    let annotations = parse_annotations(object.get("annotations"), &mut diagnostics);
    let presentation = tool_presentation(object, &summary, annotations.as_ref());
    let hints = annotations
        .as_ref()
        .map(McpToolAnnotations::hints)
        .unwrap_or_default();
    let task_support = parse_task_support(object.get("execution"), &mut diagnostics);
    if task_support == Some(McpTaskSupport::Required) {
        diagnostics.push(diagnostic(
            "upstream_mcp_task_required",
            "the MCP tool requires Upstream MCP Tasks, which v1 does not use",
        ));
    }
    let binding = McpExecutionBinding::new(
        protocol_version.to_string(),
        transport,
        server_url.to_string(),
        McpInvocationMethod::CallTool,
        Some(draft.key.as_str().to_string()),
        Some(draft.definition),
        input_schema.clone(),
        output_schema.clone(),
        annotations,
        server_task_capability.cloned(),
        task_support,
    );
    Ok(McpToolBuild {
        key: draft.key,
        relative_path,
        summary,
        presentation,
        hints,
        binding,
        input_schema,
        output_schema,
        input_schema_json,
        output_schema_json,
        diagnostics,
    })
}

fn materialize_resource_read(
    protocol_version: &str,
    transport: McpTransport,
    server_url: &str,
    server_task_capability: Option<&Value>,
    schemas: &mut SchemaGraphAccumulator,
) -> Result<McpToolBuild, CatalogDiagnostic> {
    let key = ToolKey::parse(RESOURCE_READ_KEY).map_err(|_error| invalid_tool_index())?;
    let input = serde_json::json!({
        "type": "object",
        "required": ["reference"],
        "properties": {
            "reference": {
                "not": {},
                "x-coral-opaque-type": "McpResourceRef"
            }
        },
        "additionalProperties": false
    });
    let input_schema = schemas.add_embedded_schema("mcp:resources/read:input", &input)?;
    let input_schema_json = serde_json::to_string(&canonicalize(&input)).map_err(|_error| {
        diagnostic(
            "invalid_schema_graph",
            "the synthesized MCP resource schema cannot be encoded",
        )
    })?;
    let binding = McpExecutionBinding::new(
        protocol_version.to_string(),
        transport,
        server_url.to_string(),
        McpInvocationMethod::ReadResource,
        None,
        None,
        input_schema.clone(),
        None,
        None,
        server_task_capability.cloned(),
        None,
    );
    Ok(McpToolBuild {
        key,
        relative_path: RESOURCE_READ_PATH.to_string(),
        summary: "Read one opaque Provider-bound MCP resource reference".to_string(),
        presentation: ToolPresentation::new(
            Some("Read MCP resource".to_string()),
            "Read one opaque Provider-bound MCP resource reference".to_string(),
            false,
            vec!["resources".to_string()],
            None,
        ),
        // `resources/read` reads by protocol definition; it is not a tool hint.
        hints: ToolHints::new(Some(true), None, None, None),
        binding,
        input_schema,
        output_schema: None,
        input_schema_json,
        output_schema_json: "null".to_string(),
        diagnostics: Vec::new(),
    })
}

fn tool_schema_source(key: &ToolKey, direction: &str) -> String {
    format!(
        "mcp:tool:{}:{}:{direction}",
        key.as_str().len(),
        key.as_str()
    )
}

fn schema_requires_object(root: &Value, schema: &Value, seen: &mut HashSet<String>) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    if object.get("type").and_then(Value::as_str) == Some("object") {
        return true;
    }
    if let Some(reference) = object.get("$ref").and_then(Value::as_str)
        && reference.starts_with('#')
        && seen.insert(reference.to_string())
    {
        let requires_object = root
            .pointer(reference.strip_prefix('#').unwrap_or_default())
            .is_some_and(|target| schema_requires_object(root, target, seen));
        seen.remove(reference);
        if requires_object {
            return true;
        }
    }
    if let Some(branches) = object.get("allOf").and_then(Value::as_array)
        && branches
            .iter()
            .any(|branch| schema_requires_object(root, branch, seen))
    {
        return true;
    }
    ["anyOf", "oneOf"].iter().any(|keyword| {
        object
            .get(*keyword)
            .and_then(Value::as_array)
            .is_some_and(|branches| {
                !branches.is_empty()
                    && branches
                        .iter()
                        .all(|branch| schema_requires_object(root, branch, seen))
            })
    })
}

fn retain_tool_schema(
    schemas: &mut SchemaGraphAccumulator,
    source: &str,
    value: &Value,
    code: &'static str,
    message: &'static str,
    diagnostics: &mut Vec<CatalogDiagnostic>,
) -> Result<(SchemaId, String), CatalogDiagnostic> {
    let canonical = canonicalize(value);
    let exact_json = serde_json::to_string(&canonical).map_err(|_error| {
        diagnostic(
            "invalid_mcp_discovery_snapshot",
            "an MCP tool schema cannot be encoded",
        )
    })?;
    let retained = schema_requires_object(&canonical, &canonical, &mut HashSet::new())
        .then(|| schemas.add_embedded_schema(source, &canonical))
        .transpose()
        .and_then(|schema| schema.ok_or_else(invalid_snapshot));
    match retained {
        Ok(schema) => Ok((schema, exact_json)),
        Err(_diagnostic) => {
            diagnostics.push(diagnostic(code, message));
            let fallback_source = format!("{source}:unsupported");
            let fallback = Value::Object(Map::new());
            schemas
                .add_embedded_schema(&fallback_source, &fallback)
                .map(|schema| (schema, exact_json))
        }
    }
}

fn tool_presentation(
    object: &Map<String, Value>,
    summary: &str,
    annotations: Option<&McpToolAnnotations>,
) -> ToolPresentation {
    let title = safe_tool_text(object.get("title"))
        .or_else(|| annotations.and_then(McpToolAnnotations::title))
        .map(str::to_string);
    let description = safe_tool_text(object.get("description"))
        .unwrap_or(summary)
        .to_string();
    ToolPresentation::new(title, description, false, Vec::new(), None)
}

fn safe_tool_text(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| crate::types::valid_presentation_text(value))
}

fn tool_summary(
    object: &Map<String, Value>,
    name: &str,
    diagnostics: &mut Vec<CatalogDiagnostic>,
) -> String {
    for field in ["title", "description"] {
        match object.get(field).map(safe_tool_summary_text) {
            Some(Some(value)) => return value.to_string(),
            Some(None) => diagnostics.push(CatalogDiagnostic::warning(
                "invalid_mcp_tool_metadata",
                "an MCP tool has invalid presentation metadata and uses safe fallback text",
            )),
            None => {}
        }
    }
    name.to_string()
}

/// Returns one MCP tool summary short enough to label a Tool.
fn safe_tool_summary_text(value: &Value) -> Option<&str> {
    safe_tool_text(Some(value)).filter(|value| value.len() <= 1024)
}

fn parse_annotations(
    value: Option<&Value>,
    diagnostics: &mut Vec<CatalogDiagnostic>,
) -> Option<McpToolAnnotations> {
    let value = value?;
    let Some(object) = value.as_object() else {
        diagnostics.push(diagnostic(
            "invalid_mcp_tool_annotations",
            "an MCP tool has invalid annotation evidence",
        ));
        return None;
    };
    let title = optional_string(object, "title");
    let read_only_hint = optional_bool(object, "readOnlyHint");
    let destructive_hint = optional_bool(object, "destructiveHint");
    let idempotent_hint = optional_bool(object, "idempotentHint");
    let open_world_hint = optional_bool(object, "openWorldHint");
    if title.is_err()
        || read_only_hint.is_err()
        || destructive_hint.is_err()
        || idempotent_hint.is_err()
        || open_world_hint.is_err()
    {
        diagnostics.push(diagnostic(
            "invalid_mcp_tool_annotations",
            "an MCP tool has invalid annotation evidence",
        ));
    }
    let extensions = object
        .iter()
        .filter(|(name, _value)| {
            !matches!(
                name.as_str(),
                "title" | "readOnlyHint" | "destructiveHint" | "idempotentHint" | "openWorldHint"
            )
        })
        .map(|(name, value)| (name.clone(), canonicalize(value)))
        .collect();
    Some(McpToolAnnotations::new(
        title.ok().flatten(),
        read_only_hint.ok().flatten(),
        destructive_hint.ok().flatten(),
        idempotent_hint.ok().flatten(),
        open_world_hint.ok().flatten(),
        extensions,
    ))
}

fn optional_string(object: &Map<String, Value>, field: &str) -> Result<Option<String>, ()> {
    match object.get(field) {
        None => Ok(None),
        Some(value) => safe_tool_text(Some(value))
            .map(|value| Some(value.to_string()))
            .ok_or(()),
    }
}

fn optional_bool(object: &Map<String, Value>, field: &str) -> Result<Option<bool>, ()> {
    match object.get(field) {
        None => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(()),
    }
}

fn parse_task_support(
    execution: Option<&Value>,
    diagnostics: &mut Vec<CatalogDiagnostic>,
) -> Option<McpTaskSupport> {
    let execution = execution?;
    let Some(execution) = execution.as_object() else {
        diagnostics.push(diagnostic(
            "invalid_mcp_task_support",
            "an MCP tool has invalid task-support evidence",
        ));
        return None;
    };
    match execution.get("taskSupport") {
        None => None,
        Some(Value::String(value)) => McpTaskSupport::parse(value).or_else(|| {
            diagnostics.push(diagnostic(
                "invalid_mcp_task_support",
                "an MCP tool has invalid task-support evidence",
            ));
            None
        }),
        Some(_) => {
            diagnostics.push(diagnostic(
                "invalid_mcp_task_support",
                "an MCP tool has invalid task-support evidence",
            ));
            None
        }
    }
}

fn invalid_snapshot() -> CatalogDiagnostic {
    diagnostic(
        "invalid_mcp_discovery_snapshot",
        "the pinned MCP discovery snapshot has an invalid root contract",
    )
}

fn invalid_tool_index() -> CatalogDiagnostic {
    diagnostic(
        "invalid_tool_path_index",
        "the Materialized Tool Catalog has an incoherent path index",
    )
}

fn diagnostic(code: &'static str, message: &'static str) -> CatalogDiagnostic {
    CatalogDiagnostic::stable(code, message)
}

/// Reads the secret-free contract the discovery snapshot was captured under.
/// An MCP source can be credential-free, bearer-authenticated, or OAuth-authorized.
/// A credential a person types into a header or a Basic pair has no MCP injection
/// recipe, so a snapshot that names one is refused.
pub(super) fn credential_contract(
    root: &Map<String, Value>,
) -> Result<crate::ProviderCredentialContract, CatalogDiagnostic> {
    let invalid = || {
        diagnostic(
            "invalid_credential_contract",
            "the MCP discovery snapshot carries an unsupported credential contract",
        )
    };
    let Some(value) = root.get("credentialContract") else {
        return Ok(crate::ProviderCredentialContract::NONE);
    };
    let contract: crate::ProviderCredentialContract =
        serde_json::from_value(value.clone()).map_err(|_invalid| invalid())?;
    if matches!(
        contract.authentication(),
        AuthenticationConfiguration::HeaderCredentials { .. }
            | AuthenticationConfiguration::BasicAuth { .. }
            | AuthenticationConfiguration::QueryCredentials { .. }
    ) {
        return Err(invalid());
    }
    Ok(contract)
}
