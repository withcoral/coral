use std::collections::{BTreeMap, BTreeSet};

use coral_capabilities::{
    Capability, Diagnostic, DiagnosticSeverity, DiagnosticStage, EffectProfile, InvocationSchema,
    McpTaskSupport, McpToolUpstreamBinding, OutputContract, ProviderOrigin, ProviderOriginKind,
    ShapeHints, SourceId, UpstreamBinding,
};
use coral_spec::{McpInterface, SourceSpec};
use serde_json::Value;

use crate::auth::credential_requirements;
use crate::hash::sha256_hex;
use crate::naming::OperationIdAllocator;
use crate::schema_shape::{schema_shape_view, shape_hints_from_json_schema};
use crate::{
    ImportedInterface, ImporterError, ProviderSnapshotArtifact, RawInterfaceInput, Result,
};

pub(super) fn import_mcp(
    source_id: &SourceId,
    spec: &SourceSpec,
    interface: &McpInterface,
    raw_inputs: &BTreeMap<String, RawInterfaceInput>,
) -> Result<ImportedInterface> {
    let value = mcp_tools_list_value(interface, raw_inputs)?;
    let tools = value
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut seen = BTreeSet::new();
    let mut operation_ids = OperationIdAllocator::default();
    let mut capabilities = Vec::new();
    let mut diagnostics = Vec::new();
    for tool in &tools {
        let name = tool.get("name").and_then(Value::as_str).unwrap_or_default();
        if name.is_empty() || !seen.insert(name.to_string()) {
            diagnostics.push(Diagnostic::new(
                "MCP_TOOL_UNSUPPORTED",
                DiagnosticSeverity::Warning,
                DiagnosticStage::ProviderImport,
                format!("MCP tool '{name}' is missing or duplicated"),
            ));
            continue;
        }
        capabilities.push(import_mcp_tool_capability(
            source_id,
            spec,
            interface,
            tool,
            name,
            &mut operation_ids,
        ));
    }
    let snapshot = ProviderSnapshotArtifact {
        artifact_schema_version: 1,
        source_id: source_id.clone(),
        interface_id: interface.id.clone(),
        interface_type: "mcp".to_string(),
        importer_version: "mcp-tools-list-v1".to_string(),
        source_document_sha256: sha256_hex(value.to_string().as_bytes()),
        snapshot: value.clone(),
        diagnostics,
    };
    Ok(ImportedInterface {
        snapshot,
        capabilities,
    })
}

fn import_mcp_tool_capability(
    source_id: &SourceId,
    spec: &SourceSpec,
    interface: &McpInterface,
    tool: &Value,
    name: &str,
    operation_ids: &mut OperationIdAllocator,
) -> Capability {
    let operation_id = operation_ids.allocate(name);
    let provider_ref = format!(
        "interfaces/{}/provider-snapshot.yaml#/tools/{operation_id}",
        interface.id
    );
    let mut capability = Capability::new(
        source_id.clone(),
        interface.id.clone(),
        operation_id,
        ProviderOrigin {
            kind: ProviderOriginKind::McpTool,
            snapshot_ref: provider_ref,
            provider_name: name.to_string(),
        },
        UpstreamBinding::McpTool(McpToolUpstreamBinding {
            server_ref: format!(
                "source/{source_id}/interface/{}/server/default",
                interface.id
            ),
            tool_name: name.to_string(),
            task_support: McpTaskSupport::Forbidden,
        }),
    );
    capability.display.title = tool
        .get("title")
        .or_else(|| {
            tool.get("annotations")
                .and_then(|annotations| annotations.get("title"))
        })
        .and_then(Value::as_str)
        .unwrap_or(name)
        .to_string();
    capability.display.description = tool
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let output_schema = tool.get("outputSchema").cloned();
    capability.input_schema = InvocationSchema::new(
        tool.get("inputSchema")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"type":"object"})),
    );
    capability.output_contract = OutputContract::McpStructuredContent {
        schema: output_schema.clone().map(InvocationSchema::new),
    };
    capability.effect_profile = mcp_effect_profile(tool);
    capability.shape_hints = if capability
        .effect_profile
        .effects
        .contains(&coral_capabilities::EffectKind::Read)
    {
        output_schema
            .as_ref()
            .map_or_else(ShapeHints::unknown, |schema| {
                shape_hints_from_json_schema(&schema_shape_view(schema))
            })
    } else {
        ShapeHints::unknown()
    };
    capability.credential_requirements =
        credential_requirements(spec, interface.server.auth.as_ref());
    capability
}

fn mcp_tools_list_value<'a>(
    interface: &McpInterface,
    raw_inputs: &'a BTreeMap<String, RawInterfaceInput>,
) -> Result<&'a Value> {
    let raw = raw_inputs
        .get(&interface.id)
        .ok_or_else(|| ImporterError::MissingRawInput(interface.id.clone()))?;
    let RawInterfaceInput::McpToolsList { value } = raw else {
        return Err(ImporterError::Parse {
            interface_id: interface.id.clone(),
            message: "expected MCP tools/list value".to_string(),
        });
    };
    Ok(value)
}

pub(super) fn mcp_effect_profile(tool: &Value) -> EffectProfile {
    let annotations = tool.get("annotations").unwrap_or(&Value::Null);
    if annotations
        .get("readOnlyHint")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return EffectProfile::read();
    }

    let idempotency = if annotations
        .get("idempotentHint")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        coral_capabilities::IdempotencyKind::Idempotent
    } else {
        coral_capabilities::IdempotencyKind::NonIdempotent
    };

    if annotations
        .get("destructiveHint")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return EffectProfile {
            idempotency,
            ..EffectProfile::delete()
        };
    }

    EffectProfile {
        idempotency,
        ..EffectProfile::write()
    }
}
