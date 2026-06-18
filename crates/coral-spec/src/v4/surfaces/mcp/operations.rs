use crate::v4::ir::{
    IrEntityCandidate, IrExecutionAttachment, IrOperation, IrOperationInput, IrOperationOutput,
    McpExecutionAttachment, OutputCardinality,
};
use crate::v4::pagination::{
    McpPaginationMatcher, PaginationProvenance, V4OperationTarget, V4PaginationMatcher,
    V4PaginationOutcome, duplicate_operation_overlay_error, multiple_profile_match_error,
};
use crate::{ManifestError, Result};

use super::import::McpImporter;
use super::model::McpToolDescriptor;

impl McpImporter<'_> {
    pub(super) fn import_tool(
        &mut self,
        tool: &McpToolDescriptor,
        operation_id: &str,
    ) -> Result<Option<IrOperation>> {
        let mut diagnostics = Vec::new();
        let imported_inputs = self.import_inputs(tool, operation_id, &mut diagnostics);
        let input_schema_complete = imported_inputs.schema_complete;
        if !input_schema_complete {
            self.diagnostics.extend(diagnostics);
            return Ok(None);
        }

        let inputs = imported_inputs.inputs;
        let output = self.import_output(operation_id, tool.output_schema.as_ref());
        let (pagination, offset_pagination, pagination_provenance) =
            self.resolve_pagination_overlay(tool, &inputs, &output)?;
        Ok(Some(IrOperation {
            id: operation_id.to_string(),
            method_name: "tools/call".to_string(),
            description: tool
                .description
                .clone()
                .or_else(|| tool.title.clone())
                .unwrap_or_default(),
            deprecated: false,
            read_only: tool.read_only_hint.unwrap_or(false),
            inputs,
            output,
            entity: Some(IrEntityCandidate {
                name: operation_id.to_string(),
                type_ref: format!("{operation_id}_row"),
                identity_fields: Vec::new(),
            }),
            execution: IrExecutionAttachment::Mcp(McpExecutionAttachment {
                tool_name: tool.name.clone(),
                pagination,
                offset_pagination,
                pagination_provenance,
            }),
            diagnostics,
        }))
    }
}

impl McpImporter<'_> {
    fn resolve_pagination_overlay(
        &mut self,
        tool: &McpToolDescriptor,
        inputs: &[IrOperationInput],
        output: &IrOperationOutput,
    ) -> Result<(
        Option<crate::backends::mcp::McpPaginationSpec>,
        Option<crate::backends::mcp::McpOffsetPaginationSpec>,
        PaginationProvenance,
    )> {
        let explicit_matches = self
            .surface
            .pagination
            .operations
            .iter()
            .enumerate()
            .filter(|(_, overlay)| {
                let V4OperationTarget::Mcp(target) = &overlay.target else {
                    return false;
                };
                target.tool == tool.name
            })
            .map(|(index, overlay)| (index, overlay.outcome.clone()))
            .collect::<Vec<_>>();
        if explicit_matches.len() > 1 {
            return Err(duplicate_operation_overlay_error(
                &self.manifest.common.name,
                &self.surface.id,
                &format!("tool '{}'", tool.name),
            ));
        }
        if let Some((index, outcome)) = explicit_matches.into_iter().next() {
            if let Some(matched) = self.matched_pagination_overlays.get_mut(index) {
                *matched = true;
            }
            return self.mcp_pagination_from_outcome(outcome, PaginationProvenance::Authored);
        }

        let mut matching_profiles = Vec::new();
        for profile in &self.surface.pagination.profiles {
            let V4PaginationMatcher::Mcp(matcher) = &profile.matcher else {
                continue;
            };
            if mcp_profile_matches(matcher, tool, inputs) {
                matching_profiles.push((profile.name.clone(), profile.outcome.clone()));
            }
        }
        if matching_profiles.len() > 1 {
            let names = matching_profiles
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            return Err(multiple_profile_match_error(
                &self.manifest.common.name,
                &self.surface.id,
                &format!("tool '{}'", tool.name),
                &names,
            ));
        }
        if let Some((_, outcome)) = matching_profiles.into_iter().next() {
            return self
                .mcp_pagination_from_outcome(outcome, PaginationProvenance::ProfileGenerated);
        }
        if likely_mcp_pagination(inputs, output, tool.output_schema.as_ref()) {
            self.diagnostics.push(crate::v4::Diagnostic::warning(
                "PAGINATION_OVERLAY_MISSING",
                format!(
                    "tool '{}' has pagination-like schema fields but no V4 pagination overlay matched",
                    tool.name
                ),
                self.surface.id.clone(),
                None,
            ));
        }
        Ok((None, None, PaginationProvenance::None))
    }

    fn mcp_pagination_from_outcome(
        &self,
        outcome: V4PaginationOutcome,
        provenance: PaginationProvenance,
    ) -> Result<(
        Option<crate::backends::mcp::McpPaginationSpec>,
        Option<crate::backends::mcp::McpOffsetPaginationSpec>,
        PaginationProvenance,
    )> {
        match outcome {
            V4PaginationOutcome::McpCursor(pagination) => Ok((Some(pagination), None, provenance)),
            V4PaginationOutcome::McpOffset(pagination) => Ok((None, Some(pagination), provenance)),
            V4PaginationOutcome::Unsupported { reason } => {
                let _ = reason;
                Ok((None, None, PaginationProvenance::Unsupported))
            }
            V4PaginationOutcome::Http(_) => Err(ManifestError::validation(format!(
                "source '{}' surface '{}' MCP pagination overlay must use MCP pagination",
                self.manifest.common.name, self.surface.id
            ))),
        }
    }
}

fn mcp_profile_matches(
    matcher: &McpPaginationMatcher,
    tool: &McpToolDescriptor,
    inputs: &[IrOperationInput],
) -> bool {
    if !matcher.tools.is_empty()
        && !matcher
            .tools
            .iter()
            .any(|candidate| candidate == &tool.name)
    {
        return false;
    }
    if !matcher.tool_args.is_empty()
        && !matcher
            .tool_args
            .iter()
            .all(|name| inputs.iter().any(|input| input.name == *name))
    {
        return false;
    }
    if matcher.offset_args
        && !(inputs.iter().any(|input| input.name == "limit")
            && inputs.iter().any(|input| input.name == "offset"))
    {
        return false;
    }
    matcher.response_cursor_path.is_empty()
        || tool
            .output_schema
            .as_ref()
            .is_some_and(|schema| schema_has_property_path(schema, &matcher.response_cursor_path))
}

fn likely_mcp_pagination(
    inputs: &[IrOperationInput],
    output: &IrOperationOutput,
    output_schema: Option<&serde_json::Value>,
) -> bool {
    if !matches!(
        output.cardinality,
        OutputCardinality::List | OutputCardinality::WrappedList
    ) {
        return false;
    }
    inputs.iter().any(|input| {
        matches!(
            input.name.as_str(),
            "cursor" | "after" | "page_token" | "next_cursor" | "next_token" | "limit" | "offset"
        )
    }) || output_schema.is_some_and(|schema| {
        schema_has_property_path(schema, &["nextCursor".to_string()])
            || schema_has_property_path(schema, &["nextToken".to_string()])
    })
}

fn schema_has_property_path(schema: &serde_json::Value, path: &[String]) -> bool {
    if path.is_empty() {
        return true;
    }
    let Some((first, rest)) = path.split_first() else {
        return true;
    };
    let Some(properties) = schema
        .get("properties")
        .and_then(serde_json::Value::as_object)
    else {
        return false;
    };
    let Some(property) = properties.get(first) else {
        return false;
    };
    schema_has_property_path(property, rest)
}
