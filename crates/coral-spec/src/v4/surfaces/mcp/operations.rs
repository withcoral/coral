use crate::v4::ir::{IrExecutionAttachment, IrOperation, McpExecutionAttachment};
use crate::v4::wrapped_lists::{WrappedListInferenceContext, infer_wrapped_list_row_path};
use crate::v4::{McpOperationPagination, OperationMetadata};

use super::import::McpImporter;
use super::model::McpToolDescriptor;
use super::pagination::{
    McpPaginationContracts, detect_mcp_pagination_contracts, is_list_like_output,
};

impl McpImporter<'_> {
    pub(super) fn import_tool(
        &mut self,
        tool: &McpToolDescriptor,
        operation_id: &str,
    ) -> Option<(IrOperation, OperationMetadata)> {
        let mut diagnostics = Vec::new();
        let imported_inputs = self.import_inputs(tool, operation_id, &mut diagnostics);
        let input_schema_complete = imported_inputs.schema_complete;
        if !input_schema_complete {
            self.diagnostics.extend(diagnostics);
            return None;
        }

        let inputs = imported_inputs.inputs;
        let output = self.import_output(operation_id, tool.output_schema.as_ref());
        let contracts = detect_mcp_pagination_contracts(
            &inputs,
            tool.output_schema.as_ref(),
            &tool.input_schema,
        );
        let row_path = tool.output_schema.as_ref().map_or_else(Vec::new, |schema| {
            infer_wrapped_list_row_path(WrappedListInferenceContext {
                operation_name: &tool.name,
                paginated_operation: contracts.is_paginated(),
                schema_root: schema,
                response_schema: schema,
            })
        });
        // A contract only becomes this tool's pagination once Coral reads its
        // result as a list, whether by declaration or by row path.
        let contracts = if is_list_like_output(&output, &row_path) {
            contracts
        } else {
            McpPaginationContracts::default()
        };
        let operation = IrOperation {
            id: operation_id.to_string(),
            method_name: "tools/call".to_string(),
            description: tool
                .description
                .clone()
                .or_else(|| tool.title.clone())
                .unwrap_or_default(),
            deprecated: false,
            read_only: tool.read_only_hint.unwrap_or(false),
            naming: None,
            inputs,
            output,
            entity_name: Some(operation_id.to_string()),
            execution: IrExecutionAttachment::Mcp(McpExecutionAttachment {
                tool_name: tool.name.clone(),
            }),
            diagnostics,
        };
        Some((
            operation,
            OperationMetadata::Mcp {
                row_path,
                pagination: McpOperationPagination {
                    cursor: contracts.cursor,
                    offset: contracts.offset,
                },
            },
        ))
    }
}
