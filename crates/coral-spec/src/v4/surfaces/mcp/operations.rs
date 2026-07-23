use crate::v4::ir::{
    IrEntityCandidate, IrExecutionAttachment, IrOperation, McpExecutionAttachment,
};
use crate::v4::surfaces::json_schema::{WrappedListInferenceContext, infer_wrapped_list};
use crate::v4::{McpOperationPagination, OperationMetadata};

use super::import::McpImporter;
use super::model::McpToolDescriptor;
use super::pagination::infer_mcp_pagination_contracts;

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
        let row_path = tool
            .output_schema
            .as_ref()
            .and_then(|schema| {
                infer_wrapped_list(WrappedListInferenceContext {
                    operation_name: &tool.name,
                    schema_root: schema,
                    response_schema: schema,
                })
            })
            .map(|inference| inference.row_path)
            .unwrap_or_default();
        let (pagination, offset_pagination) = infer_mcp_pagination_contracts(
            &inputs,
            &output,
            &row_path,
            tool.output_schema.as_ref(),
            &tool.input_schema,
        );
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
            entity: Some(IrEntityCandidate {
                name: operation_id.to_string(),
                type_ref: format!("{operation_id}_row"),
                identity_fields: Vec::new(),
            }),
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
                    cursor: pagination,
                    offset: offset_pagination,
                },
            },
        ))
    }
}
