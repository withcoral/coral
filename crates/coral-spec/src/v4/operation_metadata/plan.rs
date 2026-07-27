use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec};
use crate::v4::ir::{IrExecutionAttachment, IrInputLocation, IrOperation, SemanticIr};
use crate::v4::operation_metadata::model::{OperationMetadata, OperationMetadataCatalog};
use crate::v4::operation_metadata::policy::{
    resolve_output_row_type_ref, rest_pagination_owned_inputs,
    validate_operation_metadata_structure,
};
use crate::v4::operation_metadata::structural::validate_semantic_ir_structure;
use crate::{PaginationSpec, Result};

/// Structurally validated pairing of imported facts and effective policy.
#[derive(Debug, Clone, Serialize)]
pub struct ValidatedSurfacePlan {
    semantic_ir: SemanticIr,
    operation_metadata: OperationMetadataCatalog,
}

impl<'de> Deserialize<'de> for ValidatedSurfacePlan {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct UnvalidatedPlan {
            semantic_ir: SemanticIr,
            operation_metadata: OperationMetadataCatalog,
        }

        let plan = UnvalidatedPlan::deserialize(deserializer)?;
        Self::new(plan.semantic_ir, plan.operation_metadata).map_err(D::Error::custom)
    }
}

impl ValidatedSurfacePlan {
    pub fn new(
        semantic_ir: SemanticIr,
        operation_metadata: OperationMetadataCatalog,
    ) -> Result<Self> {
        validate_semantic_ir_structure(&semantic_ir)?;
        validate_operation_metadata_structure(&semantic_ir, &operation_metadata)?;
        Ok(Self {
            semantic_ir,
            operation_metadata,
        })
    }

    #[must_use]
    pub fn semantic_ir(&self) -> &SemanticIr {
        &self.semantic_ir
    }

    #[must_use]
    pub fn operation_metadata(&self) -> &OperationMetadataCatalog {
        &self.operation_metadata
    }

    #[must_use]
    /// Returns metadata for an operation in this validated plan.
    ///
    /// # Panics
    ///
    /// Panics when `operation_id` is not part of the paired semantic IR.
    pub fn metadata_for_operation(&self, operation_id: &str) -> &OperationMetadata {
        self.operation_metadata
            .operations
            .get(operation_id)
            .expect("validated plan contains metadata for every operation")
    }

    #[must_use]
    /// Returns the path from the response root to an operation's rows.
    pub fn output_row_path(&self, operation_id: &str) -> &[String] {
        self.metadata_for_operation(operation_id).row_path()
    }

    #[must_use]
    /// Returns the REST row type that remains after applying the operation's
    /// row path.
    ///
    /// # Panics
    ///
    /// Panics when the operation is absent, is not a REST operation, or the
    /// validated-plan invariant that its row path resolves has been violated.
    pub fn rest_output_type_ref(&self, operation_id: &str) -> &str {
        let operation = self
            .semantic_ir
            .operations
            .iter()
            .find(|operation| operation.id == operation_id)
            .expect("validated plan contains the requested operation");
        assert!(
            matches!(operation.execution, IrExecutionAttachment::Rest(_)),
            "MCP operation has no REST output type"
        );
        let types = self
            .semantic_ir
            .types
            .iter()
            .map(|ty| (ty.id.as_str(), ty))
            .collect();
        resolve_output_row_type_ref(
            &operation.output,
            self.output_row_path(operation_id),
            &types,
        )
        .expect("validated row path resolves to an output type")
    }

    #[must_use]
    /// Returns effective REST pagination for a REST operation.
    ///
    /// # Panics
    ///
    /// Panics when the operation is absent or is not a REST operation.
    pub fn rest_pagination(&self, operation_id: &str) -> &PaginationSpec {
        match self.metadata_for_operation(operation_id) {
            OperationMetadata::Rest { pagination, .. } => pagination,
            OperationMetadata::Mcp { .. } => panic!("REST operation has MCP metadata"),
        }
    }

    #[must_use]
    /// Returns effective cursor and offset pagination for an MCP operation.
    ///
    /// # Panics
    ///
    /// Panics when the operation is absent or is not an MCP operation.
    pub fn mcp_pagination(
        &self,
        operation_id: &str,
    ) -> (Option<&McpPaginationSpec>, Option<&McpOffsetPaginationSpec>) {
        match self.metadata_for_operation(operation_id) {
            OperationMetadata::Mcp { pagination, .. } => {
                (pagination.cursor.as_ref(), pagination.offset.as_ref())
            }
            OperationMetadata::Rest { .. } => panic!("MCP operation has REST metadata"),
        }
    }

    #[must_use]
    pub fn input_is_lookup_key(&self, operation_id: &str, input_name: &str) -> bool {
        matches!(
            self.metadata_for_operation(operation_id),
            OperationMetadata::Rest { lookup_keys, .. }
                if lookup_keys.iter().any(|candidate| candidate == input_name)
        )
    }

    #[must_use]
    /// Pagination only ever owns query (REST) or tool-arg (MCP) inputs; an
    /// input in another location that shares a pagination parameter's name is
    /// not owned.
    pub fn pagination_owns_input(
        &self,
        operation: &IrOperation,
        input_name: &str,
        location: IrInputLocation,
    ) -> bool {
        match self.metadata_for_operation(&operation.id) {
            OperationMetadata::Rest { pagination, .. } => {
                location == IrInputLocation::Query
                    && rest_pagination_owned_inputs(operation, pagination)
                        .is_ok_and(|owned| owned.contains(input_name))
            }
            OperationMetadata::Mcp { pagination, .. } => {
                location == IrInputLocation::ToolArg
                    && (pagination
                        .cursor
                        .as_ref()
                        .is_some_and(|cursor| cursor.cursor_arg == input_name)
                        || pagination.offset.as_ref().is_some_and(|offset| {
                            offset.limit_arg == input_name || offset.offset_arg == input_name
                        }))
            }
        }
    }
}
