//! Inferred execution policy for imported surfaces and its pairing with the
//! semantic IR: the artifact model, the structurally validated plan, and the
//! structural and policy validators.

mod model;
mod plan;
mod policy;
mod structural;

#[cfg(test)]
mod tests;

pub use model::{
    ImportedSurface, McpOperationPagination, OperationMetadata, OperationMetadataCatalog,
};
pub use plan::ValidatedSurfacePlan;
pub(crate) use policy::resolve_output_row_type_ref;
pub use policy::validate_operation_metadata_structure;
pub use structural::validate_semantic_ir_structure;
