//! Lazy administrative catalog projection. This projection deliberately has no
//! executable recipes, authoritative schemas, or TypeScript declarations.
use crate::{
    CatalogDiagnostic, MaterializedOperation, OperationAvailability, OperationHints, OperationKey,
};

/// Safe catalog facts needed by an administrative tree and its summary pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationSummary {
    /// Provider-local Operation identity.
    pub key: OperationKey,
    /// Operation-relative Call Path projection.
    pub relative_path: String,
    /// Provider-authored summary.
    pub summary: String,
    /// Safe Provider-authored prose, without schemas or executable material.
    pub description: String,
    /// Provider-authored grouping tags, in source order.
    pub tags: Vec<String>,
    /// Effect hints the Provider declared.
    pub hints: OperationHints,
    /// Structural availability, independent of caller readiness.
    pub availability: OperationAvailability,
    /// Safe structural diagnostics, including unavailable Operations.
    pub diagnostics: Vec<CatalogDiagnostic>,
}

impl OperationSummary {
    /// Summarizes one materialized Operation.
    pub(crate) fn of(operation: &MaterializedOperation) -> Self {
        Self {
            key: operation.key().clone(),
            relative_path: operation.relative_path().to_string(),
            summary: operation.summary().to_string(),
            description: operation.presentation().description().to_string(),
            tags: operation.presentation().tags().to_vec(),
            hints: operation.hints(),
            availability: operation.availability(),
            diagnostics: operation.diagnostics().to_vec(),
        }
    }
}
