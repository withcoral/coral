//! Lazy administrative catalog projection. This projection deliberately has no
//! executable recipes, authoritative schemas, or TypeScript declarations.
use crate::{CatalogDiagnostic, OperationAvailability, OperationHints, OperationKey};

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
